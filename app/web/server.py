"""
FastAPI-сервер веб-интерфейса WB Автоответчик.
Запуск: uvicorn app.web.server:app --reload
"""
from __future__ import annotations

import asyncio
import base64
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Depends, Request, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Инициализация путей и логов до импорта app.db (который может использовать логи)
APP_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = str(APP_DIR / "data" / "reviews.db")
LOG_PATH = str(APP_DIR / "logs" / "app.log")
STATIC_DIR = Path(__file__).resolve().parent / "static"

from app.logging_config import setup_logging
setup_logging(LOG_PATH)

from app.db import Database, Store, ItemRow, PromptRow, UserRow, AuditEventRow
from app.web import tasks as web_tasks
from app.core.net import HttpStatusError
from app.core.wb_buyer_chat import (
    WbBuyerChatClient,
    collect_thread_lines,
    fetch_events_for_chat,
    merge_good_card,
    product_title_from_wb_chat,
)
from app.core.workflows import (
    auto_process_wb_buyer_chats,
    generate_mass,
    generate_wb_buyer_chat_reply,
    load_new_all,
    send_mass_all,
)

log = logging.getLogger("web")

app = FastAPI(title="MarketAI", version="1.1")
MSK_TZ = ZoneInfo("Europe/Moscow")

def _parse_origins(value: str) -> list[str]:
    items = [x.strip() for x in (value or "").split(",")]
    return [x for x in items if x]


_cors_origins = _parse_origins(os.getenv("CORS_ORIGINS", "").strip())
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(DB_PATH)
    return _db


SESSION_COOKIE = "wb_session"
SESSION_TTL_SECONDS = 7 * 24 * 60 * 60
PBKDF2_ITERATIONS = 210_000

def _session_secret() -> str:
    s = (os.getenv("SESSION_SECRET") or "").strip()
    if not s:
        raise HTTPException(503, "SESSION_SECRET не задан на сервере (переменная окружения).")
    return s


def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64u_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _hash_password(password: str, *, salt: Optional[bytes] = None, iterations: int = PBKDF2_ITERATIONS) -> str:
    if salt is None:
        salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64u(salt)}${_b64u(dk)}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        algo, it_s, salt_s, dk_s = (stored or "").split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        it = int(it_s)
        salt = _b64u_decode(salt_s)
        dk_expected = _b64u_decode(dk_s)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, it)
        return hmac.compare_digest(dk, dk_expected)
    except Exception:
        return False


def _sign_session(payload: dict) -> str:
    secret = _session_secret().encode("utf-8")
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    body = _b64u(raw)
    sig = _b64u(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
    return f"{body}.{sig}"


def _unsign_session(token: str) -> Optional[dict]:
    try:
        body, sig = (token or "").split(".", 1)
        secret = _session_secret().encode("utf-8")
        sig2 = _b64u(hmac.new(secret, body.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, sig2):
            return None
        payload = json.loads(_b64u_decode(body).decode("utf-8"))
        if not isinstance(payload, dict):
            return None
        exp = int(payload.get("exp") or 0)
        if exp <= int(time.time()):
            return None
        return payload
    except Exception:
        return None


def _bootstrap_admin_if_needed(db: Database) -> None:
    if db.count_users() > 0:
        return
    init_pass = (os.getenv("ADMIN_INIT_PASSWORD") or "").strip()
    if not init_pass:
        raise HTTPException(503, "Нет пользователей и не задан ADMIN_INIT_PASSWORD (переменная окружения).")
    db.create_user("admin", _hash_password(init_pass), role="admin")
    log.warning("Создан пользователь admin (bootstrap). Рекомендуется сменить пароль.")


def _get_current_user(request: Request, db: Database) -> Optional[UserRow]:
    token = (request.cookies.get(SESSION_COOKIE) or "").strip()
    if not token:
        return None
    payload = _unsign_session(token)
    if not payload:
        return None
    username = str(payload.get("u") or "").strip()
    if not username:
        return None
    return db.get_user_by_username(username)


def require_user(request: Request, db: Database = Depends(get_db)) -> UserRow:
    _bootstrap_admin_if_needed(db)
    u = _get_current_user(request, db)
    if not u:
        raise HTTPException(401, "Неавторизовано")
    return u


def require_admin(user: UserRow = Depends(require_user)) -> UserRow:
    if user.role != "admin":
        raise HTTPException(403, "Только для администратора")
    return user


def require_permission(permission: str):
    def _dep(user: UserRow = Depends(require_user), db: Database = Depends(get_db)) -> UserRow:
        if user.role == "admin":
            return user
        perms = db.get_user_permissions(user.id)
        if permission not in perms:
            raise HTTPException(403, "Нет доступа")
        return user
    return _dep


@app.middleware("http")
async def _api_auth_middleware(request: Request, call_next):
    path = request.url.path or ""
    if path.startswith("/api/") and request.method.upper() != "OPTIONS":
        # public auth endpoints
        if path.startswith("/api/auth/"):
            return await call_next(request)
        # everything else requires session
        try:
            db = get_db()
            _bootstrap_admin_if_needed(db)
            if not _get_current_user(request, db):
                return JSONResponse(status_code=401, content={"detail": "Неавторизовано"})
        except HTTPException as e:
            return JSONResponse(status_code=e.status_code, content={"detail": e.detail})

    return await call_next(request)


# ---------- Auth models ----------
class LoginBody(BaseModel):
    username: str
    password: str


class MeOut(BaseModel):
    username: str
    role: str
    permissions: list[str] = []


class UserOut(BaseModel):
    id: int
    username: str
    role: str
    permissions: list[str] = []


class UserCreateBody(BaseModel):
    username: str
    password: str
    role: str = "guest"


class AdminResetBody(BaseModel):
    reset_token: str
    new_password: str


# ---------- Pydantic models ----------
class StoreOut(BaseModel):
    id: int
    marketplace: str
    name: str
    active: bool
    business_id: Optional[int] = None
    client_id: Optional[str] = None


class StoreCreate(BaseModel):
    marketplace: str  # wb | yam | ozon
    name: str
    api_key: str
    active: bool = True
    business_id: Optional[int] = None
    client_id: Optional[str] = None


class StoreUpdate(BaseModel):
    name: Optional[str] = None
    api_key: Optional[str] = None
    active: Optional[bool] = None
    business_id: Optional[int] = None
    client_id: Optional[str] = None


class ItemOut(BaseModel):
    id: int
    store_id: int
    external_id: str
    item_type: str
    date: str
    rating: Optional[int]
    text: str
    author: str
    product_title: str
    status: str
    generated_text: str
    was_viewed: bool
    extra_json: str = ""


class PromptOut(BaseModel):
    id: int
    item_type: str
    rating_group: str
    prompt_text: str


class PromptUpdate(BaseModel):
    prompt_text: str


class LoadNewBody(BaseModel):
    store_ids: Optional[list[int]] = None  # null = все магазины


class GenerateBody(BaseModel):
    item_ids: list[int]


class SendBody(BaseModel):
    item_ids: list[int]

class ApplyTemplateBody(BaseModel):
    item_ids: list[int]
    template_text: str

class BulkItemsBody(BaseModel):
    item_ids: list[int]


class WbBuyerChatGenerateBody(BaseModel):
    chat_id: str


class WbBuyerChatSendBody(BaseModel):
    reply_sign: str
    message: str

class AutoScheduleBody(BaseModel):
    enabled: bool = False
    slots: list[str] = []   # ["09:00","13:30"]
    store_ids: list[int] = []  # обязательный выбор магазинов
    schedule_mode: str = "slots"  # slots | interval
    interval_hours: int = 1
    run_reviews: bool = True
    run_questions: bool = True
    run_wb_chats: bool = False

AUTO_SCHEDULE_KEY = "auto_schedule_json"
AUTO_LAST_RUN_KEY = "auto_schedule_last_run_at"
_WB_CHAT_LIST_TTL_S = 55.0
_wb_chat_list_cache: dict[int, tuple[float, list]] = {}
_scheduler_task: Optional[asyncio.Task] = None
_scheduler_seen: set[str] = set()
_auto_run_task: Optional[asyncio.Task] = None
_auto_state: dict = {
    "running": False,
    "slot": "",
    "phase": "idle",
    "last_started_at": "",
    "last_finished_at": "",
    "last_error": "",
}

def _normalize_slots(slots: list[str]) -> list[str]:
    out: list[str] = []
    for s in slots or []:
        t = (s or "").strip()
        if len(t) != 5 or t[2] != ":":
            continue
        hh = t[:2]
        mm = t[3:]
        if not (hh.isdigit() and mm.isdigit()):
            continue
        h = int(hh)
        m = int(mm)
        if 0 <= h <= 23 and 0 <= m <= 59:
            out.append(f"{h:02d}:{m:02d}")
    return sorted(set(out))

def _get_auto_schedule(db: Database) -> dict:
    raw = (db.get_setting(AUTO_SCHEDULE_KEY) or "").strip()
    cfg = {
        "enabled": False,
        "slots": [],
        "store_ids": [],
        "schedule_mode": "slots",
        "interval_hours": 1,
        "run_reviews": True,
        "run_questions": True,
        "run_wb_chats": False,
    }
    if not raw:
        return cfg
    try:
        obj = json.loads(raw)
        cfg["enabled"] = bool(obj.get("enabled"))
        cfg["slots"] = _normalize_slots(obj.get("slots") or [])
        cfg["store_ids"] = [int(x) for x in (obj.get("store_ids") or [])]
        mode = str(obj.get("schedule_mode") or "slots").strip().lower()
        cfg["schedule_mode"] = mode if mode in ("slots", "interval") else "slots"
        cfg["interval_hours"] = max(1, min(int(obj.get("interval_hours") or 1), 24))
        cfg["run_reviews"] = bool(obj.get("run_reviews", True))
        cfg["run_questions"] = bool(obj.get("run_questions", True))
        cfg["run_wb_chats"] = bool(obj.get("run_wb_chats", False))
    except Exception:
        pass
    return cfg

def _set_auto_schedule(db: Database, body: AutoScheduleBody) -> dict:
    mode = (body.schedule_mode or "slots").strip().lower()
    if mode not in ("slots", "interval"):
        mode = "slots"
    interval_hours = max(1, min(int(body.interval_hours or 1), 24))
    cfg = {
        "enabled": bool(body.enabled),
        "slots": _normalize_slots(body.slots or []),
        "store_ids": [int(x) for x in (body.store_ids or [])],
        "schedule_mode": mode,
        "interval_hours": interval_hours,
        "run_reviews": bool(body.run_reviews),
        "run_questions": bool(body.run_questions),
        "run_wb_chats": bool(body.run_wb_chats),
    }
    if not cfg["run_reviews"] and not cfg["run_questions"] and not cfg["run_wb_chats"]:
        raise HTTPException(400, "Нужно включить хотя бы один тип: отзывы, вопросы или чаты WB")
    db.set_setting(AUTO_SCHEDULE_KEY, json.dumps(cfg, ensure_ascii=False))
    return cfg

def _collect_pending_item_ids(db: Database, store_ids: list[int], *, item_types: list[str], limit_per_type: int = 2000) -> list[int]:
    ids: list[int] = []
    for sid in store_ids:
        for tp in item_types:
            offset = 0
            while True:
                page = db.list_items_filtered(
                    item_type=tp,
                    store_id=sid,
                    statuses=["new"],
                    has_answer=False,
                    limit=500,
                    offset=offset,
                )
                if not page:
                    break
                ids.extend([r.id for r in page])
                offset += len(page)
                if offset >= limit_per_type:
                    break
    # dedupe with order
    seen = set()
    out = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out

async def _run_auto_slot(slot: str) -> None:
    global _auto_state
    db = get_db()
    cfg = _get_auto_schedule(db)
    if not cfg.get("enabled"):
        return
    store_ids = [int(x) for x in (cfg.get("store_ids") or [])]
    stores = [s for s in db.list_stores() if s.active and s.id in store_ids]
    if not stores:
        return
    started_dt = dt.datetime.now(MSK_TZ)
    started = started_dt.isoformat(timespec="seconds")
    _auto_state.update({
        "running": True,
        "slot": slot,
        "phase": "load_new",
        "last_started_at": started,
        "last_error": "",
    })
    try:
        run_reviews = bool(cfg.get("run_reviews", True))
        run_questions = bool(cfg.get("run_questions", True))
        run_wb_chats = bool(cfg.get("run_wb_chats", False))
        item_types: list[str] = []
        if run_reviews:
            item_types.append("review")
        if run_questions:
            item_types.append("question")
        deleted = 0
        added = 0
        item_ids: list[int] = []
        gen_ok = gen_failed = 0
        sent_ok = sent_skipped = sent_failed = 0
        key = (db.get_setting("openai_key") or "").strip()
        if item_types:
            deleted = db.clear_items([s.id for s in stores], item_types=item_types)
            added = await load_new_all(db, stores)
            _auto_state["phase"] = "generate"
            item_ids = _collect_pending_item_ids(db, [s.id for s in stores], item_types=item_types)
            if item_ids and key:
                gen_ok, gen_failed = await generate_mass(db, item_ids, key, model="gpt-5.2")
                _auto_state["phase"] = "send"
                sent_ok, sent_skipped, sent_failed = await send_mass_all(db, item_ids)
        wb_stats: dict = {}
        if run_wb_chats:
            _auto_state["phase"] = "wb_chats"
            if key:
                wb_stats = await auto_process_wb_buyer_chats(db, stores, openai_key=key)
            else:
                wb_stats = {"wb_chat_skipped": 1, "reason": "no_openai_key"}
        db.add_audit_event(
            actor="system",
            action="auto_run",
            item_type="mixed",
            result="ok",
            meta={
                "slot": slot,
                "store_ids": [s.id for s in stores],
                "item_types": item_types,
                "deleted_before_load": deleted,
                "added": added,
                "candidates": len(item_ids),
                "gen_ok": gen_ok,
                "gen_failed": gen_failed,
                "sent_ok": sent_ok,
                "sent_skipped": sent_skipped,
                "sent_failed": sent_failed,
                "run_wb_chats": run_wb_chats,
                **wb_stats,
            },
        )
        _auto_state["phase"] = "done"
    except asyncio.CancelledError:
        _auto_state["phase"] = "cancelled"
        raise
    except Exception as e:
        _auto_state["phase"] = "error"
        _auto_state["last_error"] = str(e)
        raise
    finally:
        _auto_state["running"] = False
        finished_dt = dt.datetime.now(MSK_TZ)
        _auto_state["last_finished_at"] = finished_dt.isoformat(timespec="seconds")
        try:
            db.set_setting(AUTO_LAST_RUN_KEY, finished_dt.isoformat(timespec="seconds"))
        except Exception:
            pass

async def _auto_scheduler_loop() -> None:
    global _scheduler_seen, _auto_run_task
    while True:
        try:
            db = get_db()
            cfg = _get_auto_schedule(db)
            now = dt.datetime.now(MSK_TZ)
            day = now.strftime("%Y-%m-%d")
            hm = now.strftime("%H:%M")
            # чистим ключи прошлых дней
            _scheduler_seen = {k for k in _scheduler_seen if k.startswith(day + "|")}
            if cfg.get("enabled") and (_auto_run_task is None or _auto_run_task.done()):
                mode = str(cfg.get("schedule_mode") or "slots")
                run_reason = ""
                if mode == "interval":
                    interval_h = max(1, int(cfg.get("interval_hours") or 1))
                    last_run_s = (db.get_setting(AUTO_LAST_RUN_KEY) or "").strip()
                    due = False
                    if not last_run_s:
                        due = True
                    else:
                        try:
                            last_dt = dt.datetime.fromisoformat(last_run_s)
                            if last_dt.tzinfo is None:
                                last_dt = last_dt.replace(tzinfo=MSK_TZ)
                            due = (now - last_dt).total_seconds() >= interval_h * 3600
                        except Exception:
                            due = True
                    if due:
                        run_reason = f"interval:{interval_h}h"
                else:
                    for slot in (cfg.get("slots") or []):
                        key = f"{day}|{slot}"
                        if hm == slot and key not in _scheduler_seen:
                            _scheduler_seen.add(key)
                            run_reason = slot
                            break
                if run_reason:
                    try:
                        _auto_run_task = asyncio.create_task(_run_auto_slot(run_reason))
                        await _auto_run_task
                    except Exception as e:
                        try:
                            db.add_audit_event(
                                actor="system",
                                action="auto_run",
                                item_type="mixed",
                                result="error",
                                meta={"slot": run_reason, "error": str(e)},
                            )
                        except Exception:
                            pass
                    finally:
                        _auto_run_task = None
            await asyncio.sleep(20)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("auto scheduler loop failed")
            await asyncio.sleep(20)

def _auto_status(db: Database) -> dict:
    cfg = _get_auto_schedule(db)
    now = dt.datetime.now(MSK_TZ)
    hm = now.strftime("%H:%M")
    # ближайший слот сегодня/завтра
    next_slot = ""
    slots = cfg.get("slots") or []
    if cfg.get("schedule_mode") == "interval":
        interval_h = max(1, int(cfg.get("interval_hours") or 1))
        last_run_s = (db.get_setting(AUTO_LAST_RUN_KEY) or "").strip()
        if last_run_s:
            try:
                last_dt = dt.datetime.fromisoformat(last_run_s)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=MSK_TZ)
                due_at = last_dt + dt.timedelta(hours=interval_h)
                next_slot = due_at.strftime("%Y-%m-%d %H:%M")
            except Exception:
                next_slot = f"каждые {interval_h}ч"
        else:
            next_slot = "при первом цикле сразу"
    else:
        if slots:
            for s in slots:
                if s >= hm:
                    next_slot = s
                    break
            if not next_slot:
                next_slot = slots[0] + " (+1d)"
    out = dict(_auto_state)
    out.update({
        "enabled": bool(cfg.get("enabled")),
        "slots": slots,
        "store_ids": cfg.get("store_ids") or [],
        "schedule_mode": cfg.get("schedule_mode") or "slots",
        "interval_hours": int(cfg.get("interval_hours") or 1),
        "run_reviews": bool(cfg.get("run_reviews", True)),
        "run_questions": bool(cfg.get("run_questions", True)),
        "run_wb_chats": bool(cfg.get("run_wb_chats", False)),
        "next_slot": next_slot,
        "timezone": "Europe/Moscow",
    })
    return out


def _store_to_out(s: Store) -> StoreOut:
    return StoreOut(
        id=s.id,
        marketplace=s.marketplace,
        name=s.name,
        active=s.active,
        business_id=s.business_id,
        client_id=s.client_id,
    )


def _item_to_out(r: ItemRow) -> ItemOut:
    return ItemOut(
        id=r.id,
        store_id=r.store_id,
        external_id=r.external_id,
        item_type=r.item_type,
        date=r.date,
        rating=r.rating,
        text=r.text,
        author=r.author,
        product_title=r.product_title,
        status=r.status,
        generated_text=r.generated_text or "",
        was_viewed=r.was_viewed,
        extra_json=getattr(r, "extra_json", "") or "",
    )


# ---------- API: auth ----------
@app.get("/api/auth/me", response_model=MeOut)
def api_me(user: UserRow = Depends(require_user), db: Database = Depends(get_db)):
    if user.role == "admin":
        perms = ["view_settings", "view_log", "view_ops_log"]
    else:
        perms = db.get_user_permissions(user.id)
    return MeOut(username=user.username, role=user.role, permissions=perms)


@app.post("/api/auth/login")
def api_login(body: LoginBody, response: Response, db: Database = Depends(get_db)):
    _bootstrap_admin_if_needed(db)
    username = (body.username or "").strip()
    password = body.password or ""
    u = db.get_user_by_username(username)
    if not u or not _verify_password(password, u.password_hash):
        raise HTTPException(401, "Неверный логин или пароль")
    now = int(time.time())
    payload = {"u": u.username, "r": u.role, "exp": now + SESSION_TTL_SECONDS}
    token = _sign_session(payload)
    response.set_cookie(
        SESSION_COOKIE,
        token,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite="lax",
        secure=bool(os.getenv("COOKIE_SECURE", "").strip()),
        path="/",
    )
    return {"ok": True, "user": {"username": u.username, "role": u.role}}


@app.post("/api/auth/logout")
def api_logout(response: Response):
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"ok": True}


@app.post("/api/auth/admin-reset")
def api_admin_reset(body: AdminResetBody, db: Database = Depends(get_db)):
    expected = (os.getenv("ADMIN_RESET_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(503, "ADMIN_RESET_TOKEN не задан на сервере")
    token = (body.reset_token or "").strip()
    if not token or not hmac.compare_digest(token, expected):
        raise HTTPException(401, "Неверный reset token")
    if len(body.new_password or "") < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    u = db.get_user_by_username("admin")
    if not u:
        raise HTTPException(404, "Пользователь admin не найден")
    db.update_user_password("admin", _hash_password(body.new_password))
    return {"ok": True}


# ---------- API: users (admin) ----------
@app.get("/api/users", response_model=list[UserOut])
def api_list_users(_: UserRow = Depends(require_admin), db: Database = Depends(get_db)):
    return [
        UserOut(id=u.id, username=u.username, role=u.role, permissions=db.get_user_permissions(u.id))
        for u in db.list_users()
    ]


@app.post("/api/users", response_model=UserOut)
def api_create_user(body: UserCreateBody, _: UserRow = Depends(require_admin), db: Database = Depends(get_db)):
    username = (body.username or "").strip()
    if not username:
        raise HTTPException(400, "username обязателен")
    if len(body.password or "") < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    role = (body.role or "guest").strip() or "guest"
    if role not in ("admin", "guest"):
        raise HTTPException(400, "role должен быть admin или guest")
    try:
        uid = db.create_user(username, _hash_password(body.password), role=role)
    except Exception:
        raise HTTPException(400, "Пользователь уже существует или данные некорректны")
    u = db.get_user_by_username(username)
    if not u:
        raise HTTPException(500, "Не удалось создать пользователя")
    return UserOut(id=uid, username=u.username, role=u.role)


@app.delete("/api/users/{user_id}")
def api_delete_user(user_id: int, me: UserRow = Depends(require_admin), db: Database = Depends(get_db)):
    if int(user_id) == int(me.id):
        raise HTTPException(400, "Нельзя удалить текущего пользователя")
    db.delete_user(int(user_id))
    return {"ok": True}


class UserPermissionsBody(BaseModel):
    permissions: list[str]  # e.g. ["view_settings", "view_log", "view_ops_log"]


@app.get("/api/users/{user_id}/permissions")
def api_get_user_permissions(user_id: int, _: UserRow = Depends(require_admin), db: Database = Depends(get_db)):
    perms = db.get_user_permissions(int(user_id))
    return {"permissions": perms}


@app.patch("/api/users/{user_id}/permissions")
def api_set_user_permissions(user_id: int, body: UserPermissionsBody, _: UserRow = Depends(require_admin), db: Database = Depends(get_db)):
    allowed = {"view_settings", "view_log", "view_ops_log"}
    perms = [p for p in (body.permissions or []) if (p or "").strip() in allowed]
    db.set_user_permissions(int(user_id), perms)
    return {"permissions": perms}


# ---------- API: stores ----------
@app.get("/api/stores", response_model=list[StoreOut])
def api_list_stores(db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    stores = db.list_stores()
    return [_store_to_out(s) for s in stores]


@app.post("/api/stores", response_model=StoreOut)
def api_create_store(body: StoreCreate, db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    if body.marketplace == "wb":
        sid = db.upsert_store_wb(body.name, body.api_key, body.active)
    elif body.marketplace == "yam":
        if body.business_id is None:
            raise HTTPException(400, "business_id обязателен для Яндекс.Маркета")
        sid = db.upsert_store_yam(body.name, body.api_key, body.business_id, body.active)
    elif body.marketplace == "ozon":
        if not (body.client_id or "").strip():
            raise HTTPException(400, "client_id обязателен для Ozon")
        sid = db.upsert_store_ozon(body.name, body.api_key, body.client_id, body.active)
    else:
        raise HTTPException(400, "marketplace должен быть wb, yam или ozon")
    stores = [s for s in db.list_stores() if s.id == sid]
    if not stores:
        raise HTTPException(500, "Магазин не найден после создания")
    return _store_to_out(stores[0])


@app.get("/api/stores/{store_id}", response_model=StoreOut)
def api_get_store(store_id: int, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    stores = [s for s in db.list_stores() if s.id == store_id]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    return _store_to_out(stores[0])


@app.patch("/api/stores/{store_id}", response_model=StoreOut)
def api_update_store(store_id: int, body: StoreUpdate, db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    stores = [s for s in db.list_stores() if s.id == store_id]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    s = stores[0]
    name = body.name if body.name is not None else s.name
    api_key = body.api_key if body.api_key is not None else s.api_key
    active = body.active if body.active is not None else s.active
    business_id = body.business_id if body.business_id is not None else s.business_id
    client_id = body.client_id if body.client_id is not None else (s.client_id or "")
    db.update_store(store_id, name, api_key, active, business_id=business_id, client_id=client_id)
    updated = [x for x in db.list_stores() if x.id == store_id][0]
    return _store_to_out(updated)


@app.delete("/api/stores/{store_id}")
def api_delete_store(store_id: int, db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    stores = [s for s in db.list_stores() if s.id == store_id]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    db.delete_store(store_id)
    return {"ok": True}


# ---------- API: items ----------
@app.get("/api/items", response_model=list[ItemOut])
def api_list_items(
    item_type: str,  # review | question
    store_id: Optional[int] = None,
    status: Optional[str] = None,  # comma-separated or 'all'
    has_answer: Optional[str] = None,  # 'yes'|'no'|None
    limit: int = 200,
    offset: int = 0,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    statuses: Optional[list[str]] = None
    st = (status or "").strip().lower()
    if st and st != "all":
        statuses = [s.strip() for s in st.split(",") if s.strip()]
    ha = (has_answer or "").strip().lower()
    ha_val: Optional[bool] = None
    if ha == "yes":
        ha_val = True
    elif ha == "no":
        ha_val = False
    rows = db.list_items_filtered(
        item_type=item_type,
        store_id=store_id,
        statuses=statuses,
        has_answer=ha_val,
        limit=limit,
        offset=offset,
    )
    return [_item_to_out(r) for r in rows]


@app.get("/api/items/{item_id}", response_model=ItemOut)
def api_get_item(item_id: int, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    row = db.get_item_by_id(item_id)
    if not row:
        raise HTTPException(404, "Элемент не найден")
    return _item_to_out(row)


@app.post("/api/items/bulk", response_model=list[ItemOut])
def api_items_bulk(body: BulkItemsBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    ids = [int(x) for x in (body.item_ids or [])][:50]
    out = []
    for iid in ids:
        row = db.get_item_by_id(iid)
        if row:
            out.append(_item_to_out(row))
    return out


# ---------- API: settings ----------
@app.get("/api/settings")
def api_get_settings(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    keys = ["openai_key", "telegram_bot_token", "telegram_chat_id", "telegram_enabled", "theme"]
    return {k: db.get_setting(k) or "" for k in keys}


@app.post("/api/settings")
def api_set_settings(body: dict[str, str], db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    for k, v in body.items():
        db.set_setting(k, v or "")
    return {"ok": True}


@app.get("/api/auto-schedule")
def api_get_auto_schedule(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return _get_auto_schedule(db)


@app.post("/api/auto-schedule")
def api_set_auto_schedule(body: AutoScheduleBody, db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return _set_auto_schedule(db, body)


@app.get("/api/auto-schedule/status")
def api_auto_schedule_status(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return _auto_status(db)


@app.post("/api/auto-schedule/stop")
async def api_auto_schedule_stop(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    global _auto_run_task
    if _auto_run_task and not _auto_run_task.done():
        _auto_run_task.cancel()
        try:
            await _auto_run_task
        except Exception:
            pass
        _auto_run_task = None
        _auto_state["running"] = False
        _auto_state["phase"] = "cancelled"
        _auto_state["last_finished_at"] = dt.datetime.now(MSK_TZ).isoformat(timespec="seconds")
        return {"ok": True, "stopped": True}
    return {"ok": True, "stopped": False}


# ---------- API: prompts ----------
@app.get("/api/prompts", response_model=list[PromptOut])
def api_list_prompts(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return [PromptOut(id=p.id, item_type=p.item_type, rating_group=p.rating_group, prompt_text=p.prompt_text) for p in db.list_prompts()]


@app.patch("/api/prompts/{prompt_id}")
def api_update_prompt(prompt_id: int, body: PromptUpdate, db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    db.update_prompt(prompt_id, body.prompt_text)
    return {"ok": True}


# ---------- API: long-running tasks ----------
@app.post("/api/load-new")
async def api_load_new(body: LoadNewBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    task_id = await web_tasks.run_load_new(db, body.store_ids)
    return {"task_id": task_id}


@app.post("/api/generate")
async def api_generate(body: GenerateBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    key = db.get_setting("openai_key") or ""
    if not key.strip():
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    task_id = await web_tasks.run_generate(db, body.item_ids, key)
    return {"task_id": task_id}


@app.post("/api/send")
async def api_send(body: SendBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    task_id = await web_tasks.run_send(db, body.item_ids)
    return {"task_id": task_id}


@app.post("/api/apply-template")
def api_apply_template(body: ApplyTemplateBody, db: Database = Depends(get_db), user: UserRow = Depends(require_user)):
    text = (body.template_text or "").strip()
    if not text:
        raise HTTPException(400, "Шаблон пустой")
    applied = 0
    skipped = 0
    for item_id in body.item_ids or []:
        row = db.get_item_by_id(int(item_id))
        if not row:
            skipped += 1
            continue
        if row.item_type != "review":
            skipped += 1
            continue
        if (row.generated_text or "").strip():
            skipped += 1
            continue
        if row.status != "new":
            skipped += 1
            continue
        db.set_generated(int(item_id), text)
        applied += 1
    try:
        db.add_audit_event(
            actor=user.username,
            action="template_apply",
            item_type="review",
            result="ok",
            meta={"applied": applied, "skipped": skipped, "item_ids": (body.item_ids or [])[:50]},
        )
    except Exception:
        pass
    return {"applied": applied, "skipped": skipped}


@app.get("/api/log/dev")
def api_log_dev(
    level: Optional[str] = None,
    action: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 400,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_admin),
):
    """
    Dev-лог: только админ. Фильтры грубые (по подстроке).
    level: INFO|WARNING|ERROR
    action: load_new|generate|send|template|auth|users|stores
    """
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            f.seek(max(0, f.seek(0, 2) - 200 * 1024))
            lines = f.read().splitlines()
    except FileNotFoundError:
        lines = []
    except Exception as e:
        return {"lines": [str(e)]}

    lvl = (level or "").strip().upper()
    act = (action or "").strip().lower()
    qq = (q or "").strip().lower()
    action_map = {
        "load_new": ["load_new", "load-new"],
        "generate": ["generate"],
        "send": ["send"],
        "template": ["template"],
        "auth": ["login", "logout", "auth"],
        "users": ["users", "permissions"],
        "stores": ["stores"],
    }
    keys = action_map.get(act, [])

    out = []
    for ln in reversed(lines):
        s = ln
        if lvl and f"| {lvl} |" not in s:
            continue
        if keys and not any(k in s.lower() for k in keys):
            continue
        if qq and qq not in s.lower():
            continue
        out.append(s)
        if len(out) >= max(10, min(int(limit), 2000)):
            break
    return {"lines": list(reversed(out))}


@app.get("/api/log/ops")
def api_log_ops(
    action: Optional[str] = None,
    item_type: Optional[str] = None,
    store_id: Optional[int] = None,
    result: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_ops_log")),
):
    rows = db.list_audit_events(
        action=action,
        item_type=item_type,
        store_id=store_id,
        result=result,
        q=q,
        limit=limit,
        offset=offset,
    )
    return {
        "items": [
            {
                "id": r.id,
                "ts": r.ts,
                "actor": r.actor,
                "action": r.action,
                "item_type": r.item_type,
                "store_id": r.store_id,
                "result": r.result,
                "meta_json": r.meta_json,
            }
            for r in rows
        ]
    }


@app.get("/api/tasks/{task_id}")
async def api_get_task(task_id: str, _: UserRow = Depends(require_user)):
    state = await web_tasks.get_task(task_id)
    if state is None:
        raise HTTPException(404, "Задача не найдена")
    return state


@app.post("/api/tasks/{task_id}/cancel")
async def api_cancel_task(task_id: str, _: UserRow = Depends(require_user)):
    ok = await web_tasks.cancel_task(task_id)
    if not ok:
        raise HTTPException(404, "Задача не найдена")
    return {"ok": True}


async def _wb_buyer_chat_list_cached(store_id: int, api_key: str, *, force_refresh: bool) -> list:
    sid = int(store_id)
    if force_refresh:
        _wb_chat_list_cache.pop(sid, None)
    now = time.monotonic()
    ent = _wb_chat_list_cache.get(sid)
    if ent and (now - ent[0]) < _WB_CHAT_LIST_TTL_S:
        return list(ent[1])
    client = WbBuyerChatClient(api_key)
    try:
        chats = await client.list_chats()
    except HttpStatusError:
        raise
    _wb_chat_list_cache[sid] = (now, chats)
    return chats


def _require_wb_store_for_chats(db: Database, store_id: int) -> Store:
    stores = [s for s in db.list_stores() if s.id == int(store_id)]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    s = stores[0]
    if s.marketplace != "wb":
        raise HTTPException(400, "Чаты WB доступны только для магазинов Wildberries")
    if not (s.api_key or "").strip():
        raise HTTPException(400, "Не задан API-ключ магазина")
    return s


def _wb_chat_http_error(e: HttpStatusError) -> HTTPException:
    body = (e.body or "")[:500]
    if e.status == 401:
        return HTTPException(
            401,
            "WB buyer-chat: 401. Проверьте API-ключ и категорию токена «Чат с покупателями» (если WB разделил права — нужен токен с доступом к buyer-chat-api).",
        )
    if e.status == 402:
        return HTTPException(402, "WB buyer-chat: платный доступ или подписка (402).")
    if e.status == 429:
        return HTTPException(429, "WB buyer-chat: слишком много запросов (429). Подождите и повторите.")
    return HTTPException(e.status, f"WB buyer-chat: {body or e.status}")


@app.get("/api/wb/buyer-chats/{store_id}")
async def api_wb_buyer_chat_list(
    store_id: int,
    refresh: bool = Query(False, description="Сбросить кэш и заново запросить список у WB"),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    try:
        chats = await _wb_buyer_chat_list_cached(store_id, s.api_key, force_refresh=refresh)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    return {"chats": chats}


@app.get("/api/wb/buyer-chats/{store_id}/{chat_id}/thread")
async def api_wb_buyer_chat_thread(
    store_id: int,
    chat_id: str,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    client = WbBuyerChatClient(s.api_key)
    try:
        chats = await _wb_buyer_chat_list_cached(store_id, s.api_key, force_refresh=False)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    chat_row = next((c for c in chats if str(c.get("chatID") or "") == str(chat_id)), None)
    if not chat_row:
        raise HTTPException(404, "Чат не найден в списке. Нажмите «Обновить список чатов».")
    try:
        events, _next = await fetch_events_for_chat(client, chat_id, max_wb_requests=3)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    gc = merge_good_card(chat_row if isinstance(chat_row, dict) else {}, events)
    lines_ts = collect_thread_lines(events, chat_id)
    if not lines_ts and isinstance(chat_row, dict):
        lm = chat_row.get("lastMessage") or {}
        t = str(lm.get("text") or "").strip()
        if t:
            lines_ts = [("client", t, int(lm.get("addTimestamp") or 0))]
    lines = [{"role": r, "text": t, "addTimestamp": ts} for r, t, ts in lines_ts]
    texts_for_title = [t for _, t, __ in lines_ts]
    product_title = product_title_from_wb_chat(gc, texts_for_title)
    reply_sign = str(chat_row.get("replySign") or "").strip()
    return {
        "chat": chat_row,
        "events": events,
        "lines": lines,
        "product_title": product_title,
        "reply_sign": reply_sign,
        "good_card": gc,
    }


@app.post("/api/wb/buyer-chats/{store_id}/generate-draft")
async def api_wb_buyer_chat_generate(
    store_id: int,
    body: WbBuyerChatGenerateBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    chat_id = (body.chat_id or "").strip()
    if not chat_id:
        raise HTTPException(400, "chat_id пустой")
    client = WbBuyerChatClient(s.api_key)
    try:
        chats = await _wb_buyer_chat_list_cached(store_id, s.api_key, force_refresh=False)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    chat_row = next((c for c in chats if str(c.get("chatID") or "") == str(chat_id)), None)
    if not chat_row:
        raise HTTPException(404, "Чат не найден в списке")
    try:
        events, _ = await fetch_events_for_chat(client, chat_id, max_wb_requests=3)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    gc = merge_good_card(chat_row if isinstance(chat_row, dict) else {}, events)
    lines_ts = collect_thread_lines(events, chat_id)
    if not lines_ts and isinstance(chat_row, dict):
        lm = chat_row.get("lastMessage") or {}
        t = str(lm.get("text") or "").strip()
        if t:
            lines_ts = [("client", t, int(lm.get("addTimestamp") or 0))]
    texts_for_title = [t for _, t, __ in lines_ts]
    product_title = product_title_from_wb_chat(gc, texts_for_title)
    excerpt_parts = []
    for role, text, _ in lines_ts:
        label = "Покупатель" if role == "client" else "Продавец" if role == "seller" else role
        excerpt_parts.append(f"{label}: {text}")
    conversation = "\n".join(excerpt_parts) if excerpt_parts else "(сообщений пока нет)"
    try:
        draft = await generate_wb_buyer_chat_reply(
            db,
            key,
            product_title=product_title,
            conversation_excerpt=conversation,
        )
    except json.JSONDecodeError:
        raise HTTPException(502, "Модель вернула не JSON — попробуйте сгенерировать ещё раз")
    except ValueError as e:
        raise HTTPException(502, str(e))
    except Exception as e:
        log.exception("wb buyer chat generate: %s", e)
        raise HTTPException(502, f"Ошибка генерации: {e}") from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="wb_buyer_chat_generate",
            item_type="wb_chat",
            store_id=store_id,
            result="ok",
            meta={"chat_id": chat_id, "product_title": product_title[:200]},
        )
    except Exception:
        pass
    return {"draft": draft, "product_title": product_title}


@app.post("/api/wb/buyer-chats/{store_id}/send")
async def api_wb_buyer_chat_send(
    store_id: int,
    body: WbBuyerChatSendBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    client = WbBuyerChatClient(s.api_key)
    msg = (body.message or "").strip()
    if not msg:
        raise HTTPException(400, "Текст сообщения пустой")
    rs = (body.reply_sign or "").strip()
    if not rs:
        raise HTTPException(400, "reply_sign пустой — обновите чат (кнопка «Загрузить переписку»)")
    try:
        out = await client.send_message(rs, msg)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    _wb_chat_list_cache.pop(int(store_id), None)
    try:
        db.add_audit_event(
            actor=user.username,
            action="wb_buyer_chat_send",
            item_type="wb_chat",
            store_id=store_id,
            result="ok",
            meta={"len": len(msg)},
        )
    except Exception:
        pass
    return {"ok": True, "result": out}


# ---------- API: stats ----------
@app.get("/api/stats")
def api_stats(db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    return db.get_stats()


# ---------- API: log ----------
@app.get("/api/log")
def api_log_tail(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_log"))):
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            f.seek(max(0, f.seek(0, 2) - 100 * 1024))
            return {"text": f.read()}
    except FileNotFoundError:
        return {"text": ""}
    except Exception as e:
        log.warning("log tail failed: %s", e)
        return {"text": str(e)}


@app.on_event("startup")
async def _startup_scheduler():
    global _scheduler_task
    try:
        db = get_db()
        _bootstrap_admin_if_needed(db)
    except Exception:
        log.exception("startup bootstrap failed")
    if _scheduler_task is None or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(_auto_scheduler_loop())
        log.info("Auto-scheduler started (MSK)")


@app.on_event("shutdown")
async def _shutdown_scheduler():
    global _scheduler_task
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except Exception:
            pass
    _scheduler_task = None


# ---------- Static SPA & PWA ----------
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/login")
    def login_page(request: Request):
        db = get_db()
        _bootstrap_admin_if_needed(db)
        if _get_current_user(request, db):
            return RedirectResponse(url="/app")
        return FileResponse(STATIC_DIR / "login.html")

    @app.get("/reset")
    def reset_page():
        return FileResponse(STATIC_DIR / "reset.html")

    @app.get("/")
    def landing_page():
        return RedirectResponse(url="/login")

    @app.get("/app")
    def index(request: Request):
        db = get_db()
        _bootstrap_admin_if_needed(db)
        if not _get_current_user(request, db):
            return RedirectResponse(url="/login")
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/sw.js")
    def sw():
        return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript")
else:
    @app.get("/")
    def index():
        return {"message": "Static files not found. Create app/web/static/ with index.html"}
