"""
FastAPI-сервер веб-интерфейса WB Автоответчик.
Запуск: uvicorn app.web.server:app --reload
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Инициализация путей и логов до импорта app.db (который может использовать логи)
APP_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = str(APP_DIR / "data" / "reviews.db")
LOG_PATH = str(APP_DIR / "logs" / "app.log")
STATIC_DIR = Path(__file__).resolve().parent / "static"

from app.logging_config import setup_logging
setup_logging(LOG_PATH)

from app.db import Database, Store, ItemRow, PromptRow, UserRow
from app.web import tasks as web_tasks

log = logging.getLogger("web")

app = FastAPI(title="WB Автоответчик", version="1.0")

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
    log.warning("Создан пользователь admin (bootstrap). СМЕНИТЕ пароль после входа.")


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


class UserOut(BaseModel):
    id: int
    username: str
    role: str


class UserCreateBody(BaseModel):
    username: str
    password: str
    role: str = "guest"


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
def api_me(user: UserRow = Depends(require_user)):
    return MeOut(username=user.username, role=user.role)


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


# ---------- API: users (admin) ----------
@app.get("/api/users", response_model=list[UserOut])
def api_list_users(_: UserRow = Depends(require_admin), db: Database = Depends(get_db)):
    return [UserOut(id=u.id, username=u.username, role=u.role) for u in db.list_users()]


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
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    if store_id is not None:
        rows = db.list_items_for_ui(store_id, item_type)
    else:
        rows = db.list_items_for_ui_all(item_type)
    return [_item_to_out(r) for r in rows]


@app.get("/api/items/{item_id}", response_model=ItemOut)
def api_get_item(item_id: int, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    row = db.get_item_by_id(item_id)
    if not row:
        raise HTTPException(404, "Элемент не найден")
    return _item_to_out(row)


# ---------- API: settings ----------
@app.get("/api/settings")
def api_get_settings(db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    keys = ["openai_key", "telegram_bot_token", "telegram_chat_id", "theme"]
    return {k: db.get_setting(k) or "" for k in keys}


@app.post("/api/settings")
def api_set_settings(body: dict[str, str], db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    for k, v in body.items():
        db.set_setting(k, v or "")
    return {"ok": True}


# ---------- API: prompts ----------
@app.get("/api/prompts", response_model=list[PromptOut])
def api_list_prompts(db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    return [PromptOut(id=p.id, item_type=p.item_type, rating_group=p.rating_group, prompt_text=p.prompt_text) for p in db.list_prompts()]


@app.patch("/api/prompts/{prompt_id}")
def api_update_prompt(prompt_id: int, body: PromptUpdate, db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
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


@app.get("/api/tasks/{task_id}")
async def api_get_task(task_id: str, _: UserRow = Depends(require_user)):
    state = await web_tasks.get_task(task_id)
    if state is None:
        raise HTTPException(404, "Задача не найдена")
    return state


# ---------- API: stats ----------
@app.get("/api/stats")
def api_stats(db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    return db.get_stats()


# ---------- API: log ----------
@app.get("/api/log")
def api_log_tail(db: Database = Depends(get_db), _: UserRow = Depends(require_admin)):
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            f.seek(max(0, f.seek(0, 2) - 100 * 1024))
            return {"text": f.read()}
    except FileNotFoundError:
        return {"text": ""}
    except Exception as e:
        log.warning("log tail failed: %s", e)
        return {"text": str(e)}


# ---------- Static SPA & PWA ----------
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/sw.js")
    def sw():
        return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript")
else:
    @app.get("/")
    def index():
        return {"message": "Static files not found. Create app/web/static/ with index.html"}
