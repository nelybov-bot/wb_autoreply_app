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
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Инициализация путей и логов до импорта app.db (который может использовать логи)
APP_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = str(APP_DIR / "data" / "reviews.db")
LOG_PATH = str(APP_DIR / "logs" / "app.log")
STATIC_DIR = Path(__file__).resolve().parent / "static"

from app.logging_config import setup_logging
setup_logging(LOG_PATH)

from app.db import (
    Database,
    Store,
    ItemRow,
    PromptRow,
    UserRow,
    AuditEventRow,
    CardErrorAlertRow,
    OzonImportantAlertRow,
)
from app.web import tasks as web_tasks
from app.web.store_locks import StoreBusyError, store_locks
from app.agent.orchestrator import handle_agent_message
from app.agent.session import clear_session, get_or_create_session, get_session_if_owner, session_public_view
from app.agent.tools import AgentContext
from app.agent.telegram_bot import configure_telegram_agent, start_telegram_agent_task, stop_telegram_agent_task
from app.core.net import HttpStatusError, OzonApiAccessError
from app.core.wb_buyer_chat import (
    WbBuyerChatClient,
    build_wb_thread_lines,
    collect_thread_lines,
    fetch_events_for_chat,
    merge_good_card,
    last_client_message_info as wb_last_client_info,
    product_title_from_wb_chat,
    wb_chat_error_message,
)
from app.core.ozon_buyer_chat import (
    collect_ozon_thread_lines,
    format_ozon_datetime_msk,
    is_ozon_buyer_chat_row,
    ozon_chat_category,
    ozon_chat_error_message,
    ozon_chat_matches_filter,
    ozon_chat_row_id,
    ozon_chat_type,
    ozon_feature_unavailable_user_message,
    last_client_message_info as ozon_last_client_info,
    ozon_http_skip_reason,
    ozon_reply_window_hint,
    product_title_from_ozon_chat,
)
from app.core.ozon_actions import (
    auto_remove_from_ozon_auto_actions,
    normalize_action_row,
    pick_actions_for_removal,
    remove_products_from_actions,
)
from app.core.ozon_client import OzonClient
from app.core.card_links import (
    apply_link_status,
    build_wb_catalog_payload,
    build_ozon_catalog_payload,
    fetch_ozon_catalog,
    fetch_wb_catalog,
    group_ozon_rows,
    group_wb_rows,
    ozon_link_by_model,
    ozon_unlink_cards,
    link_ozon_tms_qty_groups,
    parse_ozon_tms_qty_table,
    parse_articles_csv,
    group_attach_suggestions,
    suggest_attach_to_groups,
    suggest_combine_candidates,
    suggest_link_candidates,
    suggest_review_linked_groups,
    sort_catalog_rows,
    wb_disconnect_cards,
    wb_merge_cards,
    wb_merge_error_message,
    wb_content_api_error_message,
    default_ai_system_prompt,
    get_card_links_ai_prompt_stored,
    set_card_links_ai_prompt_stored,
    resolve_ai_system_prompt,
)
from app.core.chat_common import SETTING_AUTO_CHAT_MAX_AGE_DAYS, SETTING_REPLY_FROM, parse_api_error_detail
from app.core.secret_mask import (
    mask_settings_for_api,
    redact_secrets_in_text,
    resolve_secret_setting,
    sanitize_for_audit,
    SECRET_SETTING_KEYS,
)
from app.core.card_check import (
    DEFAULT_TELEGRAM_TEMPLATE,
    SETTING_CARD_CHECK_ENABLED,
    SETTING_CARD_CHECK_IN_REPORT,
    SETTING_CARD_CHECK_TELEGRAM,
    SETTING_CARD_CHECK_TEMPLATE,
    is_legacy_card_telegram_template,
)
from app.core.telegram_notify import (
    normalize_telegram_bot_token,
    resolve_telegram_chat_id,
    send_activity_report,
    test_telegram_delivery,
)
from app.core.ozon_alerts import (
    DEFAULT_TELEGRAM_TEMPLATE as DEFAULT_OZON_ALERT_TELEGRAM_TEMPLATE,
    SETTING_ENABLED as OZON_ALERTS_ENABLED,
    SETTING_FROM_DATE as OZON_ALERTS_FROM_DATE,
    SETTING_TELEGRAM as OZON_ALERTS_TELEGRAM,
    SETTING_TEMPLATE as OZON_ALERTS_TEMPLATE,
    is_legacy_telegram_template,
)
from app.core.config_backup import export_config, import_config
from app.core.quality_metrics import fetch_all_quality
from app.core.workflows import (
    auto_process_ozon_buyer_chats,
    auto_process_ozon_important_alerts,
    auto_process_wb_buyer_chats,
    scan_ozon_important_alerts_for_store,
    generate_mass,
    generate_ozon_buyer_chat_reply,
    generate_wb_buyer_chat_reply,
    load_new_all,
    load_new_items,
    ozon_buyer_chats_mass_generate_send_for_store,
    ozon_actions_auto_remove_for_store,
    send_mass_all,
    wb_buyer_chats_mass_generate_send_for_store,
    _buyer_chat_reply_from,
    _ozon_chat_eligibility,
    _wb_chat_eligibility,
)

log = logging.getLogger("web")

app = FastAPI(title="MarketAI", version="1.5")
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
    log.info("Создан пользователь admin (первый запуск). Смените пароль в настройках.")


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
    api_key_set: bool = False
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
    store_ids: Optional[list[int]] = None  # null = все магазины по очереди в одной задаче


class GenerateBody(BaseModel):
    item_ids: list[int]


class SendBody(BaseModel):
    item_ids: list[int]


class AgentChatBody(BaseModel):
    message: str = ""
    session_id: Optional[str] = None
    confirm: Optional[bool] = None

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
    chat_id: str = ""
    client_message_key: str = ""


class OzonBuyerChatGenerateBody(BaseModel):
    chat_id: str


class OzonBuyerChatSendBody(BaseModel):
    chat_id: str
    message: str
    client_message_key: str = ""


class OzonBuyerChatMassBody(BaseModel):
    max_chats: int = 50


class WbBuyerChatMassBody(BaseModel):
    """Ручная массовая обработка: только чаты, где последнее сообщение от покупателя."""
    max_chats: int = 50
    event_pages: int = 6


class AutoScheduleBody(BaseModel):
    enabled: bool = False
    slots: list[str] = []   # ["09:00","13:30"]
    store_ids: list[int] = []  # обязательный выбор магазинов
    schedule_mode: str = "slots"  # slots | interval
    interval_hours: int = 1
    run_reviews_wb: bool = True
    run_reviews_yam: bool = True
    run_reviews_ozon: bool = False
    run_questions_wb: bool = True
    run_questions_yam: bool = True
    run_questions_ozon: bool = True
    run_wb_chats: bool = False
    run_ozon_chats: bool = False
    run_ozon_alerts: bool = False
    run_ozon_actions_remove: bool = False


class OzonActionsSettingsBody(BaseModel):
    auto_remove_on_schedule: bool = False
    only_auto_add: bool = True
    watched_action_ids: list[int] = []


class OzonAlertsScanBody(BaseModel):
    rescan: bool = False


class OzonActionsRemoveBody(BaseModel):
    action_ids: list[int] = []
    only_auto_add: bool = False


OZON_ACTIONS_SETTINGS_KEY = "ozon_actions_settings_json"
AUTO_SCHEDULE_KEY = "auto_schedule_json"
AUTO_LAST_RUN_KEY = "auto_schedule_last_run_at"
TELEGRAM_REPORT_ENABLED = "telegram_report_enabled"
TELEGRAM_REPORT_INTERVAL = "telegram_report_interval"
TELEGRAM_REPORT_LAST_SENT = "telegram_report_last_sent"
_WB_CHAT_LIST_TTL_S = 300.0
_OZON_CHAT_LIST_TTL_S = 55.0
_wb_chat_list_cache: dict[int, tuple[float, list]] = {}
_ozon_chat_list_cache: dict[int, tuple[float, list, Optional[str]]] = {}
_scheduler_task: Optional[asyncio.Task] = None
_telegram_report_task: Optional[asyncio.Task] = None
_tg_report_fail_until: float = 0.0
_tg_report_fail_token: str = ""
_scheduler_seen: set[str] = set()
_auto_run_task: Optional[asyncio.Task] = None
_interval_skip_logged: bool = False
_auto_state: dict = {
    "running": False,
    "slot": "",
    "phase": "idle",
    "current_store_id": None,
    "store_index": 0,
    "store_count": 0,
    "last_started_at": "",
    "last_finished_at": "",
    "last_error": "",
}

def _get_ozon_actions_settings(db: Database) -> dict:
    raw = (db.get_setting(OZON_ACTIONS_SETTINGS_KEY) or "").strip()
    cfg: dict = {"auto_remove_on_schedule": False, "only_auto_add": True, "stores": {}}
    if not raw:
        return cfg
    try:
        obj = json.loads(raw)
        cfg["auto_remove_on_schedule"] = bool(obj.get("auto_remove_on_schedule"))
        cfg["only_auto_add"] = bool(obj.get("only_auto_add", True))
        stores = obj.get("stores") or {}
        cfg["stores"] = stores if isinstance(stores, dict) else {}
    except Exception:
        pass
    return cfg


def _store_watched_action_ids(cfg: dict, store_id: int) -> list[int]:
    stores = cfg.get("stores") or {}
    if not isinstance(stores, dict):
        return []
    ent = stores.get(str(int(store_id))) or {}
    out: list[int] = []
    for x in ent.get("watched_action_ids") or []:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


_AUTO_MP_REVIEW_KEYS = ("run_reviews_wb", "run_reviews_yam", "run_reviews_ozon")
_AUTO_MP_QUESTION_KEYS = ("run_questions_wb", "run_questions_yam", "run_questions_ozon")


def _auto_mp_flags_from_obj(obj: dict) -> dict[str, bool]:
    """Флаги отзывов/вопросов по маркетплейсам; миграция со старых run_reviews/run_questions."""
    has_new = any(k in obj for k in _AUTO_MP_REVIEW_KEYS)
    if not has_new:
        legacy_r = bool(obj.get("run_reviews", True))
        legacy_q = bool(obj.get("run_questions", True))
        ozon_r = bool(obj.get("run_ozon_reviews", False)) if "run_ozon_reviews" in obj else False
        return {
            "run_reviews_wb": legacy_r,
            "run_reviews_yam": legacy_r,
            "run_reviews_ozon": ozon_r,
            "run_questions_wb": legacy_q,
            "run_questions_yam": legacy_q,
            "run_questions_ozon": legacy_q,
        }
    return {
        "run_reviews_wb": bool(obj.get("run_reviews_wb", True)),
        "run_reviews_yam": bool(obj.get("run_reviews_yam", True)),
        "run_reviews_ozon": bool(obj.get("run_reviews_ozon", False)),
        "run_questions_wb": bool(obj.get("run_questions_wb", True)),
        "run_questions_yam": bool(obj.get("run_questions_yam", True)),
        "run_questions_ozon": bool(obj.get("run_questions_ozon", True)),
    }


def _any_auto_reviews(cfg: dict) -> bool:
    return any(bool(cfg.get(k)) for k in _AUTO_MP_REVIEW_KEYS)


def _any_auto_questions(cfg: dict) -> bool:
    return any(bool(cfg.get(k)) for k in _AUTO_MP_QUESTION_KEYS)


def _item_types_for_store(store: Store, cfg: dict) -> list[str]:
    mp = (store.marketplace or "").strip().lower()
    types: list[str] = []
    if mp == "wb":
        if cfg.get("run_reviews_wb"):
            types.append("review")
        if cfg.get("run_questions_wb"):
            types.append("question")
    elif mp == "yam":
        if cfg.get("run_reviews_yam"):
            types.append("review")
        if cfg.get("run_questions_yam"):
            types.append("question")
    elif mp == "ozon":
        if cfg.get("run_reviews_ozon"):
            types.append("review")
        if cfg.get("run_questions_ozon"):
            types.append("question")
    return types


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
        "run_reviews_wb": True,
        "run_reviews_yam": True,
        "run_reviews_ozon": False,
        "run_questions_wb": True,
        "run_questions_yam": True,
        "run_questions_ozon": True,
        "run_wb_chats": False,
        "run_ozon_chats": False,
        "run_ozon_alerts": False,
        "run_ozon_actions_remove": False,
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
        cfg.update(_auto_mp_flags_from_obj(obj))
        cfg["run_wb_chats"] = bool(obj.get("run_wb_chats", False))
        cfg["run_ozon_chats"] = bool(obj.get("run_ozon_chats", False))
        cfg["run_ozon_alerts"] = bool(obj.get("run_ozon_alerts", False))
        cfg["run_ozon_actions_remove"] = bool(obj.get("run_ozon_actions_remove", False))
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
        "run_reviews_wb": bool(body.run_reviews_wb),
        "run_reviews_yam": bool(body.run_reviews_yam),
        "run_reviews_ozon": bool(body.run_reviews_ozon),
        "run_questions_wb": bool(body.run_questions_wb),
        "run_questions_yam": bool(body.run_questions_yam),
        "run_questions_ozon": bool(body.run_questions_ozon),
        "run_wb_chats": bool(body.run_wb_chats),
        "run_ozon_chats": bool(body.run_ozon_chats),
        "run_ozon_alerts": bool(body.run_ozon_alerts),
        "run_ozon_actions_remove": bool(body.run_ozon_actions_remove),
    }
    if not (
        _any_auto_reviews(cfg)
        or _any_auto_questions(cfg)
        or cfg["run_wb_chats"]
        or cfg["run_ozon_chats"]
        or cfg["run_ozon_alerts"]
        or cfg["run_ozon_actions_remove"]
    ):
        raise HTTPException(
            400,
            "Нужно включить хотя бы одну задачу: отзывы/вопросы по WB, ЯМ или Ozon, чаты, уведомления или автоудаление из акций",
        )
    db.set_setting(AUTO_SCHEDULE_KEY, json.dumps(cfg, ensure_ascii=False))
    return cfg


def _disable_auto_schedule(db: Database) -> dict:
    """Выключить автозапуск в настройках (расписание и задачи сохраняются)."""
    cfg = _get_auto_schedule(db)
    cfg["enabled"] = False
    db.set_setting(AUTO_SCHEDULE_KEY, json.dumps(cfg, ensure_ascii=False))
    return cfg


async def _cancel_auto_run_if_busy() -> bool:
    global _auto_run_task, _interval_skip_logged
    if _auto_run_task is None or _auto_run_task.done():
        return False
    _auto_run_task.cancel()
    try:
        await _auto_run_task
    except Exception:
        pass
    _auto_run_task = None
    _interval_skip_logged = False
    _auto_state["running"] = False
    _auto_state["phase"] = "cancelled"
    _auto_state["last_finished_at"] = dt.datetime.now(MSK_TZ).isoformat(timespec="seconds")
    return True


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

async def _process_auto_store(
    db: Database,
    store: Store,
    *,
    item_types: list[str],
    run_wb_chats: bool,
    run_ozon_chats: bool,
    run_ozon_alerts: bool,
    run_ozon_actions_remove: bool,
    openai_key: str,
    ozon_actions_cfg: dict,
) -> dict:
    """Полный цикл автозапуска для одного магазина."""
    key = (openai_key or "").strip()
    result: dict = {
        "store_id": store.id,
        "marketplace": store.marketplace,
        "deleted_before_load": 0,
        "added": 0,
        "candidates": 0,
        "gen_ok": 0,
        "gen_failed": 0,
        "card_errors": 0,
        "sent_ok": 0,
        "sent_skipped": 0,
        "sent_failed": 0,
    }
    reviews_phase_error = ""
    if item_types:
        try:
            _auto_state["phase"] = "load_new"
            # Не очищаем БД до загрузки: при ошибке API (403 Ozon и т.д.) локальные записи сохраняются.
            load_reviews = "review" in item_types
            load_questions = "question" in item_types
            result["added"] = await load_new_items(
                db,
                store,
                load_reviews=load_reviews,
                load_questions=load_questions,
            )
            _auto_state["phase"] = "generate"
            item_ids = _collect_pending_item_ids(db, [store.id], item_types=item_types)
            result["candidates"] = len(item_ids)
            if item_ids and key:
                gen_ok, gen_failed, card_errors = await generate_mass(db, item_ids, key, model="gpt-5.2")
                result["gen_ok"] = gen_ok
                result["gen_failed"] = gen_failed
                result["card_errors"] = card_errors
                _auto_state["phase"] = "send"
                sent_ok, sent_skipped, sent_failed = await send_mass_all(db, item_ids)
                result["sent_ok"] = sent_ok
                result["sent_skipped"] = sent_skipped
                result["sent_failed"] = sent_failed
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except OzonApiAccessError as e:
            reviews_phase_error = str(e)[:800]
            log.warning(
                "auto_run store_id=%s: отзывы/вопросы пропущены (%s); чаты/акции продолжаются",
                store.id,
                reviews_phase_error[:200],
            )
        except Exception as e:
            reviews_phase_error = str(e)[:800]
            log.exception(
                "auto_run store_id=%s: этап отзывы/вопросы прерван; чаты/акции (если включены) выполняются отдельно",
                store.id,
            )
    if reviews_phase_error:
        result["reviews_phase_error"] = reviews_phase_error

    if run_wb_chats:
        _auto_state["phase"] = "wb_chats"
        if key:
            if store.marketplace == "wb" and (store.api_key or "").strip():
                result["wb_chats"] = await auto_process_wb_buyer_chats(db, [store], openai_key=key)
            else:
                result["wb_chats"] = {
                    "wb_chat_skipped": 1,
                    "reason": "not_wb_or_no_key",
                }
        else:
            result["wb_chats"] = {"wb_chat_skipped": 1, "reason": "no_openai_key"}

    if run_ozon_chats:
        _auto_state["phase"] = "ozon_chats"
        if key:
            if (
                store.marketplace == "ozon"
                and (store.client_id or "").strip()
                and (store.api_key or "").strip()
            ):
                result["ozon_chats"] = await auto_process_ozon_buyer_chats(db, [store], openai_key=key)
            else:
                result["ozon_chats"] = {
                    "ozon_chat_skipped": 1,
                    "reason": "not_ozon_or_no_keys",
                }
        else:
            result["ozon_chats"] = {"ozon_chat_skipped": 1, "reason": "no_openai_key"}

    if run_ozon_alerts:
        _auto_state["phase"] = "ozon_alerts"
        if key:
            if (
                store.marketplace == "ozon"
                and (store.client_id or "").strip()
                and (store.api_key or "").strip()
            ):
                result["ozon_alerts"] = await auto_process_ozon_important_alerts(
                    db, [store], openai_key=key,
                )
            else:
                result["ozon_alerts"] = {
                    "ozon_alert_skipped": 1,
                    "reason": "not_ozon_or_no_keys",
                }
        else:
            result["ozon_alerts"] = {"ozon_alert_skipped": 1, "reason": "no_openai_key"}

    if run_ozon_actions_remove:
        _auto_state["phase"] = "ozon_actions"
        if (
            store.marketplace == "ozon"
            and (store.client_id or "").strip()
            and (store.api_key or "").strip()
        ):
            watched = _store_watched_action_ids(ozon_actions_cfg, store.id)
            result["ozon_actions"] = await ozon_actions_auto_remove_for_store(
                store,
                only_auto_add=bool(ozon_actions_cfg.get("only_auto_add", True)),
                action_ids=watched if watched else None,
            )
            oa = result["ozon_actions"] if isinstance(result.get("ozon_actions"), dict) else {}
            log.info(
                "auto_run store_id=%s ozon_actions: matched=%s processed=%s removed=%s skipped=%s reason=%s",
                store.id,
                oa.get("actions_matched"),
                oa.get("actions_processed"),
                oa.get("products_removed"),
                oa.get("skipped"),
                oa.get("reason") or "",
            )
            try:
                db.add_audit_event(
                    actor="system",
                    action="ozon_actions_auto_remove",
                    item_type="ozon_action",
                    store_id=store.id,
                    result="skipped" if oa.get("skipped") else "ok",
                    meta=oa,
                )
            except Exception:
                pass
        else:
            result["ozon_actions"] = {
                "skipped": 1,
                "reason": "not_ozon_or_no_keys",
            }

    summary_parts: list[str] = []
    if item_types:
        summary_parts.append(
            f"Отзывы/вопросы: +{result.get('added', 0)}, "
            f"к ответу {result.get('candidates', 0)}, "
            f"сгенерировано {result.get('gen_ok', 0)}, "
            f"отправлено {result.get('sent_ok', 0)}"
        )
        if result.get("card_errors"):
            summary_parts.append(f"ошибок карточек {result['card_errors']}")
        if result.get("sent_failed"):
            summary_parts.append(f"ошибок отправки {result['sent_failed']}")
    wb = result.get("wb_chats") if isinstance(result.get("wb_chats"), dict) else {}
    if wb:
        if wb.get("reason"):
            summary_parts.append(f"Чаты WB: пропуск ({wb.get('reason')})")
        elif wb.get("wb_chat_sent") or wb.get("wb_chat_candidates"):
            summary_parts.append(
                f"Чаты WB: отправлено {int(wb.get('wb_chat_sent') or 0)} "
                f"(кандидатов {int(wb.get('wb_chat_candidates') or 0)})"
            )
    oz = result.get("ozon_chats") if isinstance(result.get("ozon_chats"), dict) else {}
    if oz:
        if oz.get("reason"):
            summary_parts.append(f"Чаты Ozon: пропуск ({oz.get('reason')})")
        elif oz.get("ozon_chat_skip_reason") or oz.get("message"):
            summary_parts.append(
                f"Чаты Ozon: пропуск ({oz.get('ozon_chat_skip_reason') or oz.get('message')})"
            )
        elif oz.get("ozon_chat_sent") or oz.get("ozon_chat_candidates"):
            summary_parts.append(
                f"Чаты Ozon: отправлено {int(oz.get('ozon_chat_sent') or 0)} "
                f"(кандидатов {int(oz.get('ozon_chat_candidates') or 0)})"
            )
    oz_al = result.get("ozon_alerts") if isinstance(result.get("ozon_alerts"), dict) else {}
    if oz_al:
        if oz_al.get("ozon_alert_skip_reason") == "disabled":
            pass
        elif int(oz_al.get("ozon_alert_new") or 0):
            summary_parts.append(
                f"Уведомления Ozon: важных {int(oz_al.get('ozon_alert_new') or 0)} "
                f"(чатов {int(oz_al.get('ozon_alert_chats_scanned') or 0)})"
            )
        elif oz_al.get("ozon_alert_chats_scanned"):
            summary_parts.append(
                f"Уведомления Ozon: проверено чатов {int(oz_al.get('ozon_alert_chats_scanned') or 0)}, новых нет"
            )
    oa = result.get("ozon_actions") if isinstance(result.get("ozon_actions"), dict) else {}
    if oa:
        if oa.get("skipped"):
            summary_parts.append(f"Акции Ozon: пропуск ({oa.get('reason') or oa.get('message') or '—'})")
        else:
            summary_parts.append(
                f"Акции Ozon: удалено {int(oa.get('products_removed') or 0)} товаров "
                f"из {int(oa.get('actions_processed') or 0)} акций"
            )
    if result.get("reviews_phase_error"):
        summary_parts.append(f"ошибка отзывов: {result['reviews_phase_error'][:200]}")
    try:
        db.add_audit_event(
            actor="system",
            action="store_auto",
            item_type="activity",
            store_id=store.id,
            result="partial" if result.get("reviews_phase_error") else "ok",
            meta={
                "store_name": store.name,
                "marketplace": store.marketplace,
                "summary": " · ".join(summary_parts) if summary_parts else "без действий",
                "added": result.get("added"),
                "candidates": result.get("candidates"),
                "gen_ok": result.get("gen_ok"),
                "card_errors": result.get("card_errors"),
                "sent_ok": result.get("sent_ok"),
                "wb_chats": wb or None,
                "ozon_chats": oz or None,
                "ozon_alerts": oz_al or None,
                "ozon_actions": oa or None,
            },
        )
    except Exception:
        pass
    return result


def _sum_store_results(stores_results: list[dict], key: str) -> int:
    return sum(int(r.get(key) or 0) for r in stores_results)


def _aggregate_ozon_actions_stats(stores_results: list[dict]) -> dict:
    out = {
        "actions_matched": 0,
        "actions_processed": 0,
        "products_removed": 0,
        "products_rejected": 0,
        "stores_skipped": 0,
        "stores_with_removals": 0,
    }
    for row in stores_results:
        oa = row.get("ozon_actions")
        if not isinstance(oa, dict):
            continue
        if oa.get("skipped"):
            out["stores_skipped"] += 1
            continue
        out["actions_matched"] += int(oa.get("actions_matched") or 0)
        out["actions_processed"] += int(oa.get("actions_processed") or 0)
        out["products_removed"] += int(oa.get("products_removed") or 0)
        out["products_rejected"] += int(oa.get("products_rejected") or 0)
        if int(oa.get("products_removed") or 0) > 0:
            out["stores_with_removals"] += 1
    return out


async def _run_auto_slot(slot: str, *, force: bool = False) -> None:
    global _auto_state
    db = get_db()
    cfg = _get_auto_schedule(db)
    if not force and not cfg.get("enabled"):
        return
    store_ids = [int(x) for x in (cfg.get("store_ids") or [])]
    stores = [s for s in db.list_stores() if s.active and s.id in store_ids]
    if not stores:
        log.warning(
            "auto_run slot=%s: пропуск — нет активных магазинов для store_ids=%s",
            slot,
            store_ids,
        )
        return
    sorted_stores = sorted(stores, key=lambda s: s.id)
    n_stores = len(sorted_stores)
    started_dt = dt.datetime.now(MSK_TZ)
    started = started_dt.isoformat(timespec="seconds")
    _auto_state.update({
        "running": True,
        "slot": slot,
        "phase": "load_new",
        "current_store_id": sorted_stores[0].id,
        "store_index": 0,
        "store_count": n_stores,
        "last_started_at": started,
        "last_error": "",
    })
    try:
        run_wb_chats = bool(cfg.get("run_wb_chats", False))
        run_ozon_chats = bool(cfg.get("run_ozon_chats", False))
        run_ozon_alerts = bool(cfg.get("run_ozon_alerts", False))
        run_ozon_actions_remove = bool(cfg.get("run_ozon_actions_remove", False))
        key = (db.get_setting("openai_key") or "").strip()
        ozon_actions_cfg = _get_ozon_actions_settings(db)
        log.info(
            "auto_run start slot=%s stores=%s reviews_wb=%s reviews_yam=%s reviews_ozon=%s "
            "questions_wb=%s questions_yam=%s questions_ozon=%s wb_chats=%s ozon_chats=%s ozon_alerts=%s ozon_actions=%s",
            slot,
            [s.id for s in sorted_stores],
            cfg.get("run_reviews_wb"),
            cfg.get("run_reviews_yam"),
            cfg.get("run_reviews_ozon"),
            cfg.get("run_questions_wb"),
            cfg.get("run_questions_yam"),
            cfg.get("run_questions_ozon"),
            run_wb_chats,
            run_ozon_chats,
            run_ozon_alerts,
            run_ozon_actions_remove,
        )
        stores_results: list[dict] = []
        auto_owner = f"auto:{slot}:{started}"
        store_names = {s.id: s.name for s in sorted_stores}
        for idx, slot_store in enumerate(sorted_stores):
            _auto_state["store_index"] = idx + 1
            _auto_state["current_store_id"] = slot_store.id
            log.info(
                "auto_run store %s/%s id=%s marketplace=%s",
                idx + 1,
                n_stores,
                slot_store.id,
                slot_store.marketplace,
            )
            try:
                await store_locks.acquire(
                    [slot_store.id], "auto_run", auto_owner, store_names=store_names,
                )
            except StoreBusyError as e:
                log.warning("auto_run skip store_id=%s: %s", slot_store.id, e)
                stores_results.append({
                    "store_id": slot_store.id,
                    "skipped": True,
                    "reason": str(e),
                })
                continue
            try:
                store_item_types = _item_types_for_store(slot_store, cfg)
                store_meta = await _process_auto_store(
                    db,
                    slot_store,
                    item_types=store_item_types,
                    run_wb_chats=run_wb_chats,
                    run_ozon_chats=run_ozon_chats,
                    run_ozon_alerts=run_ozon_alerts,
                    run_ozon_actions_remove=run_ozon_actions_remove,
                    openai_key=key,
                    ozon_actions_cfg=ozon_actions_cfg,
                )
                stores_results.append(store_meta)
            finally:
                await store_locks.release([slot_store.id], auto_owner)
        ozon_actions_totals = _aggregate_ozon_actions_stats(stores_results)
        meta_run = {
            "slot": slot,
            "store_ids": [s.id for s in sorted_stores],
            "stores_processed": n_stores,
            "stores_results": stores_results,
            "ozon_actions_totals": ozon_actions_totals,
            "auto_mp_flags": {k: bool(cfg.get(k)) for k in _AUTO_MP_REVIEW_KEYS + _AUTO_MP_QUESTION_KEYS},
            "deleted_before_load": _sum_store_results(stores_results, "deleted_before_load"),
            "added": _sum_store_results(stores_results, "added"),
            "candidates": _sum_store_results(stores_results, "candidates"),
            "gen_ok": _sum_store_results(stores_results, "gen_ok"),
            "gen_failed": _sum_store_results(stores_results, "gen_failed"),
            "card_errors": _sum_store_results(stores_results, "card_errors"),
            "sent_ok": _sum_store_results(stores_results, "sent_ok"),
            "sent_skipped": _sum_store_results(stores_results, "sent_skipped"),
            "sent_failed": _sum_store_results(stores_results, "sent_failed"),
            "run_wb_chats": run_wb_chats,
            "run_ozon_chats": run_ozon_chats,
            "run_ozon_alerts": run_ozon_alerts,
            "run_ozon_actions_remove": run_ozon_actions_remove,
            "wb_chat_sent": sum(
                int((r.get("wb_chats") or {}).get("wb_chat_sent") or 0) for r in stores_results
            ),
            "ozon_chat_sent": sum(
                int((r.get("ozon_chats") or {}).get("ozon_chat_sent") or 0) for r in stores_results
            ),
            "ozon_alert_new": sum(
                int((r.get("ozon_alerts") or {}).get("ozon_alert_new") or 0) for r in stores_results
            ),
        }
        phase_errors = [
            f"{r.get('store_id')}: {r['reviews_phase_error'][:200]}"
            for r in stores_results
            if r.get("reviews_phase_error")
        ]
        if phase_errors:
            meta_run["reviews_phase_errors"] = phase_errors
        run_result = "partial" if phase_errors else "ok"
        db.add_audit_event(
            actor="system",
            action="auto_run",
            item_type="mixed",
            result=run_result,
            meta=meta_run,
        )
        _auto_state["phase"] = "done"
        _auto_state["current_store_id"] = None
        _auto_state["store_index"] = 0
        finished_dt = dt.datetime.now(MSK_TZ)
        try:
            db.set_setting(AUTO_LAST_RUN_KEY, finished_dt.isoformat(timespec="seconds"))
        except Exception:
            pass
    except asyncio.CancelledError:
        _auto_state["phase"] = "cancelled"
        raise
    except Exception as e:
        _auto_state["phase"] = "error"
        _auto_state["last_error"] = str(e)
        raise
    finally:
        _auto_state["running"] = False
        _auto_state["last_finished_at"] = dt.datetime.now(MSK_TZ).isoformat(timespec="seconds")

def _telegram_report_period_seconds(interval: str) -> int:
    return 86400 if (interval or "").strip() == "day" else 3600


def _format_report_period_label(since_dt: dt.datetime, until_dt: dt.datetime) -> str:
    fmt = "%d.%m.%Y %H:%M"
    return f"{since_dt.strftime(fmt)} — {until_dt.strftime(fmt)} (МСК)"


async def _send_telegram_report(
    db: Database,
    *,
    since_dt: dt.datetime,
    until_dt: dt.datetime,
    interval: str,
    manual: bool = False,
) -> dict:
    token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
    chat_id = resolve_telegram_chat_id(db, "report")
    if not token:
        raise HTTPException(400, "Укажите токен бота в настройках Telegram")
    if not chat_id:
        raise HTTPException(
            400,
            "Укажите ID чата для отчётов или основной ID чата в настройках Telegram",
        )
    since_iso = since_dt.isoformat(timespec="seconds")
    until_iso = until_dt.isoformat(timespec="seconds")
    stats = db.get_activity_stats_since(since_iso, until_iso)
    period_label = _format_report_period_label(since_dt, until_dt)
    include_card_errors = (db.get_setting(SETTING_CARD_CHECK_IN_REPORT) or "1").strip() != "0"
    ok, tg_err = await send_activity_report(
        token,
        chat_id,
        stats,
        period_label=period_label,
        interval=interval,
        include_card_errors=include_card_errors,
        db=db,
    )
    if not ok:
        detail = "Не удалось отправить сообщение в Telegram"
        if tg_err:
            detail = f"{detail}: {tg_err}"
        raise HTTPException(502, detail)
    try:
        db.set_setting(TELEGRAM_REPORT_LAST_SENT, until_dt.isoformat(timespec="seconds"))
        db.add_audit_event(
            actor="system",
            action="telegram_report",
            item_type="activity",
            result="ok",
            meta={
                "interval": interval,
                "period_label": period_label,
                "since": since_iso,
                "manual": manual,
                **stats,
            },
        )
    except Exception:
        pass
    log.info(
        "telegram_report sent interval=%s manual=%s reviews=%s questions=%s chats=%s "
        "ozon_removed=%s ozon_alerts=%s",
        interval,
        manual,
        stats.get("reviews_sent"),
        stats.get("questions_sent"),
        stats.get("chat_replies_total"),
        stats.get("ozon_products_removed"),
        stats.get("ozon_alerts"),
    )
    return {"ok": True, "period_label": period_label, **stats}


async def _maybe_send_telegram_report() -> None:
    global _tg_report_fail_until, _tg_report_fail_token
    db = get_db()
    if (db.get_setting(TELEGRAM_REPORT_ENABLED) or "").strip() != "1":
        return
    token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
    chat_id = resolve_telegram_chat_id(db, "report")
    if not token or not chat_id:
        return
    if time.time() < _tg_report_fail_until and token == _tg_report_fail_token:
        return
    interval = (db.get_setting(TELEGRAM_REPORT_INTERVAL) or "hour").strip()
    if interval not in ("hour", "day"):
        interval = "hour"
    period_sec = _telegram_report_period_seconds(interval)
    now = dt.datetime.now(MSK_TZ)
    last_s = (db.get_setting(TELEGRAM_REPORT_LAST_SENT) or "").strip()
    since_dt: dt.datetime
    if last_s:
        try:
            last_dt = dt.datetime.fromisoformat(last_s)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=MSK_TZ)
            if (now - last_dt).total_seconds() < period_sec:
                return
            since_dt = last_dt
        except Exception:
            since_dt = now - dt.timedelta(seconds=period_sec)
    else:
        since_dt = now - dt.timedelta(seconds=period_sec)
    try:
        await _send_telegram_report(db, since_dt=since_dt, until_dt=now, interval=interval)
        _tg_report_fail_until = 0.0
        _tg_report_fail_token = ""
    except HTTPException as e:
        detail = str(e.detail or "")
        if "404" in detail or "неверный токен" in detail.lower() or "not found" in detail.lower():
            _tg_report_fail_until = time.time() + 1800
            _tg_report_fail_token = token
            log.warning("telegram_report: %s (повтор не раньше чем через 30 мин)", detail)
        else:
            log.warning("telegram_report: %s", detail)


async def _telegram_report_loop() -> None:
    while True:
        try:
            await _maybe_send_telegram_report()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("telegram report loop failed")
        await asyncio.sleep(60)


async def _auto_scheduler_loop() -> None:
    global _scheduler_seen, _auto_run_task, _interval_skip_logged
    while True:
        try:
            db = get_db()
            cfg = _get_auto_schedule(db)
            now = dt.datetime.now(MSK_TZ)
            day = now.strftime("%Y-%m-%d")
            hm = now.strftime("%H:%M")
            _scheduler_seen = {k for k in _scheduler_seen if k.startswith(day + "|")}

            if _auto_run_task is not None and _auto_run_task.done():
                try:
                    _auto_run_task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log.exception("auto_run task failed: %s", e)
                    try:
                        db.add_audit_event(
                            actor="system",
                            action="auto_run",
                            item_type="mixed",
                            result="error",
                            meta={"error": str(e)[:800]},
                        )
                    except Exception:
                        pass
                _auto_run_task = None
                _interval_skip_logged = False

            busy = _auto_run_task is not None and not _auto_run_task.done()

            if cfg.get("enabled"):
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
                        if busy:
                            if not _interval_skip_logged:
                                log.info(
                                    "auto_run: интервал %sh пропущен — предыдущий цикл ещё выполняется (магазин %s/%s)",
                                    interval_h,
                                    _auto_state.get("store_index") or "?",
                                    _auto_state.get("store_count") or "?",
                                )
                                _interval_skip_logged = True
                        else:
                            run_reason = f"interval:{interval_h}h"
                            _interval_skip_logged = False
                else:
                    for slot in (cfg.get("slots") or []):
                        key = f"{day}|{slot}"
                        if hm == slot and key not in _scheduler_seen:
                            if busy:
                                log.info(
                                    "auto_run: слот %s пропущен — предыдущий цикл ещё выполняется (магазин %s/%s)",
                                    slot,
                                    _auto_state.get("store_index") or "?",
                                    _auto_state.get("store_count") or "?",
                                )
                                try:
                                    db.add_audit_event(
                                        actor="system",
                                        action="auto_run_skipped",
                                        item_type="mixed",
                                        result="skipped",
                                        meta={
                                            "slot": slot,
                                            "reason": "previous_run_still_running",
                                            "store_index": _auto_state.get("store_index"),
                                            "store_count": _auto_state.get("store_count"),
                                            "current_store_id": _auto_state.get("current_store_id"),
                                            "phase": _auto_state.get("phase"),
                                        },
                                    )
                                except Exception:
                                    pass
                            else:
                                _scheduler_seen.add(key)
                                run_reason = slot
                            break
                if run_reason and not busy:
                    if mode == "interval":
                        try:
                            db.set_setting(AUTO_LAST_RUN_KEY, now.isoformat(timespec="seconds"))
                        except Exception:
                            pass
                    _auto_run_task = asyncio.create_task(_run_auto_slot(run_reason))

            await asyncio.sleep(15)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("auto scheduler loop failed")
            await asyncio.sleep(15)

def _auto_run_readiness(cfg: dict, db: Database, *, check_schedule: bool = True) -> str:
    """Проверка готовности цикла (магазины, задачи, ключи)."""
    sids = [int(x) for x in (cfg.get("store_ids") or [])]
    if not sids:
        return "Не выбраны магазины — отметьте их и нажмите «Сохранить автозапуск»."
    stores = [s for s in db.list_stores() if s.active and s.id in sids]
    if not stores:
        return "Выбранные магазины не найдены или снята галочка «активен» — проверьте вкладку «Магазины»."
    if check_schedule:
        mode = str(cfg.get("schedule_mode") or "slots")
        if mode == "slots":
            slots = cfg.get("slots") or []
            if not slots:
                return "Режим «по слотам»: укажите время как 09:00, 14:30 (два символа в часе) и сохраните."
    if not (
        _any_auto_reviews(cfg)
        or _any_auto_questions(cfg)
        or cfg.get("run_wb_chats")
        or cfg.get("run_ozon_chats")
        or cfg.get("run_ozon_alerts")
        or cfg.get("run_ozon_actions_remove")
    ):
        return "Включите хотя бы один тип задач в автозапуске и сохраните."
    openai_key = (db.get_setting("openai_key") or "").strip()
    if (_any_auto_reviews(cfg) or _any_auto_questions(cfg)) and not openai_key:
        return "Для автоответов на отзывы и вопросы нужен ключ OpenAI в «Настройки»."
    if (
        (_any_auto_reviews(cfg) or _any_auto_questions(cfg) or cfg.get("run_wb_chats") or cfg.get("run_ozon_chats"))
        and (db.get_setting(SETTING_CARD_CHECK_ENABLED) or "1").strip() == "0"
    ):
        return "Проверка карточек выключена в «Настройки → Карточки» — при генерации ответов она не выполняется."
    if (db.get_setting(TELEGRAM_REPORT_ENABLED) or "").strip() == "1":
        tg_token = (db.get_setting("telegram_bot_token") or "").strip()
        tg_chat = resolve_telegram_chat_id(db, "report")
        if not tg_token:
            return "Включён периодический отчёт в Telegram — укажите токен бота в «Настройки»."
        if not tg_chat:
            return "Включён периодический отчёт — укажите ID чата для отчётов или основной чат Telegram."
    if cfg.get("run_wb_chats"):
        wb_stores = [s for s in stores if s.marketplace == "wb" and (s.api_key or "").strip()]
        if not wb_stores:
            return "В цикле включены чаты WB, но среди выбранных магазинов нет WB с API-ключом."
        if not (db.get_setting("openai_key") or "").strip():
            return "Автоответы в чатах WB: нужен ключ OpenAI в «Настройки» (генерация текста)."
    if cfg.get("run_ozon_chats"):
        ozon_stores = [
            s for s in stores
            if s.marketplace == "ozon" and (s.client_id or "").strip() and (s.api_key or "").strip()
        ]
        if not ozon_stores:
            return "В цикле включены чаты Ozon, но среди выбранных магазинов нет Ozon с Client-Id и Api-Key."
        if not (db.get_setting("openai_key") or "").strip():
            return "Автоответы в чатах Ozon: нужен ключ OpenAI в «Настройки»."
    if cfg.get("run_ozon_alerts"):
        ozon_stores = [
            s for s in stores
            if s.marketplace == "ozon" and (s.client_id or "").strip() and (s.api_key or "").strip()
        ]
        if not ozon_stores:
            return "В цикле включены уведомления Ozon, но нет Ozon-магазина с Client-Id и Api-Key."
        if not (db.get_setting("openai_key") or "").strip():
            return "Уведомления Ozon: нужен ключ OpenAI в «Настройки»."
        if (db.get_setting(OZON_ALERTS_ENABLED) or "0").strip() != "1":
            return "Включите «Важные уведомления Ozon» в настройках и сохраните."
    if cfg.get("run_ozon_actions_remove"):
        ozon_stores = [
            s for s in stores
            if s.marketplace == "ozon" and (s.client_id or "").strip() and (s.api_key or "").strip()
        ]
        if not ozon_stores:
            return "В цикле включено автоудаление из акций Ozon, но нет Ozon-магазина с Client-Id и Api-Key."
    return ""


def _schedule_hint(cfg: dict, db: Database) -> str:
    """Почему автозапуск включён, но цикл не пойдет (для UI)."""
    if not cfg.get("enabled"):
        return ""
    return _auto_run_readiness(cfg, db, check_schedule=True)


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
        **{k: bool(cfg.get(k, False)) for k in _AUTO_MP_REVIEW_KEYS + _AUTO_MP_QUESTION_KEYS},
        "run_wb_chats": bool(cfg.get("run_wb_chats", False)),
        "run_ozon_chats": bool(cfg.get("run_ozon_chats", False)),
        "run_ozon_alerts": bool(cfg.get("run_ozon_alerts", False)),
        "run_ozon_actions_remove": bool(cfg.get("run_ozon_actions_remove", False)),
        "next_slot": next_slot,
        "timezone": "Europe/Moscow",
        "schedule_hint": _schedule_hint(cfg, db),
    })
    return out


def _user_can_settings(user: UserRow, db: Database) -> bool:
    if user.role == "admin":
        return True
    return "view_settings" in db.get_user_permissions(user.id)


def _build_agent_context(db: Database, user: UserRow) -> AgentContext:
    can_settings = _user_can_settings(user, db)

    async def run_auto_now() -> dict:
        global _auto_run_task
        if _auto_run_task is not None and not _auto_run_task.done():
            return {"error": "Автозапуск уже выполняется — дождитесь завершения или остановите его"}
        cfg = _get_auto_schedule(db)
        hint = _auto_run_readiness(cfg, db, check_schedule=False)
        if hint:
            return {"error": hint}
        _auto_run_task = asyncio.create_task(_run_auto_slot("manual", force=True))
        return {"message": "Автозапуск запущен"}

    async def stop_auto() -> dict:
        cancelled = await _cancel_auto_run_if_busy()
        _disable_auto_schedule(db)
        return {"message": "Автозапуск остановлен", "stopped": cancelled, "enabled": False}

    async def send_report() -> dict:
        interval = (db.get_setting(TELEGRAM_REPORT_INTERVAL) or "hour").strip()
        now = dt.datetime.now(MSK_TZ)
        period_sec = _telegram_report_period_seconds(interval)
        since_dt = now - dt.timedelta(seconds=period_sec)
        try:
            await _send_telegram_report(db, since_dt=since_dt, until_dt=now, interval=interval, manual=True)
            return {"message": "Отчёт отправлен"}
        except HTTPException as e:
            return {"error": str(e.detail)}
        except Exception as e:
            return {"error": str(e)}

    return AgentContext(
        db=db,
        username=user.username,
        user_id=user.id,
        get_auto_status=lambda: _auto_status(db),
        run_auto_now=run_auto_now if can_settings else None,
        stop_auto=stop_auto if can_settings else None,
        send_telegram_report=send_report if can_settings else None,
    )


def _first_admin_user(db: Database) -> Optional[UserRow]:
    for u in db.list_users():
        if u.role == "admin":
            return u
    return None


def _agent_context_for_telegram(db: Database) -> Optional[AgentContext]:
    admin = _first_admin_user(db)
    if not admin:
        return None
    return _build_agent_context(db, admin)


configure_telegram_agent(get_db=get_db, context_factory=_agent_context_for_telegram)


def _store_to_out(s: Store) -> StoreOut:
    return StoreOut(
        id=s.id,
        marketplace=s.marketplace,
        name=s.name,
        active=s.active,
        api_key_set=bool((s.api_key or "").strip()),
        business_id=s.business_id,
        client_id=s.client_id,
    )


def _audit_meta_for_api(raw: str) -> str:
    if not (raw or "").strip():
        return raw or ""
    try:
        obj = json.loads(raw)
        return json.dumps(sanitize_for_audit(obj), ensure_ascii=False)
    except Exception:
        return redact_secrets_in_text(raw)


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
    api_key = s.api_key
    if body.api_key is not None:
        ak = (body.api_key or "").strip()
        if ak:
            api_key = ak
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


class ItemAnswerBody(BaseModel):
    generated_text: str


@app.patch("/api/items/{item_id}/answer")
def api_update_item_answer(
    item_id: int,
    body: ItemAnswerBody,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    row = db.get_item_by_id(item_id)
    if not row:
        raise HTTPException(404, "Элемент не найден")
    if row.status in ("sent", "sending"):
        raise HTTPException(400, "Нельзя редактировать ответ после отправки или во время отправки")
    ok = db.update_generated_text(item_id, body.generated_text)
    if not ok:
        raise HTTPException(400, "Текст ответа не может быть пустым")
    updated = db.get_item_by_id(item_id)
    return _item_to_out(updated) if updated else {"ok": True}


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
    keys = [
        "openai_key",
        "telegram_bot_token",
        "telegram_chat_id",
        "telegram_report_chat_id",
        "telegram_card_error_chat_id",
        "telegram_enabled",
        TELEGRAM_REPORT_ENABLED,
        TELEGRAM_REPORT_INTERVAL,
        SETTING_CARD_CHECK_ENABLED,
        SETTING_CARD_CHECK_TELEGRAM,
        SETTING_CARD_CHECK_TEMPLATE,
        SETTING_CARD_CHECK_IN_REPORT,
        OZON_ALERTS_ENABLED,
        OZON_ALERTS_TELEGRAM,
        OZON_ALERTS_FROM_DATE,
        OZON_ALERTS_TEMPLATE,
        "ozon_alerts_telegram_chat_id",
        "telegram_agent_enabled",
        "telegram_agent_chat_id",
        "telegram_agent_user_id",
        "theme",
        SETTING_REPLY_FROM,
        SETTING_AUTO_CHAT_MAX_AGE_DAYS,
    ]
    raw = {k: db.get_setting(k) or "" for k in keys}
    return mask_settings_for_api(raw)


@app.post("/api/settings")
def api_set_settings(body: dict[str, str], db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    global _tg_report_fail_until, _tg_report_fail_token
    for k, v in body.items():
        if k.endswith("_set"):
            continue
        if k in SECRET_SETTING_KEYS:
            old = db.get_setting(k) or ""
            v = resolve_secret_setting(v, old)
        if k == "telegram_bot_token":
            v = normalize_telegram_bot_token(v or "")
            _tg_report_fail_until = 0.0
            _tg_report_fail_token = ""
        if k == OZON_ALERTS_TEMPLATE and is_legacy_telegram_template(v or ""):
            v = ""
        if k == SETTING_CARD_CHECK_TEMPLATE and is_legacy_card_telegram_template(v or ""):
            v = ""
        db.set_setting(k, v or "")
    return {"ok": True}


@app.post("/api/telegram/test")
async def api_telegram_test(
    body: dict[str, str],
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_settings")),
):
    """Проверка токена (getMe) и пробная отправка в чат."""
    token = normalize_telegram_bot_token(
        resolve_secret_setting(
            body.get("telegram_bot_token"),
            db.get_setting("telegram_bot_token") or "",
        )
    )
    chat_id = (
        (body.get("telegram_chat_id") or "").strip()
        or resolve_telegram_chat_id(db, "report")
        or (db.get_setting("telegram_chat_id") or "").strip()
    )
    if not token:
        raise HTTPException(400, "Укажите токен бота")
    if not chat_id:
        raise HTTPException(400, "Укажите ID чата (основной или для отчётов)")
    ok, err, bot = await test_telegram_delivery(token, chat_id, db=db)
    if not ok:
        raise HTTPException(502, err or "Не удалось отправить тестовое сообщение")
    username = (bot or {}).get("username") or ""
    return {
        "ok": True,
        "bot_username": username,
        "chat_id": chat_id,
        "message": f"Тест отправлен (@{username})" if username else "Тест отправлен",
    }


@app.post("/api/telegram/report-now")
async def api_telegram_report_now(
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_settings")),
):
    """Отправить отчёт за последний час/сутки вручную (для проверки настроек)."""
    interval = (db.get_setting(TELEGRAM_REPORT_INTERVAL) or "hour").strip()
    if interval not in ("hour", "day"):
        interval = "hour"
    period_sec = _telegram_report_period_seconds(interval)
    now = dt.datetime.now(MSK_TZ)
    since_dt = now - dt.timedelta(seconds=period_sec)
    return await _send_telegram_report(db, since_dt=since_dt, until_dt=now, interval=interval, manual=True)


@app.get("/api/auto-schedule")
def api_get_auto_schedule(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return _get_auto_schedule(db)


@app.post("/api/auto-schedule")
async def api_set_auto_schedule(body: AutoScheduleBody, db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    cfg = _set_auto_schedule(db, body)
    if not cfg.get("enabled"):
        await _cancel_auto_run_if_busy()
    return cfg


@app.post("/api/auto-schedule/disable")
async def api_auto_schedule_disable(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    """Сразу выключить автозапуск в БД и остановить текущий цикл, если он идёт."""
    cancelled = await _cancel_auto_run_if_busy()
    cfg = _disable_auto_schedule(db)
    return {"ok": True, "enabled": bool(cfg.get("enabled")), "cancelled_running": cancelled}


@app.get("/api/auto-schedule/status")
def api_auto_schedule_status(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return _auto_status(db)


@app.post("/api/auto-schedule/run-now")
async def api_auto_schedule_run_now(
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_settings")),
):
    """Принудительный запуск цикла автозапуска (без ожидания слота/интервала)."""
    global _auto_run_task
    if _auto_run_task is not None and not _auto_run_task.done():
        raise HTTPException(409, "Автозапуск уже выполняется — дождитесь завершения или нажмите «Остановить»")
    cfg = _get_auto_schedule(db)
    hint = _auto_run_readiness(cfg, db, check_schedule=False)
    if hint:
        raise HTTPException(400, hint)
    _auto_run_task = asyncio.create_task(_run_auto_slot("manual", force=True))
    return {"ok": True, "started": True}


@app.post("/api/auto-schedule/stop")
async def api_auto_schedule_stop(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    cancelled = await _cancel_auto_run_if_busy()
    _disable_auto_schedule(db)
    return {"ok": True, "stopped": cancelled, "enabled": False}


# ---------- API: AI agent ----------
@app.post("/api/agent/chat")
async def api_agent_chat(
    body: AgentChatBody,
    user: UserRow = Depends(require_user),
    db: Database = Depends(get_db),
):
    session = get_or_create_session(user_id=user.id, username=user.username, session_id=body.session_id)
    msg = (body.message or "").strip()
    if body.confirm is True:
        msg = "да"
    elif body.confirm is False:
        msg = "отмена"
    if not msg and body.confirm is None:
        raise HTTPException(400, "Пустое сообщение")

    ctx = _build_agent_context(db, user)
    openai_key = (db.get_setting("openai_key") or "").strip()
    out = await handle_agent_message(
        session=session,
        user_message=msg,
        ctx=ctx,
        openai_key=openai_key,
        force_confirm=body.confirm is True,
    )
    sv = session_public_view(session)
    return {
        "reply": out.get("reply", ""),
        "session_id": session.session_id,
        "messages": sv["messages"],
        "pending": sv.get("pending"),
        "needs_confirm": bool(out.get("needs_confirm")),
        "tool_used": out.get("tool_used"),
    }


@app.get("/api/agent/session/{session_id}")
def api_agent_get_session(session_id: str, user: UserRow = Depends(require_user)):
    session = get_session_if_owner(session_id, user_id=user.id)
    if not session:
        raise HTTPException(404, "Сессия не найдена")
    return session_public_view(session)


@app.delete("/api/agent/session/{session_id}")
def api_agent_delete_session(session_id: str, user: UserRow = Depends(require_user)):
    if not clear_session(session_id, user_id=user.id):
        raise HTTPException(404, "Сессия не найдена")
    return {"ok": True}


# ---------- API: prompts ----------
@app.get("/api/prompts", response_model=list[PromptOut])
def api_list_prompts(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    return [PromptOut(id=p.id, item_type=p.item_type, rating_group=p.rating_group, prompt_text=p.prompt_text) for p in db.list_prompts()]


@app.patch("/api/prompts/{prompt_id}")
def api_update_prompt(prompt_id: int, body: PromptUpdate, db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_settings"))):
    db.update_prompt(prompt_id, body.prompt_text)
    return {"ok": True}


# ---------- API: config backup ----------
class ConfigImportBody(BaseModel):
    data: dict


@app.get("/api/config/export")
def api_config_export(
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_settings")),
):
    """Выгрузка магазинов, настроек, расписания и промптов в JSON-файл (без API-ключей)."""
    payload = export_config(db)
    stamp = dt.datetime.now(MSK_TZ).strftime("%Y%m%d-%H%M%S")
    filename = f"wb-autoreply-config-{stamp}.json"
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    return Response(
        content=body.encode("utf-8"),
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/config/import")
def api_config_import(
    body: ConfigImportBody,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_admin),
):
    """Загрузка конфигурации из JSON (магазины обновляются по имени и маркетплейсу)."""
    global _tg_report_fail_until, _tg_report_fail_token
    try:
        result = import_config(db, body.data)
    except ValueError as e:
        raise HTTPException(400, str(e))
    token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
    if token:
        db.set_setting("telegram_bot_token", token)
        _tg_report_fail_until = 0.0
        _tg_report_fail_token = ""
    tpl = db.get_setting(OZON_ALERTS_TEMPLATE) or ""
    if is_legacy_telegram_template(tpl):
        db.set_setting(OZON_ALERTS_TEMPLATE, "")
    card_tpl = db.get_setting(SETTING_CARD_CHECK_TEMPLATE) or ""
    if is_legacy_card_telegram_template(card_tpl):
        db.set_setting(SETTING_CARD_CHECK_TEMPLATE, "")
    return result


class CardErrorStatusBody(BaseModel):
    status: str


def _card_error_to_out(row: CardErrorAlertRow, store_name: str = "") -> dict:
    return {
        "id": row.id,
        "ts": row.ts,
        "store_id": row.store_id,
        "store_name": store_name,
        "source_type": row.source_type,
        "source_ref": row.source_ref,
        "product_title": row.product_title,
        "customer_text": row.customer_text,
        "error_kind": row.error_kind,
        "explanation": row.explanation,
        "status": row.status,
        "telegram_sent": row.telegram_sent,
    }


@app.get("/api/card-errors")
def api_list_card_errors(
    store_id: Optional[int] = None,
    status: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    rows = db.list_card_error_alerts(
        store_id=store_id,
        status=(status or "").strip() or None,
        limit=limit,
        offset=offset,
    )
    store_names = {s.id: s.name for s in db.list_stores()}
    return [_card_error_to_out(r, store_names.get(r.store_id, "")) for r in rows]


def _ozon_alert_to_out(row: OzonImportantAlertRow, store_name: str = "") -> dict:
    return {
        "id": row.id,
        "ts": row.ts,
        "store_id": row.store_id,
        "store_name": store_name,
        "chat_id": row.chat_id,
        "message_id": row.message_id,
        "chat_type": row.chat_type,
        "message_at": row.message_at,
        "message_at_label": format_ozon_datetime_msk(row.message_at) or row.message_at,
        "message_text": row.message_text,
        "threat_type": row.threat_type,
        "amount": row.amount,
        "product_ref": row.product_ref,
        "summary": row.summary,
        "action_needed": row.action_needed,
        "status": row.status,
        "telegram_sent": row.telegram_sent,
        "alert_category": row.alert_category or "",
    }


@app.get("/api/ozon/alerts")
def api_list_ozon_alerts(
    store_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    important_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=500),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    rows = db.list_ozon_important_alerts(
        store_id=store_id,
        status=status,
        limit=limit,
    )
    if important_only:
        rows = [r for r in rows if r.status != "ignored"]
    store_names = {s.id: s.name for s in db.list_stores()}
    return [_ozon_alert_to_out(r, store_names.get(r.store_id, "")) for r in rows]


@app.patch("/api/ozon/alerts/{alert_id}")
def api_update_ozon_alert_status(
    alert_id: int,
    body: dict,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    st = str((body or {}).get("status") or "").strip()
    ok = db.update_ozon_important_alert_status(alert_id, st)
    if not ok:
        raise HTTPException(400, "Неверный статус или алерт не найден")
    return {"ok": True}


@app.post("/api/ozon/alerts/{store_id}/scan")
async def api_scan_ozon_alerts(
    store_id: int,
    body: OzonAlertsScanBody = OzonAlertsScanBody(),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_settings")),
):
    stores = [s for s in db.list_stores() if s.id == int(store_id)]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    store = stores[0]
    if store.marketplace != "ozon":
        raise HTTPException(400, "Только для магазинов Ozon")
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ")
    if (db.get_setting(OZON_ALERTS_ENABLED) or "0").strip() != "1":
        raise HTTPException(400, "Включите «Важные уведомления Ozon» в настройках")
    try:
        stats = await asyncio.wait_for(
            scan_ozon_important_alerts_for_store(
                db, store, openai_key=key, rescan=bool(body.rescan)
            ),
            timeout=600.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, "Сканирование Ozon превысило 10 минут — попробуйте снова позже") from None
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    return {"ok": True, **stats}


@app.patch("/api/card-errors/{alert_id}")
def api_update_card_error_status(
    alert_id: int,
    body: CardErrorStatusBody,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    ok = db.update_card_error_status(alert_id, body.status)
    if not ok:
        raise HTTPException(400, "Некорректный статус")
    return {"ok": True}


# ---------- API: long-running tasks ----------
@app.post("/api/load-new")
async def api_load_new(body: LoadNewBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    try:
        task_id = await web_tasks.run_load_new(db, body.store_ids)
    except StoreBusyError as e:
        raise HTTPException(409, str(e)) from e
    return {"task_id": task_id}


@app.post("/api/generate")
async def api_generate(body: GenerateBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    key = db.get_setting("openai_key") or ""
    if not key.strip():
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    try:
        task_id = await web_tasks.run_generate(db, body.item_ids, key)
    except StoreBusyError as e:
        raise HTTPException(409, str(e)) from e
    return {"task_id": task_id}


@app.post("/api/send")
async def api_send(body: SendBody, db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    try:
        task_id = await web_tasks.run_send(db, body.item_ids)
    except StoreBusyError as e:
        raise HTTPException(409, str(e)) from e
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
        out.append(redact_secrets_in_text(s))
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
                "meta_json": _audit_meta_for_api(r.meta_json),
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
    return HTTPException(e.status, wb_chat_error_message(e.status, e.body or ""))


def _ozon_chat_http_error(e: HttpStatusError) -> HTTPException:
    return HTTPException(e.status, ozon_chat_error_message(e.status, e.body or ""))


def _wb_chat_list_payload(db: Database, store: Store, chats: list, **extra: object) -> dict:
    """Ответ списка чатов WB с привязкой к магазину (фронт сверяет store_id)."""
    sid = int(store.id)
    key = (store.api_key or "").strip()
    same_key_names: list[str] = []
    if key:
        for o in db.list_stores():
            if o.marketplace != "wb" or int(o.id) == sid:
                continue
            if (o.api_key or "").strip() == key:
                same_key_names.append(str(o.name or f"ID {o.id}"))
    payload: dict = {
        "store_id": sid,
        "store_name": store.name,
        "chats": chats,
    }
    if same_key_names:
        others = ", ".join(same_key_names[:5])
        payload["same_api_key_warning"] = (
            f"У «{store.name}» тот же API-ключ, что у: {others}. "
            "WB отдаёт одни и те же чаты для одного ключа — укажите разные ключи кабинетов."
        )
    payload.update(extra)
    return payload


@app.get("/api/wb/buyer-chats/{store_id}")
async def api_wb_buyer_chat_list(
    store_id: int,
    refresh: bool = Query(False, description="Сбросить кэш и заново запросить список у WB"),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    sid = int(store_id)
    stale_ent = _wb_chat_list_cache.get(sid)
    try:
        chats = await asyncio.wait_for(
            _wb_buyer_chat_list_cached(store_id, s.api_key, force_refresh=refresh),
            timeout=90.0,
        )
    except asyncio.TimeoutError:
        if stale_ent and stale_ent[1]:
            return _wb_chat_list_payload(
                db,
                s,
                list(stale_ent[1]),
                stale=True,
                warning=(
                    "WB не ответил за 90 с (лимит API или автозапуск). Показан сохранённый список — "
                    "нажмите «Обновить список чатов» через 1–2 мин."
                ),
            )
        raise HTTPException(
            504,
            "WB чаты: таймаут 90 с. WB buyer-chat отвечает медленно или 429 — подождите 1–2 мин и нажмите «Обновить».",
        ) from None
    except HttpStatusError as e:
        if e.status == 429 and stale_ent and stale_ent[1]:
            return _wb_chat_list_payload(
                db,
                s,
                list(stale_ent[1]),
                stale=True,
                warning=(
                    "WB: лимит запросов (429). Показан сохранённый список чатов — повторите обновление позже."
                ),
            )
        raise _wb_chat_http_error(e) from e
    return _wb_chat_list_payload(db, s, chats)


@app.get("/api/wb/buyer-chats/{store_id}/{chat_id}/thread")
async def api_wb_buyer_chat_thread(
    store_id: int,
    chat_id: str,
    pages: int = Query(10, ge=1, le=50, description="Сколько страниц ленты /events обойти для этого чата"),
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
        events, next_cursor = await fetch_events_for_chat(client, chat_id, max_wb_requests=pages)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    gc = merge_good_card(chat_row if isinstance(chat_row, dict) else {}, events)
    lines_ts = build_wb_thread_lines(events, chat_id, chat_row if isinstance(chat_row, dict) else None)
    lines = [{"role": r, "text": t, "addTimestamp": ts} for r, t, ts, _mk in lines_ts]
    texts_for_title = [t for _, t, __, ___ in lines_ts]
    product_title = product_title_from_wb_chat(gc, texts_for_title)
    reply_sign = str(chat_row.get("replySign") or "").strip()
    reply_from = _buyer_chat_reply_from(db)
    eligible, skip_reason, client_msg_key, _ts = _wb_chat_eligibility(
        db, store_id, chat_id, lines_ts, reply_from
    )
    return {
        "chat": chat_row,
        "events": events,
        "lines": lines,
        "product_title": product_title,
        "reply_sign": reply_sign,
        "good_card": gc,
        "client_message_key": client_msg_key,
        "already_replied": skip_reason == "already_replied",
        "eligible_for_reply": eligible,
        "skip_reason": skip_reason if not eligible else "",
        "reply_from_date": reply_from.isoformat() if reply_from else "",
        "has_more_history": next_cursor is not None,
        "events_loaded": len(events),
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
        events, _ = await fetch_events_for_chat(client, chat_id, max_wb_requests=20)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    gc = merge_good_card(chat_row if isinstance(chat_row, dict) else {}, events)
    lines_ts = build_wb_thread_lines(
        events,
        chat_id,
        chat_row if isinstance(chat_row, dict) else None,
    )
    texts_for_title = [t for _, t, __, ___ in lines_ts]
    product_title = product_title_from_wb_chat(gc, texts_for_title)
    excerpt_parts = []
    for role, text, _, __ in lines_ts:
        label = "Покупатель" if role == "client" else "Продавец" if role == "seller" else role
        excerpt_parts.append(f"{label}: {text}")
    conversation = "\n".join(excerpt_parts) if excerpt_parts else "(сообщений пока нет)"
    client_msg_key = ""
    last_client_text = ""
    info = wb_last_client_info(lines_ts)
    if info:
        client_msg_key, _ts = info
        for role, text, _t, mk in reversed(lines_ts):
            if role == "client":
                last_client_text = text or ""
                if not client_msg_key:
                    client_msg_key = mk
                break
    try:
        draft = await generate_wb_buyer_chat_reply(
            db,
            key,
            product_title=product_title,
            conversation_excerpt=conversation,
            store_id=store_id,
            chat_id=chat_id,
            client_message_key=client_msg_key or None,
            customer_text=last_client_text or conversation,
        )
    except json.JSONDecodeError:
        raise HTTPException(502, "Модель вернула не JSON — попробуйте сгенерировать ещё раз")
    except ValueError as e:
        raise HTTPException(502, str(e))
    except HttpStatusError as e:
        st = e.status if 400 <= e.status < 600 else 502
        log.warning("wb buyer chat generate OpenAI: %s %s", st, (e.body or "")[:300])
        raise HTTPException(st, e.body or str(e)) from e
    except Exception as e:
        log.warning("wb buyer chat generate: %s", e)
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
    chat_id = (body.chat_id or "").strip()
    client_msg_key = (body.client_message_key or "").strip()
    if chat_id and client_msg_key:
        if db.is_buyer_chat_replied(store_id, "wb", chat_id, client_msg_key):
            raise HTTPException(409, "На это сообщение покупателя уже был отправлен ответ.")
    try:
        out = await client.send_message(rs, msg)
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    if chat_id and client_msg_key:
        db.mark_buyer_chat_replied(store_id, "wb", chat_id, client_msg_key)
    _wb_chat_list_cache.pop(int(store_id), None)
    try:
        db.add_audit_event(
            actor=user.username,
            action="wb_buyer_chat_send",
            item_type="wb_chat",
            store_id=store_id,
            result="ok",
            meta={"len": len(msg), "chat_id": chat_id, "client_message_key": client_msg_key},
        )
    except Exception:
        pass
    return {"ok": True, "result": out}


@app.post("/api/wb/buyer-chats/{store_id}/mass-generate-send")
async def api_wb_buyer_chat_mass_generate_send(
    store_id: int,
    body: WbBuyerChatMassBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    """
    Для выбранного магазина WB: найти чаты, где последнее в треде — от покупателя,
    сгенерировать ответ (OpenAI) и сразу отправить в WB. Ограничение max_chats за один запрос.
    """
    s = _require_wb_store_for_chats(db, store_id)
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    max_chats = max(1, min(int(body.max_chats or 50), 100))
    event_pages = max(1, min(int(body.event_pages or 6), 12))
    _wb_chat_list_cache.pop(int(store_id), None)
    try:
        stats = await wb_buyer_chats_mass_generate_send_for_store(
            db,
            s,
            openai_key=key,
            event_pages=event_pages,
            max_chats=max_chats,
            model="gpt-5.2",
            pause_between_chats_sec=1.1,
        )
    except HttpStatusError as e:
        raise _wb_chat_http_error(e) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="wb_buyer_chat_mass_send",
            item_type="wb_chat",
            store_id=store_id,
            result="ok",
            meta=dict(stats),
        )
    except Exception:
        pass
    return stats


async def _ozon_buyer_chat_list_cached(
    store_id: int,
    client_id: str,
    api_key: str,
    *,
    force_refresh: bool,
) -> tuple[list, Optional[str]]:
    sid = int(store_id)
    if force_refresh:
        _ozon_chat_list_cache.pop(sid, None)
    now = time.monotonic()
    ent = _ozon_chat_list_cache.get(sid)
    if ent and (now - ent[0]) < _OZON_CHAT_LIST_TTL_S:
        return list(ent[1]), ent[2]
    client = OzonClient(client_id, api_key)
    try:
        chats = await client.list_all_chats()
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="chat")
        if reason:
            log.info("ozon chats store=%s skipped: %s (HTTP %s)", sid, reason, e.status)
            _ozon_chat_list_cache[sid] = (now, [], reason)
            return [], reason
        raise
    _ozon_chat_list_cache[sid] = (now, chats, None)
    return chats, None


def _require_ozon_store_for_chats(db: Database, store_id: int) -> Store:
    stores = [s for s in db.list_stores() if s.id == int(store_id)]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    s = stores[0]
    if s.marketplace != "ozon":
        raise HTTPException(400, "Чаты Ozon доступны только для магазинов Ozon")
    if not (s.client_id or "").strip() or not (s.api_key or "").strip():
        raise HTTPException(400, "Не заданы Client-Id и Api-Key магазина")
    return s


async def _find_ozon_chat_row(
    store_id: int,
    client_id: str,
    api_key: str,
    chat_id: str,
) -> dict:
    cid = (chat_id or "").strip()
    if not cid:
        raise HTTPException(400, "chat_id пустой")
    rows, skip = await _ozon_buyer_chat_list_cached(store_id, client_id, api_key, force_refresh=False)
    if skip:
        raise HTTPException(400, ozon_feature_unavailable_user_message(skip, feature="chat"))
    for row in rows:
        if ozon_chat_row_id(row) == cid:
            return row if isinstance(row, dict) else {}
    raise HTTPException(404, "Чат не найден. Обновите список чатов.")


async def _assert_ozon_buyer_chat_id(
    store_id: int,
    client_id: str,
    api_key: str,
    chat_id: str,
) -> dict:
    row = await _find_ozon_chat_row(store_id, client_id, api_key, chat_id)
    if not is_ozon_buyer_chat_row(row):
        raise HTTPException(
            400,
            "Автоответы только для переписки с покупателями (Buyer_Seller).",
        )
    return row


def _ozon_chat_preview(row: dict) -> str:
    unread = int(row.get("unread_count") or 0)
    chat = row.get("chat") if isinstance(row, dict) else {}
    status = ""
    if isinstance(chat, dict):
        status = str(chat.get("chat_status") or "")
    parts = []
    if unread:
        parts.append(f"непрочит.: {unread}")
    if status:
        parts.append(status)
    return ", ".join(parts) if parts else "—"


@app.get("/api/ozon/buyer-chats/{store_id}")
async def api_ozon_buyer_chat_list(
    store_id: int,
    refresh: bool = Query(False),
    filter: str = Query("buyers"),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    filter_kind = (filter or "buyers").strip().lower()
    try:
        rows, skip = await _ozon_buyer_chat_list_cached(
            store_id, s.client_id or "", s.api_key, force_refresh=refresh
        )
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    if skip:
        return {
            "chats": [],
            "filter": filter_kind,
            "buyer_only": filter_kind == "buyers",
            "unavailable": True,
            "unavailable_reason": skip,
            "message": ozon_feature_unavailable_user_message(skip, feature="chat"),
        }
    chats = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not ozon_chat_matches_filter(row, filter_kind):
            continue
        cid = ozon_chat_row_id(row)
        if not cid:
            continue
        chat_obj = row.get("chat") if isinstance(row.get("chat"), dict) else {}
        created_raw = str(chat_obj.get("created_at") or "")
        updated_raw = str(
            chat_obj.get("last_message_at")
            or chat_obj.get("updated_at")
            or chat_obj.get("created_at")
            or ""
        )
        chats.append({
            "chat_id": cid,
            "chat_type": ozon_chat_type(row) or "—",
            "category": ozon_chat_category(row),
            "chat_status": chat_obj.get("chat_status"),
            "created_at": created_raw,
            "created_at_label": format_ozon_datetime_msk(created_raw) or created_raw or "—",
            "last_activity_at": updated_raw,
            "last_activity_label": format_ozon_datetime_msk(updated_raw) or updated_raw or "—",
            "unread_count": row.get("unread_count"),
            "preview": _ozon_chat_preview(row),
        })
    return {"chats": chats, "filter": filter_kind, "buyer_only": filter_kind == "buyers"}


@app.get("/api/ozon/buyer-chats/{store_id}/{chat_id}/thread")
async def api_ozon_buyer_chat_thread(
    store_id: int,
    chat_id: str,
    limit: int = Query(50, ge=1, le=200),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    chat_row = await _find_ozon_chat_row(store_id, s.client_id or "", s.api_key, chat_id)
    is_buyer = is_ozon_buyer_chat_row(chat_row)
    client = OzonClient(s.client_id or "", s.api_key)
    try:
        hist = await client.chat_history(chat_id, limit=limit)
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    messages = hist.get("messages") or []
    if not isinstance(messages, list):
        messages = []
    lines_raw = collect_ozon_thread_lines(messages)
    lines = [
        {"role": r, "text": t, "message_id": mid, "created_at": ca}
        for r, t, mid, ca in lines_raw
    ]
    product_title = product_title_from_ozon_chat(messages, lines_raw)
    chat_obj = chat_row.get("chat") if isinstance(chat_row.get("chat"), dict) else {}
    chat_status = str(chat_obj.get("chat_status") or "")
    window = ozon_reply_window_hint(lines_raw, chat_status=chat_status)
    reply_from = _buyer_chat_reply_from(db)
    eligible, skip_reason, client_msg_key, _ca = _ozon_chat_eligibility(
        db, store_id, chat_id, lines_raw, reply_from
    )
    if not is_buyer:
        eligible = False
        skip_reason = "not_buyer_chat"
    if window.get("blocked"):
        eligible = False
        skip_reason = "reply_window_expired"
    return {
        "lines": lines,
        "product_title": product_title,
        "category": ozon_chat_category(chat_row),
        "can_reply": bool(is_buyer and eligible),
        "client_message_key": client_msg_key,
        "already_replied": skip_reason == "already_replied",
        "eligible_for_reply": eligible,
        "skip_reason": skip_reason if not eligible else "",
        "reply_from_date": reply_from.isoformat() if reply_from else "",
        "chat_status": chat_status,
        "last_client_message_at": window.get("last_client_message_at") or "",
        "hours_since_client": window.get("hours_since_client"),
        "reply_window_blocked": bool(window.get("blocked")),
        "reply_window_reason": window.get("reason") or "",
        "reply_window_warning": window.get("warning") or "",
    }


@app.post("/api/ozon/buyer-chats/{store_id}/generate-draft")
async def api_ozon_buyer_chat_generate(
    store_id: int,
    body: OzonBuyerChatGenerateBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    chat_id = (body.chat_id or "").strip()
    if not chat_id:
        raise HTTPException(400, "chat_id пустой")
    await _assert_ozon_buyer_chat_id(store_id, s.client_id or "", s.api_key, chat_id)
    client = OzonClient(s.client_id or "", s.api_key)
    try:
        hist = await client.chat_history(chat_id, limit=50)
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    messages = hist.get("messages") or []
    if not isinstance(messages, list):
        messages = []
    lines_raw = collect_ozon_thread_lines(messages)
    product_title = product_title_from_ozon_chat(messages, lines_raw)
    excerpt_parts = []
    for role, text, _mid, _ca in lines_raw:
        label = "Покупатель" if role == "client" else "Продавец" if role == "seller" else role
        excerpt_parts.append(f"{label}: {text}")
    conversation = "\n".join(excerpt_parts) if excerpt_parts else "(сообщений пока нет)"
    client_msg_key = ""
    last_client_text = ""
    info = ozon_last_client_info(lines_raw)
    if info:
        client_msg_key, _created = info
        for role, text, _mid, _ca in reversed(lines_raw):
            if role == "client":
                last_client_text = text or ""
                break
    try:
        draft = await generate_ozon_buyer_chat_reply(
            db,
            key,
            product_title=product_title,
            conversation_excerpt=conversation,
            store_id=store_id,
            chat_id=chat_id,
            client_message_key=client_msg_key or None,
            customer_text=last_client_text or conversation,
        )
    except json.JSONDecodeError:
        raise HTTPException(502, "Модель вернула не JSON — попробуйте сгенерировать ещё раз")
    except ValueError as e:
        raise HTTPException(502, str(e))
    except HttpStatusError as e:
        st = e.status if 400 <= e.status < 600 else 502
        raise HTTPException(st, e.body or str(e)) from e
    except Exception as e:
        raise HTTPException(502, f"Ошибка генерации: {e}") from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_buyer_chat_generate",
            item_type="ozon_chat",
            store_id=store_id,
            result="ok",
            meta={"chat_id": chat_id, "product_title": product_title[:200]},
        )
    except Exception:
        pass
    return {"draft": draft, "product_title": product_title}


@app.post("/api/ozon/buyer-chats/{store_id}/send")
async def api_ozon_buyer_chat_send(
    store_id: int,
    body: OzonBuyerChatSendBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    client = OzonClient(s.client_id or "", s.api_key)
    chat_id = (body.chat_id or "").strip()
    msg = (body.message or "").strip()
    client_msg_key = (body.client_message_key or "").strip()
    if not chat_id:
        raise HTTPException(400, "chat_id пустой")
    if not msg:
        raise HTTPException(400, "Текст сообщения пустой")
    await _assert_ozon_buyer_chat_id(store_id, s.client_id or "", s.api_key, chat_id)
    if client_msg_key and db.is_buyer_chat_replied(store_id, "ozon", chat_id, client_msg_key):
        raise HTTPException(409, "На это сообщение покупателя уже был отправлен ответ.")
    try:
        out = await client.send_chat_message(chat_id, msg)
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    if client_msg_key:
        db.mark_buyer_chat_replied(store_id, "ozon", chat_id, client_msg_key)
    _ozon_chat_list_cache.pop(int(store_id), None)
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_buyer_chat_send",
            item_type="ozon_chat",
            store_id=store_id,
            result="ok",
            meta={"len": len(msg), "chat_id": chat_id, "client_message_key": client_msg_key},
        )
    except Exception:
        pass
    return {"ok": True, "result": out}


@app.post("/api/ozon/buyer-chats/{store_id}/mass-generate-send")
async def api_ozon_buyer_chat_mass_generate_send(
    store_id: int,
    body: OzonBuyerChatMassBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    max_chats = max(1, min(int(body.max_chats or 50), 100))
    _ozon_chat_list_cache.pop(int(store_id), None)
    try:
        stats = await ozon_buyer_chats_mass_generate_send_for_store(
            db, s, openai_key=key, max_chats=max_chats, model="gpt-5.2", pause_between_chats_sec=1.0
        )
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_buyer_chat_mass_send",
            item_type="ozon_chat",
            store_id=store_id,
            result="ok",
            meta=dict(stats),
        )
    except Exception:
        pass
    return stats


# ---------- API: Ozon actions (promotions) ----------
@app.get("/api/ozon/actions/settings/{store_id}")
def api_ozon_actions_settings_get(
    store_id: int,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    _require_ozon_store_for_chats(db, store_id)
    cfg = _get_ozon_actions_settings(db)
    watched = _store_watched_action_ids(cfg, store_id)
    return {
        "auto_remove_on_schedule": bool(cfg.get("auto_remove_on_schedule")),
        "only_auto_add": bool(cfg.get("only_auto_add", True)),
        "watched_action_ids": watched,
    }


@app.post("/api/ozon/actions/settings/{store_id}")
def api_ozon_actions_settings_set(
    store_id: int,
    body: OzonActionsSettingsBody,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_permission("view_settings")),
):
    _require_ozon_store_for_chats(db, store_id)
    cfg = _get_ozon_actions_settings(db)
    cfg["auto_remove_on_schedule"] = bool(body.auto_remove_on_schedule)
    cfg["only_auto_add"] = bool(body.only_auto_add)
    stores = cfg.get("stores") or {}
    if not isinstance(stores, dict):
        stores = {}
    stores[str(int(store_id))] = {
        "watched_action_ids": [int(x) for x in (body.watched_action_ids or [])],
    }
    cfg["stores"] = stores
    db.set_setting(OZON_ACTIONS_SETTINGS_KEY, json.dumps(cfg, ensure_ascii=False))
    return {
        "auto_remove_on_schedule": cfg["auto_remove_on_schedule"],
        "only_auto_add": cfg["only_auto_add"],
        "watched_action_ids": stores[str(int(store_id))]["watched_action_ids"],
    }


@app.get("/api/ozon/actions/{store_id}")
async def api_ozon_actions_list(
    store_id: int,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    client = OzonClient(s.client_id or "", s.api_key)
    try:
        raw = await client.list_actions()
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="actions")
        if reason:
            return {
                "actions": [],
                "unavailable": True,
                "unavailable_reason": reason,
                "message": ozon_feature_unavailable_user_message(reason, feature="actions"),
            }
        raise _ozon_chat_http_error(e) from e
    actions = [normalize_action_row(a) for a in raw if isinstance(a, dict)]
    actions.sort(key=lambda x: (0 if x.get("is_auto_add") else 1, -(x.get("participating_products_count") or 0)))
    return {"actions": actions}


@app.get("/api/ozon/actions/{store_id}/{action_id}/products")
async def api_ozon_action_products(
    store_id: int,
    action_id: int,
    limit: int = Query(100, ge=1, le=100),
    last_id: Optional[str] = Query(None),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    client = OzonClient(s.client_id or "", s.api_key)
    try:
        block = await client.list_action_products(int(action_id), limit=limit, last_id=last_id)
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    products = block.get("products") or []
    return {
        "products": products if isinstance(products, list) else [],
        "total": block.get("total"),
        "last_id": block.get("last_id"),
    }


@app.post("/api/ozon/actions/{store_id}/remove")
async def api_ozon_actions_remove(
    store_id: int,
    body: OzonActionsRemoveBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    client = OzonClient(s.client_id or "", s.api_key)
    action_ids = [int(x) for x in (body.action_ids or [])]
    if not action_ids:
        try:
            raw = await client.list_actions()
        except HttpStatusError as e:
            reason = ozon_http_skip_reason(e.status, e.body or "", feature="actions")
            if reason:
                return {
                    "actions_processed": 0,
                    "products_removed": 0,
                    "skipped": 1,
                    "reason": reason,
                    "message": ozon_feature_unavailable_user_message(reason, feature="actions"),
                }
            raise _ozon_chat_http_error(e) from e
        picked = pick_actions_for_removal(raw, only_auto_add=bool(body.only_auto_add))
        action_ids = [int(a.get("id")) for a in picked if a.get("id") is not None]
    if not action_ids:
        return {"actions_processed": 0, "products_removed": 0, "message": "Нет акций с товарами для удаления"}
    try:
        stats = await remove_products_from_actions(client, action_ids)
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_actions_remove",
            item_type="ozon_action",
            store_id=store_id,
            result="ok",
            meta=dict(stats),
        )
    except Exception:
        pass
    return stats


@app.post("/api/ozon/actions/{store_id}/auto-remove")
async def api_ozon_actions_auto_remove(
    store_id: int,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    cfg = _get_ozon_actions_settings(db)
    watched = _store_watched_action_ids(cfg, store_id)
    try:
        stats = await ozon_actions_auto_remove_for_store(
            s,
            only_auto_add=bool(cfg.get("only_auto_add", True)),
            action_ids=watched if watched else None,
        )
    except HttpStatusError as e:
        raise _ozon_chat_http_error(e) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_actions_auto_remove",
            item_type="ozon_action",
            store_id=store_id,
            result="ok",
            meta=dict(stats) if isinstance(stats, dict) else {"stats": str(stats)},
        )
    except Exception:
        pass
    return stats


# ---------- API: связки карточек (WB / Ozon) ----------


class CardLinksWbMergeBody(BaseModel):
    target_imt: int
    nm_ids: list[int]
    catalog_rows: list[dict] = []
    disconnect_first: bool = True


class CardLinksOzonLinkBody(BaseModel):
    offer_ids: list[str]
    model_name: str
    catalog_rows: Optional[list[dict]] = None
    unlink_first: bool = True


class CardLinksWbDisconnectBody(BaseModel):
    nm_ids: list[int]


class CardLinksOzonUnlinkBody(BaseModel):
    offer_ids: list[str]


class CardLinksOzonQtyTableBody(BaseModel):
    table: str
    dry_run: bool = False


class CardLinksAiOptions(BaseModel):
    include_linked: bool = True
    scope: str = "all"
    batch_size: int = 60
    max_products: int = 0
    max_ai_batches: int = 12
    deterministic_packs: bool = True
    split_oversized: bool = True


class CardLinksAiSuggestBody(BaseModel):
    items: Optional[list] = None
    groups: Optional[list] = None
    options: Optional[CardLinksAiOptions] = None


class CardLinksAiPromptOut(BaseModel):
    marketplace: str
    prompt_text: str
    stored_text: str
    default_prompt: str
    is_custom: bool


class CardLinksAiPromptUpdate(BaseModel):
    prompt_text: str = ""


def _card_links_ai_prompt_marketplace(marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    if mp not in ("wb", "ozon"):
        raise HTTPException(400, "marketplace должен быть wb или ozon")
    return mp


@app.get("/api/card-links/ai-prompt/{marketplace}", response_model=CardLinksAiPromptOut)
def api_card_links_ai_prompt_get(
    marketplace: str,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    mp = _card_links_ai_prompt_marketplace(marketplace)
    stored = get_card_links_ai_prompt_stored(db, mp)
    default = default_ai_system_prompt(mp)
    effective = resolve_ai_system_prompt(mp, stored) if stored else default
    return CardLinksAiPromptOut(
        marketplace=mp,
        prompt_text=effective,
        stored_text=stored,
        default_prompt=default,
        is_custom=bool(stored),
    )


@app.put("/api/card-links/ai-prompt/{marketplace}", response_model=CardLinksAiPromptOut)
def api_card_links_ai_prompt_put(
    marketplace: str,
    body: CardLinksAiPromptUpdate,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    mp = _card_links_ai_prompt_marketplace(marketplace)
    text = (body.prompt_text or "").strip()
    default = default_ai_system_prompt(mp)
    if not text or text == default:
        set_card_links_ai_prompt_stored(db, mp, "")
    else:
        set_card_links_ai_prompt_stored(db, mp, text)
    stored = get_card_links_ai_prompt_stored(db, mp)
    effective = resolve_ai_system_prompt(mp, stored) if stored else default
    return CardLinksAiPromptOut(
        marketplace=mp,
        prompt_text=effective,
        stored_text=stored,
        default_prompt=default,
        is_custom=bool(stored),
    )


def _card_links_http_error(marketplace: str, e: HttpStatusError) -> HTTPException:
    mp = "Wildberries" if marketplace == "wb" else "Ozon"
    if marketplace == "wb" and e.status == 400:
        return HTTPException(400, wb_merge_error_message(e.body or ""))
    if marketplace == "wb":
        return HTTPException(e.status, wb_content_api_error_message(e.status, e.body or ""))
    body = redact_secrets_in_text((e.body or "")[:500])
    if e.status in (401, 403):
        return HTTPException(e.status, f"{mp}: доступ запрещён. Проверьте Client-Id и Api-Key Ozon.")
    if e.status == 429:
        return HTTPException(
            429,
            f"{mp}: слишком много запросов (лимит API). Подождите 1–2 минуты и обновите каталог.",
        )
    return HTTPException(e.status, f"{mp} API {e.status}: {body or 'ошибка'}")


CARD_LINKS_CATALOG_TIMEOUT_SEC = 600.0


@app.get("/api/card-links/wb/{store_id}/catalog")
async def api_card_links_wb_catalog(
    store_id: int,
    articles: Optional[str] = Query(None, description="Артикулы через запятую или с новой строки"),
    q: Optional[str] = Query(None, description="Поиск по названию/артикулу"),
    max_pages: int = Query(100, ge=1, le=150),
    articles_only: bool = Query(False, description="Только карточки из списка артикулов"),
    suggestions: str = Query("none", description="none | review | all — вычисление предложений"),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    vendor_codes = parse_articles_csv(articles)
    if articles_only and not vendor_codes:
        raise HTTPException(400, "Укажите список артикулов продавца (vendor_code)")

    async def _load() -> dict:
        rows, catalog_meta = await fetch_wb_catalog(
            s.api_key,
            vendor_codes=vendor_codes or None,
            text_search=(q or "").strip() or None,
            max_pages=max_pages,
            articles_only=articles_only,
        )
        return build_wb_catalog_payload(
            rows,
            catalog_meta,
            store_id=store_id,
            articles_only=articles_only,
            suggestions=suggestions,
        )

    try:
        return await asyncio.wait_for(_load(), timeout=CARD_LINKS_CATALOG_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        raise HTTPException(
            504,
            "Загрузка каталога WB превысила 10 минут — уменьшите «Страниц каталога» или повторите позже.",
        ) from None
    except HttpStatusError as e:
        raise _card_links_http_error("wb", e) from e


@app.get("/api/card-links/ozon/{store_id}/catalog")
async def api_card_links_ozon_catalog(
    store_id: int,
    articles: Optional[str] = Query(None, description="offer_id через запятую или с новой строки"),
    max_pages: int = Query(30, ge=1, le=100),
    articles_only: bool = Query(False, description="Только карточки из списка артикулов"),
    suggestions: str = Query("none", description="none | review | all"),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    offer_ids = parse_articles_csv(articles)
    if articles_only and not offer_ids:
        raise HTTPException(400, "Укажите список артикулов продавца (offer_id)")
    catalog_meta: dict = {}
    try:
        rows = await fetch_ozon_catalog(
            s.client_id or "",
            s.api_key,
            offer_ids=offer_ids or None,
            max_pages=max_pages,
            meta_out=catalog_meta,
            articles_only=articles_only,
        )
    except HttpStatusError as e:
        raise _card_links_http_error("ozon", e) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return build_ozon_catalog_payload(
        rows,
        catalog_meta,
        store_id=store_id,
        articles_only=articles_only,
        suggestions=suggestions,
    )


@app.post("/api/card-links/wb/{store_id}/merge")
async def api_card_links_wb_merge(
    store_id: int,
    body: CardLinksWbMergeBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_permission("view_settings")),
):
    s = _require_wb_store_for_chats(db, store_id)
    nm_ids = [int(x) for x in body.nm_ids if x is not None]
    if not nm_ids:
        raise HTTPException(400, "nm_ids пуст")
    if len(nm_ids) > 200:
        raise HTTPException(400, "WB: не более 200 nmID за один запрос")
    try:
        result = await wb_merge_cards(
            s.api_key,
            target_imt=int(body.target_imt),
            nm_ids=nm_ids,
            catalog_rows=body.catalog_rows or None,
            disconnect_first=body.disconnect_first,
        )
    except HttpStatusError as e:
        raise _card_links_http_error("wb", e) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="wb_card_links_merge",
            item_type="card_link",
            store_id=store_id,
            result="ok",
            meta={"target_imt": int(body.target_imt), "nm_ids": nm_ids},
        )
    except Exception:
        pass
    disconnected = int((result or {}).get("disconnected") or 0)
    msg = "Запрос на объединение отправлен в WB. Склейка может занять до нескольких часов."
    if disconnected:
        msg = (
            f"Развязано {disconnected} карточек, затем отправлено объединение в imtID {int(body.target_imt)}. "
            "Склейка может занять до нескольких часов."
        )
    return {"ok": True, "result": result, "message": msg}


@app.post("/api/card-links/ozon/{store_id}/link")
async def api_card_links_ozon_link(
    store_id: int,
    body: CardLinksOzonLinkBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_permission("view_settings")),
):
    s = _require_ozon_store_for_chats(db, store_id)
    offer_ids = [str(x).strip() for x in body.offer_ids if str(x).strip()]
    model_name = (body.model_name or "").strip()
    if not offer_ids:
        raise HTTPException(400, "offer_ids пуст")
    if not model_name:
        raise HTTPException(400, "model_name пуст — укажите название модели")
    try:
        result = await ozon_link_by_model(
            s.client_id or "",
            s.api_key,
            offer_ids=offer_ids,
            model_name=model_name,
            catalog_rows=body.catalog_rows or None,
            unlink_first=body.unlink_first,
        )
    except HttpStatusError as e:
        raise _card_links_http_error("ozon", e) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_card_links_link",
            item_type="card_link",
            store_id=store_id,
            result="ok",
            meta={"offer_ids": offer_ids, "model_name": model_name},
        )
    except Exception:
        pass
    unlinked = int((result or {}).get("unlinked") or 0)
    msg = (
        "«Название модели» обновлено. Склейка на Ozon может занять до 24 часов. "
        "У вариантов должны отличаться размер, цвет или другие вариативные характеристики."
    )
    if unlinked:
        msg = (
            f"Развязано {unlinked} товаров, затем обновлено «Название модели». "
            "Склейка на Ozon может занять до 24 часов."
        )
    return {
        "ok": True,
        "result": result,
        "message": msg,
    }


@app.post("/api/card-links/ozon/{store_id}/link-qty-table")
async def api_card_links_ozon_link_qty_table(
    store_id: int,
    body: CardLinksOzonQtyTableBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_permission("view_settings")),
):
    """Связка по таблице TMS: строка = 1/2/3 шт одного товара."""
    s = _require_ozon_store_for_chats(db, store_id)
    try:
        result = await link_ozon_tms_qty_groups(
            s.client_id or "",
            s.api_key,
            table=body.table or "",
            dry_run=bool(body.dry_run),
        )
    except HttpStatusError as e:
        raise _card_links_http_error("ozon", e) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    if not body.dry_run:
        try:
            db.add_audit_event(
                actor=user.username,
                action="ozon_card_links_qty_table",
                item_type="card_link",
                store_id=store_id,
                result="ok",
                meta={
                    "group_count": result.get("group_count"),
                    "ok_count": result.get("ok_count"),
                    "fail_count": result.get("fail_count"),
                },
            )
        except Exception:
            pass
    if body.dry_run:
        return {
            "ok": True,
            "dry_run": True,
            **result,
            "message": f"Проверка: {result.get('group_count', 0)} строк, готовы к связке: {sum(1 for p in result.get('preview') or [] if p.get('ok'))}. При связке кол-во в упаковке: 1 / 2 / 3 по колонкам.",
        }
    return {
        "ok": True,
        **result,
        "message": (
            f"Связано строк: {result.get('ok_count', 0)} из {result.get('group_count', 0)}. "
            f"Ошибок: {result.get('fail_count', 0)}. Склейка на Ozon может занять до 24 часов."
        ),
    }


@app.post("/api/card-links/wb/{store_id}/disconnect")
async def api_card_links_wb_disconnect(
    store_id: int,
    body: CardLinksWbDisconnectBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_permission("view_settings")),
):
    s = _require_wb_store_for_chats(db, store_id)
    nm_ids = [int(x) for x in body.nm_ids if x is not None]
    if not nm_ids:
        raise HTTPException(400, "nm_ids пуст")
    if len(nm_ids) > 30:
        raise HTTPException(400, "WB: не более 30 nmID за операцию")
    try:
        result = await wb_disconnect_cards(s.api_key, nm_ids=nm_ids)
    except HttpStatusError as e:
        raise _card_links_http_error("wb", e) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    if result.get("errors"):
        raise HTTPException(
            502,
            f"WB: не удалось разъединить {result.get('failed')} из {len(nm_ids)} карточек",
        )
    try:
        db.add_audit_event(
            actor=user.username,
            action="wb_card_links_disconnect",
            item_type="card_link",
            store_id=store_id,
            result="ok",
            meta={"nm_ids": nm_ids, "processed": result.get("processed")},
        )
    except Exception:
        pass
    return {
        "ok": True,
        "result": result,
        "message": f"Разъединено карточек WB: {result.get('processed', len(nm_ids))}. Изменения могут отобразиться не сразу.",
    }


@app.post("/api/card-links/ozon/{store_id}/unlink")
async def api_card_links_ozon_unlink(
    store_id: int,
    body: CardLinksOzonUnlinkBody,
    db: Database = Depends(get_db),
    user: UserRow = Depends(require_permission("view_settings")),
):
    s = _require_ozon_store_for_chats(db, store_id)
    offer_ids = [str(x).strip() for x in body.offer_ids if str(x).strip()]
    if not offer_ids:
        raise HTTPException(400, "offer_ids пуст")
    titles: dict[str, str] = {}
    try:
        rows = await fetch_ozon_catalog(
            s.client_id or "",
            s.api_key,
            offer_ids=offer_ids,
            max_pages=3,
        )
        for r in rows:
            oid = str(r.get("offer_id") or "").strip()
            if oid:
                titles[oid] = str(r.get("title") or "")
    except HttpStatusError:
        pass
    try:
        result = await ozon_unlink_cards(
            s.client_id or "",
            s.api_key,
            offer_ids=offer_ids,
            titles_by_offer=titles,
        )
    except HttpStatusError as e:
        raise _card_links_http_error("ozon", e) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    try:
        db.add_audit_event(
            actor=user.username,
            action="ozon_card_links_unlink",
            item_type="card_link",
            store_id=store_id,
            result="ok",
            meta={"offer_ids": offer_ids},
        )
    except Exception:
        pass
    return {
        "ok": True,
        "result": result,
        "message": "У каждого товара задано уникальное «Название модели». Разъединение на Ozon может занять до 24 часов.",
    }


@app.post("/api/card-links/wb/{store_id}/ai-suggest")
async def api_card_links_wb_ai_suggest(
    store_id: int,
    body: CardLinksAiSuggestBody = CardLinksAiSuggestBody(),
    articles: Optional[str] = Query(None),
    max_pages: int = Query(20, ge=1, le=100),
    articles_only: bool = Query(False),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    opts = body.options or CardLinksAiOptions()
    if body.items:
        rows = list(body.items)
        groups = list(body.groups or group_wb_rows(rows))
        apply_link_status(rows, groups)
    else:
        vendor_codes = parse_articles_csv(articles)
        if articles_only and not vendor_codes:
            raise HTTPException(400, "Укажите список артикулов продавца (vendor_code)")
        try:
            rows, _catalog_meta = await fetch_wb_catalog(
                s.api_key,
                vendor_codes=vendor_codes or None,
                max_pages=max_pages,
                articles_only=articles_only,
            )
        except HttpStatusError as e:
            raise _card_links_http_error("wb", e) from e
        groups = group_wb_rows(rows)
        apply_link_status(rows, groups)
    ai_prompt = get_card_links_ai_prompt_stored(db, "wb")
    task_id = await web_tasks.run_card_links_ai_suggest(
        rows=rows,
        groups=groups,
        marketplace="wb",
        openai_key=key,
        include_linked=opts.include_linked,
        scope=opts.scope,
        batch_size=opts.batch_size,
        max_products=opts.max_products,
        max_ai_batches=opts.max_ai_batches,
        deterministic_packs=opts.deterministic_packs,
        split_oversized=opts.split_oversized,
        system_prompt=ai_prompt,
    )
    return {"task_id": task_id, "status": "running"}


@app.post("/api/card-links/ozon/{store_id}/ai-suggest")
async def api_card_links_ozon_ai_suggest(
    store_id: int,
    body: CardLinksAiSuggestBody = CardLinksAiSuggestBody(),
    articles: Optional[str] = Query(None),
    max_pages: int = Query(15, ge=1, le=50),
    articles_only: bool = Query(False),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_ozon_store_for_chats(db, store_id)
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    opts = body.options or CardLinksAiOptions()
    if body.items:
        rows = list(body.items)
        groups = list(body.groups or group_ozon_rows(rows, articles_only=False))
        apply_link_status(rows, groups)
    else:
        offer_ids = parse_articles_csv(articles)
        if articles_only and not offer_ids:
            raise HTTPException(400, "Укажите список артикулов продавца (offer_id)")
        try:
            rows = await fetch_ozon_catalog(
                s.client_id or "",
                s.api_key,
                offer_ids=offer_ids or None,
                max_pages=max_pages,
                articles_only=articles_only,
            )
        except HttpStatusError as e:
            raise _card_links_http_error("ozon", e) from e
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        groups = group_ozon_rows(rows, articles_only=articles_only)
        apply_link_status(rows, groups)
    ai_prompt = get_card_links_ai_prompt_stored(db, "ozon")
    task_id = await web_tasks.run_card_links_ai_suggest(
        rows=rows,
        groups=groups,
        marketplace="ozon",
        openai_key=key,
        include_linked=opts.include_linked,
        scope=opts.scope,
        batch_size=opts.batch_size,
        max_products=opts.max_products,
        max_ai_batches=opts.max_ai_batches,
        deterministic_packs=opts.deterministic_packs,
        split_oversized=opts.split_oversized,
        system_prompt=ai_prompt,
    )
    return {"task_id": task_id, "status": "running"}


class CardLinksMasterStepBody(BaseModel):
    max_pages: int = 100
    bundle_ids: list[str] = []


@app.get("/api/card-links/master/{store_id}/status")
def api_card_links_master_status(
    store_id: int,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    _require_wb_store_for_chats(db, store_id)
    state = db.clm_get_state(store_id)
    coverage = db.clm_coverage(store_id)
    filters = db.clm_filter_options(store_id)
    dense = db.clm_dense_categories(store_id, min_count=3)
    return {"ok": True, "state": state, "coverage": coverage, "filters": filters, "dense_categories": dense}


@app.get("/api/card-links/master/{store_id}/bundles")
def api_card_links_master_bundles(
    store_id: int,
    segment: str = Query(""),
    brand: str = Query(""),
    category: str = Query(""),
    min_bundles_in_category: int = Query(0, ge=0, le=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=5000),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    _require_wb_store_for_chats(db, store_id)
    offset = (page - 1) * page_size
    bundles, total = db.clm_load_bundles(
        store_id,
        segment=segment.strip(),
        brand=brand.strip(),
        category=category.strip(),
        min_bundles_in_category=min_bundles_in_category,
        limit=page_size,
        offset=offset,
    )
    cat_counts = db.clm_category_bundle_counts(store_id)
    return {
        "ok": True,
        "bundles": bundles,
        "total": total,
        "page": page,
        "page_size": page_size,
        "page_count": max(1, (total + page_size - 1) // page_size),
        "category_counts": cat_counts,
    }


@app.get("/api/card-links/master/{store_id}/bundle-ids")
def api_card_links_master_bundle_ids(
    store_id: int,
    segment: str = Query(""),
    brand: str = Query(""),
    category: str = Query(""),
    min_bundles_in_category: int = Query(0, ge=0, le=100),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    _require_wb_store_for_chats(db, store_id)
    ids = db.clm_list_bundle_ids(
        store_id,
        segment=segment.strip(),
        brand=brand.strip(),
        category=category.strip(),
        min_bundles_in_category=min_bundles_in_category,
    )
    return {"ok": True, "bundle_ids": ids, "total": len(ids)}


class CardLinksMasterMergeBody(BaseModel):
    bundle_ids: list[str]


@app.post("/api/card-links/master/{store_id}/merge-bundles")
def api_card_links_master_merge_bundles(
    store_id: int,
    body: CardLinksMasterMergeBody,
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    from app.core.card_links_master import master_merge_bundles

    _require_wb_store_for_chats(db, store_id)
    ids = [str(x).strip() for x in (body.bundle_ids or []) if str(x).strip()]
    if len(ids) < 2:
        raise HTTPException(400, "Укажите минимум 2 bundle_id")
    rows = db.clm_load_items(store_id)
    if not rows:
        raise HTTPException(400, "Сначала выполните шаг «Загрузить WB» и «План»")
    bundles, _ = db.clm_load_bundles(store_id, limit=100000, offset=0)
    if not bundles:
        raise HTTPException(400, "Нет плана связок — выполните шаг «План»")
    try:
        rows, bundles, meta = master_merge_bundles(rows, bundles, ids)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    db.clm_save_items(store_id, rows)
    db.clm_save_bundles(store_id, bundles)
    db.clm_append_log(
        store_id,
        f"Объединено {len(meta.get('merged_from') or [])} связок → "
        f"{meta.get('new_bundle_id')} ({meta.get('item_count')} шт)",
    )
    return {"ok": True, **meta, "coverage": db.clm_coverage(store_id)}


@app.post("/api/card-links/master/{store_id}/step/{step_name}")
async def api_card_links_master_step(
    store_id: int,
    step_name: str,
    body: CardLinksMasterStepBody = CardLinksMasterStepBody(),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    s = _require_wb_store_for_chats(db, store_id)
    step = (step_name or "").strip().lower()
    allowed = {"load", "brands", "segment", "classify", "plan", "apply"}
    if step not in allowed:
        raise HTTPException(400, f"Шаг должен быть один из: {', '.join(sorted(allowed))}")
    openai_key = (db.get_setting("openai_key") or "").strip()
    bundle_ids = body.bundle_ids if step == "apply" else None
    if step == "load":
        db.clm_clear_store(store_id)
    try:
        task_id = await web_tasks.run_card_links_master_step(
            db,
            store_id=store_id,
            step=step,
            api_key=s.api_key,
            openai_key=openai_key,
            max_pages=min(int(body.max_pages or 100), 150),
            bundle_ids=bundle_ids,
        )
    except StoreBusyError as e:
        raise HTTPException(409, str(e)) from e
    return {"task_id": task_id, "status": "running", "step": step}


# ---------- API: stats ----------
@app.get("/api/stats")
def api_stats(db: Database = Depends(get_db), _: UserRow = Depends(require_user)):
    return db.get_stats()


@app.get("/api/health")
@app.get("/health")
def api_health():
    return {"ok": True, "service": "wb-autoreply"}


@app.get("/api/quality-metrics")
async def api_quality_metrics(
    refresh: bool = Query(False),
    db: Database = Depends(get_db),
    _: UserRow = Depends(require_user),
):
    """Показатели качества Ozon по магазинам (кэш ~30 мин)."""
    stores = db.list_stores()
    try:
        return await asyncio.wait_for(
            fetch_all_quality(stores, use_cache=not refresh),
            timeout=120.0,
        )
    except asyncio.TimeoutError:
        return await fetch_all_quality(stores, use_cache=True)


def _collapse_traceback_lines_for_log_ui(lines: list[str]) -> list[str]:
    """Убирает многострочный Python-traceback из хвоста файла (старые ERROR|wf с log.exception)."""
    out: list[str] = []
    in_tb = False
    for line in lines:
        if "Traceback (most recent call last):" in line:
            in_tb = True
            continue
        if in_tb:
            if len(line) >= 10 and line[:4].isdigit() and line[4] == "-":
                in_tb = False
                out.append(line)
                continue
            st = line.lstrip()
            if st.startswith("The above exception") or st.startswith("During handling"):
                continue
            if st.startswith("File \"") or st.startswith("File '"):
                continue
            if line.startswith("    ") or line.startswith("  "):
                continue
            if not st:
                continue
            if "Error:" in line or "Exception:" in line:
                in_tb = False
                if len(line) > 500:
                    out.append(line[:480] + " …")
                else:
                    out.append(line)
                continue
            continue
        out.append(line)
    return out


def _sanitize_log_for_admin_ui(text: str) -> str:
    """Вкладка «Лог»: без километровых JSON и тел tracebacks; хвост файла."""
    if not text:
        return ""
    max_chunk = 150_000
    chunk = text[-max_chunk:] if len(text) > max_chunk else text
    lines = _collapse_traceback_lines_for_log_ui(chunk.splitlines())
    out: list[str] = []
    for line in lines:
        if len(line) > 800 and ("insufficient_quota" in line or "rate_limit" in line.lower()):
            out.append(
                line[:180]
                + " … [OpenAI JSON сокращён — в биллинге platform.openai.com] … "
                + line[-100:]
            )
        elif len(line) > 800 and ("load_new_all" in line or "Generate failed" in line):
            out.append(line[:360] + " …(обрезано)… " + line[-140:])
        elif len(line) > 800 and ("global limiter" in line or "s2s-api-auth-feedbacks" in line):
            out.append(line[:380] + " …(WB 429, см. dev.wildberries.ru)… " + line[-120:])
        elif len(line) > 800 and ("HTTP 429" in line or "buyer-chat" in line or "wb_chat" in line):
            out.append(line[:320] + " …(обрезано)… " + line[-160:])
        elif len(line) > 1500:
            out.append(line[:500] + " …(длинная строка)… " + line[-200:])
        else:
            out.append(line)
    tail_lines = [redact_secrets_in_text(ln) for ln in out[-450:]]
    return "\n".join(tail_lines)


# ---------- API: log ----------
@app.get("/api/log")
def api_log_tail(db: Database = Depends(get_db), _: UserRow = Depends(require_permission("view_log"))):
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            f.seek(max(0, f.seek(0, 2) - 120 * 1024))
            raw = f.read()
        return {"text": _sanitize_log_for_admin_ui(raw)}
    except FileNotFoundError:
        return {"text": ""}
    except Exception as e:
        log.warning("log tail failed: %s", e)
        return {"text": str(e)}


@app.on_event("startup")
async def _startup_scheduler():
    global _scheduler_task, _telegram_report_task
    # После инициализации uvicorn — ещё раз, чтобы формат логов не затирался дефолтом воркера
    setup_logging(LOG_PATH)
    try:
        db = get_db()
        _bootstrap_admin_if_needed(db)
    except Exception:
        log.exception("startup bootstrap failed")
    if _scheduler_task is None or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(_auto_scheduler_loop())
        log.info("Auto-scheduler started (MSK)")
    if _telegram_report_task is None or _telegram_report_task.done():
        _telegram_report_task = asyncio.create_task(_telegram_report_loop())
        log.info("Telegram report scheduler started (MSK)")
    start_telegram_agent_task()
    log.info("Telegram agent polling started")


@app.on_event("shutdown")
async def _shutdown_scheduler():
    global _scheduler_task, _telegram_report_task, _auto_run_task
    await stop_telegram_agent_task()
    await web_tasks.cancel_all_running()
    if _auto_run_task and not _auto_run_task.done():
        _auto_run_task.cancel()
        try:
            await _auto_run_task
        except Exception:
            pass
    _auto_run_task = None
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except Exception:
            pass
    _scheduler_task = None
    if _telegram_report_task and not _telegram_report_task.done():
        _telegram_report_task.cancel()
        try:
            await _telegram_report_task
        except Exception:
            pass
    _telegram_report_task = None


# ---------- Static SPA & PWA ----------
if STATIC_DIR.exists():

    @app.get("/static/app.js")
    def app_js():
        return FileResponse(
            STATIC_DIR / "app.js",
            media_type="application/javascript",
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )

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
        return FileResponse(
            STATIC_DIR / "index.html",
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )

    @app.get("/sw.js")
    def sw():
        return FileResponse(
            STATIC_DIR / "sw.js",
            media_type="application/javascript",
            headers={"Cache-Control": "no-cache, must-revalidate"},
        )
else:
    @app.get("/")
    def index():
        return {"message": "Static files not found. Create app/web/static/ with index.html"}
