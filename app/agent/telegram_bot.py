"""Telegram-бот для управления AI-агентом MarketAI (long polling)."""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Callable, Optional

from app.agent.formatting import escape_tg, format_agent_telegram_reply, strip_leaked_json
from app.agent.orchestrator import handle_agent_message
from app.agent.session import AgentSession, clear_session, get_or_create_session
from app.agent.tools import AgentContext
from app.core.telegram_notify import (
    normalize_telegram_bot_token,
    normalize_telegram_chat_id,
    send_telegram_message,
    telegram_answer_callback_query,
    telegram_edit_message_reply_markup,
    telegram_get_updates,
    telegram_send_chat_action,
    verify_telegram_bot_token,
)
from app.db import Database, UserRow

log = logging.getLogger("agent.telegram")

SETTING_AGENT_ENABLED = "telegram_agent_enabled"
SETTING_AGENT_CHAT_ID = "telegram_agent_chat_id"
SETTING_AGENT_USER_ID = "telegram_agent_user_id"
SETTING_AGENT_UPDATE_OFFSET = "telegram_agent_update_offset"

_HELP_TEXT = """🤖 <b><u>MarketAI</u></b>
<i>Управление маркетплейсами через Telegram</i>

<b>Примеры запросов</b>
<blockquote><i>• Покажи магазины
• Статистика очереди
• Новые отзывы без ответа
• Ответь на отзывы (полный цикл)
• Проверь чаты покупателей
• Проверь автоакции Ozon</i></blockquote>

<b>Команды</b>
• <code>/new</code> — <i>новый диалог</i>
• <code>/help</code> — <i>эта справка</i>
• <code>/id</code> — <i>ваш Telegram user_id</i>

<b>Опасные действия</b> — подтверждение кнопкой или «да».

<b>Групповой чат:</b> <i>@упоминание бота, ответ на его сообщение или</i> <code>/help</code>."""

_CONFIRM_KEYBOARD = {
    "inline_keyboard": [
        [
            {"text": "✅ Подтвердить", "callback_data": "agent:confirm"},
            {"text": "❌ Отмена", "callback_data": "agent:cancel"},
        ]
    ]
}

_context_factory: Optional[Callable[[Database], Optional[AgentContext]]] = None
_get_db_fn: Optional[Callable[[], Database]] = None
_loop_task: Optional[asyncio.Task] = None
_bot_identity: dict[str, object] = {"username": "", "id": 0, "fetched_at": 0.0}


def _is_group_chat(chat: dict) -> bool:
    return str(chat.get("type") or "private") in ("group", "supergroup")


async def _bot_identity_for(token: str) -> tuple[str, int]:
    """Кэш username и id бота (getMe), TTL 1 час."""
    now = time.time()
    cached_at = float(_bot_identity.get("fetched_at") or 0)
    if _bot_identity.get("username") and now - cached_at < 3600:
        return str(_bot_identity["username"]), int(_bot_identity["id"] or 0)
    ok, _, me = await verify_telegram_bot_token(token)
    if not ok or not me:
        return "", 0
    username = str(me.get("username") or "").strip().lower()
    bot_id = int(me.get("id") or 0)
    _bot_identity.update({"username": username, "id": bot_id, "fetched_at": now})
    return username, bot_id


def _is_reply_to_bot(message: dict, bot_id: int) -> bool:
    if not bot_id:
        return False
    reply = message.get("reply_to_message") or {}
    frm = reply.get("from") or {}
    return bool(frm.get("is_bot")) and int(frm.get("id") or 0) == bot_id


def _entities_mention_bot(message: dict, bot_username: str, bot_id: int) -> bool:
    if not bot_username:
        return False
    text = message.get("text") or ""
    uname = bot_username.lower().lstrip("@")
    for ent in message.get("entities") or []:
        if not isinstance(ent, dict):
            continue
        etype = str(ent.get("type") or "")
        if etype == "mention":
            offset = int(ent.get("offset") or 0)
            length = int(ent.get("length") or 0)
            chunk = text[offset : offset + length].lower().lstrip("@")
            if chunk == uname:
                return True
        if etype == "text_mention":
            user = ent.get("user") or {}
            if user.get("is_bot") and int(user.get("id") or 0) == bot_id:
                return True
    return False


def _strip_bot_address(text: str, bot_username: str) -> str:
    if not text or not bot_username:
        return (text or "").strip()
    uname = re.escape(bot_username.lstrip("@"))
    s = text.strip()
    s = re.sub(rf"@{uname}\b", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(rf"^(/\w+)@{uname}\b", r"\1", s, flags=re.IGNORECASE).strip()
    return s


def _command_for_this_bot(text: str, bot_username: str) -> bool:
    """Команда /help или /help@botname в группе."""
    part = (text or "").strip().split(maxsplit=1)[0].lower()
    if not part.startswith("/"):
        return False
    if "@" not in part:
        return part in ("/start", "/help", "/new", "/id")
    if not bot_username:
        return False
    return part.endswith(f"@{bot_username.lstrip('@')}")


async def _message_addresses_bot(message: dict, token: str) -> tuple[bool, str]:
    """
    В личке — всегда True.
    В группе — только @бот, ответ на бота или команда боту.
    Возвращает (обращение к боту, текст без @упоминания).
    """
    chat = message.get("chat") or {}
    text = (message.get("text") or "").strip()
    if not text:
        return False, ""
    if not _is_group_chat(chat):
        return True, text

    bot_username, bot_id = await _bot_identity_for(token)
    if _is_reply_to_bot(message, bot_id):
        return True, text
    if _entities_mention_bot(message, bot_username, bot_id):
        return True, _strip_bot_address(text, bot_username)
    if bot_username and re.search(rf"@{re.escape(bot_username)}\b", text, re.IGNORECASE):
        return True, _strip_bot_address(text, bot_username)
    if _command_for_this_bot(text, bot_username):
        return True, text
    return False, ""


def configure_telegram_agent(
    *,
    get_db: Callable[[], Database],
    context_factory: Callable[[Database], Optional[AgentContext]],
) -> None:
    global _get_db_fn, _context_factory
    _get_db_fn = get_db
    _context_factory = context_factory


def telegram_agent_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_AGENT_ENABLED) or "").strip() == "1"


def allowed_agent_chat_ids(db: Database) -> set[str]:
    ids: set[str] = set()
    raw = (db.get_setting(SETTING_AGENT_CHAT_ID) or "").strip()
    if raw:
        for part in raw.replace(";", ",").split(","):
            p = part.strip()
            if not p:
                continue
            cid = normalize_telegram_chat_id(p)
            if cid != "":
                ids.add(str(cid))
    default = (db.get_setting("telegram_chat_id") or "").strip()
    if default:
        cid = normalize_telegram_chat_id(default)
        if cid != "":
            ids.add(str(cid))
    return ids


def allowed_agent_user_ids(db: Database) -> set[str]:
    ids: set[str] = set()
    raw = (db.get_setting(SETTING_AGENT_USER_ID) or "").strip()
    if not raw:
        return ids
    for part in raw.replace(";", ",").split(","):
        p = part.strip()
        if not p:
            continue
        if p.lstrip("-").isdigit():
            ids.add(str(int(p)))
    return ids


def _request_allowed(db: Database, chat_id: object, user_id: object) -> bool:
    if not telegram_agent_enabled(db):
        return False
    allowed_users = allowed_agent_user_ids(db)
    allowed_chats = allowed_agent_chat_ids(db)
    if not allowed_users and not allowed_chats:
        return False
    if allowed_users:
        if user_id is None:
            return False
        try:
            if str(int(user_id)) not in allowed_users:
                return False
        except (TypeError, ValueError):
            return False
    if allowed_chats:
        return str(normalize_telegram_chat_id(chat_id)) in allowed_chats
    return True


def _first_admin(db: Database) -> Optional[UserRow]:
    for u in db.list_users():
        if u.role == "admin":
            return u
    return None


def _session_for_telegram(db: Database, *, chat_id: object, user_id: object) -> Optional[AgentSession]:
    admin = _first_admin(db)
    if not admin:
        return None
    if user_id is not None:
        sid = f"tg:u:{int(user_id)}"
    else:
        sid = f"tg:{normalize_telegram_chat_id(chat_id)}"
    return get_or_create_session(user_id=admin.id, username=admin.username, session_id=sid)


def _load_update_offset(db: Database) -> int:
    raw = (db.get_setting(SETTING_AGENT_UPDATE_OFFSET) or "").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _save_update_offset(db: Database, update_id: int) -> None:
    if update_id <= 0:
        return
    db.set_setting(SETTING_AGENT_UPDATE_OFFSET, str(int(update_id)))


async def _reply_agent(
    *,
    token: str,
    chat_id: object,
    text: str,
    needs_confirm: bool = False,
    html: Optional[str] = None,
    db: Database,
) -> None:
    body = strip_leaked_json((text or "").strip()) or "—"
    html_body = html if html is not None else format_agent_telegram_reply(body, needs_confirm=needs_confirm)
    markup = _CONFIRM_KEYBOARD if needs_confirm else None
    ok, err = await send_telegram_message(
        token,
        chat_id,
        html_body,
        parse_mode="HTML",
        reply_markup=markup,
        db=db,
    )
    if not ok:
        ok2, err2 = await send_telegram_message(
            token,
            chat_id,
            body,
            parse_mode=None,
            reply_markup=markup,
            db=db,
        )
        if not ok2:
            log.warning("agent telegram reply failed chat_id=%s: %s", chat_id, (err2 or err)[:200])


async def _process_agent_input(
    *,
    db: Database,
    chat_id: object,
    user_id: object,
    text: str,
    force_confirm: bool = False,
) -> None:
    if not _context_factory:
        return
    ctx = _context_factory(db)
    if not ctx:
        token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
        await _reply_agent(
            token=token,
            chat_id=chat_id,
            text="В системе нет пользователя admin — создайте его при первом запуске.",
            db=db,
        )
        return
    session = _session_for_telegram(db, chat_id=chat_id, user_id=user_id)
    if not session:
        return
    openai_key = (db.get_setting("openai_key") or "").strip()
    token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
    if force_confirm and session.pending and (
        session.pending.tool.startswith("pipeline_")
        or session.pending.tool == "remove_ozon_promotions"
    ):
        await _reply_agent(
            token=token,
            chat_id=chat_id,
            text="⏳ Выполняю… Это может занять несколько минут.",
            db=db,
        )
    await telegram_send_chat_action(token, chat_id, "typing")
    out = await handle_agent_message(
        session=session,
        user_message=text,
        ctx=ctx,
        openai_key=openai_key,
        force_confirm=force_confirm,
    )
    await _reply_agent(
        token=token,
        chat_id=chat_id,
        text=out.get("reply") or "",
        needs_confirm=bool(out.get("needs_confirm")),
        db=db,
    )


async def _handle_message(db: Database, message: dict) -> None:
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    from_user = message.get("from") or {}
    user_id = from_user.get("id")
    if chat_id is None:
        return

    token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
    if not token:
        return

    text = (message.get("text") or "").strip()
    low = text.lower()

    # /id — всегда доступен (даже до проверки чата), в группе только при обращении к боту
    if low == "/id" or low.startswith("/id@"):
        if _is_group_chat(chat):
            addressed, _ = await _message_addresses_bot(message, token)
            if not addressed:
                return
        uid = user_id if user_id is not None else "—"
        await _reply_agent(
            token=token,
            chat_id=chat_id,
            text="",
            html=f"🆔 Ваш Telegram <b>user_id</b>: <code>{escape_tg(str(uid))}</code>",
            db=db,
        )
        return

    if not _request_allowed(db, chat_id, user_id):
        return

    addressed, agent_text = await _message_addresses_bot(message, token)
    if not addressed:
        return

    text = agent_text
    low = text.lower()

    if not text:
        await _reply_agent(
            token=token,
            chat_id=chat_id,
            text="Отправьте текстовое сообщение или команду /help",
            db=db,
        )
        return

    if low in ("/start", "/help") or low.startswith("/help@"):
        await send_telegram_message(token, chat_id, _HELP_TEXT, parse_mode="HTML", db=db)
        return
    if low == "/new" or low.startswith("/new@"):
        admin = _first_admin(db)
        if admin:
            session = _session_for_telegram(db, chat_id=chat_id, user_id=user_id)
            if session:
                clear_session(session.session_id, user_id=admin.id)
        await _reply_agent(token=token, chat_id=chat_id, text="✨ Новый диалог. Чем помочь?", db=db)
        return

    await _process_agent_input(db=db, chat_id=chat_id, user_id=user_id, text=text)


async def _handle_callback(db: Database, callback: dict) -> None:
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    from_user = callback.get("from") or {}
    user_id = from_user.get("id")
    if chat_id is None:
        return
    if not _request_allowed(db, chat_id, user_id):
        await telegram_answer_callback_query(
            normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or ""),
            str(callback.get("id") or ""),
            text="Нет доступа",
            show_alert=True,
        )
        return

    token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
    data = str(callback.get("data") or "")
    cb_id = str(callback.get("id") or "")

    if data == "agent:confirm":
        await telegram_answer_callback_query(token, cb_id, text="Выполняю…")
        msg_id = message.get("message_id")
        if msg_id is not None:
            await telegram_edit_message_reply_markup(token, chat_id, int(msg_id), reply_markup={"inline_keyboard": []})
        await _process_agent_input(db=db, chat_id=chat_id, user_id=user_id, text="да", force_confirm=True)
        return
    if data == "agent:cancel":
        await telegram_answer_callback_query(token, cb_id, text="Отменено")
        msg_id = message.get("message_id")
        if msg_id is not None:
            await telegram_edit_message_reply_markup(token, chat_id, int(msg_id), reply_markup={"inline_keyboard": []})
        await _process_agent_input(db=db, chat_id=chat_id, user_id=user_id, text="отмена")
        return

    await telegram_answer_callback_query(token, cb_id)


async def _process_update(db: Database, update: dict) -> None:
    if "callback_query" in update:
        await _handle_callback(db, update["callback_query"])
        return
    message = update.get("message")
    if isinstance(message, dict):
        await _handle_message(db, message)


async def telegram_agent_loop() -> None:
    """Фоновый long polling Telegram Bot API."""
    log.info("Telegram agent loop started")
    idle_sleep = 5.0
    while True:
        try:
            if not _get_db_fn:
                await asyncio.sleep(idle_sleep)
                continue
            db = _get_db_fn()
            if not telegram_agent_enabled(db):
                await asyncio.sleep(idle_sleep)
                continue
            token = normalize_telegram_bot_token(db.get_setting("telegram_bot_token") or "")
            if not token:
                await asyncio.sleep(idle_sleep)
                continue

            offset = _load_update_offset(db)
            next_offset = offset + 1 if offset else None
            ok, err, updates = await telegram_get_updates(
                token,
                offset=next_offset,
                timeout=25,
                allowed_updates=["message", "callback_query"],
            )
            if not ok:
                if err and err != "timeout" and "409" not in err:
                    log.warning("telegram agent getUpdates: %s", err[:200])
                await asyncio.sleep(idle_sleep)
                continue

            for upd in updates:
                uid = int(upd.get("update_id") or 0)
                if uid > offset:
                    offset = uid
                try:
                    await _process_update(db, upd)
                except Exception:
                    log.exception("telegram agent update failed update_id=%s", uid)
                _save_update_offset(db, offset)

            if not updates:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("telegram agent loop error")
            await asyncio.sleep(idle_sleep)


def start_telegram_agent_task() -> asyncio.Task:
    global _loop_task
    if _loop_task is not None and not _loop_task.done():
        return _loop_task
    _loop_task = asyncio.create_task(telegram_agent_loop())
    return _loop_task


async def stop_telegram_agent_task() -> None:
    global _loop_task
    if _loop_task is None or _loop_task.done():
        _loop_task = None
        return
    _loop_task.cancel()
    try:
        await _loop_task
    except asyncio.CancelledError:
        pass
    _loop_task = None
