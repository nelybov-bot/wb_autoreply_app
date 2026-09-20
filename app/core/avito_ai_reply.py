"""
Черновики ответов Avito через ИИ → Telegram с inline-кнопками
Отправить / Изменить / Отклонить. В Avito уходит только после подтверждения.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import secrets
import time
from typing import Any, Optional

from app.db import Database, Store

from .avito_client import (
    chat_item_title,
    message_text_preview,
)
from .net import HttpStatusError
from .openai_client import OpenAIClient
from .telegram_notify import (
    escape_tg_html,
    normalize_telegram_chat_id,
    send_telegram_message,
    telegram_answer_callback_query,
    telegram_edit_message_reply_markup,
    telegram_edit_message_text,
)

log = logging.getLogger("avito_ai_reply")

SETTING_ENABLED = "avito_ai_reply_enabled"
SETTING_PROMPT = "avito_ai_reply_prompt"
SETTING_DRAFTS = "avito_ai_draft_map_json"
SETTING_EDIT_PENDING = "avito_ai_edit_pending_json"

_MAX_DRAFTS = 120
_MAX_PENDING = 40

# Задержка перед отправкой покупателю («не моментально»).
_SEND_DELAY_MIN_SEC = 45
_SEND_DELAY_MAX_SEC = 120

DEFAULT_PROMPT = """Ты продавец магазина Avito. Отвечай коротко, по-русски, вежливо, без воды.
Пиши как живой человек: иногда уместны лёгкие смайлики. Цель — помочь и продать товар.

МОЖНО отвечать:
— доставка Avito / сможем ли отправить: да, через доставку Avito и любые доступные клиенту службы доставки;
— характеристики и суть товара — только из названия и описания объявления;
— цена — только цена из объявления;
— скидка: при заказе от 2 товаров автоматическая скидка 10%; других скидок нет.

НЕЛЬЗЯ:
— наличие / остатки / «есть ли в наличии» — не отвечай (верни ровно SKIP);
— выдумывать факты, которых нет в названии, описании или цене;
— обещать другие скидки, сроки или условия, которых нет в данных;
— писать преамбулы вроде «Конечно!» без сути (можно коротко и по делу).

Если данных не хватает или вопрос не по правилам — верни ровно SKIP (одно слово).
Если можно ответить — одна реплика покупателю, без кавычек и без пояснений для оператора.
"""


def ai_reply_enabled(db: Database) -> bool:
    raw = (db.get_setting(SETTING_ENABLED) or "").strip()
    if raw == "":
        return True
    return raw == "1"


def get_prompt(db: Database) -> str:
    raw = (db.get_setting(SETTING_PROMPT) or "").strip()
    return raw or DEFAULT_PROMPT


def chat_item_offer_meta(chat: dict) -> dict[str, str]:
    """Название, цена, описание, url из context чата Avito."""
    out = {"title": "", "price": "", "description": "", "url": "", "item_id": ""}
    if not isinstance(chat, dict):
        return out
    ctx = chat.get("context") if isinstance(chat.get("context"), dict) else {}
    value = ctx.get("value") if isinstance(ctx.get("value"), dict) else {}
    item = chat.get("item") if isinstance(chat.get("item"), dict) else {}
    src = value or item or ctx
    if not isinstance(src, dict):
        src = {}
    out["title"] = str(
        src.get("title") or src.get("name") or chat_item_title(chat) or ""
    ).strip()
    if out["title"] == "—":
        out["title"] = ""
    out["price"] = str(
        src.get("price_string")
        or src.get("priceString")
        or src.get("price")
        or ""
    ).strip()
    desc = (
        src.get("description")
        or src.get("description_html")
        or src.get("descriptionHtml")
        or src.get("body")
        or ""
    )
    out["description"] = str(desc).strip()
    if len(out["description"]) > 3500:
        out["description"] = out["description"][:3497] + "…"
    out["url"] = str(src.get("url") or src.get("item_url") or "").strip()
    iid = src.get("id") or src.get("item_id")
    if iid is not None:
        out["item_id"] = str(iid).strip()
    return out


def _load_json_map(db: Database, key: str) -> dict[str, Any]:
    raw = (db.get_setting(key) or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json_map(db: Database, key: str, data: dict[str, Any], limit: int) -> None:
    if len(data) > limit:
        keys = list(data.keys())[-limit:]
        data = {k: data[k] for k in keys}
    db.set_setting(key, json.dumps(data, ensure_ascii=False))


def _new_draft_id() -> str:
    return secrets.token_hex(4)


def draft_keyboard(draft_id: str) -> dict:
    did = (draft_id or "").strip()
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Отправить", "callback_data": f"avito:s:{did}"},
                {"text": "✏️ Изменить", "callback_data": f"avito:e:{did}"},
                {"text": "❌ Отклонить", "callback_data": f"avito:r:{did}"},
            ]
        ]
    }


def save_draft(
    db: Database,
    *,
    draft_id: str,
    store_id: int,
    avito_chat_id: str,
    draft_text: str,
    item_title: str = "",
    buyer_text: str = "",
    tg_chat_id: Any = None,
    tg_message_id: Any = None,
) -> None:
    data = _load_json_map(db, SETTING_DRAFTS)
    data[draft_id] = {
        "store_id": int(store_id),
        "avito_chat_id": str(avito_chat_id),
        "text": (draft_text or "").strip(),
        "item": (item_title or "")[:120],
        "buyer": (buyer_text or "")[:280],
        "tg_chat_id": str(normalize_telegram_chat_id(tg_chat_id)) if tg_chat_id is not None else "",
        "tg_message_id": int(tg_message_id) if tg_message_id is not None else None,
        "ts": int(time.time()),
    }
    _save_json_map(db, SETTING_DRAFTS, data, _MAX_DRAFTS)


def get_draft(db: Database, draft_id: str) -> Optional[dict[str, Any]]:
    data = _load_json_map(db, SETTING_DRAFTS)
    row = data.get((draft_id or "").strip())
    return row if isinstance(row, dict) else None


def update_draft_text(db: Database, draft_id: str, text: str) -> None:
    data = _load_json_map(db, SETTING_DRAFTS)
    row = data.get(draft_id)
    if not isinstance(row, dict):
        return
    row["text"] = (text or "").strip()
    data[draft_id] = row
    _save_json_map(db, SETTING_DRAFTS, data, _MAX_DRAFTS)


def delete_draft(db: Database, draft_id: str) -> None:
    data = _load_json_map(db, SETTING_DRAFTS)
    data.pop((draft_id or "").strip(), None)
    _save_json_map(db, SETTING_DRAFTS, data, _MAX_DRAFTS)


def _pending_key(tg_chat_id: Any, user_id: Any) -> str:
    return f"{normalize_telegram_chat_id(tg_chat_id)}:{user_id}"


def set_edit_pending(db: Database, *, tg_chat_id: Any, user_id: Any, draft_id: str) -> None:
    data = _load_json_map(db, SETTING_EDIT_PENDING)
    data[_pending_key(tg_chat_id, user_id)] = {
        "draft_id": draft_id,
        "ts": int(time.time()),
    }
    _save_json_map(db, SETTING_EDIT_PENDING, data, _MAX_PENDING)


def pop_edit_pending(db: Database, *, tg_chat_id: Any, user_id: Any) -> Optional[str]:
    data = _load_json_map(db, SETTING_EDIT_PENDING)
    key = _pending_key(tg_chat_id, user_id)
    row = data.pop(key, None)
    _save_json_map(db, SETTING_EDIT_PENDING, data, _MAX_PENDING)
    if isinstance(row, dict):
        did = str(row.get("draft_id") or "").strip()
        return did or None
    return None


def peek_edit_pending(db: Database, *, tg_chat_id: Any, user_id: Any) -> Optional[str]:
    data = _load_json_map(db, SETTING_EDIT_PENDING)
    row = data.get(_pending_key(tg_chat_id, user_id))
    if isinstance(row, dict):
        did = str(row.get("draft_id") or "").strip()
        return did or None
    return None


async def generate_draft_reply(
    db: Database,
    *,
    buyer_text: str,
    item_meta: dict[str, str],
) -> Optional[str]:
    """None = SKIP / нечего слать."""
    body = (buyer_text or "").strip()
    if not body or body in ("[изображение]", "[голосовое]", "—"):
        return None
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        log.info("avito ai draft skip: no openai_key")
        return None
    system = get_prompt(db)
    user = (
        f"Объявление:\n"
        f"Название: {item_meta.get('title') or '—'}\n"
        f"Цена: {item_meta.get('price') or '—'}\n"
        f"Описание: {item_meta.get('description') or '—'}\n\n"
        f"Сообщение покупателя:\n{body}\n"
    )
    try:
        client = OpenAIClient(key)
        raw = (await client.generate(system, user) or "").strip()
    except HttpStatusError as e:
        log.warning("avito ai draft openai HTTP %s: %s", e.status, str(e.body)[:160])
        return None
    except Exception:
        log.exception("avito ai draft openai failed")
        return None
    if not raw:
        return None
    # Убрать обёртки-кавычки.
    if (raw.startswith("«") and raw.endswith("»")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1].strip()
    low = raw.lower().strip()
    if low == "skip" or low.startswith("skip"):
        return None
    if len(raw) > 1000:
        raw = raw[:997] + "…"
    return raw


def format_draft_message(
    *,
    store_name: str,
    item_title: str,
    buyer_text: str,
    draft_text: str,
) -> str:
    store = escape_tg_html(store_name or "Avito")
    item = escape_tg_html(item_title or "—")
    buyer = escape_tg_html((buyer_text or "—")[:280])
    draft = escape_tg_html(draft_text or "—")
    return "\n".join(
        [
            "🤖 <b>Черновик ответа · Avito</b>",
            f"🏪 <b>{store}</b>",
            f"📦 {item}",
            "",
            f"👤 «{buyer}»",
            "",
            f"💬 <b>{draft}</b>",
            "",
            "<i>Нажмите кнопку под сообщением</i>",
        ]
    )


async def maybe_send_ai_draft(
    db: Database,
    *,
    bot_token: str,
    tg_chat_id: str,
    store: Store,
    chat: dict,
    msg: dict,
    avito_chat_id: str,
) -> bool:
    """
    После алерта о входящем: сгенерировать черновик и отправить в TG с кнопками.
    Возвращает True, если черновик ушёл в Telegram.
    """
    if not ai_reply_enabled(db):
        return False
    mtype = str(msg.get("type") or "").strip().lower()
    if mtype not in ("text", ""):
        return False
    buyer_text = message_text_preview(msg) or ""
    if not buyer_text or buyer_text.startswith("["):
        return False
    meta = chat_item_offer_meta(chat)
    draft = await generate_draft_reply(db, buyer_text=buyer_text, item_meta=meta)
    if not draft:
        return False
    draft_id = _new_draft_id()
    text = format_draft_message(
        store_name=store.name or f"#{store.id}",
        item_title=meta.get("title") or chat_item_title(chat),
        buyer_text=buyer_text,
        draft_text=draft,
    )
    ok, err, tg_mid = await send_telegram_message(
        bot_token,
        tg_chat_id,
        text,
        parse_mode="HTML",
        reply_markup=draft_keyboard(draft_id),
        db=db,
    )
    if not ok:
        log.warning("avito ai draft tg fail store=%s: %s", store.id, err)
        return False
    save_draft(
        db,
        draft_id=draft_id,
        store_id=int(store.id),
        avito_chat_id=avito_chat_id,
        draft_text=draft,
        item_title=meta.get("title") or chat_item_title(chat),
        buyer_text=buyer_text,
        tg_chat_id=tg_chat_id,
        tg_message_id=tg_mid,
    )
    return True


async def _clear_buttons(token: str, chat_id: Any, message_id: Any) -> None:
    if message_id is None:
        return
    try:
        await telegram_edit_message_reply_markup(
            token,
            chat_id,
            int(message_id),
            reply_markup={"inline_keyboard": []},
        )
    except Exception:
        pass


async def _delayed_avito_send(
    db: Database,
    *,
    bot_token: str,
    tg_chat_id: Any,
    draft_id: str,
    delay_sec: int,
) -> None:
    await asyncio.sleep(max(1, int(delay_sec)))
    row = get_draft(db, draft_id)
    if not row:
        await send_telegram_message(
            bot_token,
            tg_chat_id,
            "⚠️ Черновик уже неактуален — отправка отменена.",
            parse_mode="HTML",
            db=db,
        )
        return
    text = str(row.get("text") or "").strip()
    store_id = int(row.get("store_id") or 0)
    avito_chat = str(row.get("avito_chat_id") or "").strip()
    item = escape_tg_html(str(row.get("item") or "чат Avito"))
    from .avito_notify import send_avito_reply_from_telegram

    ok, err = await send_avito_reply_from_telegram(
        db,
        store_id=store_id,
        avito_chat_id=avito_chat,
        text=text,
    )
    delete_draft(db, draft_id)
    if ok:
        await send_telegram_message(
            bot_token,
            tg_chat_id,
            f"✅ <b>Отправлено в Avito</b>\n📦 {item}\n💬 {escape_tg_html(text)}",
            parse_mode="HTML",
            db=db,
        )
    else:
        await send_telegram_message(
            bot_token,
            tg_chat_id,
            f"❌ Не удалось отправить в Avito: {escape_tg_html(err)}",
            parse_mode="HTML",
            db=db,
        )


async def handle_draft_callback(
    db: Database,
    *,
    bot_token: str,
    callback: dict,
) -> bool:
    """Обработка avito:s|e|r:id. True если callback наш."""
    data = str(callback.get("data") or "")
    if not data.startswith("avito:"):
        return False
    parts = data.split(":", 2)
    if len(parts) != 3:
        return False
    action, draft_id = parts[1], parts[2]
    cb_id = str(callback.get("id") or "")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    from_user = callback.get("from") or {}
    user_id = from_user.get("id")
    msg_id = message.get("message_id")

    row = get_draft(db, draft_id)
    if not row:
        await telegram_answer_callback_query(bot_token, cb_id, text="Черновик устарел", show_alert=True)
        await _clear_buttons(bot_token, chat_id, msg_id)
        return True

    if action == "r":
        delete_draft(db, draft_id)
        await telegram_answer_callback_query(bot_token, cb_id, text="Отклонено")
        await _clear_buttons(bot_token, chat_id, msg_id)
        try:
            await telegram_edit_message_text(
                bot_token,
                chat_id,
                int(msg_id),
                (message.get("text") or "") + "\n\n<i>❌ Отклонено</i>",
                parse_mode="HTML",
            )
        except Exception:
            await send_telegram_message(
                bot_token, chat_id, "❌ Черновик отклонён", parse_mode="HTML", db=db
            )
        return True

    if action == "e":
        set_edit_pending(db, tg_chat_id=chat_id, user_id=user_id, draft_id=draft_id)
        await telegram_answer_callback_query(bot_token, cb_id, text="Жду новый текст")
        await send_telegram_message(
            bot_token,
            chat_id,
            "✏️ Пришлите <b>новым сообщением</b> текст ответа покупателю.\n"
            "После этого уйдёт в Avito с небольшой задержкой (не сразу).",
            parse_mode="HTML",
            db=db,
        )
        return True

    if action == "s":
        delay = random.randint(_SEND_DELAY_MIN_SEC, _SEND_DELAY_MAX_SEC)
        await telegram_answer_callback_query(bot_token, cb_id, text=f"Отправка через ~{delay} с")
        await _clear_buttons(bot_token, chat_id, msg_id)
        await send_telegram_message(
            bot_token,
            chat_id,
            f"⏳ Отправлю в Avito через ~{delay} сек (чтобы не выглядело мгновенным ботом)…",
            parse_mode="HTML",
            db=db,
        )
        asyncio.create_task(
            _delayed_avito_send(
                db,
                bot_token=bot_token,
                tg_chat_id=chat_id,
                draft_id=draft_id,
                delay_sec=delay,
            )
        )
        return True

    await telegram_answer_callback_query(bot_token, cb_id, text="Неизвестная кнопка")
    return True


async def try_handle_edit_followup(
    db: Database,
    *,
    bot_token: str,
    message: dict,
) -> bool:
    """Если ждём текст после «Изменить» — принять и запланировать отправку."""
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    from_user = message.get("from") or {}
    user_id = from_user.get("id")
    if chat_id is None or user_id is None:
        return False
    draft_id = peek_edit_pending(db, tg_chat_id=chat_id, user_id=user_id)
    if not draft_id:
        return False
    text = (message.get("text") or message.get("caption") or "").strip()
    if not text:
        await send_telegram_message(
            bot_token,
            chat_id,
            "Нужен текстовый ответ. Пришлите текст одним сообщением.",
            parse_mode="HTML",
            db=db,
        )
        return True
    pop_edit_pending(db, tg_chat_id=chat_id, user_id=user_id)
    row = get_draft(db, draft_id)
    if not row:
        await send_telegram_message(
            bot_token,
            chat_id,
            "⚠️ Черновик устарел. Дождитесь нового сообщения покупателя.",
            parse_mode="HTML",
            db=db,
        )
        return True
    update_draft_text(db, draft_id, text)
    delay = random.randint(_SEND_DELAY_MIN_SEC, _SEND_DELAY_MAX_SEC)
    await send_telegram_message(
        bot_token,
        chat_id,
        f"⏳ Принято. Отправлю в Avito через ~{delay} сек…\n💬 {escape_tg_html(text)}",
        parse_mode="HTML",
        db=db,
    )
    # Убрать кнопки со старого черновика.
    mid = row.get("tg_message_id")
    tg_c = row.get("tg_chat_id") or chat_id
    await _clear_buttons(bot_token, tg_c, mid)
    asyncio.create_task(
        _delayed_avito_send(
            db,
            bot_token=bot_token,
            tg_chat_id=chat_id,
            draft_id=draft_id,
            delay_sec=delay,
        )
    )
    return True
