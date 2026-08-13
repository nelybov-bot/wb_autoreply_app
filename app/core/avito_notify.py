"""
Avito → Telegram: новые заказы и входящие сообщения.

Опрос активных магазинов marketplace=avito.
Первый прогон только засевает seen-id (без спама).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

from app.db import Database, Store

from .avito_client import (
    AvitoClient,
    chat_buyer_name,
    chat_id_of,
    chat_item_title,
    chat_last_message,
    message_id_of,
    message_text_preview,
    order_display_id,
    order_item_titles,
    order_total_rub,
)
from .net import HttpStatusError
from .telegram_notify import escape_tg_html, normalize_telegram_chat_id, send_telegram_message

log = logging.getLogger("avito_notify")

SETTING_ENABLED = "avito_notify_enabled"
SETTING_ORDERS = "avito_orders_notify_enabled"
SETTING_MESSAGES = "avito_messages_notify_enabled"
SETTING_CHAT_ID = "avito_notify_telegram_chat_id"
SETTING_SEEN = "avito_notify_seen_json"
SETTING_LAST_CHECK = "avito_notify_last_check"
SETTING_REPLY_MAP = "avito_tg_reply_map_json"

# Новые заказы, требующие внимания продавца.
ORDER_WATCH_STATUSES = ("on_confirmation", "ready_to_ship")

# Окно выборки заказов (сек) — 14 дней.
ORDERS_LOOKBACK_SEC = 14 * 24 * 3600

# Сколько id держим в seen-словарях на магазин.
_MAX_SEEN_ORDERS = 400
_MAX_SEEN_MESSAGES = 600
_MAX_REPLY_MAP = 250

POLL_SECONDS = 90


def notify_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_ENABLED) or "0").strip() == "1"


def orders_notify_enabled(db: Database) -> bool:
    # По умолчанию вкл., если общий флаг включён.
    raw = (db.get_setting(SETTING_ORDERS) or "").strip()
    if raw == "":
        return True
    return raw == "1"


def messages_notify_enabled(db: Database) -> bool:
    raw = (db.get_setting(SETTING_MESSAGES) or "").strip()
    if raw == "":
        return True
    return raw == "1"


def _load_seen(db: Database) -> dict[str, Any]:
    raw = (db.get_setting(SETTING_SEEN) or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_seen(db: Database, data: dict[str, Any]) -> None:
    db.set_setting(SETTING_SEEN, json.dumps(data, ensure_ascii=False))


def _store_bucket(seen: dict[str, Any], store_id: int) -> dict[str, Any]:
    key = str(int(store_id))
    bucket = seen.get(key)
    if not isinstance(bucket, dict):
        bucket = {"orders": [], "messages": {}, "seeded": False}
        seen[key] = bucket
    if not isinstance(bucket.get("orders"), list):
        bucket["orders"] = []
    if not isinstance(bucket.get("messages"), dict):
        bucket["messages"] = {}
    return bucket


def _trim_list(ids: list, limit: int) -> list:
    if len(ids) <= limit:
        return ids
    return ids[-limit:]


def _trim_dict(d: dict, limit: int) -> dict:
    if len(d) <= limit:
        return d
    # Оставляем последние по порядку вставки (Py3.7+).
    keys = list(d.keys())[-limit:]
    return {k: d[k] for k in keys}


def _avito_stores(db: Database) -> list[Store]:
    return [
        s
        for s in db.list_stores()
        if (s.marketplace or "").strip().lower() == "avito" and bool(s.active)
    ]


def _client_for_store(store: Store) -> AvitoClient:
    return AvitoClient(
        client_id=store.client_id or "",
        client_secret=store.api_key or "",
        user_id=store.business_id,
    )


async def _ensure_user_id(db: Database, store: Store, client: AvitoClient) -> int:
    if store.business_id:
        return int(store.business_id)
    uid = await client.resolve_user_id()
    try:
        db.update_store(
            int(store.id),
            store.name,
            store.api_key,
            bool(store.active),
            business_id=int(uid),
            client_id=store.client_id or "",
        )
        log.info("avito store=%s cached user_id=%s", store.id, uid)
    except Exception:
        log.exception("avito store=%s failed to cache user_id", store.id)
    return int(uid)


_ORDER_STATUS_RU = {
    "on_confirmation": "ожидает подтверждения",
    "ready_to_ship": "ждёт отправки",
    "in_transit": "в пути",
    "canceled": "отменён",
    "cancelled": "отменён",
    "delivered": "доставлен",
    "on_return": "на возврате",
    "in_dispute": "открыт спор",
    "closed": "закрыт",
    "confirming": "ожидает подтверждения",
}


def _order_status_ru(raw: str) -> str:
    key = (raw or "").strip().lower()
    return _ORDER_STATUS_RU.get(key) or (raw.strip() if raw else "—")


def _fmt_created_short(raw: str) -> str:
    """2026-08-13T15:40:00Z → 13.08 15:40 (как есть, без TZ-магии)."""
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        # RFC3339 / ISO
        body = s.replace("Z", "+00:00")
        # обрежем микросекунды если мешают
        if "." in body and "+" in body[body.find("T"):]:
            pass
        from datetime import datetime

        dt = datetime.fromisoformat(body)
        return dt.strftime("%d.%m %H:%M")
    except Exception:
        return s[:16].replace("T", " ")


def format_order_message(store_name: str, order: dict) -> str:
    oid = escape_tg_html(order_display_id(order) or "?")
    status = escape_tg_html(_order_status_ru(str(order.get("status") or "")))
    titles = escape_tg_html(order_item_titles(order))
    total = escape_tg_html(order_total_rub(order))
    created = _fmt_created_short(str(order.get("createdAt") or ""))
    store = escape_tg_html(store_name or "Avito")
    lines = [
        "🛒 <b>Новый заказ · Avito</b>",
        f"🏪 <b>{store}</b>",
        "",
        f"📦 {titles}",
        f"💰 <b>{total}</b>",
        f"📌 {status}",
    ]
    if created:
        lines.append(f"🕒 {escape_tg_html(created)}")
    lines.append(f"🧾 № <code>{oid}</code>")
    return "\n".join(lines)


def format_chat_message(
    store_name: str,
    chat: dict,
    msg: dict,
    *,
    our_user_id: Optional[int] = None,
) -> str:
    store = escape_tg_html(store_name or "Avito")
    item = escape_tg_html(chat_item_title(chat))
    preview_raw = message_text_preview(msg) or "—"
    preview = escape_tg_html(preview_raw)
    author = msg.get("author_id")
    if author is None:
        author = msg.get("authorId")
    buyer = chat_buyer_name(chat, author_id=author, our_user_id=our_user_id)
    buyer_s = escape_tg_html(buyer) if buyer else "Покупатель"

    mtype = str(msg.get("type") or "").strip().lower()
    if mtype == "image":
        body = "🖼 <i>фото</i>"
    elif mtype == "voice":
        body = "🎤 <i>голосовое</i>"
    elif mtype == "call":
        body = "📞 <i>звонок</i>"
    elif mtype in ("text", "") or preview_raw not in ("—",):
        body = f"💬 «<b>{preview}</b>»"
    else:
        body = f"💬 {preview}"

    lines = [
        "✉️ <b>Новое сообщение · Avito</b>",
        f"🏪 <b>{store}</b>",
        "",
        f"📦 {item}",
        f"👤 {buyer_s}",
        "",
        body,
        "",
        "<i>↩️ Ответьте на это сообщение — уйдёт покупателю в Avito</i>",
    ]
    return "\n".join(lines)


def _reply_map_key(tg_chat_id: Any, tg_message_id: Any) -> str:
    return f"{normalize_telegram_chat_id(tg_chat_id)}:{int(tg_message_id)}"


def _load_reply_map(db: Database) -> dict[str, Any]:
    raw = (db.get_setting(SETTING_REPLY_MAP) or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_reply_map(db: Database, data: dict[str, Any]) -> None:
    # Оставляем последние N ключей (порядок вставки).
    if len(data) > _MAX_REPLY_MAP:
        keys = list(data.keys())[-_MAX_REPLY_MAP:]
        data = {k: data[k] for k in keys}
    db.set_setting(SETTING_REPLY_MAP, json.dumps(data, ensure_ascii=False))


def remember_tg_reply_target(
    db: Database,
    *,
    tg_chat_id: Any,
    tg_message_id: Any,
    store_id: int,
    avito_chat_id: str,
    item_title: str = "",
) -> None:
    if tg_message_id is None or not avito_chat_id:
        return
    try:
        mid = int(tg_message_id)
    except (TypeError, ValueError):
        return
    data = _load_reply_map(db)
    data[_reply_map_key(tg_chat_id, mid)] = {
        "store_id": int(store_id),
        "avito_chat_id": str(avito_chat_id),
        "item": (item_title or "")[:120],
        "ts": int(time.time()),
    }
    _save_reply_map(db, data)


def lookup_tg_reply_target(
    db: Database,
    *,
    tg_chat_id: Any,
    tg_message_id: Any,
) -> Optional[dict[str, Any]]:
    if tg_message_id is None:
        return None
    try:
        mid = int(tg_message_id)
    except (TypeError, ValueError):
        return None
    data = _load_reply_map(db)
    row = data.get(_reply_map_key(tg_chat_id, mid))
    return row if isinstance(row, dict) else None


async def _send(
    db: Database,
    bot_token: str,
    chat_id: str,
    text: str,
) -> tuple[bool, str, Optional[int]]:
    return await send_telegram_message(
        bot_token,
        chat_id,
        text,
        parse_mode="HTML",
        db=db,
    )


async def send_avito_reply_from_telegram(
    db: Database,
    *,
    store_id: int,
    avito_chat_id: str,
    text: str,
) -> tuple[bool, str]:
    """Отправка текста в чат Avito от имени магазина."""
    body = (text or "").strip()
    if not body:
        return False, "пустой текст"
    stores = [s for s in db.list_stores() if int(s.id) == int(store_id)]
    if not stores:
        return False, "магазин Avito не найден"
    store = stores[0]
    if (store.marketplace or "").strip().lower() != "avito":
        return False, "магазин не Avito"
    if not store.active:
        return False, "магазин выключен"
    client = _client_for_store(store)
    try:
        await _ensure_user_id(db, store, client)
        await client.send_text_message(
            avito_chat_id,
            body,
            user_id=store.business_id,
        )
        return True, ""
    except HttpStatusError as e:
        hint = ""
        if e.status == 402:
            hint = " (нужна подписка Avito с API мессенджера)"
        return False, f"Avito HTTP {e.status}{hint}: {str(e.body)[:160]}"
    except Exception as e:
        log.exception("avito reply send failed store=%s chat=%s", store_id, avito_chat_id)
        return False, str(e)[:180]


async def poll_store_orders(
    db: Database,
    store: Store,
    *,
    bot_token: str,
    chat_id: str,
    seen: dict[str, Any],
    notify: bool,
) -> dict[str, Any]:
    bucket = _store_bucket(seen, int(store.id))
    known = set(str(x) for x in (bucket.get("orders") or []))
    client = _client_for_store(store)
    date_from = int(time.time()) - ORDERS_LOOKBACK_SEC
    try:
        orders = await client.list_orders_all(
            statuses=list(ORDER_WATCH_STATUSES),
            date_from=date_from,
            max_pages=5,
        )
    except HttpStatusError as e:
        return {
            "store_id": int(store.id),
            "ok": False,
            "error": f"заказы HTTP {e.status}: {str(e.body)[:180]}",
            "new": 0,
        }
    except Exception as e:
        log.exception("avito orders store=%s", store.id)
        return {"store_id": int(store.id), "ok": False, "error": str(e)[:180], "new": 0}

    new_ids: list[str] = []
    sent = 0
    for order in orders:
        oid = order_display_id(order)
        if not oid:
            continue
        if oid in known:
            continue
        new_ids.append(oid)
        known.add(oid)
        if notify and bucket.get("seeded"):
            text = format_order_message(store.name or f"#{store.id}", order)
            ok, err, _ = await _send(db, bot_token, chat_id, text)
            if ok:
                sent += 1
            else:
                log.warning("avito order tg fail store=%s order=%s: %s", store.id, oid, err)

    # Обновляем seen: старые + новые.
    merged = list(bucket.get("orders") or [])
    for oid in new_ids:
        if oid not in merged:
            merged.append(oid)
    bucket["orders"] = _trim_list(merged, _MAX_SEEN_ORDERS)
    return {
        "store_id": int(store.id),
        "ok": True,
        "error": None,
        "new": sent if (notify and bucket.get("seeded")) else 0,
        "discovered": len(new_ids),
        "total": len(orders),
    }


async def poll_store_messages(
    db: Database,
    store: Store,
    *,
    bot_token: str,
    chat_id: str,
    seen: dict[str, Any],
    notify: bool,
) -> dict[str, Any]:
    bucket = _store_bucket(seen, int(store.id))
    msg_map: dict = bucket.get("messages") if isinstance(bucket.get("messages"), dict) else {}
    client = _client_for_store(store)
    try:
        await _ensure_user_id(db, store, client)
        chats = await client.list_chats(unread_only=True, limit=50)
    except HttpStatusError as e:
        hint = ""
        if e.status == 402:
            hint = " (нужна подписка Avito с API мессенджера)"
        return {
            "store_id": int(store.id),
            "ok": False,
            "error": f"чаты HTTP {e.status}{hint}: {str(e.body)[:160]}",
            "new": 0,
        }
    except Exception as e:
        log.exception("avito chats store=%s", store.id)
        return {"store_id": int(store.id), "ok": False, "error": str(e)[:180], "new": 0}

    sent = 0
    discovered = 0
    for chat in chats:
        cid = chat_id_of(chat)
        if not cid:
            continue
        lm = chat_last_message(chat)
        if not lm:
            # Fallback: подтянуть последние сообщения.
            try:
                msgs = await client.list_chat_messages(cid, limit=5)
            except Exception:
                msgs = []
            lm = msgs[0] if msgs else None
        if not lm:
            continue
        mid = message_id_of(lm)
        if not mid:
            # Стабильный ключ по времени + превью.
            mid = f"{cid}:{lm.get('created') or lm.get('created_at') or message_text_preview(lm, max_len=40)}"
        prev = str(msg_map.get(cid) or "")
        if prev == mid:
            continue
        discovered += 1
        msg_map[cid] = mid
        # Не уведомляем о своих исходящих: author_id == наш user_id.
        author = lm.get("author_id")
        if author is None:
            author = lm.get("authorId")
        try:
            if store.business_id and author is not None and int(author) == int(store.business_id):
                continue
        except (TypeError, ValueError):
            pass
        if notify and bucket.get("seeded"):
            text = format_chat_message(
                store.name or f"#{store.id}",
                chat,
                lm,
                our_user_id=store.business_id,
            )
            ok, err, tg_mid = await _send(db, bot_token, chat_id, text)
            if ok:
                sent += 1
                remember_tg_reply_target(
                    db,
                    tg_chat_id=chat_id,
                    tg_message_id=tg_mid,
                    store_id=int(store.id),
                    avito_chat_id=cid,
                    item_title=chat_item_title(chat),
                )
            else:
                log.warning("avito msg tg fail store=%s chat=%s: %s", store.id, cid, err)

    bucket["messages"] = _trim_dict(msg_map, _MAX_SEEN_MESSAGES)
    return {
        "store_id": int(store.id),
        "ok": True,
        "error": None,
        "new": sent if (notify and bucket.get("seeded")) else 0,
        "discovered": discovered,
        "chats": len(chats),
    }


async def run_avito_notify_cycle(
    db: Database,
    *,
    bot_token: str,
    chat_id: str,
    force_notify: bool = False,
) -> dict[str, Any]:
    """
    Один цикл опроса. force_notify=True — слать даже на первом прогоне
    (кнопка «сейчас» после seed всё равно шлёт только реально новые).
    """
    stores = _avito_stores(db)
    seen = _load_seen(db)
    do_orders = orders_notify_enabled(db)
    do_messages = messages_notify_enabled(db)

    results_orders: list[dict] = []
    results_messages: list[dict] = []
    orders_sent = 0
    messages_sent = 0

    for store in stores:
        bucket = _store_bucket(seen, int(store.id))
        seeded = bool(bucket.get("seeded"))
        notify = bool(seeded or force_notify)
        if do_orders:
            r = await poll_store_orders(
                db,
                store,
                bot_token=bot_token,
                chat_id=chat_id,
                seen=seen,
                notify=notify,
            )
            results_orders.append(r)
            orders_sent += int(r.get("new") or 0)
        if do_messages:
            r = await poll_store_messages(
                db,
                store,
                bot_token=bot_token,
                chat_id=chat_id,
                seen=seen,
                notify=notify,
            )
            results_messages.append(r)
            messages_sent += int(r.get("new") or 0)
        bucket["seeded"] = True

    _save_seen(db, seen)
    db.set_setting(SETTING_LAST_CHECK, str(int(time.time())))

    return {
        "ok": True,
        "stores": len(stores),
        "orders_sent": orders_sent,
        "messages_sent": messages_sent,
        "orders": results_orders,
        "messages": results_messages,
        "orders_enabled": do_orders,
        "messages_enabled": do_messages,
    }
