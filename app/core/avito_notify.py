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
from .telegram_notify import escape_tg_html, send_telegram_message

log = logging.getLogger("avito_notify")

SETTING_ENABLED = "avito_notify_enabled"
SETTING_ORDERS = "avito_orders_notify_enabled"
SETTING_MESSAGES = "avito_messages_notify_enabled"
SETTING_CHAT_ID = "avito_notify_telegram_chat_id"
SETTING_SEEN = "avito_notify_seen_json"
SETTING_LAST_CHECK = "avito_notify_last_check"

# Новые заказы, требующие внимания продавца.
ORDER_WATCH_STATUSES = ("on_confirmation", "ready_to_ship")

# Окно выборки заказов (сек) — 14 дней.
ORDERS_LOOKBACK_SEC = 14 * 24 * 3600

# Сколько id держим в seen-словарях на магазин.
_MAX_SEEN_ORDERS = 400
_MAX_SEEN_MESSAGES = 600

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


def format_order_message(store_name: str, order: dict) -> str:
    oid = escape_tg_html(order_display_id(order) or "?")
    status = escape_tg_html(str(order.get("status") or "—"))
    titles = escape_tg_html(order_item_titles(order))
    total = escape_tg_html(order_total_rub(order))
    created = escape_tg_html(str(order.get("createdAt") or "—"))
    store = escape_tg_html(store_name or "Avito")
    return (
        f"<b>Avito · новый заказ</b>\n"
        f"Магазин: <b>{store}</b>\n"
        f"Заказ: <code>{oid}</code>\n"
        f"Статус: {status}\n"
        f"Сумма: {total}\n"
        f"Товары: {titles}\n"
        f"Создан: {created}"
    )


def format_chat_message(store_name: str, chat: dict, msg: dict) -> str:
    store = escape_tg_html(store_name or "Avito")
    cid = escape_tg_html(chat_id_of(chat) or "?")
    item = escape_tg_html(chat_item_title(chat))
    preview = escape_tg_html(message_text_preview(msg) or "—")
    author = msg.get("author_id") or msg.get("authorId")
    author_s = escape_tg_html(str(author) if author is not None else "—")
    return (
        f"<b>Avito · новое сообщение</b>\n"
        f"Магазин: <b>{store}</b>\n"
        f"Чат: <code>{cid}</code>\n"
        f"Объявление: {item}\n"
        f"От: {author_s}\n"
        f"Текст: {preview}"
    )


async def _send(
    db: Database,
    bot_token: str,
    chat_id: str,
    text: str,
) -> tuple[bool, str]:
    return await send_telegram_message(
        bot_token,
        chat_id,
        text,
        parse_mode="HTML",
        db=db,
    )


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
            ok, err = await _send(db, bot_token, chat_id, text)
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
            text = format_chat_message(store.name or f"#{store.id}", chat, lm)
            ok, err = await _send(db, bot_token, chat_id, text)
            if ok:
                sent += 1
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
