"""
Уведомления в Telegram: мгновенные (отзывы 1–3★, packer_issue) и периодические отчёты.
Использует Bot API: sendMessage. Без доп. зависимостей — aiohttp.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

log = logging.getLogger("telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

SETTING_REPORT_CHAT_ID = "telegram_report_chat_id"
SETTING_CARD_ERROR_CHAT_ID = "telegram_card_error_chat_id"


def resolve_telegram_chat_id(db, purpose: str) -> str:
    """
    purpose: default — мгновенные отзывы; report — периодический отчёт; card_error — ошибки карточки.
    Для report и card_error пустое значение = чат по умолчанию (telegram_chat_id).
    """
    default = (db.get_setting("telegram_chat_id") or "").strip()
    if purpose == "default":
        return default
    key = SETTING_REPORT_CHAT_ID if purpose == "report" else SETTING_CARD_ERROR_CHAT_ID
    specific = (db.get_setting(key) or "").strip()
    return specific or default


async def send_telegram_message(bot_token: str, chat_id: str, text: str) -> bool:
    token = (bot_token or "").strip()
    cid = (chat_id or "").strip()
    body = (text or "").strip()
    if not token or not cid or not body:
        return False
    url = TELEGRAM_API.format(token=token)
    payload = {"chat_id": cid, "text": body, "disable_web_page_preview": True}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    data = await resp.text()
                    log.warning("Telegram sendMessage failed: %s %s", resp.status, data[:200])
                    return False
                return True
    except Exception as e:
        log.exception("Telegram send_telegram_message: %s", e)
        return False


def format_activity_report(
    stats: dict,
    *,
    period_label: str,
    interval: str,
    include_card_errors: bool = True,
) -> str:
    """Текст периодического отчёта для Telegram."""
    reviews = int(stats.get("reviews_sent") or 0)
    questions = int(stats.get("questions_sent") or 0)
    wb_chats = int(stats.get("wb_chat_replies") or 0)
    ozon_chats = int(stats.get("ozon_chat_replies") or 0)
    chat_total = int(stats.get("chat_replies_total") or 0) or (wb_chats + ozon_chats)
    removed = int(stats.get("ozon_products_removed") or 0)
    card_errors = int(stats.get("card_errors") or 0)
    interval_ru = "за час" if interval == "hour" else "за сутки"
    lines = [
        f"Отчёт MarketAI ({interval_ru})",
        f"Период: {period_label}",
        "",
        f"Отзывы: отвечено {reviews}",
        f"Вопросы: отвечено {questions}",
        f"Чаты с покупателями: {chat_total} (WB: {wb_chats}, Ozon: {ozon_chats})",
        f"Автоакции Ozon: удалено товаров {removed}",
    ]
    if include_card_errors:
        lines.append(f"Ошибки в карточках: {card_errors}")
    return "\n".join(lines)


async def send_activity_report(
    bot_token: str,
    chat_id: str,
    stats: dict,
    *,
    period_label: str,
    interval: str,
    include_card_errors: bool = True,
) -> bool:
    body = format_activity_report(
        stats,
        period_label=period_label,
        interval=interval,
        include_card_errors=include_card_errors,
    )
    return await send_telegram_message(bot_token, chat_id, body)


async def send_review_to_chat(
    bot_token: str,
    chat_id: str,
    product_title: str,
    review_text: str,
    *,
    store_name: Optional[str] = None,
) -> bool:
    """
    Отправляет в чат Telegram сообщение: название товара + текст отзыва.
    Если store_name задан, добавляет в начало «Магазин: …».
    Возвращает True при успехе, False при ошибке (логируем).
    """
    token = (bot_token or "").strip()
    cid = (chat_id or "").strip()
    if not token or not cid:
        return False

    title = (product_title or "").strip() or "—"
    text = (review_text or "").strip() or "—"
    if store_name:
        body = f"Магазин: {store_name}\n\nТовар: {title}\n\nОтзыв:\n{text}"
    else:
        body = f"Товар: {title}\n\nОтзыв:\n{text}"
    return await send_telegram_message(token, cid, body)
