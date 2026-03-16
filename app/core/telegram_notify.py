"""
Отправка уведомлений в Telegram (отзывы 1–3 звёзд: перепутан товар, упаковка и т.д.).
Использует Bot API: sendMessage. Без доп. зависимостей — aiohttp.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

log = logging.getLogger("telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


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
        log.exception("Telegram send_review_to_chat: %s", e)
        return False
