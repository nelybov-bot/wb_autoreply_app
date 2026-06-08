"""
Уведомления в Telegram: мгновенные (отзывы 1–3★, packer_issue) и периодические отчёты.
Использует Bot API: sendMessage. Без доп. зависимостей — aiohttp.
"""
from __future__ import annotations

import html as html_lib
import json
import logging
import re
from typing import Any, Optional, Tuple, Union

import aiohttp

log = logging.getLogger("telegram")

TELEGRAM_PARSE_MODE = "HTML"

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
TELEGRAM_API = TELEGRAM_API_BASE + "/sendMessage"
_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{20,}$")

SETTING_REPORT_CHAT_ID = "telegram_report_chat_id"
SETTING_CARD_ERROR_CHAT_ID = "telegram_card_error_chat_id"
SETTING_OZON_ALERTS_CHAT_ID = "ozon_alerts_telegram_chat_id"


TELEGRAM_MAX_MESSAGE_LEN = 4096


def escape_tg_html(text: str) -> str:
    """Экранирование для parse_mode=HTML."""
    return html_lib.escape((text or "").strip() or "—")


def tg_blockquote(text: str) -> str:
    return f"<blockquote>{escape_tg_html(text)}</blockquote>"


def template_uses_html(template: str) -> bool:
    return bool(
        re.search(r"</?(?:b|i|blockquote|code|pre|u|s)>", template or "", re.I)
    )


def is_plain_telegram_template(template: str) -> bool:
    """Шаблон без HTML-тегов — подменяем на форматированный по умолчанию."""
    t = (template or "").strip()
    return bool(t) and not template_uses_html(t)


def normalize_telegram_bot_token(raw: str) -> str:
    """
    Токен от @BotFather: 123456789:AAH...
    Убирает пробелы, префикс Bot и URL, если вставили целиком.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    m = re.search(r"bot(\d+:[A-Za-z0-9_-]+)", s, re.IGNORECASE)
    if m:
        return m.group(1)
    if s.lower().startswith("bot "):
        s = s[4:].strip()
    elif s.lower().startswith("bot"):
        s = s[3:].strip()
    return s.strip()


def is_plausible_telegram_token(token: str) -> bool:
    return bool(_TOKEN_RE.match(normalize_telegram_bot_token(token)))


def describe_telegram_api_error(resp_status: int, data: dict) -> str:
    """Понятное описание ошибки Telegram для UI и логов."""
    code = int(data.get("error_code") or resp_status or 0)
    desc = str(data.get("description") or "ошибка Telegram").strip()
    low = desc.lower()
    if code == 404 or resp_status == 404 or low == "not found":
        return (
            "неверный токен бота (404 Not Found). "
            "Скопируйте токен у @BotFather — формат 123456789:AAH..., без слова Bot и без URL"
        )
    if code == 401 or "unauthorized" in low:
        return "токен бота отозван или недействителен — получите новый у @BotFather"
    if "chat not found" in low:
        return (
            f"чат не найден ({desc}). Проверьте chat_id: бот должен быть в чате, "
            "для группы ID обычно отрицательный (например -100...)"
        )
    if "bot was blocked" in low:
        return "бот заблокирован пользователем — разблокируйте бота в Telegram"
    if "group chat was upgraded" in low:
        return "группа стала супергруппой — обновите chat_id (часто -100...)"
    return desc


async def _telegram_api_call(
    token: str,
    method: str,
    *,
    json_payload: Optional[dict] = None,
) -> Tuple[bool, str, dict, int]:
    """Вызов метода Bot API. Возвращает (ok, error_text, json_body, http_status)."""
    tok = normalize_telegram_bot_token(token)
    if not tok:
        return False, "токен бота не задан", {}, 0
    url = TELEGRAM_API_BASE.format(token=tok) + "/" + method.lstrip("/")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=json_payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                raw = await resp.text()
                http_status = int(resp.status)
                try:
                    data = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    if http_status != 200:
                        return False, f"HTTP {http_status}: {raw[:200]}", {}, http_status
                    return False, "неверный ответ Telegram", {}, http_status
                if not isinstance(data, dict):
                    return False, "неверный ответ Telegram", {}, http_status
                if not data.get("ok"):
                    err = describe_telegram_api_error(http_status, data)
                    return False, err, data, http_status
                return True, "", data, http_status
    except Exception as e:
        log.exception("Telegram API %s: %s", method, e)
        return False, str(e), {}, 0


async def verify_telegram_bot_token(token: str) -> Tuple[bool, str, Optional[dict[str, Any]]]:
    """Проверка токена через getMe."""
    tok = normalize_telegram_bot_token(token)
    if not tok:
        return False, "токен бота не задан", None
    if not is_plausible_telegram_token(tok):
        return (
            False,
            "неверный формат токена (ожидается 123456789:AAH... от @BotFather)",
            None,
        )
    ok, err, data, _ = await _telegram_api_call(tok, "getMe")
    if not ok:
        return False, err, None
    result = data.get("result")
    return True, "", result if isinstance(result, dict) else None


async def test_telegram_delivery(
    bot_token: str,
    chat_id: str,
    *,
    text: str = (
        "<b>✅ Тест MarketAI</b>\n\n"
        "Telegram настроен. <i>Форматирование HTML работает.</i>"
    ),
) -> Tuple[bool, str, Optional[dict[str, Any]]]:
    """getMe + пробное сообщение в чат."""
    ok, err, bot = await verify_telegram_bot_token(bot_token)
    if not ok:
        return False, err, bot
    cid = normalize_telegram_chat_id(chat_id)
    if not cid:
        return False, "chat_id не задан", bot
    sent_ok, sent_err = await send_telegram_message(
        bot_token, str(chat_id), text, parse_mode=TELEGRAM_PARSE_MODE
    )
    if not sent_ok:
        return False, sent_err, bot
    return True, "", bot


def normalize_telegram_chat_id(chat_id: str) -> Union[str, int]:
    """Числовой chat_id — int (группы с минусом); @channel — строка."""
    cid = (chat_id or "").strip()
    if not cid:
        return ""
    if cid.startswith("@"):
        return cid
    if cid.lstrip("-").isdigit():
        return int(cid)
    return cid


def resolve_telegram_chat_id(db, purpose: str) -> str:
    """
    purpose: default — отзывы; report — отчёт; card_error — карточки; ozon_alerts — важные уведомления Ozon.
    Для остальных пустое значение = чат по умолчанию (telegram_chat_id).
    """
    default = (db.get_setting("telegram_chat_id") or "").strip()
    if purpose == "default":
        return default
    keys = {
        "report": SETTING_REPORT_CHAT_ID,
        "card_error": SETTING_CARD_ERROR_CHAT_ID,
        "ozon_alerts": SETTING_OZON_ALERTS_CHAT_ID,
    }
    key = keys.get(purpose, SETTING_CARD_ERROR_CHAT_ID)
    specific = (db.get_setting(key) or "").strip()
    return specific or default


async def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    parse_mode: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Отправка в Telegram. Возвращает (успех, описание ошибки).
    Telegram Bot API часто отвечает HTTP 200 с ok=false — проверяем поле ok в JSON.
    """
    token = normalize_telegram_bot_token(bot_token)
    cid = normalize_telegram_chat_id(chat_id)
    body = (text or "").strip()
    if not token or not cid or not body:
        return False, "не заданы токен, chat_id или текст"
    if not is_plausible_telegram_token(token):
        return (
            False,
            "неверный формат токена (ожидается 123456789:AAH... от @BotFather)",
        )
    url = TELEGRAM_API.format(token=token)
    chunks = [
        body[i : i + TELEGRAM_MAX_MESSAGE_LEN]
        for i in range(0, len(body), TELEGRAM_MAX_MESSAGE_LEN)
    ]
    try:
        async with aiohttp.ClientSession() as session:
            for chunk in chunks:
                payload: dict = {
                    "chat_id": cid,
                    "text": chunk,
                    "disable_web_page_preview": True,
                }
                if parse_mode:
                    payload["parse_mode"] = parse_mode
                async with session.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    raw = await resp.text()
                    try:
                        data = json.loads(raw) if raw else {}
                    except json.JSONDecodeError:
                        if resp.status != 200:
                            err = f"HTTP {resp.status}: {raw[:200]}"
                            log.warning("Telegram sendMessage failed: %s", err)
                            return False, err
                        return False, "неверный ответ Telegram"
                    if not isinstance(data, dict) or not data.get("ok"):
                        err = describe_telegram_api_error(int(resp.status), data)
                        log.warning(
                            "Telegram sendMessage failed: HTTP %s %s",
                            resp.status,
                            err[:240],
                        )
                        return False, err
        return True, ""
    except Exception as e:
        log.exception("Telegram send_telegram_message: %s", e)
        return False, str(e)


def format_activity_report(
    stats: dict,
    *,
    period_label: str,
    interval: str,
    include_card_errors: bool = True,
) -> str:
    """Текст периодического отчёта для Telegram (HTML)."""
    reviews = int(stats.get("reviews_sent") or 0)
    questions = int(stats.get("questions_sent") or 0)
    wb_chats = int(stats.get("wb_chat_replies") or 0)
    ozon_chats = int(stats.get("ozon_chat_replies") or 0)
    chat_total = int(stats.get("chat_replies_total") or 0) or (wb_chats + ozon_chats)
    removed = int(stats.get("ozon_products_removed") or 0)
    card_errors = int(stats.get("card_errors") or 0)
    ozon_alerts = int(stats.get("ozon_alerts") or 0)
    interval_ru = "за час" if interval == "hour" else "за сутки"
    period = escape_tg_html(period_label)
    lines = [
        f"<b>📊 Отчёт MarketAI</b> <i>({escape_tg_html(interval_ru)})</i>",
        "",
        f"<b>Период:</b> {period}",
        "",
        f"<b>Отзывы:</b> отвечено {reviews}",
        f"<b>Вопросы:</b> отвечено {questions}",
        f"<b>Чаты с покупателями:</b> {chat_total} "
        f"<i>(WB: {wb_chats}, Ozon: {ozon_chats})</i>",
        f"<b>Важные уведомления Ozon:</b> {ozon_alerts}",
        f"<b>Автоакции Ozon:</b> удалено товаров {removed}",
    ]
    if include_card_errors:
        lines.append(f"<b>Ошибки в карточках:</b> {card_errors}")
    return "\n".join(lines)


async def send_activity_report(
    bot_token: str,
    chat_id: str,
    stats: dict,
    *,
    period_label: str,
    interval: str,
    include_card_errors: bool = True,
) -> Tuple[bool, str]:
    body = format_activity_report(
        stats,
        period_label=period_label,
        interval=interval,
        include_card_errors=include_card_errors,
    )
    return await send_telegram_message(
        bot_token, chat_id, body, parse_mode=TELEGRAM_PARSE_MODE
    )


async def send_review_to_chat(
    bot_token: str,
    chat_id: str,
    product_title: str,
    review_text: str,
    *,
    store_name: Optional[str] = None,
    alert_title: str = "Упаковка / пересорт",
) -> Tuple[bool, str]:
    """Мгновенное уведомление по отзыву (упаковка, пересорт)."""
    token = normalize_telegram_bot_token(bot_token)
    cid = (chat_id or "").strip()
    if not token or not cid:
        return False, "не заданы токен или chat_id"

    title = (product_title or "").strip() or "—"
    text = (review_text or "").strip() or "—"
    parts = [f"<b>⚠️ {escape_tg_html(alert_title)}</b>", ""]
    if store_name:
        parts.append(f"<b>Магазин:</b> {escape_tg_html(store_name)}")
    parts.append(f"<b>Товар:</b> {escape_tg_html(title)}")
    parts.append("")
    parts.append("<b>Отзыв:</b>")
    parts.append(tg_blockquote(text))
    body = "\n".join(parts)
    return await send_telegram_message(token, cid, body, parse_mode=TELEGRAM_PARSE_MODE)
