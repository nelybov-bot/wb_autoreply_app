"""Проверка ошибок в карточке товара: настройки, шаблоны, обработка результата ИИ."""
from __future__ import annotations

import logging
from typing import Any, Optional

from ..db import Database
from .telegram_notify import resolve_telegram_chat_id, send_telegram_message

log = logging.getLogger("card_check")

SETTING_CARD_CHECK_ENABLED = "card_check_enabled"
SETTING_CARD_CHECK_TELEGRAM = "card_check_telegram_enabled"
SETTING_CARD_CHECK_TEMPLATE = "card_check_telegram_template"
SETTING_CARD_CHECK_IN_REPORT = "card_check_include_in_periodic_report"

DEFAULT_CARD_CHECK_PROMPT = (
    "Дополнительно проверь, указывает ли текст покупателя на вероятную ошибку в карточке товара "
    "на маркетплейсе: неверное описание, характеристики, комплектация, размерная сетка, совместимость, "
    "цвет или модель в названии, противоречие между названием и характеристиками.\n"
    "НЕ считай ошибкой карточки: доставку, упаковку, пересорт, брак при транспортировке, "
    "субъективное «не понравилось», задержки.\n"
    "card_error.suspected = true только при явном несоответствии карточки тому, что получил или ожидал покупатель."
)

DEFAULT_BUYER_CHAT_PROMPT = (
    "Ты отвечаешь покупателю в чате по товару. Учитывай контекст переписки. "
    "Русский, 2–4 предложения, без эмодзи. Не повторяй полное название товара. "
    "Не предлагай компенсации и обращения в поддержку. Не задавай вопросов покупателю."
)

DEFAULT_TELEGRAM_TEMPLATE = (
    "Ошибка в карточке (вероятно)\n"
    "Магазин: {store_name}\n"
    "Товар: {product_title}\n"
    "Источник: {source_label}\n"
    "Текст покупателя:\n{customer_text}\n\n"
    "Возможная ошибка: {error_kind}\n"
    "{explanation}"
)

SOURCE_LABELS = {
    "review": "Отзыв",
    "question": "Вопрос",
    "wb_chat": "Чат WB",
    "ozon_chat": "Чат Ozon",
}

JSON_FORMAT_SUFFIX = (
    ' Ответь строго одним JSON-объектом, без текста до или после. '
    'Формат: {"reply": "текст ответа продавца", "packer_issue": true или false, '
    '"card_error": {"suspected": true или false, "error_kind": "краткий тип", "explanation": "почему"}}. '
    'packer_issue = true только для отзывов при проблемах упаковки, логистики, пересорта, не того товара, недокомплекта. '
    'Для вопросов и чатов packer_issue всегда false. '
    'card_error.suspected = true только при вероятной ошибке в карточке (см. инструкцию проверки карточки).'
)


def card_check_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_CARD_CHECK_ENABLED) or "1").strip() != "0"


def card_check_telegram_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_CARD_CHECK_TELEGRAM) or "1").strip() != "0"


def get_telegram_template(db: Database) -> str:
    t = (db.get_setting(SETTING_CARD_CHECK_TEMPLATE) or "").strip()
    return t or DEFAULT_TELEGRAM_TEMPLATE


def render_telegram_message(
    db: Database,
    *,
    store_name: str,
    product_title: str,
    source_type: str,
    customer_text: str,
    error_kind: str,
    explanation: str,
) -> str:
    template = get_telegram_template(db)
    return template.format(
        store_name=(store_name or "—").strip(),
        product_title=(product_title or "—").strip(),
        source_label=SOURCE_LABELS.get(source_type, source_type or "—"),
        customer_text=(customer_text or "—").strip()[:2000],
        error_kind=(error_kind or "—").strip(),
        explanation=(explanation or "—").strip(),
    )


def parse_card_error(obj: dict) -> Optional[dict]:
    if not isinstance(obj, dict):
        return None
    ce = obj.get("card_error")
    if not isinstance(ce, dict):
        return None
    if not bool(ce.get("suspected")):
        return None
    kind = str(ce.get("error_kind") or "").strip()
    expl = str(ce.get("explanation") or "").strip()
    if not kind and not expl:
        return None
    return {
        "error_kind": kind or "не указано",
        "explanation": expl or "—",
    }


async def maybe_record_card_error(
    db: Database,
    obj: dict,
    *,
    store_id: int,
    store_name: str,
    product_title: str,
    customer_text: str,
    source_type: str,
    source_ref: str,
) -> Optional[int]:
    """Сохранить алерт и при необходимости отправить в Telegram. Возвращает id алерта или None."""
    if not card_check_enabled(db):
        return None
    parsed = parse_card_error(obj)
    if not parsed:
        return None
    ref = (source_ref or "").strip()
    if not ref:
        return None
    if db.has_card_error_alert(store_id, source_type, ref):
        return None
    alert_id = db.add_card_error_alert(
        store_id=store_id,
        source_type=source_type,
        source_ref=ref,
        product_title=product_title,
        customer_text=customer_text,
        error_kind=parsed["error_kind"],
        explanation=parsed["explanation"],
    )
    try:
        db.add_audit_event(
            actor="system",
            action="card_error_detected",
            item_type="card_error",
            store_id=store_id,
            result="ok",
            meta={
                "alert_id": alert_id,
                "source_type": source_type,
                "source_ref": ref,
                "product_title": (product_title or "")[:200],
                "error_kind": parsed["error_kind"],
                "customer_text_preview": (customer_text or "")[:400],
            },
        )
    except Exception:
        pass
    if card_check_telegram_enabled(db):
        token = (db.get_setting("telegram_bot_token") or "").strip()
        chat_id = resolve_telegram_chat_id(db, "card_error")
        if token and chat_id:
            body = render_telegram_message(
                db,
                store_name=store_name,
                product_title=product_title,
                source_type=source_type,
                customer_text=customer_text,
                error_kind=parsed["error_kind"],
                explanation=parsed["explanation"],
            )
            ok, _ = await send_telegram_message(token, chat_id, body)
            if ok:
                db.mark_card_error_telegram_sent(alert_id)
            else:
                log.warning("card_error telegram send failed alert_id=%s", alert_id)
    return alert_id


def build_generation_user_prompt(
    db: Database,
    *,
    task_prompt: str,
    product_title: str,
    body_label: str,
    body_text: str,
    closing: str,
) -> str:
    card_p = db.get_prompt("card_check", "general")
    if not card_p.strip():
        card_p = DEFAULT_CARD_CHECK_PROMPT
    return (
        f"{task_prompt}\n\n"
        f"--- Проверка карточки товара ---\n{card_p}\n\n"
        f"Товар: {product_title}\n{body_label}:\n{body_text}\n\n"
        f"{closing}{JSON_FORMAT_SUFFIX}"
    )
