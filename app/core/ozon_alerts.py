"""Ozon: важные уведомления из чатов поддержки (штрафы, ИС, блокировки)."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from ..db import Database
from .chat_common import MSK, parse_reply_from_date
from .openai_client import OpenAIClient
from .telegram_notify import resolve_telegram_chat_id, send_telegram_message

log = logging.getLogger("ozon_alerts")

SETTING_ENABLED = "ozon_alerts_enabled"
SETTING_TELEGRAM = "ozon_alerts_telegram_enabled"
SETTING_FROM_DATE = "ozon_alerts_check_from_date"
SETTING_TEMPLATE = "ozon_alerts_telegram_template"

DEFAULT_PROMPT = (
    "Ты анализируешь сообщения от Ozon продавцу (поддержка, уведомления, новости, CRM).\n"
    "Определи, есть ли ВАЖНОЕ уведомление, где продавцу грозит штраф, блокировка, снятие с продажи "
    "или срочные обязательные действия.\n"
    "Важно: штраф за нарушение интеллектуальной собственности (фото/контент), претензии правообладателей, "
    "штрафы за фальсификацию, срочное удаление/блокировка карточки, требование предоставить документы под угрозой санкций.\n"
    "НЕ важно: общие новости платформы, советы и обучение без санкций, реклама сервисов Ozon, поздравления, "
    "информационные рассылки без суммы штрафа и без дедлайна."
)

DEFAULT_TELEGRAM_TEMPLATE = (
    "⚠️ Ozon: важное уведомление\n"
    "Магазин: {store_name}\n"
    "Тип: {threat_type}\n"
    "Сумма: {amount}\n"
    "Товар: {product_ref}\n"
    "{summary}\n\n"
    "Действия: {action_needed}\n"
    "Чат: {chat_type}\n"
    "Дата: {message_at}\n\n"
    "Текст:\n{message_text}"
)

JSON_SUFFIX = (
    " Ответь строго одним JSON-объектом, без текста до или после. "
    'Формат: {"important": true или false, "threat_type": "краткий тип или —", '
    '"amount": "сумма штрафа или —", "product_ref": "товар/SKU/артикул или —", '
    '"summary": "краткая сводка 1–3 предложения", '
    '"action_needed": "что проверить или сделать, чтобы избежать штрафа"}'
)


def ozon_alerts_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_ENABLED) or "0").strip() == "1"


def ozon_alerts_telegram_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_TELEGRAM) or "1").strip() != "0"


def ozon_alerts_from_date(db: Database):
    return parse_reply_from_date(db.get_setting(SETTING_FROM_DATE) or "")


def get_telegram_template(db: Database) -> str:
    t = (db.get_setting(SETTING_TEMPLATE) or "").strip()
    return t or DEFAULT_TELEGRAM_TEMPLATE


def render_telegram_message(
    db: Database,
    *,
    store_name: str,
    chat_type: str,
    message_at: str,
    message_text: str,
    threat_type: str,
    amount: str,
    product_ref: str,
    summary: str,
    action_needed: str,
) -> str:
    template = get_telegram_template(db)
    return template.format(
        store_name=(store_name or "—").strip(),
        chat_type=(chat_type or "—").strip(),
        message_at=(message_at or "—").strip(),
        message_text=(message_text or "—").strip()[:2000],
        threat_type=(threat_type or "—").strip(),
        amount=(amount or "—").strip(),
        product_ref=(product_ref or "—").strip(),
        summary=(summary or "—").strip(),
        action_needed=(action_needed or "—").strip(),
    )


def parse_ozon_alert_json(txt: str) -> Optional[dict]:
    try:
        obj = json.loads(txt)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    if not bool(obj.get("important")):
        return None
    summary = str(obj.get("summary") or "").strip()
    if not summary:
        return None
    return {
        "threat_type": str(obj.get("threat_type") or "—").strip() or "—",
        "amount": str(obj.get("amount") or "—").strip() or "—",
        "product_ref": str(obj.get("product_ref") or "—").strip() or "—",
        "summary": summary,
        "action_needed": str(obj.get("action_needed") or "—").strip() or "—",
    }


def _role_label(role: str) -> str:
    if role == "client":
        return "Покупатель"
    if role == "seller":
        return "Продавец"
    if role == "support":
        return "Ozon"
    return role or "Сообщение"


def build_conversation_excerpt(
    lines: List[Tuple[str, str, str, str]],
    *,
    up_to_message_id: str,
    max_lines: int = 12,
) -> str:
    target = (up_to_message_id or "").strip()
    chunk: List[str] = []
    for role, text, mid, created in lines:
        if target and mid == target:
            break
        chunk.append(f"{_role_label(role)}: {text}")
    if not chunk:
        return ""
    tail = chunk[-max_lines:]
    return "\n".join(tail)


async def classify_ozon_support_message(
    db: Database,
    client: OpenAIClient,
    *,
    store_name: str,
    chat_type: str,
    message_text: str,
    message_at: str,
    conversation_excerpt: str,
) -> tuple[Optional[dict], bool]:
    """Возвращает (результат, пометить_как_проверенное). При сбое ИИ — не помечать."""
    task = db.get_prompt("ozon_important_alert", "general")
    if not task.strip():
        task = DEFAULT_PROMPT
    user = (
        f"{task}\n\n"
        f"Магазин: {store_name}\n"
        f"Тип чата: {chat_type}\n"
        f"Дата сообщения: {message_at or '—'}\n\n"
    )
    if conversation_excerpt.strip():
        user += f"Контекст переписки (раньше):\n{conversation_excerpt}\n\n"
    user += f"Анализируемое сообщение от Ozon/системы:\n{message_text}\n\n{JSON_SUFFIX}"
    try:
        txt = await client.generate(
            "Ты помощник продавца на Ozon. Отвечай только JSON.",
            user,
        )
    except Exception as e:
        log.warning("ozon_alert classify failed: %s", e)
        return None, False
    parsed = parse_ozon_alert_json(txt)
    if parsed:
        return parsed, False
    return None, True


async def maybe_record_ozon_alert(
    db: Database,
    parsed: dict,
    *,
    store_id: int,
    store_name: str,
    chat_id: str,
    message_id: str,
    chat_type: str,
    message_at: str,
    message_text: str,
) -> Optional[int]:
    ref = f"{chat_id}:{message_id}"
    if db.has_ozon_important_alert(store_id, chat_id, message_id):
        return None
    alert_id = db.add_ozon_important_alert(
        store_id=store_id,
        chat_id=chat_id,
        message_id=message_id,
        chat_type=chat_type,
        message_at=message_at,
        message_text=message_text,
        threat_type=parsed["threat_type"],
        amount=parsed["amount"],
        product_ref=parsed["product_ref"],
        summary=parsed["summary"],
        action_needed=parsed["action_needed"],
    )
    try:
        db.add_audit_event(
            actor="system",
            action="ozon_alert_detected",
            item_type="ozon_alert",
            store_id=store_id,
            result="ok",
            meta={
                "alert_id": alert_id,
                "chat_id": chat_id,
                "message_id": message_id,
                "threat_type": parsed["threat_type"],
                "amount": parsed["amount"],
                "summary": parsed["summary"][:400],
            },
        )
    except Exception:
        pass
    if ozon_alerts_telegram_enabled(db):
        token = (db.get_setting("telegram_bot_token") or "").strip()
        chat_tg = resolve_telegram_chat_id(db, "ozon_alerts")
        if token and chat_tg:
            body = render_telegram_message(
                db,
                store_name=store_name,
                chat_type=chat_type,
                message_at=message_at,
                message_text=message_text,
                threat_type=parsed["threat_type"],
                amount=parsed["amount"],
                product_ref=parsed["product_ref"],
                summary=parsed["summary"],
                action_needed=parsed["action_needed"],
            )
            ok = await send_telegram_message(token, chat_tg, body)
            if ok:
                db.mark_ozon_important_alert_telegram_sent(alert_id)
            else:
                log.warning("ozon_alert telegram send failed alert_id=%s", alert_id)
    return alert_id


def format_message_at_display(iso: str) -> str:
    from .ozon_buyer_chat import format_ozon_datetime_msk

    return format_ozon_datetime_msk(iso) or (iso or "—")
