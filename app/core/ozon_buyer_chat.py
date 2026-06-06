"""Ozon «Чаты с покупателями» — разбор сообщений и заголовков."""
from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, List, Optional, Tuple

from .chat_common import parse_api_error_detail


def _ozon_user_role(user: Any) -> str:
    if not isinstance(user, dict):
        return "other"
    raw = str(user.get("type") or "").strip().lower()
    # в документации иногда «Customer», иногда с кириллической «С»
    if raw in ("customer", "сustomer", "client", "buyer"):
        return "client"
    if raw == "seller":
        return "seller"
    if raw in ("support", "crm", "courier"):
        return raw
    return raw or "other"


def _message_text(msg: dict) -> str:
    parts = msg.get("data") or []
    if isinstance(parts, list):
        text = " ".join(str(x).strip() for x in parts if x is not None and str(x).strip())
        if text:
            return text
    if msg.get("is_image"):
        return "[изображение]"
    return ""


def collect_ozon_thread_lines(messages: List[dict]) -> List[Tuple[str, str, str, str]]:
    """
    (role, text, message_id, created_at ISO).
    Сообщения от новых к старым (как в API по умолчанию) — сортируем по created_at.
    """
    out: List[Tuple[str, str, str, str]] = []
    for msg in messages or []:
        if not isinstance(msg, dict):
            continue
        text = _message_text(msg)
        if not text:
            continue
        role = _ozon_user_role(msg.get("user"))
        mid = str(msg.get("message_id") or "").strip()
        created = str(msg.get("created_at") or "").strip()
        if not mid:
            continue
        out.append((role, text, mid, created))
    out.sort(key=lambda x: x[3] or x[2])
    return out


def last_client_message_info(lines: List[Tuple[str, str, str, str]]) -> Optional[Tuple[str, str]]:
    """message_id и created_at последнего сообщения покупателя."""
    for role, _text, mid, created in reversed(lines):
        if role == "client":
            return mid, created
    return None


def product_title_from_ozon_chat(messages: List[dict], lines: List[Tuple[str, str, str, str]]) -> str:
    for msg in messages or []:
        if not isinstance(msg, dict):
            continue
        ctx = msg.get("context")
        if not isinstance(ctx, dict):
            continue
        sku = ctx.get("sku")
        order = ctx.get("order_number")
        if sku is not None and str(sku).strip():
            return f"Товар SKU {sku}" + (f", заказ {order}" if order else "")
    for _role, text, _mid, _ca in lines:
        m = re.search(r"артикул\s+(\d+)", text, re.IGNORECASE)
        if m:
            return f"Товар артикул {m.group(1)}"
        m = re.search(r"товару\s*\"([^\"]+)\"", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return "Товар (название не указано в данных чата)"


def ozon_chat_row_id(row: dict) -> str:
    chat = row.get("chat") if isinstance(row, dict) else None
    if isinstance(chat, dict):
        return str(chat.get("chat_id") or "").strip()
    return ""


def ozon_chat_type(row: dict) -> str:
    if not isinstance(row, dict):
        return ""
    chat = row.get("chat")
    if isinstance(chat, dict):
        t = chat.get("chat_type")
        if t is not None and str(t).strip():
            return str(t).strip()
    return str(row.get("chat_type") or "").strip()


def _norm_ozon_chat_type(value: str) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def is_ozon_buyer_chat_row(row: dict) -> bool:
    """True только для чатов с покупателями (Buyer_Seller), не Seller_Support."""
    ct = _norm_ozon_chat_type(ozon_chat_type(row))
    if not ct:
        return False
    if ct in ("seller_support", "seller_support_chat"):
        return False
    if "support" in ct and "buyer" not in ct:
        return False
    if ct in ("buyer_seller", "buyer_sueller"):
        return True
    if "buyer" in ct and "support" not in ct:
        return True
    return False


def is_ozon_support_chat_row(row: dict) -> bool:
    ct = _norm_ozon_chat_type(ozon_chat_type(row))
    return bool(ct) and not is_ozon_buyer_chat_row(row)


def _parse_ozon_iso(iso: str) -> Optional[dt.datetime]:
    s = (iso or "").strip()
    if not s:
        return None
    try:
        msg_dt = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=dt.timezone.utc)
        return msg_dt
    except ValueError:
        return None


def hours_since_ozon_iso(iso: str, *, now: Optional[dt.datetime] = None) -> Optional[float]:
    msg_dt = _parse_ozon_iso(iso)
    if msg_dt is None:
        return None
    ref = now or dt.datetime.now(dt.timezone.utc)
    return (ref - msg_dt.astimezone(dt.timezone.utc)).total_seconds() / 3600.0


def ozon_reply_window_hint(
    lines: List[Tuple[str, str, str, str]],
    *,
    chat_status: str = "",
) -> Dict[str, Any]:
    """
    Оценка, можно ли ещё ответить в чате через API.
    Ozon часто возвращает 403 access period has expired — это срок диалога, не Premium.
    """
    st = str(chat_status or "").strip().lower()
    if st == "closed":
        return {
            "blocked": True,
            "reason": "Чат закрыт в Ozon — отправка через API недоступна.",
            "warning": "",
            "last_client_message_at": "",
            "hours_since_client": None,
        }
    info = last_client_message_info(lines)
    if not info:
        return {
            "blocked": False,
            "reason": "",
            "warning": "",
            "last_client_message_at": "",
            "hours_since_client": None,
        }
    _mid, created = info
    hours = hours_since_ozon_iso(created)
    out: Dict[str, Any] = {
        "blocked": False,
        "reason": "",
        "warning": "",
        "last_client_message_at": created,
        "hours_since_client": hours,
    }
    if hours is None:
        return out
    # По ошибкам Ozon окно часто ~10 суток; 233 ч ≈ 9.7 дня.
    if hours >= 240:
        out["blocked"] = True
        days = int(hours // 24)
        out["reason"] = (
            f"Последнее сообщение покупателя {days} дн. назад — окно ответа Ozon, скорее всего, закрыто."
        )
    elif hours >= 72:
        out["warning"] = (
            f"С последнего сообщения покупателя прошло {int(hours)} ч. "
            "Ozon может отклонить ответ, если срок диалога истёк."
        )
    return out


def is_ozon_per_chat_send_error(body: str) -> bool:
    """403 из-за закрытого окна ответа в конкретном чате — не повод скипать весь магазин."""
    low = ((body or "") + " " + parse_api_error_detail(body or "")).lower()
    return "access period has expired" in low or "actions with this chat not permitted" in low


def ozon_http_skip_reason(status: int, body: str, *, feature: str = "") -> Optional[str]:
    """
    Причина тихого пропуска магазина (нет Premium, нет доступа к API).
    None — обычная ошибка, её нужно показать или залогировать отдельно.
    """
    if status == 401:
        return None
    if status not in (402, 403, 404):
        return None
    if feature == "chat" and is_ozon_per_chat_send_error(body):
        return None
    detail = parse_api_error_detail(body or "")
    combined = f"{detail} {body or ''}".lower()
    if any(x in combined for x in ("premium", "subscription", "tariff", " plus", "plus ", "премиум", "подписк")):
        return "no_premium"
    if feature == "chat" and status in (402, 403):
        return "no_chat_access"
    if feature == "actions" and status in (402, 403):
        return "no_actions_access"
    if status in (402, 403):
        return "no_access"
    return None


def ozon_feature_unavailable_user_message(reason: str, *, feature: str = "") -> str:
    labels = {
        "chat": "чаты с покупателями",
        "actions": "акции",
    }
    feat = labels.get(feature, "этот раздел API")
    if reason == "no_premium":
        return (
            f"Магазин пропущен: для {feat} нужен Premium Plus/Pro у Ozon. "
            "Отзывы и вопросы работают без Premium."
        )
    if reason == "no_chat_access":
        return (
            "Магазин пропущен: нет доступа к чатам Ozon "
            "(Premium Plus/Pro и право «Чат» у API-ключа)."
        )
    if reason in ("no_actions_access", "no_access"):
        return f"Магазин пропущен: Ozon API недоступен для {feat} ({reason})."
    return f"Магазин пропущен: {feat} недоступны ({reason})."


def ozon_chat_error_message(status: int, body: str) -> str:
    detail = parse_api_error_detail(body or "")
    low = detail.lower()
    if status == 401:
        return f"Ozon chat: 401 — проверьте Client-Id и Api-Key. {detail}"
    if status == 403:
        if "access period has expired" in low or "actions with this chat not permitted" in low:
            return (
                "Ozon chat: окно для ответа в этом чате закрыто (истёк срок доступа). "
                "Это не Premium и не право «Чат» у ключа — Ozon запрещает отправку в этот диалог. "
                "Попробуйте более свежий чат или ответ из кабинета продавца, если там ещё доступно. "
                f"Детали: {detail}"
            )
        if any(x in low for x in ("premium", "subscription", "tariff", "plus")):
            return (
                "Ozon chat: 403 — нет доступа к чатам. Нужны Premium Plus/Pro и право «Чат» у API-ключа. "
                + detail
            )
        return f"Ozon chat: 403 — доступ запрещён. {detail}"
    if status == 429:
        return f"Ozon chat: слишком много запросов (лимит 1 req/s). {detail}"
    if status == 400:
        return f"Ozon chat: неверный параметр. {detail}"
    if status == 404:
        return f"Ozon chat: чат не найден. {detail}"
    if status == 409:
        return f"Ozon chat: конфликт (возможно, чат уже закрыт). {detail}"
    return f"Ozon chat: {detail or status}"
