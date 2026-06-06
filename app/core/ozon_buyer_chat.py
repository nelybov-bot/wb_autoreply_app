"""Ozon «Чаты с покупателями» — разбор сообщений и заголовков."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


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
