"""Общая логика чатов с покупателями (WB, Ozon): дата отсечки, ключ сообщения."""
from __future__ import annotations

import datetime as dt
import json
from typing import Any, Optional, Tuple

from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
SETTING_REPLY_FROM = "buyer_chat_reply_from_date"


def parse_reply_from_date(raw: str) -> Optional[dt.date]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        return None


def cutoff_start_msk(d: dt.date) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=MSK)


def wb_ts_ms_after_cutoff(ts_ms: int, cutoff: Optional[dt.date]) -> bool:
    if cutoff is None:
        return True
    if ts_ms <= 0:
        return False
    msg_dt = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=MSK)
    return msg_dt >= cutoff_start_msk(cutoff)


def ozon_iso_after_cutoff(iso: str, cutoff: Optional[dt.date]) -> bool:
    if cutoff is None:
        return True
    s = (iso or "").strip()
    if not s:
        return False
    try:
        msg_dt = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=dt.timezone.utc)
        return msg_dt.astimezone(MSK) >= cutoff_start_msk(cutoff)
    except ValueError:
        return False


def parse_api_error_detail(body: str, *, prefix: str = "") -> str:
    """Извлекает detail/title/error из JSON-тела ответа маркетплейса."""
    raw = (body or "").strip()
    if not raw:
        return prefix or "ошибка API"
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return f"{prefix}{raw[:400]}" if prefix else raw[:400]
    if not isinstance(obj, dict):
        return f"{prefix}{raw[:400]}" if prefix else raw[:400]
    parts: list[str] = []
    for key in ("detail", "title", "error", "message"):
        v = obj.get(key)
        if v is not None and str(v).strip():
            parts.append(str(v).strip())
    errs = obj.get("errors")
    if isinstance(errs, list) and errs:
        parts.append("; ".join(str(x) for x in errs[:5]))
    if parts:
        msg = " — ".join(dict.fromkeys(parts))
        return f"{prefix}{msg}" if prefix else msg
    return f"{prefix}{raw[:400]}" if prefix else raw[:400]
