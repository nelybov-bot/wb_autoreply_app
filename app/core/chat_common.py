"""Общая логика чатов с покупателями (WB, Ozon): дата отсечки, ключ сообщения."""
from __future__ import annotations

import datetime as dt
import json
from typing import Any, Optional, Tuple

from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")
SETTING_REPLY_FROM = "buyer_chat_reply_from_date"
SETTING_AUTO_CHAT_MAX_AGE_DAYS = "buyer_chat_auto_max_age_days"
DEFAULT_AUTO_CHAT_MAX_AGE_DAYS = 3


def parse_reply_from_date(raw: str) -> Optional[dt.date]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        return None


def parse_auto_chat_max_age_days(raw: str) -> int:
    """Сколько дней «свежести» для автоответов в чатах (по умолчанию 3)."""
    s = (raw or "").strip()
    if not s:
        return DEFAULT_AUTO_CHAT_MAX_AGE_DAYS
    try:
        n = int(s)
    except ValueError:
        return DEFAULT_AUTO_CHAT_MAX_AGE_DAYS
    return max(1, min(n, 30))


def wb_ts_within_max_age(ts_ms: int, max_age_days: int, *, now: Optional[dt.datetime] = None) -> bool:
    if max_age_days <= 0 or ts_ms <= 0:
        return False
    ref = now or dt.datetime.now(MSK)
    msg_dt = dt.datetime.fromtimestamp(ts_ms / 1000.0, tz=MSK)
    age_days = (ref - msg_dt).total_seconds() / 86400.0
    return age_days <= float(max_age_days)


def ozon_iso_within_max_age(iso: str, max_age_days: int, *, now: Optional[dt.datetime] = None) -> bool:
    if max_age_days <= 0:
        return False
    s = (iso or "").strip()
    if not s:
        return False
    try:
        msg_dt = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return False
    ref = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    age_days = (ref - msg_dt.astimezone(dt.timezone.utc)).total_seconds() / 86400.0
    return age_days <= float(max_age_days)


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
