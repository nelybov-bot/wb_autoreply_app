"""Маскирование секретов в API, логах и аудите."""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

# Ключи настроек, которые не отдаём целиком в GET /api/settings.
SECRET_SETTING_KEYS = frozenset({"openai_key", "telegram_bot_token"})

# Поля в JSON/словарях, которые редактируем перед логом или аудитом.
SENSITIVE_FIELD_NAMES = frozenset(
    {
        "openai_key",
        "api_key",
        "telegram_bot_token",
        "password",
        "token",
        "authorization",
        "client_secret",
        "secret",
    }
)

MASK_BULLETS = "••••••••"
REDACTED = "[скрыто]"

_MASK_DISPLAY_RE = re.compile(r"^[•·\*]{4,}\w{0,8}$")

# Типичные фрагменты ключей в текстах ошибок API.
_REDACT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"), "sk-[скрыто]"),
    (re.compile(r"\bBearer\s+[A-Za-z0-9._\-+/=]{8,}\b", re.I), "Bearer [скрыто]"),
    (re.compile(r"\bApi-Key[:\s]+[A-Za-z0-9._\-+/=]{8,}\b", re.I), "Api-Key: [скрыто]"),
    (re.compile(r"\b\d{8,10}:[A-Za-z0-9_-]{20,}\b"), "[telegram-token]"),
]


def mask_secret_display(value: Optional[str]) -> str:
    """Для ответа API: не отдавать полный секрет."""
    v = (value or "").strip()
    if not v:
        return ""
    if len(v) <= 4:
        return MASK_BULLETS
    return f"{MASK_BULLETS}{v[-4:]}"


def is_masked_display(value: Optional[str]) -> bool:
    s = (value or "").strip()
    if not s:
        return False
    if s == MASK_BULLETS:
        return True
    return bool(_MASK_DISPLAY_RE.match(s))


def should_preserve_secret(new_value: Optional[str], old_value: Optional[str]) -> bool:
    """Пустое или замаскированное поле при сохранении — не затирать старый секрет."""
    old = (old_value or "").strip()
    if not old:
        return False
    new = (new_value or "").strip()
    if not new:
        return True
    if new == old:
        return True
    if is_masked_display(new):
        return True
    if new == mask_secret_display(old):
        return True
    return False


def resolve_secret_setting(new_value: Optional[str], old_value: Optional[str]) -> str:
    if should_preserve_secret(new_value, old_value):
        return (old_value or "").strip()
    return (new_value or "").strip()


def mask_settings_for_api(raw: Dict[str, str]) -> Dict[str, str]:
    """Секреты не отдаём даже частично — только флаг «задан»."""
    out: Dict[str, str] = {}
    for key, val in raw.items():
        if key in SECRET_SETTING_KEYS:
            v = (val or "").strip()
            out[key] = ""
            out[f"{key}_set"] = "1" if v else "0"
        else:
            out[key] = val or ""
    return out


def redact_secrets_in_text(text: Optional[str], *, max_len: int = 2000) -> str:
    s = str(text or "")[:max_len]
    for pat, repl in _REDACT_PATTERNS:
        s = pat.sub(repl, s)
    return s


def sanitize_for_audit(data: Any, *, depth: int = 0) -> Any:
    if depth > 6:
        return "[…]"
    if isinstance(data, dict):
        out: Dict[str, Any] = {}
        for k, v in data.items():
            key = str(k).lower()
            if key in SENSITIVE_FIELD_NAMES or key.endswith("_key") or key.endswith("_token"):
                out[k] = REDACTED if v else ""
            else:
                out[k] = sanitize_for_audit(v, depth=depth + 1)
        return out
    if isinstance(data, list):
        return [sanitize_for_audit(x, depth=depth + 1) for x in data[:50]]
    if isinstance(data, str) and len(data) > 200:
        return redact_secrets_in_text(data, max_len=200)
    return data
