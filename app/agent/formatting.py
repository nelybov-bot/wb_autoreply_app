"""Форматирование ответов агента для UI и Telegram."""
from __future__ import annotations

import html as html_lib
import re

_JSON_PLAN_RE = re.compile(
    r'\{[^{}]*"type"\s*:\s*"(?:tool|message|clarify)"[^{}]*\}',
    re.IGNORECASE | re.DOTALL,
)


def strip_leaked_json(text: str) -> str:
    """Убирает случайно попавший в ответ план агента (JSON)."""
    s = (text or "").strip()
    if not s:
        return ""
    prev = None
    while prev != s:
        prev = s
        s = _JSON_PLAN_RE.sub("", s).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def escape_tg(text: str) -> str:
    return html_lib.escape((text or "").strip() or "—")


def plain_to_telegram_html(text: str) -> str:
    """Простое оформление: заголовки, списки, жирный текст для Telegram HTML."""
    s = strip_leaked_json(text)
    if not s:
        return "—"
    lines_out: list[str] = []
    for line in s.splitlines():
        raw = line.rstrip()
        if not raw.strip():
            lines_out.append("")
            continue
        stripped = raw.strip()
        if stripped.startswith("✅") or stripped.startswith("❌") or stripped.startswith("⚠️"):
            lines_out.append(escape_tg(stripped))
            continue
        if stripped.endswith(":") and len(stripped) < 60 and not stripped.startswith("•"):
            lines_out.append(f"<b>{escape_tg(stripped)}</b>")
            continue
        if stripped.startswith("• ") or stripped.startswith("- "):
            body = stripped[2:].strip()
            lines_out.append(f"• {escape_tg(body)}")
            continue
        if stripped.startswith("Ошибка:"):
            lines_out.append(f"<b>❌ {escape_tg(stripped)}</b>")
            continue
        lines_out.append(escape_tg(stripped))
    return "\n".join(lines_out).strip() or "—"


def compose_reply(intro: str, body: str, *, done_label: str = "") -> str:
    intro = strip_leaked_json(intro)
    body = strip_leaked_json(body)
    parts = []
    if done_label:
        parts.append(done_label)
    if intro:
        parts.append(intro)
    if body:
        parts.append(body)
    return "\n\n".join(parts).strip() or "Готово."
