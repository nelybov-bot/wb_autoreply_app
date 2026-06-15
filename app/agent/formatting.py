"""Форматирование ответов агента для UI и Telegram."""
from __future__ import annotations

import html as html_lib
import re

_JSON_PLAN_RE = re.compile(
    r'\{[^{}]*"type"\s*:\s*"(?:tool|message|clarify)"[^{}]*\}',
    re.IGNORECASE | re.DOTALL,
)
_INLINE_MD_RE = re.compile(
    r"\*\*(.+?)\*\*|__(.+?)__|`([^`]+)`|«([^»]+)»",
)
_SECTION_EMOJI_RE = re.compile(
    r"^[\U0001F300-\U0001FAFF\u2600-\u27BF]",
)
_STORE_LINE_RE = re.compile(r"^(WB|OZON|ЯМ|Ozon|Wildberries)\s+.+", re.IGNORECASE)
_STATUS_PREFIXES = ("✅", "❌", "⚠️", "🔄", "⏳", "⏸", "⏹")
_DONE_LABELS = frozenset({"✅ Готово", "✅ Готово."})
_CONFIRM_FOOTER_RE = re.compile(r"^Подтвердите:\s*.+$", re.IGNORECASE | re.DOTALL)


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


def _highlight_tokens(s: str) -> str:
    """Экранирование фрагмента + подсветка id и #номеров."""
    if not s:
        return ""
    out: list[str] = []
    pos = 0
    for m in re.finditer(
        r"\b(id[=:]\s*)([A-Za-z0-9_-]{4,})\b|#(\d+)\b",
        s,
        flags=re.IGNORECASE,
    ):
        if m.start() > pos:
            out.append(escape_tg(s[pos : m.start()]))
        if m.group(2):
            out.append(f"{escape_tg(m.group(1))}<code>{escape_tg(m.group(2))}</code>")
        elif m.group(3):
            out.append(f"<code>#{m.group(3)}</code>")
        pos = m.end()
    if pos < len(s):
        out.append(escape_tg(s[pos:]))
    return "".join(out)


def _inline_format(text: str) -> str:
    """**жирный**, `код`, «курсив» внутри строки."""
    s = text or ""
    if not s.strip():
        return "—"
    out: list[str] = []
    pos = 0
    for m in _INLINE_MD_RE.finditer(s):
        if m.start() > pos:
            out.append(_highlight_tokens(s[pos : m.start()]))
        if m.group(1) or m.group(2):
            out.append(f"<b>{escape_tg(m.group(1) or m.group(2))}</b>")
        elif m.group(3):
            out.append(f"<code>{escape_tg(m.group(3))}</code>")
        elif m.group(4):
            out.append(f"<i>{escape_tg(m.group(4))}</i>")
        pos = m.end()
    if pos < len(s):
        out.append(_highlight_tokens(s[pos:]))
    return "".join(out) or "—"


def _format_bullet_body(body: str) -> str:
    m = re.match(r"^([^:]{2,42}):\s*(.+)$", body.strip())
    if m and not m.group(1).startswith("http"):
        return f"<b>{escape_tg(m.group(1))}:</b> {_inline_format(m.group(2))}"
    return _inline_format(body)


def _is_section_header(stripped: str) -> bool:
    if stripped in _DONE_LABELS:
        return True
    if stripped.startswith("Итого:"):
        return True
    if _SECTION_EMOJI_RE.match(stripped):
        return True
    if _STORE_LINE_RE.match(stripped) and ":" in stripped:
        return True
    if stripped.endswith(":") and len(stripped) < 72 and not stripped.startswith("•"):
        return True
    return False


def _format_line(raw: str) -> str:
    if not raw.strip():
        return ""
    indent = len(raw) - len(raw.lstrip(" "))
    stripped = raw.strip()

    if _CONFIRM_FOOTER_RE.match(stripped):
        return f"<i>{escape_tg(stripped)}</i>"

    if stripped in _DONE_LABELS:
        return f"<b>{escape_tg(stripped)}</b>"

    if stripped.startswith("Итого:"):
        return f"<b>{_inline_format(stripped)}</b>"

    if stripped.startswith("Ошибка:"):
        return f"<b>❌ {_inline_format(stripped[8:].strip())}</b>"

    for prefix in _STATUS_PREFIXES:
        if stripped.startswith(prefix):
            rest = stripped[len(prefix) :].strip()
            if rest and prefix in ("✅", "❌", "⚠️"):
                return f"{prefix} <b>{_inline_format(rest)}</b>"
            if rest:
                return f"{prefix} {_inline_format(rest)}"
            return escape_tg(stripped)

    if stripped.startswith("…"):
        return f"<i>{escape_tg(stripped)}</i>"

    bullet = ""
    body = stripped
    if stripped.startswith("• "):
        bullet, body = "•", stripped[2:].strip()
    elif stripped.startswith("- "):
        bullet, body = "•", stripped[2:].strip()
    elif stripped.startswith("◦ "):
        bullet, body = "◦", stripped[2:].strip()

    if bullet:
        prefix = "  " if indent >= 2 else ""
        mark = "◦" if indent >= 2 else bullet
        return f"{prefix}{mark} {_format_bullet_body(body)}"

    if _is_section_header(stripped):
        return f"<b>{_inline_format(stripped)}</b>"

    return _inline_format(stripped)


def plain_to_telegram_html(text: str) -> str:
    """Оформление ответа агента для Telegram HTML."""
    s = strip_leaked_json(text)
    if not s:
        return "—"

    blocks = re.split(r"\n{2,}", s)
    html_blocks: list[str] = []
    for block in blocks:
        lines_out: list[str] = []
        for line in block.splitlines():
            formatted = _format_line(line)
            if formatted:
                lines_out.append(formatted)
        if lines_out:
            html_blocks.append("\n".join(lines_out))

    return "\n\n".join(html_blocks).strip() or "—"


def format_agent_telegram_reply(text: str, *, needs_confirm: bool = False) -> str:
    """Полное оформление ответа бота: тело, подтверждение, шапка."""
    body = strip_leaked_json(text)
    if not body:
        return "—"

    confirm_footer = ""
    m = re.search(r"\n\n(Подтвердите:.+)$", body, flags=re.IGNORECASE | re.DOTALL)
    if m:
        confirm_footer = m.group(1).strip()
        body = body[: m.start()].strip()

    parts: list[str] = []
    if needs_confirm:
        parts.append("⚠️ <b>Требуется подтверждение</b>")

    main = plain_to_telegram_html(body)
    if main and main != "—":
        if needs_confirm:
            parts.append(f"<blockquote>{main}</blockquote>")
        else:
            parts.append(main)

    if confirm_footer:
        parts.append(f"<i>{escape_tg(confirm_footer)}</i>")

    return "\n\n".join(parts).strip() or "—"


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
