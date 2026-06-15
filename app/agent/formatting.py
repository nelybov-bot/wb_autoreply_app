"""Форматирование ответов агента для UI и Telegram."""
from __future__ import annotations

import html as html_lib
import re

_JSON_PLAN_RE = re.compile(
    r'\{[^{}]*"type"\s*:\s*"(?:tool|message|clarify)"[^{}]*\}',
    re.IGNORECASE | re.DOTALL,
)
_INLINE_MD_RE = re.compile(
    r"\*\*(.+?)\*\*|__(.+?)__|\*([^*\n]+?)\*|_([^_\n]+?)_|`([^`]+)`|«([^»]+)»",
)
_SECTION_EMOJI_RE = re.compile(r"^[\U0001F300-\U0001FAFF\u2600-\u27BF]")
_STORE_LINE_RE = re.compile(r"^(WB|OZON|ЯМ|Ozon|Wildberries)\s+.+", re.IGNORECASE)
_STATUS_PREFIXES = ("✅", "❌", "⚠️", "🔄", "⏳", "⏸", "⏹")
_DONE_LABELS = frozenset({"✅ Готово", "✅ Готово."})
_CONFIRM_FOOTER_RE = re.compile(r"^Подтвердите:\s*.+$", re.IGNORECASE | re.DOTALL)
_MARKETPLACE_RE = re.compile(r"\b(WB|OZON|Ozon|ЯМ|Wildberries)\b", re.IGNORECASE)


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
    if text is None:
        return "—"
    return html_lib.escape(text)


def display_or_dash(text: str) -> str:
    s = (text or "").strip()
    return escape_tg(s) if s else "—"


def _section_title(text: str) -> str:
    return f"<b><u>{escape_tg(text.strip())}</u></b>"


def _bold_if_number(text: str) -> str:
    v = (text or "").strip()
    if re.fullmatch(r"[\d.,]+", v) or re.fullmatch(r"\d+/\d+", v):
        return f"<b>{escape_tg(v)}</b>"
    return _enrich_inline(v)


def _enrich_inline(s: str) -> str:
    """Жирные числа, маркетплейсы, курсив в скобках и [статусах]."""
    if not s:
        return ""
    out: list[str] = []
    pos = 0
    pattern = re.compile(
        r"\b(id[=:]\s*)([A-Za-z0-9_-]{4,})\b"
        r"|#(\d+)\b"
        r"|\[([^\]]+)\]"
        r"|⭐(\d+(?:[.,]\d+)?)"
        r"|\b(WB|OZON|Ozon|ЯМ|Wildberries)\b"
        r"|(\d+/\d+)"
        r"|\(([^)]+)\)"
        r"|(\b\d+\b)",
        re.IGNORECASE,
    )
    for m in pattern.finditer(s):
        if m.start() > pos:
            chunk = s[pos : m.start()]
            out.append(escape_tg(chunk))
        if m.group(2):
            out.append(f"{escape_tg(m.group(1))}<code>{escape_tg(m.group(2))}</code>")
        elif m.group(3):
            out.append(f"<code>#{m.group(3)}</code>")
        elif m.group(4):
            out.append(f"<i>[{escape_tg(m.group(4))}]</i>")
        elif m.group(5):
            out.append(f"⭐<b>{escape_tg(m.group(5))}</b>")
        elif m.group(6):
            out.append(f"<b>{escape_tg(m.group(6).upper() if m.group(6).upper() in ('WB', 'OZON', 'ЯМ') else m.group(6))}</b>")
        elif m.group(7):
            out.append(f"<b>{escape_tg(m.group(7))}</b>")
        elif m.group(8):
            inner = m.group(8).strip()
            if re.fullmatch(r"\d+", inner):
                out.append(f"({escape_tg(inner)})")
            else:
                out.append(f"<i>({escape_tg(inner)})</i>")
        elif m.group(9):
            out.append(f"<b>{escape_tg(m.group(9))}</b>")
        pos = m.end()
    if pos < len(s):
        out.append(escape_tg(s[pos:]))
    return "".join(out)


def _inline_format(text: str) -> str:
    """**жирный**, *курсив*, `код`, «курсив»."""
    s = text or ""
    if not s.strip():
        return "—"
    out: list[str] = []
    pos = 0
    for m in _INLINE_MD_RE.finditer(s):
        if m.start() > pos:
            out.append(_enrich_inline(s[pos : m.start()]))
        if m.group(1) or m.group(2):
            out.append(f"<b>{escape_tg(m.group(1) or m.group(2))}</b>")
        elif m.group(3) or m.group(4):
            out.append(f"<i>{escape_tg(m.group(3) or m.group(4))}</i>")
        elif m.group(5):
            out.append(f"<code>{escape_tg(m.group(5))}</code>")
        elif m.group(6):
            out.append(f"<i>«{escape_tg(m.group(6))}»</i>")
        pos = m.end()
    if pos < len(s):
        out.append(_enrich_inline(s[pos:]))
    return "".join(out) or "—"


def _format_metric_value(val: str) -> str:
    val = (val or "").strip()
    if not val:
        return "—"
    if "|" in val:
        return " | ".join(_format_metric_value(p.strip()) for p in val.split("|"))
    m = re.match(r"^([^:]{1,28}):\s*(.+)$", val)
    if m and not m.group(1).startswith("http"):
        return f"<i>{escape_tg(m.group(1).strip())}:</i> {_bold_if_number(m.group(2).strip())}"
    m2 = re.match(r"^(\d+)\s+(.+?)\s+из\s+(\d+)$", val)
    if m2:
        return (
            f"<b>{escape_tg(m2.group(1))}</b> "
            f"<i>{escape_tg(m2.group(2))}</i> "
            f"из <b>{escape_tg(m2.group(3))}</b>"
        )
    return _enrich_inline(val) if not re.search(r"[*_`«]", val) else _inline_format(val)


def _format_bullet_body(body: str) -> str:
    body = body.strip()
    m = re.match(r"^([^:]{2,42}):\s*(.+)$", body)
    if m and not m.group(1).startswith("http"):
        return f"<b>{escape_tg(m.group(1))}:</b> {_format_metric_value(m.group(2))}"
    m2 = re.match(r"^(.+?)\s+—\s+(.+)$", body)
    if m2:
        return f"<b>{escape_tg(m2.group(1).strip())}</b> — {_format_metric_value(m2.group(2))}"
    return _inline_format(body)


def _format_sub_line(body: str) -> str:
    if " — " in body:
        title, tail = body.split(" — ", 1)
        return f"<i>{_inline_format(title.strip())}</i> — {_format_metric_value(tail)}"
    if body.startswith("«") or ":" in body[:50]:
        return f"<i>{_inline_format(body)}</i>"
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


def _format_status_line(prefix: str, rest: str) -> str:
    if prefix in ("✅", "❌", "⚠️"):
        return f"{prefix} <b>{_inline_format(rest)}</b>"
    if prefix == "🔄" and rest.lower().startswith("задача"):
        m = re.match(r"(?i)задача\s+(.+)", rest)
        if m:
            return f"🔄 <b>Задача</b> <code>{escape_tg(m.group(1).strip())}</code>"
    return f"{prefix} {_inline_format(rest)}"


def _format_line(raw: str) -> str:
    if not raw.strip():
        return ""
    indent = len(raw) - len(raw.lstrip(" "))
    stripped = raw.strip()

    if _CONFIRM_FOOTER_RE.match(stripped):
        return f"<i>{escape_tg(stripped)}</i>"

    if stripped in _DONE_LABELS:
        return _section_title(stripped)

    if stripped.startswith("Итого:"):
        return _section_title(stripped)

    if stripped.startswith("Ошибка:"):
        return f"<b>❌ {_inline_format(stripped[8:].strip())}</b>"

    for prefix in _STATUS_PREFIXES:
        if stripped.startswith(prefix):
            rest = stripped[len(prefix) :].strip()
            if rest:
                return _format_status_line(prefix, rest)
            return escape_tg(stripped)

    if stripped.startswith("…"):
        return f"<i>{escape_tg(stripped)}</i>"

    if indent >= 2:
        body = stripped
        if body.startswith("• "):
            body = body[2:].strip()
        elif body.startswith("◦ "):
            body = body[2:].strip()
        return f"  ◦ {_format_sub_line(body)}"

    bullet = ""
    body = stripped
    if stripped.startswith("• "):
        bullet, body = "•", stripped[2:].strip()
    elif stripped.startswith("- "):
        bullet, body = "•", stripped[2:].strip()
    elif stripped.startswith("◦ "):
        bullet, body = "◦", stripped[2:].strip()

    if bullet:
        return f"{bullet} {_format_bullet_body(body)}"

    if _is_section_header(stripped):
        return _section_title(stripped)

    if _STORE_LINE_RE.match(stripped) and ":" in stripped:
        mp, _, tail = stripped.partition(":")
        return f"<b>{escape_tg(mp.strip())}:</b> {_format_metric_value(tail)}"

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
        parts.append("⚠️ <b><u>Требуется подтверждение</u></b>")

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
