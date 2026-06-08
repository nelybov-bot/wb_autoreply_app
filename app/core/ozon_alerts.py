"""Ozon: важные уведомления из чатов поддержки (штрафы, ИС, блокировки)."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from ..db import Database
from .chat_common import MSK, parse_reply_from_date
from .openai_client import OpenAIClient
from .telegram_notify import (
    TELEGRAM_PARSE_MODE,
    escape_tg_html,
    resolve_telegram_chat_id,
    send_telegram_message,
    template_uses_html,
)

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
    "⚠️ <b>{telegram_title}</b>\n\n"
    "🏪 <b>Магазин:</b> {store_name}\n"
    "{optional_threat_type}"
    "📅 <b>Срок до:</b> {deadline_html}\n"
    "⚡ <b>Последствия:</b> {consequence}\n"
    "{optional_amount}{optional_product}"
    "\n<blockquote>{summary}</blockquote>\n\n"
    "✅ <b>Действия:</b> {action_needed}\n"
    "🕐 {message_at_html} · {chat_type}"
)

ALERT_CAT_CERT = "cert_request"
ALERT_CAT_HIDDEN = "product_hidden"
ALERT_CAT_THREAT = "threat"
ALERT_CAT_OTHER = "other"

_SKU_RE = re.compile(r"\b(\d{6,12})\b")

_TITLE_RULES: list[tuple[str, str]] = [
    (r"сертификат|документ.{0,20}качеств", "Запрос сертификата качества"),
    (r"интеллектуальн|правообладател", "Нарушение интеллектуальной собственности"),
    (r"фальсификат|подделк", "Подозрение в фальсификации"),
    (r"блокиров|скрыть.{0,15}площадк|снять с продаж", "Угроза снятия с продажи"),
    (r"штраф", "Штраф от Ozon"),
]

JSON_SUFFIX = (
    " Ответь строго одним JSON-объектом, без текста до или после. "
    'Формат: {"important": true или false, '
    '"telegram_title": "заголовок 3–6 слов, напр. Запрос сертификата качества", '
    '"threat_type": "тип до 40 символов или —", '
    '"deadline": "срок, напр. 7 дней или 15.06.2026 или —", '
    '"consequence": "до 50 символов, напр. скрытие товара с площадки или —", '
    '"amount": "сумма штрафа или —", '
    '"product_ref": "SKU и краткое название или —", '
    '"summary": "одно короткое предложение, до 120 символов", '
    '"action_needed": "одна короткая фраза до 100 символов, без путей меню целиком", '
    '"alert_category": "cert_request — запрос сертификата/декларации/документов; '
    'product_hidden — товар уже скрыт/снят с продажи; '
    'threat — угроза скрытия/штрафа (ещё не скрыли); other — прочее"}'
)


def extract_product_skus(*texts: str) -> list[str]:
    """SKU / offer_id из product_ref и текста Ozon (6–12 цифр)."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in texts:
        for m in _SKU_RE.finditer(raw or ""):
            sku = m.group(1)
            if sku not in seen:
                seen.add(sku)
                out.append(sku)
    return out


def _alert_text_blob(*parts: str) -> str:
    return " ".join(x for x in parts if (x or "").strip())


def _has_fine_in_alert(amount: str, *texts: str) -> bool:
    a = (amount or "").strip()
    if a and a not in ("—", "-", "нет", "не указано"):
        return True
    blob = _alert_text_blob(*texts).lower()
    return bool(re.search(r"штраф|балл|₽|\bруб", blob))


def _fine_amount_label(amount: str, *texts: str) -> str:
    a = (amount or "").strip()
    if a and a not in ("—", "-", "нет", "не указано"):
        return _truncate_text(a, 70)
    blob = _alert_text_blob(*texts)
    m = re.search(r"(\d[\d\s]*\s*₽[^\n.]{0,50})", blob)
    if m:
        return _truncate_text(m.group(1).strip(), 70)
    if re.search(r"штраф|балл", blob.lower()):
        return "штраф (сумма в уведомлении)"
    return "сумма не указана"


def _classify_threat_for_report(amount: str, threat_type: str, summary: str, message_text: str) -> tuple[str, str]:
    """
    Угроза для отчёта: либо скрытие без штрафа, либо со штрафом (с суммой).
    Возвращает (kind, amount_label), kind = 'fine' | 'hide'.
    """
    blob = _alert_text_blob(threat_type, summary, message_text)
    if _has_fine_in_alert(amount, blob):
        return "fine", _fine_amount_label(amount, blob)
    return "hide", ""


def classify_alert_category(
    *,
    threat_type: str = "",
    summary: str = "",
    message_text: str = "",
    telegram_title: str = "",
    consequence: str = "",
    alert_category: str = "",
) -> str:
    """Категория для отчётов: запрос документов vs фактическое скрытие."""
    raw_cat = (alert_category or "").strip().lower()
    if raw_cat in (ALERT_CAT_CERT, ALERT_CAT_HIDDEN, ALERT_CAT_THREAT, ALERT_CAT_OTHER):
        return raw_cat
    blob = " ".join(
        x for x in (threat_type, summary, message_text, telegram_title, consequence) if x
    ).lower()
    hidden_markers = (
        r"скрыли\b",
        r"был скрыт",
        r"товар скрыт",
        r"скрыт с",
        r"снят с продаж",
        r"сняли с продаж",
        r"заблокирован",
        r"удалён с",
        r"удален с",
        r"недоступен для покуп",
    )
    for pat in hidden_markers:
        if re.search(pat, blob):
            return ALERT_CAT_HIDDEN
    cert_markers = (
        r"сертификат",
        r"декларац",
        r"документ.{0,20}качеств",
        r"запросил документ",
        r"предоставьте документ",
        r"загрузите документ",
        r"соответств",
    )
    for pat in cert_markers:
        if re.search(pat, blob):
            return ALERT_CAT_CERT
    threat_markers = (
        r"скроют",
        r"скроем",
        r"придётся скрыть",
        r"будет скрыт",
        r"скрыть карточ",
        r"снять с продаж",
        r"снимем",
        r"начисл",
        r"штраф",
        r"претенз",
    )
    for pat in threat_markers:
        if re.search(pat, blob):
            return ALERT_CAT_THREAT
    return ALERT_CAT_OTHER


def enrich_alert_record_fields(
    *,
    threat_type: str,
    product_ref: str,
    summary: str,
    message_text: str,
    parsed: Optional[dict] = None,
) -> tuple[str, str]:
    """(alert_category, product_skus_csv)."""
    p = parsed or {}
    cat = classify_alert_category(
        threat_type=threat_type,
        summary=summary,
        message_text=message_text,
        telegram_title=str(p.get("telegram_title") or ""),
        consequence=str(p.get("consequence") or ""),
        alert_category=str(p.get("alert_category") or ""),
    )
    skus = extract_product_skus(product_ref, message_text, str(p.get("product_ref") or ""))
    return cat, ",".join(skus)


def ozon_product_stats_for_period(
    db: Database,
    since_iso: str,
    until_iso: Optional[str] = None,
) -> dict:
    """
    Уникальные товары по первому уведомлению в категории за интервал [since, until).
    Повторные напоминания Ozon по тому же SKU не увеличивают счётчик в новых сутках/часах.
    """
    from ..db import iso_to_unix

    since_u = iso_to_unix(since_iso)
    until_u = iso_to_unix(until_iso) if until_iso else None
    if since_u <= 0:
        return _empty_ozon_product_stats()
    if until_u is None or until_u <= since_u:
        until_u = since_u + 86400 * 365

    rows = db.list_ozon_alerts_for_product_report()
    first_seen: dict[tuple[int, str, str], dict] = {}

    for row in rows:
        if (row.get("status") or "") == "ignored":
            continue
        cat = (row.get("alert_category") or "").strip()
        if not cat:
            cat = classify_alert_category(
                threat_type=str(row.get("threat_type") or ""),
                summary=str(row.get("summary") or ""),
                message_text=str(row.get("message_text") or ""),
            )
        if cat not in (ALERT_CAT_CERT, ALERT_CAT_HIDDEN, ALERT_CAT_THREAT):
            continue
        skus_raw = (row.get("product_skus") or "").strip()
        if skus_raw:
            skus = [s for s in skus_raw.split(",") if s.strip()]
        else:
            skus = extract_product_skus(
                str(row.get("product_ref") or ""),
                str(row.get("message_text") or ""),
            )
        if not skus:
            continue
        ts_u = iso_to_unix(str(row.get("ts") or ""))
        if ts_u <= 0:
            continue
        store_id = int(row.get("store_id") or 0)
        threat_kind = ""
        fine_amount = ""
        if cat == ALERT_CAT_THREAT:
            threat_kind, fine_amount = _classify_threat_for_report(
                str(row.get("amount") or ""),
                str(row.get("threat_type") or ""),
                str(row.get("summary") or ""),
                str(row.get("message_text") or ""),
            )
        for sku in skus:
            key = (store_id, sku, cat)
            prev = first_seen.get(key)
            if prev is None or ts_u < prev["ts_u"]:
                first_seen[key] = {
                    "ts_u": ts_u,
                    "threat_kind": threat_kind,
                    "fine_amount": fine_amount,
                }

    cert_n = 0
    hidden_n = 0
    threat_hide_n = 0
    threat_fine_n = 0
    threat_fine_by_amount: dict[str, int] = {}
    for (_store, _sku, cat), info in first_seen.items():
        ts_u = info["ts_u"]
        if ts_u < since_u or ts_u >= until_u:
            continue
        if cat == ALERT_CAT_CERT:
            cert_n += 1
        elif cat == ALERT_CAT_HIDDEN:
            hidden_n += 1
        elif cat == ALERT_CAT_THREAT:
            if info.get("threat_kind") == "fine":
                threat_fine_n += 1
                amt = str(info.get("fine_amount") or "сумма не указана")
                threat_fine_by_amount[amt] = threat_fine_by_amount.get(amt, 0) + 1
            else:
                threat_hide_n += 1

    return {
        "ozon_cert_requests_products": cert_n,
        "ozon_hidden_products": hidden_n,
        "ozon_threat_hide_products": threat_hide_n,
        "ozon_threat_fine_products": threat_fine_n,
        "ozon_threat_fine_by_amount": threat_fine_by_amount,
    }


def _empty_ozon_product_stats() -> dict:
    return {
        "ozon_cert_requests_products": 0,
        "ozon_hidden_products": 0,
        "ozon_threat_hide_products": 0,
        "ozon_threat_fine_products": 0,
        "ozon_threat_fine_by_amount": {},
    }


def ozon_alerts_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_ENABLED) or "0").strip() == "1"


def ozon_alerts_telegram_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_TELEGRAM) or "1").strip() != "0"


def ozon_alerts_from_date(db: Database):
    return parse_reply_from_date(db.get_setting(SETTING_FROM_DATE) or "")


def _dash(val: str) -> bool:
    v = (val or "").strip()
    return not v or v in ("—", "-", "нет", "не указано")


def _truncate_text(val: str, max_len: int) -> str:
    s = re.sub(r"\s+", " ", (val or "").strip())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _optional_html_line(label: str, value: str, emoji: str = "") -> str:
    if _dash(value):
        return ""
    return f"{emoji}<b>{label}:</b> {escape_tg_html(value)}\n"


def is_legacy_telegram_template(template: str) -> bool:
    """Старый шаблон с полным текстом Ozon — заменяем на компактный."""
    t = (template or "").strip()
    if not t:
        return False
    if "{message_text}" in t:
        return True
    if "Ozon: важное уведомление" in t:
        return True
    if "Текст:" in t and not template_uses_html(t):
        return True
    if template_uses_html(t) and "<blockquote>" not in t and "{summary}" in t:
        return False
    return False


def get_telegram_template(db: Database) -> str:
    t = (db.get_setting(SETTING_TEMPLATE) or "").strip()
    if not t or is_legacy_telegram_template(t):
        return DEFAULT_TELEGRAM_TEMPLATE
    return t


def _strip_urls(text: str) -> str:
    return re.sub(r"https?://\S+", "", text or "").strip()


def _infer_title(blob: str, threat_type: str, telegram_title: str) -> str:
    title = (telegram_title or "").strip()
    if title and not _dash(title) and len(title) <= 60:
        return title
    low = (blob + " " + threat_type).lower()
    for pattern, label in _TITLE_RULES:
        if re.search(pattern, low):
            return label
    if threat_type and not _dash(threat_type):
        return _truncate_text(threat_type, 50)
    return "Важное уведомление Ozon"


def _infer_deadline(blob: str, deadline: str) -> str:
    if deadline and not _dash(deadline):
        return _truncate_text(deadline, 40)
    m = re.search(r"в течение\s+(\d+)\s+дн", blob, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        word = "день" if n % 10 == 1 and n % 100 != 11 else (
            "дня" if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else "дней"
        )
        return f"{n} {word}"
    m = re.search(r"до\s+(\d{1,2}[.\-/]\d{1,2}(?:[.\-/]\d{2,4})?)", blob, re.IGNORECASE)
    if m:
        return f"до {m.group(1)}"
    return "—"


def _infer_consequence(blob: str, consequence: str) -> str:
    if consequence and not _dash(consequence):
        return _truncate_text(consequence, 60)
    low = blob.lower()
    if re.search(r"скрыть|скроем|скрыти", low):
        return "скрытие товара с площадки"
    if re.search(r"снять с продаж|сняти", low):
        return "снятие с продажи"
    if re.search(r"блокиров", low):
        return "блокировка товара"
    if re.search(r"штраф", low):
        return "штраф"
    return "—"


def _shorten_product_ref(ref: str) -> str:
    s = _strip_urls((ref or "").strip())
    m = re.match(r"^(\d+)\s*[\(—\-]?\s*(.+)$", s)
    if m:
        sku, name = m.group(1), m.group(2).rstrip(")").strip()
        name = _truncate_text(name, 45)
        return f"{sku} — {name}"
    return _truncate_text(s, 70)


def _shorten_action(action: str, blob: str) -> str:
    a = _strip_urls((action or "").strip())
    if len(a) <= 110:
        return a
    low = (a + " " + blob).lower()
    if "сертификат" in low or "документ" in low and "качеств" in low:
        return "Загрузить сертификат в «Товары → Сертификаты» и привязать к товару"
    if "удал" in low or "снять" in low:
        return "Срочно выполнить требование Ozon по товару"
    return _truncate_text(a, 110)


def _shorten_summary(summary: str, blob: str) -> str:
    s = _strip_urls((summary or "").strip())
    if len(s) <= 140:
        return s
    low = blob.lower()
    if "сертификат" in low or ("документ" in low and "качеств" in low):
        return "Покупатель запросил документ качества — нужно загрузить в ЛК."
    return _truncate_text(s, 140)


def normalize_alert_for_telegram(parsed: dict, message_text: str) -> dict:
    """Сжимает поля ИИ + эвристики для короткого Telegram-сообщения."""
    blob = _strip_urls((message_text or "") + "\n" + (parsed.get("summary") or ""))
    threat_type = str(parsed.get("threat_type") or "—").strip() or "—"
    out = dict(parsed)
    out["telegram_title"] = _infer_title(
        blob, threat_type, str(parsed.get("telegram_title") or "")
    )
    out["threat_type"] = _truncate_text(threat_type, 45) if not _dash(threat_type) else "—"
    out["deadline"] = _infer_deadline(blob, str(parsed.get("deadline") or ""))
    out["consequence"] = _infer_consequence(blob, str(parsed.get("consequence") or ""))
    out["product_ref"] = _shorten_product_ref(str(parsed.get("product_ref") or ""))
    out["summary"] = _shorten_summary(str(parsed.get("summary") or ""), blob)
    out["action_needed"] = _shorten_action(str(parsed.get("action_needed") or ""), blob)
    return out


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
    telegram_title: str = "",
    deadline: str = "",
    consequence: str = "",
) -> Tuple[str, str]:
    """Текст для Telegram (HTML, parse_mode=HTML)."""
    template = get_telegram_template(db)
    esc = escape_tg_html

    def _deadline_html(val: str) -> str:
        if _dash(val):
            return "—"
        return f"<b><u>{esc(val.strip())}</u></b>"

    def _message_at_html(val: str) -> str:
        s = (val or "").strip()
        if not s:
            return "—"
        parts = s.split()
        if len(parts) >= 2:
            return f"<b>{esc(parts[0])}</b> {esc(' '.join(parts[1:]))}"
        return esc(s)

    title = _truncate_text(telegram_title or "Важное уведомление Ozon", 60)
    threat_short = _truncate_text(threat_type, 45) if not _dash(threat_type) else ""
    known_titles = {label for _, label in _TITLE_RULES}
    show_type = bool(
        threat_short
        and title not in known_titles
        and threat_short.lower() not in (title or "").lower()
        and len(threat_short) <= 45
    )
    ctx = {
        "store_name": esc(store_name),
        "chat_type": esc(chat_type),
        "message_at": esc(message_at),
        "message_at_html": _message_at_html(message_at),
        "message_text": esc(_truncate_text(message_text, 200)),
        "message_text_short": esc(_truncate_text(message_text, 80)),
        "threat_type": esc(threat_short or "—"),
        "amount": esc(amount),
        "product_ref": esc(product_ref),
        "summary": esc(summary),
        "action_needed": esc(action_needed),
        "telegram_title": esc(title),
        "deadline": esc(deadline if not _dash(deadline) else "—"),
        "deadline_html": _deadline_html(deadline),
        "consequence": esc(consequence if not _dash(consequence) else "—"),
        "optional_threat_type": _optional_html_line("Тип", threat_short, "📋 ") if show_type else "",
        "optional_amount": _optional_html_line("Сумма", amount, "💰 "),
        "optional_product": _optional_html_line("Товар", product_ref, "📦 "),
    }
    try:
        body = template.format(**ctx).strip()
    except KeyError:
        body = DEFAULT_TELEGRAM_TEMPLATE.format(**ctx).strip()
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body, TELEGRAM_PARSE_MODE


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
    threat_type = str(obj.get("threat_type") or "—").strip() or "—"
    return {
        "telegram_title": str(obj.get("telegram_title") or threat_type or "—").strip() or "—",
        "threat_type": threat_type,
        "deadline": str(obj.get("deadline") or "—").strip() or "—",
        "consequence": str(obj.get("consequence") or "—").strip() or "—",
        "amount": str(obj.get("amount") or "—").strip() or "—",
        "product_ref": str(obj.get("product_ref") or "—").strip() or "—",
        "summary": summary,
        "action_needed": str(obj.get("action_needed") or "—").strip() or "—",
        "alert_category": str(obj.get("alert_category") or "").strip().lower(),
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
    alert_category, product_skus = enrich_alert_record_fields(
        threat_type=parsed["threat_type"],
        product_ref=parsed["product_ref"],
        summary=parsed["summary"],
        message_text=message_text,
        parsed=parsed,
    )
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
        alert_category=alert_category,
        product_skus=product_skus,
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
            compact = normalize_alert_for_telegram(parsed, message_text)
            body, parse_mode = render_telegram_message(
                db,
                store_name=store_name,
                chat_type=chat_type,
                message_at=format_message_at_display(message_at),
                message_text=message_text,
                threat_type=compact["threat_type"],
                amount=compact["amount"],
                product_ref=compact["product_ref"],
                summary=compact["summary"],
                action_needed=compact["action_needed"],
                telegram_title=compact.get("telegram_title", ""),
                deadline=compact.get("deadline", ""),
                consequence=compact.get("consequence", ""),
            )
            ok, _ = await send_telegram_message(token, chat_tg, body, parse_mode=parse_mode)
            if ok:
                db.mark_ozon_important_alert_telegram_sent(alert_id)
            else:
                log.warning("ozon_alert telegram send failed alert_id=%s", alert_id)
    return alert_id


def format_message_at_display(iso: str) -> str:
    from .ozon_buyer_chat import format_ozon_datetime_msk

    return format_ozon_datetime_msk(iso) or (iso or "—")
