"""WB: новости портала продавца — фильтр шума, ИИ, Telegram."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from ..db import Database
from .chat_common import parse_reply_from_date
from .openai_client import OpenAIClient
from .telegram_notify import (
    TELEGRAM_PARSE_MODE,
    escape_tg_html,
    resolve_telegram_chat_id,
    send_telegram_message,
)
from .wb_common_client import WbCommonClient

log = logging.getLogger("wb_alerts")

SETTING_ENABLED = "wb_alerts_enabled"
SETTING_FROM_DATE = "wb_alerts_check_from_date"
SETTING_TELEGRAM = "wb_alerts_telegram_enabled"
SETTING_TEMPLATE = "wb_alerts_telegram_template"

DEFAULT_PROMPT = (
    "Ты анализируешь новости портала продавца Wildberries.\n"
    "Определи, требует ли новость СРОЧНОГО внимания продавца: штрафы, блокировки, снятие с продажи, "
    "обязательные документы и сертификаты, претензии, изменения правил с дедлайном и санкциями.\n"
    "Важно: угроза штрафа или блокировки, срочная загрузка документов, скрытие товаров, "
    "обязательные действия до конкретной даты.\n"
    "НЕ важно: маркетинг и реклама сервисов WB, обучение и вебинары без санкций, "
    "общие новости API без дедлайна, поздравления, советы без срока и последствий.\n"
    "Уже отфильтрованы уведомления о новых заказах и скорой отмене — их не анализируй."
)

DEFAULT_TELEGRAM_TEMPLATE = (
    "📢 <b>{telegram_title}</b>\n\n"
    "🏪 <b>Магазин:</b> {store_name}\n"
    "🏷 <b>Категория:</b> {news_types}\n\n"
    "<blockquote>{summary}</blockquote>\n\n"
    "✅ <b>Действия:</b> {action_needed}\n"
    "🕐 {news_date}"
)

JSON_SUFFIX = (
    " Ответь строго одним JSON-объектом, без текста до или после. "
    'Формат: {"important": true или false, '
    '"telegram_title": "заголовок 3–6 слов, напр. Запрос сертификата", '
    '"summary": "суть новости до 140 символов", '
    '"action_needed": "что сделать продавцу или —"}'
)

# Новые заказы / сборочные задания — шум для раздела «важные уведомления».
_RE_NEW_ORDER = re.compile(
    r"(нов(ый|ые|ая|ое)\s+заказ"
    r"|поступил[аи]?\s+(новый\s+)?заказ"
    r"|поступил[аи]?\s+сборочн"
    r"|нов(ое|ая|ые)\s+сборочн"
    r"|сборочн(ое|ая|ые)\s+задани"
    r"|новое\s+задани[ея]\s+на\s+сборк"
    r"|заказ\s+№?\s*\d+.{0,40}(поступил|оформлен|создан))",
    re.I,
)

# Скоро отменятся / риск автоотмены.
_RE_CANCEL_SOON = re.compile(
    r"(скоро\s+.{0,24}отмен"
    r"|отменится"
    r"|будет\s+отмен"
    r"|ожидает\s+отмен"
    r"|авто.?отмен"
    r"|риск\s+отмен"
    r"|отмен[аы]\s+заказ"
    r"|заказ.{0,30}отмен)",
    re.I,
)

_EXCLUDED_TYPE_KEYWORDS = (
    "заказ",
    "сбороч",
    "fbs",
    "доставк",
)


def wb_alerts_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_ENABLED) or "0").strip() == "1"


def wb_alerts_telegram_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_TELEGRAM) or "1").strip() != "0"


def wb_alerts_from_date(db: Database) -> Optional[str]:
    raw = (db.get_setting(SETTING_FROM_DATE) or "").strip()
    d = parse_reply_from_date(raw)
    return d.isoformat() if d else None


def resolve_wb_alerts_from_date(
    db: Database,
    override: Optional[str] = None,
) -> tuple[str, str]:
    """Дата начала выборки (ISO) и источник: override | setting | default."""
    if override:
        d = parse_reply_from_date(str(override).strip())
        if d:
            return d.isoformat(), "override"
    configured = wb_alerts_from_date(db)
    if configured:
        return configured, "setting"
    from datetime import date, timedelta

    return (date.today() - timedelta(days=30)).isoformat(), "default"


def _news_on_or_after(item: dict, cutoff_iso: str) -> bool:
    cutoff = parse_reply_from_date(cutoff_iso)
    if not cutoff:
        return True
    item_d = parse_reply_from_date(str(item.get("date") or "")[:10])
    if not item_d:
        return True
    return item_d >= cutoff


def get_wb_telegram_template(db: Database) -> str:
    t = (db.get_setting(SETTING_TEMPLATE) or "").strip()
    return t or DEFAULT_TELEGRAM_TEMPLATE


def _truncate_text(val: str, max_len: int) -> str:
    s = re.sub(r"\s+", " ", (val or "").strip())
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _news_blob(item: dict) -> str:
    header = str(item.get("header") or "")
    content = str(item.get("content") or "")
    types = item.get("types") or []
    type_names = []
    if isinstance(types, list):
        for t in types:
            if isinstance(t, dict) and t.get("name"):
                type_names.append(str(t["name"]))
    return f"{header}\n{content}\n{' '.join(type_names)}"


def is_excluded_wb_news(item: dict) -> bool:
    """Исключить уведомления о новых заказах и скорой отмене."""
    blob = _news_blob(item)
    if _RE_NEW_ORDER.search(blob):
        return True
    if _RE_CANCEL_SOON.search(blob):
        return True
    types = item.get("types") or []
    if isinstance(types, list):
        for t in types:
            if not isinstance(t, dict):
                continue
            name = str(t.get("name") or "").lower()
            if any(k in name for k in _EXCLUDED_TYPE_KEYWORDS):
                return True
    return False


def format_wb_news_types(item: dict) -> str:
    types = item.get("types") or []
    if not isinstance(types, list):
        return ""
    names = [str(t.get("name") or "").strip() for t in types if isinstance(t, dict)]
    names = [n for n in names if n]
    return ", ".join(names)


def truncate_text(text: str, limit: int = 280) -> str:
    return _truncate_text(text, limit)


def format_wb_news_date_display(iso: str) -> str:
    from .ozon_buyer_chat import format_ozon_datetime_msk

    return format_ozon_datetime_msk(iso) or (iso or "—")


def parse_wb_alert_json(txt: str) -> Optional[dict]:
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
    title = str(obj.get("telegram_title") or "").strip()
    return {
        "telegram_title": title or _truncate_text(summary, 60),
        "summary": summary,
        "action_needed": str(obj.get("action_needed") or "—").strip() or "—",
    }


def render_wb_telegram_message(
    db: Database,
    *,
    store_name: str,
    news_date: str,
    news_types: str,
    header: str,
    summary: str,
    action_needed: str,
    telegram_title: str,
) -> Tuple[str, str]:
    template = get_wb_telegram_template(db)
    esc = escape_tg_html
    title = _truncate_text(telegram_title or header or "Новость WB", 60)
    ctx = {
        "store_name": esc(store_name),
        "news_date": esc(format_wb_news_date_display(news_date)),
        "news_types": esc(news_types or "—"),
        "header": esc(_truncate_text(header, 120)),
        "summary": esc(summary),
        "action_needed": esc(action_needed),
        "telegram_title": esc(title),
    }
    try:
        body = template.format(**ctx).strip()
    except KeyError:
        body = DEFAULT_TELEGRAM_TEMPLATE.format(**ctx).strip()
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body, TELEGRAM_PARSE_MODE


async def classify_wb_portal_news(
    db: Database,
    client: OpenAIClient,
    *,
    store_name: str,
    header: str,
    content: str,
    news_date: str,
    types_label: str,
) -> tuple[Optional[dict], bool, str]:
    """Возвращает (результат, пометить_как_ignored, источник)."""
    task = db.get_prompt("wb_important_alert", "general")
    if not task.strip():
        task = DEFAULT_PROMPT
    user = (
        f"{task}\n\n"
        f"Магазин: {store_name}\n"
        f"Дата новости: {news_date or '—'}\n"
        f"Категории: {types_label or '—'}\n\n"
        f"Заголовок:\n{header}\n\n"
        f"Текст новости:\n{content}\n\n"
        f"{JSON_SUFFIX}"
    )
    try:
        txt = await client.generate(
            "Ты помощник продавца на Wildberries. Отвечай только JSON.",
            user,
        )
    except Exception as e:
        log.warning("wb_alert classify failed: %s", e)
        return None, False, "ai_failed"
    parsed = parse_wb_alert_json(txt)
    if parsed:
        return parsed, False, "ai"
    return None, True, "ai"


async def _send_wb_alert_telegram(
    db: Database,
    *,
    alert_id: int,
    news_id: int,
    store_name: str,
    news_date: str,
    news_types: str,
    header: str,
    parsed: dict,
) -> bool:
    if not wb_alerts_telegram_enabled(db):
        return False
    if db.wb_portal_news_telegram_already_sent(news_id, exclude_alert_id=alert_id):
        db.mark_wb_portal_alert_telegram_sent(alert_id)
        return True
    token = (db.get_setting("telegram_bot_token") or "").strip()
    chat_tg = resolve_telegram_chat_id(db, "wb_alerts")
    if not token or not chat_tg:
        return False
    body, parse_mode = render_wb_telegram_message(
        db,
        store_name=store_name,
        news_date=news_date,
        news_types=news_types,
        header=header,
        summary=parsed["summary"],
        action_needed=parsed["action_needed"],
        telegram_title=parsed.get("telegram_title", ""),
    )
    ok, _ = await send_telegram_message(token, chat_tg, body, parse_mode=parse_mode, db=db)
    if ok:
        db.mark_wb_portal_alert_telegram_sent(alert_id)
    else:
        log.warning("wb_alert telegram send failed alert_id=%s", alert_id)
    return ok


async def flush_pending_wb_alert_telegrams(
    db: Database,
    *,
    store_id: int,
    store_name: str,
) -> int:
    sent = 0
    for row in db.list_wb_portal_alerts_pending_telegram(store_id):
        parsed = {
            "telegram_title": row.get("telegram_title") or row.get("summary") or "",
            "summary": row.get("summary") or "—",
            "action_needed": row.get("action_needed") or "—",
        }
        if await _send_wb_alert_telegram(
            db,
            alert_id=int(row["id"]),
            news_id=int(row.get("news_id") or 0),
            store_name=store_name,
            news_date=str(row.get("news_date") or ""),
            news_types=str(row.get("types_label") or ""),
            header=str(row.get("header") or ""),
            parsed=parsed,
        ):
            sent += 1
    return sent


async def maybe_record_wb_alert(
    db: Database,
    parsed: dict,
    *,
    store_id: int,
    news_id: int,
    header: str,
    content: str,
    news_date: str,
    types_json: str,
    types_label: str,
    store_name: str,
) -> Optional[int]:
    if db.has_wb_portal_news_id(news_id):
        return None
    alert_id = db.add_wb_portal_alert(
        store_id=store_id,
        news_id=news_id,
        header=header,
        content=content,
        news_date=news_date,
        types_json=types_json,
        status="new",
        summary=parsed["summary"],
        action_needed=parsed["action_needed"],
        telegram_title=parsed.get("telegram_title", ""),
    )
    try:
        await _send_wb_alert_telegram(
            db,
            alert_id=alert_id,
            news_id=news_id,
            store_name=store_name,
            news_date=news_date,
            news_types=types_label,
            header=header,
            parsed=parsed,
        )
    except Exception:
        log.exception("wb_alert telegram store=%s news=%s", store_id, news_id)
    return alert_id


async def scan_wb_portal_news_for_store(
    db: Database,
    store,
    *,
    openai_key: str = "",
    rescan: bool = False,
    from_date_override: Optional[str] = None,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "wb_alert_fetched": 0,
        "wb_alert_new": 0,
        "wb_alert_excluded": 0,
        "wb_alert_skipped_old": 0,
        "wb_alert_ai_ignored": 0,
        "wb_alert_ai_failed": 0,
        "wb_alert_ai_calls": 0,
        "wb_alert_skip_reason": "",
        "wb_alert_ignored_cleared": 0,
        "wb_alert_telegram_resent": 0,
        "wb_alert_duplicate": 0,
        "wb_alert_from_date": "",
        "wb_alert_from_date_source": "",
    }
    if rescan:
        stats["wb_alert_ignored_cleared"] = db.clear_wb_ignored_alerts(store.id)
    if not wb_alerts_enabled(db):
        stats["wb_alert_skip_reason"] = "disabled"
        return stats
    if store.marketplace != "wb":
        stats["wb_alert_skip_reason"] = "not_wb"
        return stats
    if not (store.api_key or "").strip():
        stats["wb_alert_skip_reason"] = "no_keys"
        return stats
    key = (openai_key or "").strip()
    if not key:
        stats["wb_alert_skip_reason"] = "no_openai_key"
        return stats

    store_name = store.name or f"WB #{store.id}"
    stats["wb_alert_telegram_resent"] = await flush_pending_wb_alert_telegrams(
        db, store_id=store.id, store_name=store_name
    )

    from_date, from_source = resolve_wb_alerts_from_date(db, from_date_override)
    stats["wb_alert_from_date"] = from_date
    stats["wb_alert_from_date_source"] = from_source

    client = WbCommonClient(store.api_key)
    oai = OpenAIClient(key)
    try:
        items = await client.list_news(from_date=from_date)
    except Exception as e:
        log.exception("wb_alerts fetch store=%s", store.id)
        stats["wb_alert_skip_reason"] = "fetch_error"
        stats["error"] = str(e)[:300]
        return stats

    stats["wb_alert_fetched"] = len(items)
    for item in items:
        if not _news_on_or_after(item, from_date):
            stats["wb_alert_skipped_old"] += 1
            continue
        news_id = item.get("id")
        if news_id is None:
            continue
        try:
            nid = int(news_id)
        except (TypeError, ValueError):
            continue
        if db.has_wb_portal_news_id(nid):
            if not db.has_wb_portal_alert(store.id, nid):
                stats["wb_alert_duplicate"] += 1
            continue

        header = str(item.get("header") or "")
        content = str(item.get("content") or "")
        news_date = str(item.get("date") or "")
        types_json = json.dumps(item.get("types") or [], ensure_ascii=False)
        types_label = format_wb_news_types(item)

        if is_excluded_wb_news(item):
            stats["wb_alert_excluded"] += 1
            db.add_wb_portal_alert(
                store_id=store.id,
                news_id=nid,
                header=header,
                content=content,
                news_date=news_date,
                types_json=types_json,
                status="ignored",
            )
            continue

        try:
            parsed, mark_ignored, classify_source = await classify_wb_portal_news(
                db,
                oai,
                store_name=store_name,
                header=header,
                content=content,
                news_date=news_date,
                types_label=types_label,
            )
            if classify_source in ("ai", "ai_failed"):
                stats["wb_alert_ai_calls"] += 1
        except Exception:
            log.exception("wb_alert classify store=%s news=%s", store.id, nid)
            stats["wb_alert_ai_failed"] += 1
            continue

        if not parsed:
            if mark_ignored:
                stats["wb_alert_ai_ignored"] += 1
                db.add_wb_portal_alert(
                    store_id=store.id,
                    news_id=nid,
                    header=header,
                    content=content,
                    news_date=news_date,
                    types_json=types_json,
                    status="ignored",
                )
            else:
                stats["wb_alert_ai_failed"] += 1
            continue

        alert_id = await maybe_record_wb_alert(
            db,
            parsed,
            store_id=store.id,
            news_id=nid,
            header=header,
            content=content,
            news_date=news_date,
            types_json=types_json,
            types_label=types_label,
            store_name=store_name,
        )
        if alert_id:
            stats["wb_alert_new"] += 1
    return stats


async def auto_process_wb_portal_news(
    db: Database,
    *,
    store_ids: Optional[List[int]] = None,
    openai_key: str = "",
) -> Dict[str, Any]:
    stores = [s for s in db.list_stores() if s.active and s.marketplace == "wb"]
    if store_ids:
        wanted = {int(x) for x in store_ids}
        stores = [s for s in stores if s.id in wanted]
    total_new = 0
    per_store: List[Dict[str, Any]] = []
    for store in stores:
        part = await scan_wb_portal_news_for_store(db, store, openai_key=openai_key)
        per_store.append({"store_id": store.id, **part})
        total_new += int(part.get("wb_alert_new") or 0)
    return {"wb_alert_new": total_new, "stores": per_store}
