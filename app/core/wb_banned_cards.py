"""Сводка заблокированных карточек WB по всем магазинам + текст для Telegram."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from app.db import Database

from .net import HttpStatusError
from .telegram_notify import escape_tg_html
from .wb_analytics_client import WbAnalyticsClient

log = logging.getLogger("wb_banned_cards")

SETTING_ENABLED = "wb_banned_cards_enabled"
SETTING_INTERVAL = "wb_banned_cards_interval"
SETTING_CHAT_ID = "wb_banned_cards_telegram_chat_id"
SETTING_LAST_SENT = "wb_banned_cards_last_sent"
SETTING_IN_REPORT = "wb_banned_cards_in_report"

# Пауза между магазинами (лимит WB ~1 req / 10 сек на аккаунт).
_STORE_GAP_SEC = 11.0


def banned_cards_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_ENABLED) or "0").strip() == "1"


def banned_cards_in_report(db: Database) -> bool:
    return (db.get_setting(SETTING_IN_REPORT) or "1").strip() != "0"


def banned_cards_interval(db: Database) -> str:
    interval = (db.get_setting(SETTING_INTERVAL) or "hour").strip()
    return "day" if interval == "day" else "hour"


async def count_banned_for_store(api_key: str) -> int:
    client = WbAnalyticsClient(api_key)
    rows = await client.list_banned_blocked()
    return len(rows)


async def collect_banned_cards_summary(db: Database) -> dict[str, Any]:
    """
    Обходит активные WB-магазины и считает заблокированные карточки.
    Всегда возвращает total (включая 0). Ошибки магазина — в stores[].error.
    """
    stores = [
        s
        for s in db.list_stores()
        if (s.marketplace or "").strip().lower() == "wb" and bool(s.active)
    ]
    out_stores: list[dict[str, Any]] = []
    total = 0
    for i, store in enumerate(stores):
        if i > 0:
            await asyncio.sleep(_STORE_GAP_SEC)
        row: dict[str, Any] = {
            "store_id": int(store.id),
            "store_name": store.name or f"#{store.id}",
            "count": 0,
            "error": None,
        }
        key = (store.api_key or "").strip()
        if not key:
            row["error"] = "нет API-ключа"
            out_stores.append(row)
            continue
        try:
            n = await count_banned_for_store(key)
            row["count"] = int(n)
            total += int(n)
        except HttpStatusError as e:
            msg = f"HTTP {e.status}"
            if e.status in (401, 403):
                msg = "нет доступа к Analytics API (проверьте категорию «Аналитика» у токена)"
            row["error"] = msg
            log.warning("wb_banned_cards store=%s: %s", store.id, msg)
        except Exception as e:
            row["error"] = str(e)[:160]
            log.exception("wb_banned_cards store=%s failed", store.id)
        out_stores.append(row)
    return {
        "total": total,
        "stores": out_stores,
        "stores_ok": sum(1 for s in out_stores if not s.get("error")),
        "stores_failed": sum(1 for s in out_stores if s.get("error")),
        "stores_total": len(out_stores),
    }


def format_banned_cards_message(summary: dict[str, Any]) -> str:
    """Отдельное сообщение в беседку (включая total=0)."""
    total = int(summary.get("total") or 0)
    lines = [
        "<b>🚫 Заблокированные карточки WB</b>",
        "",
        f"<b>Итого:</b> {total}",
    ]
    stores = summary.get("stores") or []
    if stores:
        lines.append("")
        for s in stores:
            name = escape_tg_html(str(s.get("store_name") or "—"))
            err = s.get("error")
            if err:
                lines.append(f"· {name}: <i>ошибка — {escape_tg_html(str(err))}</i>")
            else:
                lines.append(f"· {name}: {int(s.get('count') or 0)}")
    elif not stores:
        lines.append("")
        lines.append("<i>Нет активных магазинов WB</i>")
    return "\n".join(lines)


def format_banned_cards_report_block(summary: Optional[dict[str, Any]]) -> list[str]:
    """Фрагмент для вставки в периодический сводный отчёт."""
    if not summary:
        return []
    total = int(summary.get("total") or 0)
    lines = ["", f"<b>Заблокированные карточки WB:</b> {total}"]
    for s in summary.get("stores") or []:
        name = escape_tg_html(str(s.get("store_name") or "—"))
        err = s.get("error")
        if err:
            lines.append(f"  · {name}: <i>ошибка</i>")
        else:
            lines.append(f"  · {name}: {int(s.get('count') or 0)}")
    return lines
