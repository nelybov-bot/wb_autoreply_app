"""Сводка заблокированных карточек WB по всем магазинам + текст для Telegram."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from app.db import Database

from .net import HttpStatusError
from .telegram_notify import escape_tg_html
from .wb_analytics_client import WbAnalyticsClient

log = logging.getLogger("wb_banned_cards")

SETTING_ENABLED = "wb_banned_cards_enabled"
SETTING_INTERVAL = "wb_banned_cards_interval"  # legacy, не используется
SETTING_CHAT_ID = "wb_banned_cards_telegram_chat_id"
SETTING_LAST_SENT = "wb_banned_cards_last_sent"
SETTING_LAST_SLOT = "wb_banned_cards_last_slot"
SETTING_LAST_CHECK = "wb_banned_cards_last_check"
SETTING_LAST_SNAPSHOT = "wb_banned_cards_last_snapshot"
SETTING_IN_REPORT = "wb_banned_cards_in_report"

# Плановые сводки (МСК).
DIGEST_HOURS_MSK = (9, 15, 21)

# Живой опрос WB Analytics — раз в час (срочные алерты при изменении).
LIVE_POLL_SECONDS = 60 * 60

# Пауза между магазинами (лимит WB ~1 req / 10 сек на аккаунт).
_STORE_GAP_SEC = 11.0


def banned_cards_enabled(db: Database) -> bool:
    return (db.get_setting(SETTING_ENABLED) or "0").strip() == "1"


def banned_cards_in_report(db: Database) -> bool:
    return (db.get_setting(SETTING_IN_REPORT) or "1").strip() != "0"


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


def summary_to_snapshot(summary: dict[str, Any]) -> dict[str, Any]:
    by_store: dict[str, dict[str, Any]] = {}
    for s in summary.get("stores") or []:
        if s.get("error"):
            continue
        sid = str(int(s.get("store_id") or 0))
        by_store[sid] = {
            "name": str(s.get("store_name") or sid),
            "count": int(s.get("count") or 0),
        }
    return {
        "total": int(summary.get("total") or 0),
        "by_store": by_store,
    }


def load_snapshot(db: Database) -> Optional[dict[str, Any]]:
    raw = (db.get_setting(SETTING_LAST_SNAPSHOT) or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def save_snapshot(db: Database, summary: dict[str, Any]) -> None:
    snap = summary_to_snapshot(summary)
    db.set_setting(SETTING_LAST_SNAPSHOT, json.dumps(snap, ensure_ascii=False))


def compute_banned_delta(
    prev: Optional[dict[str, Any]],
    curr_summary: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Сравнение с прошлым запросом. None — нет предыдущего снимка или изменений нет."""
    if not prev or not isinstance(prev.get("by_store"), dict):
        return None
    curr = summary_to_snapshot(curr_summary)
    prev_by = prev.get("by_store") or {}
    curr_by = curr.get("by_store") or {}
    ids = set(prev_by.keys()) | set(curr_by.keys())
    store_rows: list[dict[str, Any]] = []
    for sid in sorted(ids, key=lambda x: (prev_by.get(x) or curr_by.get(x) or {}).get("name") or x):
        p = prev_by.get(sid) or {}
        c = curr_by.get(sid) or {}
        pc = int(p.get("count") or 0)
        cc = int(c.get("count") or 0)
        if pc == cc and sid in prev_by and sid in curr_by:
            continue
        if sid not in prev_by and cc == 0:
            continue
        if sid not in curr_by and pc == 0:
            continue
        name = str(c.get("name") or p.get("name") or sid)
        store_rows.append({
            "store_id": sid,
            "store_name": name,
            "prev": pc,
            "curr": cc,
            "delta": cc - pc,
        })
    total_prev = int(prev.get("total") or 0)
    total_curr = int(curr.get("total") or 0)
    total_delta = total_curr - total_prev
    if total_delta == 0 and not store_rows:
        return None
    if total_delta > 0:
        kind = "increase"
    elif total_delta < 0:
        kind = "decrease"
    else:
        kind = "reshuffle"
    return {
        "kind": kind,
        "total_prev": total_prev,
        "total_curr": total_curr,
        "total_delta": total_delta,
        "stores": store_rows,
    }


def format_banned_cards_message(summary: dict[str, Any]) -> str:
    """Плановая сводка в беседку (включая total=0)."""
    total = int(summary.get("total") or 0)
    lines = [
        "<b>🚫 Заблокированные карточки WB</b>",
        "<i>плановая сводка 09:00 / 15:00 / 21:00 МСК</i>",
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
    else:
        lines.append("")
        lines.append("<i>Нет активных магазинов WB</i>")
    return "\n".join(lines)


def format_banned_delta_message(delta: dict[str, Any]) -> str:
    """Срочное уведомление при изменении относительно прошлого запроса."""
    kind = delta.get("kind")
    total_prev = int(delta.get("total_prev") or 0)
    total_curr = int(delta.get("total_curr") or 0)
    total_delta = int(delta.get("total_delta") or 0)
    if kind == "increase":
        lines = [
            "<b>🚨 СРОЧНО СТАЛО БОЛЬШЕ ЗАБЛОКИРОВАННЫХ</b>",
            "",
            f"<b>Было:</b> {total_prev} → <b>стало:</b> {total_curr} "
            f"(<b>+{total_delta}</b>)",
        ]
    elif kind == "decrease":
        unlocked = abs(total_delta)
        lines = [
            "<b>✅ РАЗБЛОКИРОВАЛИ КАРТОЧКИ</b>",
            "",
            f"<b>Разблокировано:</b> {unlocked} шт.",
            f"<b>Было:</b> {total_prev} → <b>стало:</b> {total_curr}",
        ]
    else:
        lines = [
            "<b>⚠️ ИЗМЕНИЛОСЬ ПО МАГАЗИНАМ</b>",
            "",
            f"<b>Итого без изменений:</b> {total_curr}",
        ]
    stores = delta.get("stores") or []
    if stores:
        lines.append("")
        for s in stores:
            name = escape_tg_html(str(s.get("store_name") or "—"))
            d = int(s.get("delta") or 0)
            sign = f"+{d}" if d > 0 else str(d)
            lines.append(
                f"· {name}: {int(s.get('prev') or 0)} → {int(s.get('curr') or 0)} "
                f"({sign})"
            )
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


def snapshot_as_report_summary(snap: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Снимок → формат для блока в сводном отчёте (без нового запроса к WB)."""
    if not snap or not isinstance(snap, dict):
        return None
    by_store = snap.get("by_store") or {}
    if not isinstance(by_store, dict):
        by_store = {}
    stores = []
    for sid, row in sorted(
        by_store.items(),
        key=lambda kv: str((kv[1] or {}).get("name") or kv[0]),
    ):
        if not isinstance(row, dict):
            continue
        stores.append({
            "store_id": sid,
            "store_name": str(row.get("name") or sid),
            "count": int(row.get("count") or 0),
            "error": None,
        })
    return {
        "total": int(snap.get("total") or 0),
        "stores": stores,
        "from_cache": True,
    }


def digest_slot_key(now) -> Optional[str]:
    """Ключ слота YYYY-MM-DD|HH, если сейчас ровно час 09/15/21 МСК."""
    if int(now.hour) not in DIGEST_HOURS_MSK:
        return None
    return f"{now.date().isoformat()}|{int(now.hour):02d}"


def due_digest_slot(now, last_slot: str) -> Optional[str]:
    """
    Первый пропущенный/наступивший слот 09/15/21 за сегодня (МСК).

    Если сервис спал и проснулся в 10:30 — вернёт сегодняшний 09,
    чтобы плановая сводка всё равно ушла (catch-up).
    """
    last = (last_slot or "").strip()
    today = now.date().isoformat()
    for h in DIGEST_HOURS_MSK:
        if int(now.hour) < int(h):
            break
        key = f"{today}|{int(h):02d}"
        if last == key:
            continue
        if not last or last < key:
            return key
    return None
