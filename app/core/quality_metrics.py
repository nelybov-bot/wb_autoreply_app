"""
Показатели качества для сводки: WB (рейтинг по отзывам), Ozon (рейтинги + индекс ошибок FBS).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from app.core.net import HttpStatusError
from app.core.ozon_client import OzonClient
from app.core.wb_client import WbClient
from app.db import Store

log = logging.getLogger("quality")

_CACHE: dict[int, tuple[float, dict]] = {}
_CACHE_TTL_SEC = 30 * 60

# Пороги для подсветки (Ozon FBS, ориентиры из ЛК)
_OZON_CANCEL_WARN = 1.5
_OZON_CANCEL_DANGER = 2.0
_OZON_OVERDUE_WARN = 4.0
_OZON_OVERDUE_DANGER = 5.0
_OZON_ERROR_INDEX_WARN = 2.0
_OZON_ERROR_INDEX_DANGER = 10.0

_WB_RATING_WARN = 4.0
_WB_RATING_DANGER = 3.5


def _metric_level(value: Optional[float], *, warn: float, danger: float, lower_is_better: bool = True) -> str:
    if value is None:
        return "na"
    if lower_is_better:
        if value >= danger:
            return "danger"
        if value >= warn:
            return "warn"
        return "ok"
    if value <= danger:
        return "danger"
    if value <= warn:
        return "warn"
    return "ok"


def _ozon_status_level(status: str) -> str:
    st = (status or "").strip().upper()
    if st == "CRITICAL":
        return "danger"
    if st == "WARNING":
        return "warn"
    if st == "OK":
        return "ok"
    return "na"


def _find_ozon_item(items: list[dict], *needles: str) -> Optional[dict]:
    needles_l = [n.lower() for n in needles if n]
    for it in items or []:
        if not isinstance(it, dict):
            continue
        blob = " ".join(
            str(it.get(k) or "")
            for k in ("rating", "name", "rating_name")
        ).lower()
        if any(n in blob for n in needles_l):
            return it
    return None


def _parse_ozon_summary(data: dict) -> list[dict]:
    metrics: list[dict] = []
    groups = data.get("groups") if isinstance(data, dict) else None
    if not isinstance(groups, list):
        return metrics

    all_items: list[dict] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        items = g.get("items")
        if isinstance(items, list):
            all_items.extend(x for x in items if isinstance(x, dict))

    cancel_it = _find_ozon_item(all_items, "cancel", "отмен")
    if cancel_it:
        val = cancel_it.get("current_value")
        try:
            v = float(val) if val is not None else None
        except (TypeError, ValueError):
            v = None
        lvl = _ozon_status_level(str(cancel_it.get("status") or ""))
        if lvl == "na":
            lvl = _metric_level(v, warn=_OZON_CANCEL_WARN, danger=_OZON_CANCEL_DANGER)
        metrics.append({
            "key": "cancellation",
            "label": str(cancel_it.get("name") or "Процент отмен"),
            "value": v,
            "unit": "percent",
            "level": lvl,
            "status": cancel_it.get("status"),
        })

    overdue_it = _find_ozon_item(
        all_items,
        "delay",
        "shipment",
        "late",
        "overdue",
        "просроч",
        "отгруз",
    )
    if overdue_it:
        try:
            v = float(overdue_it.get("current_value")) if overdue_it.get("current_value") is not None else None
        except (TypeError, ValueError):
            v = None
        lvl = _ozon_status_level(str(overdue_it.get("status") or ""))
        if lvl == "na":
            lvl = _metric_level(v, warn=_OZON_OVERDUE_WARN, danger=_OZON_OVERDUE_DANGER)
        metrics.append({
            "key": "overdue",
            "label": str(overdue_it.get("name") or "Просроченные отгрузки"),
            "value": v,
            "unit": "percent",
            "level": lvl,
            "status": overdue_it.get("status"),
        })

    on_time_it = _find_ozon_item(all_items, "on_time", "вовремя", "rating_on_time")
    if on_time_it:
        try:
            v = float(on_time_it.get("current_value")) if on_time_it.get("current_value") is not None else None
        except (TypeError, ValueError):
            v = None
        lvl = _ozon_status_level(str(on_time_it.get("status") or ""))
        if lvl == "na" and v is not None:
            lvl = _metric_level(100.0 - v, warn=5.0, danger=10.0) if v < 100 else "ok"
        metrics.append({
            "key": "on_time",
            "label": str(on_time_it.get("name") or "Заказы вовремя"),
            "value": v,
            "unit": "percent",
            "level": lvl,
            "status": on_time_it.get("status"),
        })

    return metrics


def _extract_error_index(data: Any) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    block = data.get("result") if isinstance(data.get("result"), dict) else data
    for key in (
        "index",
        "error_index",
        "index_value",
        "value",
        "rating_index",
        "current_value",
        "localization_index",
    ):
        raw = block.get(key) if isinstance(block, dict) else None
        if raw is None:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


async def _fetch_wb_quality(store: Store) -> dict:
    out: dict[str, Any] = {
        "store_id": store.id,
        "store_name": store.name,
        "marketplace": "wb",
        "ok": False,
        "error": "",
        "metrics": [],
        "fetched_at": None,
    }
    key = (store.api_key or "").strip()
    if not key:
        out["error"] = "не задан API-ключ"
        return out
    try:
        client = WbClient(key)
        data = await client.get_seller_rating()
        valuation = data.get("valuation")
        count = data.get("feedbackCount") or data.get("feedback_count")
        try:
            rating = float(valuation) if valuation is not None else None
        except (TypeError, ValueError):
            rating = None
        try:
            feedback_count = int(count) if count is not None else None
        except (TypeError, ValueError):
            feedback_count = None
        lvl = _metric_level(rating, warn=_WB_RATING_WARN, danger=_WB_RATING_DANGER, lower_is_better=False)
        out["metrics"] = [
            {
                "key": "review_rating",
                "label": "Рейтинг по отзывам",
                "value": rating,
                "unit": "stars",
                "level": lvl,
                "extra": f"{feedback_count:,}".replace(",", " ") + " отзывов" if feedback_count is not None else "",
            }
        ]
        out["ok"] = rating is not None
        out["fetched_at"] = time.time()
    except HttpStatusError as e:
        out["error"] = f"WB API {e.status}"
        log.warning("quality wb store_id=%s: HTTP %s", store.id, e.status)
    except Exception as e:
        out["error"] = str(e)[:200]
        log.exception("quality wb store_id=%s failed", store.id)
    return out


async def _fetch_ozon_quality(store: Store) -> dict:
    out: dict[str, Any] = {
        "store_id": store.id,
        "store_name": store.name,
        "marketplace": "ozon",
        "ok": False,
        "error": "",
        "metrics": [],
        "fetched_at": None,
    }
    cid = (store.client_id or "").strip()
    key = (store.api_key or "").strip()
    if not cid or not key:
        out["error"] = "не задан Client ID или API-ключ"
        return out
    try:
        client = OzonClient(cid, key)
        summary = await client.rating_summary()
        metrics = _parse_ozon_summary(summary if isinstance(summary, dict) else {})

        try:
            idx_data = await client.rating_index_fbs_info()
            idx_val = _extract_error_index(idx_data)
            if idx_val is not None:
                metrics.append({
                    "key": "error_index",
                    "label": "Индекс ошибок FBS",
                    "value": idx_val,
                    "unit": "percent",
                    "level": _metric_level(
                        idx_val,
                        warn=_OZON_ERROR_INDEX_WARN,
                        danger=_OZON_ERROR_INDEX_DANGER,
                    ),
                })
        except HttpStatusError as e:
            log.warning("quality ozon index fbs store_id=%s: HTTP %s", store.id, e.status)
        except Exception:
            log.exception("quality ozon index fbs store_id=%s failed", store.id)

        out["metrics"] = metrics
        out["ok"] = bool(metrics)
        out["fetched_at"] = time.time()
        if not metrics:
            out["error"] = "нет данных (возможно, магазин без FBS-отправлений)"
    except HttpStatusError as e:
        out["error"] = f"Ozon API {e.status}"
        log.warning("quality ozon store_id=%s: HTTP %s", store.id, e.status)
    except Exception as e:
        out["error"] = str(e)[:200]
        log.exception("quality ozon store_id=%s failed", store.id)
    return out


async def fetch_store_quality(store: Store, *, use_cache: bool = True) -> dict:
    now = time.time()
    if use_cache:
        hit = _CACHE.get(int(store.id))
        if hit and (now - hit[0]) < _CACHE_TTL_SEC:
            return hit[1]

    mp = (store.marketplace or "").strip().lower()
    if mp == "wb":
        result = await _fetch_wb_quality(store)
    elif mp == "ozon":
        result = await _fetch_ozon_quality(store)
    else:
        result = {
            "store_id": store.id,
            "store_name": store.name,
            "marketplace": mp,
            "ok": False,
            "error": "показатели для этого маркетплейса не поддерживаются",
            "metrics": [],
            "fetched_at": None,
        }

    _CACHE[int(store.id)] = (now, result)
    return result


async def fetch_all_quality(stores: list[Store], *, use_cache: bool = True, active_only: bool = True) -> dict:
    rows = [s for s in stores if (not active_only or s.active)]
    wb_stores = [s for s in rows if (s.marketplace or "").lower() == "wb"]
    ozon_stores = [s for s in rows if (s.marketplace or "").lower() == "ozon"]

    import asyncio

    wb_results = await asyncio.gather(*[fetch_store_quality(s, use_cache=use_cache) for s in wb_stores])
    ozon_results = await asyncio.gather(*[fetch_store_quality(s, use_cache=use_cache) for s in ozon_stores])

    return {
        "cache_ttl_sec": _CACHE_TTL_SEC,
        "wb": list(wb_results),
        "ozon": list(ozon_results),
    }
