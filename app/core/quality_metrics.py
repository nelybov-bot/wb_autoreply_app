"""
Показатели качества для сводки: WB (рейтинг по отзывам), Ozon (рейтинги + индекс ошибок FBS).
"""
from __future__ import annotations

import asyncio
import json
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
_WB_STORE_GAP_SEC = 1.25  # не бить 6 ключей параллельно — у WB общий лимитер

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


_OZON_METRIC_LABELS = {
    "cancellation": "Отмены",
    "overdue": "Просрочки",
    "error_index": "Индекс",
}

# Временный лог сырого JSON Ozon (смотреть в «Журнал» по QUALITY_DEBUG)
_QUALITY_DEBUG_LOG = True
_QUALITY_DEBUG_MAX_LEN = 2500


def _classify_ozon_rating_item(it: dict) -> Optional[str]:
    """Отмены и просрочки — разные rating-slug в /v1/rating/summary."""
    rating = str(it.get("rating") or "").lower()
    name = str(it.get("name") or "").lower()

    if rating in (
        "rating_global_cancellation",
        "rating_global_cancellation_percent",
        "rating_cancellation_global",
        "rating_fbs_cancellation",
        "rating_fbs_cancellation_percent",
        "rating_order_cancellation",
        "rating_order_cancel",
        "rating_cancellation",
        "rating_fbs_order_cancellation",
    ):
        return "cancellation"
    if rating in (
        "rating_global_late_shipment",
        "rating_late_shipment_global",
        "rating_global_shipment_delay",
        "rating_fbs_late_shipment",
        "rating_fbs_late_shipment_percent",
        "rating_shipment_delay",
        "rating_fbs_shipment_delay",
        "rating_order_shipment_delay",
    ):
        return "overdue"

    if any(tok in rating for tok in (
        "late_shipment",
        "shipment_late",
        "shipment_delay",
        "overdue_shipment",
        "late_delivery",
        "delay_shipment",
    )):
        return "overdue"
    if "просроч" in name and "отмен" not in name:
        return "overdue"
    if ("late" in rating or "delay" in rating or "overdue" in rating) and "cancel" not in rating:
        return "overdue"

    if any(tok in rating for tok in (
        "cancellation",
        "canceled",
        "cancelled",
        "order_cancel",
        "cancel_global",
        "cancel_percent",
    )):
        return "cancellation"
    if "отмен" in name and "просроч" not in name:
        return "cancellation"
    if "cancel" in rating and "late" not in rating and "delay" not in rating:
        return "cancellation"

    return None


def _ozon_metric_from_item(it: dict, key: str) -> dict:
    try:
        v = float(it.get("current_value")) if it.get("current_value") is not None else None
    except (TypeError, ValueError):
        v = None
    if str(it.get("value_type") or "").upper() == "PERCENT":
        v = _normalize_ozon_percent(v)

    lvl = _ozon_status_level(str(it.get("status") or ""))
    if lvl == "na":
        if key == "cancellation":
            lvl = _metric_level(v, warn=_OZON_CANCEL_WARN, danger=_OZON_CANCEL_DANGER)
        elif key == "overdue":
            lvl = _metric_level(v, warn=_OZON_OVERDUE_WARN, danger=_OZON_OVERDUE_DANGER)

    api_name = str(it.get("name") or "").strip()
    rating_hint = {
        "cancellation": "Рейтинг продавца · риск блокировки (~2%)",
        "overdue": "Рейтинг продавца · просрочки отгрузки (~5%)",
    }.get(key, api_name)
    return {
        "key": key,
        "label": _OZON_METRIC_LABELS.get(key, api_name or key),
        "hint": f"{rating_hint}. {api_name}".strip() if api_name else rating_hint,
        "value": v,
        "unit": "percent",
        "level": lvl,
        "status": it.get("status"),
        "rating": it.get("rating"),
    }


def _parse_ozon_summary(data: dict) -> list[dict]:
    groups = data.get("groups") if isinstance(data, dict) else None
    if not isinstance(groups, list):
        return []

    all_items: list[dict] = []
    for g in groups:
        if not isinstance(g, dict):
            continue
        items = g.get("items")
        if isinstance(items, list):
            all_items.extend(x for x in items if isinstance(x, dict))

    by_key: dict[str, dict] = {}
    for it in all_items:
        key = _classify_ozon_rating_item(it)
        if not key or key in by_key:
            continue
        by_key[key] = _ozon_metric_from_item(it, key)

    if not by_key.get("cancellation") or not by_key.get("overdue"):
        for it in all_items:
            r = str(it.get("rating") or "").lower()
            n = str(it.get("name") or "").lower()
            if not by_key.get("cancellation") and (
                "отмен" in n or "cancellation" in n or "cancel" in r
            ) and "просроч" not in n and "delay" not in r and "shipment" not in r:
                by_key["cancellation"] = _ozon_metric_from_item(it, "cancellation")
            if not by_key.get("overdue") and (
                "просроч" in n
                or "shipment_delay" in r
                or "late_shipment" in r
                or ("отгруз" in n and ("просроч" in n or "своеврем" not in n))
            ):
                by_key["overdue"] = _ozon_metric_from_item(it, "overdue")

    if not by_key.get("cancellation") and all_items:
        slugs = [
            f"{x.get('rating') or '?'}:{x.get('name') or '?'}"
            for x in all_items
        ]
        log.info("quality ozon: cancellation not found, items=%s", slugs)

    # Фиксированный порядок: сначала отмены, потом просрочки
    return [by_key[k] for k in ("cancellation", "overdue") if k in by_key]


def _quality_debug_log(store_id: int, label: str, payload: Any) -> None:
    if not _QUALITY_DEBUG_LOG:
        return
    try:
        txt = json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        txt = str(payload)
    if len(txt) > _QUALITY_DEBUG_MAX_LEN:
        txt = txt[:_QUALITY_DEBUG_MAX_LEN] + "…"
    log.info("QUALITY_DEBUG store_id=%s %s: %s", store_id, label, txt)


def _normalize_ozon_percent(value: Optional[float]) -> Optional[float]:
    """Привести к шкале 0–100: Ozon API часто отдаёт долю (0.25 = 25%)."""
    if value is None:
        return None
    av = abs(value)
    if 0 < av < 1:
        return round(value * 100, 4)
    return value


def _wb_rating_http_error(e: HttpStatusError) -> str:
    body = (e.body or "").lower()
    if "personal token" in body:
        return "Сервисный токен WB «Вопросы и отзывы»"
    if e.status == 403:
        return "Нет доступа к рейтингу (403)"
    if e.status == 401:
        return "Токен WB не принят (401)"
    if e.status == 429:
        return "Лимит запросов WB (429)"
    return f"WB API {e.status}"


def _pick_float(d: dict, *keys: str) -> Optional[float]:
    for k in keys:
        raw = d.get(k)
        if raw is None:
            continue
        if isinstance(raw, dict):
            for nk in ("value", "index", "percent", "current_value", "index_value"):
                if nk in raw:
                    raw = raw[nk]
                    break
            else:
                continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _pick_nested_float(block: dict, parent_keys: tuple[str, ...], value_keys: tuple[str, ...]) -> Optional[float]:
    for pk in parent_keys:
        sub = block.get(pk)
        if isinstance(sub, dict):
            v = _pick_float(sub, *value_keys)
            if v is not None:
                return v
    return None


def _parse_ozon_error_index(data: Any) -> Optional[dict]:
    """
    POST /v1/rating/index/fbs/info — итоговый индекс, компоненты (просрочки/отмены), множитель платы.
    Имена полей в API могут отличаться — подбираем по списку + временный QUALITY_DEBUG лог.
    """
    if not isinstance(data, dict):
        return None

    block: dict = data
    res = data.get("result")
    if isinstance(res, dict):
        block = res

    nested = block
    for nest_key in ("index", "error_index", "rating_index", "fbs_index"):
        sub = block.get(nest_key)
        if isinstance(sub, dict):
            nested = sub
            break

    total = _pick_float(
        block,
        "total_index",
        "error_index_value",
        "fbs_error_index",
        "total",
        "index_value",
        "rating_index",
        "current_value",
        "value",
    )
    if total is None:
        total = _pick_float(
            nested,
            "total_index",
            "error_index_value",
            "index_value",
            "value",
            "current_value",
            "index",
        )

    cancel = _pick_nested_float(
        block,
        ("cancellation", "cancel", "canceled", "cancelled", "cancellation_index", "index_cancellation"),
        ("index", "value", "percent", "current_value", "index_value", "cancellation_index", "cancel_index"),
    )
    if cancel is None:
        cancel = _pick_float(
            block,
            "cancellation_index",
            "cancel_index",
            "index_cancellation",
            "cancellation_percent",
            "cancel_percent",
            "canceled_percent",
        )

    delay = _pick_nested_float(
        block,
        ("delay", "late", "overdue", "shipment_delay", "late_shipment", "delay_index", "shipment"),
        ("index", "value", "percent", "current_value", "index_value", "delay_index", "late_index"),
    )
    if delay is None:
        delay = _pick_float(
            block,
            "delay_index",
            "late_index",
            "overdue_index",
            "shipment_delay_index",
            "late_shipment_index",
            "delay_percent",
            "overdue_percent",
        )

    tariff = _pick_float(
        block,
        "tariff_multiplier",
        "multiplier",
        "fee_multiplier",
        "payment_multiplier",
        "tariff_coefficient",
        "coefficient",
        "tariff",
    )
    if tariff is None:
        tariff = _pick_nested_float(
            block,
            ("tariff", "payment", "fee"),
            ("multiplier", "coefficient", "value", "tariff_multiplier"),
        )

    total = _normalize_ozon_percent(total)
    cancel = _normalize_ozon_percent(cancel)
    delay = _normalize_ozon_percent(delay)

    if total is None and cancel is None and delay is None:
        return None

    hint_parts: list[str] = ["Индекс ошибок FBS/rFBS за 14 дней"]
    if delay is not None:
        hint_parts.append(f"за просрочки {delay:g}%")
    if cancel is not None:
        hint_parts.append(f"за отмены {cancel:g}%")
    if tariff is not None and tariff > 1:
        hint_parts.append(f"плата ×{tariff:g}")

    extra = ""
    if tariff is not None and tariff > 1:
        extra = f"×{tariff:g}".replace(".0", "")

    return {
        "total": total,
        "cancel_component": cancel,
        "delay_component": delay,
        "tariff_multiplier": tariff,
        "hint": " · ".join(hint_parts),
        "extra": extra,
    }


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
        rating: Optional[float] = None
        feedback_count: Optional[int] = None
        rating_hint = "Рейтинг продавца по отзывам (WB API)"

        # Только GET /api/common/v1/rating — как на странице продавца WB.
        # Очередь неотвеченных отзывов (isAnswered=false) к рейтингу магазина не относится.
        try:
            data = await client.get_seller_rating()
            _quality_debug_log(store.id, "wb_rating", data)
            valuation = data.get("valuation")
            count = data.get("feedbackCount") or data.get("feedback_count")
            if valuation is not None:
                try:
                    rating = float(valuation)
                except (TypeError, ValueError):
                    rating = None
            if count is not None:
                try:
                    feedback_count = int(count)
                except (TypeError, ValueError):
                    feedback_count = None
        except HttpStatusError as e:
            _quality_debug_log(store.id, "wb_rating_error", {"status": e.status, "body": (e.body or "")[:800]})
            out["error"] = _wb_rating_http_error(e)
            log.warning("quality wb store_id=%s: rating HTTP %s", store.id, e.status)
            return out

        if rating is None:
            out["error"] = "WB: API не вернул рейтинг продавца"
            return out

        lvl = _metric_level(rating, warn=_WB_RATING_WARN, danger=_WB_RATING_DANGER, lower_is_better=False)
        extra = f"{feedback_count:,}".replace(",", " ") + " отзывов" if feedback_count is not None else ""
        out["metrics"] = [
            {
                "key": "review_rating",
                "label": "Рейтинг",
                "value": rating,
                "unit": "stars",
                "level": lvl,
                "hint": rating_hint,
                "extra": extra,
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
        if isinstance(summary, dict):
            _quality_debug_log(store.id, "ozon_rating_summary", summary)
        metrics = _parse_ozon_summary(summary if isinstance(summary, dict) else {})

        try:
            idx_data = await client.rating_index_fbs_info()
            _quality_debug_log(store.id, "ozon_index_fbs_info", idx_data)
            parsed_idx = _parse_ozon_error_index(idx_data)
            if parsed_idx and parsed_idx.get("total") is not None:
                idx_val = parsed_idx["total"]
                metrics.append({
                    "key": "error_index",
                    "label": _OZON_METRIC_LABELS["error_index"],
                    "value": idx_val,
                    "unit": "percent",
                    "level": _metric_level(
                        idx_val,
                        warn=_OZON_ERROR_INDEX_WARN,
                        danger=_OZON_ERROR_INDEX_DANGER,
                    ),
                    "hint": parsed_idx.get("hint") or "Индекс ошибок FBS/rFBS",
                    "extra": parsed_idx.get("extra") or "",
                    "components": {
                        "delay": parsed_idx.get("delay_component"),
                        "cancel": parsed_idx.get("cancel_component"),
                    },
                    "tariff_multiplier": parsed_idx.get("tariff_multiplier"),
                })
            elif parsed_idx:
                log.warning(
                    "quality ozon index fbs store_id=%s: parsed without total, parts=%s",
                    store.id,
                    parsed_idx,
                )
        except HttpStatusError as e:
            _quality_debug_log(store.id, "ozon_index_fbs_error", {"status": e.status, "body": (e.body or "")[:800]})
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

    wb_results: list[dict] = []
    for i, s in enumerate(wb_stores):
        wb_results.append(await fetch_store_quality(s, use_cache=use_cache))
        if i + 1 < len(wb_stores):
            await asyncio.sleep(_WB_STORE_GAP_SEC)

    ozon_results: list[dict] = []
    for s in ozon_stores:
        ozon_results.append(await fetch_store_quality(s, use_cache=use_cache))

    return {
        "cache_ttl_sec": _CACHE_TTL_SEC,
        "wb": list(wb_results),
        "ozon": list(ozon_results),
    }
