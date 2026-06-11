"""
Показатели качества для сводки: WB (рейтинг по отзывам), Ozon (рейтинги + индекс ошибок FBS).
"""
from __future__ import annotations

import asyncio
import hashlib
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
_WB_STORE_GAP_SEC = 1.25  # пауза между запросами rating для разных API key
_WB_RATING_CACHE_TTL_SEC = 30 * 60
_WB_RATING_COOLDOWN_SEC = 60
_WB_RATING_COOLDOWN_MSG = (
    "WB разрешает получать рейтинг продавца не чаще одного раза в минуту. "
    "Данные будут обновлены позже."
)
_WB_TOKEN_MSG = 'Нужен сервисный токен WB категории "Вопросы и отзывы".'

# WB seller rating: кэш и cooldown по нормализованному API key (лимит WB: 1 req/min/account)
_WB_RATING_BY_KEY: dict[str, tuple[float, dict]] = {}
_WB_RATING_LAST_OK: dict[str, tuple[float, dict]] = {}
_WB_RATING_COOLDOWN_UNTIL: dict[str, float] = {}
_WB_RATING_COOLDOWN_KIND: dict[str, str] = {}  # "rate" | "auth"
_WB_RATING_LOCKS: dict[str, asyncio.Lock] = {}
_WB_AUTH_ERROR_CACHE_SEC = 5 * 60
_WB_RATING_CLEANUP_MARGIN_SEC = 5 * 60

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

# Сырой JSON в журнал (QUALITY_DEBUG) — по умолчанию выкл.
_QUALITY_DEBUG_LOG = False
_QUALITY_DEBUG_MAX_LEN = 2500

# Точные slug из /v1/rating/summary (см. QUALITY_DEBUG в журнале)
_OZON_SLUG_TO_KEY: dict[str, str] = {
    "rating_order_cancellation_cb": "cancellation",
    "rating_order_cancellation_fbs": "cancellation",
    "rating_order_cancellation_rfbs": "cancellation",
    "rating_order_cancellation_global": "cancellation",
    "rating_shipment_delay_cb": "overdue",
    "rating_shipment_delay_fbs": "overdue",
    "rating_shipment_delay_rfbs": "overdue",
    "rating_shipment_delay_rfbs_sd": "overdue",
}

_OZON_SLUG_PRIORITY: dict[str, tuple[str, ...]] = {
    "cancellation": (
        "rating_order_cancellation_fbs",
        "rating_order_cancellation_rfbs",
        "rating_order_cancellation_cb",
        "rating_order_cancellation_global",
    ),
    "overdue": (
        "rating_shipment_delay_fbs",
        "rating_shipment_delay_rfbs",
        "rating_shipment_delay_rfbs_sd",
        "rating_shipment_delay_cb",
    ),
}


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
    if str(it.get("value_type") or "").upper() in ("PERCENT", "RATIO"):
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
    by_slug: dict[str, dict] = {}
    for it in all_items:
        slug = str(it.get("rating") or "").lower()
        if slug:
            by_slug[slug] = it

    for metric_key, slugs in _OZON_SLUG_PRIORITY.items():
        for slug in slugs:
            it = by_slug.get(slug)
            if it is not None:
                by_key[metric_key] = _ozon_metric_from_item(it, metric_key)
                break

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


def _norm_wb_api_key(key: str) -> str:
    k = (key or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
    if k.lower().startswith("bearer "):
        k = k[7:].strip()
    return k


def _wb_key_log_hash(norm_key: str) -> str:
    if not norm_key:
        return "empty"
    return hashlib.sha256(norm_key.encode("utf-8")).hexdigest()[:10]


def _wb_rating_http_error(e: HttpStatusError) -> str:
    body = (e.body or "").lower()
    if e.status == 429:
        return _WB_RATING_COOLDOWN_MSG
    if e.status in (401, 403) or "personal token" in body:
        return _WB_TOKEN_MSG
    return "Не удалось получить рейтинг продавца WB. Проверьте токен и повторите позже."


def _wb_rating_template_empty(*, error: str = "") -> dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "metrics": [],
        "fetched_at": None,
    }


def _apply_wb_template_to_store(template: dict[str, Any], store: Store) -> dict[str, Any]:
    return {
        "store_id": store.id,
        "store_name": store.name,
        "marketplace": "wb",
        "ok": bool(template.get("ok")),
        "error": str(template.get("error") or ""),
        "metrics": list(template.get("metrics") or []),
        "fetched_at": template.get("fetched_at"),
    }


def _wb_rating_lock_for(norm_key: str) -> asyncio.Lock:
    lock = _WB_RATING_LOCKS.get(norm_key)
    if lock is None:
        lock = asyncio.Lock()
        _WB_RATING_LOCKS[norm_key] = lock
    return lock


def _wb_active_keys_from_stores(wb_stores: list[Store]) -> set[str]:
    return {
        nk for s in wb_stores
        if (nk := _norm_wb_api_key(s.api_key or ""))
    }


def _wb_rating_lazy_cleanup(active_keys: set[str]) -> None:
    """Удалить устаревшие записи для ключей, которых нет среди активных WB-магазинов."""
    now = time.time()
    max_age = _WB_RATING_CACHE_TTL_SEC + _WB_RATING_CLEANUP_MARGIN_SEC

    def _stale(norm_key: str, ts: float) -> bool:
        return norm_key not in active_keys and (now - ts) >= max_age

    for norm_key in list(_WB_RATING_BY_KEY.keys()):
        ts, _ = _WB_RATING_BY_KEY[norm_key]
        if _stale(norm_key, ts):
            _WB_RATING_BY_KEY.pop(norm_key, None)

    for norm_key in list(_WB_RATING_LAST_OK.keys()):
        ts, _ = _WB_RATING_LAST_OK[norm_key]
        if _stale(norm_key, ts):
            _WB_RATING_LAST_OK.pop(norm_key, None)

    for norm_key in list(_WB_RATING_COOLDOWN_UNTIL.keys()):
        if norm_key in active_keys:
            continue
        ts = _WB_RATING_BY_KEY.get(norm_key, (0.0, {}))[0]
        last_ts = _WB_RATING_LAST_OK.get(norm_key, (0.0, {}))[0]
        ref_ts = max(ts, last_ts, _WB_RATING_COOLDOWN_UNTIL.get(norm_key, 0.0) - _WB_RATING_COOLDOWN_SEC)
        if ref_ts and (now - ref_ts) < max_age:
            continue
        _WB_RATING_COOLDOWN_UNTIL.pop(norm_key, None)
        _WB_RATING_COOLDOWN_KIND.pop(norm_key, None)

    for norm_key in list(_WB_RATING_LOCKS.keys()):
        if norm_key in active_keys:
            continue
        lock = _WB_RATING_LOCKS[norm_key]
        if lock.locked():
            continue
        if norm_key in _WB_RATING_BY_KEY or norm_key in _WB_RATING_LAST_OK or norm_key in _WB_RATING_COOLDOWN_UNTIL:
            continue
        _WB_RATING_LOCKS.pop(norm_key, None)


def _wb_rating_cache_get(norm_key: str, now: float) -> Optional[dict[str, Any]]:
    hit = _WB_RATING_BY_KEY.get(norm_key)
    if not hit:
        return None
    ts, payload = hit
    ttl = _WB_AUTH_ERROR_CACHE_SEC if _WB_RATING_COOLDOWN_KIND.get(norm_key) == "auth" else _WB_RATING_CACHE_TTL_SEC
    if (now - ts) >= ttl:
        return None
    return dict(payload)


def _wb_rating_serve_from_last_ok(norm_key: str, store_ids: list[int]) -> Optional[dict[str, Any]]:
    hit = _WB_RATING_LAST_OK.get(norm_key)
    if not hit:
        return None
    fetched_at, payload = hit
    if (time.time() - fetched_at) >= _WB_RATING_CACHE_TTL_SEC:
        return None
    log.info(
        "quality wb rating cooldown key=%s stores=%s (last successful result)",
        _wb_key_log_hash(norm_key),
        store_ids,
    )
    return dict(payload)


def _wb_rating_resolve_cooldown(
    norm_key: str,
    store_ids: list[int],
    now: float,
) -> Optional[dict[str, Any]]:
    """Ответ из cooldown без запроса в WB. None — cooldown не активен."""
    cooldown_until = _WB_RATING_COOLDOWN_UNTIL.get(norm_key, 0.0)
    if cooldown_until <= now:
        return None

    key_hash = _wb_key_log_hash(norm_key)
    kind = _WB_RATING_COOLDOWN_KIND.get(norm_key, "rate")

    if kind == "auth":
        cached = _wb_rating_cache_get(norm_key, now)
        if cached is not None:
            return cached
        return _wb_rating_template_empty(error=_WB_TOKEN_MSG)

    stale = _wb_rating_serve_from_last_ok(norm_key, store_ids)
    if stale is not None:
        return stale
    cached = _wb_rating_cache_get(norm_key, now)
    if cached is not None and cached.get("ok"):
        return cached
    log.info("quality wb rating cooldown key=%s stores=%s", key_hash, store_ids)
    return _wb_rating_template_empty(error=_WB_RATING_COOLDOWN_MSG)


async def _call_wb_rating_api(api_key: str) -> dict[str, Any]:
    """Один запрос GET /api/common/v1/rating. Возвращает шаблон без store_id."""
    key = (api_key or "").strip()
    if not key:
        return _wb_rating_template_empty(error="не задан API-ключ")
    try:
        client = WbClient(key)
        data = await client.get_seller_rating()
        rating: Optional[float] = None
        feedback_count: Optional[int] = None
        valuation = data.get("valuation") if isinstance(data, dict) else None
        count = None
        if isinstance(data, dict):
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
        if rating is None:
            return _wb_rating_template_empty(error="WB: API не вернул рейтинг продавца")
        lvl = _metric_level(rating, warn=_WB_RATING_WARN, danger=_WB_RATING_DANGER, lower_is_better=False)
        extra = f"{feedback_count:,}".replace(",", " ") + " отзывов" if feedback_count is not None else ""
        return {
            "ok": True,
            "error": "",
            "metrics": [
                {
                    "key": "review_rating",
                    "label": "Рейтинг",
                    "value": rating,
                    "unit": "stars",
                    "level": lvl,
                    "hint": "Рейтинг продавца по отзывам (WB API)",
                    "extra": extra,
                }
            ],
            "fetched_at": time.time(),
        }
    except HttpStatusError as e:
        return {
            **_wb_rating_template_empty(error=_wb_rating_http_error(e)),
            "_http_status": e.status,
        }
    except Exception as e:
        log.exception("quality wb rating API failed key=%s", _wb_key_log_hash(_norm_wb_api_key(key)))
        return _wb_rating_template_empty(error=str(e)[:200])


async def _get_wb_rating_for_key(
    norm_key: str,
    api_key: str,
    store_ids: list[int],
    *,
    use_cache: bool,
) -> dict[str, Any]:
    """Рейтинг продавца WB для группы магазинов с одним API key."""
    if not norm_key:
        return _wb_rating_template_empty(error="не задан API-ключ")

    lock = _wb_rating_lock_for(norm_key)
    async with lock:
        now = time.time()
        key_hash = _wb_key_log_hash(norm_key)

        resolved = _wb_rating_resolve_cooldown(norm_key, store_ids, now)
        if resolved is not None:
            return resolved

        if use_cache:
            cached = _wb_rating_cache_get(norm_key, now)
            if cached is not None:
                return cached

        template = await _call_wb_rating_api(api_key)
        now = time.time()
        status = template.get("_http_status")
        clean = {k: v for k, v in template.items() if k != "_http_status"}

        _WB_RATING_BY_KEY[norm_key] = (now, dict(clean))

        if clean.get("ok"):
            _WB_RATING_COOLDOWN_KIND[norm_key] = "rate"
            _WB_RATING_COOLDOWN_UNTIL[norm_key] = now + _WB_RATING_COOLDOWN_SEC
            _WB_RATING_LAST_OK[norm_key] = (now, dict(clean))
            return clean

        if status == 429:
            _WB_RATING_COOLDOWN_KIND[norm_key] = "rate"
            _WB_RATING_COOLDOWN_UNTIL[norm_key] = now + _WB_RATING_COOLDOWN_SEC
            stale = _wb_rating_serve_from_last_ok(norm_key, store_ids)
            if stale is not None:
                return stale
            log.info("quality wb rating 429 key=%s stores=%s", key_hash, store_ids)
            return clean

        if status in (401, 403):
            if clean.get("error") != _WB_TOKEN_MSG:
                clean = {**clean, "error": _WB_TOKEN_MSG}
            _WB_RATING_COOLDOWN_KIND[norm_key] = "auth"
            _WB_RATING_COOLDOWN_UNTIL[norm_key] = now + _WB_AUTH_ERROR_CACHE_SEC
            _WB_RATING_BY_KEY[norm_key] = (now, dict(clean))
            log.info("quality wb rating auth key=%s stores=%s status=%s", key_hash, store_ids, status)
            return clean

        log.warning(
            "quality wb rating error key=%s stores=%s status=%s msg=%s",
            key_hash,
            store_ids,
            status or "?",
            (clean.get("error") or "")[:120],
        )
        return clean


def _wb_key_duplicate_groups(wb_stores: list[Store]) -> list[dict[str, Any]]:
    groups: dict[str, list[Store]] = {}
    for s in wb_stores:
        nk = _norm_wb_api_key(s.api_key or "")
        if not nk:
            continue
        groups.setdefault(nk, []).append(s)
    out: list[dict[str, Any]] = []
    for nk, stores in groups.items():
        if len(stores) < 2:
            continue
        out.append({
            "count": len(stores),
            "store_ids": [int(s.id) for s in stores],
            "key_hash": _wb_key_log_hash(nk),
        })
    return out


async def _fetch_all_wb_quality(wb_stores: list[Store], *, use_cache: bool) -> list[dict]:
    if not wb_stores:
        return []

    groups: dict[str, list[Store]] = {}
    for s in wb_stores:
        nk = _norm_wb_api_key(s.api_key or "")
        groups.setdefault(nk, []).append(s)

    templates: dict[str, dict[str, Any]] = {}
    keys_in_order: list[str] = []
    seen: set[str] = set()
    for s in wb_stores:
        nk = _norm_wb_api_key(s.api_key or "")
        if nk in seen:
            continue
        seen.add(nk)
        keys_in_order.append(nk)

    for i, nk in enumerate(keys_in_order):
        group = groups[nk]
        if not nk:
            templates[nk] = _wb_rating_template_empty(error="не задан API-ключ")
            continue
        store_ids = [int(s.id) for s in group]
        templates[nk] = await _get_wb_rating_for_key(
            nk, group[0].api_key, store_ids, use_cache=use_cache,
        )
        if i + 1 < len(keys_in_order):
            await asyncio.sleep(_WB_STORE_GAP_SEC)

    return [
        _apply_wb_template_to_store(templates[_norm_wb_api_key(s.api_key or "")], s)
        for s in wb_stores
    ]


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


def _ozon_error_index_tariff(index_pct: float) -> float:
    """Множитель платы за ошибки по шкале Ozon (ориентир ЛК)."""
    if index_pct <= 5:
        return 1.0
    if index_pct <= 10:
        return 2.0
    return 3.0


def _parse_ozon_error_index(data: Any) -> Optional[dict]:
    """
    POST /v1/rating/index/fbs/info
    Ответ: {"index": 0.25, "processing_costs_sum": 501.81, "defects": [...], ...}
    index — доля 0–1 (0.25 = 25%).
    """
    if not isinstance(data, dict):
        return None

    block: dict = data
    res = data.get("result")
    if isinstance(res, dict):
        block = res

    raw_idx = block.get("index")
    total: Optional[float] = None
    if isinstance(raw_idx, (int, float)):
        total = float(raw_idx)
    elif isinstance(raw_idx, dict):
        total = _pick_float(raw_idx, "value", "index", "current_value")

    if total is None:
        total = _pick_float(block, "index")

    total = _normalize_ozon_percent(total)
    if total is None:
        return None

    costs: Optional[float] = None
    try:
        if block.get("processing_costs_sum") is not None:
            costs = float(block["processing_costs_sum"])
    except (TypeError, ValueError):
        costs = None

    tariff = _ozon_error_index_tariff(total)
    hint_parts = [f"Индекс ошибок FBS/rFBS · {total:g}%"]
    if costs is not None and costs > 0:
        hint_parts.append(f"плата {costs:,.0f} ₽".replace(",", " "))
    if tariff > 1:
        hint_parts.append(f"тариф ×{tariff:g}")

    extra = f"×{tariff:g}".replace(".0", "") if tariff > 1 else ""

    return {
        "total": total,
        "cancel_component": None,
        "delay_component": None,
        "tariff_multiplier": tariff if tariff > 1 else None,
        "hint": " · ".join(hint_parts),
        "extra": extra,
    }


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
        nk = _norm_wb_api_key(store.api_key or "")
        if not nk:
            result = _apply_wb_template_to_store(_wb_rating_template_empty(error="не задан API-ключ"), store)
        else:
            template = await _get_wb_rating_for_key(nk, store.api_key, [int(store.id)], use_cache=use_cache)
            result = _apply_wb_template_to_store(template, store)
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

    if mp != "wb":
        _CACHE[int(store.id)] = (now, result)
    return result


async def fetch_all_quality(stores: list[Store], *, use_cache: bool = True, active_only: bool = True) -> dict:
    rows = [s for s in stores if (not active_only or s.active)]
    wb_stores = [s for s in rows if (s.marketplace or "").lower() == "wb"]
    ozon_stores = [s for s in rows if (s.marketplace or "").lower() == "ozon"]

    _wb_rating_lazy_cleanup(_wb_active_keys_from_stores(wb_stores))
    wb_results = await _fetch_all_wb_quality(wb_stores, use_cache=use_cache)

    ozon_results: list[dict] = []
    for s in ozon_stores:
        ozon_results.append(await fetch_store_quality(s, use_cache=use_cache))

    return {
        "cache_ttl_sec": _CACHE_TTL_SEC,
        "wb_rating_cache_ttl_sec": _WB_RATING_CACHE_TTL_SEC,
        "wb_rating_cooldown_sec": _WB_RATING_COOLDOWN_SEC,
        "wb": list(wb_results),
        "wb_key_groups": _wb_key_duplicate_groups(wb_stores),
        "ozon": list(ozon_results),
    }
