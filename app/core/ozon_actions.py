"""Ozon акции: список, отбор автоакций, удаление товаров, синхронизация по порогу скидки."""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .net import HttpStatusError
from .ozon_buyer_chat import ozon_feature_unavailable_user_message, ozon_http_skip_reason
from .ozon_client import OzonClient

log = logging.getLogger("ozon_actions")


def is_ozon_auto_add_action(action: dict) -> bool:
    """Акция с автодобавлением товаров (Ozon сам добавляет SKU)."""
    if not isinstance(action, dict):
        return False
    auto_dates = action.get("auto_add_dates") or []
    return isinstance(auto_dates, list) and len(auto_dates) > 0


def action_participating_count(action: dict) -> int:
    if not isinstance(action, dict):
        return 0
    try:
        return int(action.get("participating_products_count") or 0)
    except (TypeError, ValueError):
        return 0


def normalize_action_row(action: dict) -> dict:
    """Плоский словарь для API/UI."""
    if not isinstance(action, dict):
        return {}
    aid = action.get("id")
    try:
        action_id = int(aid) if aid is not None else None
    except (TypeError, ValueError):
        action_id = None
    return {
        "id": action_id,
        "title": str(action.get("title") or "").strip() or f"Акция {action_id or '?'}",
        "action_type": str(action.get("action_type") or "").strip(),
        "description": str(action.get("description") or "").strip(),
        "date_start": str(action.get("date_start") or "").strip(),
        "date_end": str(action.get("date_end") or "").strip(),
        "freeze_date": str(action.get("freeze_date") or "").strip(),
        "is_participating": bool(action.get("is_participating")),
        "is_voucher_action": bool(action.get("is_voucher_action")),
        "with_targeting": bool(action.get("with_targeting")),
        "potential_products_count": int(action.get("potential_products_count") or 0),
        "participating_products_count": action_participating_count(action),
        "banned_products_count": int(action.get("banned_products_count") or 0),
        "auto_add_dates": action.get("auto_add_dates") if isinstance(action.get("auto_add_dates"), list) else [],
        "is_auto_add": is_ozon_auto_add_action(action),
        "discount_type": str(action.get("discount_type") or "").strip(),
        "discount_value": action.get("discount_value"),
    }


def pick_actions_for_removal(
    actions: List[dict],
    *,
    only_auto_add: bool = True,
    action_ids: Optional[List[int]] = None,
    require_participating: bool = True,
) -> List[dict]:
    """Отбор акций для удаления товаров."""
    rows = [a for a in (actions or []) if isinstance(a, dict)]
    if action_ids:
        id_set = {int(x) for x in action_ids}
        rows = [a for a in rows if int(a.get("id") or 0) in id_set]
    elif only_auto_add:
        rows = [a for a in rows if is_ozon_auto_add_action(a)]
    if require_participating:
        rows = [a for a in rows if action_participating_count(a) > 0]
    return rows


async def list_all_action_products(client: OzonClient, action_id: int, *, max_pages: int = 50) -> List[dict]:
    """Все участвующие товары акции (пагинация /v1/actions/products)."""
    products: List[dict] = []
    last_id: Any = None
    for _ in range(max(1, max_pages)):
        block = await client.list_action_products(int(action_id), limit=100, last_id=last_id)
        chunk = block.get("products") or []
        if isinstance(chunk, list):
            products.extend([p for p in chunk if isinstance(p, dict)])
        nxt = block.get("last_id")
        if nxt in (None, "", 0):
            break
        if last_id is not None and nxt == last_id:
            break
        last_id = nxt
    return products


async def remove_products_from_actions(
    client: OzonClient,
    action_ids: List[int],
    *,
    batch_size: int = 100,
) -> Dict[str, Any]:
    """Удалить все участвующие товары из указанных акций."""
    stats: Dict[str, Any] = {
        "actions_processed": 0,
        "actions_skipped_empty": 0,
        "products_removed": 0,
        "products_rejected": 0,
        "action_ids": [],
        "errors": [],
    }
    for raw_id in action_ids or []:
        try:
            aid = int(raw_id)
        except (TypeError, ValueError):
            continue
        try:
            products = await list_all_action_products(client, aid)
        except Exception as e:
            log.warning("ozon_actions list products action=%s: %s", aid, e)
            stats["errors"].append({"action_id": aid, "error": str(e)[:300]})
            continue
        pids: List[int] = []
        for p in products:
            pid = p.get("id")
            if pid is None:
                pid = p.get("product_id")
            if pid is None:
                continue
            try:
                pids.append(int(pid))
            except (TypeError, ValueError):
                continue
        if not pids:
            stats["actions_skipped_empty"] += 1
            continue
        removed_total = 0
        rejected_total = 0
        for i in range(0, len(pids), batch_size):
            chunk = pids[i : i + batch_size]
            try:
                result = await client.deactivate_action_products(aid, chunk)
            except Exception as e:
                log.warning("ozon_actions deactivate action=%s: %s", aid, e)
                stats["errors"].append({"action_id": aid, "error": str(e)[:300]})
                rejected_total += len(chunk)
                continue
            removed = result.get("product_ids") or []
            rejected = result.get("rejected") or []
            removed_total += len(removed) if isinstance(removed, list) else 0
            rejected_total += len(rejected) if isinstance(rejected, list) else 0
        stats["actions_processed"] += 1
        stats["action_ids"].append(aid)
        stats["products_removed"] += removed_total
        stats["products_rejected"] += rejected_total
    return stats


async def auto_remove_from_ozon_auto_actions(
    client: OzonClient,
    *,
    only_auto_add: bool = True,
    action_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Загрузить акции и удалить товары из автоакций (или выбранных id)."""
    try:
        raw = await client.list_actions()
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="actions")
        if reason:
            log.info("ozon_actions skipped: %s (HTTP %s)", reason, e.status)
            return {
                "skipped": 1,
                "reason": reason,
                "message": ozon_feature_unavailable_user_message(reason, feature="actions"),
                "actions_matched": 0,
                "actions_processed": 0,
                "products_removed": 0,
                "only_auto_add": only_auto_add,
            }
        raise
    picked = pick_actions_for_removal(
        raw,
        only_auto_add=only_auto_add,
        action_ids=action_ids,
        require_participating=False,
    )
    ids = [int(a.get("id")) for a in picked if a.get("id") is not None]
    stats = await remove_products_from_actions(client, ids)
    stats["actions_matched"] = len(ids)
    stats["only_auto_add"] = only_auto_add
    return stats


SKIP_REASON_LABELS: Dict[str, str] = {
    "no_price": "нет цены price",
    "no_action_price": "нет action_price и max_action_price",
    "no_max_action_price": "нет max_action_price",
    "action_price_above_price": "action_price > price",
    "max_action_price_above_price": "max_action_price > price",
    "invalid_product_id": "нет product_id",
    "already_participating": "уже в акции",
}


def _parse_positive_float(val: Any) -> Optional[float]:
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    return n


def _product_id_from_row(row: dict) -> Optional[int]:
    if not isinstance(row, dict):
        return None
    for key in ("id", "product_id"):
        raw = row.get(key)
        if raw is None:
            continue
        try:
            pid = int(raw)
        except (TypeError, ValueError):
            continue
        if pid > 0:
            return pid
    return None


def participating_discount_percent(product: dict) -> Tuple[Optional[float], Optional[str]]:
    """Фактическая скидка участника: (price - action_price) / price."""
    price = _parse_positive_float(product.get("price"))
    if price is None:
        return None, "no_price"
    action_price = _parse_positive_float(product.get("action_price"))
    if action_price is not None:
        if action_price > price:
            return None, "action_price_above_price"
        return round((price - action_price) / price * 100.0, 4), None
    max_ap = _parse_positive_float(product.get("max_action_price"))
    if max_ap is not None:
        if max_ap > price:
            return None, "max_action_price_above_price"
        return round((price - max_ap) / price * 100.0, 4), None
    return None, "no_action_price"


def candidate_min_discount_percent(product: dict) -> Tuple[Optional[float], Optional[str]]:
    """Минимальная скидка для входа: (price - max_action_price) / price."""
    price = _parse_positive_float(product.get("price"))
    if price is None:
        return None, "no_price"
    max_ap = _parse_positive_float(product.get("max_action_price"))
    if max_ap is None:
        return None, "no_max_action_price"
    if max_ap > price:
        return None, "max_action_price_above_price"
    return round((price - max_ap) / price * 100.0, 4), None


def target_action_price(price: float, threshold_pct: float, max_action_price: float) -> float:
    """Цена участия: не глубже порога, но в рамках лимита Ozon (max_action_price)."""
    by_threshold = price * (1.0 - threshold_pct / 100.0)
    return round(min(by_threshold, max_action_price), 2)


def _parse_iso_dt(raw: str) -> Optional[datetime]:
    s = (raw or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _action_sync_skip_reason(
    action: dict,
    *,
    now: Optional[datetime] = None,
    exclude_voucher: bool = False,
    exclude_ids: Optional[set[int]] = None,
) -> Optional[str]:
    if not isinstance(action, dict):
        return "invalid_action"
    try:
        aid = int(action.get("id") or 0)
    except (TypeError, ValueError):
        aid = 0
    if exclude_ids and aid in exclude_ids:
        return "excluded_action_id"
    if exclude_voucher and action.get("is_voucher_action"):
        return "voucher_action"
    cur = now or datetime.now(timezone.utc)
    end = _parse_iso_dt(str(action.get("date_end") or ""))
    if end and end < cur:
        return "action_ended"
    freeze = _parse_iso_dt(str(action.get("freeze_date") or ""))
    if freeze and freeze <= cur:
        return "action_frozen"
    return None


async def list_all_action_candidates(
    client: OzonClient,
    action_id: int,
    *,
    max_pages: int = 50,
) -> List[dict]:
    products: List[dict] = []
    last_id: Any = None
    for _ in range(max(1, max_pages)):
        block = await client.list_action_candidates(int(action_id), limit=100, last_id=last_id)
        chunk = block.get("products") or []
        if isinstance(chunk, list):
            products.extend([p for p in chunk if isinstance(p, dict)])
        nxt = block.get("last_id")
        if nxt in (None, "", 0):
            break
        if last_id is not None and nxt == last_id:
            break
        last_id = nxt
    return products


async def list_all_auto_add_products(
    client: OzonClient,
    action_id: int,
    *,
    max_pages: int = 50,
) -> List[dict]:
    products: List[dict] = []
    last_id: Any = None
    for _ in range(max(1, max_pages)):
        block = await client.list_auto_add_products(int(action_id), limit=100, last_id=last_id)
        chunk = block.get("products") or []
        if isinstance(chunk, list):
            products.extend([p for p in chunk if isinstance(p, dict)])
        nxt = block.get("last_id")
        if nxt in (None, "", 0):
            break
        if last_id is not None and nxt == last_id:
            break
        last_id = nxt
    return products


def _inc_skip(
    stats: dict,
    *,
    action_id: int,
    product_id: Optional[int],
    reason: str,
    sample_limit: int = 150,
) -> None:
    ctr: Counter = stats.setdefault("_skip_ctr", Counter())
    ctr[reason] += 1
    samples: List[dict] = stats.setdefault("skipped_samples", [])
    if len(samples) < sample_limit:
        samples.append({
            "action_id": action_id,
            "product_id": product_id,
            "reason": reason,
            "reason_label": SKIP_REASON_LABELS.get(reason, reason),
        })


def _finalize_sync_stats(stats: dict) -> dict:
    ctr: Counter = stats.pop("_skip_ctr", Counter())
    stats["skip_reasons"] = dict(ctr)
    stats["skipped_data_count"] = sum(ctr.values())
    stats.setdefault("skipped_samples", [])
    stats["products_removed"] = int(stats.get("participants_removed") or 0)
    stats["products_added"] = int(stats.get("candidates_added") or 0)
    return stats


async def sync_actions_by_discount_threshold(
    client: OzonClient,
    *,
    threshold_percent: float = 3.0,
    enable_remove: bool = True,
    enable_add: bool = True,
    exclude_voucher_actions: bool = False,
    exclude_action_ids: Optional[List[int]] = None,
    only_action_ids: Optional[List[int]] = None,
    batch_size: int = 100,
    max_pages_per_list: int = 50,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, Any]:
    """
    Синхронизация всех акций по порогу скидки.
    ≤ порога — оставить / добавить; > порога — снять (+ очистить auto-add).
    """
    threshold = max(0.0, float(threshold_percent))
    exclude_ids = {int(x) for x in (exclude_action_ids or [])}
    only_ids = {int(x) for x in (only_action_ids or [])} if only_action_ids else None

    stats: Dict[str, Any] = {
        "mode": "discount_threshold",
        "threshold_percent": threshold,
        "actions_total": 0,
        "actions_checked": 0,
        "actions_skipped": 0,
        "participants_kept": 0,
        "participants_removed": 0,
        "candidates_added": 0,
        "products_rejected": 0,
        "auto_add_cleared": 0,
        "actions_processed": 0,
        "errors": [],
        "skipped_samples": [],
    }

    try:
        raw_actions = await client.list_actions()
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="actions")
        if reason:
            return {
                "skipped": 1,
                "reason": reason,
                "message": ozon_feature_unavailable_user_message(reason, feature="actions"),
                "mode": "discount_threshold",
            }
        raise

    actions = [a for a in (raw_actions or []) if isinstance(a, dict)]
    stats["actions_total"] = len(actions)
    now = datetime.now(timezone.utc)
    todo = []
    for a in actions:
        skip = _action_sync_skip_reason(
            a, now=now, exclude_voucher=exclude_voucher_actions, exclude_ids=exclude_ids,
        )
        if skip:
            stats["actions_skipped"] += 1
            continue
        try:
            aid = int(a.get("id") or 0)
        except (TypeError, ValueError):
            stats["actions_skipped"] += 1
            continue
        if not aid:
            stats["actions_skipped"] += 1
            continue
        if only_ids is not None and aid not in only_ids:
            continue
        todo.append(aid)

    total = len(todo)
    for idx, aid in enumerate(todo):
        if progress_cb:
            progress_cb(idx, max(total, 1), f"Акция {aid} ({idx + 1}/{total})")
        stats["actions_checked"] += 1
        participating_ids: set[int] = set()
        try:
            participants = await list_all_action_products(
                client, aid, max_pages=max_pages_per_list,
            )
        except Exception as e:
            log.warning("ozon sync list products action=%s: %s", aid, e)
            stats["errors"].append({"action_id": aid, "phase": "list_products", "error": str(e)[:300]})
            continue

        remove_ids: List[int] = []
        for row in participants:
            pid = _product_id_from_row(row)
            if not pid:
                _inc_skip(stats, action_id=aid, product_id=None, reason="invalid_product_id")
                continue
            participating_ids.add(pid)
            pct, reason = participating_discount_percent(row)
            if pct is None:
                _inc_skip(stats, action_id=aid, product_id=pid, reason=reason or "no_action_price")
                continue
            if pct <= threshold + 1e-6:
                stats["participants_kept"] += 1
            elif enable_remove:
                remove_ids.append(pid)
            else:
                stats["participants_kept"] += 1

        if enable_remove and remove_ids:
            removed_n = 0
            rejected_n = 0
            all_removed_ids: List[int] = []
            for i in range(0, len(remove_ids), batch_size):
                chunk = remove_ids[i : i + batch_size]
                try:
                    result = await client.deactivate_action_products(aid, chunk)
                except Exception as e:
                    log.warning("ozon sync deactivate action=%s: %s", aid, e)
                    stats["errors"].append({"action_id": aid, "phase": "deactivate", "error": str(e)[:300]})
                    rejected_n += len(chunk)
                    continue
                removed = result.get("product_ids") or []
                rejected = result.get("rejected") or []
                if isinstance(removed, list):
                    for x in removed:
                        try:
                            all_removed_ids.append(int(x))
                        except (TypeError, ValueError):
                            pass
                removed_n += len(removed) if isinstance(removed, list) else 0
                rejected_n += len(rejected) if isinstance(rejected, list) else 0
            stats["participants_removed"] += removed_n
            stats["products_rejected"] += rejected_n

            if all_removed_ids:
                for i in range(0, len(all_removed_ids), batch_size):
                    chunk = all_removed_ids[i : i + batch_size]
                    try:
                        ar = await client.delete_auto_add_products(aid, chunk)
                    except Exception as e:
                        log.warning("ozon sync auto-add delete action=%s: %s", aid, e)
                        continue
                    cleared = ar.get("product_ids") or []
                    stats["auto_add_cleared"] += len(cleared) if isinstance(cleared, list) else 0

        if not enable_add:
            stats["actions_processed"] += 1
            continue

        try:
            candidates = await list_all_action_candidates(
                client, aid, max_pages=max_pages_per_list,
            )
        except Exception as e:
            log.warning("ozon sync list candidates action=%s: %s", aid, e)
            stats["errors"].append({"action_id": aid, "phase": "list_candidates", "error": str(e)[:300]})
            stats["actions_processed"] += 1
            continue

        to_add: List[dict] = []
        for row in candidates:
            pid = _product_id_from_row(row)
            if not pid:
                _inc_skip(stats, action_id=aid, product_id=None, reason="invalid_product_id")
                continue
            if pid in participating_ids:
                _inc_skip(stats, action_id=aid, product_id=pid, reason="already_participating")
                continue
            min_pct, reason = candidate_min_discount_percent(row)
            if min_pct is None:
                _inc_skip(stats, action_id=aid, product_id=pid, reason=reason or "no_max_action_price")
                continue
            if min_pct > threshold + 1e-6:
                continue
            price = _parse_positive_float(row.get("price"))
            max_ap = _parse_positive_float(row.get("max_action_price"))
            if price is None or max_ap is None:
                _inc_skip(stats, action_id=aid, product_id=pid, reason="no_price")
                continue
            act_price = target_action_price(price, threshold, max_ap)
            item: Dict[str, Any] = {"product_id": pid, "action_price": act_price}
            if row.get("stock") is not None:
                try:
                    item["stock"] = int(row["stock"])
                except (TypeError, ValueError):
                    pass
            to_add.append(item)

        added_n = 0
        for i in range(0, len(to_add), batch_size):
            chunk = to_add[i : i + batch_size]
            try:
                result = await client.activate_action_products(aid, chunk)
            except Exception as e:
                log.warning("ozon sync activate action=%s: %s", aid, e)
                stats["errors"].append({"action_id": aid, "phase": "activate", "error": str(e)[:300]})
                stats["products_rejected"] += len(chunk)
                continue
            added = result.get("product_ids") or []
            rejected = result.get("rejected") or []
            added_n += len(added) if isinstance(added, list) else 0
            stats["products_rejected"] += len(rejected) if isinstance(rejected, list) else 0
        stats["candidates_added"] += added_n
        stats["actions_processed"] += 1

    if progress_cb and total:
        progress_cb(total, max(total, 1), "Готово")
    stats["actions_matched"] = stats["actions_checked"]
    return _finalize_sync_stats(stats)
