"""Ozon акции: список, отбор автоакций, удаление товаров."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

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
    raw = await client.list_actions()
    picked = pick_actions_for_removal(
        raw,
        only_auto_add=only_auto_add,
        action_ids=action_ids,
    )
    ids = [int(a.get("id")) for a in picked if a.get("id") is not None]
    stats = await remove_products_from_actions(client, ids)
    stats["actions_matched"] = len(ids)
    stats["only_auto_add"] = only_auto_add
    return stats
