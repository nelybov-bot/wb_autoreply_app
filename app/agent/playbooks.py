"""Многошаговые сценарии MarketAI для агента."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from app.core.ozon_actions import normalize_action_row, remove_products_from_actions
from app.core.ozon_buyer_chat import ozon_feature_unavailable_user_message, ozon_http_skip_reason
from app.core.ozon_client import OzonClient
from app.core.card_links import (
    fetch_ozon_catalog,
    fetch_wb_catalog,
    group_ozon_rows,
    group_wb_rows,
    ozon_link_by_model,
    parse_articles_csv,
    suggest_link_candidates,
    ozon_unlink_cards,
    wb_disconnect_cards,
    wb_merge_cards,
)
from app.core.net import HttpStatusError
from app.db import Database
from app.web import tasks as web_tasks
from app.web.store_locks import StoreBusyError

log = logging.getLogger("agent.playbooks")

_TASK_POLL_INTERVAL = 2.5
_TASK_TIMEOUT_SEC = 900
_ITEM_BATCH = 80


async def _wait_task(task_id: str, *, label: str = "") -> dict[str, Any]:
    deadline = time.monotonic() + _TASK_TIMEOUT_SEC
    while time.monotonic() < deadline:
        st = await web_tasks.get_task(task_id)
        if not st:
            return {"status": "lost", "error": f"Задача {task_id} не найдена"}
        status = str(st.get("status") or "")
        if status in ("done", "error", "cancelled"):
            return st
        await asyncio.sleep(_TASK_POLL_INTERVAL)
    return {"status": "timeout", "error": f"Превышено время ожидания ({label or task_id})"}


def _collect_item_ids(
    db: Database,
    *,
    item_type: str,
    store_ids: Optional[list[int]],
    status: str,
    has_answer: Optional[bool],
    limit: int = 200,
) -> list[int]:
    rows = db.list_items_filtered(
        item_type=item_type,
        store_id=None,
        statuses=[status],
        has_answer=has_answer,
        limit=limit,
    )
    if store_ids:
        want = {int(x) for x in store_ids}
        rows = [r for r in rows if int(r.store_id) in want]
    return [int(r.id) for r in rows]


async def pipeline_answer_items(
    db: Database,
    *,
    item_type: str,
    store_ids: Optional[list[int]] = None,
    openai_key: str,
) -> dict[str, Any]:
    """
    Полный цикл: загрузка → генерация → отправка.
    item_type: review | question
    """
    label = "отзывы" if item_type == "review" else "вопросы"
    steps: list[str] = []
    summary: dict[str, Any] = {"item_type": item_type, "steps": steps}

    # 1. Загрузка
    try:
        load_id = await web_tasks.run_load_new(db, store_ids)
    except StoreBusyError as e:
        return {"error": str(e), **summary}
    steps.append(f"1. Загрузка запущена ({load_id})")
    load_st = await _wait_task(load_id, label="загрузка")
    if load_st.get("status") == "error":
        return {"error": load_st.get("error") or "Ошибка загрузки", **summary}
    if load_st.get("status") == "timeout":
        return {"error": load_st.get("error"), "task_id": load_id, **summary}
    steps.append("1. Загрузка завершена")

    # 2. Сбор новых без ответа
    item_ids = _collect_item_ids(
        db,
        item_type=item_type,
        store_ids=store_ids,
        status="new",
        has_answer=False,
    )
    summary["found_new"] = len(item_ids)
    if not item_ids:
        steps.append(f"2. Новых {label} для ответа нет — цикл завершён")
        summary["message"] = f"Загрузка выполнена. Новых {label} для обработки нет."
        return summary

    # 3. Генерация (батчами)
    gen_ok = 0
    gen_failed = 0
    for i in range(0, len(item_ids), _ITEM_BATCH):
        batch = item_ids[i : i + _ITEM_BATCH]
        try:
            gen_id = await web_tasks.run_generate(db, batch, openai_key)
        except StoreBusyError as e:
            return {"error": str(e), **summary}
        gen_st = await _wait_task(gen_id, label="генерация")
        if gen_st.get("status") == "error":
            return {"error": gen_st.get("error") or "Ошибка генерации", **summary}
        res = gen_st.get("result") or {}
        gen_ok += int(res.get("ok") or 0)
        gen_failed += int(res.get("failed") or 0)
    steps.append(f"2. Сгенерировано: {gen_ok}, ошибок: {gen_failed}")

    # 4. Отправка
    send_ids = _collect_item_ids(
        db,
        item_type=item_type,
        store_ids=store_ids,
        status="generated",
        has_answer=True,
    )
    if not send_ids:
        send_ids = _collect_item_ids(
            db,
            item_type=item_type,
            store_ids=store_ids,
            status="new",
            has_answer=True,
        )
    summary["ready_to_send"] = len(send_ids)
    if not send_ids:
        steps.append("3. Нет ответов для отправки")
        summary["message"] = f"Генерация выполнена, но отправлять нечего ({label})."
        return summary

    sent_ok = 0
    sent_failed = 0
    for i in range(0, len(send_ids), _ITEM_BATCH):
        batch = send_ids[i : i + _ITEM_BATCH]
        try:
            send_id = await web_tasks.run_send(db, batch)
        except StoreBusyError as e:
            return {"error": str(e), **summary}
        send_st = await _wait_task(send_id, label="отправка")
        if send_st.get("status") == "error":
            return {"error": send_st.get("error") or "Ошибка отправки", **summary}
        res = send_st.get("result") or {}
        sent_ok += int(res.get("sent_ok") or 0)
        sent_failed += int(res.get("failed") or 0)
    steps.append(f"3. Отправлено: {sent_ok}, ошибок: {sent_failed}")
    summary["sent_ok"] = sent_ok
    summary["sent_failed"] = sent_failed
    summary["message"] = (
        f"Цикл по {label} завершён: загружено → сгенерировано {gen_ok} → отправлено {sent_ok}."
    )
    return summary


def _ozon_stores(db: Database, store_id: Optional[int] = None) -> list:
    stores = [s for s in db.list_stores() if s.active and s.marketplace == "ozon"]
    if store_id is not None:
        stores = [s for s in stores if int(s.id) == int(store_id)]
    return [s for s in stores if (s.client_id or "").strip() and (s.api_key or "").strip()]


async def check_ozon_promotions(
    db: Database,
    *,
    store_id: Optional[int] = None,
    only_auto_add: Optional[bool] = None,
    session_context: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Список акций Ozon по магазинам с количеством товаров."""
    stores = _ozon_stores(db, store_id)
    if not stores:
        return {"error": "Нет активных магазинов Ozon с ключами API"}

    by_store: list[dict[str, Any]] = []
    total_actions = 0
    total_products = 0

    for s in stores:
        client = OzonClient(s.client_id or "", s.api_key)
        try:
            raw = await client.list_actions()
        except HttpStatusError as e:
            reason = ozon_http_skip_reason(e.status, e.body or "", feature="actions")
            by_store.append({
                "store_id": s.id,
                "store_name": s.name,
                "error": ozon_feature_unavailable_user_message(reason, feature="actions") if reason else str(e),
                "actions": [],
            })
            continue

        actions_out: list[dict[str, Any]] = []
        for a in raw or []:
            if not isinstance(a, dict):
                continue
            row = normalize_action_row(a)
            if only_auto_add is True and not row.get("is_auto_add"):
                continue
            if only_auto_add is False and row.get("is_auto_add"):
                continue
            cnt = int(row.get("participating_products_count") or 0)
            actions_out.append({
                "id": row.get("id"),
                "title": row.get("title"),
                "participating_products_count": cnt,
                "is_auto_add": bool(row.get("is_auto_add")),
                "date_end": row.get("date_end"),
            })
            total_actions += 1
            total_products += cnt

        actions_out.sort(key=lambda x: (-int(x.get("participating_products_count") or 0), str(x.get("title") or "")))
        by_store.append({
            "store_id": s.id,
            "store_name": s.name,
            "actions": actions_out,
            "actions_count": len(actions_out),
        })

    result = {
        "stores": by_store,
        "total_actions": total_actions,
        "total_products": total_products,
    }
    if session_context is not None:
        session_context["ozon_promotions"] = by_store
    return result


async def remove_ozon_promotions(
    db: Database,
    *,
    store_id: Optional[int] = None,
    action_ids: Optional[list[int]] = None,
    only_auto_add: bool = True,
    session_context: Optional[dict[str, Any]] = None,
    use_last_check: bool = False,
) -> dict[str, Any]:
    """Удалить товары из акций Ozon."""
    stores = _ozon_stores(db, store_id)
    if not stores:
        return {"error": "Нет активных магазинов Ozon"}

    if use_last_check and session_context and not action_ids:
        action_ids = []
        for block in session_context.get("ozon_promotions") or []:
            for a in block.get("actions") or []:
                if int(a.get("participating_products_count") or 0) > 0 and a.get("id") is not None:
                    action_ids.append(int(a["id"]))

    aggregated = {
        "stores_processed": 0,
        "actions_processed": 0,
        "products_removed": 0,
        "products_rejected": 0,
        "errors": [],
        "per_store": [],
    }

    for s in stores:
        client = OzonClient(s.client_id or "", s.api_key)
        ids = list(action_ids or [])
        if not ids:
            try:
                raw = await client.list_actions()
            except HttpStatusError as e:
                aggregated["errors"].append({"store": s.name, "error": str(e)[:200]})
                continue
            from app.core.ozon_actions import pick_actions_for_removal
            picked = pick_actions_for_removal(raw, only_auto_add=only_auto_add)
            ids = [int(a.get("id")) for a in picked if a.get("id") is not None]
        if not ids:
            aggregated["per_store"].append({
                "store_name": s.name,
                "message": "Нет акций с товарами для удаления",
            })
            continue
        try:
            stats = await remove_products_from_actions(client, ids)
        except Exception as e:
            aggregated["errors"].append({"store": s.name, "error": str(e)[:200]})
            continue
        aggregated["stores_processed"] += 1
        aggregated["actions_processed"] += int(stats.get("actions_processed") or 0)
        aggregated["products_removed"] += int(stats.get("products_removed") or 0)
        aggregated["products_rejected"] += int(stats.get("products_rejected") or 0)
        aggregated["per_store"].append({
            "store_name": s.name,
            "actions_processed": stats.get("actions_processed"),
            "products_removed": stats.get("products_removed"),
            "action_ids": stats.get("action_ids"),
        })

    if aggregated["products_removed"]:
        aggregated["message"] = (
            f"Удалено товаров: {aggregated['products_removed']} "
            f"из {aggregated['actions_processed']} акций ({aggregated['stores_processed']} магазинов)."
        )
    elif aggregated["per_store"]:
        aggregated["message"] = "Акции проверены — удалять нечего или нет доступа."
    else:
        aggregated["message"] = "Не удалось выполнить удаление."
    return aggregated


def _wb_stores(db: Database, store_id: Optional[int] = None) -> list:
    stores = [
        s for s in db.list_stores()
        if s.active and s.marketplace == "wb" and (s.api_key or "").strip()
    ]
    if store_id is not None:
        stores = [s for s in stores if int(s.id) == int(store_id)]
    return stores


def _filter_stores(stores: list, store_ids: Optional[list[int]]) -> list:
    if not store_ids:
        return stores
    want = {int(x) for x in store_ids}
    return [s for s in stores if int(s.id) in want]


async def _scan_wb_store(db: Database, store, *, max_preview: int = 12) -> dict[str, Any]:
    from app.core.wb_buyer_chat import (
        WbBuyerChatClient,
        build_wb_thread_lines,
        collect_global_events_by_chat,
        merge_good_card,
        product_title_from_wb_chat,
    )
    from app.core.workflows import _buyer_chat_reply_from, _last_client_text_from_lines, _wb_chat_eligibility

    reply_from = _buyer_chat_reply_from(db)
    block: dict[str, Any] = {"store_id": store.id, "store_name": store.name, "chats": [], "eligible_count": 0}
    try:
        client = WbBuyerChatClient(store.api_key)
        chats = await client.list_chats()
        events_by_chat = await collect_global_events_by_chat(client, max_pages=6)
    except Exception as e:
        block["error"] = str(e)[:200]
        return block

    chat_rows = [c for c in chats if isinstance(c, dict) and c.get("chatID")]
    chat_rows.sort(
        key=lambda c: int((c.get("lastMessage") or {}).get("addTimestamp") or 0),
        reverse=True,
    )
    eligible: list[dict[str, Any]] = []
    eligible_count = 0
    for row in chat_rows[: max(30, max_preview * 4)]:
        cid = str(row.get("chatID") or "").strip()
        if not cid:
            continue
        evs = events_by_chat.get(cid) or []
        lines_ts = build_wb_thread_lines(evs, cid, row)
        ok, _reason, _mk, _ts = _wb_chat_eligibility(db, store.id, cid, lines_ts, reply_from)
        if not ok:
            continue
        eligible_count += 1
        if len(eligible) >= max_preview:
            continue
        gc = merge_good_card(row, evs)
        texts = [t for _, t, __, ___ in lines_ts]
        title = product_title_from_wb_chat(gc, texts)
        preview = (_last_client_text_from_lines(lines_ts) or "")[:100]
        eligible.append({"chat_id": cid, "product": title[:60], "preview": preview})
    block["eligible_count"] = eligible_count
    block["chats"] = eligible
    return block


async def _scan_ozon_store(db: Database, store, *, max_preview: int = 12) -> dict[str, Any]:
    from app.core.ozon_buyer_chat import (
        collect_ozon_thread_lines,
        is_ozon_buyer_chat_row,
        ozon_chat_row_id,
        ozon_feature_unavailable_user_message,
        ozon_http_skip_reason,
        ozon_reply_window_hint,
        product_title_from_ozon_chat,
    )
    from app.core.workflows import _buyer_chat_reply_from, _last_client_text_from_lines, _ozon_chat_eligibility

    reply_from = _buyer_chat_reply_from(db)
    block: dict[str, Any] = {"store_id": store.id, "store_name": store.name, "chats": [], "eligible_count": 0}
    client = OzonClient(store.client_id or "", store.api_key)
    try:
        rows = await client.list_all_buyer_chats(unread_only=False)
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="chat")
        block["error"] = ozon_feature_unavailable_user_message(reason, feature="chat") if reason else str(e)[:200]
        return block
    except Exception as e:
        block["error"] = str(e)[:200]
        return block

    eligible: list[dict[str, Any]] = []
    eligible_count = 0
    for row in rows:
        if not isinstance(row, dict) or not is_ozon_buyer_chat_row(row):
            continue
        chat_id = ozon_chat_row_id(row)
        if not chat_id:
            continue
        try:
            hist = await client.chat_history(chat_id, limit=80)
        except Exception:
            continue
        messages = hist.get("messages") or []
        if not isinstance(messages, list):
            messages = []
        lines = collect_ozon_thread_lines(messages)
        chat_obj = row.get("chat") if isinstance(row.get("chat"), dict) else {}
        window = ozon_reply_window_hint(lines, chat_status=str(chat_obj.get("chat_status") or ""))
        if window.get("blocked"):
            continue
        ok, _reason, _mk, _created = _ozon_chat_eligibility(db, store.id, chat_id, lines, reply_from)
        if not ok:
            continue
        eligible_count += 1
        if len(eligible) >= max_preview:
            continue
        title = product_title_from_ozon_chat(messages, lines)
        preview = (_last_client_text_from_lines(lines) or "")[:100]
        eligible.append({"chat_id": chat_id, "product": title[:60], "preview": preview})
    block["eligible_count"] = eligible_count
    block["chats"] = eligible
    return block


async def check_buyer_chats(
    db: Database,
    *,
    marketplace: str = "all",
    store_id: Optional[int] = None,
    store_ids: Optional[list[int]] = None,
    session_context: Optional[dict[str, Any]] = None,
    max_preview: int = 12,
) -> dict[str, Any]:
    """Сканирование чатов покупателей, где нужен ответ (без отправки)."""
    mp = (marketplace or "all").strip().lower()
    stores_data: list[dict[str, Any]] = []
    total = 0

    if mp in ("wb", "all"):
        for store in _filter_stores(_wb_stores(db, store_id), store_ids):
            block = await _scan_wb_store(db, store, max_preview=max_preview)
            stores_data.append({"marketplace": "wb", **block})
            total += int(block.get("eligible_count") or 0)

    if mp in ("ozon", "all"):
        for store in _filter_stores(_ozon_stores(db, store_id), store_ids):
            block = await _scan_ozon_store(db, store, max_preview=max_preview)
            stores_data.append({"marketplace": "ozon", **block})
            total += int(block.get("eligible_count") or 0)

    result = {"marketplace": mp, "stores": stores_data, "total_eligible": total}
    if session_context is not None:
        session_context["buyer_chats_scan"] = result
    return result


async def pipeline_buyer_chats(
    db: Database,
    *,
    marketplace: str = "all",
    store_ids: Optional[list[int]] = None,
    openai_key: str,
    max_chats_per_store: int = 50,
) -> dict[str, Any]:
    """Генерация и отправка ответов в чатах покупателей (WB и/или Ozon)."""
    from app.core.workflows import (
        ozon_buyer_chats_mass_generate_send_for_store,
        wb_buyer_chats_mass_generate_send_for_store,
    )

    mp = (marketplace or "all").strip().lower()
    key = (openai_key or "").strip()
    if not key:
        return {"error": "Не задан OpenAI ключ"}

    steps: list[str] = []
    per_store: list[dict[str, Any]] = []
    total_sent = 0

    if mp in ("wb", "all"):
        for store in _filter_stores(_wb_stores(db), store_ids):
            try:
                stats = await wb_buyer_chats_mass_generate_send_for_store(
                    db,
                    store,
                    openai_key=key,
                    max_chats=max_chats_per_store,
                    event_pages=8,
                    pause_between_chats_sec=1.1,
                    audit_actor="agent",
                )
            except Exception as e:
                per_store.append({"marketplace": "wb", "store_name": store.name, "error": str(e)[:200]})
                continue
            sent = int(stats.get("wb_chat_sent") or 0)
            total_sent += sent
            steps.append(f"WB {store.name}: ответов отправлено {sent}")
            per_store.append({"marketplace": "wb", "store_name": store.name, **stats})

    if mp in ("ozon", "all"):
        for store in _filter_stores(_ozon_stores(db), store_ids):
            try:
                stats = await ozon_buyer_chats_mass_generate_send_for_store(
                    db,
                    store,
                    openai_key=key,
                    max_chats=max_chats_per_store,
                    pause_between_chats_sec=1.0,
                    audit_actor="agent",
                )
            except Exception as e:
                per_store.append({"marketplace": "ozon", "store_name": store.name, "error": str(e)[:200]})
                continue
            if stats.get("ozon_chat_skip_reason"):
                steps.append(f"Ozon {store.name}: {stats.get('message') or stats.get('ozon_chat_skip_reason')}")
                per_store.append({"marketplace": "ozon", "store_name": store.name, **stats})
                continue
            sent = int(stats.get("ozon_chat_sent") or 0)
            total_sent += sent
            steps.append(f"Ozon {store.name}: ответов отправлено {sent}")
            per_store.append({"marketplace": "ozon", "store_name": store.name, **stats})

    if not per_store:
        return {"error": "Нет магазинов с чатами для обработки", "marketplace": mp}

    return {
        "marketplace": mp,
        "total_sent": total_sent,
        "steps": steps,
        "per_store": per_store,
        "message": f"Чаты обработаны. Всего отправлено ответов: {total_sent}.",
    }


async def list_product_cards_catalog(
    db: Database,
    *,
    marketplace: str,
    store_id: Optional[int] = None,
    articles: Optional[str] = None,
    max_pages: int = 15,
) -> dict[str, Any]:
    mp = (marketplace or "").strip().lower()
    if mp not in ("wb", "ozon"):
        return {"error": "marketplace: wb или ozon"}
    if store_id is None:
        stores = _wb_stores(db) if mp == "wb" else _ozon_stores(db)
        if not stores:
            return {"error": f"Нет магазинов {mp.upper()}"}
        if len(stores) > 1:
            return {
                "error": "Укажите store_id или store_name",
                "stores": [{"id": s.id, "name": s.name} for s in stores],
            }
        store = stores[0]
    else:
        stores = [s for s in db.list_stores() if int(s.id) == int(store_id)]
        if not stores:
            return {"error": "Магазин не найден"}
        store = stores[0]
        if store.marketplace != mp:
            return {"error": f"Магазин не {mp.upper()}"}

    vendor_codes = parse_articles_csv(articles)
    try:
        if mp == "wb":
            if not (store.api_key or "").strip():
                return {"error": "Не задан API-ключ WB"}
            rows = await fetch_wb_catalog(
                store.api_key,
                vendor_codes=vendor_codes or None,
                max_pages=max_pages,
            )
            groups = group_wb_rows(rows)
        else:
            if not (store.client_id or "").strip() or not (store.api_key or "").strip():
                return {"error": "Не заданы Client-Id и Api-Key Ozon"}
            rows = await fetch_ozon_catalog(
                store.client_id or "",
                store.api_key,
                offer_ids=vendor_codes or None,
                max_pages=max_pages,
            )
            groups = group_ozon_rows(rows)
    except HttpStatusError as e:
        return {"error": f"API {e.status}: {(e.body or '')[:300]}"}

    preview = []
    for r in rows[:25]:
        preview.append(
            {
                "article": r.get("vendor_code") or r.get("offer_id"),
                "mp_id": r.get("nm_id") or r.get("sku"),
                "title": (r.get("title") or "")[:120],
                "photo_url": r.get("photo_url"),
                "linked": r.get("linked"),
                "group": r.get("link_group_label"),
            }
        )
    linked_groups = sum(
        1 for g in groups if g.get("linked") and g.get("group_id") != "__unlinked__"
    )
    return {
        "store_id": store.id,
        "store_name": store.name,
        "marketplace": mp,
        "count": len(rows),
        "linked_groups": linked_groups,
        "preview": preview,
        "candidates_count": len(suggest_link_candidates(rows, marketplace=mp)),
        "message": f"{store.name}: загружено {len(rows)} карточек, связок {linked_groups}.",
    }


async def link_product_cards(
    db: Database,
    *,
    marketplace: str,
    store_id: Optional[int],
    articles: Optional[list[str]] = None,
    target_imt: Optional[int] = None,
    model_name: Optional[str] = None,
) -> dict[str, Any]:
    mp = (marketplace or "").strip().lower()
    if mp not in ("wb", "ozon"):
        return {"error": "marketplace: wb или ozon"}
    if store_id is None:
        return {"error": "Укажите store_id"}
    stores = [s for s in db.list_stores() if int(s.id) == int(store_id)]
    if not stores:
        return {"error": "Магазин не найден"}
    store = stores[0]
    if store.marketplace != mp:
        return {"error": f"Магазин не {mp.upper()}"}

    arts = [str(x).strip() for x in (articles or []) if str(x).strip()]
    if not arts:
        return {"error": "Укажите артикулы (articles)"}

    try:
        if mp == "wb":
            rows = await fetch_wb_catalog(store.api_key, vendor_codes=arts, max_pages=5)
            by_vendor = {str(r.get("vendor_code") or "").strip(): r for r in rows}
            nm_ids = []
            for a in arts:
                row = by_vendor.get(a)
                if not row:
                    return {"error": f"Карточка WB не найдена: {a}"}
                nm_ids.append(int(row.get("nm_id") or 0))
            nm_ids = [x for x in nm_ids if x]
            if not nm_ids:
                return {"error": "nmID не найдены"}
            tgt = int(target_imt or 0)
            if not tgt:
                tgt = int(rows[0].get("imt_id") or 0)
            if not tgt:
                return {"error": "Укажите target_imt (imtID целевой связки)"}
            result = await wb_merge_cards(store.api_key, target_imt=tgt, nm_ids=nm_ids)
            return {
                "ok": True,
                "marketplace": "wb",
                "target_imt": tgt,
                "nm_ids": nm_ids,
                "result": result,
                "message": f"WB: запрос на объединение {len(nm_ids)} карточек в imtID {tgt}.",
            }

        model = (model_name or "").strip()
        if not model:
            return {"error": "Укажите model_name (название модели Ozon, атрибут 9048)"}
        result = await ozon_link_by_model(
            store.client_id or "",
            store.api_key,
            offer_ids=arts,
            model_name=model,
        )
        return {
            "ok": True,
            "marketplace": "ozon",
            "offer_ids": arts,
            "model_name": model,
            "result": result,
            "message": f"Ozon: обновлён атрибут модели для {len(arts)} товаров.",
        }
    except HttpStatusError as e:
        return {"error": f"API {e.status}: {(e.body or '')[:300]}"}
    except ValueError as e:
        return {"error": str(e)}


async def unlink_product_cards(
    db: Database,
    *,
    marketplace: str,
    store_id: Optional[int],
    articles: Optional[list[str]] = None,
    nm_ids: Optional[list[int]] = None,
) -> dict[str, Any]:
    mp = (marketplace or "").strip().lower()
    if mp not in ("wb", "ozon"):
        return {"error": "marketplace: wb или ozon"}
    if store_id is None:
        return {"error": "Укажите store_id"}
    stores = [s for s in db.list_stores() if int(s.id) == int(store_id)]
    if not stores:
        return {"error": "Магазин не найден"}
    store = stores[0]
    if store.marketplace != mp:
        return {"error": f"Магазин не {mp.upper()}"}

    try:
        if mp == "wb":
            ids = [int(x) for x in (nm_ids or []) if x is not None]
            arts = [str(x).strip() for x in (articles or []) if str(x).strip()]
            if not ids and arts:
                rows = await fetch_wb_catalog(store.api_key, vendor_codes=arts, max_pages=5)
                by_vendor = {str(r.get("vendor_code") or "").strip(): r for r in rows}
                for a in arts:
                    row = by_vendor.get(a)
                    if not row:
                        return {"error": f"Карточка WB не найдена: {a}"}
                    nid = int(row.get("nm_id") or 0)
                    if nid:
                        ids.append(nid)
            ids = [x for x in ids if x]
            if not ids:
                return {"error": "Укажите nm_ids или артикулы (articles)"}
            result = await wb_disconnect_cards(store.api_key, nm_ids=ids)
            if result.get("errors"):
                return {
                    "error": f"WB: ошибки при разъединении {result.get('failed')} карточек",
                    "details": result,
                }
            return {
                "ok": True,
                "marketplace": "wb",
                "nm_ids": ids,
                "result": result,
                "message": f"WB: разъединено {result.get('processed', len(ids))} карточек.",
            }

        arts = [str(x).strip() for x in (articles or []) if str(x).strip()]
        if not arts:
            return {"error": "Укажите offer_id (articles)"}
        titles: dict[str, str] = {}
        rows = await fetch_ozon_catalog(
            store.client_id or "",
            store.api_key,
            offer_ids=arts,
            max_pages=3,
        )
        for r in rows:
            oid = str(r.get("offer_id") or "").strip()
            if oid:
                titles[oid] = str(r.get("title") or "")
        result = await ozon_unlink_cards(
            store.client_id or "",
            store.api_key,
            offer_ids=arts,
            titles_by_offer=titles,
        )
        return {
            "ok": True,
            "marketplace": "ozon",
            "offer_ids": arts,
            "result": result,
            "message": f"Ozon: у {len(arts)} товаров заданы уникальные названия модели.",
        }
    except HttpStatusError as e:
        return {"error": f"API {e.status}: {(e.body or '')[:300]}"}
    except ValueError as e:
        return {"error": str(e)}
