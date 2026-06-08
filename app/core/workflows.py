"""
Workflows: загрузка новых, массовая генерация, массовая отправка.

Все функции async, чтобы исполнялись в AsyncRunner.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from queue import Queue
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..db import Database, Store
from .net import HttpStatusError, UnauthorizedStoreError
from .wb_client import WbClient
from .yam_client import YamClient
from .ozon_client import OzonClient
from .openai_client import OpenAIClient
from .card_check import build_generation_user_prompt, maybe_record_card_error
from .telegram_notify import send_review_to_chat
from .chat_common import (
    SETTING_REPLY_FROM,
    ozon_iso_after_cutoff,
    parse_auto_chat_max_age_days,
    parse_reply_from_date,
    SETTING_AUTO_CHAT_MAX_AGE_DAYS,
    wb_ts_within_max_age,
    ozon_iso_within_max_age,
    wb_ts_ms_after_cutoff,
)
from .ozon_actions import auto_remove_from_ozon_auto_actions
from .ozon_buyer_chat import (
    collect_ozon_thread_lines,
    is_ozon_buyer_chat_row,
    last_client_message_info as ozon_last_client_info,
    ozon_chat_row_id,
    ozon_reply_window_hint,
    ozon_http_skip_reason,
    ozon_feature_unavailable_user_message,
    product_title_from_ozon_chat,
)
from .wb_buyer_chat import (
    WbBuyerChatClient,
    collect_thread_lines,
    fallback_line_from_chat_row,
    fetch_events_for_chat,
    last_client_message_info as wb_last_client_info,
    merge_good_card,
    product_title_from_wb_chat,
)

log = logging.getLogger("wf")


def _wb_http_body_one_line(body: str, *, max_len: int = 240) -> str:
    s = (body or "").replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())
    return (s[:max_len] + "…") if len(s) > max_len else s


def _rating_group(rating: Optional[int]) -> str:
    if rating is None:
        return "general"
    if rating <= 1:
        return "1"
    if rating == 2:
        return "2"
    if rating == 3:
        return "3"
    return "4-5"

def _iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")

def _store_name(db: Database, store_id: int) -> str:
    for s in db.list_stores():
        if s.id == store_id:
            return s.name
    return str(store_id)


def _audit_activity(
    db: Database,
    *,
    action: str,
    store_id: Optional[int],
    result: str,
    meta: dict,
    actor: str = "system",
    item_type: str = "activity",
) -> None:
    try:
        db.add_audit_event(
            actor=actor,
            action=action,
            item_type=item_type,
            store_id=store_id,
            result=result,
            meta=meta,
        )
    except Exception:
        pass


def _put_progress(q: Optional[Queue], current: int, total: int) -> None:
    if q is not None and total > 0:
        try:
            q.put(("progress", current, total))
        except Exception:
            pass


async def load_new_items(
    db: Database,
    store: Store,
    progress_queue: Optional[Queue] = None,
) -> int:
    """
    Тянет новые вопросы+отзывы с маркетплейса и upsert'ит в SQLite.
    progress_queue: при наличии кладутся ("progress", 0, 1) в начале и (1, 1) в конце.
    При 401 поднимает UnauthorizedStoreError с названием магазина.
    """
    _put_progress(progress_queue, 0, 1)
    store_id = store.id
    if store.marketplace == "wb":
        n = await _load_new_wb(db, store)
    elif store.marketplace == "yam":
        if store.business_id is None:
            log.warning("YAM store id=%s без business_id, пропуск загрузки", store_id)
            _put_progress(progress_queue, 1, 1)
            return 0
        n = await _load_new_yam(db, store)
    elif store.marketplace == "ozon":
        if not (store.client_id or "").strip():
            log.warning("Ozon store id=%s без client_id, пропуск загрузки", store_id)
            _put_progress(progress_queue, 1, 1)
            return 0
        # Перепроверка обработанных (чтобы исключить дубли): если на Ozon уже PROCESSED,
        # то локально помечаем как sent и не показываем как "новое".
        try:
            await _ozon_recheck_processed(db, store)
        except Exception as e:
            log.warning("Ozon recheck processed failed store_id=%s: %s", store_id, e)
        n = await _load_new_ozon(db, store)
    else:
        log.warning("Неизвестный маркетплейс %s для store_id=%s", store.marketplace, store_id)
        _put_progress(progress_queue, 1, 1)
        return 0
    _put_progress(progress_queue, 1, 1)
    return n


async def _load_new_wb(db: Database, store: Store) -> int:
    store_id = store.id
    wb = WbClient(store.api_key)
    try:
        trig = await wb.has_new()
        data = (trig or {}).get("data") or {}
        if not (data.get("hasNewQuestions") or data.get("hasNewFeedbacks")):
            return 0

        added = 0
        q = await wb.list_questions(take=100, skip=0)
        qdata = (q or {}).get("data") or {}
        for it in qdata.get("questions", []) or []:
            ext_id = str(it.get("id",""))
            text = str(it.get("text") or "")
            created = str(it.get("createdDate") or "")
            was_viewed = bool(it.get("wasViewed"))
            author = ""
            product_title = str(((it.get("productDetails") or {}).get("productName")) or "")
            _, was_new = db.upsert_item(
                store_id=store_id,
                external_id=ext_id,
                item_type="question",
                date=created or _iso_now(),
                rating=None,
                text=text,
                author=author,
                product_title=product_title,
                was_viewed=was_viewed,
            )
            if was_new:
                added += 1

        f = await wb.list_feedbacks(take=100, skip=0)
        fdata = (f or {}).get("data") or {}
        for it in fdata.get("feedbacks", []) or []:
            ext_id = str(it.get("id",""))
            text = str(it.get("text") or "")
            pros = str(it.get("pros") or "")
            cons = str(it.get("cons") or "")
            full_text = text
            if pros.strip():
                full_text += ("\nПлюсы: " + pros.strip())
            if cons.strip():
                full_text += ("\nМинусы: " + cons.strip())
            created = str(it.get("createdDate") or "")
            rating = it.get("productValuation")
            rating_i = int(rating) if rating is not None else None
            was_viewed = bool(it.get("wasViewed"))
            author = ""
            product_title = str(((it.get("productDetails") or {}).get("productName")) or "")
            _, was_new = db.upsert_item(
                store_id=store_id,
                external_id=ext_id,
                item_type="review",
                date=created or _iso_now(),
                rating=rating_i,
                text=full_text.strip(),
                author=author,
                product_title=product_title,
                was_viewed=was_viewed,
            )
            if was_new:
                added += 1
        return added
    except HttpStatusError as e:
        if e.status == 401:
            raise UnauthorizedStoreError(store_id, store.name, str(e)) from e
        if e.status == 429:
            log.warning(
                "WB load_new store_id=%s name=%s: HTTP 429 feedbacks-api (global limiter) — %s",
                store_id,
                store.name,
                _wb_http_body_one_line(e.body or ""),
            )
            await asyncio.sleep(4)
            return 0
        if e.status in (400, 403, 404):
            log.warning("WB API %s: %s", e.status, _wb_http_body_one_line(e.body or ""))
            return 0
        if e.status >= 500:
            log.error(
                "WB load_new store_id=%s: HTTP %s — %s",
                store_id,
                e.status,
                _wb_http_body_one_line(e.body or ""),
            )
            return 0
        log.warning(
            "WB load_new store_id=%s: HTTP %s — %s",
            store_id,
            e.status,
            _wb_http_body_one_line(e.body or ""),
        )
        return 0


async def _load_new_yam(db: Database, store: Store) -> int:
    store_id = store.id
    assert store.business_id is not None
    yam = YamClient(store.api_key, store.business_id)
    try:
        trig = await yam.has_new()
        if not (trig.get("feedbacks") or trig.get("questions")):
            return 0

        q = await yam.list_questions(limit=50, need_answer=True)
        f = await yam.list_feedbacks(limit=50, reaction_status="NEED_REACTION")

        # Собираем все offerId из вопросов и отзывов для запроса названий товаров
        offer_ids: set[str] = set()
        q_result = (q or {}).get("result") or {}
        for it in q_result.get("questions", []) or []:
            ids_obj = it.get("questionIdentifiers") or {}
            oid = (ids_obj.get("offerId") or "").strip()
            if oid:
                offer_ids.add(oid)
        f_result = (f or {}).get("result") or {}
        for it in f_result.get("feedbacks", []) or []:
            idents = it.get("identifiers") or {}
            oid = (idents.get("offerId") or "").strip()
            if oid:
                offer_ids.add(oid)

        offer_names: Dict[str, str] = {}
        if offer_ids:
            try:
                offer_names = await yam.get_offer_names(list(offer_ids))
            except Exception as e:
                log.warning("YAM get_offer_names failed, используем offerId как название: %s", e)

        def _product_title(offer_id: str) -> str:
            return offer_names.get(offer_id, offer_id) if offer_id else ""

        added = 0

        for it in q_result.get("questions", []) or []:
            ids_obj = it.get("questionIdentifiers") or {}
            ext_id = str(ids_obj.get("id", ""))
            if not ext_id:
                continue
            text = str(it.get("text") or "")
            created = str(it.get("createdAt") or "")
            author_obj = it.get("author") or {}
            author = str(author_obj.get("name") or "")
            offer_id = str(ids_obj.get("offerId") or "").strip()
            product_title = _product_title(offer_id) or offer_id
            _, was_new = db.upsert_item(
                store_id=store_id,
                external_id=ext_id,
                item_type="question",
                date=created or _iso_now(),
                rating=None,
                text=text,
                author=author,
                product_title=product_title,
                was_viewed=False,
            )
            if was_new:
                added += 1

        for it in f_result.get("feedbacks", []) or []:
            ext_id = str(it.get("feedbackId", ""))
            if not ext_id:
                continue
            desc = it.get("description") or {}
            comment = str(desc.get("comment") or "")
            advantages = str(desc.get("advantages") or "")
            disadvantages = str(desc.get("disadvantages") or "")
            full_text = comment
            if advantages.strip():
                full_text += ("\nПлюсы: " + advantages.strip())
            if disadvantages.strip():
                full_text += ("\nМинусы: " + disadvantages.strip())
            full_text = full_text.strip()
            created = str(it.get("createdAt") or "")
            stats = it.get("statistics") or {}
            rating = stats.get("rating")
            rating_i = int(rating) if rating is not None else None
            need_reaction = bool(it.get("needReaction"))
            idents = it.get("identifiers") or {}
            offer_id = str(idents.get("offerId") or "").strip()
            product_title = _product_title(offer_id) or offer_id
            author = str(it.get("author") or "")
            _, was_new = db.upsert_item(
                store_id=store_id,
                external_id=ext_id,
                item_type="review",
                date=created or _iso_now(),
                rating=rating_i,
                text=full_text,
                author=author,
                product_title=product_title,
                was_viewed=not need_reaction,
            )
            if was_new:
                added += 1
        return added
    except HttpStatusError as e:
        if e.status == 401:
            raise UnauthorizedStoreError(store_id, store.name, str(e)) from e
        if e.status in (400, 403, 404):
            log.warning("YAM API %s: %s", e.status, e.body)
            return 0
        raise


def _ozon_product_title(it: dict, ext_id: str) -> str:
    """product_name → он; иначе sku → 'SKU <sku>'; иначе 'ID <external_id>'."""
    product_obj = it.get("product") or it.get("product_info") or {}
    name = str(
        it.get("product_name") or it.get("title") or it.get("name") or it.get("product_title")
        or product_obj.get("name") or product_obj.get("title") or product_obj.get("product_name") or ""
    ).strip()
    if name:
        return name
    sku_val = (
        it.get("sku") or it.get("product_id") or it.get("offer_id") or it.get("sku_id")
        or it.get("SKU") or it.get("ProductId")
        or product_obj.get("sku") or product_obj.get("product_id")
    )
    try:
        sku_int = int(sku_val) if sku_val is not None else None
    except (TypeError, ValueError):
        sku_int = None
    if sku_int is not None:
        return f"SKU {sku_int}"
    return f"ID {ext_id}"


def _ozon_sku_from_item(it: dict) -> Optional[int]:
    sku_val = (
        it.get("sku") or it.get("product_id") or it.get("offer_id") or it.get("sku_id")
        or it.get("SKU") or it.get("ProductId")
        or (it.get("product") or {}).get("sku") or (it.get("product_info") or {}).get("product_id")
    )
    try:
        return int(sku_val) if sku_val is not None else None
    except (TypeError, ValueError):
        return None


async def _load_new_ozon(db: Database, store: Store) -> int:
    store_id = store.id
    assert (store.client_id or "").strip()
    ozon = OzonClient(store.client_id, store.api_key)
    try:
        trig = await ozon.has_new()
        if not (trig.get("feedbacks") or trig.get("questions")):
            return 0

        # Собираем вопросы и отзывы в память, затем подгрузим названия батчами и сохраним
        questions_to_save: List[dict] = []
        reviews_to_save: List[dict] = []
        _logged_ozon_question_keys = False
        max_ozon_pages = 50
        max_ozon_items = 5000
        last_id = ""
        page = 0
        while page < max_ozon_pages and len(questions_to_save) < max_ozon_items:
            page += 1
            q = await ozon.list_questions(last_id=last_id, status="NEW")
            q_result = (q or {}).get("result") or (q or {})
            questions = q_result.get("questions") or q_result.get("items") or q_result.get("list") or []
            if not questions:
                break
            for it in questions:
                if not _logged_ozon_question_keys and isinstance(it, dict):
                    log.debug("Ozon question item keys: %s", list(it.keys()))
                    _logged_ozon_question_keys = True
                ext_id = str(it.get("id") or it.get("question_id") or it.get("question_id_str") or "")
                if not ext_id:
                    continue
                product_title_raw = _ozon_product_title(it, ext_id)
                sku_int = _ozon_sku_from_item(it)
                questions_to_save.append({
                    "ext_id": ext_id,
                    "text": str(it.get("text") or it.get("question_text") or it.get("comment") or ""),
                    "created": str(it.get("created_at") or it.get("createdAt") or it.get("create_date") or _iso_now()),
                    "sku_int": sku_int,
                    "product_title_raw": product_title_raw,
                })
            last_id = (q_result.get("last_id") or (q or {}).get("last_id") or "").strip()
            if not last_id:
                break

        last_id = ""
        page = 0
        while page < max_ozon_pages and len(reviews_to_save) < max_ozon_items:
            page += 1
            f = await ozon.list_feedbacks(limit=100, last_id=last_id, status="UNPROCESSED")
            f_result = (f or {}).get("result") or (f or {})
            reviews = f_result.get("reviews") or f_result.get("items") or f_result.get("list") or []
            if not reviews:
                break
            for it in reviews:
                ext_id = str(it.get("id") or it.get("review_id") or it.get("review_id_str") or "")
                if not ext_id:
                    continue
                rating_val = it.get("rating") or it.get("score")
                try:
                    rating_i = int(rating_val) if rating_val is not None else None
                except (TypeError, ValueError):
                    rating_i = None
                product_title_raw = _ozon_product_title(it, ext_id)
                sku_int = _ozon_sku_from_item(it)
                reviews_to_save.append({
                    "ext_id": ext_id,
                    "text": str(it.get("text") or it.get("comment") or it.get("review_text") or ""),
                    "created": str(it.get("published_at") or it.get("created_at") or it.get("createdAt") or it.get("create_date") or _iso_now()),
                    "rating_i": rating_i,
                    "sku_int": sku_int,
                    "product_title_raw": product_title_raw,
                })
            last_id = (f_result.get("last_id") or (f or {}).get("last_id") or "").strip()
            has_next = f_result.get("has_next") if f_result.get("has_next") is not None else (f or {}).get("has_next")
            if not has_next or not last_id:
                break

        # SKU без имени (product_title_raw == "SKU <sku>")
        sku_without_name = set()
        for row in questions_to_save:
            if row["sku_int"] is not None and (row["product_title_raw"] or "").strip() == f"SKU {row['sku_int']}":
                sku_without_name.add(row["sku_int"])
        for row in reviews_to_save:
            if row["sku_int"] is not None and (row["product_title_raw"] or "").strip() == f"SKU {row['sku_int']}":
                sku_without_name.add(row["sku_int"])

        names_map: Dict[int, str] = {}
        if sku_without_name:
            cached = db.get_ozon_sku_names(list(sku_without_name))
            names_map.update(cached)
            to_fetch = [s for s in sku_without_name if s not in cached]
            for i in range(0, len(to_fetch), 1000):
                batch = to_fetch[i : i + 1000]
                try:
                    batch_names = await ozon.get_product_names_by_sku(batch)
                    if batch_names:
                        db.upsert_ozon_sku_names(batch_names)
                        names_map.update(batch_names)
                except HttpStatusError as e:
                    if e.status == 401:
                        raise UnauthorizedStoreError(store_id, store.name, str(e)) from e
                    if e.status in (400, 403, 404):
                        log.warning("Ozon /v3/product/info/list %s: %s", e.status, e.body)
                    else:
                        raise

        def _final_title(raw: str, sku: Optional[int]) -> str:
            if sku is not None and (raw or "").strip() == f"SKU {sku}":
                return names_map.get(sku) or raw
            return raw

        added = 0
        for row in questions_to_save:
            product_title = _final_title(row["product_title_raw"], row["sku_int"])
            extra_json = json.dumps({"sku": row["sku_int"]}) if row["sku_int"] is not None else None
            _, was_new = db.upsert_item(
                store_id=store_id,
                external_id=row["ext_id"],
                item_type="question",
                date=row["created"] or _iso_now(),
                rating=None,
                text=row["text"],
                author="",
                product_title=product_title,
                was_viewed=False,
                extra_json=extra_json,
            )
            if was_new:
                added += 1
        for row in reviews_to_save:
            product_title = _final_title(row["product_title_raw"], row["sku_int"])
            _, was_new = db.upsert_item(
                store_id=store_id,
                external_id=row["ext_id"],
                item_type="review",
                date=row["created"] or _iso_now(),
                rating=row["rating_i"],
                text=row["text"],
                author="",
                product_title=product_title,
                was_viewed=False,
            )
            if was_new:
                added += 1

        return added
    except HttpStatusError as e:
        if e.status == 401:
            raise UnauthorizedStoreError(store_id, store.name, str(e)) from e
        if e.status in (400, 403, 404):
            log.warning("Ozon API %s: %s", e.status, e.body)
            return 0
        raise


async def _ozon_recheck_processed(db: Database, store: Store) -> int:
    """
    Синхронизация обработанных отзывов на Ozon:
    - тянем review/list со status=PROCESSED (постранично)
    - если такой review есть у нас со статусом new/generated, переводим в sent (чтобы не отвечать повторно)
    Возвращает количество обновлённых локальных записей.
    """
    store_id = store.id
    assert (store.client_id or "").strip()
    ozon = OzonClient(store.client_id, store.api_key)
    updated = 0
    max_pages = 20
    max_items = 2000
    last_id = ""
    page = 0
    seen = 0
    while page < max_pages and seen < max_items:
        page += 1
        f = await ozon.list_feedbacks(limit=100, last_id=last_id, status="PROCESSED")
        f_result = (f or {}).get("result") or (f or {})
        reviews = f_result.get("reviews") or f_result.get("items") or f_result.get("list") or []
        if not reviews:
            break
        for it in reviews:
            ext_id = str(it.get("id") or it.get("review_id") or it.get("review_id_str") or "")
            if not ext_id:
                continue
            item_id = db.find_item_id(store_id, "review", ext_id)
            if not item_id:
                continue
            row = db.get_item_by_id(item_id)
            if row and row.status in ("new", "generated"):
                db.set_sent(item_id, _iso_now())
                updated += 1
        seen += len(reviews)
        last_id = (f_result.get("last_id") or (f or {}).get("last_id") or "").strip()
        has_next = f_result.get("has_next") if f_result.get("has_next") is not None else (f or {}).get("has_next")
        if not has_next or not last_id:
            break
    return updated


async def load_new_all(
    db: Database,
    store_list: List[Store],
    progress_queue: Optional[Queue] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> int:
    """
    Загружает новые отзывы/вопросы по всем магазинам из store_list.
    progress_queue: при наличии в неё кладутся ("progress", current, total).
    progress_cb: при наличии вызывается с (current, total) для обновления прогресса (например в веб-задаче).
    Возвращает суммарное количество добавленных записей.
    """
    def _progress(cur: int, tot: int) -> None:
        _put_progress(progress_queue, cur, tot)
        if progress_cb:
            try:
                progress_cb(cur, tot)
            except Exception:
                pass

    total = 0
    n_stores = len(store_list)
    _progress(0, n_stores)
    for i, store in enumerate(store_list):
        try:
            n = await load_new_items(db, store)
            total += n
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except UnauthorizedStoreError:
            raise
        except Exception as e:
            if isinstance(e, HttpStatusError):
                lvl = log.warning if e.status == 429 else log.error
                lvl(
                    "load_new_all магазин %s (%s) store_id=%s: HTTP %s — %s",
                    store.name,
                    store.marketplace,
                    store.id,
                    e.status,
                    _wb_http_body_one_line(e.body or "", max_len=400),
                )
            else:
                log.exception("load_new_all магазин %s (%s) store_id=%s: %s", store.name, store.marketplace, store.id, e)
        _progress(i + 1, n_stores)
    return total


# Сколько отзывов/вопросов генерировать параллельно (упирается в лимиты API).
GENERATE_CONCURRENCY = 5


async def _apply_packer_issue_telegram(
    db: Database,
    row: ItemRow,
    packer_issue: bool,
) -> None:
    if row.item_type != "review" or not packer_issue:
        return
    telegram_enabled = (db.get_setting("telegram_enabled") or "1").strip() != "0"
    telegram_token = db.get_setting("telegram_bot_token")
    telegram_chat_id = db.get_setting("telegram_chat_id")
    if not (telegram_enabled and telegram_token and telegram_chat_id):
        return
    store_name = _store_name(db, row.store_id)
    await send_review_to_chat(
        telegram_token,
        telegram_chat_id,
        row.product_title or row.external_id,
        row.text or "",
        store_name=store_name,
    )


def _parse_generation_json(txt: str, *, context: str) -> dict:
    obj = json.loads(txt)
    if not isinstance(obj, dict):
        raise ValueError(f"{context}: ответ не объект JSON")
    reply = (obj.get("reply") or "").strip()
    if not reply:
        raise ValueError(f"{context}: в JSON нет reply")
    return obj


async def _generate_one(
    db: Database,
    client: OpenAIClient,
    system: str,
    item_id: int,
    sem: asyncio.Semaphore,
) -> bool:
    """Генерирует ответ для одного item. Возвращает True при успехе. Ответ ИИ — строго JSON {reply, packer_issue}."""
    async with sem:
        row = db.get_item_by_id(item_id)
        if not row:
            return False
        if row.item_type == "review":
            rg = _rating_group(row.rating)
            p = db.get_prompt("review", "general" if not row.text.strip() else rg)
            body_text = row.text.strip() if row.text.strip() else "Отзыв без текста."
            body_label = f"Оценка: {row.rating}\nТекст отзыва"
            user = build_generation_user_prompt(
                db,
                task_prompt=p,
                product_title=row.product_title or row.external_id,
                body_label=body_label,
                body_text=body_text,
                closing="Сформируй ответ на отзыв.",
            )
            source_type = "review"
        else:
            p = db.get_prompt("question", "general")
            user = build_generation_user_prompt(
                db,
                task_prompt=p,
                product_title=row.product_title or row.external_id,
                body_label="Вопрос",
                body_text=row.text or "",
                closing="Сформируй ответ на вопрос.",
            )
            source_type = "question"
        try:
            txt = await client.generate(system, user)
            txt = (txt or "").strip()
            if not txt:
                log.warning("Generate item_id=%s: пустой ответ от модели", item_id)
                return False
            try:
                obj = _parse_generation_json(txt, context=f"item_id={item_id}")
            except (json.JSONDecodeError, ValueError) as e:
                log.warning("Generate item_id=%s: %s — %s", item_id, e, txt[:200])
                return False
            reply = (obj.get("reply") or "").strip()
            db.set_generated(item_id, reply)
            await _apply_packer_issue_telegram(db, row, bool(obj.get("packer_issue")))
            await maybe_record_card_error(
                db,
                obj,
                store_id=row.store_id,
                store_name=_store_name(db, row.store_id),
                product_title=row.product_title or row.external_id,
                customer_text=row.text or "",
                source_type=source_type,
                source_ref=str(item_id),
            )
            return True
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception as e:
            if isinstance(e, HttpStatusError):
                log.warning("Generate item=%s: %s", item_id, e)
            else:
                log.exception("Generate failed item=%s: %s", item_id, e)
            return False


async def generate_mass(
    db: Database,
    item_ids: List[int],
    openai_key: str,
    model: str = "gpt-5.2",
    progress_queue: Optional[Queue] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Tuple[int, int]:
    """
    Генерирует ответы для items (new/generated можно перегенерить) и сохраняет в DB.
    progress_queue: при наличии в неё кладутся ("progress", current, total).
    progress_cb: при наличии вызывается с (current, total).
    Возвращает (ok, failed).
    """
    def _progress(cur: int, tot: int) -> None:
        _put_progress(progress_queue, cur, tot)
        if progress_cb:
            try:
                progress_cb(cur, tot)
            except Exception:
                pass

    if not item_ids:
        return 0, 0
    total = len(item_ids)
    _progress(0, total)
    client = OpenAIClient(openai_key, model=model)
    system = "Ты — официальный представитель магазина на Wildberries. Отвечай строго вежливо, кратко и по делу. Без эмодзи. Без фамильярности. Без предложений решений, компенсаций или обращений в поддержку. Не задавай вопросов покупателю. Не выдумывай факты. 2–3 коротких предложения максимум. Не повторяй полное название товара в ответе. Не уточняй ничего"
    sem = asyncio.Semaphore(GENERATE_CONCURRENCY)
    done = 0
    done_lock = asyncio.Lock()

    async def run_one(iid: int) -> bool:
        nonlocal done
        r = await _generate_one(db, client, system, iid, sem)
        async with done_lock:
            done += 1
            _progress(done, total)
        return r

    tasks = [run_one(iid) for iid in item_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ok = 0
    failed = 0
    for r in results:
        if r is True:
            ok += 1
        elif r is False:
            failed += 1
        else:
            failed += 1
            if isinstance(r, (asyncio.CancelledError, GeneratorExit)):
                raise r
    if client._quota_exhausted.is_set():
        log.warning(
            "generate_mass: OpenAI insufficient_quota — после первого отказа по биллингу остальные items "
            "частично пропущены без лишних запросов; проверьте https://platform.openai.com/account/billing"
        )
    return ok, failed

async def send_mass(
    db: Database,
    store: Store,
    item_ids: List[int],
    progress_queue: Optional[Queue] = None,
) -> Tuple[int, int, int]:
    """
    Отправляет ответы по items. Требует generated_text.
    progress_queue: при наличии в неё кладутся ("progress", current, total).
    Возвращает (sent_ok, skipped_no_text, failed).
    """
    store_id = store.id
    if store.marketplace == "wb":
        client = WbClient(store.api_key)
    elif store.marketplace == "yam":
        if store.business_id is None:
            log.warning("YAM store id=%s без business_id, отправка невозможна", store_id)
            return 0, 0, len(item_ids)
        client = YamClient(store.api_key, store.business_id)
    elif store.marketplace == "ozon":
        if not (store.client_id or "").strip():
            log.warning("Ozon store id=%s без client_id, отправка невозможна", store_id)
            return 0, 0, len(item_ids)
        client = OzonClient(store.client_id, store.api_key)
    else:
        log.warning("Неизвестный маркетплейс %s для store_id=%s", store.marketplace, store_id)
        return 0, 0, len(item_ids)

    sent_ok = 0
    skipped = 0
    failed = 0
    total = len(item_ids)
    _put_progress(progress_queue, 0, total)

    for idx, item_id in enumerate(item_ids):
        row = db.get_item_by_id(item_id)
        if not row:
            failed += 1
            _put_progress(progress_queue, idx + 1, total)
            continue
        # Защита от повторной отправки (на всякий случай, особенно для Ozon).
        if getattr(row, "status", "") == "sent":
            skipped += 1
            _put_progress(progress_queue, idx + 1, total)
            continue
        text = (row.generated_text or "").strip()
        if not text:
            skipped += 1
            _put_progress(progress_queue, idx + 1, total)
            continue
        try:
            if row.item_type == "review":
                await client.answer_feedback(row.external_id, text)
            else:
                if store.marketplace == "ozon":
                    try:
                        extra = json.loads(row.extra_json or "{}") if (row.extra_json or "").strip() else {}
                        sku = int(extra.get("sku"))
                    except (json.JSONDecodeError, TypeError, ValueError, KeyError):
                        log.warning("Ozon question item_id=%s: нет sku в extra_json, пропуск", item_id)
                        failed += 1
                        _put_progress(progress_queue, idx + 1, total)
                        continue
                    await client.answer_question(row.external_id, text, sku)
                else:
                    await client.answer_question(row.external_id, text)
            db.set_sent(item_id, _iso_now())
            sent_ok += 1
        except HttpStatusError as e:
            if e.status == 401:
                raise UnauthorizedStoreError(store_id, store.name, str(e)) from e
            log.exception("Send failed item=%s: %s", item_id, e)
            failed += 1
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception as e:
            log.exception("Send failed item=%s: %s", item_id, e)
            failed += 1
        _put_progress(progress_queue, idx + 1, total)

    return sent_ok, skipped, failed


async def send_mass_all(
    db: Database,
    item_ids: List[int],
    progress_queue: Optional[Queue] = None,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Tuple[int, int, int]:
    """
    Отправляет ответы по items из разных магазинов.
    progress_queue: при наличии передаётся в send_mass для каждого магазина.
    progress_cb: при наличии вызывается с (current, total).
    Возвращает (total_sent_ok, total_skipped, total_failed).
    """
    def _progress(cur: int, tot: int) -> None:
        _put_progress(progress_queue, cur, tot)
        if progress_cb:
            try:
                progress_cb(cur, tot)
            except Exception:
                pass

    stores_by_id = {s.id: s for s in db.list_stores()}
    by_store: Dict[int, List[int]] = {}
    for item_id in item_ids:
        row = db.get_item_by_id(item_id)
        if not row or not (row.generated_text or "").strip():
            continue
        by_store.setdefault(row.store_id, []).append(item_id)
    sent_ok = 0
    skipped = 0
    failed = 0
    store_list = [(sid, ids) for sid, ids in by_store.items()]
    total_items = sum(len(ids) for _, ids in store_list)
    done_items = 0
    _progress(0, total_items if total_items else 1)
    for store_id, ids in store_list:
        store = stores_by_id.get(store_id)
        if not store or not store.api_key:
            failed += len(ids)
            done_items += len(ids)
            _progress(done_items, total_items or 1)
            continue
        if store.marketplace == "yam" and store.business_id is None:
            failed += len(ids)
            done_items += len(ids)
            _progress(done_items, total_items or 1)
            continue
        if store.marketplace == "ozon" and not (store.client_id or "").strip():
            failed += len(ids)
            done_items += len(ids)
            _progress(done_items, total_items or 1)
            continue
        try:
            so, sk, fl = await send_mass(db, store, ids)
            sent_ok += so
            skipped += sk
            failed += fl
            done_items += len(ids)
            _progress(done_items, total_items or 1)
        except HttpStatusError as e:
            if e.status == 401:
                raise UnauthorizedStoreError(store_id, store.name, str(e)) from e
            log.exception("send_mass_all store_id=%s: %s", store_id, e)
            failed += len(ids)
            done_items += len(ids)
            _progress(done_items, total_items or 1)
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception as e:
            log.exception("send_mass_all store_id=%s: %s", store_id, e)
            failed += len(ids)
            done_items += len(ids)
            _progress(done_items, total_items or 1)
    return sent_ok, skipped, failed


async def generate_wb_buyer_chat_reply(
    db: Database,
    openai_key: str,
    *,
    product_title: str,
    conversation_excerpt: str,
    model: str = "gpt-5.2",
    openai_client: Optional[OpenAIClient] = None,
    store_id: Optional[int] = None,
    chat_id: Optional[str] = None,
    client_message_key: Optional[str] = None,
    customer_text: Optional[str] = None,
) -> str:
    """
    Черновик ответа продавца в чате WB: тот же JSON-формат, что и для вопросов/отзывов.
    openai_client: общий экземпляр (например массовая обработка чатов) — один circuit insufficient_quota.
    """
    client = openai_client or OpenAIClient(openai_key, model=model)
    system = (
        "Ты — официальный представитель магазина на Wildberries в переписке с покупателем. "
        "Отвечай строго вежливо, кратко и по делу. Без эмодзи. Без фамильярности. "
        "Без предложений компенсаций или обращений в поддержку. Не задавай вопросов покупателю. "
        "Не выдумывай факты. 2–4 коротких предложения. Не повторяй полное название товара в ответе."
    )
    p = db.get_prompt("buyer_chat", "general")
    user = build_generation_user_prompt(
        db,
        task_prompt=p,
        product_title=product_title,
        body_label="Переписка (покупатель / продавец)",
        body_text=conversation_excerpt,
        closing="Сформируй ответ продавца на последние сообщения покупателя.",
    )
    txt = await client.generate(system, user)
    txt = (txt or "").strip()
    if not txt:
        raise ValueError("Пустой ответ модели")
    obj = _parse_generation_json(txt, context="wb_chat")
    reply = (obj.get("reply") or "").strip()
    if store_id is not None and chat_id and client_message_key:
        await maybe_record_card_error(
            db,
            obj,
            store_id=int(store_id),
            store_name=_store_name(db, int(store_id)),
            product_title=product_title,
            customer_text=customer_text or conversation_excerpt,
            source_type="wb_chat",
            source_ref=f"{chat_id}:{client_message_key}",
        )
    return reply


def _buyer_chat_reply_from(db: Database) -> Optional[dt.date]:
    return parse_reply_from_date(db.get_setting(SETTING_REPLY_FROM))


def _buyer_chat_auto_max_age_days(db: Database) -> int:
    return parse_auto_chat_max_age_days(db.get_setting(SETTING_AUTO_CHAT_MAX_AGE_DAYS))


def _wb_chat_eligibility(
    db: Database,
    store_id: int,
    chat_id: str,
    lines_ts: List[tuple],
    reply_from: Optional[dt.date],
    *,
    max_age_days: Optional[int] = None,
) -> Tuple[bool, str, str, int]:
    """
    (eligible, skip_reason, client_message_key, client_ts_ms).
    skip_reason: last_not_client | no_client | before_cutoff | too_old | already_replied | ok
    """
    if not lines_ts or lines_ts[-1][0] != "client":
        return False, "last_not_client", "", 0
    info = wb_last_client_info(lines_ts)
    if not info:
        return False, "no_client", "", 0
    msg_key, ts = info
    if max_age_days is not None and not wb_ts_within_max_age(ts, max_age_days):
        return False, "too_old", msg_key, ts
    if not wb_ts_ms_after_cutoff(ts, reply_from):
        return False, "before_cutoff", msg_key, ts
    if db.is_buyer_chat_replied(store_id, "wb", chat_id, msg_key):
        return False, "already_replied", msg_key, ts
    return True, "ok", msg_key, ts


def _ozon_chat_eligibility(
    db: Database,
    store_id: int,
    chat_id: str,
    lines: List[tuple],
    reply_from: Optional[dt.date],
    *,
    max_age_days: Optional[int] = None,
) -> Tuple[bool, str, str, str]:
    """(eligible, skip_reason, client_message_key, created_at)."""
    if not lines or lines[-1][0] != "client":
        return False, "last_not_client", "", ""
    info = ozon_last_client_info(lines)
    if not info:
        return False, "no_client", "", ""
    msg_key, created = info
    if max_age_days is not None and not ozon_iso_within_max_age(created, max_age_days):
        return False, "too_old", msg_key, created
    if not ozon_iso_after_cutoff(created, reply_from):
        return False, "before_cutoff", msg_key, created
    if db.is_buyer_chat_replied(store_id, "ozon", chat_id, msg_key):
        return False, "already_replied", msg_key, created
    return True, "ok", msg_key, created


async def generate_ozon_buyer_chat_reply(
    db: Database,
    openai_key: str,
    *,
    product_title: str,
    conversation_excerpt: str,
    model: str = "gpt-5.2",
    openai_client: Optional[OpenAIClient] = None,
    store_id: Optional[int] = None,
    chat_id: Optional[str] = None,
    client_message_key: Optional[str] = None,
    customer_text: Optional[str] = None,
) -> str:
    client = openai_client or OpenAIClient(openai_key, model=model)
    system = (
        "Ты — официальный представитель магазина на Ozon в переписке с покупателем. "
        "Отвечай строго вежливо, кратко и по делу. Без эмодзи. Без фамильярности. "
        "Без предложений компенсаций или обращений в поддержку. Не задавай вопросов покупателю. "
        "Не выдумывай факты. 2–4 коротких предложения. Не повторяй полное название товара в ответе."
    )
    p = db.get_prompt("buyer_chat", "general")
    user = build_generation_user_prompt(
        db,
        task_prompt=p,
        product_title=product_title,
        body_label="Переписка (покупатель / продавец)",
        body_text=conversation_excerpt,
        closing="Сформируй ответ продавца на последние сообщения покупателя.",
    )
    txt = await client.generate(system, user)
    txt = (txt or "").strip()
    if not txt:
        raise ValueError("Пустой ответ модели")
    obj = _parse_generation_json(txt, context="ozon_chat")
    reply = (obj.get("reply") or "").strip()
    if store_id is not None and chat_id and client_message_key:
        await maybe_record_card_error(
            db,
            obj,
            store_id=int(store_id),
            store_name=_store_name(db, int(store_id)),
            product_title=product_title,
            customer_text=customer_text or conversation_excerpt,
            source_type="ozon_chat",
            source_ref=f"{chat_id}:{client_message_key}",
        )
    return reply


def _last_client_text_from_lines(lines_ts: List[tuple]) -> str:
    for role, text, *_ in reversed(lines_ts):
        if role == "client" and (text or "").strip():
            return str(text).strip()
    return ""


async def wb_buyer_chats_mass_generate_send_for_store(
    db: Database,
    store: Store,
    *,
    openai_key: str,
    event_pages: int = 12,
    max_chats: int = 50,
    model: str = "gpt-5.2",
    pause_between_chats_sec: float = 0.0,
    max_message_age_days: Optional[int] = None,
    audit_actor: str = "system",
) -> Dict[str, int]:
    """
    Чаты WB, где в треде последнее сообщение от покупателя: сгенерировать ответ (OpenAI) и сразу отправить в WB.
    Не больше max_chats чатов за вызов. pause_between_chats_sec снижает риск 429 на buyer-chat.
    max_message_age_days — только для автоответов (None = без лимита по свежести).
    """
    stats: Dict[str, int] = {
        "wb_chat_candidates": 0,
        "wb_chat_eligible": 0,
        "wb_chat_sent": 0,
        "wb_chat_gen_failed": 0,
        "wb_chat_send_failed": 0,
        "wb_chat_skipped_no_reply_sign": 0,
        "wb_chat_skipped_already_replied": 0,
        "wb_chat_skipped_before_cutoff": 0,
        "wb_chat_skipped_too_old": 0,
    }
    key = (openai_key or "").strip()
    if not key:
        return stats
    if store.marketplace != "wb" or not (store.api_key or "").strip():
        return stats
    reply_from = _buyer_chat_reply_from(db)
    oai = OpenAIClient(openai_key, model=model)
    client = WbBuyerChatClient(store.api_key)
    chats = await client.list_chats()
    chat_rows = [c for c in chats if isinstance(c, dict) and c.get("chatID")]
    chat_rows.sort(
        key=lambda c: int((c.get("lastMessage") or {}).get("addTimestamp") or 0),
        reverse=True,
    )
    candidates: List[tuple[str, dict, List[tuple], str, List[dict]]] = []
    max_scan = min(len(chat_rows), max(30, int(max_chats) * 6))
    for row in chat_rows[:max_scan]:
        cid = str(row.get("chatID") or "").strip()
        if not cid:
            continue
        try:
            evs, _ = await fetch_events_for_chat(client, cid, max_wb_requests=event_pages)
        except HttpStatusError as e:
            log.warning("wb_chat_mass events store=%s chat=%s: HTTP %s", store.id, cid, e.status)
            continue
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            log.exception("wb_chat_mass events store=%s chat=%s", store.id, cid)
            continue
        lines_ts = collect_thread_lines(evs, cid)
        if not lines_ts:
            lines_ts = fallback_line_from_chat_row(row)
        ok, reason, _mk, _ts = _wb_chat_eligibility(
            db, store.id, cid, lines_ts, reply_from, max_age_days=max_message_age_days
        )
        if not ok:
            if reason == "already_replied":
                stats["wb_chat_skipped_already_replied"] += 1
            elif reason == "before_cutoff":
                stats["wb_chat_skipped_before_cutoff"] += 1
            elif reason == "too_old":
                stats["wb_chat_skipped_too_old"] += 1
            continue
        candidates.append((cid, row, lines_ts, _mk, evs))
        if len(candidates) >= max(0, int(max_chats)):
            break
    stats["wb_chat_eligible"] = len(candidates)
    take = candidates[: max(0, int(max_chats))]
    stats["wb_chat_candidates"] = len(take)
    for i, (cid, row, lines_ts, _pre_mk, evs) in enumerate(take):
        gc = merge_good_card(row, evs)
        _ok, _reason, client_msg_key, _ts = _wb_chat_eligibility(
            db, store.id, cid, lines_ts, reply_from, max_age_days=max_message_age_days
        )
        texts = [t for _, t, __, ___ in lines_ts]
        title = product_title_from_wb_chat(gc, texts)
        excerpt_parts = []
        for role, text, _, __ in lines_ts:
            label = "Покупатель" if role == "client" else "Продавец" if role == "seller" else role
            excerpt_parts.append(f"{label}: {text}")
        conversation = "\n".join(excerpt_parts)
        reply_sign = str(row.get("replySign") or "").strip()
        if not reply_sign:
            stats["wb_chat_skipped_no_reply_sign"] += 1
            continue
        try:
            draft = await generate_wb_buyer_chat_reply(
                db,
                key,
                product_title=title,
                conversation_excerpt=conversation,
                model=model,
                openai_client=oai,
                store_id=store.id,
                chat_id=cid,
                client_message_key=client_msg_key,
                customer_text=_last_client_text_from_lines(lines_ts),
            )
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except HttpStatusError as e:
            log.warning("wb_chat_mass generate store=%s chat=%s: %s", store.id, cid, e)
            stats["wb_chat_gen_failed"] += 1
            if pause_between_chats_sec > 0 and i + 1 < len(take):
                await asyncio.sleep(pause_between_chats_sec)
            continue
        except Exception:
            log.exception("wb_chat_mass generate store=%s chat=%s", store.id, cid)
            stats["wb_chat_gen_failed"] += 1
            if pause_between_chats_sec > 0 and i + 1 < len(take):
                await asyncio.sleep(pause_between_chats_sec)
            continue
        try:
            await client.send_message(reply_sign, draft)
            db.mark_buyer_chat_replied(store.id, "wb", cid, client_msg_key)
            stats["wb_chat_sent"] += 1
            _audit_activity(
                db,
                action="wb_buyer_chat_send",
                store_id=store.id,
                result="ok",
                item_type="wb_chat",
                actor=audit_actor,
                meta={
                    "chat_id": cid,
                    "product_title": title[:200],
                    "message_preview": draft[:400],
                    "source": "auto" if audit_actor == "system" else "manual",
                },
            )
        except HttpStatusError:
            log.warning("wb_chat_mass send HTTP store=%s chat=%s", store.id, cid)
            stats["wb_chat_send_failed"] += 1
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            log.exception("wb_chat_mass send store=%s chat=%s", store.id, cid)
            stats["wb_chat_send_failed"] += 1
        if pause_between_chats_sec > 0 and i + 1 < len(take):
            await asyncio.sleep(pause_between_chats_sec)
    return stats


async def auto_process_wb_buyer_chats(
    db: Database,
    stores: List[Store],
    *,
    openai_key: str,
    event_pages: int = 12,
    max_autosend_per_store: int = 5,
    model: str = "gpt-5.2",
) -> Dict[str, int]:
    """
    Автоответ в чатах WB: один проход по ленте событий, чаты где последнее сообщение от покупателя —
    генерим и отправляем (как по отзывам/вопросам). Не больше max_autosend_per_store чатов за слот на магазин.
    """
    stats: Dict[str, int] = {
        "wb_chat_stores": 0,
        "wb_chat_candidates": 0,
        "wb_chat_sent": 0,
        "wb_chat_gen_failed": 0,
        "wb_chat_send_failed": 0,
    }
    key = (openai_key or "").strip()
    if not key:
        return stats
    for store in stores:
        if store.marketplace != "wb" or not (store.api_key or "").strip():
            continue
        stats["wb_chat_stores"] += 1
        try:
            part = await wb_buyer_chats_mass_generate_send_for_store(
                db,
                store,
                openai_key=key,
                event_pages=event_pages,
                max_chats=max_autosend_per_store,
                model=model,
                pause_between_chats_sec=0.0,
                max_message_age_days=_buyer_chat_auto_max_age_days(db),
            )
        except HttpStatusError as e:
            log.warning("wb_chat_auto store=%s: HTTP %s", store.id, e.status)
            continue
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            log.exception("wb_chat_auto store=%s: list/events failed", store.id)
            continue
        stats["wb_chat_candidates"] += int(part.get("wb_chat_candidates") or 0)
        stats["wb_chat_sent"] += int(part.get("wb_chat_sent") or 0)
        stats["wb_chat_gen_failed"] += int(part.get("wb_chat_gen_failed") or 0)
        stats["wb_chat_send_failed"] += int(
            int(part.get("wb_chat_send_failed") or 0) + int(part.get("wb_chat_skipped_no_reply_sign") or 0)
        )
        _audit_activity(
            db,
            action="store_wb_chats_auto",
            store_id=store.id,
            result="ok" if int(part.get("wb_chat_sent") or 0) else "skip",
            item_type="wb_chat",
            meta={
                "store_name": store.name,
                **part,
            },
        )
    return stats


async def ozon_buyer_chats_mass_generate_send_for_store(
    db: Database,
    store: Store,
    *,
    openai_key: str,
    max_chats: int = 50,
    model: str = "gpt-5.2",
    pause_between_chats_sec: float = 1.0,
    max_message_age_days: Optional[int] = None,
    audit_actor: str = "system",
    history_limit: int = 100,
) -> Dict[str, int]:
    stats: Dict[str, int] = {
        "ozon_chat_candidates": 0,
        "ozon_chat_eligible": 0,
        "ozon_chat_sent": 0,
        "ozon_chat_gen_failed": 0,
        "ozon_chat_send_failed": 0,
        "ozon_chat_skipped_already_replied": 0,
        "ozon_chat_skipped_before_cutoff": 0,
        "ozon_chat_skipped_support": 0,
        "ozon_chat_skipped_reply_window": 0,
        "ozon_chat_skipped_too_old": 0,
        "ozon_chat_skipped_no_access": 0,
        "ozon_chat_skip_reason": "",
    }
    key = (openai_key or "").strip()
    if not key:
        return stats
    if store.marketplace != "ozon" or not (store.client_id or "").strip() or not (store.api_key or "").strip():
        return stats
    reply_from = _buyer_chat_reply_from(db)
    oai = OpenAIClient(openai_key, model=model)
    client = OzonClient(store.client_id or "", store.api_key)
    try:
        rows = await client.list_all_buyer_chats(unread_only=False)
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="chat")
        if reason:
            stats["ozon_chat_skipped_no_access"] = 1
            stats["ozon_chat_skip_reason"] = reason
            stats["message"] = ozon_feature_unavailable_user_message(reason, feature="chat")
            log.info("ozon_chat_mass store=%s skipped: %s (HTTP %s)", store.id, reason, e.status)
            return stats
        raise
    candidates: List[tuple[str, dict, List[tuple], str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        chat_id = ozon_chat_row_id(row)
        if not chat_id:
            continue
        if not is_ozon_buyer_chat_row(row):
            stats["ozon_chat_skipped_support"] += 1
            continue
        hist = await client.chat_history(chat_id, limit=history_limit)
        messages = hist.get("messages") or []
        if not isinstance(messages, list):
            messages = []
        lines = collect_ozon_thread_lines(messages)
        chat_obj = row.get("chat") if isinstance(row.get("chat"), dict) else {}
        chat_status = str(chat_obj.get("chat_status") or "")
        window = ozon_reply_window_hint(lines, chat_status=chat_status)
        if window.get("blocked"):
            stats["ozon_chat_skipped_reply_window"] += 1
            continue
        ok, reason, msg_key, _created = _ozon_chat_eligibility(
            db, store.id, chat_id, lines, reply_from, max_age_days=max_message_age_days
        )
        if not ok:
            if reason == "already_replied":
                stats["ozon_chat_skipped_already_replied"] += 1
            elif reason == "before_cutoff":
                stats["ozon_chat_skipped_before_cutoff"] += 1
            elif reason == "too_old":
                stats["ozon_chat_skipped_too_old"] += 1
            continue
        candidates.append((chat_id, row, lines, msg_key))
    stats["ozon_chat_eligible"] = len(candidates)
    take = candidates[: max(0, int(max_chats))]
    stats["ozon_chat_candidates"] = len(take)
    for i, (chat_id, _row, lines, client_msg_key) in enumerate(take):
        title = product_title_from_ozon_chat([], lines)
        excerpt_parts = []
        for role, text, _mid, _ca in lines:
            label = "Покупатель" if role == "client" else "Продавец" if role == "seller" else role
            excerpt_parts.append(f"{label}: {text}")
        conversation = "\n".join(excerpt_parts)
        try:
            draft = await generate_ozon_buyer_chat_reply(
                db,
                key,
                product_title=title,
                conversation_excerpt=conversation,
                model=model,
                openai_client=oai,
                store_id=store.id,
                chat_id=chat_id,
                client_message_key=client_msg_key,
                customer_text=_last_client_text_from_lines(lines),
            )
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except HttpStatusError as e:
            log.warning("ozon_chat_mass generate store=%s chat=%s: %s", store.id, chat_id, e)
            stats["ozon_chat_gen_failed"] += 1
            if pause_between_chats_sec > 0 and i + 1 < len(take):
                await asyncio.sleep(pause_between_chats_sec)
            continue
        except Exception:
            log.exception("ozon_chat_mass generate store=%s chat=%s", store.id, chat_id)
            stats["ozon_chat_gen_failed"] += 1
            if pause_between_chats_sec > 0 and i + 1 < len(take):
                await asyncio.sleep(pause_between_chats_sec)
            continue
        try:
            await client.send_chat_message(chat_id, draft)
            db.mark_buyer_chat_replied(store.id, "ozon", chat_id, client_msg_key)
            stats["ozon_chat_sent"] += 1
            _audit_activity(
                db,
                action="ozon_buyer_chat_send",
                store_id=store.id,
                result="ok",
                item_type="ozon_chat",
                actor=audit_actor,
                meta={
                    "chat_id": chat_id,
                    "product_title": title[:200],
                    "message_preview": draft[:400],
                    "source": "auto" if audit_actor == "system" else "manual",
                },
            )
        except HttpStatusError:
            log.warning("ozon_chat_mass send HTTP store=%s chat=%s", store.id, chat_id)
            stats["ozon_chat_send_failed"] += 1
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            log.exception("ozon_chat_mass send store=%s chat=%s", store.id, chat_id)
            stats["ozon_chat_send_failed"] += 1
        if pause_between_chats_sec > 0 and i + 1 < len(take):
            await asyncio.sleep(pause_between_chats_sec)
    return stats


async def auto_process_ozon_buyer_chats(
    db: Database,
    stores: List[Store],
    *,
    openai_key: str,
    max_autosend_per_store: int = 5,
    model: str = "gpt-5.2",
) -> Dict[str, int]:
    stats: Dict[str, int] = {
        "ozon_chat_stores": 0,
        "ozon_chat_candidates": 0,
        "ozon_chat_sent": 0,
        "ozon_chat_gen_failed": 0,
        "ozon_chat_send_failed": 0,
        "ozon_chat_skipped_no_access": 0,
    }
    key = (openai_key or "").strip()
    if not key:
        return stats
    for store in stores:
        if store.marketplace != "ozon":
            continue
        if not (store.client_id or "").strip() or not (store.api_key or "").strip():
            continue
        stats["ozon_chat_stores"] += 1
        try:
            part = await ozon_buyer_chats_mass_generate_send_for_store(
                db,
                store,
                openai_key=key,
                max_chats=max_autosend_per_store,
                model=model,
                pause_between_chats_sec=1.0,
                max_message_age_days=_buyer_chat_auto_max_age_days(db),
            )
        except HttpStatusError as e:
            log.warning("ozon_chat_auto store=%s: HTTP %s", store.id, e.status)
            continue
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            log.exception("ozon_chat_auto store=%s: list/history failed", store.id)
            continue
        stats["ozon_chat_candidates"] += int(part.get("ozon_chat_candidates") or 0)
        stats["ozon_chat_sent"] += int(part.get("ozon_chat_sent") or 0)
        stats["ozon_chat_gen_failed"] += int(part.get("ozon_chat_gen_failed") or 0)
        stats["ozon_chat_send_failed"] += int(part.get("ozon_chat_send_failed") or 0)
        stats["ozon_chat_skipped_no_access"] += int(part.get("ozon_chat_skipped_no_access") or 0)
        sent = int(part.get("ozon_chat_sent") or 0)
        no_access = int(part.get("ozon_chat_skipped_no_access") or 0)
        _audit_activity(
            db,
            action="store_ozon_chats_auto",
            store_id=store.id,
            result="ok" if sent else ("skip" if no_access or part.get("ozon_chat_skip_reason") else "skip"),
            item_type="ozon_chat",
            meta={
                "store_name": store.name,
                **part,
            },
        )
    return stats


async def ozon_actions_auto_remove_for_store(
    store: Store,
    *,
    only_auto_add: bool = True,
    action_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Удалить товары из автоакций Ozon (или из указанных action_ids)."""
    if store.marketplace != "ozon":
        return {"skipped": 1, "reason": "not_ozon_store"}
    if not (store.client_id or "").strip() or not (store.api_key or "").strip():
        return {"skipped": 1, "reason": "no_ozon_keys"}
    client = OzonClient(store.client_id or "", store.api_key)
    try:
        return await auto_remove_from_ozon_auto_actions(
            client,
            only_auto_add=only_auto_add,
            action_ids=action_ids,
        )
    except HttpStatusError as e:
        reason = ozon_http_skip_reason(e.status, e.body or "", feature="actions")
        if reason:
            log.info("ozon_actions_auto_remove store=%s skipped: %s (HTTP %s)", store.id, reason, e.status)
            return {
                "skipped": 1,
                "reason": reason,
                "message": ozon_feature_unavailable_user_message(reason, feature="actions"),
                "actions_matched": 0,
                "actions_processed": 0,
                "products_removed": 0,
            }
        log.warning("ozon_actions_auto_remove store=%s: HTTP %s", store.id, e.status)
        return {"skipped": 1, "reason": "http_error", "status": e.status, "body": (e.body or "")[:300]}
    except (asyncio.CancelledError, GeneratorExit):
        raise
    except Exception as e:
        log.exception("ozon_actions_auto_remove store=%s", store.id)
        return {"skipped": 1, "reason": "error", "error": str(e)[:300]}
