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
from typing import Callable, Dict, List, Optional, Tuple

from ..db import Database, Store
from .net import HttpStatusError, UnauthorizedStoreError
from .wb_client import WbClient
from .yam_client import YamClient
from .ozon_client import OzonClient
from .openai_client import OpenAIClient
from .telegram_notify import send_review_to_chat

log = logging.getLogger("wf")

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
        if e.status in (400, 403, 404):
            log.warning("WB API %s: %s", e.status, e.body)
            return 0
        raise


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
            log.exception("load_new_all магазин %s (%s) store_id=%s: %s", store.name, store.marketplace, store.id, e)
        _progress(i + 1, n_stores)
    return total


# Сколько отзывов/вопросов генерировать параллельно (упирается в лимиты API).
GENERATE_CONCURRENCY = 5


_JSON_FORMAT_INSTRUCTION = (
    ' Ответь строго одним JSON-объектом, без текста до или после. '
    'Формат: {"reply": "текст ответа продавца", "packer_issue": true или false}. '
    'packer_issue = true только если отзыв касается упаковки, повреждения товара, пересорта, не того товара, недоложенного товара, проблем комплектации. '
    'Для вопросов всегда packer_issue: false.'
)


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
            if row.text.strip():
                user = f"{p}\n\nТовар: {row.product_title}\nОценка: {row.rating}\nТекст отзыва:\n{row.text}\n\nСформируй ответ.{_JSON_FORMAT_INSTRUCTION}"
            else:
                user = f"{p}\n\nТовар: {row.product_title}\nОценка: {row.rating}\nОтзыв без текста.\n\nСформируй ответ.{_JSON_FORMAT_INSTRUCTION}"
        else:
            p = db.get_prompt("question", "general")
            user = f"{p}\n\nТовар: {row.product_title}\nВопрос:\n{row.text}\n\nСформируй ответ.{_JSON_FORMAT_INSTRUCTION}"
        try:
            txt = await client.generate(system, user)
            txt = (txt or "").strip()
            if not txt:
                log.warning("Generate item_id=%s: пустой ответ от модели", item_id)
                return False
            try:
                obj = json.loads(txt)
            except json.JSONDecodeError as e:
                log.warning("Generate item_id=%s: ответ не JSON (%s), пропуск: %s", item_id, e, txt[:200])
                return False
            reply = (obj.get("reply") or "").strip()
            if not reply:
                log.warning("Generate item_id=%s: в JSON нет reply или он пустой", item_id)
                return False
            db.set_generated(item_id, reply)
            packer_issue = bool(obj.get("packer_issue"))
            if row.item_type == "review" and packer_issue:
                telegram_token = db.get_setting("telegram_bot_token")
                telegram_chat_id = db.get_setting("telegram_chat_id")
                if telegram_token and telegram_chat_id:
                    store_name = _store_name(db, row.store_id)
                    await send_review_to_chat(
                        telegram_token,
                        telegram_chat_id,
                        row.product_title or row.external_id,
                        row.text or "",
                        store_name=store_name,
                    )
            return True
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception as e:
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
