"""
Клиент Ozon Seller API: отзывы и вопросы.

Авторизация: Client-Id + Api-Key.
Лимит: 1 req/s для всех запросов (RateLimiter).
"""
from __future__ import annotations

import json
import logging
import socket
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .ozon_buyer_chat import is_ozon_buyer_chat_row

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("ozon")

BASE = "https://api-seller.ozon.ru"


class OzonClient:
    def __init__(self, client_id: str, api_key: str, *, timeout_s: float = 20.0) -> None:
        self.client_id = (client_id or "").strip()
        self.api_key = (api_key or "").strip()
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        self._limiter = RateLimiter(1.0)

    def _headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(self, method: str, path: str, *, json_body: Optional[dict] = None) -> Any:
        url = BASE + path

        async def _do():
            await self._limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
            async with connector:
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.request(method, url, headers=self._headers(), json=json_body) as resp:
                        txt = await resp.text()
                        if resp.status >= 400:
                            raise HttpStatusError(resp.status, txt)
                        if resp.status == 204:
                            return None
                        if not txt:
                            return None
                        try:
                            return json.loads(txt)
                        except Exception as e:
                            log.warning("Ozon API invalid JSON: %s", e)
                            raise HttpStatusError(502, f"Invalid JSON: {(str(e)[:200])}")

        return await retry(_do)

    def _list_block(self, data: dict) -> dict:
        if not isinstance(data, dict):
            return {}
        res = data.get("result")
        return res if isinstance(res, dict) else data

    def _feedback_list(self, data: dict) -> list:
        """Из ответа review/list извлекает список отзывов."""
        block = self._list_block(data or {})
        for key in ("reviews", "items", "list"):
            rows = block.get(key)
            if isinstance(rows, list):
                return rows
        return []

    def _question_list(self, data: dict) -> list:
        """Из ответа question/list извлекает список вопросов."""
        block = self._list_block(data or {})
        for key in ("questions", "items", "list"):
            rows = block.get(key)
            if isinstance(rows, list):
                return rows
        return []

    @staticmethod
    def pagination_state(data: dict) -> tuple[str, bool]:
        """last_id и has_next из ответа Ozon (с result или без)."""
        block = OzonClient._list_block_static(data or {})
        last_id = str(block.get("last_id") or (data or {}).get("last_id") or "").strip()
        has_next = block.get("has_next")
        if has_next is None:
            has_next = (data or {}).get("has_next")
        return last_id, bool(has_next)

    @staticmethod
    def _list_block_static(data: dict) -> dict:
        if not isinstance(data, dict):
            return {}
        res = data.get("result")
        return res if isinstance(res, dict) else data

    async def has_new(self) -> dict:
        """Есть ли отзывы/вопросы, требующие ответа. Возвращает {feedbacks: bool, questions: bool}."""
        fb_list: list = []
        q_list: list = []
        try:
            fb = await self.list_feedbacks(limit=20, status="UNPROCESSED")
            fb_list = self._feedback_list(fb or {})
        except HttpStatusError:
            pass
        try:
            for st in ("NEW", "UNPROCESSED"):
                q = await self.list_questions(status=st)
                chunk = self._question_list(q or {})
                if chunk:
                    q_list = chunk
                    break
        except HttpStatusError:
            pass
        return {
            "feedbacks": len(fb_list) > 0,
            "questions": len(q_list) > 0,
        }

    @staticmethod
    def _review_status_v2(status: str) -> Optional[str]:
        """v2: NEW | VIEWED | PROCESSED. None — без фильтра (все)."""
        st = (status or "").strip().upper()
        if st in ("NEW", "VIEWED", "PROCESSED"):
            return st
        if st == "UNPROCESSED":
            return "NEW"
        if st == "ALL":
            return None
        return "NEW"

    @staticmethod
    def _review_status_v1(status: str) -> str:
        """v1: ALL | UNPROCESSED | PROCESSED."""
        st = (status or "").strip().upper()
        if st in ("ALL", "UNPROCESSED", "PROCESSED"):
            return st
        if st in ("NEW", "VIEWED"):
            return "UNPROCESSED"
        return "UNPROCESSED"

    async def list_feedbacks(
        self,
        *,
        limit: int = 100,
        last_id: str = "",
        sort_dir: str = "DESC",
        status: str = "UNPROCESSED",
    ) -> dict:
        """Список отзывов: /v2/review/list (filters.status), при ошибке — /v1/review/list."""
        lim = min(max(limit, 20), 100)
        lid = last_id or ""
        v2_filter = self._review_status_v2(status)
        v2_body: Dict[str, Any] = {
            "last_id": lid,
            "limit": lim,
            "sort_dir": sort_dir,
        }
        if v2_filter:
            v2_body["filters"] = {"status": v2_filter}
        try:
            data = await self._request("POST", "/v2/review/list", json_body=v2_body)
            if isinstance(data, dict):
                return data
        except HttpStatusError as e:
            if e.status not in (400, 404, 405):
                raise
            log.info("Ozon review/list v2 HTTP %s, fallback to v1", e.status)
        v1_body: Dict[str, Any] = {
            "last_id": lid,
            "limit": lim,
            "sort_dir": sort_dir,
            "status": self._review_status_v1(status),
        }
        return await self._request("POST", "/v1/review/list", json_body=v1_body)

    async def list_questions(
        self,
        *,
        last_id: str = "",
        status: str = "NEW",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> dict:
        """POST /v1/question/list. filter.status: ALL | NEW | VIEWED | PROCESSED | UNPROCESSED."""
        body: Dict[str, Any] = {
            "filter": {"status": status},
            "last_id": last_id or "",
        }
        if date_from:
            body["filter"]["date_from"] = date_from
        if date_to:
            body["filter"]["date_to"] = date_to
        return await self._request("POST", "/v1/question/list", json_body=body)

    async def answer_feedback(self, feedback_id: str, text: str) -> None:
        """POST /v1/review/comment/create с mark_review_as_processed=true."""
        payload = {
            "mark_review_as_processed": True,
            "parent_comment_id": "",
            "review_id": str(feedback_id),
            "text": (text or "").strip()[:4096],
        }
        await self._request("POST", "/v1/review/comment/create", json_body=payload)

    async def answer_question(self, question_id: str, text: str, sku: int) -> None:
        """POST /v1/question/answer/create. sku обязателен для Ozon. text: 2..3000 символов."""
        t = (text or "").strip()
        if len(t) > 3000:
            t = t[:3000]
        payload = {
            "question_id": str(question_id),
            "sku": int(sku),
            "text": t,
        }
        await self._request("POST", "/v1/question/answer/create", json_body=payload)

    async def get_product_names_by_sku(self, skus: List[int]) -> Dict[int, str]:
        """
        POST /v3/product/info/list — названия по SKU (до 1000 за запрос).
        Документация: 200 OK возвращает {"items": [...]} на верхнем уровне.
        """
        if not skus:
            return {}
        sku_list = list(dict.fromkeys(skus))[:1000]
        body: Dict[str, Any] = {"sku": [str(s) for s in sku_list]}
        data = await self._request("POST", "/v3/product/info/list", json_body=body)
        items = data.get("items")
        if items is None:
            items = (data.get("result") or {}).get("items")
        items = items or []
        out: Dict[int, str] = {}
        for it in items:
            sku_val = it.get("sku")
            try:
                sku_id = int(sku_val) if sku_val is not None else None
            except (TypeError, ValueError):
                continue
            name = (it.get("name") or "").strip()
            if sku_id is not None and name:
                out[sku_id] = name
        return out

    async def list_chats(
        self,
        *,
        limit: int = 30,
        unread_only: bool = False,
        chat_status: str = "Opened",
        chat_type: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> dict:
        """POST /v3/chat/list — все чаты или с фильтром chat_type."""
        filt: Dict[str, Any] = {"chat_status": chat_status}
        if chat_type:
            filt["chat_type"] = chat_type
        if unread_only:
            filt["unread_only"] = True
        body: Dict[str, Any] = {
            "filter": filt,
            "limit": min(max(int(limit), 1), 100),
        }
        if cursor:
            body["cursor"] = str(cursor)
        data = await self._request("POST", "/v3/chat/list", json_body=body)
        return data if isinstance(data, dict) else {}

    async def list_buyer_chats(
        self,
        *,
        limit: int = 30,
        unread_only: bool = False,
        chat_status: str = "Opened",
        cursor: Optional[str] = None,
    ) -> dict:
        """POST /v3/chat/list — только Buyer_Seller."""
        return await self.list_chats(
            limit=limit,
            unread_only=unread_only,
            chat_status=chat_status,
            chat_type="Buyer_Seller",
            cursor=cursor,
        )

    async def list_all_chats(
        self,
        *,
        unread_only: bool = False,
        chat_status: str = "Opened",
        max_pages: int = 20,
    ) -> List[dict]:
        """Все чаты магазина (без фильтра по типу)."""
        rows: List[dict] = []
        cursor: Optional[str] = None
        for _ in range(max(1, max_pages)):
            block = await self.list_chats(
                limit=100,
                unread_only=unread_only,
                chat_status=chat_status,
                cursor=cursor,
            )
            chunk = block.get("chats") or []
            if isinstance(chunk, list):
                rows.extend(chunk)
            has_next = block.get("has_next")
            if has_next in (False, "false", 0, "0", None):
                break
            nxt = block.get("cursor")
            if not nxt:
                break
            cursor = str(nxt)
        return [r for r in rows if isinstance(r, dict)]

    async def list_all_buyer_chats(
        self,
        *,
        unread_only: bool = False,
        chat_status: str = "Opened",
        max_pages: int = 20,
    ) -> List[dict]:
        rows: List[dict] = []
        cursor: Optional[str] = None
        for _ in range(max(1, max_pages)):
            block = await self.list_chats(
                limit=100,
                unread_only=unread_only,
                chat_status=chat_status,
                chat_type="Buyer_Seller",
                cursor=cursor,
            )
            chunk = block.get("chats") or []
            if isinstance(chunk, list):
                rows.extend(chunk)
            has_next = block.get("has_next")
            if has_next in (False, "false", 0, "0", None):
                break
            nxt = block.get("cursor")
            if not nxt:
                break
            cursor = str(nxt)
        return [r for r in rows if isinstance(r, dict) and is_ozon_buyer_chat_row(r)]

    async def chat_history(
        self,
        chat_id: str,
        *,
        limit: int = 50,
        direction: str = "Backward",
    ) -> dict:
        """POST /v3/chat/history."""
        body: Dict[str, Any] = {
            "chat_id": str(chat_id),
            "limit": min(max(int(limit), 1), 1000),
            "direction": direction,
        }
        data = await self._request("POST", "/v3/chat/history", json_body=body)
        return data if isinstance(data, dict) else {}

    async def send_chat_message(self, chat_id: str, text: str) -> dict:
        """POST /v1/chat/send/message."""
        t = (text or "").strip()
        if not t:
            raise ValueError("text обязателен")
        body = {"chat_id": str(chat_id), "text": t[:4000]}
        data = await self._request("POST", "/v1/chat/send/message", json_body=body)
        if isinstance(data, dict):
            res = str(data.get("result") or "").strip().lower()
            if res and res not in ("success", "ok"):
                raise HttpStatusError(502, json.dumps(data))
        return data if isinstance(data, dict) else {}

    async def list_actions(self) -> List[dict]:
        """GET /v1/actions — список акций Ozon."""
        data = await self._request("GET", "/v1/actions")
        if not isinstance(data, dict):
            return []
        res = data.get("result")
        return res if isinstance(res, list) else []

    async def list_action_products(
        self,
        action_id: int,
        *,
        limit: int = 100,
        last_id: Any = None,
    ) -> dict:
        """POST /v1/actions/products — товары, участвующие в акции."""
        body: Dict[str, Any] = {
            "action_id": int(action_id),
            "limit": min(max(int(limit), 1), 100),
        }
        if last_id is not None and str(last_id).strip():
            body["last_id"] = last_id
        data = await self._request("POST", "/v1/actions/products", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

    async def list_action_candidates(
        self,
        action_id: int,
        *,
        limit: int = 100,
        last_id: Any = None,
    ) -> dict:
        """POST /v1/actions/candidates — товары, доступные для акции."""
        body: Dict[str, Any] = {
            "action_id": int(action_id),
            "limit": min(max(int(limit), 1), 100),
        }
        if last_id is not None and str(last_id).strip():
            body["last_id"] = last_id
        data = await self._request("POST", "/v1/actions/candidates", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

    async def deactivate_action_products(self, action_id: int, product_ids: List[int]) -> dict:
        """POST /v1/actions/products/deactivate — убрать товары из акции."""
        ids = [int(x) for x in (product_ids or []) if x is not None]
        if not ids:
            return {"product_ids": [], "rejected": []}
        body = {"action_id": int(action_id), "product_ids": ids}
        data = await self._request("POST", "/v1/actions/products/deactivate", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

    async def activate_action_products(self, action_id: int, products: List[dict]) -> dict:
        """POST /v1/actions/products/activate — добавить товары в акцию."""
        norm: List[dict] = []
        for p in products[:1000]:
            if not isinstance(p, dict):
                continue
            pid = p.get("product_id")
            if pid is None:
                pid = p.get("id")
            try:
                pid_i = int(pid)
                price = float(p.get("action_price"))
            except (TypeError, ValueError):
                continue
            if pid_i <= 0 or price <= 0:
                continue
            row: Dict[str, Any] = {"product_id": pid_i, "action_price": price}
            if p.get("stock") is not None:
                try:
                    row["stock"] = int(p["stock"])
                except (TypeError, ValueError):
                    pass
            norm.append(row)
        if not norm:
            return {"product_ids": [], "rejected": []}
        body = {"action_id": int(action_id), "products": norm}
        data = await self._request("POST", "/v1/actions/products/activate", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

    async def list_auto_add_products(
        self,
        action_id: int,
        *,
        limit: int = 100,
        last_id: Any = None,
    ) -> dict:
        """POST /v1/actions/auto-add/products/list — товары в очереди автодобавления."""
        body: Dict[str, Any] = {
            "action_id": int(action_id),
            "limit": min(max(int(limit), 1), 100),
        }
        if last_id is not None and str(last_id).strip():
            body["last_id"] = last_id
        data = await self._request("POST", "/v1/actions/auto-add/products/list", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

    async def delete_auto_add_products(self, action_id: int, product_ids: List[int]) -> dict:
        """POST /v1/actions/auto-add/products/delete — убрать из автодобавления."""
        ids = [int(x) for x in (product_ids or []) if x is not None]
        if not ids:
            return {"product_ids": [], "rejected": []}
        body = {"action_id": int(action_id), "product_ids": ids[:1000]}
        data = await self._request("POST", "/v1/actions/auto-add/products/delete", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

    async def rating_summary(self) -> dict:
        """POST /v1/rating/summary — текущие рейтинги продавца."""
        data = await self._request("POST", "/v1/rating/summary", json_body={})
        return data if isinstance(data, dict) else {}

    async def rating_index_fbs_info(self) -> dict:
        """POST /v1/rating/index/fbs/info — индекс ошибок FBS/rFBS."""
        data = await self._request("POST", "/v1/rating/index/fbs/info", json_body={})
        return data if isinstance(data, dict) else {}

    # ---------- Каталог / связки карточек ----------

    OZON_MODEL_ATTR_ID = 9048

    async def list_products(
        self,
        *,
        limit: int = 100,
        last_id: str = "",
        offer_ids: Optional[List[str]] = None,
        product_ids: Optional[List[int]] = None,
        visibility: str = "ALL",
    ) -> dict:
        """POST /v3/product/list — одна страница списка товаров."""
        body: Dict[str, Any] = {
            "filter": {"visibility": visibility},
            "limit": min(max(int(limit), 1), 1000),
            "last_id": str(last_id or ""),
        }
        filt = body["filter"]
        if offer_ids:
            filt["offer_id"] = [str(x).strip() for x in offer_ids if str(x).strip()]
        if product_ids:
            filt["product_id"] = [int(x) for x in product_ids if x is not None]
        data = await self._request("POST", "/v3/product/list", json_body=body)
        return data if isinstance(data, dict) else {}

    async def list_products_all(
        self,
        *,
        max_pages: int = 30,
        offer_ids: Optional[List[str]] = None,
        visibility: str = "ALL",
        meta_out: Optional[dict] = None,
    ) -> List[dict]:
        """Пагинация /v3/product/list."""
        if offer_ids:
            data = await self.list_products(limit=1000, offer_ids=offer_ids, visibility=visibility)
            block = self._list_block(data)
            items = block.get("items") or []
            out = [x for x in items if isinstance(x, dict)]
            if meta_out is not None:
                meta_out["pages_fetched"] = 1
                meta_out["max_pages"] = max_pages
                meta_out["page_size"] = 1000
                meta_out["truncated"] = False
            return out

        rows: List[dict] = []
        last_id = ""
        pages = 0
        truncated = False
        page_size = 1000
        for _ in range(max(1, max_pages)):
            pages += 1
            data = await self.list_products(limit=page_size, last_id=last_id, visibility=visibility)
            block = self._list_block(data)
            batch = block.get("items") or []
            if not isinstance(batch, list) or not batch:
                break
            for it in batch:
                if isinstance(it, dict):
                    rows.append(it)
            last_id = str(block.get("last_id") or "").strip()
            has_next = bool(block.get("has_next"))
            if not last_id or not has_next:
                break
            if pages >= max_pages:
                truncated = True
                break
        if meta_out is not None:
            meta_out["pages_fetched"] = pages
            meta_out["max_pages"] = max_pages
            meta_out["page_size"] = page_size
            meta_out["truncated"] = truncated
        return rows

    async def product_info_list(
        self,
        *,
        offer_ids: Optional[List[str]] = None,
        product_ids: Optional[List[int]] = None,
        skus: Optional[List[int]] = None,
    ) -> List[dict]:
        """POST /v3/product/info/list — детали товаров (название, фото, sku)."""
        body: Dict[str, Any] = {}
        if offer_ids:
            body["offer_id"] = [str(x).strip() for x in offer_ids if str(x).strip()][:1000]
        if product_ids:
            body["product_id"] = [int(x) for x in product_ids if x is not None][:1000]
        if skus:
            body["sku"] = [str(int(x)) for x in skus if x is not None][:1000]
        if not body:
            return []
        data = await self._request("POST", "/v3/product/info/list", json_body=body)
        items = data.get("items")
        if items is None:
            items = (data.get("result") or {}).get("items")
        return [x for x in (items or []) if isinstance(x, dict)]

    async def product_info_attributes(
        self,
        *,
        offer_ids: Optional[List[str]] = None,
        product_ids: Optional[List[int]] = None,
        limit: int = 100,
        last_id: str = "",
    ) -> dict:
        """POST /v4/product/info/attributes — атрибуты (в т.ч. «Название модели»)."""
        filt: Dict[str, Any] = {"visibility": "ALL"}
        if offer_ids:
            filt["offer_id"] = [str(x).strip() for x in offer_ids if str(x).strip()]
        if product_ids:
            filt["product_id"] = [int(x) for x in product_ids if x is not None]
        body: Dict[str, Any] = {
            "filter": filt,
            "limit": min(max(int(limit), 1), 1000),
            "last_id": str(last_id or ""),
            "sort_dir": "ASC",
        }
        data = await self._request("POST", "/v4/product/info/attributes", json_body=body)
        return data if isinstance(data, dict) else {}

    async def product_related_sku_get(self, skus: List[int]) -> List[dict]:
        """POST /v1/product/related-sku/get — связанные SKU."""
        if not skus:
            return []
        sku_list = list(dict.fromkeys(int(s) for s in skus if s is not None))[:1000]
        body = {"sku": [str(s) for s in sku_list]}
        data = await self._request("POST", "/v1/product/related-sku/get", json_body=body)
        if isinstance(data, dict):
            items = data.get("items")
            if items is None:
                items = (data.get("result") or {}).get("items")
            return [x for x in (items or []) if isinstance(x, dict)]
        return []

    async def description_category_attributes(
        self,
        *,
        description_category_id: int,
        type_id: int,
        language: str = "DEFAULT",
    ) -> List[dict]:
        """POST /v1/description-category/attribute — схема характеристик категории (is_aspect и т.д.)."""
        body = {
            "description_category_id": int(description_category_id),
            "type_id": int(type_id),
            "language": str(language or "DEFAULT"),
        }
        data = await self._request("POST", "/v1/description-category/attribute", json_body=body)
        if not isinstance(data, dict):
            return []
        res = data.get("result")
        if isinstance(res, list):
            return [x for x in res if isinstance(x, dict)]
        return []

    async def update_product_attributes(self, items: List[dict]) -> dict:
        """POST /v1/product/attributes/update — обновить атрибуты (склейка через «Название модели»)."""
        if not items:
            return {}
        data = await self._request(
            "POST",
            "/v1/product/attributes/update",
            json_body={"items": items[:100]},
        )
        return data if isinstance(data, dict) else {}

    async def product_certificate_types(self) -> List[dict]:
        """GET /v1/product/certificate/types — справочник типов документов."""
        data = await self._request("GET", "/v1/product/certificate/types")
        if isinstance(data, dict):
            res = data.get("result") or data.get("types") or data.get("items")
            if isinstance(res, list):
                return [x for x in res if isinstance(x, dict)]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []

    async def product_certificate_list(
        self,
        *,
        page: int = 1,
        page_size: int = 100,
    ) -> List[dict]:
        """POST /v1/product/certificate/list — документы в кабинете Ozon."""
        body = {
            "page": max(int(page), 1),
            "page_size": min(max(int(page_size), 1), 1000),
        }
        data = await self._request("POST", "/v1/product/certificate/list", json_body=body)
        if not isinstance(data, dict):
            return []
        res = data.get("result") or data
        if isinstance(res, dict):
            certs = res.get("certificates") or res.get("items") or []
        else:
            certs = res if isinstance(res, list) else []
        return [x for x in (certs or []) if isinstance(x, dict)]

    async def product_certificate_create(self, payload: dict) -> dict:
        """POST /v1/product/certificate/create."""
        data = await self._request("POST", "/v1/product/certificate/create", json_body=payload)
        return data if isinstance(data, dict) else {}

    async def product_certificate_bind(
        self,
        *,
        certificate_id: int,
        product_ids: List[int],
    ) -> dict:
        """POST /v1/product/certificate/bind."""
        if not certificate_id or not product_ids:
            return {}
        body = {
            "certificate_id": int(certificate_id),
            "product_id": [int(x) for x in product_ids if x],
        }
        data = await self._request("POST", "/v1/product/certificate/bind", json_body=body)
        return data if isinstance(data, dict) else {}

