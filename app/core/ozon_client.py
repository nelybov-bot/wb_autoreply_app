"""
Клиент Ozon Seller API: отзывы и вопросы.

Авторизация: Client-Id + Api-Key.
Лимит: 1 req/s для всех запросов (RateLimiter).
"""
from __future__ import annotations

import json
import logging
import socket
from typing import Any, Dict, List, Optional

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

    def _feedback_list(self, data: dict) -> list:
        """Из ответа review/list извлекает список отзывов (reviews или items)."""
        if not data:
            return []
        res = data.get("result") or data
        return res.get("reviews") or res.get("items") or []

    def _question_list(self, data: dict) -> list:
        """Из ответа question/list извлекает список вопросов (questions или items)."""
        if not data:
            return []
        res = data.get("result") or data
        return res.get("questions") or res.get("items") or []

    async def has_new(self) -> dict:
        """Есть ли отзывы/вопросы, требующие ответа. Возвращает {feedbacks: bool, questions: bool}."""
        fb = await self.list_feedbacks(limit=20, status="UNPROCESSED")
        q = await self.list_questions(status="NEW")
        fb_list = self._feedback_list(fb or {})
        q_list = self._question_list(q or {})
        return {
            "feedbacks": len(fb_list) > 0,
            "questions": len(q_list) > 0,
        }

    async def list_feedbacks(
        self,
        *,
        limit: int = 100,
        last_id: str = "",
        sort_dir: str = "DESC",
        status: str = "UNPROCESSED",
    ) -> dict:
        """POST /v1/review/list. status: ALL | UNPROCESSED | PROCESSED. limit: 20..100 по API."""
        body: Dict[str, Any] = {
            "last_id": last_id or "",
            "limit": min(max(limit, 20), 100),
            "sort_dir": sort_dir,
            "status": status,
        }
        return await self._request("POST", "/v1/review/list", json_body=body)

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
        body = {"action_id": int(action_id), "products": products[:1000]}
        data = await self._request("POST", "/v1/actions/products/activate", json_body=body)
        if isinstance(data, dict):
            res = data.get("result")
            return res if isinstance(res, dict) else {}
        return {}

