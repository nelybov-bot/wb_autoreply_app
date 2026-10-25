"""
Клиент API Яндекс Маркета: отзывы, вопросы, каталог и связки (группы вариантов).

- Авторизация: заголовок Api-Key.
- business_id обязателен (int); при отсутствии — понятная ошибка.
- Лимиты: READ ~2 req/s, WRITE ~0.25 req/s (net.RateLimiter).
- Связки: параметр 200 «Название группы вариантов» + distinctive-хар-ки категории.
"""
from __future__ import annotations

import json
import logging
import socket
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("yam")

BASE = "https://api.partner.market.yandex.ru"

# Категорийная характеристика «Название группы вариантов» — ключ склейки.
YAM_GROUP_PARAM_ID = 200


class YamClient:
    def __init__(self, api_key: str, business_id: int, *, timeout_s: float = 20.0) -> None:
        self.api_key = api_key.strip()
        if business_id is None or not isinstance(business_id, int):
            raise ValueError(
                "business_id обязателен для Яндекс Маркета и должен быть целым числом (ID кабинета)."
            )
        self.business_id = business_id
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        self._limiter_read = RateLimiter(2.0)
        self._limiter_write = RateLimiter(0.25)

    def _headers(self) -> Dict[str, str]:
        return {
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
        use_write_limiter: bool = False,
    ) -> Any:
        url = BASE + path

        async def _do():
            limiter = self._limiter_write if use_write_limiter else self._limiter_read
            await limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
            async with connector:
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.request(
                        method, url, headers=self._headers(), json=json_body
                    ) as resp:
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
                            log.warning("YAM API invalid JSON: %s", e)
                            raise HttpStatusError(502, f"Invalid JSON: {(str(e)[:200])}")

        return await retry(_do)

    def _path_v2(self, suffix: str) -> str:
        return f"/v2/businesses/{self.business_id}{suffix}"

    def _path_v1(self, suffix: str) -> str:
        return f"/v1/businesses/{self.business_id}{suffix}"

    async def has_new(self) -> dict:
        """Возвращает {feedbacks: bool, questions: bool} — есть ли непрочитанные."""
        fb = await self.list_feedbacks(limit=1, reaction_status="NEED_REACTION")
        q = await self.list_questions(limit=1, need_answer=True)
        result = (fb or {}).get("result") or {}
        feedbacks_list = result.get("feedbacks") or []
        q_result = (q or {}).get("result") or {}
        questions_list = q_result.get("questions") or []
        return {
            "feedbacks": len(feedbacks_list) > 0,
            "questions": len(questions_list) > 0,
        }

    async def list_feedbacks(
        self,
        *,
        limit: int = 50,
        page_token: Optional[str] = None,
        reaction_status: Optional[str] = None,
    ) -> dict:
        """POST v2/businesses/{businessId}/goods-feedback."""
        body: Dict[str, Any] = {"limit": min(max(limit, 1), 50)}
        if page_token:
            body["pageToken"] = page_token
        if reaction_status:
            body["reactionStatus"] = reaction_status
        return await self._request(
            "POST", self._path_v2("/goods-feedback"), json_body=body
        )

    async def list_questions(
        self,
        *,
        limit: int = 50,
        page_token: Optional[str] = None,
        need_answer: bool = True,
    ) -> dict:
        """POST v1/businesses/{businessId}/goods-questions. Фильтр needAnswer: true."""
        body: Dict[str, Any] = {"limit": min(max(limit, 1), 50), "needAnswer": need_answer}
        if page_token:
            body["pageToken"] = page_token
        return await self._request(
            "POST", self._path_v1("/goods-questions"), json_body=body
        )

    async def answer_feedback(self, feedback_id: str, text: str) -> None:
        """POST v2/.../goods-feedback/comments/update — новый комментарий к отзыву."""
        payload = {
            "feedbackId": int(feedback_id),
            "comment": {"text": (text or "").strip()[:4096]},
        }
        await self._request(
            "POST",
            self._path_v2("/goods-feedback/comments/update"),
            json_body=payload,
            use_write_limiter=True,
        )

    async def answer_question(self, question_id: str, text: str) -> None:
        """POST v1/.../goods-questions/update — CREATE ответ на вопрос."""
        payload = {
            "operationType": "CREATE",
            "parentEntityId": {"id": int(question_id), "type": "QUESTION"},
            "text": (text or "").strip()[:5000],
        }
        await self._request(
            "POST",
            self._path_v1("/goods-questions/update"),
            json_body=payload,
            use_write_limiter=True,
        )

    async def get_offer_names(self, offer_ids: List[str]) -> Dict[str, str]:
        """
        POST v2/businesses/{businessId}/offer-mappings — названия товаров по offerId.
        Возвращает {offerId: название или marketSkuName/marketModelName, при отсутствии — offerId}.
        Макс. 100 offerIds за запрос; при большем количестве делаются батчи.
        """
        if not offer_ids:
            return {}
        out: Dict[str, str] = {}
        chunk_size = 100
        for i in range(0, len(offer_ids), chunk_size):
            chunk = offer_ids[i : i + chunk_size]
            body = {"offerIds": chunk}
            data = await self._request(
                "POST",
                self._path_v2("/offer-mappings"),
                json_body=body,
            )
            result = (data or {}).get("result") or {}
            for entry in result.get("offerMappings") or []:
                offer = entry.get("offer") or {}
                mapping = entry.get("mapping") or {}
                oid = str(offer.get("offerId") or "").strip()
                if not oid:
                    continue
                name = (
                    (mapping.get("marketSkuName") or "").strip()
                    or (mapping.get("marketModelName") or "").strip()
                )
                out[oid] = name or oid
        return out

    async def list_offer_mappings(
        self,
        *,
        offer_ids: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: int = 200,
    ) -> dict:
        """
        POST v2/businesses/{businessId}/offer-mappings.
        Каталог / фильтр по offerIds. В offer есть groupId и params (в т.ч. название группы).
        """
        qs: Dict[str, str] = {"limit": str(min(max(int(limit), 1), 200))}
        if page_token:
            qs["page_token"] = page_token
        path = self._path_v2("/offer-mappings") + "?" + urlencode(qs)
        body: Optional[dict] = None
        if offer_ids is not None:
            body = {"offerIds": [str(x).strip() for x in offer_ids if str(x).strip()][:200]}
        return await self._request("POST", path, json_body=body)

    async def list_offer_mappings_all(
        self,
        *,
        offer_ids: Optional[List[str]] = None,
        max_pages: int = 200,
        page_size: int = 200,
        meta_out: Optional[dict] = None,
    ) -> List[dict]:
        """
        Полный обход каталога (или батчи по offerIds).
        Возвращает список сырых offerMappings-элементов.
        """
        codes = [str(x).strip() for x in (offer_ids or []) if str(x).strip()]
        out: List[dict] = []
        pages = 0
        truncated = False

        if codes:
            for i in range(0, len(codes), 200):
                chunk = codes[i : i + 200]
                data = await self.list_offer_mappings(offer_ids=chunk, limit=200)
                pages += 1
                batch = ((data or {}).get("result") or {}).get("offerMappings") or []
                out.extend(batch)
                if pages >= max_pages:
                    truncated = i + 200 < len(codes)
                    break
        else:
            token: Optional[str] = None
            while pages < max_pages:
                data = await self.list_offer_mappings(page_token=token, limit=page_size)
                pages += 1
                result = (data or {}).get("result") or {}
                batch = result.get("offerMappings") or []
                out.extend(batch)
                token = (result.get("paging") or {}).get("nextPageToken")
                if not token or not batch:
                    break
            else:
                truncated = True

        if meta_out is not None:
            meta_out["pages"] = pages
            meta_out["count"] = len(out)
            meta_out["truncated"] = truncated
        return out

    async def get_offer_cards(self, offer_ids: List[str]) -> List[dict]:
        """
        POST v2/businesses/{businessId}/offer-cards — статусы карточек, parameterValues, groupId.
        Батчи по 200.
        """
        codes = [str(x).strip() for x in offer_ids if str(x).strip()]
        if not codes:
            return []
        out: List[dict] = []
        for i in range(0, len(codes), 200):
            chunk = codes[i : i + 200]
            data = await self._request(
                "POST",
                self._path_v2("/offer-cards"),
                json_body={"offerIds": chunk},
            )
            out.extend(((data or {}).get("result") or {}).get("offerCards") or [])
        return out

    async def get_category_parameters(self, category_id: int) -> dict:
        """
        POST v2/category/{categoryId}/parameters?businessId=…
        Схема категории: id=200, distinctive-поля.
        """
        cid = int(category_id)
        if cid <= 0:
            raise ValueError("category_id должен быть > 0")
        path = f"/v2/category/{cid}/parameters?{urlencode({'businessId': self.business_id})}"
        return await self._request("POST", path, json_body={})

    async def update_offer_cards(
        self,
        offers_content: List[dict],
    ) -> dict:
        """
        POST v2/businesses/{businessId}/offer-cards/update — категорийные хар-ки.
        Каждый элемент: offerId, categoryId?, parameterValues[].
        Макс. 100 за запрос; при большем — батчи.
        """
        items = [x for x in offers_content if isinstance(x, dict) and x.get("offerId")]
        if not items:
            raise ValueError("offers_content пуст")
        last: dict = {}
        for i in range(0, len(items), 100):
            chunk = items[i : i + 100]
            last = await self._request(
                "POST",
                self._path_v2("/offer-cards/update"),
                json_body={"offersContent": chunk},
                use_write_limiter=True,
            )
        return last

    @staticmethod
    def extract_group_name(offer: dict) -> str:
        """Имя группы из params / groupId оффера offer-mappings."""
        if not isinstance(offer, dict):
            return ""
        for p in offer.get("params") or []:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").lower()
            if "групп" in name or "модел" in name and "объединен" in name:
                val = str(p.get("value") or "").strip()
                if val:
                    return val
        return str(offer.get("groupId") or "").strip()
