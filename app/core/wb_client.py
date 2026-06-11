"""
Клиент Wildberries "User communication" (отзывы/вопросы).

Важно:
- Лимит 3 req/s: используем RateLimiter.
- Для вопросов PATCH делаем сначала без state. Если 400 "Empty state" -> повтор с state="wbRu".
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
from typing import Any, Dict, Optional

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("wb")

BASE = "https://feedbacks-api.wildberries.ru"

class WbClient:
    def __init__(self, api_key: str, *, timeout_s: float = 20.0) -> None:
        self.api_key = api_key.strip()
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        # Док: до 3 rps; «global limiter» на стороне WB чаще срабатывает при плотных сериях — держим ~1 rps.
        self.limiter = RateLimiter(1.0)

    def _api_key_clean(self) -> str:
        key = (self.api_key or "").strip()
        if key.lower().startswith("bearer "):
            return key[7:].strip()
        return key

    def _headers(self, *, authorization: Optional[str] = None) -> Dict[str, str]:
        key = self._api_key_clean()
        auth = authorization if authorization is not None else f"Bearer {key}"
        return {
            "Authorization": auth,
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        headers_override: Optional[dict] = None,
    ) -> Any:
        url = BASE + path
        headers = self._headers()
        if headers_override:
            headers = {**headers, **headers_override}

        async def _do():
            await self.limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
            async with connector:
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.request(method, url, headers=headers, params=params, json=json_body) as resp:
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
                            log.warning("WB API invalid JSON: %s", e)
                            raise HttpStatusError(502, f"Invalid JSON: {(str(e)[:200])}")
        # 429 от WB «global limiter» повторять бессмысно — только усугубляет лимит; отдаём наверх сразу.
        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=4)

    async def get_seller_rating(self) -> dict:
        """GET /api/common/v1/rating — рейтинг продавца по отзывам и их количество."""
        key = self._api_key_clean()
        last_err: Optional[HttpStatusError] = None
        # Док WB: HeaderApiKey — сначала ключ без Bearer, затем Bearer (для JWT).
        for auth in (key, f"Bearer {key}"):
            try:
                data = await self._request(
                    "GET",
                    "/api/common/v1/rating",
                    headers_override={"Authorization": auth},
                )
            except HttpStatusError as e:
                last_err = e
                if e.status in (401, 403):
                    continue
                raise
            else:
                if not isinstance(data, dict):
                    return {}
                if "valuation" in data or "feedbackCount" in data or "feedback_count" in data:
                    return data
                inner = data.get("data")
                return inner if isinstance(inner, dict) else data
        if last_err:
            raise last_err
        return {}

    async def estimate_seller_rating_from_feedbacks(self, *, take: int = 100) -> dict:
        """
        Средняя productValuation по неотвеченным отзывам в очереди (isAnswered=false).
        НЕ использовать как рейтинг магазина — для этого только GET /api/common/v1/rating.
        """
        f = await self.list_feedbacks(take=take, skip=0)
        fdata = (f or {}).get("data") if isinstance(f, dict) else None
        if not isinstance(fdata, dict):
            fdata = f if isinstance(f, dict) else {}
        items = fdata.get("feedbacks") or []
        vals: list[float] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            raw = it.get("productValuation")
            if raw is None:
                continue
            try:
                vals.append(float(raw))
            except (TypeError, ValueError):
                continue
        rating = (sum(vals) / len(vals)) if vals else None
        return {"rating": rating, "sample_size": len(vals), "feedbacks_total": len(items)}

    async def has_new(self) -> dict:
        return await self._request("GET", "/api/v1/new-feedbacks-questions")

    async def list_questions(self, *, take: int = 100, skip: int = 0) -> dict:
        params = {
            "isAnswered": "false",
            "take": str(take),
            "skip": str(skip),
            "order": "dateDesc",
        }
        return await self._request("GET", "/api/v1/questions", params=params)

    async def list_feedbacks(self, *, take: int = 100, skip: int = 0) -> dict:
        params = {
            "isAnswered": "false",
            "take": str(take),
            "skip": str(skip),
            "order": "dateDesc",
        }
        return await self._request("GET", "/api/v1/feedbacks", params=params)

    async def answer_feedback(self, feedback_id: str, text: str) -> None:
        payload = {"id": str(feedback_id), "text": text}
        await self._request("POST", "/api/v1/feedbacks/answer", json_body=payload)

    async def answer_question(self, question_id: str, text: str) -> None:
        # 1) пробуем без state
        payload = {"id": str(question_id), "answer": {"text": text}}
        try:
            await self._request("PATCH", "/api/v1/questions", json_body=payload)
            return
        except HttpStatusError as e:
            body = e.body or ""
            if e.status == 400 and "empty state" in (body or "").lower():
                payload2 = {"id": str(question_id), "state": "wbRu", "answer": {"text": text}}
                await self._request("PATCH", "/api/v1/questions", json_body=payload2)
                return
            raise
