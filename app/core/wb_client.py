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
        self.limiter = RateLimiter(3.0)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(self, method: str, path: str, *, params: Optional[dict]=None, json_body: Optional[dict]=None) -> Any:
        url = BASE + path

        async def _do():
            await self.limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
            async with connector:
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.request(method, url, headers=self._headers(), params=params, json=json_body) as resp:
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
        return await retry(_do)

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
