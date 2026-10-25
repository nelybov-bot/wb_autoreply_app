"""Клиент Wildberries common-api: новости портала продавца."""
from __future__ import annotations

import json
import logging
import socket
from typing import Any, Dict, List, Optional

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("wb_common")

BASE = "https://common-api.wildberries.ru"


class WbCommonClient:
    def __init__(self, api_key: str, *, timeout_s: float = 25.0) -> None:
        self.api_key = (api_key or "").strip()
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        # Док WB: 1 запрос / мин на новости; держим ~50 с между вызовами.
        self.limiter = RateLimiter(1 / 50.0)

    def _api_key_clean(self) -> str:
        key = self.api_key
        if key.lower().startswith("bearer "):
            return key[7:].strip()
        return key

    def _headers(self) -> Dict[str, str]:
        key = self._api_key_clean()
        return {
            "Authorization": key,
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
    ) -> Any:
        url = BASE + path
        headers = self._headers()

        async def _do():
            await self.limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
            async with connector:
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.request(method, url, headers=headers, params=params) as resp:
                        txt = await resp.text()
                        if resp.status >= 400:
                            raise HttpStatusError(resp.status, txt)
                        if not txt:
                            return None
                        try:
                            return json.loads(txt)
                        except Exception as e:
                            log.warning("WB common API invalid JSON: %s", e)
                            raise HttpStatusError(502, f"Invalid JSON: {str(e)[:200]}")

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=3)

    async def list_news(
        self,
        *,
        from_date: Optional[str] = None,
        from_id: Optional[int] = None,
    ) -> List[dict]:
        """
        GET /api/communications/v2/news — до 100 новостей за запрос.
        Нужен параметр from (YYYY-MM-DD) или fromID.
        """
        params: dict[str, str] = {}
        if from_id is not None:
            params["fromID"] = str(int(from_id))
        elif from_date:
            params["from"] = str(from_date)[:10]
        else:
            raise ValueError("from_date or from_id required")
        data = await self._request("GET", "/api/communications/v2/news", params=params)
        if isinstance(data, dict):
            items = data.get("data")
            if isinstance(items, list):
                return [x for x in items if isinstance(x, dict)]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []
