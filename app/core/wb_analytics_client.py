"""Клиент Wildberries seller-analytics-api: заблокированные карточки и т.п."""
from __future__ import annotations

import json
import logging
import socket
from typing import Any, Dict, List, Optional

import aiohttp

from .net import USER_AGENT, HttpStatusError, RateLimiter, retry

log = logging.getLogger("wb_analytics")

BASE = "https://seller-analytics-api.wildberries.ru"


class WbAnalyticsClient:
    """Нужен API-ключ с категорией «Аналитика»."""

    def __init__(self, api_key: str, *, timeout_s: float = 30.0) -> None:
        self.api_key = (api_key or "").strip()
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        # Док WB: 1 запрос / 10 сек на banned-products (персональный токен).
        self.limiter = RateLimiter(1 / 10.5)

    def _api_key_clean(self) -> str:
        key = self.api_key
        if key.lower().startswith("bearer "):
            return key[7:].strip()
        return key

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": self._api_key_clean(),
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
                            log.warning("WB analytics invalid JSON: %s", e)
                            raise HttpStatusError(502, f"Invalid JSON: {str(e)[:200]}")

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=3)

    async def list_banned_blocked(
        self,
        *,
        sort: str = "nmId",
        order: str = "asc",
    ) -> List[dict]:
        """
        GET /api/v1/analytics/banned-products/blocked
        Список заблокированных карточек с причинами.
        """
        data = await self._request(
            "GET",
            "/api/v1/analytics/banned-products/blocked",
            params={"sort": sort, "order": order},
        )
        if isinstance(data, dict):
            report = data.get("report")
            if isinstance(report, list):
                return [x for x in report if isinstance(x, dict)]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        return []
