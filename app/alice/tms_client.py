"""Async-клиент TMS Warehouse API (aiohttp), строго по docs/WAREHOUSE_API.md."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional, Union

import aiohttp

from app.core.net import USER_AGENT

log = logging.getLogger("alice.tms")


@dataclass
class TmsError(Exception):
    code: str
    message: str
    http_status: int
    details: Optional[dict] = None
    body: Optional[dict] = None

    def __str__(self) -> str:
        return f"TMS {self.code} (HTTP {self.http_status}): {self.message}"


class TmsClient:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        operator: str = "Alice-Warehouse",
        timeout_s: float = 8.0,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.base_url = (base_url or "").rstrip("/")
        self._token = (token or "").strip()
        self.operator = (operator or "Alice-Warehouse")[:64]
        self.timeout = aiohttp.ClientTimeout(connect=4, total=timeout_s)
        self._session = session
        self._owns_session = session is None

    async def aclose(self) -> None:
        if self._owns_session and self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
            self._owns_session = True
        return self._session

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Operator": self.operator,
            "User-Agent": USER_AGENT,
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        allow_get_retry: bool = False,
    ) -> dict[str, Any]:
        if not self._token:
            raise TmsError("UNAUTHORIZED", "TMS_API_TOKEN не задан", 401)

        url = f"{self.base_url}{path}"
        session = await self._ensure_session()

        async def _once() -> tuple[int, dict[str, Any], Optional[str]]:
            async with session.request(
                method,
                url,
                headers=self._headers(),
                params=params,
                json=json_body,
            ) as resp:
                retry_after = resp.headers.get("Retry-After")
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    text = await resp.text()
                    data = {"ok": False, "error": {"code": "INTERNAL_ERROR", "message": text[:400]}}
                if not isinstance(data, dict):
                    data = {"ok": False, "error": {"code": "INTERNAL_ERROR", "message": "bad response"}}
                return int(resp.status), data, retry_after

        status, data, retry_after = await _once()

        # Один контролируемый повтор только для GET при RATE_LIMITED
        if (
            allow_get_retry
            and method.upper() == "GET"
            and status == 429
            and (data.get("error") or {}).get("code") == "RATE_LIMITED"
        ):
            wait_s = 1
            try:
                wait_s = int(retry_after or (data.get("error") or {}).get("details", {}).get("retry_after") or 1)
            except Exception:
                wait_s = 1
            wait_s = max(1, min(wait_s, 5))
            log.warning("alice_tms_rate_limited path=%s retry_after=%s", path, wait_s)
            await asyncio.sleep(wait_s)
            status, data, retry_after = await _once()

        if data.get("ok") is True:
            return data

        err = data.get("error") if isinstance(data.get("error"), dict) else {}
        code = str(err.get("code") or "INTERNAL_ERROR")
        message = str(err.get("message") or "Ошибка TMS")
        details = err.get("details") if isinstance(err.get("details"), dict) else None

        # Одиночный PATCH может вернуть ok:false + data.results
        raise TmsError(code=code, message=message, http_status=status, details=details, body=data)

    async def get_product_by_sku(self, sku: int) -> dict[str, Any]:
        data = await self._request(
            "GET",
            "/api/v1/warehouse/product",
            params={"sku": int(sku)},
            allow_get_retry=True,
        )
        product = data.get("data")
        if not isinstance(product, dict):
            raise TmsError("INTERNAL_ERROR", "Пустой ответ товара", 500, body=data)
        return product

    async def patch_stock_absolute(self, sku: int, stock: int) -> dict[str, Any]:
        """PATCH абсолютного остатка: {"sku": N, "stock": N}."""
        return await self._patch_product({"sku": int(sku), "stock": int(stock)})

    async def patch_stock_add(self, sku: int, add: int, operation_id: str) -> dict[str, Any]:
        """PATCH приращения: {"sku": N, "stock": {"add": N, "operation_id": "..."}}."""
        return await self._patch_product(
            {
                "sku": int(sku),
                "stock": {"add": int(add), "operation_id": str(operation_id)[:64]},
            }
        )

    async def _patch_product(self, body: dict[str, Any]) -> dict[str, Any]:
        # PATCH: без автоматических ретраев (идемпотентность через operation_id / dedup)
        data = await self._request(
            "PATCH",
            "/api/v1/warehouse/product",
            json_body=body,
            allow_get_retry=False,
        )
        # Успех: data.results[0]
        payload = data.get("data") if isinstance(data.get("data"), dict) else {}
        results = payload.get("results") if isinstance(payload, dict) else None
        if isinstance(results, list) and results:
            item = results[0]
            if isinstance(item, dict) and item.get("status") == "failed":
                err = item.get("error") if isinstance(item.get("error"), dict) else {}
                raise TmsError(
                    code=str(err.get("code") or "INTERNAL_ERROR"),
                    message=str(err.get("message") or "Ошибка записи"),
                    http_status=409,
                    details=err.get("details") if isinstance(err.get("details"), dict) else None,
                    body=data,
                )
            if isinstance(item, dict) and isinstance(item.get("data"), dict):
                return item["data"]
        # fallback: весь data
        if isinstance(payload, dict) and "sku" in payload:
            return payload
        return payload if isinstance(payload, dict) else {}


StockWriteMode = Union[int, dict]  # absolute int or {"add", "operation_id"}
