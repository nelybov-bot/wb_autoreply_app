"""Wildberries Content API: карточки товаров, объединение nmID."""
from __future__ import annotations

import json
import logging
import socket
import asyncio
from typing import Any, Dict, List, Optional

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT

log = logging.getLogger("wb.content")

BASE = "https://content-api.wildberries.ru"


class WbContentClient:
    def __init__(self, api_key: str, *, timeout_s: float = 45.0) -> None:
        self.api_key = (api_key or "").strip()
        if self.api_key.lower().startswith("bearer "):
            self.api_key = self.api_key[7:].strip()
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        # Content API: ~100 req/min на продавца — чтение и мутации раздельно
        self._read_limiter = RateLimiter(0.75)
        self._mutate_limiter = RateLimiter(0.35)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(self, method: str, path: str, *, json_body: Optional[dict] = None) -> Any:
        return await self._request_raw(
            method, path, json_body=json_body, allow_retry=True, limiter=self._read_limiter,
        )

    async def _request_mutate(self, method: str, path: str, *, json_body: Optional[dict] = None) -> Any:
        """POST-мутации WB (moveNm): retry только на 429, не на 400 duplicate."""
        return await self._request_raw(
            method,
            path,
            json_body=json_body,
            allow_retry=True,
            retry_on_status=(429,),
            retries=4,
            retry_delays=(2.0, 5.0, 10.0, 15.0),
            limiter=self._mutate_limiter,
        )

    async def _request_raw(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict] = None,
        allow_retry: bool,
        limiter: Optional[RateLimiter] = None,
        retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504),
        retries: int = 4,
        retry_delays: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0),
    ) -> Any:
        url = BASE + path
        lim = limiter or self._read_limiter

        async def _do():
            await lim.wait()
            connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
            async with connector:
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.request(method, url, headers=self._headers(), json=json_body) as resp:
                        txt = await resp.text()
                        if resp.status >= 400:
                            raise HttpStatusError(resp.status, txt)
                        if not txt:
                            return {}
                        try:
                            return json.loads(txt)
                        except Exception as e:
                            log.warning("WB Content API invalid JSON: %s", e)
                            raise HttpStatusError(502, f"Invalid JSON: {str(e)[:200]}")

        if allow_retry:
            last_exc: Optional[BaseException] = None
            for attempt in range(max(1, retries)):
                try:
                    return await _do()
                except HttpStatusError as e:
                    last_exc = e
                    if e.status not in retry_on_status or attempt >= retries - 1:
                        raise
                delay = retry_delays[attempt] if attempt < len(retry_delays) else retry_delays[-1]
                await asyncio.sleep(delay)
            if last_exc:
                raise last_exc
        return await _do()

    async def list_subjects(self) -> List[dict]:
        """GET /content/v2/object/all — предметы и родительские категории."""
        data = await self._request("GET", "/content/v2/object/all")
        if isinstance(data, dict):
            rows = data.get("data")
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
        return []

    async def list_cards(
        self,
        *,
        limit: int = 100,
        text_search: Optional[str] = None,
        vendor_codes: Optional[List[str]] = None,
        nm_ids: Optional[List[int]] = None,
        cursor_updated_at: Optional[str] = None,
        cursor_nm_id: Optional[int] = None,
    ) -> dict:
        """POST /content/v2/get/cards/list — одна страница."""
        filt: Dict[str, Any] = {"withPhoto": -1}
        if text_search:
            filt["textSearch"] = str(text_search).strip()
        if vendor_codes:
            # WB ищет по textSearch; для нескольких артикулов — через запятую или по одному снаружи
            filt["textSearch"] = ",".join(v.strip() for v in vendor_codes if v and str(v).strip())
        elif nm_ids and len(nm_ids) == 1:
            # nmID в filter не работает — ищем по vendorCode через textSearch снаружи
            pass
        cursor: Dict[str, Any] = {"limit": min(max(int(limit), 1), 100)}
        if cursor_updated_at:
            cursor["updatedAt"] = cursor_updated_at
        if cursor_nm_id is not None:
            cursor["nmID"] = int(cursor_nm_id)
        body = {
            "settings": {
                "sort": {"ascending": False},
                "filter": filt,
                "cursor": cursor,
            }
        }
        data = await self._request("POST", "/content/v2/get/cards/list", json_body=body)
        return data if isinstance(data, dict) else {}

    async def list_cards_all(
        self,
        *,
        max_pages: int = 100,
        text_search: Optional[str] = None,
        vendor_codes: Optional[List[str]] = None,
        meta_out: Optional[dict] = None,
    ) -> List[dict]:
        """Пагинация cards/list. meta_out: pages_fetched, max_pages, truncated, last_batch_size."""
        max_p = max(1, int(max_pages))
        if vendor_codes:
            out: List[dict] = []
            seen: set[int] = set()
            for vc in vendor_codes:
                v = (vc or "").strip()
                if not v:
                    continue
                page = await self.list_cards(limit=100, text_search=v)
                for card in page.get("cards") or []:
                    if not isinstance(card, dict):
                        continue
                    nid = int(card.get("nmID") or 0)
                    if nid and nid not in seen:
                        seen.add(nid)
                        out.append(card)
            return out

        rows: List[dict] = []
        updated_at: Optional[str] = None
        nm_id: Optional[int] = None
        pages_fetched = 0
        last_batch_size = 0
        truncated = False
        for _ in range(max_p):
            page = await self.list_cards(
                limit=100,
                text_search=text_search,
                cursor_updated_at=updated_at,
                cursor_nm_id=nm_id,
            )
            pages_fetched += 1
            batch = page.get("cards") or []
            if not isinstance(batch, list) or not batch:
                break
            last_batch_size = len(batch)
            for card in batch:
                if isinstance(card, dict):
                    rows.append(card)
            cur = page.get("cursor") or {}
            if not isinstance(cur, dict):
                break
            total = int(cur.get("total") or 0)
            updated_at = str(cur.get("updatedAt") or "") or None
            nm_id = int(cur.get("nmID") or 0) or None
            if total < 100 or not updated_at or not nm_id:
                break
            if pages_fetched >= max_p:
                truncated = last_batch_size >= 100
                break
        if meta_out is not None:
            meta_out.update(
                {
                    "pages_fetched": pages_fetched,
                    "max_pages": max_p,
                    "truncated": truncated,
                    "last_batch_size": last_batch_size,
                    "count": len(rows),
                }
            )
        return rows

    async def merge_cards(self, *, target_imt: int, nm_ids: List[int]) -> dict:
        """POST /content/v2/cards/moveNm — объединить в существующую связку."""
        ids = [int(x) for x in nm_ids if x is not None][:30]
        if not ids:
            raise ValueError("nm_ids пуст")
        body = {"targetIMT": int(target_imt), "nmIDs": ids}
        return await self._request_mutate("POST", "/content/v2/cards/moveNm", json_body=body)

    async def disconnect_cards(self, nm_ids: List[int]) -> dict:
        """POST /content/v2/cards/moveNm — разъединить (без targetIMT, по одному nmID)."""
        ids = [int(x) for x in nm_ids if x is not None]
        if not ids:
            raise ValueError("nm_ids пуст")
        results: List[dict] = []
        errors: List[dict] = []
        for i, nid in enumerate(ids[:30]):
            try:
                body = {"nmIDs": [int(nid)]}
                data = await self._request_mutate("POST", "/content/v2/cards/moveNm", json_body=body)
                results.append({"nm_id": int(nid), "ok": True, "result": data})
            except HttpStatusError as e:
                errors.append({"nm_id": int(nid), "status": e.status, "body": (e.body or "")[:300]})
            if i + 1 < min(len(ids), 30):
                await asyncio.sleep(1.2)
        return {
            "ok": not errors,
            "processed": len(results),
            "failed": len(errors),
            "results": results,
            "errors": errors,
        }
