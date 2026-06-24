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
_PAGE_LIMIT = 100


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
        cursor: Dict[str, Any] = {"limit": min(max(int(limit), 1), _PAGE_LIMIT)}
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

    async def _list_cards_with_session(
        self,
        session: aiohttp.ClientSession,
        *,
        limit: int = _PAGE_LIMIT,
        text_search: Optional[str] = None,
        vendor_codes: Optional[List[str]] = None,
        cursor_updated_at: Optional[str] = None,
        cursor_nm_id: Optional[int] = None,
    ) -> dict:
        """POST /content/v2/get/cards/list — одна страница (общая сессия для пагинации)."""
        filt: Dict[str, Any] = {"withPhoto": -1}
        if text_search:
            filt["textSearch"] = str(text_search).strip()
        if vendor_codes:
            filt["textSearch"] = ",".join(v.strip() for v in vendor_codes if v and str(v).strip())
        cursor: Dict[str, Any] = {"limit": min(max(int(limit), 1), _PAGE_LIMIT)}
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
        url = BASE + "/content/v2/get/cards/list"
        await self._read_limiter.wait()
        async with session.request("POST", url, headers=self._headers(), json=body) as resp:
            txt = await resp.text()
            if resp.status >= 400:
                raise HttpStatusError(resp.status, txt)
            if not txt:
                return {}
            try:
                data = json.loads(txt)
            except Exception as e:
                log.warning("WB Content API invalid JSON: %s", e)
                raise HttpStatusError(502, f"Invalid JSON: {str(e)[:200]}") from e
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
            allowed = {str(v).strip().casefold() for v in vendor_codes if str(v).strip()}
            for vc in vendor_codes:
                v = (vc or "").strip()
                if not v:
                    continue
                page = await self.list_cards(limit=100, text_search=v)
                for card in page.get("cards") or []:
                    if not isinstance(card, dict):
                        continue
                    card_vc = str(card.get("vendorCode") or card.get("supplierVendorCode") or "").strip().casefold()
                    if allowed and card_vc and card_vc not in allowed:
                        continue
                    nid = int(card.get("nmID") or 0)
                    if nid and nid not in seen:
                        seen.add(nid)
                        out.append(card)
            if meta_out is not None:
                meta_out.update(
                    {
                        "pages_fetched": len(vendor_codes),
                        "max_pages": max_p,
                        "truncated": False,
                        "last_batch_size": len(out),
                        "page_size": _PAGE_LIMIT,
                        "count": len(out),
                        "scope": "articles_only",
                    }
                )
            return out

        rows: List[dict] = []
        seen_nm: set[int] = set()
        updated_at: Optional[str] = None
        nm_id: Optional[int] = None
        pages_fetched = 0
        last_batch_size = 0
        truncated = False
        bulk_timeout = aiohttp.ClientTimeout(
            connect=15,
            total=max(120.0, float(getattr(self.timeout, "total", None) or 45.0)),
        )
        page_retry_delays = (1.5, 3.0, 6.0)
        connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
        async with connector:
            async with aiohttp.ClientSession(timeout=bulk_timeout, connector=connector) as session:
                for _ in range(max_p):
                    page: dict = {}
                    last_page_exc: Optional[HttpStatusError] = None
                    for attempt in range(len(page_retry_delays) + 1):
                        try:
                            page = await self._list_cards_with_session(
                                session,
                                limit=_PAGE_LIMIT,
                                text_search=text_search,
                                cursor_updated_at=updated_at,
                                cursor_nm_id=nm_id,
                            )
                            last_page_exc = None
                            break
                        except HttpStatusError as e:
                            last_page_exc = e
                            if e.status in (429, 500, 502, 503, 504) and attempt < len(page_retry_delays):
                                await asyncio.sleep(page_retry_delays[attempt])
                                continue
                            if pages_fetched > 0 and e.status >= 500:
                                log.warning(
                                    "WB catalog: page %s failed after %s ok pages: %s %s",
                                    pages_fetched + 1,
                                    pages_fetched,
                                    e.status,
                                    (e.body or "")[:200],
                                )
                                truncated = True
                                if meta_out is not None:
                                    meta_out["partial"] = True
                                    meta_out["wb_error_status"] = e.status
                                    meta_out["wb_error_body"] = (e.body or "")[:500]
                                break
                            raise
                    if last_page_exc is not None and pages_fetched > 0:
                        break
                    pages_fetched += 1
                    batch = page.get("cards") or []
                    if not isinstance(batch, list) or not batch:
                        break
                    last_batch_size = len(batch)
                    for card in batch:
                        if not isinstance(card, dict):
                            continue
                        try:
                            nid = int(card.get("nmID") or card.get("nmId") or 0)
                        except (TypeError, ValueError):
                            nid = 0
                        if nid:
                            if nid in seen_nm:
                                continue
                            seen_nm.add(nid)
                        rows.append(card)
                    cur = page.get("cursor") or {}
                    if not isinstance(cur, dict):
                        break
                    updated_at = str(cur.get("updatedAt") or "").strip() or None
                    nm_raw = cur.get("nmID")
                    if nm_raw is None:
                        next_nm_id = None
                    else:
                        try:
                            next_nm_id = int(nm_raw)
                        except (TypeError, ValueError):
                            next_nm_id = None
                    # cursor.total — общее число карточек в кабинете; ориентир — размер страницы
                    if last_batch_size < _PAGE_LIMIT or not updated_at or next_nm_id is None:
                        break
                    nm_id = next_nm_id
                    if pages_fetched >= max_p:
                        truncated = last_batch_size >= _PAGE_LIMIT
                        break
        if meta_out is not None:
            meta_out.update(
                {
                    "pages_fetched": pages_fetched,
                    "max_pages": max_p,
                    "truncated": truncated,
                    "last_batch_size": last_batch_size,
                    "page_size": _PAGE_LIMIT,
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
