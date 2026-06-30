"""Клиент публичного реестра Росаккредитации (pub.fsa.gov.ru)."""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry
from .pdf_registry import is_probably_pdf, make_registry_pdf

log = logging.getLogger("fsa.registry")

FSA_BASE = "https://pub.fsa.gov.ru"
FSA_LOGIN_USER = "anonymous"
FSA_LOGIN_PASS = "hrgesf7HDR67Bd"

# idStatus 6 = действует (по опыту парсеров реестра)
_FSA_ACTIVE_STATUS = {6, "6", "ACTIVE", "Действует"}

_NETWORK_ERRORS: Tuple[type, ...] = (
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
)
try:
    from aiohttp import ClientConnectorError, ClientOSError, ServerDisconnectedError

    _NETWORK_ERRORS = _NETWORK_ERRORS + (
        ClientConnectorError,
        ClientOSError,
        ServerDisconnectedError,
    )
except ImportError:
    pass


@dataclass
class FsaRecord:
    doc_type: str  # declaration | certificate
    fsa_id: int
    number: str
    reg_date: str = ""
    end_date: str = ""
    status_id: Any = None
    status_label: str = ""
    product_names: List[str] = field(default_factory=list)
    applicant: str = ""
    manufacturer: str = ""
    view_url: str = ""
    raw: dict = field(default_factory=dict)

    def is_active(self) -> bool:
        if self.status_id in _FSA_ACTIVE_STATUS:
            return True
        low = str(self.status_label or "").casefold()
        return "действ" in low and "прекращ" not in low and "отмен" not in low


@dataclass
class FsaLookupResult:
    doc_number: str
    doc_type: str
    found: bool = False
    record: Optional[FsaRecord] = None
    pdf_bytes: bytes = b""
    pdf_source: str = ""  # registry | generated | none
    message: str = ""
    error: bool = False
    error_kind: str = ""  # network | api


def _fsa_proxy_url() -> Optional[str]:
    for key in ("FSA_PROXY_URL", "HTTPS_PROXY", "HTTP_PROXY"):
        val = str(os.environ.get(key) or "").strip()
        if val:
            return val
    return None


def _is_network_error(exc: BaseException) -> bool:
    if isinstance(exc, _NETWORK_ERRORS):
        return True
    msg = str(exc).casefold()
    return any(x in msg for x in ("timeout", "connection", "cannot connect", "network is unreachable"))


def _fsa_user_error(exc: BaseException) -> Tuple[str, str]:
    """(error_kind, user_message)."""
    if isinstance(exc, asyncio.TimeoutError) or "timeout" in str(exc).casefold():
        return (
            "network",
            "Нет доступа к pub.fsa.gov.ru (таймаут). Реестр ФСА доступен из сети РФ. "
            "Запустите приложение локально или задайте FSA_PROXY_URL (HTTP-прокси в России) на сервере.",
        )
    if _is_network_error(exc):
        return (
            "network",
            f"Нет связи с реестром ФСА: {str(exc)[:120]}. "
            "Проверьте сеть или задайте FSA_PROXY_URL на сервере.",
        )
    return "api", f"Ошибка ФСА: {str(exc)[:200]}"


def _norm_number(num: str) -> str:
    return re.sub(r"\s+", " ", str(num or "").strip())


def _search_variants(number: str) -> List[str]:
    n = _norm_number(number)
    if not n:
        return []
    out = [n]
    compact = re.sub(r"\s+", "", n)
    if compact != n:
        out.append(compact)
    # хвост номера (например RA01.V.12345/25)
    m = re.search(r"[\w./-]+/\d{2}\s*$", n)
    if m:
        tail = m.group(0).strip()
        if tail not in out:
            out.append(tail)
    return out


def _date_dmy(raw: Any) -> str:
    t = str(raw or "").strip()
    if not t:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        return f"{m.group(3)}.{m.group(2)}.{m.group(1)}"
    return t


def _entity_name(block: Any) -> str:
    if not isinstance(block, dict):
        return ""
    for key in ("fullName", "name", "shortName", "full_name"):
        v = str(block.get(key) or "").strip()
        if v:
            return v
    return ""


def _extract_product_names(item: dict) -> List[str]:
    names: List[str] = []
    product = item.get("product")
    if isinstance(product, dict):
        fn = str(product.get("fullName") or product.get("name") or "").strip()
        if fn:
            names.append(fn)
        ids_block = product.get("identifications")
        if isinstance(ids_block, list):
            for ident in ids_block:
                if not isinstance(ident, dict):
                    continue
                nm = str(ident.get("name") or "").strip()
                if nm and nm not in names:
                    names.append(nm)
    elif isinstance(product, list):
        for p in product:
            if isinstance(p, dict):
                fn = str(p.get("fullName") or p.get("name") or "").strip()
                if fn and fn not in names:
                    names.append(fn)
    return names[:20]


def _filter_payload(number: str, *, sort_column: str) -> dict:
    return {
        "size": 10,
        "page": 0,
        "filter": {
            "status": [],
            "idDeclType": [],
            "idCertObjectType": [],
            "idProductType": [],
            "idGroupRU": [],
            "idGroupEEU": [],
            "idTechReg": [],
            "idApplicantType": [],
            "regDate": {"minDate": None, "maxDate": None},
            "endDate": {"minDate": None, "maxDate": None},
            "columnsSearch": [
                {
                    "name": "number",
                    "search": number,
                    "type": 0,
                    "translated": False,
                }
            ],
            "idProductOrigin": [],
            "idProductEEU": [],
            "idProductRU": [],
            "idDeclScheme": [],
            "awaitForApprove": None,
            "awaitOperatorCheck": None,
            "editApp": None,
            "violationSendDate": None,
        },
        "columnsSort": [{"column": sort_column, "sort": "DESC"}],
    }


class FsaRegistryClient:
    def __init__(self, *, timeout_s: float = 45.0, proxy_url: Optional[str] = None) -> None:
        self.timeout = aiohttp.ClientTimeout(connect=20, total=timeout_s)
        self._limiter = RateLimiter(0.7)
        self._token: str = ""
        self._token_ts: float = 0.0
        self._proxy = proxy_url if proxy_url is not None else _fsa_proxy_url()

    def _headers(self, *, referer: str) -> Dict[str, str]:
        h = {
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "Origin": FSA_BASE,
            "Referer": referer,
        }
        if self._token:
            h["Authorization"] = self._token
        return h

    async def _ensure_token(self) -> None:
        if self._token and (time.time() - self._token_ts) < 6 * 3600:
            return

        async def _do() -> None:
            await self._limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True)
            async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                async with s.post(
                    f"{FSA_BASE}/login",
                    json={"username": FSA_LOGIN_USER, "password": FSA_LOGIN_PASS},
                    headers={
                        "Content-Type": "application/json",
                        "Origin": FSA_BASE,
                        "User-Agent": USER_AGENT,
                    },
                    proxy=self._proxy,
                ) as resp:
                    txt = await resp.text()
                    if resp.status >= 400:
                        raise HttpStatusError(resp.status, txt[:500])
                    auth = resp.headers.get("Authorization") or resp.headers.get("authorization")
                    if not auth and txt.strip().startswith("eyJ"):
                        token_body = txt.strip().strip('"')
                        auth = f"Bearer {token_body}"
                    if not auth:
                        raise HttpStatusError(502, "ФСА: не получен токен авторизации")
                    self._token = auth if auth.lower().startswith("bearer") else f"Bearer {auth}"
                    self._token_ts = time.time()

        await retry(_do, retries=3)

    async def _post_json(self, path: str, body: dict, *, referer: str) -> Any:
        await self._ensure_token()

        async def _do() -> Any:
            await self._limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True)
            async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                async with s.post(
                    f"{FSA_BASE}{path}",
                    json=body,
                    headers=self._headers(referer=referer),
                    proxy=self._proxy,
                ) as resp:
                    txt = await resp.text()
                    if resp.status == 401:
                        self._token = ""
                        raise HttpStatusError(401, txt[:300])
                    if resp.status >= 400:
                        raise HttpStatusError(resp.status, txt[:500])
                    if not txt:
                        return {}
                    return await resp.json()

        try:
            return await retry(_do, retries=3)
        except HttpStatusError as e:
            if e.status == 401:
                self._token = ""
                await self._ensure_token()
                return await retry(_do, retries=2)
            raise

    async def _get_bytes(self, path: str, *, referer: str) -> bytes:
        await self._ensure_token()

        async def _do() -> bytes:
            await self._limiter.wait()
            connector = aiohttp.TCPConnector(force_close=True)
            async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                async with s.get(
                    f"{FSA_BASE}{path}",
                    headers=self._headers(referer=referer),
                    proxy=self._proxy,
                ) as resp:
                    if resp.status >= 400:
                        txt = await resp.text()
                        raise HttpStatusError(resp.status, txt[:300])
                    return await resp.read()

        return await retry(_do, retries=2)

    async def _search_items(
        self,
        doc_type: str,
        number: str,
    ) -> List[dict]:
        if doc_type == "certificate":
            path = "/api/v1/rss/common/certificates/get"
            referer = f"{FSA_BASE}/rss/certificate"
            sort_col = "certDate"
        else:
            path = "/api/v1/rds/common/declarations/get"
            referer = f"{FSA_BASE}/rds/declaration"
            sort_col = "declDate"

        for variant in _search_variants(number):
            data = await self._post_json(
                path,
                _filter_payload(variant, sort_column=sort_col),
                referer=referer,
            )
            items = []
            if isinstance(data, dict):
                items = data.get("items") or data.get("content") or []
            if not isinstance(items, list):
                items = []
            if items:
                return [x for x in items if isinstance(x, dict)]
        return []

    def _item_to_record(self, doc_type: str, item: dict) -> FsaRecord:
        fsa_id = int(item.get("id") or item.get("idDeclaration") or item.get("idCertificate") or 0)
        number = str(item.get("number") or "").strip()
        reg = _date_dmy(item.get("declDate") or item.get("certDate") or item.get("regDate"))
        end = _date_dmy(
            item.get("declEndDate")
            or item.get("certEndDate")
            or item.get("endDate")
            or item.get("dateEnd")
        )
        status_id = item.get("idStatus")
        status_label = str(item.get("statusName") or item.get("status") or "").strip()
        if doc_type == "declaration":
            view = f"{FSA_BASE}/rds/declaration/view/{fsa_id}" if fsa_id else ""
        else:
            view = f"{FSA_BASE}/rss/certificate/view/{fsa_id}" if fsa_id else ""
        return FsaRecord(
            doc_type=doc_type,
            fsa_id=fsa_id,
            number=number,
            reg_date=reg,
            end_date=end,
            status_id=status_id,
            status_label=status_label,
            product_names=_extract_product_names(item),
            applicant=_entity_name(item.get("applicant")),
            manufacturer=_entity_name(item.get("manufacturer")),
            view_url=view,
            raw=item,
        )

    def _collect_file_ids(self, item: dict) -> List[int]:
        ids: List[int] = []
        labs = item.get("testingLabs")
        if isinstance(labs, list):
            for lab in labs:
                if not isinstance(lab, dict):
                    continue
                protos = lab.get("protocols")
                if not isinstance(protos, list):
                    continue
                for pr in protos:
                    if not isinstance(pr, dict):
                        continue
                    try:
                        fid = int(pr.get("idFile") or 0)
                    except (TypeError, ValueError):
                        fid = 0
                    if fid:
                        ids.append(fid)
        docs = item.get("documents")
        if isinstance(docs, dict):
            for v in docs.values():
                if isinstance(v, list):
                    for d in v:
                        if isinstance(d, dict):
                            try:
                                fid = int(d.get("idFile") or d.get("id") or 0)
                            except (TypeError, ValueError):
                                fid = 0
                            if fid:
                                ids.append(fid)
        return list(dict.fromkeys(ids))

    async def _try_download_file(self, file_id: int, *, referer: str) -> bytes:
        paths = [
            f"/api/v1/rds/common/file/download/{file_id}",
            f"/api/v1/rds/common/files/{file_id}/download",
            f"/api/v1/common/file/download/{file_id}",
            f"/api/v1/common/download/file/{file_id}",
        ]
        for p in paths:
            try:
                data = await self._get_bytes(p, referer=referer)
                if data and (is_probably_pdf(data) or len(data) > 200):
                    return data
            except HttpStatusError:
                continue
        return b""

    async def _build_pdf(self, record: FsaRecord) -> Tuple[bytes, str]:
        referer = (
            f"{FSA_BASE}/rss/certificate"
            if record.doc_type == "certificate"
            else f"{FSA_BASE}/rds/declaration"
        )
        for fid in self._collect_file_ids(record.raw):
            data = await self._try_download_file(fid, referer=referer)
            if is_probably_pdf(data):
                return data, "registry_file"

        lines = [
            f"Number: {record.number}",
            f"Registration: {record.reg_date or '-'}",
            f"Valid until: {record.end_date or '-'}",
            f"Status: {record.status_label or record.status_id or '-'}",
        ]
        if record.applicant:
            lines.append(f"Applicant: {record.applicant}")
        if record.manufacturer:
            lines.append(f"Manufacturer: {record.manufacturer}")
        if record.product_names:
            lines.append("Products:")
            lines.extend(f"- {n}" for n in record.product_names[:8])
        if record.view_url:
            lines.append(f"Registry: {record.view_url}")
        title = "Declaration of conformity" if record.doc_type == "declaration" else "Certificate of conformity"
        return make_registry_pdf(title=title, lines=lines), "generated"

    async def lookup(
        self,
        doc_number: str,
        *,
        doc_type: str = "unknown",
        fetch_pdf: bool = True,
    ) -> FsaLookupResult:
        number = _norm_number(doc_number)
        if not number:
            return FsaLookupResult(doc_number=doc_number, doc_type=doc_type, message="Пустой номер")

        types_to_try: List[str] = []
        if doc_type == "declaration":
            types_to_try = ["declaration"]
        elif doc_type == "certificate":
            types_to_try = ["certificate"]
        else:
            types_to_try = ["declaration", "certificate"]

        record: Optional[FsaRecord] = None
        for t in types_to_try:
            items = await self._search_items(t, number)
            if items:
                record = self._item_to_record(t, items[0])
                break

        if not record:
            return FsaLookupResult(
                doc_number=number,
                doc_type=doc_type,
                found=False,
                message="Не найдено в реестре ФСА",
            )

        msg = "Найдено в реестре ФСА"
        if not record.is_active():
            msg = f"Найдено, статус: {record.status_label or record.status_id}"

        pdf_bytes = b""
        pdf_source = "none"
        if fetch_pdf:
            try:
                pdf_bytes, pdf_source = await self._build_pdf(record)
            except Exception as e:
                log.warning("FSA pdf for %s: %s", number, e)
                msg = f"{msg}; PDF: ошибка ({e})"

        if fetch_pdf and not pdf_bytes:
            msg = f"{msg}; PDF не получен"

        return FsaLookupResult(
            doc_number=number,
            doc_type=record.doc_type,
            found=True,
            record=record,
            pdf_bytes=pdf_bytes,
            pdf_source=pdf_source,
            message=msg,
        )


async def lookup_fsa_batch(
    items: List[Tuple[str, str]],
    *,
    fetch_pdf: bool = True,
    progress_cb=None,
) -> Dict[str, FsaLookupResult]:
    """items: [(doc_number, doc_type), ...] — уникальные номера."""
    client = FsaRegistryClient()
    out: Dict[str, FsaLookupResult] = {}
    keys = list(dict.fromkeys(_norm_number(n) for n, _ in items if _norm_number(n)))
    total = max(len(keys), 1)

    def _type_for(number: str) -> str:
        for n, t in items:
            if _norm_number(n) == number:
                return t
        return "unknown"

    def _error_result(number: str, doc_type: str, kind: str, message: str) -> FsaLookupResult:
        return FsaLookupResult(
            doc_number=number,
            doc_type=doc_type,
            found=False,
            error=True,
            error_kind=kind,
            message=message,
        )

    try:
        await client._ensure_token()
    except Exception as e:
        kind, msg = _fsa_user_error(e)
        log.warning("FSA probe failed (%s): %s", kind, e)
        for i, number in enumerate(keys):
            out[number] = _error_result(number, _type_for(number), kind, msg)
            if progress_cb:
                progress_cb(i + 1, total, f"ФСА: {number[:40]}")
        return out

    for i, number in enumerate(keys):
        doc_type = _type_for(number)
        try:
            out[number] = await client.lookup(number, doc_type=doc_type, fetch_pdf=fetch_pdf)
        except Exception as e:
            kind, msg = _fsa_user_error(e)
            log.warning("FSA lookup %s (%s): %s", number, kind, e)
            out[number] = _error_result(number, doc_type, kind, msg)
        if progress_cb:
            progress_cb(i + 1, total, f"ФСА: {number[:40]}")
        await asyncio.sleep(0.05)
    return out
