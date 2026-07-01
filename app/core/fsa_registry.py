"""Клиент публичного реестра Росаккредитации (pub.fsa.gov.ru)."""
from __future__ import annotations

import asyncio
import json
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
    pdf_source: str = ""  # registry_file | registry_print | generated | none
    message: str = ""
    error: bool = False
    error_kind: str = ""  # network | api


def _fsa_proxy_url() -> Optional[str]:
    for key in ("FSA_PROXY_URL", "HTTPS_PROXY", "HTTP_PROXY"):
        val = str(os.environ.get(key) or "").strip()
        if val:
            return val
    return None


def fsa_proxy_configured() -> bool:
    return bool(_fsa_proxy_url())


def fsa_hosted_on_render() -> bool:
    return bool(os.environ.get("RENDER") or os.environ.get("RENDER_SERVICE_ID"))


def fsa_render_needs_proxy() -> bool:
    return fsa_hosted_on_render() and not fsa_proxy_configured()


_RENDER_FSA_MSG = (
    "На Render реестр ФСА недоступен без прокси. "
    "В Environment добавьте FSA_PROXY_URL (HTTP-прокси в РФ). "
    "Инструкция: deploy/fsa-proxy/README.md"
)


@dataclass
class _ParsedProxy:
    url: str
    auth: Optional[aiohttp.BasicAuth]
    label: str


def _parse_proxy_url(raw: Optional[str]) -> Optional[_ParsedProxy]:
    if not raw:
        return None
    try:
        from urllib.parse import urlparse, unquote

        p = urlparse(str(raw).strip())
        if not p.hostname:
            return None
        scheme = p.scheme or "http"
        port = p.port or (8080 if scheme == "http" else 443)
        host = p.hostname
        user = unquote(p.username) if p.username else None
        password = unquote(p.password) if p.password else None
        auth = aiohttp.BasicAuth(user, password) if user and password else None
        return _ParsedProxy(
            url=f"{scheme}://{host}:{port}",
            auth=auth,
            label=f"{host}:{port}",
        )
    except Exception:
        return None


def _proxy_host_label(proxy_url: Optional[str]) -> str:
    parsed = _parse_proxy_url(proxy_url)
    return parsed.label if parsed else ""


async def check_fsa_access() -> Dict[str, Any]:
    """Проверка конфигурации и доступности pub.fsa.gov.ru (для UI/диагностики)."""
    proxy = _fsa_proxy_url()
    parsed = _parse_proxy_url(proxy)
    out: Dict[str, Any] = {
        "render": fsa_hosted_on_render(),
        "proxy_configured": bool(proxy),
        "proxy_host": parsed.label if parsed else _proxy_host_label(proxy),
        "proxy_reachable": None,
        "reachable": False,
        "message": "",
        "error_kind": "",
    }
    if fsa_render_needs_proxy():
        out["message"] = _RENDER_FSA_MSG
        out["error_kind"] = "config"
        return out
    if parsed:
        try:
            await _ping_proxy(parsed)
            out["proxy_reachable"] = True
        except Exception as e:
            out["proxy_reachable"] = False
            out["message"] = (
                f"Прокси {parsed.label} недоступен с сервера: {str(e)[:160]}. "
                "Проверьте FSA_PROXY_URL в Render и в Proxy.Market отключите привязку только к вашему IP."
            )
            out["error_kind"] = "proxy"
            return out
    try:
        client = FsaRegistryClient(timeout_s=25.0, retries=1)
        await client._ensure_token()
        out["reachable"] = True
        out["message"] = (
            f"Связь с pub.fsa.gov.ru установлена"
            + (f" через прокси {parsed.label}" if parsed else "")
        )
        return out
    except Exception as e:
        kind, msg = _fsa_user_error(e, proxy_label=parsed.label if parsed else "")
        out["message"] = msg
        out["error_kind"] = kind
        return out


async def _ping_proxy(parsed: _ParsedProxy) -> None:
    timeout = aiohttp.ClientTimeout(connect=12, total=20)
    kw: Dict[str, Any] = {"proxy": parsed.url}
    if parsed.auth:
        kw["proxy_auth"] = parsed.auth
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(f"{FSA_BASE}/login", **kw) as resp:
            await resp.read()
            if resp.status >= 500:
                raise ConnectionError(f"HTTP {resp.status}")


def _is_network_error(exc: BaseException) -> bool:
    if isinstance(exc, _NETWORK_ERRORS):
        return True
    msg = str(exc).casefold()
    return any(x in msg for x in ("timeout", "connection", "cannot connect", "network is unreachable"))


def _fsa_user_error(exc: BaseException, *, proxy_label: str = "") -> Tuple[str, str]:
    """(error_kind, user_message)."""
    err = str(exc)
    low = err.casefold()
    host = (proxy_label or "").split(":")[0]

    if host and (host in err or "proxy" in low and "timeout" in low):
        return (
            "proxy",
            f"Прокси {proxy_label} не отвечает с сервера Render. "
            "В Proxy.Market разрешите доступ с любого IP (не только ваш). "
            "Проверьте логин, пароль и порт в FSA_PROXY_URL.",
        )
    if isinstance(exc, asyncio.TimeoutError) or "timeout" in low:
        if proxy_label:
            return (
                "network",
                f"Таймаут ФСА через прокси {proxy_label}. Прокси доступен, но pub.fsa.gov.ru не ответил — попробуйте позже.",
            )
        return (
            "network",
            "Нет доступа к pub.fsa.gov.ru (таймаут). На Render задайте FSA_PROXY_URL (HTTP-прокси в РФ).",
        )
    if _is_network_error(exc):
        if proxy_label:
            return (
                "proxy",
                f"Ошибка прокси {proxy_label}: {err[:140]}. Проверьте FSA_PROXY_URL и whitelist в Proxy.Market.",
            )
        return (
            "network",
            f"Нет связи с реестром ФСА: {err[:120]}. Задайте FSA_PROXY_URL на сервере.",
        )
    return "api", f"Ошибка ФСА: {err[:200]}"


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
    def __init__(
        self,
        *,
        timeout_s: float = 45.0,
        proxy_url: Optional[str] = None,
        retries: int = 3,
    ) -> None:
        self.timeout = aiohttp.ClientTimeout(connect=20, total=timeout_s)
        self._limiter = RateLimiter(0.7)
        self._token: str = ""
        self._token_ts: float = 0.0
        raw_proxy = proxy_url if proxy_url is not None else _fsa_proxy_url()
        self._parsed_proxy = _parse_proxy_url(raw_proxy)
        self._retries = max(1, int(retries))

    def _proxy_kwargs(self) -> Dict[str, Any]:
        if not self._parsed_proxy:
            return {}
        kw: Dict[str, Any] = {"proxy": self._parsed_proxy.url}
        if self._parsed_proxy.auth:
            kw["proxy_auth"] = self._parsed_proxy.auth
        return kw

    def _proxy_label(self) -> str:
        return self._parsed_proxy.label if self._parsed_proxy else ""

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
                    **self._proxy_kwargs(),
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

        await retry(_do, retries=self._retries)

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
                    **self._proxy_kwargs(),
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
            return await retry(_do, retries=self._retries)
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
                    **self._proxy_kwargs(),
                ) as resp:
                    if resp.status >= 400:
                        txt = await resp.text()
                        raise HttpStatusError(resp.status, txt[:300])
                    return await resp.read()

        return await retry(_do, retries=2)

    async def _get_json(self, path: str, *, referer: str) -> Any:
        raw = await self._get_bytes(path, referer=referer)
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}

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

    async def _fetch_item_detail(self, record: FsaRecord) -> dict:
        """Полная карточка по id — в ней есть idFile вложений (в /get его часто нет)."""
        fsa_id = int(record.fsa_id or 0)
        if not fsa_id:
            return record.raw
        if record.doc_type == "certificate":
            path = f"/api/v1/rss/common/certificates/{fsa_id}"
            referer = record.view_url or f"{FSA_BASE}/rss/certificate/view/{fsa_id}"
        else:
            path = f"/api/v1/rds/common/declarations/{fsa_id}"
            referer = record.view_url or f"{FSA_BASE}/rds/declaration/view/{fsa_id}"
        try:
            data = await self._get_json(path, referer=referer)
        except HttpStatusError as e:
            log.warning("FSA detail %s id=%s: %s", record.doc_type, fsa_id, e)
            return record.raw
        if isinstance(data, dict) and data:
            merged = dict(record.raw)
            merged.update(data)
            return merged
        return record.raw

    def _collect_file_ids(self, item: dict) -> List[int]:
        ids: List[int] = []

        def _add(raw: Any) -> None:
            try:
                fid = int(raw or 0)
            except (TypeError, ValueError):
                return
            if fid > 0:
                ids.append(fid)

        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                for key, val in obj.items():
                    if key in ("idFile", "fileId", "id_file"):
                        _add(val)
                    else:
                        _walk(val)
            elif isinstance(obj, list):
                for x in obj:
                    _walk(x)

        _walk(item)
        return list(dict.fromkeys(ids))

    async def _try_download_file(self, file_id: int, *, referer: str, doc_type: str) -> bytes:
        if doc_type == "certificate":
            prefixes = ("/api/v1/rss/common", "/api/v1/rds/common", "/api/v1/common")
        else:
            prefixes = ("/api/v1/rds/common", "/api/v1/rss/common", "/api/v1/common")
        paths: List[str] = []
        for prefix in prefixes:
            paths.extend([
                f"{prefix}/file/download/{file_id}",
                f"{prefix}/files/{file_id}/download",
                f"{prefix}/download/file/{file_id}",
            ])
        for p in paths:
            try:
                data = await self._get_bytes(p, referer=referer)
                if data and is_probably_pdf(data):
                    return data
            except HttpStatusError:
                continue
        return b""

    async def _try_download_print_pdf(self, record: FsaRecord) -> Tuple[bytes, str]:
        fsa_id = int(record.fsa_id or 0)
        if not fsa_id:
            return b"", ""
        referer = record.view_url or FSA_BASE
        if record.doc_type == "certificate":
            paths = [
                f"/api/v1/rss/common/certificates/{fsa_id}/print",
                f"/api/v1/rss/common/certificates/print/{fsa_id}",
            ]
        else:
            paths = [
                f"/api/v1/rds/common/declarations/{fsa_id}/print",
                f"/api/v1/rds/common/declarations/print/{fsa_id}",
            ]
        for p in paths:
            try:
                data = await self._get_bytes(p, referer=referer)
                if is_probably_pdf(data):
                    return data, "registry_print"
            except HttpStatusError:
                continue
        return b"", ""

    async def _build_pdf(self, record: FsaRecord) -> Tuple[bytes, str]:
        full = await self._fetch_item_detail(record)
        record.raw = full
        referer = record.view_url or (
            f"{FSA_BASE}/rss/certificate/view/{record.fsa_id}"
            if record.doc_type == "certificate"
            else f"{FSA_BASE}/rds/declaration/view/{record.fsa_id}"
        )

        print_data, print_src = await self._try_download_print_pdf(record)
        if print_data:
            return print_data, print_src

        for fid in self._collect_file_ids(full):
            data = await self._try_download_file(fid, referer=referer, doc_type=record.doc_type)
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
            else:
                if pdf_source == "generated":
                    msg = (
                        f"{msg}; PDF: сформирован из данных реестра "
                        "(официальный скан не скачан — откройте ссылку вручную)"
                    )
                elif pdf_source in ("registry_file", "registry_print"):
                    msg = f"{msg}; PDF: из реестра ФСА"

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

    if fsa_render_needs_proxy():
        log.error("FSA: %s", _RENDER_FSA_MSG)
        for i, number in enumerate(keys):
            out[number] = _error_result(number, _type_for(number), "config", _RENDER_FSA_MSG)
            if progress_cb:
                progress_cb(i + 1, total, f"ФСА: {number[:40]}")
        return out

    try:
        await client._ensure_token()
    except Exception as e:
        kind, msg = _fsa_user_error(e, proxy_label=client._proxy_label())
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
            kind, msg = _fsa_user_error(e, proxy_label=client._proxy_label())
            log.warning("FSA lookup %s (%s): %s", number, kind, e)
            out[number] = _error_result(number, doc_type, kind, msg)
        if progress_cb:
            progress_cb(i + 1, total, f"ФСА: {number[:40]}")
        await asyncio.sleep(0.05)
    return out
