"""PDF выписки с зеркала реестра (декларации-соответствия.рус) для Ozon."""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, unquote

import aiohttp

from .compliance_docs import detect_doc_type
from .fsa_registry import FsaLookupResult, _norm_number
from .net import USER_AGENT, HttpStatusError
from .pdf_registry import is_probably_pdf

log = logging.getLogger("compliance.mirror")

MIRROR_BASE = "https://декларации-соответствия.рус"

_HOMO_TO_LAT = str.maketrans({
    "А": "a", "а": "a", "В": "v", "в": "v", "С": "c", "с": "c",
    "Е": "E", "е": "e", "Н": "N", "н": "n", "О": "O", "о": "o",
    "Р": "R", "р": "r", "Т": "T", "т": "t", "У": "U", "у": "u",
    "Х": "H", "х": "h", "Д": "D", "д": "d", "Э": "E", "э": "e",
    "М": "M", "м": "m", "К": "K", "к": "k", "И": "I", "и": "i",
    "Й": "Y", "й": "y", "Ь": "", "ь": "", "Ъ": "", "ъ": "",
    "Г": "G", "г": "g", "П": "P", "п": "p", "Л": "L", "л": "l",
    "З": "Z", "з": "z", "Ж": "Zh", "ж": "zh", "Ц": "Ts", "ц": "ts",
    "Ч": "Ch", "ч": "ch", "Ш": "Sh", "ш": "sh", "Щ": "Sch", "щ": "sch",
    "Ы": "Y", "ы": "y", "Ю": "Yu", "ю": "yu", "Я": "Ya", "я": "ya",
})

_RE_PDF_HREF = re.compile(
    r"""href=["']([^"']+\.pdf[^"']*)["']""",
    re.I,
)
_RE_DOWNLOAD_PDF = re.compile(
    r"""href=["']([^"']+)["'][^>]*>[\s\S]{0,120}?(?:выписк|скачать)[\s\S]{0,80}?pdf""",
    re.I,
)


def _latinize(text: str) -> str:
    return str(text or "").translate(_HOMO_TO_LAT)


def mirror_slugs(doc_number: str) -> List[str]:
    """Варианты URL-slug для зеркала (как на сайте)."""
    raw = str(doc_number or "").strip()
    if not raw:
        return []

    slugs: List[str] = []

    m_eaes = re.match(
        r"^(?:ЕАЭС|EАЭС|EAES)\s+N\s+RU\s+(.+)$",
        raw,
        re.I,
    )
    if m_eaes:
        tail = _latinize(m_eaes.group(1)).lower()
        tail = tail.replace(".", "").replace("/", "").replace(" ", "")
        slugs.append(f"eaes-n-ru-{tail}")

    m_ross = re.match(
        r"^(?:РОСС|POCC|POSS|ROSS)\s+RU\s+(.+)$",
        raw,
        re.I,
    )
    if m_ross:
        tail = _latinize(m_ross.group(1)).lower()
        tail = tail.replace(".", "").replace("/", "").replace(" ", "")
        slugs.append(f"ross-ru-{tail}")
        slugs.append(f"ross-n-ru-{tail}")

    compact = re.sub(r"[^a-zA-Z0-9]+", "", _latinize(raw)).lower()
    if compact:
        slugs.append(compact)

    generic = re.sub(r"[^a-z0-9]+", "-", _latinize(raw).lower()).strip("-")
    generic = re.sub(r"-+", "-", generic)
    if generic:
        slugs.append(generic)

    out: List[str] = []
    seen: set[str] = set()
    for s in slugs:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _extract_pdf_urls(html: str, page_url: str) -> List[str]:
    urls: List[str] = []
    seen: set[str] = set()
    for m in _RE_PDF_HREF.finditer(html or ""):
        href = unquote(m.group(1).strip())
        if href and href not in seen:
            seen.add(href)
            urls.append(href)
    for m in _RE_DOWNLOAD_PDF.finditer(html or ""):
        href = unquote(m.group(1).strip())
        if href and href not in seen:
            seen.add(href)
            urls.append(href)
    abs_urls: List[str] = []
    for href in urls:
        if href.startswith("//"):
            abs_urls.append("https:" + href)
        elif href.startswith("/"):
            abs_urls.append(urljoin(MIRROR_BASE, href))
        elif href.startswith("http"):
            abs_urls.append(href)
        else:
            abs_urls.append(urljoin(page_url, href))
    return abs_urls


def _parse_dates_from_html(html: str) -> Tuple[str, str]:
    reg = end = ""
    m_reg = re.search(
        r"Дата\s+регистрации[^<]{0,40}<[^>]+>[^<]*(\d{1,2}\s+\w+\s+\d{4})",
        html or "",
        re.I,
    )
    if m_reg:
        reg = m_reg.group(1).strip()
    m_end = re.search(
        r"Дата\s+завершения[^<]{0,40}<[^>]+>[^<]*(\d{1,2}\s+\w+\s+\d{4})",
        html or "",
        re.I,
    )
    if m_end:
        end = m_end.group(1).strip()
    return reg, end


class ComplianceMirrorClient:
    def __init__(self, *, timeout_s: float = 45.0) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)

    async def _get_text(self, url: str) -> str:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                if resp.status >= 400:
                    raise HttpStatusError(resp.status, await resp.text())
                return await resp.text()

    async def _get_bytes(self, url: str, *, referer: str = "") -> bytes:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/pdf,*/*",
        }
        if referer:
            headers["Referer"] = referer
        async with aiohttp.ClientSession(timeout=self._timeout) as session:
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                if resp.status >= 400:
                    raise HttpStatusError(resp.status, (await resp.read())[:300].decode("utf-8", "replace"))
                return await resp.read()

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

        dtype = doc_type if doc_type in ("declaration", "certificate") else detect_doc_type(number)
        slugs = mirror_slugs(number)
        if not slugs:
            return FsaLookupResult(
                doc_number=number,
                doc_type=dtype,
                found=False,
                message="Не удалось построить ссылку для зеркала",
            )

        last_err = ""
        for slug in slugs:
            page_url = f"{MIRROR_BASE}/document/{slug}/"
            try:
                html = await self._get_text(page_url)
            except HttpStatusError as e:
                last_err = f"HTTP {e.status}"
                continue
            except Exception as e:
                last_err = str(e)[:120]
                continue

            if "404" in html[:500].casefold() or "не найден" in html.casefold()[:2000]:
                last_err = "страница не найдена"
                continue

            reg_d, end_d = _parse_dates_from_html(html)
            msg = f"Найдено на зеркале ({page_url})"
            if reg_d:
                msg += f"; рег.: {reg_d}"
            if end_d:
                msg += f"; до: {end_d}"

            pdf_bytes = b""
            pdf_source = "none"
            if fetch_pdf:
                for pdf_url in _extract_pdf_urls(html, page_url):
                    try:
                        data = await self._get_bytes(pdf_url, referer=page_url)
                        if is_probably_pdf(data):
                            pdf_bytes = data
                            pdf_source = "mirror_site"
                            msg = f"{msg}; PDF: выписка с зеркала"
                            break
                    except Exception as e:
                        log.debug("mirror pdf %s: %s", pdf_url, e)
                        continue
                if fetch_pdf and not pdf_bytes:
                    msg = f"{msg}; PDF: ссылка на странице не скачалась"

            return FsaLookupResult(
                doc_number=number,
                doc_type=dtype,
                found=True,
                pdf_bytes=pdf_bytes,
                pdf_source=pdf_source,
                message=msg,
            )

        return FsaLookupResult(
            doc_number=number,
            doc_type=dtype,
            found=False,
            message=f"Не найдено на зеркале ({last_err or 'нет совпадений'})",
        )


async def lookup_mirror_batch(
    items: List[Tuple[str, str]],
    *,
    fetch_pdf: bool = True,
    progress_cb=None,
) -> Dict[str, FsaLookupResult]:
    """items: [(doc_number, doc_type), ...] — уникальные номера."""
    client = ComplianceMirrorClient()
    out: Dict[str, FsaLookupResult] = {}
    keys = list(dict.fromkeys(_norm_number(n) for n, _ in items if _norm_number(n)))
    total = max(len(keys), 1)

    def _type_for(number: str) -> str:
        for n, t in items:
            if _norm_number(n) == number:
                return t
        return "unknown"

    for i, number in enumerate(keys):
        if progress_cb:
            progress_cb(i + 1, total, number[:60])
        out[number] = await client.lookup(
            number,
            doc_type=_type_for(number),
            fetch_pdf=fetch_pdf,
        )
        if i + 1 < len(keys):
            await asyncio.sleep(0.8)
    return out
