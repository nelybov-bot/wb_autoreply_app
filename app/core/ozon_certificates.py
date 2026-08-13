"""Ozon: декларации/сертификаты — ФСА → PDF → create/bind."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .compliance_docs import CertInputRow, detect_doc_type, doc_type_label
from .compliance_mirror import lookup_mirror_batch
from .fsa_registry import FsaLookupResult, _norm_number, lookup_fsa_batch
from .net import HttpStatusError
from .ozon_client import OzonClient

log = logging.getLogger("ozon.certificates")

ProgressCb = Callable[[int, int, str], None]

_LEGACY_TYPE = {
    "declaration": "DECLARATION",
    "certificate": "GOST_CERTIFICATE",
}

_DECL_TYPE_HINTS = ("декларац", "declaration")
_CERT_TYPE_HINTS = ("сертификат", "certificate", "гост", "gost")
_SGR_TYPE_HINTS = ("сгр", "sgr", "государствен")
_ACCORDANCE_HINTS = (
    "техническ", "регламент", "тр тс", "тр еаэс", "еаэс", "eaeu", "tr ts", "соответств",
)


@dataclass
class _OzonCertCatalog:
    doc_types: List[dict] = field(default_factory=list)
    accordance_types: List[dict] = field(default_factory=list)


_catalog_by_client: Dict[str, _OzonCertCatalog] = {}


@dataclass
class OzonCertRowResult:
    vendor_code: str
    doc_number: str
    doc_type: str = "unknown"
    product_id: int = 0
    status: str = "pending"
    message: str = ""
    fsa_found: bool = False
    error_kind: str = ""
    pdf_source: str = ""
    certificate_id: int = 0


def _norm_offer(v: str) -> str:
    return str(v or "").strip()


def _iso_date(dmy: str) -> str:
    t = str(dmy or "").strip()
    m = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})", t)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}T00:00:00Z"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}T00:00:00Z"
    return ""


def _date_key(dmy_or_iso: str) -> str:
    iso = _iso_date(dmy_or_iso)
    return iso[:10] if iso else ""


def _cert_dates(cert: dict) -> Tuple[str, str]:
    issue = _date_key(
        cert.get("issue_date") or cert.get("issueDate") or cert.get("date_issue") or ""
    )
    expire = _date_key(
        cert.get("expire_date") or cert.get("expireDate") or cert.get("date_expire") or ""
    )
    return issue, expire


def _cert_needs_replace(
    ozon_cert: dict,
    *,
    fsa_issue: str,
    fsa_expire: str,
) -> bool:
    oz_issue, oz_expire = _cert_dates(ozon_cert)
    if fsa_expire and oz_expire and oz_expire != fsa_expire:
        return True
    if fsa_issue and oz_issue and oz_issue != fsa_issue:
        return True
    return False


def _type_entry_code(entry: dict) -> str:
    for key in ("value", "code", "type_code"):
        val = str(entry.get(key) or "").strip()
        if val:
            return val
    return ""


def _type_entry_name(entry: dict) -> str:
    for key in ("name", "title", "label"):
        val = str(entry.get(key) or "").strip()
        if val:
            return val
    return ""


def _accordance_entry_code(entry: dict) -> str:
    for key in ("code", "value", "type_code"):
        val = str(entry.get(key) or "").strip()
        if val:
            return val
    return ""


def _accordance_entry_title(entry: dict) -> str:
    for key in ("title", "name", "label"):
        val = str(entry.get(key) or "").strip()
        if val:
            return val
    return ""


def _type_match_score(text: str, doc_type: str) -> int:
    t = text.casefold()
    if doc_type == "declaration":
        if any(h in t for h in _DECL_TYPE_HINTS):
            return 100
        if any(h in t for h in _CERT_TYPE_HINTS + _SGR_TYPE_HINTS):
            return -50
        return 0
    if doc_type == "certificate":
        if any(h in t for h in _CERT_TYPE_HINTS):
            return 100
        if any(h in t for h in _DECL_TYPE_HINTS):
            return -50
        if any(h in t for h in _SGR_TYPE_HINTS):
            return 10
        return 0
    if any(h in t for h in _DECL_TYPE_HINTS):
        return 80
    if any(h in t for h in _CERT_TYPE_HINTS):
        return 60
    return 0


def _resolve_type_code(doc_types: List[dict], doc_type: str) -> str:
    best_code = ""
    best_score = -999
    legacy = _LEGACY_TYPE.get(doc_type, "")
    available = {_type_entry_code(x) for x in doc_types if _type_entry_code(x)}

    if legacy and legacy in available:
        return legacy

    for entry in doc_types:
        code = _type_entry_code(entry)
        if not code:
            continue
        label = f"{_type_entry_name(entry)} {code}"
        score = _type_match_score(label, doc_type)
        if score > best_score:
            best_score = score
            best_code = code

    if best_code and best_score > 0:
        return best_code
    if legacy:
        return legacy
    if doc_types:
        return _type_entry_code(doc_types[0])
    return ""


def _resolve_accordance_type_code(accordance_types: List[dict], doc_type: str) -> str:
    if not accordance_types:
        return ""
    best_code = ""
    best_score = -999
    for entry in accordance_types:
        code = _accordance_entry_code(entry)
        if not code:
            continue
        title = _accordance_entry_title(entry)
        text = f"{title} {code}".casefold()
        score = sum(3 for h in _ACCORDANCE_HINTS if h in text)
        if doc_type == "declaration" and "декларац" in text:
            score += 5
        if doc_type == "certificate" and "сертификат" in text:
            score += 5
        if score > best_score:
            best_score = score
            best_code = code
    if best_code and best_score > 0:
        return best_code
    return _accordance_entry_code(accordance_types[0])


async def _load_ozon_cert_catalog(client: OzonClient) -> _OzonCertCatalog:
    key = client.client_id or "default"
    cached = _catalog_by_client.get(key)
    if cached and (cached.doc_types or cached.accordance_types):
        return cached

    catalog = _OzonCertCatalog()
    try:
        catalog.doc_types = await client.product_certificate_types()
    except HttpStatusError as e:
        log.warning("ozon certificate types: %s", e)
    try:
        catalog.accordance_types = await client.product_certificate_accordance_types()
    except HttpStatusError as e:
        log.warning("ozon certificate accordance types: %s", e)

    if catalog.doc_types or catalog.accordance_types:
        _catalog_by_client[key] = catalog
        log.info(
            "ozon cert catalog client=%s: %d doc types, %d accordance types",
            key[:8],
            len(catalog.doc_types),
            len(catalog.accordance_types),
        )
    return catalog


def _ozon_type_code(doc_type: str, doc_types: Optional[List[dict]] = None) -> str:
    if doc_types:
        code = _resolve_type_code(doc_types, doc_type)
        if code:
            return code
    return _LEGACY_TYPE.get(doc_type, "DECLARATION")


def _ozon_accordance_type_code(
    doc_type: str,
    accordance_types: Optional[List[dict]] = None,
) -> str:
    if accordance_types:
        return _resolve_accordance_type_code(accordance_types, doc_type)
    return ""


def _extract_certificate_id(data: dict) -> int:
    if not isinstance(data, dict):
        return 0
    for key in ("certificate_id", "id"):
        try:
            val = int(data.get(key) or 0)
        except (TypeError, ValueError):
            val = 0
        if val:
            return val
    res = data.get("result")
    if isinstance(res, dict):
        try:
            return int(res.get("certificate_id") or res.get("id") or 0)
        except (TypeError, ValueError):
            pass
    return 0


async def _map_offers_to_product_ids(
    client: OzonClient,
    offer_ids: List[str],
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    oids = [_norm_offer(x) for x in offer_ids if _norm_offer(x)]
    for i in range(0, len(oids), 1000):
        batch = oids[i : i + 1000]
        for info in await client.product_info_list(offer_ids=batch):
            if not isinstance(info, dict):
                continue
            oid = _norm_offer(info.get("offer_id") or info.get("offerId"))
            try:
                pid = int(info.get("id") or info.get("product_id") or 0)
            except (TypeError, ValueError):
                pid = 0
            if oid and pid:
                out[oid] = pid
    return out


async def _find_ozon_certificate(
    client: OzonClient,
    doc_number: str,
) -> Optional[dict]:
    target = re.sub(r"\s+", "", str(doc_number or "").casefold())
    if not target:
        return None
    for page in range(1, 21):
        certs = await client.product_certificate_list(page=page, page_size=100)
        if not certs:
            break
        for c in certs:
            num = re.sub(r"\s+", "", str(c.get("number") or c.get("certificate_number") or "").casefold())
            if num and (num == target or target in num or num in target):
                return c
    return None


async def _find_ozon_certificate_id(
    client: OzonClient,
    doc_number: str,
) -> int:
    cert = await _find_ozon_certificate(client, doc_number)
    if not cert:
        return 0
    try:
        return int(cert.get("certificate_id") or cert.get("id") or 0)
    except (TypeError, ValueError):
        return 0


async def _ensure_ozon_cert_dates(client: OzonClient, cert: dict) -> dict:
    """Дополняет даты из /info, если в list их не было."""
    oz_issue, oz_expire = _cert_dates(cert)
    if oz_issue and oz_expire:
        return cert
    try:
        cid = int(cert.get("certificate_id") or cert.get("id") or 0)
    except (TypeError, ValueError):
        return cert
    if not cid:
        return cert
    try:
        info = await client.product_certificate_info(cid)
    except HttpStatusError:
        return cert
    if not isinstance(info, dict):
        return cert
    merged = dict(cert)
    res = info.get("result") if isinstance(info.get("result"), dict) else info
    if isinstance(res, dict):
        merged.update(res)
    return merged


async def _resolve_ozon_certificate_id(
    client: OzonClient,
    *,
    doc_key: str,
    row: CertInputRow,
    doc_type: str,
    fsa: FsaLookupResult,
    cert_cache: Dict[str, int],
    cert_catalog: _OzonCertCatalog,
) -> Tuple[int, str, str]:
    """Один certificate_id на номер документа: кэш пачки → поиск в Ozon → create."""
    cached = cert_cache.get(doc_key, 0)
    if cached:
        return cached, "reused_batch", f"сертификат {cached} (из этой пачки)"

    issue = _iso_date(row.reg_date or (fsa.record.reg_date if fsa.record else ""))
    expire = _iso_date(row.valid_until or (fsa.record.end_date if fsa.record else ""))
    fsa_issue_key = _date_key(row.reg_date or (fsa.record.reg_date if fsa.record else ""))
    fsa_expire_key = _date_key(row.valid_until or (fsa.record.end_date if fsa.record else ""))

    replaced_from = 0
    existing_cert = await _find_ozon_certificate(client, row.doc_number)
    if existing_cert:
        existing_cert = await _ensure_ozon_cert_dates(client, existing_cert)
        try:
            existing_id = int(existing_cert.get("certificate_id") or existing_cert.get("id") or 0)
        except (TypeError, ValueError):
            existing_id = 0
        if existing_id and not _cert_needs_replace(
            existing_cert,
            fsa_issue=fsa_issue_key,
            fsa_expire=fsa_expire_key,
        ):
            cert_cache[doc_key] = existing_id
            return existing_id, "reused_ozon", f"сертификат {existing_id} уже в Ozon"
        if existing_id:
            try:
                await client.product_certificate_delete(existing_id)
            except HttpStatusError as e:
                return 0, "error", (
                    f"Не удалось удалить устаревший сертификат {existing_id}: {str(e)[:200]}"
                )
            replaced_from = existing_id
            cert_cache.pop(doc_key, None)

    if not issue:
        return 0, "error", "Нет даты регистрации для Ozon"

    type_code = _ozon_type_code(doc_type, cert_catalog.doc_types)
    accordance_code = _ozon_accordance_type_code(doc_type, cert_catalog.accordance_types)
    try:
        cert_id, note = await _create_ozon_certificate(
            client,
            doc_number=row.doc_number,
            doc_type=doc_type,
            issue_date=issue,
            expire_date=expire,
            pdf_bytes=fsa.pdf_bytes,
            title=doc_type_label(doc_type),
            catalog=cert_catalog,
        )
    except HttpStatusError as e:
        return 0, "error", (
            f"Ozon create (type={type_code}, accordance={accordance_code or '-'}): {str(e)[:220]}"
        )
    if not cert_id:
        return 0, "error", f"Ozon create: {note}"

    cert_cache[doc_key] = cert_id
    if replaced_from:
        return cert_id, "replaced_ozon", f"заменён сертификат {replaced_from} → {cert_id}"
    return cert_id, "created", f"создан сертификат {cert_id}"


async def _create_ozon_certificate(
    client: OzonClient,
    *,
    doc_number: str,
    doc_type: str,
    issue_date: str,
    expire_date: str,
    pdf_bytes: bytes,
    title: str,
    catalog: Optional[_OzonCertCatalog] = None,
) -> Tuple[int, str]:
    if not pdf_bytes:
        return 0, "Пустой PDF"
    if catalog is None:
        catalog = await _load_ozon_cert_catalog(client)

    type_code = _ozon_type_code(doc_type, catalog.doc_types)
    accordance_code = _ozon_accordance_type_code(doc_type, catalog.accordance_types)
    if not type_code:
        return 0, "Не удалось определить type_code (справочник Ozon пуст)"

    safe_name = re.sub(r"[^\w.\-]+", "_", str(doc_number or "doc"))[:80] or "document"
    filename = f"{safe_name}.pdf"
    log.info(
        "ozon certificate create: doc_type=%s type_code=%s accordance=%s number=%s",
        doc_type,
        type_code,
        accordance_code or "-",
        doc_number[:60],
    )
    data = await client.product_certificate_create(
        name=title[:250] or doc_number[:250],
        type_code=type_code,
        number=doc_number,
        issue_date=issue_date,
        expire_date=expire_date,
        accordance_type_code=accordance_code,
        pdf_bytes=pdf_bytes,
        filename=filename,
    )
    cid = _extract_certificate_id(data)
    if cid:
        note = f"created (type={type_code}"
        if accordance_code:
            note += f", accordance={accordance_code}"
        note += ")"
        return cid, note
    return 0, str(data)[:300]


def _fsa_row_status(fsa: Optional[FsaLookupResult], *, preview_if_found: bool = False) -> str:
    if not fsa:
        return "fsa_not_found"
    if fsa.error:
        return "fsa_error"
    if fsa.found:
        return "preview" if preview_if_found else "ok"
    return "fsa_not_found"


async def lookup_fsa_for_rows(
    rows: List[CertInputRow],
    *,
    fetch_pdf: bool = True,
    progress_cb: Optional[ProgressCb] = None,
) -> Dict[str, FsaLookupResult]:
    items = [(r.doc_number, detect_doc_type(r.doc_number)) for r in rows]
    return await lookup_fsa_batch(items, fetch_pdf=fetch_pdf, progress_cb=progress_cb)


async def apply_ozon_certificates_for_store(
    client_id: str,
    api_key: str,
    *,
    rows: List[CertInputRow],
    fsa_by_number: Dict[str, FsaLookupResult],
    dry_run: bool = False,
    fsa_only: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    client = OzonClient(client_id, api_key, timeout_s=90.0)
    offer_ids = [_norm_offer(r.vendor_code) for r in rows]
    product_by_offer = await _map_offers_to_product_ids(client, offer_ids)
    cert_catalog = await _load_ozon_cert_catalog(client)

    results: List[OzonCertRowResult] = []
    cert_cache: Dict[str, int] = {}
    cert_action_by_doc: Dict[str, str] = {}
    total = max(len(rows), 1)
    step = 0

    # Группируем по номеру документа — один create на Ozon, bind на каждый offer_id
    rows_by_doc: Dict[str, List[CertInputRow]] = {}
    for row in rows:
        key = _norm_number(row.doc_number)
        if not key:
            step += 1
            if progress_cb:
                progress_cb(step, total, f"{row.vendor_code}: проверка")
            results.append(
                OzonCertRowResult(
                    vendor_code=row.vendor_code,
                    doc_number=row.doc_number,
                    status="error",
                    message="Пустой номер документа",
                )
            )
            continue
        rows_by_doc.setdefault(key, []).append(row)

    for doc_key, doc_rows in rows_by_doc.items():
        sample = doc_rows[0]
        doc_type = detect_doc_type(sample.doc_number)
        fsa = fsa_by_number.get(doc_key)

        for row in doc_rows:
            step += 1
            if progress_cb:
                progress_cb(step, total, f"{row.vendor_code}: проверка")

            res = OzonCertRowResult(
                vendor_code=row.vendor_code,
                doc_number=row.doc_number,
                doc_type=doc_type,
            )

            if not fsa or fsa.error or not fsa.found:
                res.status = _fsa_row_status(fsa)
                res.message = (fsa.message if fsa else "") or "Документ не найден"
                if fsa:
                    res.error_kind = fsa.error_kind or ""
                results.append(res)
                continue

            res.fsa_found = True
            res.pdf_source = fsa.pdf_source or ""

            if not fsa.pdf_bytes:
                res.status = "no_pdf"
                res.message = "PDF не получен"
                results.append(res)
                continue

            if fsa.pdf_source == "generated":
                res.status = "no_pdf"
                res.message = (
                    "Получена только текстовая заглушка, не официальный документ. "
                    "Повторите «Проверить PDF (ФСА)» или используйте зеркало."
                )
                results.append(res)
                continue

            if fsa_only:
                res.status = "preview"
                res.message = fsa.message or "ФСА OK"
                results.append(res)
                continue

            oid = _norm_offer(row.vendor_code)
            pid = product_by_offer.get(oid, 0)
            res.product_id = pid
            if not pid:
                res.status = "not_found"
                res.message = "Товар не найден в Ozon (offer_id)"
                results.append(res)
                continue

            if dry_run:
                if doc_key not in cert_cache:
                    existing_cert = await _find_ozon_certificate(client, row.doc_number)
                    if existing_cert:
                        existing_cert = await _ensure_ozon_cert_dates(client, existing_cert)
                        try:
                            found = int(existing_cert.get("certificate_id") or existing_cert.get("id") or 0)
                        except (TypeError, ValueError):
                            found = 0
                        if found:
                            cert_cache[doc_key] = found
                existing = cert_cache.get(doc_key, 0)
                n_same = len(doc_rows)
                fsa_issue_key = _date_key(row.reg_date or (fsa.record.reg_date if fsa.record else ""))
                fsa_expire_key = _date_key(row.valid_until or (fsa.record.end_date if fsa.record else ""))
                if existing:
                    res.certificate_id = existing
                    res.status = "preview"
                    existing_cert = await _find_ozon_certificate(client, row.doc_number)
                    needs_replace = bool(
                        existing_cert
                        and _cert_needs_replace(
                            existing_cert,
                            fsa_issue=fsa_issue_key,
                            fsa_expire=fsa_expire_key,
                        )
                    )
                    if needs_replace:
                        res.message = (
                            f"Заменить сертификат {existing} (даты на Ozon не совпадают с ФСА)"
                            + (f" (+{n_same - 1} товар(ов) с тем же номером)" if n_same > 1 else "")
                        )
                    else:
                        res.message = (
                            f"Привязка к существующему сертификату {existing}"
                            + (f" (ещё {n_same - 1} товар(ов) с тем же номером)" if n_same > 1 else "")
                        )
                else:
                    issue = _iso_date(row.reg_date or (fsa.record.reg_date if fsa.record else ""))
                    res.status = "preview"
                    res.message = (
                        f"Создать сертификат и привязать товар; PDF: {fsa.pdf_source}; дата: {issue or '—'}"
                        + (f" (+{n_same - 1} товар(ов) к тому же сертификату)" if n_same > 1 else "")
                    )
                results.append(res)
                continue

            if doc_key not in cert_action_by_doc:
                cert_id, action, action_msg = await _resolve_ozon_certificate_id(
                    client,
                    doc_key=doc_key,
                    row=sample,
                    doc_type=doc_type,
                    fsa=fsa,
                    cert_cache=cert_cache,
                    cert_catalog=cert_catalog,
                )
                cert_action_by_doc[doc_key] = action_msg

            cert_id = cert_cache.get(doc_key, 0)
            if not cert_id:
                res.status = "error"
                res.message = cert_action_by_doc.get(doc_key, "Не удалось получить certificate_id")
                results.append(res)
                continue

            res.certificate_id = cert_id
            try:
                bind_data = await client.product_certificate_bind(
                    certificate_id=cert_id,
                    product_ids=[pid],
                )
                res.status = "ok"
                base = cert_action_by_doc.get(doc_key, f"сертификат {cert_id}")
                res.message = f"{base} → привязан товар {row.vendor_code}"
                if isinstance(bind_data, dict):
                    err = bind_data.get("error") or bind_data.get("message")
                    if err:
                        err_s = str(err).casefold()
                        if "уже" in err_s or "already" in err_s or "привязан" in err_s:
                            res.status = "ok"
                            res.message = f"{base} → товар {row.vendor_code} уже привязан"
                        else:
                            res.status = "error"
                            res.message = str(err)[:250]
            except HttpStatusError as e:
                err_s = str(e).casefold()
                if "уже" in err_s or "already" in err_s or "привязан" in err_s:
                    res.status = "ok"
                    res.message = f"сертификат {cert_id} → товар {row.vendor_code} уже привязан"
                else:
                    res.status = "error"
                    res.message = f"Ozon bind: {str(e)[:250]}"

            results.append(res)

    prepared = sum(1 for r in results if r.status in ("ok", "preview"))
    bound = sum(1 for r in results if r.status == "ok")
    return {
        "parsed": len(rows),
        "products_found": sum(1 for r in results if r.product_id),
        "prepared": prepared,
        "bound": bound,
        "rows": [r.__dict__ for r in results],
    }


async def apply_ozon_certificates_multi_store(
    stores: List[Tuple[int, str, str, str]],
    *,
    rows: List[CertInputRow],
    dry_run: bool = False,
    fsa_only: bool = False,
    pdf_source: str = "fsa",
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """stores: (store_id, store_name, client_id, api_key). pdf_source: fsa | mirror."""
    fsa_items = [(r.doc_number, detect_doc_type(r.doc_number)) for r in rows]
    unique_docs = max(len({_norm_number(r.doc_number) for r in rows if _norm_number(r.doc_number)}), 1)
    source = str(pdf_source or "fsa").strip().casefold()
    if source not in ("fsa", "mirror"):
        source = "fsa"
    source_label = "ФСА" if source == "fsa" else "зеркало"

    total_steps = unique_docs + (0 if fsa_only else len(stores) * max(len(rows), 1))

    def _lookup_progress(cur: int, tot: int, detail: str) -> None:
        if progress_cb:
            progress_cb(cur, total_steps, f"{source_label} {cur}/{tot}: {detail}")

    if source == "mirror":
        fsa_by_number = await lookup_mirror_batch(
            fsa_items,
            fetch_pdf=not fsa_only,
            progress_cb=_lookup_progress,
        )
    else:
        fsa_by_number = await lookup_fsa_batch(
            fsa_items,
            fetch_pdf=not fsa_only,
            progress_cb=_lookup_progress,
        )

    if fsa_only:
        row_results = []
        for row in rows:
            fsa = fsa_by_number.get(_norm_number(row.doc_number))
            row_results.append({
                "vendor_code": row.vendor_code,
                "doc_number": row.doc_number,
                "doc_type": detect_doc_type(row.doc_number),
                "status": _fsa_row_status(fsa, preview_if_found=True),
                "fsa_found": bool(fsa and fsa.found),
                "error_kind": (fsa.error_kind if fsa else "") or "",
                "pdf_source": (fsa.pdf_source if fsa else "") or "",
                "message": (fsa.message if fsa else f"Не найдено ({source_label})"),
                "product_names": (fsa.record.product_names[:5] if fsa and fsa.record else []),
                "view_url": (fsa.record.view_url if fsa and fsa.record else ""),
            })
        return {
            "fsa_only": True,
            "pdf_source": source,
            "fsa_checked": len(fsa_by_number),
            "fsa": fsa_results_to_api(fsa_by_number),
            "stores": [{
                "store_id": 0,
                "store_name": source_label,
                "parsed": len(rows),
                "prepared": sum(1 for r in row_results if r["fsa_found"]),
                "rows": row_results,
            }],
        }

    out_stores = []
    base_done = unique_docs
    for si, (store_id, store_name, client_id, api_key) in enumerate(stores):
        store_offset = base_done + si * len(rows)

        def _store_progress(cur: int, tot: int, detail: str, _off=store_offset, _name=store_name) -> None:
            if progress_cb:
                progress_cb(_off + cur, total_steps, f"{_name}: {detail}")

        try:
            part = await apply_ozon_certificates_for_store(
                client_id,
                api_key,
                rows=rows,
                fsa_by_number=fsa_by_number,
                dry_run=dry_run,
                fsa_only=False,
                progress_cb=_store_progress,
            )
            part["store_id"] = store_id
            part["store_name"] = store_name
            out_stores.append(part)
        except Exception as e:
            log.exception("ozon certificates store %s: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:300],
                "rows": [],
            })

    return {
        "pdf_source": source,
        "fsa_checked": len(fsa_by_number),
        "fsa": fsa_results_to_api(fsa_by_number),
        "stores": out_stores,
    }


def fsa_results_to_api(fsa_by_number: Dict[str, FsaLookupResult]) -> List[dict]:
    out = []
    for num, fsa in fsa_by_number.items():
        rec = fsa.record
        out.append({
            "doc_number": num,
            "doc_type": fsa.doc_type,
            "found": fsa.found,
            "error": fsa.error,
            "error_kind": fsa.error_kind,
            "message": fsa.message,
            "pdf_source": fsa.pdf_source,
            "pdf_size": len(fsa.pdf_bytes) if fsa.pdf_bytes else 0,
            "reg_date": rec.reg_date if rec else "",
            "end_date": rec.end_date if rec else "",
            "product_names": rec.product_names[:5] if rec else [],
            "view_url": rec.view_url if rec else "",
        })
    return out
