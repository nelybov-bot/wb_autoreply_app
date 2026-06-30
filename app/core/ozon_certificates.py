"""Ozon: декларации/сертификаты — ФСА → PDF → create/bind."""
from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .compliance_docs import CertInputRow, detect_doc_type, doc_type_label
from .fsa_registry import FsaLookupResult, _norm_number, lookup_fsa_batch
from .net import HttpStatusError
from .ozon_client import OzonClient

log = logging.getLogger("ozon.certificates")

ProgressCb = Callable[[int, int, str], None]


@dataclass
class OzonCertRowResult:
    vendor_code: str
    doc_number: str
    doc_type: str = "unknown"
    product_id: int = 0
    status: str = "pending"
    message: str = ""
    fsa_found: bool = False
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


def _ozon_type_code(doc_type: str) -> str:
    if doc_type == "certificate":
        return "CERTIFICATE"
    return "DECLARATION"


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


async def _find_ozon_certificate_id(
    client: OzonClient,
    doc_number: str,
) -> int:
    target = re.sub(r"\s+", "", str(doc_number or "").casefold())
    if not target:
        return 0
    for page in range(1, 6):
        certs = await client.product_certificate_list(page=page, page_size=100)
        if not certs:
            break
        for c in certs:
            num = re.sub(r"\s+", "", str(c.get("number") or c.get("certificate_number") or "").casefold())
            if num and (num == target or target in num or num in target):
                try:
                    return int(c.get("certificate_id") or c.get("id") or 0)
                except (TypeError, ValueError):
                    continue
    return 0


async def _create_ozon_certificate(
    client: OzonClient,
    *,
    doc_number: str,
    doc_type: str,
    issue_date: str,
    pdf_bytes: bytes,
    title: str,
) -> Tuple[int, str]:
    b64_pdf = base64.standard_b64encode(pdf_bytes).decode("ascii")
    payload = {
        "name": title[:250] or doc_number[:250],
        "type_code": _ozon_type_code(doc_type),
        "number": doc_number,
        "issue_date": issue_date,
        "files": [b64_pdf],
    }
    data = await client.product_certificate_create(payload)
    cid = _extract_certificate_id(data)
    if cid:
        return cid, "created"
    return 0, str(data)[:300]


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

    results: List[OzonCertRowResult] = []
    cert_cache: Dict[str, int] = {}
    total = max(len(rows), 1)
    step = 0

    # группировка по номеру документа для create (кэш cert_cache)
    for row in rows:
        step += 1
        if progress_cb:
            progress_cb(step, total, f"{row.vendor_code}: проверка")

        doc_type = detect_doc_type(row.doc_number)
        fsa_key = _norm_number(row.doc_number)
        fsa = fsa_by_number.get(fsa_key)

        res = OzonCertRowResult(
            vendor_code=row.vendor_code,
            doc_number=row.doc_number,
            doc_type=doc_type,
        )

        if not fsa or not fsa.found:
            res.status = "fsa_not_found"
            res.message = (fsa.message if fsa else "") or "Не найдено в ФСА"
            results.append(res)
            continue

        res.fsa_found = True
        res.pdf_source = fsa.pdf_source or ""

        if not fsa.pdf_bytes:
            res.status = "no_pdf"
            res.message = "PDF не получен из реестра"
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

        doc_key = fsa_key
        cert_id = cert_cache.get(doc_key, 0)

        if dry_run:
            issue = _iso_date(row.reg_date or (fsa.record.reg_date if fsa.record else ""))
            res.status = "preview"
            res.message = (
                f"ФСА: {fsa.message}; товар product_id={pid}; "
                f"PDF: {fsa.pdf_source}; дата: {issue or '—'}"
            )
            results.append(res)
            continue

        if not cert_id:
            existing = await _find_ozon_certificate_id(client, row.doc_number)
            if existing:
                cert_id = existing
                cert_cache[doc_key] = cert_id
            else:
                issue = _iso_date(
                    row.reg_date
                    or (fsa.record.reg_date if fsa.record else "")
                )
                if not issue:
                    res.status = "error"
                    res.message = "Нет даты регистрации для Ozon"
                    results.append(res)
                    continue
                title = doc_type_label(doc_type)
                try:
                    cert_id, note = await _create_ozon_certificate(
                        client,
                        doc_number=row.doc_number,
                        doc_type=doc_type,
                        issue_date=issue,
                        pdf_bytes=fsa.pdf_bytes,
                        title=title,
                    )
                except HttpStatusError as e:
                    res.status = "error"
                    res.message = f"Ozon create: {str(e)[:250]}"
                    results.append(res)
                    continue
                if not cert_id:
                    res.status = "error"
                    res.message = f"Ozon create: {note}"
                    results.append(res)
                    continue
                cert_cache[doc_key] = cert_id

        res.certificate_id = cert_id
        try:
            bind_data = await client.product_certificate_bind(
                certificate_id=cert_id,
                product_ids=[pid],
            )
            res.status = "ok"
            res.message = f"Привязано к сертификату {cert_id}"
            if isinstance(bind_data, dict):
                err = bind_data.get("error") or bind_data.get("message")
                if err:
                    res.status = "error"
                    res.message = str(err)[:250]
        except HttpStatusError as e:
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
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """stores: (store_id, store_name, client_id, api_key)."""
    fsa_items = [(r.doc_number, detect_doc_type(r.doc_number)) for r in rows]
    unique_docs = max(len({ _norm_number(r.doc_number) for r in rows if _norm_number(r.doc_number) }), 1)

    total_steps = unique_docs + (0 if fsa_only else len(stores) * max(len(rows), 1))

    def _fsa_progress(cur: int, tot: int, detail: str) -> None:
        if progress_cb:
            progress_cb(cur, total_steps, f"ФСА {cur}/{tot}: {detail}")

    fsa_by_number = await lookup_fsa_batch(
        fsa_items,
        fetch_pdf=not fsa_only,
        progress_cb=_fsa_progress,
    )

    if fsa_only:
        row_results = []
        for row in rows:
            fsa = fsa_by_number.get(_norm_number(row.doc_number))
            row_results.append({
                "vendor_code": row.vendor_code,
                "doc_number": row.doc_number,
                "doc_type": detect_doc_type(row.doc_number),
                "status": "preview" if fsa and fsa.found else "fsa_not_found",
                "fsa_found": bool(fsa and fsa.found),
                "pdf_source": (fsa.pdf_source if fsa else "") or "",
                "message": (fsa.message if fsa else "Не найдено в ФСА"),
                "product_names": (fsa.record.product_names[:5] if fsa and fsa.record else []),
                "view_url": (fsa.record.view_url if fsa and fsa.record else ""),
            })
        return {
            "fsa_only": True,
            "fsa_checked": len(fsa_by_number),
            "fsa": fsa_results_to_api(fsa_by_number),
            "stores": [{
                "store_id": 0,
                "store_name": "ФСА",
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
            "message": fsa.message,
            "pdf_source": fsa.pdf_source,
            "pdf_size": len(fsa.pdf_bytes) if fsa.pdf_bytes else 0,
            "reg_date": rec.reg_date if rec else "",
            "end_date": rec.end_date if rec else "",
            "product_names": rec.product_names[:5] if rec else [],
            "view_url": rec.view_url if rec else "",
        })
    return out
