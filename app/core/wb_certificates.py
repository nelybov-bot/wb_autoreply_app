"""Заполнение сертификатов/деклараций WB по артикулу продавца (cards/list + cards/update)."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .compliance_docs import (
    CertInputRow,
    detect_doc_type,
    doc_type_label,
    filter_cert_rows,
    parse_certificates_file,
    parse_certificates_text,
    _norm_vendor,
)
from .net import HttpStatusError
from .wb_content_client import WbContentClient

log = logging.getLogger("wb.certificates")

ProgressCb = Callable[[int, int, str], None]

_RE_DOC_NUMBER = re.compile(
    r"номер.*(сертификат|декларац)|(сертификат|декларац).*номер|"
    r"регистрационн.*номер|номер.*документ|документ.*номер",
    re.I,
)
_RE_DECL_NUMBER = re.compile(r"номер.*декларац|декларац.*номер", re.I)
_RE_CERT_NUMBER = re.compile(r"номер.*сертификат|сертификат.*номер", re.I)
_RE_REG_DATE = re.compile(
    r"дата.*регистрац|регистрац.*дата|дата.*начала|начало.*действ|действует\s*от",
    re.I,
)
_RE_DECL_REG_DATE = re.compile(
    r"декларац.*(дата|регистрац|действует\s*от)|(дата|регистрац|действует\s*от).*декларац",
    re.I,
)
_RE_CERT_REG_DATE = re.compile(
    r"сертификат.*(дата|регистрац|действует\s*от)|(дата|регистрац|действует\s*от).*сертификат",
    re.I,
)
_RE_VALID_UNTIL = re.compile(
    r"действует.*до|дата.*окончан|окончан.*действ|срок.*действ|конец.*действ",
    re.I,
)
_RE_DECL_VALID = re.compile(
    r"декларац.*(действует\s*до|окончан|срок)|(действует\s*до|окончан|срок).*декларац",
    re.I,
)
_RE_CERT_VALID = re.compile(
    r"сертификат.*(действует\s*до|окончан|срок)|(действует\s*до|окончан|срок).*сертификат",
    re.I,
)


@dataclass
class CertFieldMap:
    subject_id: int
    decl_number_id: Optional[int] = None
    decl_reg_date_id: Optional[int] = None
    decl_valid_until_id: Optional[int] = None
    cert_number_id: Optional[int] = None
    cert_reg_date_id: Optional[int] = None
    cert_valid_until_id: Optional[int] = None
    generic_number_id: Optional[int] = None
    generic_reg_date_id: Optional[int] = None
    generic_valid_until_id: Optional[int] = None
    # имена полей (для отчёта)
    decl_number_name: str = ""
    cert_number_name: str = ""
    generic_number_name: str = ""

    def ok(self) -> bool:
        return bool(
            self.decl_number_id or self.cert_number_id or self.generic_number_id
            or self.decl_reg_date_id or self.cert_reg_date_id or self.generic_reg_date_id
            or self.decl_valid_until_id or self.cert_valid_until_id or self.generic_valid_until_id
        )


@dataclass
class CertApplyRowResult:
    vendor_code: str
    nm_id: int = 0
    status: str = "pending"  # ok | skipped | error | not_found | no_fields
    message: str = ""
    doc_type: str = ""
    mapped_fields: Optional[List[str]] = None


def _charc_id(ch: dict) -> int:
    for key in ("charcID", "charcId", "id"):
        try:
            val = int(ch.get(key) or 0)
        except (TypeError, ValueError):
            val = 0
        if val:
            return val
    return 0


def _charc_name(ch: dict) -> str:
    return str(ch.get("name") or "").strip()


def _match_charc(ch: dict, pattern: re.Pattern[str]) -> bool:
    return bool(pattern.search(_charc_name(ch)))


def _map_fields_from_charcs(charcs: List[dict]) -> CertFieldMap:
    subject_id = 0
    for ch in charcs:
        try:
            subject_id = int(ch.get("subjectID") or ch.get("subjectId") or 0)
        except (TypeError, ValueError):
            subject_id = 0
        if subject_id:
            break
    m = CertFieldMap(subject_id=subject_id)
    for ch in charcs:
        if not isinstance(ch, dict):
            continue
        cid = _charc_id(ch)
        name = _charc_name(ch)
        if not cid:
            continue
        if _RE_DECL_NUMBER.search(name):
            m.decl_number_id = cid
            m.decl_number_name = name
        elif _RE_CERT_NUMBER.search(name):
            m.cert_number_id = cid
            m.cert_number_name = name
        elif _RE_DOC_NUMBER.search(name) and not m.generic_number_id:
            m.generic_number_id = cid
            m.generic_number_name = name
        if _RE_DECL_REG_DATE.search(name):
            m.decl_reg_date_id = cid
        elif _RE_CERT_REG_DATE.search(name):
            m.cert_reg_date_id = cid
        elif _RE_REG_DATE.search(name) and not m.generic_reg_date_id:
            m.generic_reg_date_id = cid
        if _RE_DECL_VALID.search(name):
            m.decl_valid_until_id = cid
        elif _RE_CERT_VALID.search(name):
            m.cert_valid_until_id = cid
        elif _RE_VALID_UNTIL.search(name) and not m.generic_valid_until_id:
            m.generic_valid_until_id = cid
    return m


def _map_fields_from_card(card: dict) -> CertFieldMap:
    try:
        subject_id = int(card.get("subjectID") or card.get("subjectId") or 0)
    except (TypeError, ValueError):
        subject_id = 0
    m = CertFieldMap(subject_id=subject_id)
    for ch in card.get("characteristics") or []:
        if not isinstance(ch, dict):
            continue
        cid = _charc_id(ch)
        name = _charc_name(ch)
        if not cid:
            continue
        if _RE_DECL_NUMBER.search(name):
            m.decl_number_id = cid
            m.decl_number_name = name
        elif _RE_CERT_NUMBER.search(name):
            m.cert_number_id = cid
            m.cert_number_name = name
        elif _RE_DOC_NUMBER.search(name) and not m.generic_number_id:
            m.generic_number_id = cid
            m.generic_number_name = name
        if _RE_DECL_REG_DATE.search(name):
            m.decl_reg_date_id = cid
        elif _RE_CERT_REG_DATE.search(name):
            m.cert_reg_date_id = cid
        elif _RE_REG_DATE.search(name) and not m.generic_reg_date_id:
            m.generic_reg_date_id = cid
        if _RE_DECL_VALID.search(name):
            m.decl_valid_until_id = cid
        elif _RE_CERT_VALID.search(name):
            m.cert_valid_until_id = cid
        elif _RE_VALID_UNTIL.search(name) and not m.generic_valid_until_id:
            m.generic_valid_until_id = cid
    return m


def _empty_char_value(existing: Any) -> Any:
    if isinstance(existing, list):
        return []
    return ""


def _build_cert_patch_ids(row: CertInputRow, fmap: CertFieldMap) -> Tuple[Dict[int, Any], List[str]]:
    """Патч characteristics: правильные поля по типу документа + очистка «чужого» типа."""
    doc_number = str(row.doc_number or "").strip()
    doc_type = detect_doc_type(doc_number)
    patches: Dict[int, Any] = {}
    mapped: List[str] = []

    def _set(cid: Optional[int], val: str, label: str) -> None:
        if cid and val:
            patches[cid] = val
            mapped.append(label)

    def _clear(cid: Optional[int], label: str) -> None:
        if cid:
            patches[cid] = ""
            mapped.append(f"очистка: {label}")

    def _generic_ok_for_decl() -> bool:
        name = (fmap.generic_number_name or "").casefold()
        if not fmap.generic_number_id:
            return False
        if "декларац" in name and "сертификат" in name:
            return True
        return "декларац" in name or "сертификат" not in name

    def _generic_ok_for_cert() -> bool:
        name = (fmap.generic_number_name or "").casefold()
        if not fmap.generic_number_id:
            return False
        if "декларац" in name and "сертификат" in name:
            return True
        return "сертификат" in name or "декларац" not in name

    if doc_type == "declaration":
        if fmap.decl_number_id:
            _set(fmap.decl_number_id, doc_number, f"декларация номер ({fmap.decl_number_name or fmap.decl_number_id})")
        elif _generic_ok_for_decl():
            _set(fmap.generic_number_id, doc_number, f"номер ({fmap.generic_number_name or fmap.generic_number_id})")
        elif fmap.decl_number_id is None and fmap.generic_number_id and not _generic_ok_for_decl():
            mapped.append(
                f"⚠ нет поля декларации; «{fmap.generic_number_name}» похоже на сертификат — номер не записан"
            )
        if fmap.decl_reg_date_id:
            _set(fmap.decl_reg_date_id, row.reg_date, "декларация дата рег.")
        elif fmap.generic_reg_date_id:
            _set(fmap.generic_reg_date_id, row.reg_date, "дата рег.")
        if fmap.decl_valid_until_id:
            _set(fmap.decl_valid_until_id, row.valid_until, "декларация действует до")
        elif fmap.generic_valid_until_id:
            _set(fmap.generic_valid_until_id, row.valid_until, "действует до")
        _clear(fmap.cert_number_id, "номер сертификата")
        _clear(fmap.cert_reg_date_id, "дата сертификата")
        _clear(fmap.cert_valid_until_id, "срок сертификата")
    elif doc_type == "certificate":
        if fmap.cert_number_id:
            _set(fmap.cert_number_id, doc_number, f"сертификат номер ({fmap.cert_number_name or fmap.cert_number_id})")
        elif _generic_ok_for_cert():
            _set(fmap.generic_number_id, doc_number, f"номер ({fmap.generic_number_name or fmap.generic_number_id})")
        if fmap.cert_reg_date_id:
            _set(fmap.cert_reg_date_id, row.reg_date, "сертификат дата рег.")
        elif fmap.generic_reg_date_id:
            _set(fmap.generic_reg_date_id, row.reg_date, "дата рег.")
        if fmap.cert_valid_until_id:
            _set(fmap.cert_valid_until_id, row.valid_until, "сертификат действует до")
        elif fmap.generic_valid_until_id:
            _set(fmap.generic_valid_until_id, row.valid_until, "действует до")
        _clear(fmap.decl_number_id, "номер декларации")
        _clear(fmap.decl_reg_date_id, "дата декларации")
        _clear(fmap.decl_valid_until_id, "срок декларации")
    else:
        cid = fmap.generic_number_id or fmap.decl_number_id or fmap.cert_number_id
        _set(cid, doc_number, "номер (тип не определён)")
        rid = fmap.generic_reg_date_id or fmap.decl_reg_date_id or fmap.cert_reg_date_id
        _set(rid, row.reg_date, "дата рег.")
        uid = fmap.generic_valid_until_id or fmap.decl_valid_until_id or fmap.cert_valid_until_id
        _set(uid, row.valid_until, "действует до")

    return patches, mapped


def _value_nonempty(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, list):
        return any(str(x).strip() for x in val)
    return bool(str(val).strip())


def _char_value(val: str, existing: Any) -> Any:
    if isinstance(existing, list):
        return [val] if val else (existing if existing is not None else [])
    return val


def _format_wb_error(e: HttpStatusError) -> str:
    body = (e.body or "").strip()
    if not body:
        return f"Ошибка WB {e.status}"
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return f"Ошибка WB {e.status}: {body[:350]}"
    if isinstance(data, dict):
        parts: List[str] = []
        for key in ("errorText", "message", "detail", "title"):
            v = data.get(key)
            if v:
                parts.append(str(v))
        extra = data.get("additionalErrors") or data.get("errors")
        if isinstance(extra, list):
            parts.extend(str(x) for x in extra[:4])
        elif extra:
            parts.append(str(extra))
        if parts:
            return f"Ошибка WB {e.status}: {'; '.join(parts)}"[:400]
    return f"Ошибка WB {e.status}: {body[:350]}"


def _normalize_sizes(card: dict) -> List[dict]:
    out: List[dict] = []
    for sz in card.get("sizes") or []:
        if not isinstance(sz, dict):
            continue
        item: Dict[str, Any] = {}
        if sz.get("chrtID") is not None:
            try:
                item["chrtID"] = int(sz["chrtID"])
            except (TypeError, ValueError):
                pass
        if sz.get("techSize") is not None:
            item["techSize"] = str(sz["techSize"])
        if sz.get("wbSize") is not None:
            item["wbSize"] = str(sz["wbSize"])
        skus = [str(x).strip() for x in (sz.get("skus") or []) if str(x).strip()]
        if skus:
            item["skus"] = skus
        if item.get("chrtID") or item.get("skus"):
            out.append(item)
    return out


def build_card_update_payload(
    card: dict,
    row: CertInputRow,
    fmap: CertFieldMap,
    *,
    patch_ids: Optional[Dict[int, Any]] = None,
) -> dict:
    """Собирает тело cards/update с сохранением остальных полей карточки."""
    if patch_ids is None:
        patch_ids, _ = _build_cert_patch_ids(row, fmap)
    nm = int(card.get("nmID") or card.get("nmId") or 0)
    payload: Dict[str, Any] = {
        "nmID": nm,
        "vendorCode": str(card.get("vendorCode") or card.get("supplierVendorCode") or row.vendor_code),
        "brand": str(card.get("brand") or ""),
        "title": str(card.get("title") or ""),
        "description": str(card.get("description") or ""),
    }
    if card.get("kizMarked") is not None:
        payload["kizMarked"] = bool(card.get("kizMarked"))

    dims = card.get("dimensions")
    if isinstance(dims, dict) and dims:
        payload["dimensions"] = {
            k: dims[k]
            for k in ("length", "width", "height", "weightBrutto", "isValid")
            if dims.get(k) is not None
        }

    patch_ids = dict(patch_ids or {})
    chars_out: List[dict] = []
    seen: Set[int] = set()
    for ch in card.get("characteristics") or []:
        if not isinstance(ch, dict):
            continue
        cid = _charc_id(ch)
        if not cid:
            continue
        val = ch.get("value")
        if cid in patch_ids:
            pval = patch_ids[cid]
            if pval == "" or pval is None:
                val = _empty_char_value(val)
            elif pval:
                val = _char_value(str(pval), val)
        chars_out.append({"id": cid, "value": val})
        seen.add(cid)

    for cid, pval in patch_ids.items():
        if not cid or cid in seen:
            continue
        if pval == "" or pval is None:
            chars_out.append({"id": cid, "value": ""})
        elif pval:
            chars_out.append({"id": cid, "value": pval})

    payload["characteristics"] = chars_out
    payload["sizes"] = _normalize_sizes(card)
    return payload


def build_card_char_patches_payload(
    card: dict,
    patches: Dict[int, Any],
    *,
    vendor_code: str = "",
) -> dict:
    """cards/update с подстановкой произвольных characteristics по id."""
    nm = int(card.get("nmID") or card.get("nmId") or 0)
    vc = str(
        card.get("vendorCode")
        or card.get("supplierVendorCode")
        or vendor_code
        or ""
    )
    payload: Dict[str, Any] = {
        "nmID": nm,
        "vendorCode": vc,
        "brand": str(card.get("brand") or ""),
        "title": str(card.get("title") or ""),
        "description": str(card.get("description") or ""),
    }
    if card.get("kizMarked") is not None:
        payload["kizMarked"] = bool(card.get("kizMarked"))

    dims = card.get("dimensions")
    if isinstance(dims, dict) and dims:
        payload["dimensions"] = {
            k: dims[k]
            for k in ("length", "width", "height", "weightBrutto", "isValid")
            if dims.get(k) is not None
        }

    chars_out: List[dict] = []
    seen: Set[int] = set()
    for ch in card.get("characteristics") or []:
        if not isinstance(ch, dict):
            continue
        cid = _charc_id(ch)
        if not cid:
            continue
        val = ch.get("value")
        if cid in patches and patches[cid] is not None and _value_nonempty(patches[cid]):
            val = _char_value(patches[cid], val)
        chars_out.append({"id": cid, "value": val})
        seen.add(cid)

    for cid, pval in patches.items():
        if cid and cid not in seen and pval is not None and _value_nonempty(pval):
            chars_out.append({"id": int(cid), "value": pval})

    payload["characteristics"] = chars_out
    payload["sizes"] = _normalize_sizes(card)
    return payload


async def _load_field_maps(
    client: WbContentClient,
    cards: List[dict],
    cache: Dict[int, CertFieldMap],
) -> None:
    subjects: Set[int] = set()
    for card in cards:
        try:
            sid = int(card.get("subjectID") or card.get("subjectId") or 0)
        except (TypeError, ValueError):
            sid = 0
        if sid:
            subjects.add(sid)
    for sid in sorted(subjects):
        if sid in cache:
            continue
        try:
            charcs = await client.get_subject_charcs(sid)
            fmap = _map_fields_from_charcs(charcs)
            if not fmap.ok():
                for card in cards:
                    try:
                        cs = int(card.get("subjectID") or card.get("subjectId") or 0)
                    except (TypeError, ValueError):
                        cs = 0
                    if cs == sid:
                        fmap = _map_fields_from_card(card)
                        if fmap.ok():
                            break
            cache[sid] = fmap
        except HttpStatusError as e:
            log.warning("WB charcs subject %s: %s", sid, (e.body or "")[:200])
            cache[sid] = CertFieldMap(subject_id=sid)
        await asyncio.sleep(0.65)


async def apply_certificates_for_store(
    api_key: str,
    *,
    rows: List[CertInputRow],
    dry_run: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """Сопоставляет артикулы с карточками WB и обновляет поля сертификата."""
    client = WbContentClient(api_key, timeout_s=120.0)
    vendor_codes = list(dict.fromkeys(r.vendor_code for r in rows if r.vendor_code))
    by_vendor = {_norm_vendor(r.vendor_code).casefold(): r for r in rows}

    if progress_cb:
        progress_cb(0, max(len(vendor_codes), 1), "Загрузка карточек WB…")

    cards = await client.list_cards_all(vendor_codes=vendor_codes, max_pages=max(10, len(vendor_codes) // 50 + 5))
    card_by_vendor: Dict[str, dict] = {}
    for card in cards:
        vc = _norm_vendor(card.get("vendorCode") or card.get("supplierVendorCode") or "").casefold()
        if vc and vc not in card_by_vendor:
            card_by_vendor[vc] = card

    field_cache: Dict[int, CertFieldMap] = {}
    await _load_field_maps(client, cards, field_cache)

    results: List[CertApplyRowResult] = []
    updates: List[dict] = []
    pending_results: List[CertApplyRowResult] = []
    total = len(rows)
    done = 0

    for row in rows:
        done += 1
        key = _norm_vendor(row.vendor_code).casefold()
        card = card_by_vendor.get(key)
        if not card:
            results.append(CertApplyRowResult(
                vendor_code=row.vendor_code,
                status="not_found",
                message="Артикул не найден в каталоге магазина",
            ))
            if progress_cb:
                progress_cb(done, total, f"Не найден: {row.vendor_code}")
            continue

        nm = int(card.get("nmID") or card.get("nmId") or 0)
        try:
            sid = int(card.get("subjectID") or card.get("subjectId") or 0)
        except (TypeError, ValueError):
            sid = 0
        fmap = field_cache.get(sid) or _map_fields_from_card(card)
        if not fmap.ok():
            results.append(CertApplyRowResult(
                vendor_code=row.vendor_code,
                nm_id=nm,
                status="no_fields",
                message="В категории не найдены поля сертификата/декларации",
            ))
            if progress_cb:
                progress_cb(done, total, f"Нет полей: {row.vendor_code}")
            continue

        patch_ids, mapped = _build_cert_patch_ids(row, fmap)
        doc_type = detect_doc_type(row.doc_number)
        payload = build_card_update_payload(card, row, fmap, patch_ids=patch_ids)
        if not payload.get("sizes"):
            results.append(CertApplyRowResult(
                vendor_code=row.vendor_code,
                nm_id=nm,
                status="error",
                message="В карточке нет размеров (chrtID/skus) для обновления",
            ))
            if progress_cb:
                progress_cb(done, total, f"Нет sizes: {row.vendor_code}")
            continue

        updates.append(payload)
        preview_msg = "Будет отправлено"
        if dry_run:
            parts = [doc_type_label(doc_type)]
            if mapped:
                parts.append("; ".join(mapped))
            preview_msg = " · ".join(parts)

        res_row = CertApplyRowResult(
            vendor_code=row.vendor_code,
            nm_id=nm,
            status="ok" if not dry_run else "preview",
            message=preview_msg if dry_run else "Отправка…",
            doc_type=doc_type,
            mapped_fields=mapped,
        )
        pending_results.append(res_row)
        results.append(res_row)
        if progress_cb:
            progress_cb(done, total, f"Подготовлено: {row.vendor_code}")

    sent = 0
    errors: List[dict] = []
    if not dry_run and updates:
        for i, (payload, res) in enumerate(zip(updates, pending_results)):
            if progress_cb:
                progress_cb(total, total, f"Отправка на WB: {res.vendor_code} ({i + 1}/{len(updates)})")
            try:
                await client.update_cards([payload])
                sent += 1
                res.message = "Отправлено"
            except HttpStatusError as e:
                msg = _format_wb_error(e)
                errors.append({
                    "vendor_code": res.vendor_code,
                    "nm_id": res.nm_id,
                    "status": e.status,
                    "body": (e.body or "")[:500],
                })
                res.status = "error"
                res.message = msg
                log.warning("WB cards/update %s nm=%s: %s", res.vendor_code, res.nm_id, msg)
            if i + 1 < len(updates):
                await asyncio.sleep(6.5)

    ok_n = sum(1 for r in results if r.status in ("ok", "preview"))
    return {
        "dry_run": dry_run,
        "parsed": len(rows),
        "cards_found": len(card_by_vendor),
        "prepared": len(updates),
        "sent": sent,
        "errors": errors,
        "rows": [
            {
                "vendor_code": r.vendor_code,
                "nm_id": r.nm_id,
                "status": r.status,
                "message": r.message,
                "doc_type": r.doc_type,
                "mapped_fields": r.mapped_fields or [],
            }
            for r in results
        ],
    }


async def apply_certificates_multi_store(
    stores: List[Tuple[int, str, str]],
    *,
    rows: List[CertInputRow],
    dry_run: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """stores: (store_id, store_name, api_key)."""
    out_stores: List[dict] = []
    total_stores = len(stores)
    row_total = max(len(rows), 1)
    grand_total = max(total_stores * row_total, 1)

    for i, (store_id, store_name, api_key) in enumerate(stores):
        store_offset = i * row_total

        def _cb(
            cur: int,
            tot: int,
            detail: str,
            _offset=store_offset,
            _name=store_name,
            _si=i,
        ) -> None:
            if progress_cb:
                safe_tot = max(int(tot or 0), 1)
                safe_cur = max(0, min(int(cur or 0), safe_tot))
                progress_cb(
                    _offset + safe_cur,
                    grand_total,
                    f"Магазин {_si + 1}/{total_stores} · {_name}: {detail}",
                )

        if progress_cb:
            progress_cb(
                store_offset,
                grand_total,
                f"Магазин {i + 1}/{total_stores}: {store_name}…",
            )

        try:
            part = await apply_certificates_for_store(
                api_key,
                rows=rows,
                dry_run=dry_run,
                progress_cb=_cb if progress_cb else None,
            )
            part["store_id"] = store_id
            part["store_name"] = store_name
            out_stores.append(part)
        except HttpStatusError as e:
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e.body or e)[:400],
                "rows": [],
            })
        except Exception as e:
            log.exception("wb certificates store %s: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:400],
                "rows": [],
            })

    return {"stores": out_stores}
