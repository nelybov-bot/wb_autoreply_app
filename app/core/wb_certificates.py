"""Заполнение сертификатов/деклараций WB по артикулу продавца (cards/list + cards/update)."""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .compliance_docs import (
    CertInputRow,
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
_RE_REG_DATE = re.compile(
    r"дата.*регистрац|регистрац.*дата|дата.*начала|начало.*действ",
    re.I,
)
_RE_VALID_UNTIL = re.compile(
    r"действует.*до|дата.*окончан|окончан.*действ|срок.*действ|конец.*действ",
    re.I,
)


@dataclass
class CertFieldMap:
    subject_id: int
    number_id: Optional[int] = None
    reg_date_id: Optional[int] = None
    valid_until_id: Optional[int] = None
    number_named: Optional[str] = None
    reg_date_named: Optional[str] = None
    valid_until_named: Optional[str] = None

    def ok(self) -> bool:
        return bool(
            self.number_id or self.number_named
            or self.reg_date_id or self.reg_date_named
            or self.valid_until_id or self.valid_until_named
        )


@dataclass
class CertApplyRowResult:
    vendor_code: str
    nm_id: int = 0
    status: str = "pending"  # ok | skipped | error | not_found | no_fields
    message: str = ""


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
        named = bool(ch.get("existNamedField"))
        if _match_charc(ch, _RE_DOC_NUMBER):
            if named and name:
                m.number_named = name
            elif cid:
                m.number_id = cid
        elif _match_charc(ch, _RE_REG_DATE):
            if named and name:
                m.reg_date_named = name
            elif cid:
                m.reg_date_id = cid
        elif _match_charc(ch, _RE_VALID_UNTIL):
            if named and name:
                m.valid_until_named = name
            elif cid:
                m.valid_until_id = cid
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
        if _match_charc(ch, _RE_DOC_NUMBER) and cid:
            m.number_id = cid
        elif _match_charc(ch, _RE_REG_DATE) and cid:
            m.reg_date_id = cid
        elif _match_charc(ch, _RE_VALID_UNTIL) and cid:
            m.valid_until_id = cid
        elif name and _match_charc(ch, _RE_DOC_NUMBER):
            m.number_named = name
    return m


def _value_nonempty(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, list):
        return any(str(x).strip() for x in val)
    return bool(str(val).strip())


def _char_value(val: str, existing: Any) -> Any:
    if isinstance(existing, list):
        return [val] if val else []
    return val


def build_card_update_payload(card: dict, row: CertInputRow, fmap: CertFieldMap) -> dict:
    """Собирает тело cards/update с сохранением остальных полей карточки."""
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
    if isinstance(dims, dict):
        payload["dimensions"] = {
            k: dims[k]
            for k in ("length", "width", "height", "weightBrutto")
            if dims.get(k) is not None
        }

    patch_ids = {
        fmap.number_id: row.doc_number,
        fmap.reg_date_id: row.reg_date,
        fmap.valid_until_id: row.valid_until,
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
        if cid in patch_ids and patch_ids[cid]:
            val = _char_value(patch_ids[cid], val)
        if _value_nonempty(val):
            chars_out.append({"id": cid, "value": val})
        seen.add(cid)

    for cid, pval in patch_ids.items():
        if cid and cid not in seen and pval:
            chars_out.append({"id": cid, "value": pval})

    payload["characteristics"] = chars_out

    if fmap.number_named and row.doc_number:
        payload[fmap.number_named] = row.doc_number
    if fmap.reg_date_named and row.reg_date:
        payload[fmap.reg_date_named] = row.reg_date
    if fmap.valid_until_named and row.valid_until:
        payload[fmap.valid_until_named] = row.valid_until

    sizes_out: List[dict] = []
    for sz in card.get("sizes") or []:
        if not isinstance(sz, dict):
            continue
        item: Dict[str, Any] = {}
        if sz.get("chrtID") is not None:
            item["chrtID"] = int(sz["chrtID"])
        if sz.get("techSize") is not None:
            item["techSize"] = str(sz["techSize"])
        if sz.get("wbSize") is not None:
            item["wbSize"] = str(sz["wbSize"])
        skus = sz.get("skus") or []
        if skus:
            item["skus"] = [str(x) for x in skus if str(x).strip()]
        if item.get("skus") or item.get("chrtID"):
            sizes_out.append(item)
    if not sizes_out:
        for sz in card.get("sizes") or []:
            if isinstance(sz, dict) and sz.get("skus"):
                sizes_out.append({"skus": [str(x) for x in sz["skus"]]})
                break
    payload["sizes"] = sizes_out
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

        payload = build_card_update_payload(card, row, fmap)
        updates.append(payload)
        results.append(CertApplyRowResult(
            vendor_code=row.vendor_code,
            nm_id=nm,
            status="ok" if not dry_run else "preview",
            message="Будет отправлено" if dry_run else "Отправка…",
        ))
        if progress_cb:
            progress_cb(done, total, f"Подготовлено: {row.vendor_code}")

    sent = 0
    errors: List[dict] = []
    if not dry_run and updates:
        batch_size = 30
        batches = [updates[i : i + batch_size] for i in range(0, len(updates), batch_size)]
        for bi, batch in enumerate(batches):
            if progress_cb:
                progress_cb(total, total, f"Отправка на WB: пакет {bi + 1}/{len(batches)}")
            try:
                await client.update_cards(batch)
                sent += len(batch)
            except HttpStatusError as e:
                errors.append({"batch": bi + 1, "status": e.status, "body": (e.body or "")[:500]})
                for r in results:
                    if r.status == "ok":
                        r.status = "error"
                        r.message = f"Ошибка WB {e.status}"
            if bi + 1 < len(batches):
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
