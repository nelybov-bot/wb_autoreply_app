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
            if cid:
                m.number_id = cid
            elif named and name:
                m.number_named = name
        elif _match_charc(ch, _RE_REG_DATE):
            if cid:
                m.reg_date_id = cid
            elif named and name:
                m.reg_date_named = name
        elif _match_charc(ch, _RE_VALID_UNTIL):
            if cid:
                m.valid_until_id = cid
            elif named and name:
                m.valid_until_named = name
    _resolve_named_field_ids(m, charcs)
    return m


def _resolve_named_field_ids(fmap: CertFieldMap, charcs: List[dict]) -> None:
    """Сопоставляет именованные поля схемы с id (для characteristics, не в корень JSON)."""
    named_map = (
        (fmap.number_named, "number_id"),
        (fmap.reg_date_named, "reg_date_id"),
        (fmap.valid_until_named, "valid_until_id"),
    )
    for named, attr in named_map:
        if not named or getattr(fmap, attr):
            continue
        target = str(named).strip().casefold()
        for ch in charcs:
            if _charc_name(ch).casefold() == target:
                cid = _charc_id(ch)
                if cid:
                    setattr(fmap, attr, cid)
                    break


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
    if isinstance(dims, dict) and dims:
        payload["dimensions"] = {
            k: dims[k]
            for k in ("length", "width", "height", "weightBrutto", "isValid")
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
        chars_out.append({"id": cid, "value": val})
        seen.add(cid)

    for cid, pval in patch_ids.items():
        if cid and cid not in seen and pval:
            chars_out.append({"id": cid, "value": pval})

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

        payload = build_card_update_payload(card, row, fmap)
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
        res_row = CertApplyRowResult(
            vendor_code=row.vendor_code,
            nm_id=nm,
            status="ok" if not dry_run else "preview",
            message="Будет отправлено" if dry_run else "Отправка…",
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
