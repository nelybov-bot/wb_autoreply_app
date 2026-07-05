"""Черновики WB: cards/error/list + пустые обязательные характеристики (без ИИ)."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .compliance_docs import _norm_vendor
from .net import HttpStatusError
from .wb_certificates import _charc_id, _value_nonempty
from .wb_content_client import WbContentClient

log = logging.getLogger("wb.card_drafts")

ProgressCb = Callable[[int, int, str], None]


@dataclass
class CardDraftRow:
    vendor_code: str
    nm_id: int = 0
    subject_id: int = 0
    subject_name: str = ""
    title: str = ""
    wb_errors: List[str] = field(default_factory=list)
    missing_required: List[dict] = field(default_factory=list)
    updated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "vendor_code": self.vendor_code,
            "nm_id": self.nm_id,
            "subject_id": self.subject_id,
            "subject_name": self.subject_name,
            "title": self.title,
            "wb_errors": list(self.wb_errors),
            "missing_required": list(self.missing_required),
            "updated_at": self.updated_at,
        }


def _flatten_error_batches(batches: List[dict]) -> Dict[str, dict]:
    """vendor_code (casefold) → {vendor_code, subject_id, subject_name, wb_errors, updated_at}."""
    out: Dict[str, dict] = {}
    for batch in batches:
        if not isinstance(batch, dict):
            continue
        subjects = batch.get("subjects") if isinstance(batch.get("subjects"), dict) else {}
        errors_map = batch.get("errors") if isinstance(batch.get("errors"), dict) else {}
        updated_at = str(batch.get("updatedAt") or "")
        vendor_codes = batch.get("vendorCodes") or []
        if not isinstance(vendor_codes, list):
            vendor_codes = list(errors_map.keys())
        for vc_raw in vendor_codes:
            vc = _norm_vendor(vc_raw)
            if not vc:
                continue
            key = vc.casefold()
            subj = subjects.get(vc_raw) or subjects.get(vc) or {}
            if not isinstance(subj, dict):
                subj = {}
            try:
                sid = int(subj.get("id") or subj.get("subjectID") or subj.get("subjectId") or 0)
            except (TypeError, ValueError):
                sid = 0
            subj_name = str(subj.get("name") or subj.get("subjectName") or "")
            errs = errors_map.get(vc_raw) or errors_map.get(vc) or []
            if not isinstance(errs, list):
                errs = [str(errs)] if errs else []
            err_texts = [str(e).strip() for e in errs if str(e).strip()]
            prev = out.get(key)
            if prev:
                merged_errs = list(dict.fromkeys(prev.get("wb_errors") or []) + err_texts)
                prev["wb_errors"] = merged_errs
                if updated_at and (not prev.get("updated_at") or updated_at > prev.get("updated_at", "")):
                    prev["updated_at"] = updated_at
                if sid and not prev.get("subject_id"):
                    prev["subject_id"] = sid
                    prev["subject_name"] = subj_name
            else:
                out[key] = {
                    "vendor_code": vc,
                    "subject_id": sid,
                    "subject_name": subj_name,
                    "wb_errors": err_texts,
                    "updated_at": updated_at,
                }
    return out


def _card_filled_charcs(card: dict) -> Dict[int, Any]:
    filled: Dict[int, Any] = {}
    for ch in card.get("characteristics") or []:
        if not isinstance(ch, dict):
            continue
        cid = _charc_id(ch)
        if cid and _value_nonempty(ch.get("value")):
            filled[cid] = ch.get("value")
    return filled


def _is_charc_required(sch: dict) -> bool:
    return bool(sch.get("required")) or bool(sch.get("isRequiredForCreate"))


def find_missing_required_characteristics(
    card: dict,
    charcs_schema: List[dict],
) -> List[dict]:
    """Пустые обязательные поля категории по схеме get_subject_charcs."""
    filled = _card_filled_charcs(card)
    missing: List[dict] = []
    for sch in charcs_schema:
        if not isinstance(sch, dict):
            continue
        cid = _charc_id(sch)
        if not cid or not _is_charc_required(sch):
            continue
        if cid in filled:
            continue
        missing.append({
            "charc_id": cid,
            "name": str(sch.get("name") or "").strip(),
            "unit_name": str(sch.get("unitName") or "").strip(),
            "charc_type": sch.get("charcType"),
            "required": bool(sch.get("required")),
            "is_required_for_create": bool(sch.get("isRequiredForCreate")),
        })
    missing.sort(key=lambda x: (x.get("name") or "").casefold())
    return missing


async def _load_charcs_cache(
    client: WbContentClient,
    subject_ids: Set[int],
    cache: Dict[int, List[dict]],
) -> None:
    for sid in sorted(subject_ids):
        if sid in cache or sid <= 0:
            continue
        try:
            cache[sid] = await client.get_subject_charcs(sid)
        except HttpStatusError as e:
            log.warning("WB charcs subject %s: %s", sid, (e.body or "")[:200])
            cache[sid] = []
        await asyncio.sleep(0.65)


async def scan_card_drafts_for_store(
    api_key: str,
    *,
    vendor_codes: Optional[List[str]] = None,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """Список черновиков WB + пустые обязательные характеристики (без отправки на WB)."""
    client = WbContentClient(api_key, timeout_s=120.0)

    if progress_cb:
        progress_cb(0, 3, "Загрузка черновиков WB…")

    batches = await client.list_card_errors_all(
        progress_cb=lambda cur, tot, detail: (
            progress_cb(0, 3, detail) if progress_cb else None
        ),
    )
    flat = _flatten_error_batches(batches)

    allowed: Optional[Set[str]] = None
    if vendor_codes:
        allowed = {_norm_vendor(v).casefold() for v in vendor_codes if _norm_vendor(v)}

    entries = [
        e for e in flat.values()
        if allowed is None or e["vendor_code"].casefold() in allowed
    ]
    entries.sort(key=lambda x: (x.get("vendor_code") or "").casefold())

    if progress_cb:
        progress_cb(1, 3, f"Найдено черновиков: {len(entries)}")

    if not entries:
        return {
            "batches": len(batches),
            "draft_count": 0,
            "rows": [],
        }

    vendor_list = [e["vendor_code"] for e in entries]
    if progress_cb:
        progress_cb(1, 3, f"Загрузка карточек ({len(vendor_list)})…")

    cards = await client.list_cards_all(vendor_codes=vendor_list)
    card_by_vendor: Dict[str, dict] = {}
    for card in cards:
        vc = _norm_vendor(card.get("vendorCode") or card.get("supplierVendorCode") or "").casefold()
        if vc and vc not in card_by_vendor:
            card_by_vendor[vc] = card

    subject_ids: Set[int] = set()
    for e in entries:
        if e.get("subject_id"):
            subject_ids.add(int(e["subject_id"]))
    for card in card_by_vendor.values():
        try:
            sid = int(card.get("subjectID") or card.get("subjectId") or 0)
        except (TypeError, ValueError):
            sid = 0
        if sid:
            subject_ids.add(sid)

    charcs_cache: Dict[int, List[dict]] = {}
    await _load_charcs_cache(client, subject_ids, charcs_cache)

    if progress_cb:
        progress_cb(2, 3, "Анализ обязательных полей…")

    rows: List[CardDraftRow] = []
    total = max(len(entries), 1)
    for i, entry in enumerate(entries):
        vc_key = entry["vendor_code"].casefold()
        card = card_by_vendor.get(vc_key)
        nm = 0
        title = ""
        sid = int(entry.get("subject_id") or 0)
        subj_name = str(entry.get("subject_name") or "")
        missing: List[dict] = []

        if card:
            nm = int(card.get("nmID") or card.get("nmId") or 0)
            title = str(card.get("title") or "")[:200]
            try:
                csid = int(card.get("subjectID") or card.get("subjectId") or 0)
            except (TypeError, ValueError):
                csid = 0
            if csid:
                sid = csid
            if not subj_name:
                subj_name = str(card.get("subjectName") or card.get("subject") or "")
            schema = charcs_cache.get(sid) or []
            if schema:
                missing = find_missing_required_characteristics(card, schema)

        rows.append(CardDraftRow(
            vendor_code=entry["vendor_code"],
            nm_id=nm,
            subject_id=sid,
            subject_name=subj_name,
            title=title,
            wb_errors=list(entry.get("wb_errors") or []),
            missing_required=missing,
            updated_at=str(entry.get("updated_at") or ""),
        ))
        if progress_cb:
            progress_cb(2, 3, f"Анализ: {entry['vendor_code']} ({i + 1}/{total})")

    if progress_cb:
        progress_cb(3, 3, f"Готово: {len(rows)} черновиков")

    with_missing = sum(1 for r in rows if r.missing_required)
    return {
        "batches": len(batches),
        "draft_count": len(rows),
        "with_missing_required": with_missing,
        "rows": [r.to_dict() for r in rows],
    }


async def scan_card_drafts_multi_store(
    stores: List[Tuple[int, str, str]],
    *,
    vendor_codes: Optional[List[str]] = None,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """stores: (store_id, store_name, api_key)."""
    out_stores: List[dict] = []
    total_stores = len(stores)
    grand_total = max(total_stores * 3, 1)

    for i, (store_id, store_name, api_key) in enumerate(stores):
        store_offset = i * 3

        def _cb(
            cur: int,
            tot: int,
            detail: str,
            _offset=store_offset,
            _name=store_name,
            _si=i,
        ) -> None:
            if progress_cb:
                progress_cb(
                    _offset + max(0, min(int(cur or 0), 3)),
                    grand_total,
                    f"Магазин {_si + 1}/{total_stores} · {_name}: {detail}",
                )

        if progress_cb:
            progress_cb(store_offset, grand_total, f"Магазин {i + 1}/{total_stores}: {store_name}…")

        try:
            part = await scan_card_drafts_for_store(
                api_key,
                vendor_codes=vendor_codes,
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
            log.exception("wb card drafts store %s: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:400],
                "rows": [],
            })

    return {"stores": out_stores}
