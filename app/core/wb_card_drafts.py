"""Черновики WB: cards/error/list + пустые обязательные характеристики + ИИ-дозаполнение."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .compliance_docs import _norm_vendor
from .net import HttpStatusError
from .wb_certificates import (
    _char_value,
    _charc_id,
    _format_wb_error,
    _value_nonempty,
    build_card_char_patches_payload,
)
from .wb_content_client import WbContentClient

log = logging.getLogger("wb.card_drafts")

ProgressCb = Callable[[int, int, str], None]

_AI_FILL_SYSTEM = """Ты помогаешь заполнить обязательные характеристики карточки товара Wildberries.
На вход — JSON с названием, описанием, брендом, категорией и списком полей для заполнения.

Верни ТОЛЬКО JSON (без markdown):
{"fills": [{"charc_id": 123, "value": "текст или число", "confidence": "high|medium|low", "reason": "кратко"}]}

Правила:
1) Заполняй только поля из fields_to_fill; charc_id должен совпадать.
2) value — строка или число. Для объёма в мл (unit_name «мл») — только число (например 14), без «мл».
3) Опирайся на title и description. Не выдумывай ТН ВЭД, ГОСТ, состав, если их нет в тексте.
4) Каждое поле из fields_to_fill ОБЯЗАТЕЛЬНО должно получить value — пустые строки запрещены, иначе товар останется в черновике.
5) Для аромата косметики без указания в тексте — «без аромата» (confidence medium).
6) Для «Объем товара» в мл: ищи число с «мл» в title/description. Если только «N капсул» — суммарный объём ≈ N × 0.35 мл (округли до 1 знака).
7) Если объём совсем неясен — поставь разумную оценку (например 10 мл для масел/сывороток), не 0 и не пусто.
8) НИКОГДА не ставь 0 в объём.
9) Язык value — русский, как принято на WB.
"""


@dataclass
class CardDraftRow:
    vendor_code: str
    nm_id: int = 0
    subject_id: int = 0
    subject_name: str = ""
    title: str = ""
    brand: str = ""
    description: str = ""
    wb_errors: List[str] = field(default_factory=list)
    missing_required: List[dict] = field(default_factory=list)
    updated_at: str = ""
    ai_fills: List[dict] = field(default_factory=list)
    status: str = "pending"
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "vendor_code": self.vendor_code,
            "nm_id": self.nm_id,
            "subject_id": self.subject_id,
            "subject_name": self.subject_name,
            "title": self.title,
            "brand": self.brand,
            "description": self.description[:500] if self.description else "",
            "wb_errors": list(self.wb_errors),
            "missing_required": list(self.missing_required),
            "updated_at": self.updated_at,
            "ai_fills": list(self.ai_fills),
            "status": self.status,
            "message": self.message,
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
            brand = str(card.get("brand") or "")[:80]
            description = str(card.get("description") or "")
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
            brand=brand,
            description=description,
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


def _is_volume_field(name: str, unit_name: str) -> bool:
    n = (name or "").casefold().replace("ё", "е")
    u = (unit_name or "").casefold().replace("ё", "е")
    return "объем" in n or u == "мл"


def _numeric_le_zero(val: Any) -> bool:
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val) <= 0
    s = str(val or "").strip().replace(",", ".")
    if not s:
        return False
    try:
        return float(s) <= 0
    except ValueError:
        return False


def _fill_value_usable(
    val: Any,
    *,
    name: str = "",
    unit_name: str = "",
) -> bool:
    if val is None:
        return False
    if isinstance(val, list):
        return any(_fill_value_usable(x, name=name, unit_name=unit_name) for x in val)
    s = str(val).strip()
    if not s:
        return False
    if _is_volume_field(name, unit_name) and _numeric_le_zero(val):
        return False
    return True


def _extract_volume_ml_from_text(*texts: str) -> Optional[float]:
    combined = " ".join(t for t in texts if (t or "").strip())
    if not combined:
        return None
    for pat in (
        r"(\d+(?:[.,]\d+)?)\s*мл\b",
        r"объ[её]м\w*\s*[:—\-]?\s*(\d+(?:[.,]\d+)?)",
        r"(\d+(?:[.,]\d+)?)\s*ml\b",
    ):
        m = re.search(pat, combined, re.I)
        if m:
            v = float(m.group(1).replace(",", "."))
            if v > 0:
                return round(v, 2)
    cap_m = re.search(r"(\d+)\s*капсул", combined, re.I)
    if cap_m:
        n = int(cap_m.group(1))
        if 1 <= n <= 200:
            return round(n * 0.35, 1)
    return None


def _default_volume_ml(row: CardDraftRow) -> float:
    vol = _extract_volume_ml_from_text(row.title, row.description)
    if vol and vol > 0:
        return vol
    text = f"{row.title} {row.description}".casefold()
    cat = (row.subject_name or "").casefold()
    if "капсул" in text or "ампул" in text:
        return 2.5
    if "масл" in cat or "сыворот" in cat or "концентрат" in text:
        return 10.0
    return 10.0


def _default_fill_for_required(row: CardDraftRow, m: dict) -> dict:
    name = str(m.get("name") or "")
    unit = str(m.get("unit_name") or "")
    cid = int(m.get("charc_id") or 0)
    base: Dict[str, Any] = {
        "charc_id": cid,
        "name": name,
        "unit_name": unit,
        "charc_type": m.get("charc_type"),
    }
    nlow = name.casefold().replace("ё", "е")

    if _is_volume_field(name, unit):
        vol = _default_volume_ml(row)
        return {
            **base,
            "value": vol,
            "confidence": "medium",
            "reason": "Объём из текста, оценка для капсул или значение по умолчанию",
        }

    if "аромат" in nlow:
        return {
            **base,
            "value": "без аромата",
            "confidence": "medium",
            "reason": "Аромат не указан в карточке",
        }

    if m.get("charc_type") == 4 or (unit or "").casefold() in ("мл", "г", "кг", "см", "мм"):
        return {
            **base,
            "value": 1,
            "confidence": "low",
            "reason": "Числовое значение по умолчанию — проверьте вручную",
        }

    snippet = (row.title or row.brand or "стандартный").strip()[:80]
    return {
        **base,
        "value": snippet,
        "confidence": "low",
        "reason": "Текстовое значение по умолчанию из названия — проверьте вручную",
    }


def _ensure_all_required_filled(row: CardDraftRow, fills: List[dict]) -> List[dict]:
    """Гарантирует value для каждого обязательного поля — пустые не допускаются."""
    by_id: Dict[int, dict] = {}
    for f in fills:
        try:
            cid = int(f.get("charc_id") or 0)
        except (TypeError, ValueError):
            continue
        if cid:
            by_id[cid] = f

    out: List[dict] = []
    for m in row.missing_required:
        try:
            cid = int(m.get("charc_id") or 0)
        except (TypeError, ValueError):
            continue
        if not cid:
            continue
        name = str(m.get("name") or "")
        unit = str(m.get("unit_name") or "")
        cur = by_id.get(cid)
        if cur and _fill_value_usable(cur.get("value"), name=name, unit_name=unit):
            out.append(cur)
        else:
            out.append(_default_fill_for_required(row, m))
    return out


def _parse_ai_fill_json(raw: str) -> List[dict]:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    obj = json.loads(text)
    if isinstance(obj, dict):
        fills = obj.get("fills")
        if isinstance(fills, list):
            return [x for x in fills if isinstance(x, dict)]
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    return []


def _normalize_ai_value(val: Any, *, unit_name: str = "", charc_type: Any = None, name: str = "") -> Any:
    if val is None:
        return ""
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if _is_volume_field(name, unit_name) and float(val) <= 0:
            return ""
        if charc_type == 4 or (unit_name or "").strip().lower() in ("мл", "г", "кг", "см", "мм"):
            return val
        return str(val)
    s = str(val).strip()
    if not s:
        return ""
    if _is_volume_field(name, unit_name) and _numeric_le_zero(s):
        return ""
    unit = (unit_name or "").strip().lower()
    if unit in ("мл", "г", "кг") or charc_type == 4:
        m = re.search(r"(\d+(?:[.,]\d+)?)", s.replace(",", "."))
        if m:
            num = m.group(1)
            parsed: Any = float(num) if "." in num else int(num)
            if _is_volume_field(name, unit_name) and _numeric_le_zero(parsed):
                return ""
            return parsed
    return s


def _coerce_patch_value(
    raw_val: Any,
    existing: Any,
    *,
    unit_name: str = "",
    charc_type: Any = None,
    name: str = "",
) -> Any:
    norm = _normalize_ai_value(raw_val, unit_name=unit_name, charc_type=charc_type, name=name)
    if not _value_nonempty(norm):
        return norm
    if isinstance(existing, list):
        return _char_value(str(norm) if not isinstance(norm, (int, float)) else norm, existing)
    if isinstance(norm, (int, float)) and charc_type == 4:
        return norm
    return str(norm)


async def _ai_suggest_fills_for_row(
    row: CardDraftRow,
    *,
    openai_key: str,
    client: Any = None,
) -> List[dict]:
    from app.core.openai_client import OpenAIClient

    if not row.missing_required:
        return []
    ai_client = client or OpenAIClient(openai_key)
    payload = {
        "vendor_code": row.vendor_code,
        "title": row.title,
        "description": (row.description or "")[:3000],
        "brand": row.brand,
        "category": row.subject_name,
        "fields_to_fill": row.missing_required,
    }
    raw = await ai_client.generate(_AI_FILL_SYSTEM, json.dumps(payload, ensure_ascii=False))
    parsed = _parse_ai_fill_json(raw)
    allowed = {int(m["charc_id"]) for m in row.missing_required if m.get("charc_id")}
    meta_by_id = {int(m["charc_id"]): m for m in row.missing_required if m.get("charc_id")}
    out: List[dict] = []
    seen: Set[int] = set()
    for item in parsed:
        try:
            cid = int(item.get("charc_id") or 0)
        except (TypeError, ValueError):
            continue
        if not cid or cid not in allowed or cid in seen:
            continue
        meta = meta_by_id.get(cid) or {}
        fname = str(meta.get("name") or item.get("name") or "")
        funit = str(meta.get("unit_name") or "")
        val = _normalize_ai_value(
            item.get("value"),
            unit_name=funit,
            charc_type=meta.get("charc_type"),
            name=fname,
        )
        if not _fill_value_usable(val, name=fname, unit_name=funit):
            val = ""
        out.append({
            "charc_id": cid,
            "name": fname,
            "value": val,
            "confidence": str(item.get("confidence") or "medium").strip().lower(),
            "reason": str(item.get("reason") or "")[:200],
            "unit_name": str(meta.get("unit_name") or ""),
            "charc_type": meta.get("charc_type"),
        })
        seen.add(cid)
    return out


def _fills_to_patches(fills: List[dict], card: dict) -> Dict[int, Any]:
    existing_by_id: Dict[int, Any] = {}
    for ch in card.get("characteristics") or []:
        if isinstance(ch, dict):
            cid = _charc_id(ch)
            if cid:
                existing_by_id[cid] = ch.get("value")
    patches: Dict[int, Any] = {}
    for f in fills:
        try:
            cid = int(f.get("charc_id") or 0)
        except (TypeError, ValueError):
            continue
        if not cid or not _fill_value_usable(
            f.get("value"),
            name=str(f.get("name") or ""),
            unit_name=str(f.get("unit_name") or ""),
        ):
            continue
        patches[cid] = _coerce_patch_value(
            f.get("value"),
            existing_by_id.get(cid),
            unit_name=str(f.get("unit_name") or ""),
            charc_type=f.get("charc_type"),
            name=str(f.get("name") or ""),
        )
    return patches


def _merge_manual_fills(
    row: CardDraftRow,
    manual: Optional[List[dict]],
) -> List[dict]:
    if not manual:
        return list(row.ai_fills)
    by_id = {int(f["charc_id"]): f for f in row.ai_fills if f.get("charc_id")}
    for f in manual:
        try:
            cid = int(f.get("charc_id") or 0)
        except (TypeError, ValueError):
            continue
        if cid:
            by_id[cid] = {**by_id.get(cid, {}), **f, "charc_id": cid}
    return list(by_id.values())


async def fill_card_drafts_for_store(
    api_key: str,
    *,
    openai_key: str = "",
    vendor_codes: Optional[List[str]] = None,
    dry_run: bool = True,
    manual_fills: Optional[Dict[str, List[dict]]] = None,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """ИИ-подбор значений для пустых полей + опциональная отправка cards/update."""
    scan = await scan_card_drafts_for_store(
        api_key,
        vendor_codes=vendor_codes,
        progress_cb=progress_cb,
    )
    rows_data = scan.get("rows") or []
    if not rows_data:
        return {**scan, "dry_run": dry_run, "prepared": 0, "sent": 0, "rows": []}

    client = WbContentClient(api_key, timeout_s=120.0)
    vendor_list = [str(r.get("vendor_code") or "") for r in rows_data if r.get("vendor_code")]
    cards = await client.list_cards_all(vendor_codes=vendor_list)
    card_by_vendor: Dict[str, dict] = {}
    for card in cards:
        vc = _norm_vendor(card.get("vendorCode") or card.get("supplierVendorCode") or "").casefold()
        if vc and vc not in card_by_vendor:
            card_by_vendor[vc] = card

    draft_rows: List[CardDraftRow] = []
    for r in rows_data:
        row = CardDraftRow(
            vendor_code=str(r.get("vendor_code") or ""),
            nm_id=int(r.get("nm_id") or 0),
            subject_id=int(r.get("subject_id") or 0),
            subject_name=str(r.get("subject_name") or ""),
            title=str(r.get("title") or ""),
            brand=str(r.get("brand") or ""),
            description=str(r.get("description") or ""),
            wb_errors=list(r.get("wb_errors") or []),
            missing_required=list(r.get("missing_required") or []),
            updated_at=str(r.get("updated_at") or ""),
        )
        draft_rows.append(row)

    ai_client = None
    if openai_key and not manual_fills:
        from app.core.openai_client import OpenAIClient
        ai_client = OpenAIClient(openai_key)

    total = max(len(draft_rows), 1)
    for i, row in enumerate(draft_rows):
        if progress_cb:
            progress_cb(i, total, f"ИИ: {row.vendor_code} ({i + 1}/{total})")
        if not row.missing_required:
            row.status = "skipped"
            row.message = "Нет пустых обязательных полей"
            continue
        manual = (manual_fills or {}).get(row.vendor_code) or (manual_fills or {}).get(row.vendor_code.casefold())
        if manual_fills is not None:
            if not manual:
                row.status = "error"
                row.message = "Нет подсказок для отправки"
                continue
            row.ai_fills = _merge_manual_fills(row, manual)
            row.ai_fills = _ensure_all_required_filled(row, row.ai_fills)
        elif ai_client:
            try:
                row.ai_fills = await _ai_suggest_fills_for_row(row, openai_key=openai_key, client=ai_client)
                row.ai_fills = _ensure_all_required_filled(row, row.ai_fills)
            except (json.JSONDecodeError, ValueError) as e:
                log.warning("AI fill JSON %s: %s", row.vendor_code, e)
                row.ai_fills = _ensure_all_required_filled(row, [])
            except Exception as e:
                log.warning("AI fill %s: %s", row.vendor_code, e)
                row.ai_fills = _ensure_all_required_filled(row, [])
        else:
            row.ai_fills = _ensure_all_required_filled(row, [])

        if not row.missing_required:
            row.status = "skipped"
            row.message = "Нет пустых обязательных полей"
            continue

        filled_count = sum(
            1 for f in row.ai_fills
            if _fill_value_usable(f.get("value"), name=str(f.get("name") or ""), unit_name=str(f.get("unit_name") or ""))
        )
        if filled_count < len(row.missing_required):
            row.status = "error"
            row.message = f"Заполнено {filled_count} из {len(row.missing_required)} полей"
        else:
            row.status = "preview" if dry_run else "pending"
            row.message = "Просмотр" if dry_run else "Готово к отправке"

    updates: List[Tuple[CardDraftRow, dict]] = []
    if not dry_run:
        for row in draft_rows:
            if row.status in ("error", "skipped", "no_suggestions"):
                continue
            card = card_by_vendor.get(row.vendor_code.casefold())
            if not card:
                row.status = "error"
                row.message = "Карточка не найдена"
                continue
            patches = _fills_to_patches(row.ai_fills, card)
            if not patches:
                row.status = "error"
                row.message = "Не удалось собрать характеристики для отправки"
                continue
            payload = build_card_char_patches_payload(card, patches, vendor_code=row.vendor_code)
            if not payload.get("sizes"):
                row.status = "error"
                row.message = "В карточке нет sizes"
                continue
            updates.append((row, payload))

        for i, (row, payload) in enumerate(updates):
            if progress_cb:
                progress_cb(i, len(updates), f"Отправка: {row.vendor_code} ({i + 1}/{len(updates)})")
            try:
                await client.update_cards([payload])
                row.status = "ok"
                row.message = "Отправлено"
            except HttpStatusError as e:
                row.status = "error"
                row.message = _format_wb_error(e)
            if i + 1 < len(updates):
                await asyncio.sleep(6.5)

    sent = sum(1 for r in draft_rows if r.status == "ok")
    prepared = sum(
        1 for r in draft_rows
        if r.status in ("preview", "pending", "ok")
        and len(r.ai_fills) >= len(r.missing_required)
        and all(
            _fill_value_usable(f.get("value"), name=str(f.get("name") or ""), unit_name=str(f.get("unit_name") or ""))
            for f in r.ai_fills
        )
    )
    return {
        **scan,
        "dry_run": dry_run,
        "prepared": prepared,
        "sent": sent,
        "rows": [r.to_dict() for r in draft_rows],
    }


async def fill_card_drafts_multi_store(
    stores: List[Tuple[int, str, str]],
    *,
    openai_key: str = "",
    vendor_codes: Optional[List[str]] = None,
    dry_run: bool = True,
    manual_fills_by_store: Optional[Dict[int, Dict[str, List[dict]]]] = None,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    out_stores: List[dict] = []
    total_stores = len(stores)
    row_total = max(len(vendor_codes) if vendor_codes else 10, 1)
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
            progress_cb(store_offset, grand_total, f"Магазин {i + 1}/{total_stores}: {store_name}…")

        manual = (manual_fills_by_store or {}).get(store_id)
        try:
            part = await fill_card_drafts_for_store(
                api_key,
                openai_key=openai_key,
                vendor_codes=vendor_codes,
                dry_run=dry_run,
                manual_fills=manual,
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
            log.exception("wb card drafts fill store %s: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:400],
                "rows": [],
            })

    return {"stores": out_stores, "dry_run": dry_run}
