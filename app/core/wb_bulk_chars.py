"""Массовое редактирование характеристик карточек WB (cards/update)."""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .net import HttpStatusError
from .packaging_dims import (
    _CATALOG_MAX_PAGES,
    _build_card_index,
    _fetch_full_catalog_from_wb,
    _load_named_field_char_ids,
    _load_wb_cards_for_compare,
    _lookup_card,
    _norm_vendor as _norm_vendor_sku,
)
from .wb_certificates import (
    _charc_id,
    _charc_name,
    _format_wb_error,
    build_card_char_patches_payload,
)
from .wb_content_client import WbContentClient

log = logging.getLogger("wb.bulk_chars")

ProgressCb = Callable[[int, int, str], None]

_RE_SKU_HEADER = re.compile(r"sku|артикул|vendor|offer", re.I)

# Быстрые подсказки для поиска поля в схеме категории
_CHAR_QUERY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "ндс": ("ставка ндс", "ндс"),
    "vat": ("ставка ндс", "ндс"),
    "ставка ндс": ("ставка ндс",),
}


@dataclass
class BulkCharPlanRow:
    vendor_code: str
    nm_id: int = 0
    subject_id: int = 0
    char_id: int = 0
    char_name: str = ""
    current_value: str = ""
    new_value: str = ""
    status: str = "pending"
    message: str = ""


def _split_line(line: str) -> List[str]:
    if "\t" in line:
        return [p.strip() for p in line.split("\t")]
    if ";" in line:
        return [p.strip() for p in line.split(";")]
    return [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]


def parse_vendor_list_text(text: str) -> Tuple[List[str], List[str]]:
    """Одна колонка артикулов (vendorCode / nmID) — по строке."""
    warnings: List[str] = []
    rows: List[str] = []
    seen: Set[str] = set()
    lines = (text or "").splitlines()
    col_idx = 0
    start = 0
    if lines:
        parts0 = _split_line(lines[0].strip())
        if parts0 and _RE_SKU_HEADER.search(" ".join(parts0)):
            for i, p in enumerate(parts0):
                if _RE_SKU_HEADER.search(p):
                    col_idx = i
                    break
            start = 1

    for line_no, raw in enumerate(lines[start:], start=start + 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = _split_line(line)
        if not parts:
            continue
        sku = _norm_vendor_sku(parts[col_idx] if col_idx < len(parts) else parts[0])
        if not sku:
            warnings.append(f"Строка {line_no}: пустой артикул — пропущена")
            continue
        key = sku.casefold()
        if key in seen:
            warnings.append(f"Дубликат артикула {sku} — оставлена первая строка")
            continue
        seen.add(key)
        rows.append(sku)
    if not rows and (text or "").strip():
        return [], warnings or ["Не удалось разобрать ни одной строки"]
    return rows, warnings


def _normalize_query(query: str) -> str:
    return re.sub(r"\s+", " ", str(query or "").strip()).casefold()


def _query_variants(query: str) -> List[str]:
    q = _normalize_query(query)
    if not q:
        return []
    out: List[str] = [q]
    for key, aliases in _CHAR_QUERY_ALIASES.items():
        if q == key or q in aliases:
            for a in aliases:
                if a not in out:
                    out.append(a)
    return out


def find_char_by_name(charcs: List[dict], query: str) -> Tuple[int, str]:
    """Найти charcID в схеме предмета по названию (частичное совпадение)."""
    variants = _query_variants(query)
    if not variants:
        return 0, ""

    candidates: List[Tuple[int, str, int]] = []
    for ch in charcs or []:
        if not isinstance(ch, dict):
            continue
        cid = _charc_id(ch)
        name = _charc_name(ch)
        if not cid or not name:
            continue
        nlow = name.casefold()
        for q in variants:
            if nlow == q:
                return cid, name
            if q in nlow:
                score = 1000 - len(name) + (10 if nlow.startswith(q) else 0)
                candidates.append((score, cid, name))

    if not candidates:
        words = variants[0].split()
        for ch in charcs or []:
            if not isinstance(ch, dict):
                continue
            cid = _charc_id(ch)
            name = _charc_name(ch)
            if not cid or not name:
                continue
            nlow = name.casefold()
            if words and all(w in nlow for w in words):
                score = 500 - len(name)
                candidates.append((score, cid, name))

    if not candidates:
        return 0, ""
    candidates.sort(key=lambda x: (-x[0], x[2]))
    _, cid, name = candidates[0]
    return cid, name


def _format_char_value(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join(str(x).strip() for x in val if str(x).strip())
    return str(val).strip()


def _values_equal(current: Any, new_value: str) -> bool:
    cur = _format_char_value(current)
    new = str(new_value or "").strip()
    if cur == new:
        return True
    cur_digits = re.sub(r"[^\d]", "", cur)
    new_digits = re.sub(r"[^\d]", "", new)
    if cur_digits and new_digits and cur_digits == new_digits:
        return True
    return False


def _card_char_value(card: dict, char_id: int) -> Any:
    for ch in card.get("characteristics") or []:
        if isinstance(ch, dict) and _charc_id(ch) == char_id:
            return ch.get("value")
    return None


async def _resolve_char_ids_by_subject(
    client: WbContentClient,
    subject_ids: Set[int],
    char_query: str,
    *,
    db: Any = None,
    progress_cb: Optional[ProgressCb] = None,
) -> Tuple[Dict[int, int], Dict[int, str], List[str]]:
    """subject_id → (char_id, char_name); warnings для предметов без поля."""
    char_id_by_subject: Dict[int, int] = {}
    char_name_by_subject: Dict[int, str] = {}
    warnings: List[str] = []
    subjects = sorted(s for s in subject_ids if s > 0)
    total = max(len(subjects), 1)
    for i, sid in enumerate(subjects, start=1):
        if progress_cb:
            progress_cb(i, total, f"Схема категорий: {i}/{total}")
        charcs: List[dict] = []
        if db and hasattr(db, "packaging_dims_charcs_get"):
            cached = db.packaging_dims_charcs_get(sid)
            if cached is not None:
                charcs = cached
        if not charcs:
            try:
                charcs = await client.get_subject_charcs(sid)
            except Exception as e:
                warnings.append(f"Предмет {sid}: схема недоступна ({e})")
                continue
            if db and hasattr(db, "packaging_dims_charcs_put") and charcs:
                db.packaging_dims_charcs_put(sid, charcs)
            await asyncio.sleep(0.55)
        cid, cname = find_char_by_name(charcs, char_query)
        if cid:
            char_id_by_subject[sid] = cid
            char_name_by_subject[sid] = cname
        else:
            warnings.append(f"Предмет {sid}: поле «{char_query}» не найдено в схеме")
    return char_id_by_subject, char_name_by_subject, warnings


def estimate_bulk_char_steps(
    item_count: int,
    store_count: int = 1,
    *,
    force_refresh: bool = False,
) -> int:
    stores = max(int(store_count or 0), 1)
    n = max(int(item_count or 0), 1)
    if force_refresh:
        return stores * (_CATALOG_MAX_PAGES + n + max(1, (n + 99) // 100))
    return stores * (2 + n + max(1, (n + 99) // 100))


async def apply_bulk_chars_for_store(
    api_key: str,
    *,
    char_name: str,
    char_value: str,
    vendor_codes: Optional[List[str]] = None,
    all_catalog: bool = False,
    store_id: Optional[int] = None,
    db: Any = None,
    dry_run: bool = False,
    only_if_different: bool = True,
    force_refresh: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    query = str(char_name or "").strip()
    new_val = str(char_value or "").strip()
    if not query:
        raise ValueError("Укажите название характеристики (например: Ставка НДС)")
    if not new_val:
        raise ValueError("Укажите новое значение (например: 5)")

    client = WbContentClient(api_key, timeout_s=600.0)
    codes = list(dict.fromkeys(_norm_vendor_sku(v) for v in (vendor_codes or []) if _norm_vendor_sku(v)))

    if progress_cb:
        progress_cb(0, _CATALOG_MAX_PAGES, "Загрузка каталога WB…")

    if all_catalog or not codes:
        if store_id and db is not None:
            by_vendor, by_nm_id, by_barcode, load_meta = await _load_wb_cards_for_compare(
                client,
                [],
                store_id=store_id,
                db=db,
                force_refresh=force_refresh,
                progress_cb=progress_cb,
            )
        else:
            cards, load_meta = await _fetch_full_catalog_from_wb(client, progress_cb=progress_cb)
            by_vendor, by_nm_id, by_barcode = _build_card_index(cards)
        target_cards: List[dict] = []
        seen_nm: Set[int] = set()
        for card in list(by_nm_id.values()) + list(by_vendor.values()):
            if not isinstance(card, dict):
                continue
            try:
                nm = int(card.get("nmID") or card.get("nmId") or 0)
            except (TypeError, ValueError):
                nm = 0
            if nm and nm not in seen_nm:
                seen_nm.add(nm)
                target_cards.append(card)
        scope_label = "весь каталог"
        if load_meta.get("cache_hit"):
            scope_label += " (из кэша)"
    else:
        by_vendor, by_nm_id, by_barcode, load_meta = await _load_wb_cards_for_compare(
            client,
            codes,
            store_id=store_id,
            db=db,
            force_refresh=force_refresh,
            progress_cb=progress_cb,
        )
        target_cards = []
        seen_nm = set()
        for vc in codes:
            card = _lookup_card(by_vendor, by_nm_id, by_barcode, vc)
            if not card:
                continue
            nm = int(card.get("nmID") or card.get("nmId") or 0)
            if nm and nm not in seen_nm:
                seen_nm.add(nm)
                target_cards.append(card)
        scope_label = f"список ({len(codes)} арт.)"

    subject_ids: Set[int] = set()
    for card in target_cards:
        try:
            sid = int(card.get("subjectID") or card.get("subjectId") or 0)
        except (TypeError, ValueError):
            sid = 0
        if sid:
            subject_ids.add(sid)

    char_id_by_subject, char_name_by_subject, schema_warnings = await _resolve_char_ids_by_subject(
        client, subject_ids, query, db=db, progress_cb=progress_cb,
    )
    if progress_cb:
        progress_cb(0, max(len(target_cards), 1), f"Проверка {len(target_cards)} карточек…")
    named_field_ids = await _load_named_field_char_ids(client, subject_ids, db=db)

    results: List[dict] = []
    updates: List[dict] = []
    pending: List[dict] = []
    skipped = 0
    not_found_list = 0
    no_field = 0
    total_work = max(len(target_cards), 1)
    step = 0

    if not all_catalog and codes:
        found_vcs = {
            _norm_vendor_sku(
                str(c.get("vendorCode") or c.get("supplierVendorCode") or "")
            ).casefold()
            for c in target_cards
        }
        for vc in codes:
            step += 1
            key = _norm_vendor_sku(vc).casefold()
            if key not in found_vcs:
                not_found_list += 1
                results.append({
                    "vendor_code": vc,
                    "nm_id": 0,
                    "status": "not_found",
                    "message": "Артикул не найден в каталоге магазина",
                    "current_value": "",
                    "new_value": new_val,
                    "char_name": "",
                })
                if progress_cb:
                    progress_cb(step, total_work, f"Не найден: {vc}")

    for card in target_cards:
        step += 1
        vc = str(card.get("vendorCode") or card.get("supplierVendorCode") or "")
        nm = int(card.get("nmID") or card.get("nmId") or 0)
        try:
            sid = int(card.get("subjectID") or card.get("subjectId") or 0)
        except (TypeError, ValueError):
            sid = 0

        char_id = char_id_by_subject.get(sid, 0)
        resolved_name = char_name_by_subject.get(sid, query)
        if not char_id:
            no_field += 1
            results.append({
                "vendor_code": vc,
                "nm_id": nm,
                "status": "no_field",
                "message": f"Поле «{query}» не найдено для предмета {sid}",
                "current_value": "",
                "new_value": new_val,
                "char_name": "",
            })
            if progress_cb:
                progress_cb(step, total_work, f"Нет поля: {vc}")
            continue

        current = _card_char_value(card, char_id)
        cur_s = _format_char_value(current)
        if only_if_different and _values_equal(current, new_val):
            skipped += 1
            results.append({
                "vendor_code": vc,
                "nm_id": nm,
                "status": "skipped",
                "message": f"Уже «{cur_s or '—'}»",
                "current_value": cur_s,
                "new_value": new_val,
                "char_name": resolved_name,
            })
            if progress_cb:
                progress_cb(step, total_work, f"Без изменений: {vc}")
            continue

        payload = build_card_char_patches_payload(
            card,
            {char_id: new_val},
            vendor_code=vc,
            strip_char_ids=named_field_ids or None,
        )
        if not payload.get("sizes"):
            results.append({
                "vendor_code": vc,
                "nm_id": nm,
                "status": "error",
                "message": "В карточке нет sizes (chrtID/skus) для обновления",
                "current_value": cur_s,
                "new_value": new_val,
                "char_name": resolved_name,
            })
            continue

        msg = f"«{cur_s or '—'}» → «{new_val}» ({resolved_name})"
        row_out = {
            "vendor_code": vc,
            "nm_id": nm,
            "status": "preview" if dry_run else "pending",
            "message": msg,
            "current_value": cur_s,
            "new_value": new_val,
            "char_name": resolved_name,
        }
        updates.append(payload)
        pending.append(row_out)
        results.append(row_out)
        if progress_cb:
            progress_cb(step, total_work, f"{'Проверено' if dry_run else 'Подготовлено'}: {vc}")

    sent = 0
    errors: List[dict] = []
    if not dry_run and updates:
        def _send_prog(cur: int, tot: int, detail: str) -> None:
            if progress_cb:
                progress_cb(total_work + cur, total_work + tot, detail)

        sent, batch_errors = await client.update_cards_batched(updates, progress_cb=_send_prog)
        err_by_vc = {
            _norm_vendor_sku(e.get("vendor_code") or "").casefold(): e
            for e in batch_errors
            if e.get("vendor_code")
        }
        for res in pending:
            key = _norm_vendor_sku(res.get("vendor_code") or "").casefold()
            if key in err_by_vc:
                e = err_by_vc[key]
                msg = _format_wb_error(
                    HttpStatusError(status=int(e.get("status") or 0), body=str(e.get("body") or ""))
                )
                errors.append(e)
                res["status"] = "error"
                res["message"] = msg
            else:
                res["status"] = "ok"
                res["message"] = "Отправлено на WB"

    display_rows = [r for r in results if r["status"] not in ("skipped",)]

    return {
        "dry_run": dry_run,
        "char_name": query,
        "char_value": new_val,
        "scope": scope_label,
        "only_if_different": only_if_different,
        "parsed": len(codes) if codes else len(target_cards),
        "catalog_cards": load_meta.get("cards_loaded") or len(target_cards),
        "prepared": len(updates),
        "sent": sent,
        "skipped": skipped,
        "not_found": not_found_list,
        "no_field": no_field,
        "preview": sum(1 for r in results if r["status"] == "preview"),
        "ok": sum(1 for r in results if r["status"] == "ok"),
        "errors_count": sum(1 for r in results if r["status"] == "error"),
        "schema_warnings": schema_warnings[:30],
        "load_mode": load_meta.get("load_mode"),
        "cache_hit": load_meta.get("cache_hit"),
        "rows": display_rows[:500],
        "rows_total": len(display_rows),
        "errors": errors[:20],
    }


async def apply_bulk_chars_multi_store(
    stores: List[Tuple[int, str, str]],
    *,
    char_name: str,
    char_value: str,
    vendor_codes: Optional[List[str]] = None,
    all_catalog: bool = False,
    db: Any = None,
    dry_run: bool = False,
    only_if_different: bool = True,
    force_refresh: bool = False,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    out_stores: List[dict] = []
    n_items = len(vendor_codes) if vendor_codes else 5000
    grand = max(len(stores) * estimate_bulk_char_steps(n_items, 1, force_refresh=force_refresh), 1)
    offset = 0

    for i, (store_id, store_name, api_key) in enumerate(stores):
        store_grand = estimate_bulk_char_steps(n_items, 1, force_refresh=force_refresh)

        def _cb(cur: int, tot: int, detail: str, _off=offset, _name=store_name, _si=i) -> None:
            if not progress_cb:
                return
            d = str(detail or "")
            tot_i = max(int(tot or 0), 1)
            cur_i = max(0, min(int(cur or 0), tot_i))
            # Фаза загрузки каталога: показываем страницы/150, а не 1% от 5000+
            if tot_i <= _CATALOG_MAX_PAGES and "каталог" in d.casefold():
                progress_cb(cur_i, tot_i, f"Магазин {_si + 1}/{len(stores)} · {_name}: {detail}")
                return
            progress_cb(_off + cur_i, grand, f"Магазин {_si + 1}/{len(stores)} · {_name}: {detail}")

        if progress_cb:
            progress_cb(offset, grand, f"Магазин {i + 1}/{len(stores)}: {store_name}…")

        try:
            part = await apply_bulk_chars_for_store(
                api_key,
                char_name=char_name,
                char_value=char_value,
                vendor_codes=vendor_codes,
                all_catalog=all_catalog,
                store_id=store_id,
                db=db,
                dry_run=dry_run,
                only_if_different=only_if_different,
                force_refresh=force_refresh,
                progress_cb=_cb,
            )
            part["store_id"] = store_id
            part["store_name"] = store_name
            out_stores.append(part)
        except Exception as e:
            log.exception("wb bulk chars store %s: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:300],
                "rows": [],
            })
        offset += store_grand

    return {
        "char_name": char_name,
        "char_value": char_value,
        "all_catalog": all_catalog,
        "stores": out_stores,
    }
