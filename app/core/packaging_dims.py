"""Сравнение фактических габаритов упаковки с данными карточек WB."""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .net import HttpStatusError
from .wb_certificates import _format_wb_error
from .wb_content_client import WbContentClient

log = logging.getLogger("packaging_dims")

ProgressCb = Callable[[int, int, str], None]

_DIM_TOLERANCE_CM = 0.05
# От этого порога — один проход по всему каталогу WB (до 15k карточек), не запрос на каждый артикул.
_FULL_CATALOG_THRESHOLD = 15
_CATALOG_MAX_PAGES = 150


def estimate_compare_steps(row_count: int, store_count: int = 1) -> int:
    """Оценка шагов прогресса: загрузка каталога + сравнение строк."""
    n = max(int(row_count or 0), 0)
    stores = max(int(store_count or 0), 1)
    if n >= _FULL_CATALOG_THRESHOLD:
        per_store = _CATALOG_MAX_PAGES + n
    else:
        per_store = max((n + 39) // 40, 1) + n
    return max(per_store * stores, 1)

_RE_HEADER = re.compile(
    r"sku|артикул|vendor|offer|fact[_\s-]?length|fact[_\s-]?width|fact[_\s-]?height|длина|ширина|высота",
    re.I,
)


@dataclass
class PackagingDimRow:
    vendor_code: str
    fact_length: float
    fact_width: float
    fact_height: float


def _norm_vendor(v: str) -> str:
    return str(v or "").strip()


def _parse_float(val: str) -> Optional[float]:
    s = str(val or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _split_line(line: str) -> List[str]:
    if "\t" in line:
        return [p.strip() for p in line.split("\t")]
    if ";" in line:
        return [p.strip() for p in line.split(";")]
    if "," in line and line.count(",") >= 2:
        try:
            return [p.strip() for p in next(csv.reader(io.StringIO(line), delimiter=","))]
        except Exception:
            pass
    return [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]


def _is_header_row(parts: List[str]) -> bool:
    joined = " ".join(parts)
    return bool(_RE_HEADER.search(joined))


def _column_map(header_parts: List[str]) -> Optional[Dict[str, int]]:
    idx: Dict[str, int] = {}
    for i, raw in enumerate(header_parts):
        key = re.sub(r"[\s_\-]+", "", str(raw or "").strip().casefold())
        if not key:
            continue
        if key in ("sku", "артикул", "vendor", "vendorcode", "offer", "offerid", "артикулпродавца"):
            idx["sku"] = i
        elif key in ("factlength", "длина", "length", "len"):
            idx["length"] = i
        elif key in ("factwidth", "ширина", "width"):
            idx["width"] = i
        elif key in ("factheight", "высота", "height"):
            idx["height"] = i
    if "sku" in idx and {"length", "width", "height"}.issubset(idx.keys()):
        return idx
    return None


def parse_packaging_dims_text(text: str) -> Tuple[List[PackagingDimRow], List[str]]:
    """Разбор таблицы: sku, fact_length, fact_width, fact_height (таб / ; / CSV)."""
    warnings: List[str] = []
    rows: List[PackagingDimRow] = []
    seen: Set[str] = set()
    col_map: Optional[Dict[str, int]] = None

    raw_lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not raw_lines:
        return [], ["Нет данных в таблице"]

    for line_no, line in enumerate(raw_lines, start=1):
        parts = _split_line(line)
        if not parts:
            continue

        if col_map is None and _is_header_row(parts):
            mapped = _column_map(parts)
            if mapped:
                col_map = mapped
                continue
            if line_no == 1 and len(parts) >= 4 and not _parse_float(parts[0]):
                warnings.append("Первая строка похожа на заголовок — пропущена")
                continue

        if col_map:
            sku = _norm_vendor(parts[col_map["sku"]] if col_map["sku"] < len(parts) else "")
            fl = _parse_float(parts[col_map["length"]] if col_map["length"] < len(parts) else "")
            fw = _parse_float(parts[col_map["width"]] if col_map["width"] < len(parts) else "")
            fh = _parse_float(parts[col_map["height"]] if col_map["height"] < len(parts) else "")
        elif len(parts) >= 4:
            sku = _norm_vendor(parts[0])
            fl = _parse_float(parts[1])
            fw = _parse_float(parts[2])
            fh = _parse_float(parts[3])
        else:
            warnings.append(f"Строка {line_no}: мало колонок — пропущена")
            continue

        if not sku:
            warnings.append(f"Строка {line_no}: пустой артикул — пропущена")
            continue
        if fl is None or fw is None or fh is None:
            warnings.append(f"Строка {line_no} ({sku}): не удалось разобрать габариты — пропущена")
            continue

        key = sku.casefold()
        if key in seen:
            warnings.append(f"Дубликат артикула {sku} — оставлена первая строка")
            continue
        seen.add(key)
        rows.append(PackagingDimRow(vendor_code=sku, fact_length=fl, fact_width=fw, fact_height=fh))

    if not rows:
        return [], warnings or ["Не удалось разобрать ни одной строки"]
    return rows, warnings


def filter_dim_rows(
    rows: List[PackagingDimRow],
    vendor_codes: List[str],
) -> Tuple[List[PackagingDimRow], List[str]]:
    allowed = {_norm_vendor(v).casefold() for v in vendor_codes if _norm_vendor(v)}
    if not allowed:
        return rows, []
    out = [r for r in rows if _norm_vendor(r.vendor_code).casefold() in allowed]
    missing = allowed - {_norm_vendor(r.vendor_code).casefold() for r in out}
    warnings = [f"Артикул не найден в таблице: {v}" for v in sorted(missing)[:20]]
    if len(missing) > 20:
        warnings.append(f"…и ещё {len(missing) - 20} артикулов не в таблице")
    return out, warnings


def dims_rows_to_api(rows: List[PackagingDimRow]) -> List[dict]:
    return [
        {
            "vendor_code": r.vendor_code,
            "fact_length": r.fact_length,
            "fact_width": r.fact_width,
            "fact_height": r.fact_height,
        }
        for r in rows
    ]


def _extract_wb_dims(card: dict) -> Optional[Dict[str, float]]:
    dims = card.get("dimensions")
    if not isinstance(dims, dict):
        return None
    out: Dict[str, float] = {}
    for key in ("length", "width", "height"):
        val = _parse_float(dims.get(key))
        if val is None:
            return None
        out[key] = val
    return out


def _dims_equal(a: float, b: float, tol: float = _DIM_TOLERANCE_CM) -> bool:
    return abs(a - b) <= tol


def _compare_row(row: PackagingDimRow, card: Optional[dict]) -> dict:
    if not card:
        return {
            "vendor_code": row.vendor_code,
            "nm_id": None,
            "title": "",
            "status": "not_found",
            "message": "Артикул не найден в каталоге магазина",
            "fact_length": row.fact_length,
            "fact_width": row.fact_width,
            "fact_height": row.fact_height,
            "wb_length": None,
            "wb_width": None,
            "wb_height": None,
            "diff_length": None,
            "diff_width": None,
            "diff_height": None,
        }

    nm = int(card.get("nmID") or card.get("nmId") or 0)
    title = str(card.get("title") or "")[:120]
    wb_dims = _extract_wb_dims(card)
    if not wb_dims:
        return {
            "vendor_code": row.vendor_code,
            "nm_id": nm or None,
            "title": title,
            "status": "no_dims",
            "message": "В карточке WB нет габаритов упаковки",
            "fact_length": row.fact_length,
            "fact_width": row.fact_width,
            "fact_height": row.fact_height,
            "wb_length": None,
            "wb_width": None,
            "wb_height": None,
            "diff_length": None,
            "diff_width": None,
            "diff_height": None,
        }

    dl = round(row.fact_length - wb_dims["length"], 2)
    dw = round(row.fact_width - wb_dims["width"], 2)
    dh = round(row.fact_height - wb_dims["height"], 2)
    match = (
        _dims_equal(row.fact_length, wb_dims["length"])
        and _dims_equal(row.fact_width, wb_dims["width"])
        and _dims_equal(row.fact_height, wb_dims["height"])
    )
    diffs = []
    if not _dims_equal(row.fact_length, wb_dims["length"]):
        diffs.append(f"длина {dl:+.1f}")
    if not _dims_equal(row.fact_width, wb_dims["width"]):
        diffs.append(f"ширина {dw:+.1f}")
    if not _dims_equal(row.fact_height, wb_dims["height"]):
        diffs.append(f"высота {dh:+.1f}")

    return {
        "vendor_code": row.vendor_code,
        "nm_id": nm or None,
        "title": title,
        "status": "match" if match else "mismatch",
        "message": "Совпадает" if match else "; ".join(diffs),
        "fact_length": row.fact_length,
        "fact_width": row.fact_width,
        "fact_height": row.fact_height,
        "wb_length": wb_dims["length"],
        "wb_width": wb_dims["width"],
        "wb_height": wb_dims["height"],
        "diff_length": dl,
        "diff_width": dw,
        "diff_height": dh,
    }


def _index_cards_by_vendor(cards: List[dict]) -> Dict[str, dict]:
    card_by_vendor: Dict[str, dict] = {}
    for card in cards:
        vc = _norm_vendor(card.get("vendorCode") or card.get("supplierVendorCode") or "").casefold()
        if vc and vc not in card_by_vendor:
            card_by_vendor[vc] = card
    return card_by_vendor


async def _load_wb_cards_for_compare(
    client: WbContentClient,
    vendor_codes: List[str],
    *,
    progress_cb: Optional[ProgressCb] = None,
) -> Tuple[Dict[str, dict], dict]:
    """Загрузка карточек: полный каталог (большие списки) или пачками по артикулам."""
    codes = list(dict.fromkeys(v for v in vendor_codes if v))
    meta: dict = {}
    use_full_catalog = len(codes) >= _FULL_CATALOG_THRESHOLD

    if use_full_catalog:
        if progress_cb:
            progress_cb(0, _CATALOG_MAX_PAGES, "Загрузка каталога WB…")

        def _catalog_prog(cur: int, tot: int, detail: str) -> None:
            if progress_cb:
                progress_cb(cur, tot, detail)

        cards = await client.list_cards_all(
            max_pages=_CATALOG_MAX_PAGES,
            meta_out=meta,
            progress_cb=_catalog_prog,
        )
        meta["load_mode"] = "full_catalog"
    else:
        chunks = max(1, (len(codes) + 39) // 40)

        def _batch_prog(cur: int, tot: int, detail: str) -> None:
            if progress_cb:
                progress_cb(cur, tot, detail)

        cards = await client.list_cards_all(
            vendor_codes=codes,
            max_pages=max(5, chunks),
            meta_out=meta,
            progress_cb=_batch_prog,
        )
        meta["load_mode"] = "vendor_batches"

    meta["cards_loaded"] = len(cards)
    return _index_cards_by_vendor(cards), meta


async def compare_dims_for_store(
    api_key: str,
    *,
    rows: List[PackagingDimRow],
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    client = WbContentClient(api_key, timeout_s=600.0)
    vendor_codes = list(dict.fromkeys(r.vendor_code for r in rows if r.vendor_code))
    total = len(rows)

    card_by_vendor, load_meta = await _load_wb_cards_for_compare(
        client,
        vendor_codes,
        progress_cb=progress_cb,
    )
    load_steps = int(load_meta.get("pages_fetched") or 0)
    compare_total = max(load_steps + total, total, 1)

    results: List[dict] = []
    for i, row in enumerate(rows, start=1):
        key = _norm_vendor(row.vendor_code).casefold()
        card = card_by_vendor.get(key)
        results.append(_compare_row(row, card))
        if progress_cb:
            progress_cb(
                load_steps + i,
                compare_total,
                f"Сравнение {i}/{total}: {row.vendor_code}",
            )

    matched = sum(1 for r in results if r["status"] == "match")
    mismatched = sum(1 for r in results if r["status"] == "mismatch")
    not_found = sum(1 for r in results if r["status"] == "not_found")
    no_dims = sum(1 for r in results if r["status"] == "no_dims")

    return {
        "parsed": len(rows),
        "cards_found": len(card_by_vendor),
        "matched": matched,
        "mismatched": mismatched,
        "not_found": not_found,
        "no_dims": no_dims,
        "load_mode": load_meta.get("load_mode"),
        "catalog_truncated": bool(load_meta.get("truncated")),
        "catalog_cards": load_meta.get("cards_loaded"),
        "rows": results,
    }


async def compare_dims_multi_store(
    stores: List[Tuple[int, str, str]],
    *,
    rows: List[PackagingDimRow],
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    """stores: (store_id, store_name, api_key)."""
    out_stores: List[dict] = []
    total_stores = len(stores)

    for i, (store_id, store_name, api_key) in enumerate(stores):
        def _cb(
            cur: int,
            tot: int,
            detail: str,
            _name=store_name,
            _si=i,
        ) -> None:
            if progress_cb:
                safe_tot = max(int(tot or 0), 1)
                safe_cur = max(0, min(int(cur or 0), safe_tot))
                progress_cb(
                    safe_cur,
                    safe_tot,
                    f"Магазин {_si + 1}/{total_stores} · {_name}: {detail}",
                )

        if progress_cb:
            progress_cb(
                0,
                estimate_compare_steps(len(rows), 1),
                f"Магазин {i + 1}/{total_stores}: {store_name}…",
            )

        try:
            part = await compare_dims_for_store(api_key, rows=rows, progress_cb=_cb if progress_cb else None)
            part["store_id"] = store_id
            part["store_name"] = store_name
            out_stores.append(part)
        except Exception as e:
            log.exception("packaging_dims store %s failed: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:400],
                "rows": [],
            })

    return {"stores": out_stores}


def _dim_num(v: float) -> float:
    return round(float(v), 2)


def _card_has_weight_brutto(card: dict) -> bool:
    dims = card.get("dimensions")
    if not isinstance(dims, dict):
        return False
    try:
        return float(dims.get("weightBrutto") or 0) > 0
    except (TypeError, ValueError):
        return False


def _dims_need_cm_rounding(row: PackagingDimRow) -> bool:
    from .wb_certificates import _dim_cm_value

    for v in (row.fact_length, row.fact_width, row.fact_height):
        if abs(_dim_cm_value(v) - float(v)) > 0.001:
            return True
    return False


def _wb_cm_dims(row: PackagingDimRow) -> Tuple[int, int, int]:
    from .wb_certificates import _dim_cm_value

    return (
        _dim_cm_value(row.fact_length),
        _dim_cm_value(row.fact_width),
        _dim_cm_value(row.fact_height),
    )


def build_dims_update_payload(
    card: dict,
    row: PackagingDimRow,
    *,
    strip_char_ids: Optional[Set[int]] = None,
) -> Optional[dict]:
    """cards/update: подставить fact_* в dimensions, остальное карточки сохранить."""
    from .wb_certificates import (
        _dim_cm_value,
        _dim_wb_value,
        build_card_char_patches_payload,
        sanitize_wb_card_update_payload,
    )

    payload = build_card_char_patches_payload(
        card,
        {},
        vendor_code=row.vendor_code,
        strip_char_ids=strip_char_ids,
    )
    if not payload.get("sizes"):
        return None
    old_dims = card.get("dimensions") if isinstance(card.get("dimensions"), dict) else {}
    try:
        wb = float(old_dims.get("weightBrutto") or 0)
    except (TypeError, ValueError):
        wb = 0
    if wb <= 0:
        return None
    payload["dimensions"] = {
        "length": _dim_cm_value(row.fact_length),
        "width": _dim_cm_value(row.fact_width),
        "height": _dim_cm_value(row.fact_height),
        "weightBrutto": _dim_wb_value(wb),
    }
    return sanitize_wb_card_update_payload(payload, strip_char_ids=strip_char_ids)


def _preview_dims_message(cmp: dict, row: PackagingDimRow) -> str:
    l, w, h = _wb_cm_dims(row)
    wb_s = f"{l}×{w}×{h}"
    fact_s = f"{_dim_num(row.fact_length)}×{_dim_num(row.fact_width)}×{_dim_num(row.fact_height)}"
    if cmp.get("wb_length") is not None:
        old_s = f"{cmp.get('wb_length')}×{cmp.get('wb_width')}×{cmp.get('wb_height')}"
        msg = f"Будет {wb_s} см (сейчас {old_s})"
    else:
        msg = f"Будет {wb_s} см"
    if _dims_need_cm_rounding(row):
        msg += f" — в таблице {fact_s}, WB: только целые см"
    return msg


async def apply_dims_for_store(
    api_key: str,
    *,
    rows: List[PackagingDimRow],
    dry_run: bool = False,
    only_mismatch: bool = True,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    client = WbContentClient(api_key, timeout_s=600.0)
    vendor_codes = list(dict.fromkeys(r.vendor_code for r in rows if r.vendor_code))
    total = len(rows)

    card_by_vendor, load_meta = await _load_wb_cards_for_compare(
        client,
        vendor_codes,
        progress_cb=progress_cb,
    )
    load_steps = int(load_meta.get("pages_fetched") or 0)
    compare_total = max(load_steps + total, total, 1)

    named_field_ids: Set[int] = set()
    charcs_cache: Dict[int, List[dict]] = {}
    from .wb_certificates import collect_named_field_char_ids

    for card in card_by_vendor.values():
        try:
            sid = int(card.get("subjectID") or card.get("subjectId") or 0)
        except (TypeError, ValueError):
            sid = 0
        if not sid or sid in charcs_cache:
            continue
        try:
            charcs_cache[sid] = await client.get_subject_charcs(sid)
        except Exception as e:
            log.warning("packaging_dims charcs subject %s: %s", sid, e)
            charcs_cache[sid] = []
        named_field_ids |= collect_named_field_char_ids(charcs_cache[sid])

    results: List[dict] = []
    updates: List[dict] = []
    pending: List[dict] = []

    for i, row in enumerate(rows, start=1):
        key = _norm_vendor(row.vendor_code).casefold()
        card = card_by_vendor.get(key)
        cmp = _compare_row(row, card)

        if cmp["status"] == "match" and only_mismatch:
            cmp = {**cmp, "status": "skipped", "message": "Уже совпадает — не меняем"}
            results.append(cmp)
            if progress_cb:
                progress_cb(load_steps + i, compare_total, f"Пропуск: {row.vendor_code}")
            continue

        if cmp["status"] in ("not_found", "no_dims"):
            results.append(cmp)
            if progress_cb:
                progress_cb(load_steps + i, compare_total, f"{cmp['status']}: {row.vendor_code}")
            continue

        payload = build_dims_update_payload(
            card,
            row,
            strip_char_ids=named_field_ids or None,
        )
        if not payload:
            if not _card_has_weight_brutto(card):
                msg = "Нет веса упаковки (weightBrutto) в карточке WB"
            else:
                msg = "В карточке нет sizes (chrtID/skus) для обновления"
            cmp = {**cmp, "status": "error", "message": msg}
            results.append(cmp)
            if progress_cb:
                progress_cb(load_steps + i, compare_total, f"{msg}: {row.vendor_code}")
            continue

        row_out = {**cmp}
        if dry_run:
            row_out["status"] = "preview"
            row_out["message"] = _preview_dims_message(cmp, row)
        else:
            row_out["status"] = "pending"
            row_out["message"] = _preview_dims_message(cmp, row)

        updates.append(payload)
        pending.append(row_out)
        results.append(row_out)
        if progress_cb:
            progress_cb(load_steps + i, compare_total, f"Подготовлено: {row.vendor_code}")

    sent = 0
    errors: List[dict] = []
    if not dry_run and updates:
        send_total = len(updates)

        def _send_prog(cur: int, tot: int, detail: str) -> None:
            if progress_cb:
                progress_cb(compare_total + cur, compare_total + tot, detail)

        sent, batch_errors = await client.update_cards_batched(
            updates,
            progress_cb=_send_prog,
        )
        err_by_vc = {
            _norm_vendor(e.get("vendor_code") or "").casefold(): e
            for e in batch_errors
            if e.get("vendor_code")
        }
        for res in pending:
            key = _norm_vendor(res.get("vendor_code") or "").casefold()
            if key in err_by_vc:
                e = err_by_vc[key]
                msg = _format_wb_error(
                    HttpStatusError(
                        status=int(e.get("status") or 0),
                        body=str(e.get("body") or ""),
                    )
                )
                errors.append(e)
                res["status"] = "error"
                res["message"] = msg
                log.warning(
                    "WB dims update %s nm=%s: %s",
                    res.get("vendor_code"),
                    res.get("nm_id"),
                    msg,
                )
            else:
                res["status"] = "ok"
                res["message"] = "Отправлено на WB"

    prepared = len(updates)
    skipped = sum(1 for r in results if r["status"] == "skipped")
    preview = sum(1 for r in results if r["status"] == "preview")
    ok = sum(1 for r in results if r["status"] == "ok")
    err_n = sum(1 for r in results if r["status"] == "error")

    return {
        "dry_run": dry_run,
        "only_mismatch": only_mismatch,
        "parsed": len(rows),
        "cards_found": len(card_by_vendor),
        "prepared": prepared,
        "sent": sent,
        "skipped": skipped,
        "preview": preview,
        "ok": ok,
        "errors_count": err_n,
        "load_mode": load_meta.get("load_mode"),
        "catalog_truncated": bool(load_meta.get("truncated")),
        "catalog_cards": load_meta.get("cards_loaded"),
        "errors": errors,
        "rows": results,
    }


async def apply_dims_multi_store(
    stores: List[Tuple[int, str, str]],
    *,
    rows: List[PackagingDimRow],
    dry_run: bool = False,
    only_mismatch: bool = True,
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    out_stores: List[dict] = []
    total_stores = len(stores)

    for i, (store_id, store_name, api_key) in enumerate(stores):
        def _cb(
            cur: int,
            tot: int,
            detail: str,
            _name=store_name,
            _si=i,
        ) -> None:
            if progress_cb:
                safe_tot = max(int(tot or 0), 1)
                safe_cur = max(0, min(int(cur or 0), safe_tot))
                progress_cb(
                    safe_cur,
                    safe_tot,
                    f"Магазин {_si + 1}/{total_stores} · {_name}: {detail}",
                )

        if progress_cb:
            progress_cb(
                0,
                estimate_apply_steps(len(rows), 1, only_mismatch=only_mismatch),
                f"Магазин {i + 1}/{total_stores}: {store_name}…",
            )

        try:
            part = await apply_dims_for_store(
                api_key,
                rows=rows,
                dry_run=dry_run,
                only_mismatch=only_mismatch,
                progress_cb=_cb if progress_cb else None,
            )
            part["store_id"] = store_id
            part["store_name"] = store_name
            out_stores.append(part)
        except Exception as e:
            log.exception("packaging_dims apply store %s failed: %s", store_id, e)
            out_stores.append({
                "store_id": store_id,
                "store_name": store_name,
                "error": str(e)[:400],
                "rows": [],
            })

    return {"stores": out_stores, "dry_run": dry_run}


def estimate_apply_steps(row_count: int, store_count: int = 1, *, only_mismatch: bool = True) -> int:
    """Загрузка каталога + отправка пачками по 100."""
    base = estimate_compare_steps(row_count, store_count)
    n = max(int(row_count or 0), 0)
    send_batches = max(1, (n + 99) // 100)
    return max(base + send_batches, 1)
