"""Сравнение фактических габаритов упаковки с данными карточек WB."""
from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .wb_content_client import WbContentClient

log = logging.getLogger("packaging_dims")

ProgressCb = Callable[[int, int, str], None]

_DIM_TOLERANCE_CM = 0.05

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


async def compare_dims_for_store(
    api_key: str,
    *,
    rows: List[PackagingDimRow],
    progress_cb: Optional[ProgressCb] = None,
) -> dict:
    client = WbContentClient(api_key, timeout_s=120.0)
    vendor_codes = list(dict.fromkeys(r.vendor_code for r in rows if r.vendor_code))

    if progress_cb:
        progress_cb(0, max(len(rows), 1), "Загрузка карточек WB…")

    cards = await client.list_cards_all(
        vendor_codes=vendor_codes,
        max_pages=max(10, len(vendor_codes) // 50 + 5),
    )
    card_by_vendor: Dict[str, dict] = {}
    for card in cards:
        vc = _norm_vendor(card.get("vendorCode") or card.get("supplierVendorCode") or "").casefold()
        if vc and vc not in card_by_vendor:
            card_by_vendor[vc] = card

    results: List[dict] = []
    total = len(rows)
    for i, row in enumerate(rows, start=1):
        key = _norm_vendor(row.vendor_code).casefold()
        card = card_by_vendor.get(key)
        results.append(_compare_row(row, card))
        if progress_cb:
            progress_cb(i, total, f"Сравнение: {row.vendor_code}")

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
