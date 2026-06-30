"""Общий разбор таблицы сертификатов и деклараций (WB + Ozon)."""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

_RE_HEADER = re.compile(r"артикул|vendor|сертифик|декларац|дата\s*рег|действует", re.I)
_RE_DECLARATION = re.compile(
    r"(?:^|[\s/])Д[\.\-]|N\s*RU\s*Д|Д-RU|Д-CN|ДЕКЛАРАЦ",
    re.I,
)
_RE_CERTIFICATE = re.compile(
    r"(?:^|[\s/])С[\.\-]|N\s*RU\s*С|С-RU|С-CN|RU\.С|СЕРТИФИКАТ",
    re.I,
)


@dataclass
class CertInputRow:
    vendor_code: str
    doc_number: str
    reg_date: str = ""
    valid_until: str = ""


def _norm_vendor(v: str) -> str:
    return str(v or "").strip()


def _norm_date(s: str) -> str:
    t = str(s or "").strip()
    if not t:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        return f"{m.group(3)}.{m.group(2)}.{m.group(1)}"
    m = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})", t)
    if m:
        return f"{int(m.group(1)):02d}.{int(m.group(2)):02d}.{m.group(3)}"
    return t


def _split_line(line: str) -> List[str]:
    if "\t" in line:
        return [p.strip() for p in line.split("\t")]
    if ";" in line:
        return [p.strip() for p in line.split(";")]
    return [p.strip() for p in re.split(r"\s{2,}", line) if p.strip()]


def detect_doc_type(doc_number: str) -> str:
    """declaration | certificate | unknown — по номеру документа ЕАЭС/ГОСТ."""
    text = str(doc_number or "").strip()
    if not text:
        return "unknown"
    if _RE_DECLARATION.search(text):
        return "declaration"
    if _RE_CERTIFICATE.search(text):
        return "certificate"
    return "unknown"


def doc_type_label(doc_type: str) -> str:
    return {
        "declaration": "Декларация",
        "certificate": "Сертификат",
        "unknown": "—",
    }.get(str(doc_type or "").strip(), "—")


def parse_certificates_text(text: str) -> Tuple[List[CertInputRow], List[str]]:
    """Парсит вставку из таблицы (TSV/CSV) или plain text."""
    warnings: List[str] = []
    rows: List[CertInputRow] = []
    lines = [ln.strip() for ln in (text or "").replace("\r\n", "\n").split("\n") if ln.strip()]
    if not lines:
        return [], ["Пустой ввод"]

    start = 0
    if _RE_HEADER.search(lines[0]):
        start = 1

    for i, line in enumerate(lines[start:], start=start + 1):
        parts = _split_line(line)
        if len(parts) < 2:
            warnings.append(f"Строка {i}: мало колонок — пропуск")
            continue
        vendor = _norm_vendor(parts[0])
        doc = str(parts[1] or "").strip()
        if not vendor:
            warnings.append(f"Строка {i}: пустой артикул — пропуск")
            continue
        if not doc:
            warnings.append(f"Строка {i}: пустой номер документа — пропуск")
            continue
        reg = _norm_date(parts[2]) if len(parts) > 2 else ""
        until = _norm_date(parts[3]) if len(parts) > 3 else ""
        rows.append(CertInputRow(vendor_code=vendor, doc_number=doc, reg_date=reg, valid_until=until))

    if not rows:
        warnings.append("Не распознано ни одной строки с данными")
    return rows, warnings


def filter_cert_rows(
    rows: List[CertInputRow],
    vendor_codes: Optional[List[str]] = None,
) -> Tuple[List[CertInputRow], List[str]]:
    """Оставляет только строки с артикулами из vendor_codes (если список не пуст)."""
    if not vendor_codes:
        return rows, []
    allowed = {_norm_vendor(v).casefold() for v in vendor_codes if _norm_vendor(v)}
    if not allowed:
        return rows, []
    out: List[CertInputRow] = []
    missing: List[str] = []
    seen_allowed: Set[str] = set()
    for row in rows:
        key = _norm_vendor(row.vendor_code).casefold()
        if key in allowed:
            out.append(row)
            seen_allowed.add(key)
    for v in vendor_codes:
        key = _norm_vendor(v).casefold()
        if key and key not in seen_allowed:
            missing.append(_norm_vendor(v))
    warnings: List[str] = []
    if missing:
        warnings.append(f"В таблице нет артикулов: {', '.join(missing[:20])}" + (
            f" и ещё {len(missing) - 20}" if len(missing) > 20 else ""
        ))
    return out, warnings


def parse_certificates_file(content: bytes, filename: str = "") -> Tuple[List[CertInputRow], List[str]]:
    """CSV/TSV файл (без xlsx — вставка из Excel копируется как TSV)."""
    name = (filename or "").lower()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = content.decode("cp1251")
        except UnicodeDecodeError:
            return [], ["Не удалось прочитать файл (кодировка)"]

    if name.endswith(".csv") or ";" in text.split("\n", 1)[0]:
        reader = csv.reader(io.StringIO(text), delimiter=";" if ";" in text.split("\n", 1)[0] else ",")
        lines = ["\t".join(row) for row in reader if any(str(c).strip() for c in row)]
        return parse_certificates_text("\n".join(lines))
    return parse_certificates_text(text)


def cert_row_to_dict(row: CertInputRow) -> dict:
    doc_type = detect_doc_type(row.doc_number)
    return {
        "vendor_code": row.vendor_code,
        "doc_number": row.doc_number,
        "reg_date": row.reg_date,
        "valid_until": row.valid_until,
        "doc_type": doc_type,
        "doc_type_label": doc_type_label(doc_type),
    }


def cert_rows_to_api(rows: List[CertInputRow]) -> List[dict]:
    return [cert_row_to_dict(r) for r in rows]
