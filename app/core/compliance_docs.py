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

# Похожие кириллические буквы → латиница (в коде документа после RU Д-/С-)
_HOMO_TO_LAT = {
    "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K", "М": "M",
    "О": "O", "Р": "P", "Т": "T", "Х": "X",
    "а": "a", "в": "b", "с": "c", "е": "e", "н": "h", "к": "k", "м": "m",
    "о": "o", "р": "p", "т": "t", "х": "x",
}


def _latinize_homoglyphs(text: str) -> str:
    return "".join(_HOMO_TO_LAT.get(ch, ch) for ch in str(text or ""))


def normalize_doc_number(num: str) -> str:
    """Латиница в коде документа после RU Д-/С- (Excel часто подставляет РА вместо PA)."""
    s = str(num or "").strip()
    if not s:
        return s
    m = re.search(r"^(.*?RU\s*[ДСDCдс]\s*[-\u2013])(.*)$", s, re.I)
    if m:
        return m.group(1) + _latinize_homoglyphs(m.group(2))
    m = re.search(r"^(RU\s*[ДСDCдс]\s*[-\u2013])(.*)$", s, re.I)
    if m:
        return m.group(1) + _latinize_homoglyphs(m.group(2))
    return s


def doc_number_was_normalized(num: str) -> bool:
    raw = str(num or "").strip()
    return bool(raw) and normalize_doc_number(raw) != raw


@dataclass
class CertInputRow:
    vendor_code: str
    doc_number: str
    reg_date: str = ""
    valid_until: str = ""


def _norm_vendor(v: str) -> str:
    """Артикул из таблицы: trim, Excel 528657007.0 → 528657007."""
    s = str(v or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        return s.split(".", 1)[0]
    try:
        if re.fullmatch(r"[\d.eE+\-]+", s) and ("e" in s.lower() or "." in s):
            f = float(s)
            if abs(f - round(f)) < 1e-9:
                return str(int(round(f)))
    except (TypeError, ValueError, OverflowError):
        pass
    return s


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
    """declaration | certificate | unknown — по номеру документа ЕАЭС/ГОСТ/РОСС."""
    text = str(doc_number or "").strip()
    if not text:
        return "unknown"
    # РОСС/POCC RU: буква после RU — Д = декларация, С = сертификат (инструкция WB)
    m_ross = re.search(
        r"^(?:POCC|POSS|РОСС|ROSS)\s+RU\s+([СCДD])",
        text,
        re.I,
    )
    if m_ross:
        letter = m_ross.group(1).upper().replace("D", "Д").replace("C", "С")
        if letter == "Д":
            return "declaration"
        if letter == "С":
            return "certificate"
    if re.search(r"^RU\.(?:С|C)\b", text, re.I):
        return "certificate"
    if re.search(r"^RU\s+Д", text, re.I):
        return "declaration"
    if re.search(r"ЕАЭС\s+N\s+RU\s+Д", text, re.I):
        return "declaration"
    if re.search(r"ЕАЭС\s+N\s+RU\s+С", text, re.I):
        return "certificate"
    if re.search(r"ЕАЭС\s+RU\s+С", text, re.I):
        return "certificate"
    if re.search(r"ТС\s+N\s+RU\s+Д", text, re.I):
        return "declaration"
    if re.search(r"ТС\s+RU\s+С", text, re.I):
        return "certificate"
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
        if doc_number_was_normalized(doc):
            norm = normalize_doc_number(doc)
            warnings.append(
                f"Строка {i} ({vendor}): в коде номера есть кириллица, похожая на латиницу "
                f"(РА/PA и т.д.) — отправим как в таблице: «{doc}»"
            )
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
