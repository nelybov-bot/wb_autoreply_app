"""Минимальная генерация PDF без внешних зависимостей (запись реестра ФСА)."""
from __future__ import annotations

import re
from typing import Iterable, List

_CYR_MAP = {
    "А": "A", "Б": "B", "В": "V", "Г": "G", "Д": "D", "Е": "E", "Ё": "E", "Ж": "Zh", "З": "Z",
    "И": "I", "Й": "Y", "К": "K", "Л": "L", "М": "M", "Н": "N", "О": "O", "П": "P", "Р": "R",
    "С": "S", "Т": "T", "У": "U", "Ф": "F", "Х": "Kh", "Ц": "Ts", "Ч": "Ch", "Ш": "Sh",
    "Щ": "Sch", "Ъ": "", "Ы": "Y", "Ь": "", "Э": "E", "Ю": "Yu", "Я": "Ya",
}


def _latinize(text: str) -> str:
    out: List[str] = []
    for ch in str(text or ""):
        up = ch.upper()
        if up in _CYR_MAP:
            rep = _CYR_MAP[up]
            out.append(rep.lower() if ch.islower() else rep)
        elif ord(ch) < 128:
            out.append(ch)
        else:
            out.append("?")
    return "".join(out)


def _pdf_escape(text: str) -> str:
    return (
        _latinize(text)
        .replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )


def make_registry_pdf(
    *,
    title: str,
    lines: Iterable[str],
) -> bytes:
    """Одностраничный PDF (Helvetica, Latin) для загрузки в Ozon."""
    content_lines = [_pdf_escape(str(title or "Registry record"))]
    for ln in lines:
        t = str(ln or "").strip()
        if t:
            content_lines.append(_pdf_escape(t))

    y = 780
    stream_parts = ["BT", "/F1 11 Tf"]
    for i, line in enumerate(content_lines[:40]):
        if i:
            stream_parts.append(f"1 0 0 1 50 {y} Tm")
        else:
            stream_parts.append(f"1 0 0 1 50 {y} Tm")
        stream_parts.append(f"({line}) Tj")
        y -= 16
        if y < 60:
            break
    stream_parts.append("ET")
    stream = "\n".join(stream_parts).encode("latin-1", errors="replace")
    stream_len = len(stream)

    objects: List[bytes] = []
    objects.append(b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n")
    objects.append(b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n")
    objects.append(
        b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>endobj\n"
    )
    objects.append(
        b"4 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n"
    )
    objects.append(
        f"5 0 obj<< /Length {stream_len} >>stream\n".encode("ascii")
        + stream
        + b"\nendstream endobj\n"
    )

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf))
        pdf.extend(obj)
    xref_pos = len(pdf)
    pdf.extend(f"xref\n0 {len(offsets)}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode("ascii")
    )
    return bytes(pdf)


def is_probably_pdf(data: bytes) -> bool:
    return bool(data) and data[:4] == b"%PDF"
