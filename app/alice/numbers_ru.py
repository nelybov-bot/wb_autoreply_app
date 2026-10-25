"""Парсер русских числительных для складских команд (без внешних библиотек)."""
from __future__ import annotations

import re
from typing import Optional, Tuple

# Базовые слова → значение
_ONES = {
    "ноль": 0,
    "один": 1,
    "одна": 1,
    "одно": 1,
    "одну": 1,
    "два": 2,
    "две": 2,
    "три": 3,
    "четыре": 4,
    "пять": 5,
    "шесть": 6,
    "семь": 7,
    "восемь": 8,
    "девять": 9,
}

_TEENS = {
    "десять": 10,
    "одиннадцать": 11,
    "двенадцать": 12,
    "тринадцать": 13,
    "четырнадцать": 14,
    "пятнадцать": 15,
    "шестнадцать": 16,
    "семнадцать": 17,
    "восемнадцать": 18,
    "девятнадцать": 19,
}

_TENS = {
    "двадцать": 20,
    "тридцать": 30,
    "сорок": 40,
    "пятьдесят": 50,
    "шестьдесят": 60,
    "семьдесят": 70,
    "восемьдесят": 80,
    "девяносто": 90,
}

_HUNDREDS = {
    "сто": 100,
    "двести": 200,
    "триста": 300,
    "четыреста": 400,
    "пятьсот": 500,
    "шестьсот": 600,
    "семьсот": 700,
    "восемьсот": 800,
    "девятьсот": 900,
}

_ALL_WORDS = set(_ONES) | set(_TEENS) | set(_TENS) | set(_HUNDREDS) | {"тысяча", "тысячи", "тысяч"}

_DIGIT_RE = re.compile(r"\d+")


def normalize_text(text: str) -> str:
    s = (text or "").lower().replace("ё", "е")
    s = re.sub(r"[^\w\s\-]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_number_words(tokens: list[str]) -> Optional[Tuple[int, int]]:
    """
    Парсит префикс токенов как русское число.
    Возвращает (value, consumed_token_count) или None.
    Поддержка разумного диапазона складских операций (до тысяч).
    """
    if not tokens:
        return None

    i = 0
    total = 0
    current = 0
    consumed = 0

    while i < len(tokens):
        w = tokens[i]
        if w in _HUNDREDS:
            current += _HUNDREDS[w]
            i += 1
            consumed = i
            continue
        if w in _TENS:
            current += _TENS[w]
            i += 1
            consumed = i
            # optional ones after tens
            if i < len(tokens) and tokens[i] in _ONES:
                current += _ONES[tokens[i]]
                i += 1
                consumed = i
            continue
        if w in _TEENS:
            current += _TEENS[w]
            i += 1
            consumed = i
            continue
        if w in _ONES:
            current += _ONES[w]
            i += 1
            consumed = i
            continue
        if w in ("тысяча", "тысячи", "тысяч"):
            if current == 0:
                current = 1
            total += current * 1000
            current = 0
            i += 1
            consumed = i
            continue
        break

    if consumed == 0:
        return None
    return total + current, consumed


def extract_leading_quantity(text: str) -> Optional[Tuple[int, str]]:
    """
    Извлекает количество в начале фразы (цифра или слова).
    Возвращает (qty, remainder_text) или None.
    """
    norm = normalize_text(text)
    if not norm:
        return None
    tokens = norm.split()
    if not tokens:
        return None

    # digit first
    if _DIGIT_RE.fullmatch(tokens[0]):
        qty = int(tokens[0])
        rest = " ".join(tokens[1:])
        return qty, rest

    parsed = parse_number_words(tokens)
    if not parsed:
        return None
    qty, n = parsed
    rest = " ".join(tokens[n:])
    return qty, rest


def parse_quantity_anywhere(text: str) -> Optional[int]:
    """Находит первое разумное количество: цифру или числительное."""
    norm = normalize_text(text)
    tokens = norm.split()
    for i, tok in enumerate(tokens):
        if _DIGIT_RE.fullmatch(tok):
            val = int(tok)
            # SKU обычно длинный (>=5); quantity обычно короче
            if len(tok) <= 4:
                return val
            continue
        parsed = parse_number_words(tokens[i:])
        if parsed:
            return parsed[0]
    return None


def parse_russian_number(phrase: str) -> Optional[int]:
    """Парсит целую фразу как одно число (слова или цифры)."""
    norm = normalize_text(phrase)
    if not norm:
        return None
    if _DIGIT_RE.fullmatch(norm.replace(" ", "")):
        return int(norm.replace(" ", ""))
    tokens = norm.split()
    parsed = parse_number_words(tokens)
    if not parsed:
        return None
    value, consumed = parsed
    if consumed != len(tokens):
        return None
    return value


def parse_under_1000(tokens: list[str]) -> Optional[Tuple[int, int]]:
    """Одна группа 0..999 (сотни-десятки-единицы). Без «тысяч»."""
    if not tokens:
        return None
    i = 0
    value = 0
    started = False
    if tokens[i] in _HUNDREDS:
        value += _HUNDREDS[tokens[i]]
        i += 1
        started = True
    if i < len(tokens) and tokens[i] in _TENS:
        value += _TENS[tokens[i]]
        i += 1
        started = True
        if i < len(tokens) and tokens[i] in _ONES:
            value += _ONES[tokens[i]]
            i += 1
    elif i < len(tokens) and tokens[i] in _TEENS:
        value += _TEENS[tokens[i]]
        i += 1
        started = True
    elif i < len(tokens) and tokens[i] in _ONES:
        value += _ONES[tokens[i]]
        i += 1
        started = True
    if not started:
        return None
    return value, i


def extract_spoken_sku(text: str) -> Optional[Tuple[int, str]]:
    """
    Достаёт артикул из русской речи Алисы.

    Поддержка:
    - «пятьсот сорок девять тысяч семьсот сорок восемь» → 549748
    - «пятьсот сорок девять семьсот сорок восемь» (две тройки) → 549748
    - «пять четыре девять семь четыре восемь» (по цифрам) → 549748

    Возвращает (sku, matched_phrase) или None.
    """
    norm = normalize_text(text)
    tokens = norm.split()
    if not tokens:
        return None

    best: Optional[Tuple[int, str]] = None

    for start in range(len(tokens)):
        if tokens[start] not in _ALL_WORDS:
            continue
        cand = _sku_from_tokens(tokens[start:])
        if not cand:
            continue
        sku, n = cand
        phrase = " ".join(tokens[start : start + n])
        if best is None or len(str(sku)) > len(str(best[0])):
            best = (sku, phrase)
    return best


def _sku_from_tokens(tokens: list[str]) -> Optional[Tuple[int, int]]:
    """Возвращает (sku, consumed_token_count) начиная с tokens[0]."""
    if not tokens:
        return None

    # 1) По цифрам: пять четыре девять …
    digits: list[str] = []
    i = 0
    while i < len(tokens) and tokens[i] in _ONES:
        digits.append(str(_ONES[tokens[i]]))
        i += 1
    if 4 <= len(digits) <= 9:
        return int("".join(digits)), i

    # 2) С «тысяч»: [AAA] тысяч [BBB]
    g1 = parse_under_1000(tokens)
    if g1:
        val1, n1 = g1
        if n1 < len(tokens) and tokens[n1] in ("тысяча", "тысячи", "тысяч"):
            total = val1 * 1000
            consumed = n1 + 1
            g2 = parse_under_1000(tokens[consumed:])
            if g2:
                total += g2[0]
                consumed += g2[1]
            if 4 <= len(str(total)) <= 9:
                return total, consumed

    # 3) Склейка групп <1000: «пятьсот сорок девять семьсот сорок восемь»
    parts: list[str] = []
    pos = 0
    while pos < len(tokens):
        if tokens[pos] in ("тысяча", "тысячи", "тысяч"):
            break
        g = parse_under_1000(tokens[pos:])
        if not g:
            break
        val, n = g
        parts.append(str(val))
        pos += n
        joined = "".join(parts)
        if len(joined) >= 9:
            break
    joined = "".join(parts)
    # две+ группы или одна группа из ≥5 цифр (редко) / ≥4 при одной группе только если ≥5
    if 5 <= len(joined) <= 9 and len(parts) >= 1:
        if len(parts) >= 2 or len(joined) >= 5:
            return int(joined), pos
    if 4 <= len(joined) <= 9 and len(parts) >= 2:
        return int(joined), pos

    return None


def sku_from_nlu_numbers(numbers: list[int]) -> Optional[int]:
    """
    YANDEX.NUMBER сущности: одно длинное число или несколько коротких групп.
    [549748] → 549748; [549, 748] → 549748
    """
    vals = [int(x) for x in numbers if isinstance(x, (int, float))]
    vals = [int(x) for x in vals if int(x) >= 0]
    if not vals:
        return None
    if len(vals) == 1:
        v = vals[0]
        if 4 <= len(str(v)) <= 9:
            return v
        return None
    # склейка групп (каждая обычно < 1000)
    parts = [str(v) for v in vals if v < 10000]
    if not parts:
        return None
    joined = "".join(parts)
    if 4 <= len(joined) <= 9:
        return int(joined)
    return None
