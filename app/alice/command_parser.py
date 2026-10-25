"""Детерминированный intent-parser голосовых команд склада."""
from __future__ import annotations

import re
from typing import Optional, Tuple

from .models import Intent, ParsedCommand
from .numbers_ru import (
    extract_spoken_sku,
    normalize_text,
    parse_number_words,
    parse_russian_number,
    sku_from_nlu_numbers,
)

# SKU: обычно 5–7 цифр; допускаем 4–9
_SKU_RE = re.compile(r"\b(\d{4,9})\b")
_SHORT_QTY_RE = re.compile(r"\b(\d{1,3})\b")

_CONFIRM = {
    "да",
    "ага",
    "угу",
    "подтверждаю",
    "подтвердить",
    "верно",
    "списывай",
    "выполняй",
    "да списывай",
    "да выполняй",
    "да подтверждаю",
}

_CANCEL = {
    "нет",
    "отмена",
    "отмени",
    "не надо",
    "не списывай",
    "стоп",
    "отбой",
    "не надо списывать",
}

_INCREASE_VERBS = (
    "добавь",
    "добавить",
    "прибавь",
    "прибавить",
    "плюс",
    "плюсани",
    "накинь",
    "накинуть",
    "пришло",
    "пришел",
    "пришла",
)

_DECREASE_VERBS = (
    "спиши",
    "списать",
    "убавь",
    "убавить",
    "вычти",
    "вычесть",
    "минус",
    "сними",
    "снять",
    "убери",
    "убрать",
)

_SET_VERBS = (
    "поставь остаток",
    "установи остаток",
    "сделай остаток",
    "поставь",
    "установи",
    "сделай",
)

_STOCK_TRIGGERS = (
    "остаток",
    "сколько осталось",
    "сколько есть",
    "сколько штук",
    "проверь остаток",
    "че там по",
    "чё там по",
    "что там по",
    "сколько",
)

_LOCATION_TRIGGERS = (
    "где лежит",
    "где находится",
    "на какой полке",
    "адрес",
    "полка",
    "где он",
    "где",
)

_STILL_STOCK = (
    "сколько теперь",
    "а сейчас сколько",
    "сейчас сколько",
    "сколько сейчас",
)

_CONTEXT_MORE = (
    "еще одну",
    "ещё одну",
    "еще две",
    "ещё две",
    "еще один",
    "ещё один",
    "еще",
    "ещё",
)


def _extract_sku(
    text: str,
    *,
    nlu_numbers: Optional[list[int]] = None,
) -> tuple[Optional[int], Optional[str]]:
    """
    Извлекает SKU.
    Приоритет: цифры в тексте → речь словами → YANDEX.NUMBER.
    Возвращает (sku, spoken_phrase_to_strip).
    """
    matches = list(_SKU_RE.finditer(text))
    if matches:
        long_ones = [m for m in matches if len(m.group(1)) >= 5]
        chosen = long_ones[0] if long_ones else matches[0]
        return int(chosen.group(1)), None

    spoken = extract_spoken_sku(text)
    if spoken:
        return spoken[0], spoken[1]

    from_nlu = sku_from_nlu_numbers(nlu_numbers or [])
    if from_nlu is not None:
        return from_nlu, None

    return None, None


def _strip_sku(text: str, sku: Optional[int], spoken_phrase: Optional[str] = None) -> str:
    out = text
    if spoken_phrase:
        out = out.replace(spoken_phrase, " ")
    if sku is not None:
        out = re.sub(rf"\b{sku}\b", " ", out)
    return re.sub(r"\s+", " ", out).strip()


def _find_verb(text: str, verbs: Tuple[str, ...]) -> Optional[str]:
    for v in sorted(verbs, key=len, reverse=True):
        if re.search(rf"(?<!\w){re.escape(v)}(?!\w)", text):
            return v
    return None


def _quantity_near_verb(text: str, verb: str) -> Optional[int]:
    """Количество сразу после/до глагола или в хвосте фразы."""
    # «плюс 5», «минус 3»
    m = re.search(rf"{re.escape(verb)}\s+(\d{{1,4}})\b", text)
    if m:
        return int(m.group(1))

    # слова после глагола
    after = text.split(verb, 1)[-1].strip()
    after = re.sub(
        r"\b(штук[аиу]?|шт|единиц[уыа]?|еще|ещё|одну|одну штуку)\b",
        " ",
        after,
    )
    after = re.sub(r"\s+", " ", after).strip()
    tokens = after.split()
    # убрать служебные «с артикула …»
    filtered = []
    skip_next_sku = False
    for i, tok in enumerate(tokens):
        if tok in ("с", "к", "у", "артикула", "артикул", "товара", "товар"):
            skip_next_sku = True
            continue
        if skip_next_sku and re.fullmatch(r"\d{4,9}", tok):
            skip_next_sku = False
            continue
        skip_next_sku = False
        filtered.append(tok)

    if filtered:
        if re.fullmatch(r"\d{1,4}", filtered[0]):
            return int(filtered[0])
        parsed = parse_number_words(filtered)
        if parsed:
            return parsed[0]

    # «две» перед глаголом: «две добавь» редко; чаще «добавь две»
    before = text.split(verb, 1)[0].strip()
    before_tokens = before.split()
    if before_tokens:
        # last number-like chunk
        for i in range(len(before_tokens) - 1, -1, -1):
            chunk = before_tokens[i:]
            if re.fullmatch(r"\d{1,4}", chunk[0]):
                return int(chunk[0])
            parsed = parse_number_words(chunk)
            if parsed and parsed[1] == len(chunk):
                return parsed[0]
            break

    # «еще одну» / «одну»
    if re.search(r"\b(еще|ещё)?\s*(одну|один|одна)\b", text):
        return 1
    if re.search(r"\b(еще|ещё)?\s*(две|два)\b", text):
        return 2

    return None


def _set_stock_quantity(text: str) -> Optional[int]:
    """«поставь остаток 20» / «установи остаток двадцать»."""
    m = re.search(r"остаток\s+(\d{1,6})\b", text)
    if m:
        return int(m.group(1))
    m = re.search(r"остаток\s+([а-я\s]+)$", text)
    if m:
        return parse_russian_number(m.group(1).strip())
    # «поставь 20» без слова остаток — только если есть set-verb
    m = re.search(r"(?:поставь|установи|сделай)\s+(\d{1,6})\b", text)
    if m:
        return int(m.group(1))
    return None


def parse_command(
    raw_text: str,
    *,
    nlu_numbers: Optional[list[int]] = None,
) -> ParsedCommand:
    raw = (raw_text or "").strip()
    text = normalize_text(raw)
    if not text:
        return ParsedCommand(intent=Intent.UNKNOWN, raw_text=raw)

    # точные confirm/cancel (короткие фразы)
    compact = re.sub(r"\s+", " ", text).strip()
    if compact in _CONFIRM or compact.startswith("да,"):
        return ParsedCommand(intent=Intent.CONFIRM, raw_text=raw)
    if compact in _CANCEL:
        return ParsedCommand(intent=Intent.CANCEL, raw_text=raw)

    sku, spoken_phrase = _extract_sku(text, nlu_numbers=nlu_numbers)
    work = _strip_sku(text, sku, spoken_phrase)
    work = re.sub(r"\b(артикул[а]?|товар[а]?)\b", " ", work)
    work = re.sub(r"\s+", " ", work).strip()

    # SET_STOCK
    if any(v in text for v in ("поставь остаток", "установи остаток", "сделай остаток")) or (
        _find_verb(work, ("поставь", "установи", "сделай")) and "остаток" in text
    ):
        qty = _set_stock_quantity(text)
        return ParsedCommand(
            intent=Intent.SET_STOCK,
            sku=sku,
            quantity=qty,
            raw_text=raw,
        )

    # INCREASE
    inc = _find_verb(work, _INCREASE_VERBS) or _find_verb(text, _INCREASE_VERBS)
    if inc:
        qty = _quantity_near_verb(text if sku else work, inc)
        if qty is None and re.search(r"\b(еще|ещё)\b", text):
            qty = 1
        return ParsedCommand(
            intent=Intent.INCREASE_STOCK,
            sku=sku,
            quantity=qty,
            raw_text=raw,
        )

    # DECREASE
    dec = _find_verb(work, _DECREASE_VERBS) or _find_verb(text, _DECREASE_VERBS)
    if dec:
        qty = _quantity_near_verb(text if sku else work, dec)
        if qty is None and re.search(r"\b(еще|ещё)\b", text):
            qty = 1
        return ParsedCommand(
            intent=Intent.DECREASE_STOCK,
            sku=sku,
            quantity=qty,
            raw_text=raw,
        )

    # «ещё одну / ещё две» без глагола — неоднозначно
    if any(compact == c or compact.startswith(c + " ") for c in _CONTEXT_MORE) or re.fullmatch(
        r"(еще|ещё)(\s+\w+){0,3}", compact
    ):
        # количество
        qty = 1
        if re.search(r"\b(две|два|2)\b", compact):
            qty = 2
        elif re.search(r"\b(три|3)\b", compact):
            qty = 3
        else:
            m = _SHORT_QTY_RE.search(compact)
            if m and len(m.group(1)) <= 2:
                qty = int(m.group(1))
        return ParsedCommand(
            intent=Intent.CLARIFY,
            sku=sku,
            quantity=qty,
            raw_text=raw,
            ambiguous=True,
        )

    # контекстный остаток без SKU
    if any(t in compact for t in _STILL_STOCK) or compact in ("сколько", "остаток"):
        return ParsedCommand(intent=Intent.GET_STOCK, sku=sku, raw_text=raw)

    # локация
    for trig in sorted(_LOCATION_TRIGGERS, key=len, reverse=True):
        if trig in text or compact == trig.rstrip():
            # «полка 789868» / «адрес 789868»
            return ParsedCommand(intent=Intent.GET_LOCATION, sku=sku, raw_text=raw)

    # остаток / сколько
    for trig in sorted(_STOCK_TRIGGERS, key=len, reverse=True):
        if trig in text:
            return ParsedCommand(intent=Intent.GET_STOCK, sku=sku, raw_text=raw)

    # просто артикул → select + можно считать как GET_STOCK
    if sku is not None and re.fullmatch(rf"(артикул[а]?\s+)?{sku}", text):
        return ParsedCommand(intent=Intent.SELECT_PRODUCT, sku=sku, raw_text=raw)

    # «789868» только число
    if sku is not None and compact == str(sku):
        return ParsedCommand(intent=Intent.SELECT_PRODUCT, sku=sku, raw_text=raw)

    # артикул только словами / NLU без других команд
    if sku is not None and spoken_phrase and (
        compact == spoken_phrase
        or compact in (f"артикул {spoken_phrase}", f"артикула {spoken_phrase}")
    ):
        return ParsedCommand(intent=Intent.SELECT_PRODUCT, sku=sku, raw_text=raw)

    if sku is not None and not any(
        v in text for v in ("добав", "прибав", "спиш", "убав", "минус", "плюс", "поставь", "установи")
    ):
        # «какой остаток …» уже пойман выше; голый артикул словами → выбор товара
        cleaned = work.strip()
        if not cleaned or cleaned in ("какой", "какая", "какие", "назови", "покажи"):
            return ParsedCommand(intent=Intent.SELECT_PRODUCT, sku=sku, raw_text=raw)

    return ParsedCommand(intent=Intent.UNKNOWN, sku=sku, raw_text=raw)
