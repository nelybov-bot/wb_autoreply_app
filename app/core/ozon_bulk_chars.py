"""Массовая замена характеристик карточек Ozon (ТН ВЭД, бренд и др.)."""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .net import HttpStatusError
from .ozon_client import OzonClient

log = logging.getLogger("ozon.bulk_chars")

ProgressCb = Callable[[int, int, str], None]

# Код ТН ВЭД ЕАЭС — общий attribute_id в большинстве категорий Ozon.
OZON_TNVED_ATTR_ID = 22232
# Частые ID «Бренд» (уточняются по схеме категории).
OZON_BRAND_ATTR_IDS = (85, 31)

_FIELD_ALIASES: Dict[str, str] = {
    "tnved": "tnved",
    "tn_ved": "tnved",
    "tn-ved": "tnved",
    "тнвэд": "tnved",
    "тн вэд": "tnved",
    "тн вэд еаэс": "tnved",
    "код тн вэд": "tnved",
    "код тн вэд еаэс": "tnved",
    "brand": "brand",
    "бренд": "brand",
}

_TNVED_NAME_RE = re.compile(r"тн\s*вэд|hs\s*code|eaeu", re.I)
_BRAND_NAME_RE = re.compile(r"^бренд$|^brand$", re.I)

_UPDATE_BATCH = 100
_ATTR_FETCH_BATCH = 100
_CATALOG_MAX_PAGES = 150
_SCHEMA_CACHE: Dict[str, Tuple[Dict[int, str], Set[int]]] = {}
_DICT_CACHE: Dict[str, Optional[Tuple[int, str]]] = {}


def _raise_if_cancelled(cancel: Any) -> None:
    if cancel is None:
        return
    fn = getattr(cancel, "raise_if_cancelled", None)
    if callable(fn):
        fn()
    elif bool(getattr(cancel, "cancelled", False)):
        raise asyncio.CancelledError()


def normalize_field_key(field: str) -> str:
    raw = re.sub(r"\s+", " ", str(field or "").strip()).casefold()
    if not raw:
        return ""
    if raw in _FIELD_ALIASES:
        return _FIELD_ALIASES[raw]
    if _TNVED_NAME_RE.search(raw):
        return "tnved"
    if _BRAND_NAME_RE.search(raw) or raw == "бренд":
        return "brand"
    return raw


def field_label(field_key: str) -> str:
    if field_key == "tnved":
        return "Код ТН ВЭД ЕАЭС"
    if field_key == "brand":
        return "Бренд"
    return field_key or "характеристика"


def normalize_tnved_value(value: str) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits


def _norm_offer(s: str) -> str:
    v = str(s or "").strip()
    if re.fullmatch(r"\d+\.0+", v):
        v = v.split(".", 1)[0]
    return v


_RE_SKU_HEADER = re.compile(r"sku|артикул|vendor|offer|offer_id", re.I)
_RE_BRAND_HEADER = re.compile(r"^бренд$|^brand$", re.I)
_RE_TNVED_HEADER = re.compile(
    r"тн\s*вэд|tn_?ved|hs\s*code|код\s*тн|еаэс",
    re.I,
)


def _looks_like_tnved_token(s: str) -> bool:
    raw = str(s or "").strip()
    digits = normalize_tnved_value(raw)
    return bool(raw) and len(digits) >= 8 and re.fullmatch(r"[\d\s.\-]+", raw) is not None


def _split_table_line(line: str) -> List[str]:
    """Колонки: таб / ; / , / два пробела; иначе первый токен = артикул, хвост = значение(я).

    Примеры без таба:
    ``820305 balea`` → артикул + бренд
    ``820305 3304990000`` → артикул + ТН ВЭД
    ``820305 The Body Shop 3304990000`` → артикул + бренд + ТН ВЭД
    """
    raw = str(line or "").strip()
    if not raw:
        return []
    if "\t" in raw:
        return [p.strip() for p in raw.split("\t")]
    if ";" in raw:
        return [p.strip() for p in raw.split(";")]
    if "," in raw and re.search(r",\s*\S", raw):
        return [p.strip() for p in raw.split(",")]
    if re.search(r"\s{2,}", raw):
        return [p.strip() for p in re.split(r"\s{2,}", raw) if p.strip()]

    tokens = raw.split()
    if len(tokens) <= 1:
        return tokens
    if len(tokens) == 2:
        return tokens
    last = tokens[-1]
    if _looks_like_tnved_token(last):
        sku = tokens[0]
        brand = " ".join(tokens[1:-1]).strip()
        return [sku, brand, last] if brand else [sku, last]
    return [tokens[0], " ".join(tokens[1:]).strip()]


def parse_ozon_chars_table(text: str) -> Tuple[List[dict], List[str]]:
    """Таблица: offer_id + бренд и/или ТН ВЭД (уникальные значения на артикул).

    Заголовки (любой регистр): артикул/sku/offer_id, бренд/brand, тн вэд/tnved.
    Без заголовка: 2 колонки = артикул + значение (нужен контекст снаружи),
    3 колонки = артикул | бренд | тнвэд.
    """
    warnings: List[str] = []
    lines = (text or "").splitlines()
    if not any(ln.strip() for ln in lines):
        return [], ["Пустая таблица"]

    # Найти первую непустую строку
    start = 0
    while start < len(lines) and not lines[start].strip():
        start += 1
    if start >= len(lines):
        return [], ["Пустая таблица"]

    parts0 = _split_table_line(lines[start].strip())
    sku_i = brand_i = tnved_i = -1
    data_start = start
    has_header = bool(parts0) and any(
        _RE_SKU_HEADER.search(p) or _RE_BRAND_HEADER.search(p) or _RE_TNVED_HEADER.search(p)
        for p in parts0
    )
    if has_header:
        for i, p in enumerate(parts0):
            if sku_i < 0 and _RE_SKU_HEADER.search(p):
                sku_i = i
            elif brand_i < 0 and _RE_BRAND_HEADER.search(p):
                brand_i = i
            elif tnved_i < 0 and _RE_TNVED_HEADER.search(p):
                tnved_i = i
        data_start = start + 1
        if sku_i < 0:
            sku_i = 0
        if brand_i < 0 and tnved_i < 0:
            # Заголовок только артикул — дальше одна колонка значений не определена
            warnings.append("В заголовке нет колонок «Бренд» / «ТН ВЭД» — ожидайте колонки значений")
    else:
        # Без заголовка
        sku_i = 0
        if len(parts0) >= 3:
            brand_i, tnved_i = 1, 2
        elif len(parts0) == 2:
            # Артикул + одно значение: эвристика — цифры → тнвэд, иначе бренд
            second = parts0[1]
            if re.fullmatch(r"[\d\s.\-]+", second or "") and len(normalize_tnved_value(second)) >= 8:
                tnved_i = 1
            else:
                brand_i = 1
        else:
            return [], warnings or ["Нужны колонки: артикул и бренд и/или ТН ВЭД"]

    rows: List[dict] = []
    seen: Set[str] = set()
    for line_no, raw in enumerate(lines[data_start:], start=data_start + 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = _split_table_line(line)
        if not parts:
            continue
        oid = _norm_offer(parts[sku_i] if sku_i < len(parts) else parts[0])
        if not oid:
            warnings.append(f"Строка {line_no}: пустой артикул — пропущена")
            continue
        key = oid.casefold()
        if key in seen:
            warnings.append(f"Дубликат {oid} — оставлена первая строка")
            continue
        seen.add(key)

        brand = ""
        tnved = ""
        if brand_i >= 0 and brand_i < len(parts):
            brand = str(parts[brand_i] or "").strip()
        if tnved_i >= 0 and tnved_i < len(parts):
            tnved = normalize_tnved_value(parts[tnved_i])

        if not brand and not tnved:
            warnings.append(f"Строка {line_no} ({oid}): нет бренда и ТН ВЭД — пропущена")
            continue
        rows.append({"offer_id": oid, "brand": brand, "tnved": tnved})

    if not rows and (text or "").strip():
        return [], warnings or ["Не удалось разобрать ни одной строки"]
    return rows, warnings


def table_rows_to_api(rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        out.append({
            "offer_id": r.get("offer_id") or "",
            "brand": r.get("brand") or "",
            "tnved": r.get("tnved") or "",
        })
    return out


def _attr_id(a: dict) -> int:
    for key in ("id", "attribute_id", "attributeId"):
        try:
            val = int(a.get(key) or 0)
        except (TypeError, ValueError):
            val = 0
        if val:
            return val
    return 0


def _attr_value_text(a: dict) -> str:
    vals = a.get("values") or []
    if not isinstance(vals, list) or not vals:
        return ""
    first = vals[0]
    if isinstance(first, dict):
        text = str(first.get("value") or "").strip()
        if text:
            return text
        dv = str(first.get("dictionary_value") or "").strip()
        if dv and not dv.isdigit():
            return dv
        try:
            did = int(first.get("dictionary_value_id") or 0)
        except (TypeError, ValueError):
            did = 0
        if did:
            return str(did)
    elif first is not None:
        return str(first).strip()
    return ""


def _parse_attr_items(page: dict) -> List[dict]:
    if not isinstance(page, dict):
        return []
    items: Optional[List[dict]] = None
    res = page.get("result")
    if isinstance(res, dict):
        raw = res.get("items")
        if isinstance(raw, list):
            items = [x for x in raw if isinstance(x, dict)]
    elif isinstance(res, list):
        items = [x for x in res if isinstance(x, dict)]
    if items is None:
        raw = page.get("items")
        if isinstance(raw, list):
            items = [x for x in raw if isinstance(x, dict)]
    return items or []


def _attrs_page_cursor(page: dict) -> Tuple[str, bool]:
    if not isinstance(page, dict):
        return "", False
    res = page.get("result")
    block = res if isinstance(res, dict) else page
    last_id = str(block.get("last_id") or "").strip()
    has_next = block.get("has_next")
    if has_next is None:
        has_next = bool(last_id)
    return last_id, bool(has_next)


async def _fetch_products_meta(
    client: OzonClient,
    offer_ids: List[str],
    *,
    progress_cb: Optional[ProgressCb] = None,
    cancel: Any = None,
) -> Dict[str, dict]:
    """offer_id → {product_id, attrs, description_category_id, type_id, title}."""
    out: Dict[str, dict] = {}
    oids = [_norm_offer(x) for x in offer_ids if _norm_offer(x)]
    total = max(len(oids), 1)
    for i in range(0, len(oids), _ATTR_FETCH_BATCH):
        _raise_if_cancelled(cancel)
        batch = oids[i : i + _ATTR_FETCH_BATCH]
        last_id = ""
        for _ in range(50):
            page = await client.product_info_attributes(
                offer_ids=batch, limit=1000, last_id=last_id,
            )
            for it in _parse_attr_items(page):
                oid = _norm_offer(str(it.get("offer_id") or it.get("offerId") or ""))
                if not oid:
                    continue
                try:
                    pid = int(it.get("id") or it.get("product_id") or 0)
                except (TypeError, ValueError):
                    pid = 0
                try:
                    dc = int(it.get("description_category_id") or it.get("descriptionCategoryId") or 0)
                except (TypeError, ValueError):
                    dc = 0
                try:
                    tid = int(it.get("type_id") or it.get("typeId") or 0)
                except (TypeError, ValueError):
                    tid = 0
                attrs = it.get("attributes") or it.get("attribute") or []
                if not isinstance(attrs, list):
                    attrs = []
                out[oid] = {
                    "offer_id": oid,
                    "product_id": pid,
                    "description_category_id": dc,
                    "type_id": tid,
                    "title": str(it.get("name") or "").strip(),
                    "attributes": attrs,
                }
            next_id, has_next = _attrs_page_cursor(page)
            if not has_next or not next_id or next_id == last_id:
                break
            last_id = next_id
        if progress_cb:
            progress_cb(
                min(i + len(batch), total),
                total,
                f"Атрибуты Ozon {min(i + len(batch), total)}/{total}",
            )
    return out


async def _load_category_schema(
    client: OzonClient,
    description_category_id: int,
    type_id: int,
) -> Tuple[Dict[int, str], Set[int]]:
    key = f"{description_category_id}:{type_id}"
    cached = _SCHEMA_CACHE.get(key)
    if cached:
        return cached
    names: Dict[int, str] = {}
    brand_ids: Set[int] = set()
    if not description_category_id or not type_id:
        _SCHEMA_CACHE[key] = (names, set(OZON_BRAND_ATTR_IDS))
        return _SCHEMA_CACHE[key]
    try:
        raw = await client.description_category_attributes(
            description_category_id=description_category_id,
            type_id=type_id,
        )
    except Exception as e:
        log.warning("ozon schema %s: %s", key, e)
        raw = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            aid = int(row.get("id") or 0)
        except (TypeError, ValueError):
            aid = 0
        if not aid:
            continue
        nm = str(row.get("name") or "").strip()
        if nm:
            names[aid] = nm
        if "бренд" in nm.casefold() or nm.casefold() == "brand":
            brand_ids.add(aid)
    if not brand_ids:
        brand_ids = set(OZON_BRAND_ATTR_IDS)
    _SCHEMA_CACHE[key] = (names, brand_ids)
    return names, brand_ids


def _resolve_attr_id(
    field_key: str,
    *,
    names: Dict[int, str],
    brand_ids: Set[int],
    attrs: List[dict],
) -> Tuple[int, str]:
    if field_key == "tnved":
        for aid, nm in names.items():
            if _TNVED_NAME_RE.search(nm or ""):
                return aid, nm
        for a in attrs:
            aid = _attr_id(a)
            if aid == OZON_TNVED_ATTR_ID:
                return aid, "Код ТН ВЭД ЕАЭС"
        return OZON_TNVED_ATTR_ID, "Код ТН ВЭД ЕАЭС"

    if field_key == "brand":
        for aid in brand_ids:
            return aid, names.get(aid) or "Бренд"
        for a in attrs:
            aid = _attr_id(a)
            if aid in OZON_BRAND_ATTR_IDS:
                return aid, "Бренд"
        return int(OZON_BRAND_ATTR_IDS[0]), "Бренд"

    q = field_key.casefold()
    for aid, nm in names.items():
        if nm.casefold() == q or q in nm.casefold():
            return aid, nm
    return 0, ""


def _current_value(attrs: List[dict], attr_id: int) -> str:
    for a in attrs or []:
        if _attr_id(a) == attr_id:
            return _attr_value_text(a)
    return ""


async def _resolve_brand_dictionary(
    client: OzonClient,
    *,
    attribute_id: int,
    description_category_id: int,
    type_id: int,
    brand_name: str,
) -> Tuple[Optional[int], str, str]:
    """→ (dictionary_value_id, matched_value, error_message)."""
    q = str(brand_name or "").strip()
    if len(q) < 2:
        return None, "", "Название бренда слишком короткое (мин. 2 символа)"
    if not description_category_id or not type_id:
        return None, "", "У товара нет category/type — нельзя найти бренд в справочнике"
    cache_key = f"{description_category_id}:{type_id}:{attribute_id}:{q.casefold()}"
    if cache_key in _DICT_CACHE:
        hit = _DICT_CACHE[cache_key]
        if hit is None:
            return None, "", f"Бренд «{q}» не найден в справочнике Ozon"
        return hit[0], hit[1], ""

    try:
        rows = await client.search_attribute_values(
            attribute_id=attribute_id,
            description_category_id=description_category_id,
            type_id=type_id,
            value=q,
            limit=50,
        )
    except HttpStatusError as e:
        return None, "", f"Справочник брендов: HTTP {e.status}"
    except Exception as e:
        return None, "", f"Справочник брендов: {e}"

    exact = None
    soft = None
    q_cf = q.casefold()
    for row in rows:
        val = str(row.get("value") or "").strip()
        try:
            did = int(row.get("id") or 0)
        except (TypeError, ValueError):
            did = 0
        if not did or not val:
            continue
        if val.casefold() == q_cf:
            exact = (did, val)
            break
        if soft is None and (q_cf in val.casefold() or val.casefold() in q_cf):
            soft = (did, val)
    picked = exact or soft
    _DICT_CACHE[cache_key] = picked
    if not picked:
        return None, "", f"Бренд «{q}» не найден в справочнике Ozon"
    return picked[0], picked[1], ""


def estimate_ozon_bulk_steps(n_offers: int, stores: int = 1) -> int:
    stores = max(int(stores), 1)
    n = max(int(n_offers), 1)
    pages = max(1, (n + _ATTR_FETCH_BATCH - 1) // _ATTR_FETCH_BATCH)
    updates = max(1, (n + _UPDATE_BATCH - 1) // _UPDATE_BATCH)
    return stores * (pages + n + updates + 2)


async def apply_bulk_chars_for_store(
    client_id: str,
    api_key: str,
    *,
    field: str = "",
    value: str = "",
    offer_ids: Optional[List[str]] = None,
    all_catalog: bool = False,
    per_offer: Optional[Dict[str, Dict[str, str]]] = None,
    dry_run: bool = False,
    only_if_different: bool = True,
    progress_cb: Optional[ProgressCb] = None,
    cancel: Any = None,
    store_name: str = "",
) -> dict:
    """Массовая замена.

    Режим таблицы: ``per_offer`` = {offer_id: {brand?, tnved?}}.
    Режим одно значение: ``field`` + ``value`` (+ offer_ids / all_catalog).
    """
    targets: Dict[str, Dict[str, str]] = {}
    mode = "table" if per_offer else "single"

    if per_offer:
        for oid_raw, vals in per_offer.items():
            oid = _norm_offer(str(oid_raw or ""))
            if not oid or not isinstance(vals, dict):
                continue
            brand = str(vals.get("brand") or "").strip()
            tnved = normalize_tnved_value(str(vals.get("tnved") or ""))
            if not brand and not tnved:
                continue
            targets[oid] = {"brand": brand, "tnved": tnved}
        if not targets:
            raise ValueError("В таблице нет строк с брендом или ТН ВЭД")
        field_key = "table"
        label = "Бренд / ТН ВЭД"
        new_value = ""
        codes = list(targets.keys())
    else:
        field_key = normalize_field_key(field)
        if field_key not in ("tnved", "brand"):
            raise ValueError("Укажите поле: ТН ВЭД или Бренд")
        raw_value = str(value or "").strip()
        if not raw_value:
            raise ValueError("Укажите новое значение")
        new_value = normalize_tnved_value(raw_value) if field_key == "tnved" else raw_value
        if field_key == "tnved" and not new_value:
            raise ValueError("Код ТН ВЭД должен содержать цифры")
        label = field_label(field_key)
        codes = list(dict.fromkeys(_norm_offer(v) for v in (offer_ids or []) if _norm_offer(v)))

    _raise_if_cancelled(cancel)
    client = OzonClient(client_id, api_key, timeout_s=120.0)

    if progress_cb:
        progress_cb(0, 1, f"{store_name or 'Ozon'}: список товаров…")

    list_meta: dict = {}
    if mode == "single" and (all_catalog or not codes):
        listed = await client.list_products_all(
            max_pages=_CATALOG_MAX_PAGES,
            meta_out=list_meta,
        )
        codes = []
        for it in listed:
            oid = _norm_offer(str(it.get("offer_id") or ""))
            if oid:
                codes.append(oid)
        scope_label = f"весь каталог ({len(codes)})"
        if list_meta.get("truncated"):
            scope_label += ", усечён"
        for oid in codes:
            targets[oid] = {field_key: new_value}
    else:
        if mode == "single":
            for oid in codes:
                targets[oid] = {field_key: new_value}
        # Не зовём /v3/product/list по всему списку: Ozon режет offer_id на 1000,
        # а атрибуты и так грузятся пачками в _fetch_products_meta.
        scope_label = f"таблица ({len(codes)} арт.)" if mode == "table" else f"список ({len(codes)} арт.)"

    _raise_if_cancelled(cancel)
    meta_by_offer = await _fetch_products_meta(
        client, codes, progress_cb=progress_cb, cancel=cancel,
    )

    results: List[dict] = []
    updates: List[dict] = []
    # offer_id → list of result row indices that belong to this update item
    pending_by_offer: Dict[str, List[int]] = {}
    skipped = 0
    not_found = 0
    no_field = 0
    brand_miss = 0
    total = max(len(codes), 1)

    for step, oid in enumerate(codes, start=1):
        if step % 25 == 0:
            _raise_if_cancelled(cancel)
        want = targets.get(oid) or {}
        meta = meta_by_offer.get(oid)
        if not meta:
            not_found += 1
            results.append({
                "offer_id": oid,
                "status": "not_found",
                "message": "Артикул не найден в каталоге Ozon",
                "current_value": "",
                "new_value": want.get("brand") or want.get("tnved") or new_value,
                "char_name": label,
            })
            continue

        attrs = meta.get("attributes") or []
        names, brand_ids = await _load_category_schema(
            client,
            int(meta.get("description_category_id") or 0),
            int(meta.get("type_id") or 0),
        )
        attr_payloads: List[dict] = []
        row_idxs: List[int] = []

        for fk in ("brand", "tnved"):
            val = str(want.get(fk) or "").strip()
            if not val:
                continue
            if fk == "tnved":
                val = normalize_tnved_value(val)
                if not val:
                    continue
            fl = field_label(fk)
            attr_id, attr_name = _resolve_attr_id(
                fk, names=names, brand_ids=brand_ids, attrs=attrs,
            )
            if not attr_id:
                no_field += 1
                results.append({
                    "offer_id": oid,
                    "status": "no_field",
                    "message": f"Поле «{fl}» не найдено в категории",
                    "current_value": "",
                    "new_value": val,
                    "char_name": fl,
                })
                continue

            current = _current_value(attrs, attr_id)
            dict_id: Optional[int] = None
            send_value = val
            if fk == "brand":
                dict_id, matched, err = await _resolve_brand_dictionary(
                    client,
                    attribute_id=attr_id,
                    description_category_id=int(meta.get("description_category_id") or 0),
                    type_id=int(meta.get("type_id") or 0),
                    brand_name=val,
                )
                if err or not dict_id:
                    brand_miss += 1
                    results.append({
                        "offer_id": oid,
                        "status": "brand_not_found",
                        "message": err or "Бренд не найден",
                        "current_value": current,
                        "new_value": val,
                        "char_name": attr_name or fl,
                    })
                    continue
                send_value = matched or val

            if only_if_different and current and current.casefold() == send_value.casefold():
                skipped += 1
                results.append({
                    "offer_id": oid,
                    "status": "skip_same",
                    "message": "Уже совпадает",
                    "current_value": current,
                    "new_value": send_value,
                    "char_name": attr_name or fl,
                })
                continue

            if fk == "brand" and dict_id:
                value_payload = {"dictionary_value_id": int(dict_id), "value": send_value}
            else:
                value_payload = {"dictionary_value_id": 0, "value": send_value}
            attr_payloads.append({"id": int(attr_id), "values": [value_payload]})
            row_idxs.append(len(results))
            results.append({
                "offer_id": oid,
                "status": "pending" if not dry_run else "would_update",
                "message": "Будет обновлено" if dry_run else "В очереди",
                "current_value": current,
                "new_value": send_value,
                "char_name": attr_name or fl,
            })

        if attr_payloads:
            item_body: dict = {"offer_id": oid, "attributes": attr_payloads}
            pid = int(meta.get("product_id") or 0)
            if pid:
                item_body["product_id"] = pid
            updates.append(item_body)
            pending_by_offer[oid] = row_idxs

        if progress_cb and step % 20 == 0:
            progress_cb(step, total, f"Проверка {step}/{total}")

    sent = 0
    errors = 0
    if not dry_run and updates:
        for i in range(0, len(updates), _UPDATE_BATCH):
            _raise_if_cancelled(cancel)
            batch = updates[i : i + _UPDATE_BATCH]
            batch_oids = {str(u.get("offer_id") or "") for u in batch}
            try:
                await client.update_product_attributes(batch)
                sent += len(batch)
                for oid in batch_oids:
                    for idx in pending_by_offer.get(oid) or []:
                        if 0 <= idx < len(results) and results[idx].get("status") == "pending":
                            results[idx]["status"] = "updated"
                            results[idx]["message"] = "Отправлено на Ozon"
            except HttpStatusError as e:
                errors += len(batch)
                msg = f"Ozon HTTP {e.status}: {str(e)[:180]}"
                for oid in batch_oids:
                    for idx in pending_by_offer.get(oid) or []:
                        if 0 <= idx < len(results) and results[idx].get("status") == "pending":
                            results[idx]["status"] = "error"
                            results[idx]["message"] = msg
                log.warning("ozon bulk chars batch failed: %s", msg)
            except Exception as e:
                errors += len(batch)
                msg = str(e)[:200]
                for oid in batch_oids:
                    for idx in pending_by_offer.get(oid) or []:
                        if 0 <= idx < len(results) and results[idx].get("status") == "pending":
                            results[idx]["status"] = "error"
                            results[idx]["message"] = msg
                log.exception("ozon bulk chars batch: %s", e)
            if progress_cb:
                progress_cb(
                    min(i + len(batch), len(updates)),
                    max(len(updates), 1),
                    f"Отправка {min(i + len(batch), len(updates))}/{len(updates)}",
                )
            if i + _UPDATE_BATCH < len(updates):
                await asyncio.sleep(0.8)

    would = sum(1 for r in results if r.get("status") == "would_update")
    updated = sum(1 for r in results if r.get("status") == "updated")
    return {
        "store_name": store_name,
        "mode": mode,
        "field": field_key,
        "field_label": label,
        "value": new_value,
        "scope": scope_label,
        "dry_run": dry_run,
        "total": len(codes),
        "would_update": would,
        "updated": updated,
        "sent": sent,
        "skipped_same": skipped,
        "not_found": not_found,
        "no_field": no_field,
        "brand_not_found": brand_miss,
        "errors": errors,
        "truncated": bool(list_meta.get("truncated")),
        "rows": results[:500],
        "rows_truncated": len(results) > 500,
    }


async def apply_bulk_chars_multi_store(
    stores: List[Tuple[int, str, str, str]],
    *,
    field: str = "",
    value: str = "",
    offer_ids: Optional[List[str]] = None,
    all_catalog: bool = False,
    per_offer: Optional[Dict[str, Dict[str, str]]] = None,
    dry_run: bool = False,
    only_if_different: bool = True,
    progress_cb: Optional[ProgressCb] = None,
    cancel: Any = None,
) -> dict:
    """stores: (store_id, name, client_id, api_key)."""
    parts: List[dict] = []
    n_stores = max(len(stores), 1)
    mode = "table" if per_offer else "single"
    for idx, (sid, name, cid, key) in enumerate(stores):
        _raise_if_cancelled(cancel)

        def _cb(done: int, total: int, detail: str, _i: int = idx) -> None:
            if not progress_cb:
                return
            progress_cb(
                _i * 10_000 + int(done),
                n_stores * 10_000 + max(int(total), 1),
                f"{name}: {detail}",
            )

        try:
            part = await apply_bulk_chars_for_store(
                cid,
                key,
                field=field,
                value=value,
                offer_ids=offer_ids,
                all_catalog=all_catalog,
                per_offer=per_offer,
                dry_run=dry_run,
                only_if_different=only_if_different,
                progress_cb=_cb,
                cancel=cancel,
                store_name=name,
            )
            part["store_id"] = sid
            parts.append(part)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("ozon bulk chars store %s: %s", sid, e)
            parts.append({
                "store_id": sid,
                "store_name": name,
                "mode": mode,
                "field": normalize_field_key(field) if not per_offer else "table",
                "field_label": "Бренд / ТН ВЭД" if per_offer else field_label(normalize_field_key(field)),
                "value": str(value or "").strip(),
                "dry_run": dry_run,
                "error": str(e)[:300],
                "total": 0,
                "would_update": 0,
                "updated": 0,
                "sent": 0,
                "skipped_same": 0,
                "not_found": 0,
                "no_field": 0,
                "brand_not_found": 0,
                "errors": 1,
                "rows": [],
            })

    def _sum(key: str) -> int:
        return sum(int(p.get(key) or 0) for p in parts)

    return {
        "dry_run": dry_run,
        "mode": mode,
        "field": "table" if per_offer else normalize_field_key(field),
        "field_label": "Бренд / ТН ВЭД" if per_offer else field_label(normalize_field_key(field)),
        "value": str(value or "").strip(),
        "stores": parts,
        "total": _sum("total"),
        "would_update": _sum("would_update"),
        "updated": _sum("updated"),
        "sent": _sum("sent"),
        "skipped_same": _sum("skipped_same"),
        "not_found": _sum("not_found"),
        "no_field": _sum("no_field"),
        "brand_not_found": _sum("brand_not_found"),
        "errors": _sum("errors"),
    }
