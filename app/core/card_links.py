"""Связки карточек WB и Ozon: выгрузка каталога, проверка групп, привязка."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.core.net import HttpStatusError
from app.core.ozon_client import OzonClient
from app.core.wb_content_client import WbContentClient

log = logging.getLogger("card_links")

AI_SUGGEST_PARALLEL = 3

OZON_MODEL_ATTR_ID = OzonClient.OZON_MODEL_ATTR_ID
# Частые ID «Бренд» в разных категориях Ozon (уточняются по схеме категории).
OZON_BRAND_ATTR_IDS = (85, 31)

_ozon_category_schema_cache: Dict[str, Tuple[float, set, Dict[int, str], set]] = {}
_OZON_SCHEMA_CACHE_TTL_S = 600.0

# У каждого варианта свой артикул/TMS — не сравниваем при связке «Модель».
_OZON_SKIP_BASE_COMPARE_NAME_RE = re.compile(
    r"код\s*продавца|артикул\s*прод|offer|"
    r"штрих|barcode|ean|"
    r"part\s*number|vendor\s*code|seller\s*code",
    re.I,
)
# Кол-во в упаковке — главный вариант для связки 1/2/3 шт.
_OZON_QTY_PACK_NAME_RE = re.compile(
    r"количеств.*упаков|числ.*упаков|"
    r"кол\.?\s*в\s*уп|"
    r"qty|pack\s*size|units?\s*per",
    re.I,
)


def _ozon_skip_base_compare_attr(name: str) -> bool:
    return bool(_OZON_SKIP_BASE_COMPARE_NAME_RE.search(name or ""))


def _ozon_qty_pack_attr(name: str) -> bool:
    return bool(_OZON_QTY_PACK_NAME_RE.search(name or ""))


def _ozon_qty_pack_attr_id(attr_names: Dict[int, str]) -> Optional[int]:
    for aid, nm in attr_names.items():
        if _ozon_qty_pack_attr(nm):
            return aid
    return None

_PACK_RE = re.compile(
    r"\b(\d+)\s*(шт|штук|уп|упак|pack|pcs)\b|"
    r"\bx\s*(\d+)\b|"
    r"\bкомплект\b|"
    r"\bнабор\b",
    re.IGNORECASE,
)

MAX_LINK_ITEMS = 30
CARD_LINKS_AI_PROMPT_KEY_WB = "card_links_ai_prompt_wb"
CARD_LINKS_AI_PROMPT_KEY_OZON = "card_links_ai_prompt_ozon"


def card_links_ai_prompt_setting_key(marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    return CARD_LINKS_AI_PROMPT_KEY_WB if mp == "wb" else CARD_LINKS_AI_PROMPT_KEY_OZON


def get_card_links_ai_prompt_stored(db: Any, marketplace: str) -> str:
    return (db.get_setting(card_links_ai_prompt_setting_key(marketplace)) or "").strip()


def set_card_links_ai_prompt_stored(db: Any, marketplace: str, prompt_text: str) -> None:
    db.set_setting(card_links_ai_prompt_setting_key(marketplace), (prompt_text or "").strip())
# Эвристики предложений: O(n²) по названиям — ограничиваем на больших каталогах
_CLUSTER_TITLE_SIMILARITY_MAX = 120
_ATTACH_SUGGEST_LIMIT = 200
_ATTACH_UNLINKED_SCAN_CAP = 1000
_ATTACH_TARGET_GROUP_CAP = 150
_REVIEW_MULTI_GROUP_CAP = 120

_TITLE_STOP_WORDS = frozenset(
    {
        "для",
        "и",
        "в",
        "на",
        "с",
        "по",
        "из",
        "от",
        "до",
        "the",
        "for",
        "with",
        "and",
    }
)


def parse_articles_csv(raw: Optional[str]) -> List[str]:
    """Артикулы из строки: запятая, точка с запятой, перевод строки."""
    if not raw:
        return []
    parts = re.split(r"[,;\n\r\t]+", str(raw))
    out: List[str] = []
    seen: set[str] = set()
    for p in parts:
        v = p.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _norm_article_key(value: Optional[str]) -> str:
    return str(value or "").strip().casefold()


def filter_rows_by_articles(
    rows: List[dict],
    articles: List[str],
    *,
    marketplace: str,
) -> Tuple[List[dict], List[str]]:
    """Оставляет только карточки из списка артикулов; возвращает (rows, missing)."""
    if not articles:
        return rows, []
    allowed = {_norm_article_key(a): a for a in articles if str(a).strip()}
    if not allowed:
        return rows, []
    found: set[str] = set()
    out: List[dict] = []
    mp = (marketplace or "").strip().lower()
    for r in rows:
        if mp == "wb":
            key = _norm_article_key(r.get("vendor_code"))
        else:
            key = _norm_article_key(r.get("offer_id"))
        if key and key in allowed:
            found.add(key)
            out.append(r)
    missing = [allowed[k] for k in allowed if k not in found]
    return out, missing


def _apply_articles_scope_meta(
    catalog_meta: dict,
    *,
    articles: List[str],
    rows: List[dict],
    missing: List[str],
) -> None:
    catalog_meta["scope"] = "articles_only"
    catalog_meta["requested_articles"] = len(articles)
    catalog_meta["found_articles"] = len(rows)
    catalog_meta["missing_articles"] = missing[:50]
    catalog_meta["missing_count"] = len(missing)


_TMS_QTY_CELL_RE = re.compile(r"^\d{4,12}$")


def parse_ozon_tms_qty_table(raw: Optional[str]) -> List[List[str]]:
    """
    Таблица TMS: одна строка = связка по кол-ву (1/2/3 шт).
    Колонки через таб, запятую или пробелы. Берём только числовые артикулы TMS.
    """
    groups: List[List[str]] = []
    for line in str(raw or "").splitlines():
        text = line.strip()
        if not text:
            continue
        if re.search(r"артикул", text, re.I) and not _TMS_QTY_CELL_RE.search(text):
            continue
        if re.search(r"[\t,;]", text):
            parts = re.split(r"[\t,;]+", text)
        else:
            parts = text.split()
        oids: List[str] = []
        seen: set[str] = set()
        for part in parts:
            v = part.strip().strip('"').strip("'").replace("\u00a0", "")
            if not v or not _TMS_QTY_CELL_RE.fullmatch(v):
                continue
            if v in seen:
                continue
            seen.add(v)
            oids.append(v)
        if len(oids) >= 2:
            groups.append(oids[:MAX_LINK_ITEMS])
    return groups


async def link_ozon_tms_qty_groups(
    client_id: str,
    api_key: str,
    *,
    table: str,
    dry_run: bool = False,
    pause_s: float = 3.0,
) -> dict:
    """Связать строки TMS-таблицы (1/2/3 шт) через «Название модели»."""
    groups = parse_ozon_tms_qty_table(table)
    if not groups:
        raise ValueError(
            "Не найдено строк для связки. Вставьте таблицу: в каждой строке минимум 2 числовых артикула TMS "
            "(колонки 1 шт / 2 шт / 3 шт)."
        )

    all_oids = sorted({oid for grp in groups for oid in grp})
    catalog = await fetch_ozon_catalog(
        client_id,
        api_key,
        offer_ids=all_oids,
        max_pages=max(3, (len(all_oids) + 99) // 100),
    )
    by_oid = {str(r.get("offer_id") or "").strip(): r for r in catalog if str(r.get("offer_id") or "").strip()}

    preview: List[dict] = []
    results: List[dict] = []
    ok_count = 0
    fail_count = 0

    for row_no, oids in enumerate(groups, start=1):
        rows = [by_oid[oid] for oid in oids if oid in by_oid]
        missing = [oid for oid in oids if oid not in by_oid]
        model_name = _suggested_model_name(rows) if rows else ""
        if not model_name and oids:
            model_name = f"TMS {oids[0]}"

        entry = {
            "row": row_no,
            "offer_ids": oids,
            "pack_qty": [{"offer_id": oid, "qty": i + 1} for i, oid in enumerate(oids)],
            "model_name": model_name,
            "missing_on_ozon": missing,
            "titles": [(r.get("title") or "")[:80] for r in rows],
        }

        if missing:
            entry["ok"] = False
            entry["error"] = f"Не найдено на Ozon: {', '.join(missing)}"
            fail_count += 1
            preview.append(entry)
            results.append(entry)
            continue

        if len(rows) < 2:
            entry["ok"] = False
            entry["error"] = "В строке меньше 2 найденных артикулов"
            fail_count += 1
            preview.append(entry)
            results.append(entry)
            continue

        preview.append({**entry, "ok": True, "error": None})

        try:
            await ozon_link_by_model(
                client_id,
                api_key,
                offer_ids=oids,
                model_name=model_name,
                catalog_rows=catalog,
                qty_pack=True,
                validate_only=dry_run,
            )
            if dry_run:
                continue
            ok_count += 1
            results.append({**entry, "ok": True, "error": None})
        except (ValueError, HttpStatusError) as e:
            msg = str(e)
            if isinstance(e, HttpStatusError):
                msg = (e.body or str(e.status))[:400]
            fail_count += 1
            entry_fail = {**entry, "ok": False, "error": msg}
            results.append(entry_fail)
            if dry_run:
                preview[-1] = entry_fail

        if not dry_run and row_no < len(groups):
            await asyncio.sleep(max(0.5, float(pause_s)))

    return {
        "dry_run": dry_run,
        "group_count": len(groups),
        "ok_count": ok_count,
        "fail_count": fail_count,
        "preview": preview,
        "results": results,
    }


def _wb_photo_url(card: dict) -> str:
    photos = card.get("photos") or []
    if photos and isinstance(photos[0], dict):
        p = photos[0]
        for key in ("c246x328", "square", "big", "c516x688", "tm"):
            url = (p.get(key) or "").strip()
            if url:
                return url
    media = card.get("mediaFiles") or []
    if media and isinstance(media[0], dict):
        return str(media[0].get("url") or media[0].get("big") or "").strip()
    return ""


def normalize_wb_card(card: dict) -> dict:
    nm = int(card.get("nmID") or card.get("nmId") or 0)
    imt = int(card.get("imtID") or card.get("imtId") or 0)
    vendor = str(card.get("vendorCode") or card.get("supplierVendorCode") or "").strip()
    title = str(card.get("title") or card.get("subjectName") or "").strip()
    brand = str(card.get("brand") or "").strip()
    subject_id = int(card.get("subjectID") or card.get("subjectId") or 0)
    subject_name = str(card.get("subjectName") or "").strip()
    return {
        "vendor_code": vendor,
        "nm_id": nm,
        "imt_id": imt,
        "title": title,
        "brand": brand,
        "subject_id": subject_id,
        "subject_name": subject_name,
        "parent_id": 0,
        "parent_name": "",
        "photo_url": _wb_photo_url(card),
        "linked": False,
        "link_group_id": imt if imt else None,
        "link_group_label": f"imtID {imt}" if imt else None,
    }


def _ozon_primary_image(item: dict) -> str:
    for key in ("primary_image", "primaryImage"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, list) and val:
            return str(val[0]).strip()
    images = item.get("images") or item.get("image") or []
    if isinstance(images, list) and images:
        return str(images[0]).strip()
    return ""


def _ozon_attr_id(a: dict) -> int:
    for key in ("id", "attribute_id", "attributeId"):
        try:
            val = int(a.get(key) or 0)
        except (TypeError, ValueError):
            val = 0
        if val:
            return val
    return 0


def _ozon_attr_values_normalized(a: dict) -> List[str]:
    vals = a.get("values") or []
    out: List[str] = []
    for v in vals if isinstance(vals, list) else []:
        if isinstance(v, dict):
            text = str(v.get("value") or "").strip()
            if not text:
                dv = str(v.get("dictionary_value") or "").strip()
                if dv:
                    text = dv
                elif v.get("dictionary_value_id") not in (None, 0, "0"):
                    text = str(v.get("dictionary_value_id"))
            if text:
                out.append(text)
        elif v is not None:
            s = str(v).strip()
            if s:
                out.append(s)
    return sorted(set(out))


def _ozon_attribute_fingerprint(attrs: Optional[List[dict]]) -> Dict[int, str]:
    """Все заполненные характеристики товара → id → нормализованное значение."""
    fp: Dict[int, str] = {}
    for a in attrs or []:
        if not isinstance(a, dict):
            continue
        aid = _ozon_attr_id(a)
        if not aid:
            continue
        parts = _ozon_attr_values_normalized(a)
        if parts:
            fp[aid] = "|".join(parts)
    return fp


def _ozon_brand_from_fingerprint(fp: Dict[int, str], brand_attr_ids: Optional[set] = None) -> str:
    ids = brand_attr_ids or set(OZON_BRAND_ATTR_IDS)
    for bid in ids:
        raw = fp.get(int(bid), "")
        if raw:
            return raw.split("|")[0].strip()
    return ""


def _attr_value(attrs: List[dict], attr_id: int) -> str:
    for a in attrs or []:
        if not isinstance(a, dict):
            continue
        aid = 0
        for key in ("id", "attribute_id", "attributeId"):
            try:
                val = int(a.get(key) or 0)
            except (TypeError, ValueError):
                val = 0
            if val:
                aid = val
                break
        if aid != attr_id:
            continue
        vals = a.get("values") or []
        if isinstance(vals, list) and vals:
            first = vals[0]
            if isinstance(first, dict):
                text = str(first.get("value") or "").strip()
                if text:
                    return text
                # Словарное значение — только если это текст, не числовой ID
                dv = str(first.get("dictionary_value") or "").strip()
                if dv and not dv.isdigit():
                    return dv
            elif first is not None:
                text = str(first).strip()
                if text and not text.isdigit():
                    return text
            continue
        raw = a.get("value")
        if isinstance(raw, str) and raw.strip() and not raw.strip().isdigit():
            return raw.strip()
    return ""


def _enrich_ozon_model_from_peers(rows: List[dict]) -> None:
    """Если у связанных SKU «Название модели» пришло не у всех — подтянуть от соседа."""
    by_sku: Dict[int, dict] = {}
    for r in rows:
        try:
            sku = int(r.get("sku") or 0)
        except (TypeError, ValueError):
            sku = 0
        if sku:
            by_sku[sku] = r

    def _apply_model(row: dict, model: str) -> None:
        m = (model or "").strip()
        if not m:
            return
        row["model_name"] = m
        row["link_group_id"] = m
        row["link_group_label"] = m

    for r in rows:
        if (r.get("model_name") or "").strip():
            continue
        rel = [int(x) for x in (r.get("related_skus") or []) if x]
        for sku in rel:
            peer = by_sku.get(sku)
            if not peer:
                continue
            pm = (peer.get("model_name") or "").strip()
            if pm:
                _apply_model(r, pm)
                break
        if (r.get("model_name") or "").strip():
            continue
        # Обратная связь: у соседа пусто, но у текущего после другого прохода могло появиться
        try:
            my_sku = int(r.get("sku") or 0)
        except (TypeError, ValueError):
            my_sku = 0
        if not my_sku:
            continue
        for peer in rows:
            if peer is r:
                continue
            rel_p = peer.get("related_skus") or []
            if my_sku in rel_p and (peer.get("model_name") or "").strip():
                _apply_model(r, peer["model_name"])
                break


def normalize_ozon_product(
    info: dict,
    *,
    attrs: Optional[List[dict]] = None,
    related_skus: Optional[List[int]] = None,
) -> dict:
    offer_id = str(info.get("offer_id") or info.get("offerId") or "").strip()
    sku_raw = info.get("sku")
    try:
        sku = int(sku_raw) if sku_raw is not None else 0
    except (TypeError, ValueError):
        sku = 0
    try:
        product_id = int(info.get("id") or info.get("product_id") or 0)
    except (TypeError, ValueError):
        product_id = 0
    model_name = _attr_value(attrs or [], OZON_MODEL_ATTR_ID)
    rel = sorted(set(int(x) for x in (related_skus or []) if x))
    cat_key, cat_label = _ozon_category_key(info)
    attr_fp = _ozon_attribute_fingerprint(attrs)
    brand = _ozon_brand_from_fingerprint(attr_fp)
    try:
        dc_i = int(info.get("description_category_id") or info.get("descriptionCategoryId") or 0)
    except (TypeError, ValueError):
        dc_i = 0
    try:
        tid_i = int(info.get("type_id") or info.get("typeId") or 0)
    except (TypeError, ValueError):
        tid_i = 0
    group_label = model_name or (f"SKU {rel[0]}" if rel else None)
    return {
        "offer_id": offer_id,
        "sku": sku,
        "product_id": product_id,
        "title": str(info.get("name") or "").strip(),
        "photo_url": _ozon_primary_image(info),
        "model_name": model_name,
        "brand": brand,
        "related_skus": rel,
        "category_key": cat_key,
        "category_label": cat_label,
        "description_category_id": dc_i,
        "type_id": tid_i,
        "attribute_fingerprint": attr_fp,
        "linked": False,
        "link_group_id": model_name or (str(rel[0]) if rel else None),
        "link_group_label": group_label,
    }


def _ozon_category_key(info: dict) -> Tuple[str, str]:
    dc = info.get("description_category_id") or info.get("descriptionCategoryId")
    tid = info.get("type_id") or info.get("typeId")
    try:
        dc_i = int(dc) if dc is not None else 0
    except (TypeError, ValueError):
        dc_i = 0
    try:
        tid_i = int(tid) if tid is not None else 0
    except (TypeError, ValueError):
        tid_i = 0
    key = f"{dc_i}:{tid_i}"
    label = str(info.get("type_name") or info.get("typeName") or "").strip()
    if not label and (dc_i or tid_i):
        label = ""  # не показываем сырые ID категории в UI
    return key, label or "категория Ozon"


def apply_link_status(rows: List[dict], groups: List[dict]) -> None:
    """linked=True только у карточек в существующей связке (2+ SKU)."""
    for r in rows:
        r["linked"] = False
    for g in groups:
        items = g.get("items") or []
        gid = str(g.get("group_id") or "")
        is_linked = len(items) > 1 and gid != "__unlinked__"
        g["linked"] = is_linked
        if is_linked:
            for it in items:
                it["linked"] = True


def _suggested_model_name(items: List[dict]) -> str:
    bases: List[str] = []
    for it in items:
        t = _title_base_key(it.get("title") or "")
        if t:
            bases.append(t)
    if not bases:
        return ""
    base = max(bases, key=len)
    return base[:120].strip().title() if base else ""


def _split_items_by_category(items: List[dict], *, marketplace: str) -> List[List[dict]]:
    """Делит кандидатов по категории — нельзя связывать разные subject/категории Ozon."""
    buckets: Dict[str, List[dict]] = {}
    for it in items:
        if marketplace == "wb":
            sid = int(it.get("subject_id") or 0)
            pid = int(it.get("parent_id") or 0)
            key = f"wb:{sid}:{pid}"
        else:
            key = f"oz:{it.get('category_key') or '0:0'}"
        buckets.setdefault(key, []).append(it)
    return [grp for grp in buckets.values() if len(grp) >= 2]


def sort_catalog_rows(rows: List[dict], *, marketplace: str) -> List[dict]:
    """Каталог: категория → связки подряд (крупные первыми) → одиночки."""
    group_sizes: Dict[str, int] = {}
    for r in rows:
        if not r.get("linked"):
            continue
        gid = str(r.get("link_group_id") or r.get("imt_id") or r.get("model_name") or "")
        if gid:
            group_sizes[gid] = group_sizes.get(gid, 0) + 1

    def _sort_key(r: dict) -> tuple:
        cat = _candidate_label([r], marketplace=marketplace).lower()
        if r.get("linked"):
            gid = str(r.get("link_group_id") or r.get("imt_id") or r.get("model_name") or "")
            size = group_sizes.get(gid, 1)
            return (cat, 0, -size, gid.lower(), str(r.get("title") or "").lower())
        solo = str(r.get("nm_id") or r.get("offer_id") or r.get("vendor_code") or "")
        return (cat, 1, 0, solo.lower(), str(r.get("title") or "").lower())

    return sorted(rows, key=_sort_key)


def _wb_target_imt(items: List[dict]) -> int:
    imts = [int(x.get("imt_id") or 0) for x in items if int(x.get("imt_id") or 0)]
    return imts[0] if imts else 0


def _candidate_label(items: List[dict], *, marketplace: str) -> str:
    if marketplace == "wb":
        name = str(items[0].get("subject_name") or "").strip()
        sid = int(items[0].get("subject_id") or 0)
        parent = str(items[0].get("parent_name") or "").strip()
        base = name or (f"subjectID {sid}" if sid else "категория WB")
        if parent:
            return f"{parent} → {base}"
        return base
    label = str(items[0].get("category_label") or items[0].get("category_key") or "").strip()
    return label or "категория Ozon"


def _title_base_key(title: str) -> str:
    t = (title or "").lower().strip()
    t = _PACK_RE.sub(" ", t)
    t = re.sub(
        r"[,–—-]\s*(\d+)\s*(pcs|pc|шт|штук|уп|упак|pack)\b",
        " ",
        t,
        flags=re.IGNORECASE,
    )
    t = re.sub(r"\s+", " ", t).strip()
    return t[:120]


def _product_pack_key(row: dict, marketplace: str) -> str:
    """Ключ одного товара без указания фасовки (1/2/3 шт)."""
    base = _title_base_key(row.get("title") or "")
    if len(base) < 6:
        return ""
    return f"{_row_category_key(row, marketplace)}|{base}"


def _items_are_pack_variants(items: List[dict], *, marketplace: str) -> bool:
    if len(items) < 2:
        return False
    if any(_PACK_RE.search(x.get("title") or "") for x in items):
        return True
    bases = [_title_base_key(x.get("title") or "") for x in items]
    if len(set(bases)) == 1:
        return True
    root = bases[0]
    return all(_titles_related_enough(root, b) for b in bases[1:])


def _title_tokens(title: str) -> set[str]:
    t = re.sub(r"[^\w\s]+", " ", (title or "").lower(), flags=re.UNICODE)
    return {w for w in t.split() if len(w) >= 3 and w not in _TITLE_STOP_WORDS}


def _titles_related_enough(base_a: str, base_b: str) -> bool:
    if _titles_similar(base_a, base_b):
        return True
    ta, tb = _title_tokens(base_a), _title_tokens(base_b)
    if not ta or not tb:
        return False
    inter = ta & tb
    if len(inter) < 2:
        return False
    smaller = min(len(ta), len(tb))
    return len(inter) >= max(2, int(smaller * 0.35))


def _candidate_category_key(c: dict, *, marketplace: str) -> str:
    items = c.get("items") or []
    first = items[0] if items else {}
    if marketplace == "wb":
        sid = int(c.get("subject_id") or first.get("subject_id") or 0)
        pid = int(first.get("parent_id") or 0)
        return f"wb:{sid}:{pid}"
    return f"oz:{c.get('category_key') or first.get('category_key') or ''}"


def _candidate_title_base(c: dict) -> str:
    items = c.get("items") or []
    sug = _suggested_model_name(items)
    if sug:
        return _title_base_key(sug)
    if items:
        return _title_base_key(items[0].get("title") or "")
    return ""


def _merge_candidate_items(candidates: List[dict], *, marketplace: str) -> List[dict]:
    seen: set[str] = set()
    out: List[dict] = []
    for c in candidates:
        for it in c.get("items") or []:
            if marketplace == "wb":
                key = str(it.get("nm_id") or it.get("vendor_code") or "")
            else:
                key = str(it.get("offer_id") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(it)
    return out


def suggest_combine_candidates(
    candidates: List[dict],
    *,
    marketplace: str,
) -> List[dict]:
    """Объединить несколько предложений new_link одной категории в одно."""
    pool = [c for c in candidates if (c.get("kind") or "") == "new_link"]
    if len(pool) < 2:
        return []

    by_cat: Dict[str, List[dict]] = {}
    for c in pool:
        by_cat.setdefault(_candidate_category_key(c, marketplace=marketplace), []).append(c)

    out: List[dict] = []
    seq = 0
    consumed: set[str] = set()

    for _cat, group in by_cat.items():
        if len(group) < 2:
            continue
        n = len(group)
        parent = list(range(n))

        def _find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def _union(a: int, b: int) -> None:
            ra, rb = _find(a), _find(b)
            if ra != rb:
                parent[rb] = ra

        for i in range(n):
            bi = _candidate_title_base(group[i])
            if not bi:
                continue
            for j in range(i + 1, n):
                bj = _candidate_title_base(group[j])
                if bj and _titles_related_enough(bi, bj):
                    _union(i, j)

        clusters: Dict[int, List[dict]] = {}
        for i in range(n):
            clusters.setdefault(_find(i), []).append(group[i])

        for cluster in clusters.values():
            if len(cluster) < 2:
                continue
            ids = [str(c.get("candidate_id") or "") for c in cluster]
            if not ids[0] or any(i in consumed for i in ids if i):
                continue
            merged = _merge_candidate_items(cluster, marketplace=marketplace)
            if len(merged) < 2 or len(merged) > MAX_LINK_ITEMS:
                continue
            for i in ids:
                if i:
                    consumed.add(i)
            seq += 1
            cat_label = _candidate_label(merged, marketplace=marketplace)
            sug_model = _suggested_model_name(merged)
            labels = [str(c.get("hint") or c.get("category_label") or "")[:40] for c in cluster]
            out.append(
                {
                    "candidate_id": f"combine-{marketplace}-{seq}",
                    "kind": "combine_suggestions",
                    "marketplace": marketplace,
                    "category_label": cat_label,
                    "subject_id": merged[0].get("subject_id") if marketplace == "wb" else None,
                    "category_key": merged[0].get("category_key") if marketplace == "ozon" else None,
                    "count": len(merged),
                    "source_candidate_ids": ids,
                    "source_count": len(cluster),
                    "suggested_target_imt": _wb_target_imt(merged) if marketplace == "wb" else None,
                    "suggested_model_name": sug_model,
                    "hint": f"Соединить {len(cluster)} предложения · {cat_label}",
                    "items": merged,
                }
            )

    out.sort(key=lambda x: (-(x.get("count") or 0), x.get("hint") or ""))
    return out[:40]


def validate_wb_link_capacity(
    rows: List[dict],
    *,
    target_imt: int,
    nm_ids: List[int],
) -> None:
    """Не более MAX_LINK_ITEMS карточек в одной связке WB."""
    target = int(target_imt)
    existing: set[int] = set()
    for r in rows:
        if int(r.get("imt_id") or 0) != target:
            continue
        nid = int(r.get("nm_id") or 0)
        if nid:
            existing.add(nid)
    adding = {int(x) for x in nm_ids if int(x) not in existing}
    total = len(existing) + len(adding)
    if total > MAX_LINK_ITEMS:
        raise ValueError(
            f"В связке WB не более {MAX_LINK_ITEMS} товаров "
            f"(сейчас {len(existing)}, добавляете {len(adding)} — всего {total})"
        )


def validate_ozon_link_capacity(
    rows: List[dict],
    *,
    model_name: str,
    offer_ids: List[str],
) -> None:
    """Не более MAX_LINK_ITEMS товаров с одним названием модели на Ozon."""
    model = (model_name or "").strip()
    existing: set[str] = set()
    for r in rows:
        if str(r.get("model_name") or "").strip() != model:
            continue
        oid = str(r.get("offer_id") or "").strip()
        if oid:
            existing.add(oid)
    adding = {str(x).strip() for x in offer_ids if str(x).strip() and str(x).strip() not in existing}
    total = len(existing) + len(adding)
    if total > MAX_LINK_ITEMS:
        raise ValueError(
            f"В связке Ozon не более {MAX_LINK_ITEMS} товаров "
            f"(сейчас {len(existing)}, добавляете {len(adding)} — всего {total})"
        )


def validate_ozon_link_rows(
    rows: List[dict],
    offer_ids: List[str],
    *,
    aspect_attr_ids: set,
    attr_names: Dict[int, str],
    brand_attr_ids: set,
    qty_pack: bool = False,
) -> None:
    """
    Правила Ozon перед объединением:
    - одна категория;
    - qty_pack (TMS 1/2/3 шт): только категория; кол-во в упаковке выставится при связке;
    - иначе: все НЕ-вариативные характеристики совпадают (кроме артикула/TMS);
    - вариативные (is_aspect) должны отличаться между SKU.
    """
    oid_set = {str(x).strip() for x in offer_ids if str(x).strip()}
    selected = [r for r in rows if str(r.get("offer_id") or "").strip() in oid_set]
    if len(selected) < 2:
        return

    cats = {str(r.get("category_key") or "") for r in selected}
    if len(cats) > 1:
        labels = sorted({str(r.get("category_label") or r.get("category_key") or "") for r in selected})
        raise ValueError(
            f"Разные категории Ozon ({', '.join(labels)}). Связывайте товары одного типа."
        )

    def _brand(row: dict) -> str:
        b = (row.get("brand") or "").strip()
        if b:
            return b
        fp = row.get("attribute_fingerprint") or {}
        return _ozon_brand_from_fingerprint(fp, brand_attr_ids        )

    if qty_pack:
        return

    fingerprints = [r.get("attribute_fingerprint") or {} for r in selected]
    all_ids: set[int] = set()
    for fp in fingerprints:
        for k in fp:
            try:
                all_ids.add(int(k))
            except (TypeError, ValueError):
                pass

    skip = set(aspect_attr_ids) | {OZON_MODEL_ATTR_ID}
    for aid in all_ids - skip:
        nm = attr_names.get(aid, f"характеристика {aid}")
        if _ozon_skip_base_compare_attr(nm):
            continue
        vals = {str(fp.get(aid) or "").strip() for fp in fingerprints}
        vals.discard("")
        if len(vals) > 1:
            raise ValueError(
                f"Различается «{nm}»: у связуемых товаров должны совпадать все характеристики, "
                f"кроме вариантов (размер, цвет, количество в упаковке…)."
            )

    if not aspect_attr_ids:
        return

    for i in range(len(selected)):
        for j in range(i + 1, len(selected)):
            a, b = selected[i], selected[j]
            fp_a, fp_b = fingerprints[i], fingerprints[j]
            filled = [aid for aid in aspect_attr_ids if fp_a.get(aid) and fp_b.get(aid)]
            if filled and all(fp_a[aid] == fp_b[aid] for aid in filled):
                names = [attr_names.get(aid, str(aid)) for aid in filled[:5]]
                raise ValueError(
                    f"Артикулы {a.get('offer_id')} и {b.get('offer_id')}: одинаковые вариативные "
                    f"характеристики ({', '.join(names)}). Измените хотя бы одну — размер, цвет, "
                    f"кол-во в упаковке и т.п."
                )


async def _ozon_category_schema(
    client: OzonClient,
    description_category_id: int,
    type_id: int,
) -> Tuple[set, Dict[int, str], set]:
    """Схема категории: id вариативных атрибутов (is_aspect), названия, id бренда."""
    key = f"{description_category_id}:{type_id}"
    now = time.time()
    cached = _ozon_category_schema_cache.get(key)
    if cached and now - cached[0] < _OZON_SCHEMA_CACHE_TTL_S:
        return cached[1], cached[2], cached[3]

    raw = await client.description_category_attributes(
        description_category_id=description_category_id,
        type_id=type_id,
    )
    aspect_ids: set[int] = set()
    names: Dict[int, str] = {}
    brand_ids: set[int] = set()
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
        if row.get("is_aspect"):
            aspect_ids.add(aid)
        if "бренд" in nm.lower():
            brand_ids.add(aid)
    if not brand_ids:
        brand_ids = set(OZON_BRAND_ATTR_IDS)

    _ozon_category_schema_cache[key] = (now, aspect_ids, names, brand_ids)
    return aspect_ids, names, brand_ids


def group_wb_rows(rows: List[dict]) -> List[dict]:
    """Группы по imtID (существующие связки WB)."""
    buckets: Dict[int, List[dict]] = {}
    for r in rows:
        imt = int(r.get("imt_id") or 0)
        if not imt:
            continue
        buckets.setdefault(imt, []).append(r)
    groups: List[dict] = []
    for imt, items in sorted(buckets.items(), key=lambda x: -len(x[1])):
        linked = len(items) > 1
        cat_label = _candidate_label(items, marketplace="wb")
        group_label = f"{cat_label} · imtID {imt}" if cat_label else f"imtID {imt}"
        for it in items:
            it["linked"] = linked
            it["link_group_id"] = imt
            it["link_group_label"] = group_label
        groups.append(
            {
                "group_id": str(imt),
                "group_label": group_label,
                "marketplace": "wb",
                "linked": linked,
                "count": len(items),
                "subject_id": items[0].get("subject_id"),
                "subject_name": items[0].get("subject_name"),
                "parent_id": items[0].get("parent_id"),
                "parent_name": items[0].get("parent_name"),
                "items": items,
            }
        )
    return groups


def group_ozon_rows(rows: List[dict], *, articles_only: bool = False) -> List[dict]:
    """Группы по «Название модели» или кластеру related_sku."""
    by_model: Dict[str, List[dict]] = {}
    orphan: List[dict] = []
    for r in rows:
        model = (r.get("model_name") or "").strip()
        if model:
            by_model.setdefault(model, []).append(r)
        else:
            orphan.append(r)

    groups: List[dict] = []
    for model, items in sorted(by_model.items(), key=lambda x: -len(x[1])):
        linked = len(items) > 1 or (
            not articles_only and any(len(x.get("related_skus") or []) > 1 for x in items)
        )
        cat_label = str(items[0].get("category_label") or "").strip()
        group_label = f"{cat_label} · {model}" if cat_label else model
        for it in items:
            it["linked"] = linked
            it["link_group_id"] = model
            it["link_group_label"] = group_label
        groups.append(
            {
                "group_id": model,
                "group_label": group_label,
                "marketplace": "ozon",
                "linked": linked,
                "count": len(items),
                "category_key": items[0].get("category_key"),
                "category_label": items[0].get("category_label"),
                "items": items,
            }
        )

    # Одиночные без model_name — по related_skus (только полный каталог)
    rel_map: Dict[Tuple[int, ...], List[dict]] = {}
    if not articles_only:
        for r in orphan:
            rel = tuple(sorted(set(r.get("related_skus") or [])))
            if len(rel) > 1:
                rel_map.setdefault(rel, []).append(r)

    for rel, items in rel_map.items():
        label = f"связка SKU {', '.join(str(x) for x in rel[:5])}"
        linked = len(items) > 1 or len(rel) > 1
        gid = "rel:" + "_".join(str(x) for x in rel)
        for it in items:
            it["linked"] = linked
            it["link_group_id"] = gid
            it["link_group_label"] = label
        groups.append(
            {
                "group_id": gid,
                "group_label": label,
                "marketplace": "ozon",
                "linked": linked,
                "count": len(items),
                "items": items,
            }
        )

    singles = (
        list(orphan)
        if articles_only
        else [r for r in orphan if len(r.get("related_skus") or []) <= 1]
    )
    if singles:
        groups.append(
            {
                "group_id": "__unlinked__",
                "group_label": "Без связки",
                "marketplace": "ozon",
                "linked": False,
                "count": len(singles),
                "items": singles,
            }
        )
    return groups


def _cluster_items_by_title(items: List[dict]) -> List[List[dict]]:
    """Группирует товары с похожими названиями (для Ozon и мягкой эвристики)."""
    if len(items) < 2:
        return []
    if len(items) > _CLUSTER_TITLE_SIMILARITY_MAX:
        buckets: Dict[str, List[dict]] = {}
        for it in items:
            base = _title_base_key(it.get("title") or "")
            if not base:
                continue
            buckets.setdefault(base, []).append(it)
        return [grp for grp in buckets.values() if len(grp) >= 2]
    n = len(items)
    parent = list(range(n))

    def _find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(a: int, b: int) -> None:
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent[rb] = ra

    bases: List[str] = []
    for it in items:
        base = _title_base_key(it.get("title") or "")
        bases.append(base)

    for i in range(n):
        if not bases[i]:
            continue
        for j in range(i + 1, n):
            if not bases[j]:
                continue
            if _titles_related_enough(bases[i], bases[j]):
                _union(i, j)

    clusters: Dict[int, List[dict]] = {}
    for i in range(n):
        clusters.setdefault(_find(i), []).append(items[i])
    return [grp for grp in clusters.values() if len(grp) >= 2]


def _parse_ozon_attributes_by_offer(page: dict) -> Dict[str, List[dict]]:
    """Разбор ответа /v4/product/info/attributes → offer_id → attributes."""
    out: Dict[str, List[dict]] = {}
    if not isinstance(page, dict):
        return out
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
    for it in items or []:
        oid = str(it.get("offer_id") or it.get("offerId") or "").strip()
        if not oid:
            continue
        attrs = it.get("attributes") or it.get("attribute") or []
        out[oid] = attrs if isinstance(attrs, list) else []
    return out


def _append_new_link_candidate(
    out: List[dict],
    *,
    seq: int,
    items: List[dict],
    marketplace: str,
) -> int:
    if len(items) < 2:
        return seq
    if marketplace == "wb":
        imts = {int(x.get("imt_id") or 0) for x in items}
        imts.discard(0)
        nms = {int(x.get("nm_id") or 0) for x in items}
        nms.discard(0)
        if len(imts) <= 1 or len(nms) < 2:
            return seq
    else:
        models = {(x.get("model_name") or "").strip() for x in items}
        if len(models) == 1 and list(models)[0]:
            return seq
    seq += 1
    cat_label = _candidate_label(items, marketplace=marketplace)
    sug_model = _suggested_model_name(items)
    out.append(
        {
            "candidate_id": f"new-{marketplace}-{seq}",
            "kind": "new_link",
            "marketplace": marketplace,
            "category_label": cat_label,
            "subject_id": items[0].get("subject_id") if marketplace == "wb" else None,
            "category_key": items[0].get("category_key") if marketplace == "ozon" else None,
            "count": len(items),
            "suggested_target_imt": _wb_target_imt(items) if marketplace == "wb" else None,
            "suggested_model_name": sug_model,
            "hint": f"Похожие названия · {cat_label}",
            "items": items,
        }
    )
    return seq


def suggest_link_candidates(rows: List[dict], *, marketplace: str) -> List[dict]:
    """Новые связки: похожие несвязанные карточки в одной категории."""
    pool = [r for r in rows if not r.get("linked")]
    out: List[dict] = []
    seq = 0
    by_cat: Dict[str, List[dict]] = {}

    for r in pool:
        if marketplace == "wb":
            sid = int(r.get("subject_id") or 0)
            if not sid:
                continue
            pid = int(r.get("parent_id") or 0)
            key = f"wb:{sid}:{pid}"
        else:
            key = str(r.get("category_key") or "0:0")
        by_cat.setdefault(key, []).append(r)

    for cat_items in by_cat.values():
        for items in _cluster_items_by_title(cat_items):
            seq = _append_new_link_candidate(out, seq=seq, items=items, marketplace=marketplace)

    out.sort(
        key=lambda x: (
            str(x.get("category_label") or "").lower(),
            -(x.get("count") or 0),
        )
    )
    return out[:150]


def suggest_attach_to_groups(
    rows: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
) -> List[dict]:
    """Добавить одиночные карточки в уже существующие связки."""
    unlinked = [r for r in rows if not r.get("linked")]
    if not unlinked:
        return []
    multi = [g for g in groups if g.get("linked") and len(g.get("items") or []) > 1]
    if len(multi) > _ATTACH_TARGET_GROUP_CAP:
        multi = sorted(multi, key=lambda g: -len(g.get("items") or []))[:_ATTACH_TARGET_GROUP_CAP]
    unlinked_scan = (
        unlinked[:_ATTACH_UNLINKED_SCAN_CAP]
        if len(unlinked) > _ATTACH_UNLINKED_SCAN_CAP
        else unlinked
    )
    out: List[dict] = []
    seq = 0
    seen: set[str] = set()
    for g in multi:
        ref_items = g.get("items") or []
        if not ref_items:
            continue
        if len(ref_items) >= MAX_LINK_ITEMS:
            continue
        ref = ref_items[0]
        for u in unlinked_scan:
            if marketplace == "wb":
                u_pid = int(u.get("parent_id") or 0)
                g_pid = int(g.get("parent_id") or ref.get("parent_id") or 0)
                if int(u.get("subject_id") or 0) != int(g.get("subject_id") or ref.get("subject_id") or 0):
                    continue
                if u_pid and g_pid and u_pid != g_pid:
                    continue
                uid = str(u.get("nm_id") or u.get("vendor_code") or "")
                target_imt = int(g.get("group_id") or ref.get("imt_id") or 0)
            else:
                if str(u.get("category_key") or "") != str(ref.get("category_key") or ""):
                    continue
                uid = str(u.get("offer_id") or "")
                target_imt = None
            if not _item_matches_group_attach(u, g):
                continue
            dedupe = f"{uid}:{g.get('group_id')}"
            if dedupe in seen:
                continue
            seen.add(dedupe)
            seq += 1
            cat_label = _candidate_label([u], marketplace=marketplace)
            entry: Dict[str, Any] = {
                "candidate_id": f"attach-{marketplace}-{seq}",
                "kind": "attach",
                "marketplace": marketplace,
                "category_label": cat_label,
                "count": 1,
                "target_group_count": len(ref_items),
                "hint": f"Добавить в «{g.get('group_label')}» ({len(ref_items)} шт.)",
                "target_group_id": g.get("group_id"),
                "target_group_label": g.get("group_label"),
                "items": [u],
                "sample_items": ref_items[:3],
            }
            if marketplace == "wb":
                entry["suggested_target_imt"] = target_imt
                entry["subject_id"] = u.get("subject_id")
            else:
                entry["suggested_model_name"] = str(g.get("group_label") or "").strip()
                entry["category_key"] = u.get("category_key")
            out.append(entry)
            if len(out) >= _ATTACH_SUGGEST_LIMIT:
                break
        if len(out) >= _ATTACH_SUGGEST_LIMIT:
            break
    out.sort(
        key=lambda x: (
            str(x.get("category_label") or "").lower(),
            str(x.get("target_group_label") or "").lower(),
            str((x.get("items") or [{}])[0].get("title") or "").lower(),
        )
    )
    return out[:_ATTACH_SUGGEST_LIMIT]


def group_attach_suggestions(suggestions: List[dict], *, marketplace: str) -> List[dict]:
    """Несколько attach в одну связку — один пул товаров."""
    by_target: Dict[str, List[dict]] = {}
    for c in suggestions:
        if (c.get("kind") or "") != "attach":
            continue
        if marketplace == "wb":
            key = str(c.get("target_group_id") or c.get("suggested_target_imt") or "")
        else:
            key = str(c.get("target_group_label") or c.get("suggested_model_name") or "")
        if not key:
            continue
        by_target.setdefault(key, []).append(c)

    consumed: set[str] = set()
    batches: List[dict] = []
    seq = 0
    for _key, group in by_target.items():
        if len(group) < 2:
            continue
        items: List[dict] = []
        seen: set[str] = set()
        source_ids: List[str] = []
        for c in group:
            cid = str(c.get("candidate_id") or "")
            if cid:
                source_ids.append(cid)
                consumed.add(cid)
            for it in c.get("items") or []:
                if marketplace == "wb":
                    uid = str(it.get("nm_id") or it.get("vendor_code") or "")
                else:
                    uid = str(it.get("offer_id") or "")
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                items.append(it)
        if len(items) < 2:
            continue
        ref = group[0]
        seq += 1
        label = str(ref.get("target_group_label") or "")
        tgt_n = int(ref.get("target_group_count") or 0)
        cat_label = str(ref.get("category_label") or _candidate_label(items, marketplace=marketplace))
        entry: Dict[str, Any] = {
            "candidate_id": f"attach-batch-{marketplace}-{seq}",
            "kind": "attach_batch",
            "marketplace": marketplace,
            "category_label": cat_label,
            "count": len(items),
            "target_group_count": tgt_n,
            "target_group_id": ref.get("target_group_id"),
            "target_group_label": label,
            "source_candidate_ids": source_ids,
            "hint": f"Добавить {len(items)} товаров в «{label}» ({tgt_n} шт.)",
            "items": items,
            "sample_items": ref.get("sample_items") or [],
        }
        if marketplace == "wb":
            entry["suggested_target_imt"] = int(ref.get("suggested_target_imt") or ref.get("target_group_id") or 0)
            entry["subject_id"] = ref.get("subject_id")
        else:
            entry["suggested_model_name"] = str(ref.get("suggested_model_name") or label).strip()
            entry["category_key"] = ref.get("category_key")
        batches.append(entry)

    if not batches:
        return list(suggestions)

    out: List[dict] = []
    for c in suggestions:
        cid = str(c.get("candidate_id") or "")
        if cid and cid in consumed:
            continue
        out.append(c)
    out.extend(batches)
    out.sort(
        key=lambda x: (
            str(x.get("category_label") or "").lower(),
            0 if (x.get("kind") or "") == "attach_batch" else 1,
            str(x.get("target_group_label") or "").lower(),
            -(x.get("count") or 0),
        )
    )
    return out[:120]


def _titles_similar(base_a: str, base_b: str) -> bool:
    if not base_a or not base_b:
        return False
    if base_a == base_b:
        return True
    return base_a in base_b or base_b in base_a


def _linked_groups(groups: List[dict]) -> List[dict]:
    return [
        g
        for g in groups
        if g.get("linked")
        and len(g.get("items") or []) >= 2
        and str(g.get("group_id") or "") != "__unlinked__"
    ]


def _groups_same_category(g1: dict, g2: dict, *, marketplace: str) -> bool:
    if marketplace == "wb":
        s1 = int(g1.get("subject_id") or 0)
        s2 = int(g2.get("subject_id") or 0)
        if s1 and s2 and s1 != s2:
            return False
        p1 = int(g1.get("parent_id") or 0)
        p2 = int(g2.get("parent_id") or 0)
        if p1 and p2 and p1 != p2:
            return False
        return True
    c1 = str(g1.get("category_key") or (g1.get("items") or [{}])[0].get("category_key") or "")
    c2 = str(g2.get("category_key") or (g2.get("items") or [{}])[0].get("category_key") or "")
    if c1 and c2 and c1 != c2:
        return False
    return True


def _groups_titles_related(g1: dict, g2: dict) -> bool:
    """Похожи ли названия товаров в двух связках."""
    for it1 in (g1.get("items") or [])[:10]:
        b1 = _title_base_key(it1.get("title") or "")
        if not b1:
            continue
        for it2 in (g2.get("items") or [])[:10]:
            b2 = _title_base_key(it2.get("title") or "")
            if b2 and _titles_related_enough(b1, b2):
                return True
    return False


def _item_matches_group(item: dict, group: dict) -> bool:
    u_base = _title_base_key(item.get("title") or "")
    if not u_base:
        return False
    for it in group.get("items") or []:
        b = _title_base_key(it.get("title") or "")
        if b and _titles_related_enough(u_base, b):
            return True
    return False


def _item_matches_group_attach(item: dict, group: dict) -> bool:
    """Мягче, чем для перепроверки: одиночку чаще предлагаем в существующую связку."""
    if _item_matches_group(item, group):
        return True
    u_base = _title_base_key(item.get("title") or "")
    if not u_base:
        return False
    brand_u = (item.get("brand") or "").lower().strip()
    u_tokens = _title_tokens(u_base)
    for it in group.get("items") or []:
        g_base = _title_base_key(it.get("title") or "")
        if not g_base:
            continue
        if _titles_similar(u_base, g_base):
            return True
        g_tokens = _title_tokens(g_base)
        shared = u_tokens & g_tokens
        if not shared:
            continue
        brand_g = (it.get("brand") or "").lower().strip()
        if brand_u and brand_g and brand_u == brand_g:
            return True
        if len(shared) >= 2:
            return True
        if len(shared) == 1 and any(len(tok) >= 5 for tok in shared):
            return True
    return False


def _best_larger_target_group(
    item: dict,
    source_g: dict,
    groups: List[dict],
    *,
    marketplace: str,
) -> Optional[dict]:
    """Связка побольше, куда логичнее перенести товар (чем больше — тем лучше)."""
    src_n = len(source_g.get("items") or [])
    src_id = str(source_g.get("group_id") or "")
    best: Optional[dict] = None
    best_n = 0
    for h in groups:
        hid = str(h.get("group_id") or "")
        if not hid or hid == src_id:
            continue
        if not _groups_same_category(source_g, h, marketplace=marketplace):
            continue
        h_n = len(h.get("items") or [])
        if h_n <= src_n or h_n >= MAX_LINK_ITEMS:
            continue
        if marketplace == "wb":
            if int(item.get("imt_id") or 0) == int(h.get("group_id") or 0):
                continue
        else:
            if str(item.get("model_name") or "").strip() == str(h.get("group_label") or "").strip():
                continue
        if not _item_matches_group(item, h):
            continue
        if h_n > best_n:
            best = h
            best_n = h_n
    return best


def suggest_review_linked_groups(
    groups: List[dict],
    *,
    marketplace: str,
) -> List[dict]:
    """Перепроверка существующих связок: перепривязка и объединение двух групп."""
    multi = _linked_groups(groups)
    if len(multi) > _REVIEW_MULTI_GROUP_CAP:
        multi = sorted(multi, key=lambda g: -len(g.get("items") or []))[:_REVIEW_MULTI_GROUP_CAP]
    if len(multi) < 1:
        return []

    out: List[dict] = []
    seq = 0
    seen_merge: set[str] = set()
    seen_relocate: set[str] = set()

    for i, g1 in enumerate(multi):
        items1 = g1.get("items") or []
        if not items1:
            continue
        for g2 in multi[i + 1 :]:
            if not _groups_same_category(g1, g2, marketplace=marketplace):
                continue
            gid1 = str(g1.get("group_id") or "")
            gid2 = str(g2.get("group_id") or "")
            if not gid1 or gid1 == gid2:
                continue
            pair_key = "|".join(sorted([gid1, gid2]))
            if pair_key in seen_merge:
                continue
            items2 = g2.get("items") or []
            if not _groups_titles_related(g1, g2):
                continue
            if len(items1) >= len(items2):
                target_g, source_g = g1, g2
            else:
                target_g, source_g = g2, g1
            source_items = list(source_g.get("items") or [])
            target_items = list(target_g.get("items") or [])
            if marketplace == "wb":
                target_id = int(target_g.get("group_id") or 0)
                to_move = [
                    it
                    for it in source_items
                    if int(it.get("imt_id") or 0) != target_id
                ]
            else:
                target_model = str(target_g.get("group_label") or "").strip()
                to_move = [
                    it
                    for it in source_items
                    if str(it.get("model_name") or "").strip() != target_model
                ]
            if not to_move:
                continue
            if len(target_items) + len(to_move) > MAX_LINK_ITEMS:
                continue
            seen_merge.add(pair_key)
            seq += 1
            cat_label = _candidate_label(to_move, marketplace=marketplace)
            entry: Dict[str, Any] = {
                "candidate_id": f"merge-grp-{marketplace}-{seq}",
                "kind": "merge_groups",
                "marketplace": marketplace,
                "category_label": cat_label,
                "count": len(to_move),
                "hint": (
                    f"Объединить «{source_g.get('group_label')}» ({len(source_items)}) "
                    f"→ «{target_g.get('group_label')}» ({len(target_items)})"
                ),
                "source_group_id": source_g.get("group_id"),
                "source_group_label": source_g.get("group_label"),
                "target_group_id": target_g.get("group_id"),
                "target_group_label": target_g.get("group_label"),
                "source_group_count": len(source_items),
                "target_group_count": len(target_items),
                "items": to_move,
                "sample_items": target_items[:5],
            }
            ref0 = items1[0] if items1 else {}
            if marketplace == "wb":
                entry["suggested_target_imt"] = int(target_g.get("group_id") or 0)
                entry["subject_id"] = target_g.get("subject_id") or ref0.get("subject_id")
            else:
                entry["suggested_model_name"] = str(target_g.get("group_label") or "").strip()
                entry["category_key"] = (
                    target_g.get("category_key") or ref0.get("category_key")
                )
            out.append(entry)

    for g in multi:
        items = g.get("items") or []
        if not items:
            continue
        gid = str(g.get("group_id") or "")
        src_n = len(items)
        for it in items:
            best_h = _best_larger_target_group(it, g, multi, marketplace=marketplace)
            if not best_h:
                continue
            h_items = best_h.get("items") or []
            hgid = str(best_h.get("group_id") or "")
            if marketplace == "wb":
                uid = str(it.get("nm_id") or it.get("vendor_code") or "")
            else:
                uid = str(it.get("offer_id") or "")
            dedupe = f"{uid}:{hgid}"
            if dedupe in seen_relocate:
                continue
            seen_relocate.add(dedupe)
            seq += 1
            h_n = len(h_items)
            cat_label = _candidate_label([it], marketplace=marketplace)
            entry = {
                "candidate_id": f"relocate-{marketplace}-{seq}",
                "kind": "relocate",
                "marketplace": marketplace,
                "category_label": cat_label,
                "count": 1,
                "source_group_count": src_n,
                "target_group_count": h_n,
                "hint": (
                    f"В более крупную связку: из «{g.get('group_label')}» ({src_n} шт.) "
                    f"→ «{best_h.get('group_label')}» ({h_n} шт.)"
                ),
                "source_group_id": g.get("group_id"),
                "source_group_label": g.get("group_label"),
                "target_group_id": best_h.get("group_id"),
                "target_group_label": best_h.get("group_label"),
                "items": [it],
                "sample_items": h_items[:5],
            }
            if marketplace == "wb":
                entry["suggested_target_imt"] = int(best_h.get("group_id") or 0)
                entry["subject_id"] = it.get("subject_id")
            else:
                entry["suggested_model_name"] = str(best_h.get("group_label") or "").strip()
                entry["category_key"] = it.get("category_key")
            out.append(entry)

    out.sort(
        key=lambda x: (
            0 if x.get("kind") == "merge_groups" else 1,
            -(x.get("target_group_count") or x.get("count") or 0),
            x.get("source_group_count") or 0,
            x.get("hint") or "",
        )
    )
    return out[:120]


_IKEA_BRAND_RE = re.compile(r"\bikea\b|ике[яи]", re.I)
_NO_NAME_BRAND_RE = re.compile(
    r"ноунейм|no[\s-]?name|без\s*бренда|noname|нет\s*бренда|безымян",
    re.I,
)
_VOLUME_RE = re.compile(
    r"\b\d+[\.,]?\d*\s*(мл|ml|л|l|г|g|кг|kg|мм|mm|см|cm)\b",
    re.I,
)


def _row_article(row: dict, marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    if mp == "wb":
        return str(row.get("vendor_code") or "").strip()
    return str(row.get("offer_id") or "").strip()


def _row_category_key(row: dict, marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    if mp == "wb":
        sid = int(row.get("subject_id") or 0)
        pid = int(row.get("parent_id") or 0)
        return f"wb:{sid}:{pid}"
    return f"oz:{row.get('category_key') or '0:0'}"


def _row_category_label(row: dict, marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    name = str(row.get("subject_name") or row.get("category_label") or "").strip()
    if name:
        return name
    if mp == "wb":
        parent = str(row.get("parent_name") or "").strip()
        if parent:
            return parent
    return _row_category_key(row, mp)


def _build_ai_category_batch_jobs(
    by_cat: Dict[str, List[dict]],
    groups_by_cat: Dict[str, List[dict]],
    *,
    marketplace: str,
    max_per_request: int,
) -> List[Tuple[str, List[dict], List[dict], str]]:
    """Один запрос ИИ на категорию; режем только если категория больше лимита."""
    mp = (marketplace or "").strip().lower()
    cap = max(10, min(int(max_per_request or 60), 150))
    jobs: List[Tuple[str, List[dict], List[dict], str]] = []
    for ck, cat_rows in by_cat.items():
        if len(cat_rows) < 2:
            continue
        cat_groups = groups_by_cat.get(ck, [])
        cat_label = _row_category_label(cat_rows[0], mp)
        if len(cat_rows) <= cap:
            jobs.append((ck, cat_rows, cat_groups, cat_label))
            continue
        total_parts = (len(cat_rows) + cap - 1) // cap
        for part_idx, i in enumerate(range(0, len(cat_rows), cap), start=1):
            batch = cat_rows[i : i + cap]
            if len(batch) < 2:
                continue
            part_label = f"{cat_label} ({part_idx}/{total_parts})" if total_parts > 1 else cat_label
            jobs.append((ck, batch, cat_groups, part_label))
    jobs.sort(key=lambda x: -len(x[1]))
    return jobs


_TITLE_BRAND_STOP = frozenset(
    {
        "гигиеническая", "гигиенический", "гигиеническую", "помада", "помаду", "помады",
        "бальзам", "крем", "жидкая", "natural", "для", "губ", "spf", "bio", "liquid", "pure",
        "naturkosmetik", "natürkosmetik", "naturokosmetik", "the", "for", "lip",
        "men", "med", "care", "sensitive", "original", "hyaluron",
    },
)

_TITLE_BRAND_EXTRACT_PATTERNS = [
    re.compile(r"помад\w*\s+([A-Za-z][A-Za-z0-9\-]{1,25})", re.I),
    re.compile(r"бальзам\s+для\s+губ\s+([A-Za-z][A-Za-z0-9\-]{1,25})", re.I),
    re.compile(r"[-–—]\s*([A-Za-z][A-Za-z0-9\-]{1,25})\s*[-–—,]", re.I),
    re.compile(r"\b([A-Z][A-Z0-9\-]{2,24})\b"),
]


_STRICT_SEPARATE_BRANDS = frozenset(
    {
        "labello", "balea", "isana", "lavera", "alverde", "alviana",
        "benecos", "cosnature", "denkmit", "sante", "sundance",
    },
)

_STRICT_BRAND_RE = re.compile(
    r"\b("
    + "|".join(sorted(map(re.escape, _STRICT_SEPARATE_BRANDS), key=len, reverse=True))
    + r")\b",
    re.I,
)

_USE_MERGE_BRAND_SCOPE = frozenset({"lips", "lipstick", "lip_care", "hair_gel"})
# Склейка по бренду (не imtID / не линейке): губы, блески, гели для волос.


def _strict_brands_in_text(text: str) -> set[str]:
    return {m.group(1).lower() for m in _STRICT_BRAND_RE.finditer(text or "")}


def _strict_brands_in_row(row: dict) -> set[str]:
    parts = [str(row.get("brand") or ""), str(row.get("title") or "")]
    found: set[str] = set()
    for p in parts:
        found |= _strict_brands_in_text(p)
    return found


def _row_brand_key(row: dict) -> str:
    b = str(row.get("brand") or "").strip()
    if b and not _NO_NAME_BRAND_RE.search(b):
        bk = b.lower()
        if bk in _STRICT_SEPARATE_BRANDS:
            return bk
    title = str(row.get("title") or "")
    found = _strict_brands_in_text(f"{b} {title}")
    if len(found) == 1:
        return next(iter(found))
    if found:
        for m in _STRICT_BRAND_RE.finditer(title):
            return m.group(1).lower()
    for pat in _TITLE_BRAND_EXTRACT_PATTERNS:
        m = pat.search(title)
        if m:
            w = m.group(1).lower()
            if w not in _TITLE_BRAND_STOP and len(w) >= 2:
                return w
    return ""


def _items_same_brand_key(items: List[dict]) -> bool:
    strict: set[str] = set()
    fallback: set[str] = set()
    for x in items:
        sb = _strict_brands_in_row(x)
        if len(sb) > 1:
            return False
        strict |= sb
        fk = _row_brand_key(x)
        if fk and fk not in _STRICT_SEPARATE_BRANDS:
            fallback.add(fk)
    if len(strict) > 1:
        return False
    if strict and fallback and not fallback <= strict:
        return False
    if len(strict) == 1:
        return True
    fallback.discard("")
    return len(fallback) <= 1


def _product_line_key(row: dict, *, marketplace: str = "wb") -> str:
    """Линейка товара (бренд + название без фасовки). Объём в г/мл не используем для разделения."""
    brand = _row_brand_key(row)
    base = _title_base_key(row.get("title") or "")
    base = re.sub(r"\b\d+[\.,]?\d*\s*(мл|ml|л|l|г|g)\b", " ", base, flags=re.I)
    base = re.sub(r"\s+", " ", base).strip()
    if brand:
        return f"{brand}\x00{base}"
    return base


def _use_merge_scope_key(row: dict, merge_use: str, *, marketplace: str) -> str:
    if merge_use in _USE_MERGE_BRAND_SCOPE:
        return _row_brand_key(row) or _product_line_key(row, marketplace=marketplace)
    return _product_line_key(row, marketplace=marketplace)


def _row_brand_extended(row: dict) -> str:
    b = str(row.get("brand") or "").strip()
    if b and not _NO_NAME_BRAND_RE.search(b):
        return b
    key = _row_brand_key(row)
    if not key:
        return ""
    title = str(row.get("title") or "")
    m = re.search(re.escape(key), title, re.I)
    if m:
        return m.group(0)
    return key


def _brand_bucket(row: dict) -> str:
    b = str(row.get("brand") or "").strip().lower()
    t = (row.get("title") or "").lower()
    if _IKEA_BRAND_RE.search(b) or _IKEA_BRAND_RE.search(t):
        return "ikea"
    if not b or _NO_NAME_BRAND_RE.search(b) or _NO_NAME_BRAND_RE.search(t):
        return "noname"
    return "named"


def _items_brand_buckets_compatible(items: List[dict]) -> bool:
    buckets = {_brand_bucket(x) for x in items}
    if "ikea" in buckets and "noname" in buckets:
        return False
    if "noname" in buckets and "named" in buckets:
        return False
    return True


_PRODUCT_USE_PATTERNS: List[Tuple[str, str]] = [
    (
        "hair_rinse",
        r"бальзам[\s\-–—]*ополаскиватель|ополаскиватель(?:\s+для)?\s+волос|"
        r"кондиционер\s+для\s+волос|бальзам\s+для\s+волос|"
        r"haarspülung|conditioner",
    ),
    (
        "hair_gel",
        r"гель\s+для\s+волос|гель[\s\-–—]*стайлинг|"
        r"haargel|hair\s+gel|hair\s+styling\s+gel",
    ),
    ("hair", r"шампунь|маска\s+для\s+волос|спрей\s+для\s+волос|shampoo|trockenshampoo"),
    (
        "lips",
        r"гигиеническ\w*\s+помад|"
        r"бальзам\s+для\s+губ|для\s+губ|губн|"
        r"lip\s+balms?|lip\s+care|lippenpflege|lippenbalsam|"
        r"\blabello\b",
    ),
    (
        "lipstick",
        r"губная\s+помада|помада\s+для\s+губ|жидкая\s+помад\w*|"
        r"блеск\s+для\s+губ|тинт\s+для\s+губ|"
        r"lipstick|lip\s*gloss|lip\s*tint|lippenstift",
    ),
    (
        "hands",
        r"бальзам\s+для\s+рук|крем\s+для\s+рук|"
        r"hand\s+balms?|hand\s+creams?|handcreme",
    ),
    (
        "feet",
        r"бальзам\s+для\s+ног|крем\s+для\s+ног|"
        r"foot\s+creams?|fußcreme|fusscreme",
    ),
    (
        "body",
        r"бальзам\s+для\s+тела|крем\s+для\s+тела|"
        r"лосьон\s+для\s+тела|молочко\s+для\s+тела|"
        r"гель\s+для\s+душа|duschgel|duschcreme|"
        r"body\s+creams?|body\s+lotion|körpercreme|körperlotion",
    ),
    (
        "face",
        r"крем\s+для\s+лица|"
        r"face\s+creams?|gesichtscreme|"
        r"дневной\s+крем|ночной\s+крем|увлажняющий\s+крем",
    ),
]

_CATEGORY_USE_HINTS: List[Tuple[str, str]] = [
    ("hair_rinse", r"ополаскиватель|кондиционер|бальзам.*волос"),
    ("hair_gel", r"гель.*волос|haargel"),
    ("hair", r"шампун"),
    ("lips", r"гигиеническ.*помад|бальзам.*губ|губн|lip\s*balm"),
    ("lipstick", r"блеск|губная\s+помад|помада\s+для|жидкая\s+помад|lipstick"),
    ("hands", r"для\s+рук|handcreme"),
    ("feet", r"для\s+ног"),
    ("body", r"для\s+тела|душ|duschgel|duschcreme|körper"),
    ("face", r"для\s+лица|gesicht"),
]

_USE_BUCKET_MERGEABLE = frozenset(
    {"lips", "lipstick", "hands", "feet", "body", "face", "hair_rinse", "hair", "hair_gel"},
)

# lips + lipstick (гигиеническая помада + цветная помада) — одна склейка в subjectID
_USE_BUCKET_MERGE_GROUP: Dict[str, str] = {
    "lips": "lip_care",
    "lipstick": "lip_care",
}

_USE_BUCKET_LABEL_RU: Dict[str, str] = {
    "lip_care": "для губ (бальзамы и гигиенические помады)",
    "lips": "для губ (бальзамы)",
    "lipstick": "помады и блеск",
    "hands": "для рук",
    "feet": "для ног",
    "body": "для тела",
    "face": "для лица",
    "hair_rinse": "для волос (ополаскиватель)",
    "hair": "для волос",
    "hair_gel": "гель для волос",
}


def _use_bucket_merge_group(use: str) -> str:
    return _USE_BUCKET_MERGE_GROUP.get(use, use)


def _category_use_bucket(row: dict) -> str:
    text = " ".join(
        str(row.get(k) or "") for k in ("subject_name", "parent_name")
    ).strip().lower()
    if not text:
        return ""
    for key, pat in _CATEGORY_USE_HINTS:
        if re.search(pat, text, re.IGNORECASE):
            return key
    return ""


def _product_use_bucket(title: str) -> str:
    t = (title or "").strip().lower()
    if not t:
        return ""
    for key, pat in _PRODUCT_USE_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return key
    if re.search(r"\bбальзам\b", t, re.IGNORECASE):
        return "balm_unspecified"
    if re.search(r"\bкрем\b", t, re.IGNORECASE):
        return "cream_unspecified"
    return ""


def _row_use_bucket(row: dict) -> str:
    bucket = _product_use_bucket(str(row.get("title") or ""))
    if bucket in ("", "balm_unspecified", "cream_unspecified"):
        from_cat = _category_use_bucket(row)
        if from_cat:
            return from_cat
    return bucket


def _items_product_use_compatible(items: List[dict]) -> bool:
    buckets = {_row_use_bucket(x) for x in items}
    buckets.discard("")
    if len(buckets) <= 1:
        return True
    return False


def _items_same_category(items: List[dict], marketplace: str) -> bool:
    if not items:
        return False
    mp = (marketplace or "").strip().lower()
    if mp == "wb":
        sids = {int(x.get("subject_id") or 0) for x in items}
        sids.discard(0)
        return len(sids) <= 1
    cats = {str(x.get("category_key") or "") for x in items}
    cats.discard("")
    return len(cats) <= 1


def _split_items_to_bundles(items: List[dict], max_size: int = MAX_LINK_ITEMS) -> List[List[dict]]:
    if len(items) <= max_size:
        return [items]
    out: List[List[dict]] = []
    chunk: List[dict] = []
    for it in items:
        chunk.append(it)
        if len(chunk) >= max_size:
            out.append(chunk)
            chunk = []
    if len(chunk) >= 2:
        out.append(chunk)
    elif chunk and out:
        out[-1].extend(chunk)
    return out


def _compact_product_for_ai(row: dict, marketplace: str) -> dict:
    mp = (marketplace or "").strip().lower()
    art = _row_article(row, mp)
    gid = str(row.get("link_group_id") or row.get("imt_id") or row.get("model_name") or "")
    return {
        "article": art,
        "mp_id": row.get("nm_id") or row.get("sku"),
        "title": (row.get("title") or "")[:120],
        "brand": _row_brand_extended(row),
        "brand_bucket": _brand_bucket(row),
        "category": row.get("subject_name") or row.get("category_label") or row.get("category_key"),
        "subject_id": row.get("subject_id"),
        "linked": bool(row.get("linked")),
        "current_group_id": gid or None,
        "current_group_label": row.get("link_group_label") or gid or None,
        "normalized_name": _title_base_key(row.get("title") or ""),
    }


def _compact_group_for_ai(g: dict, marketplace: str) -> dict:
    items = g.get("items") or []
    sample = items[0] if items else {}
    return {
        "group_id": g.get("group_id"),
        "group_label": g.get("group_label"),
        "category": g.get("subject_name") or g.get("category_label"),
        "subject_id": g.get("subject_id") or sample.get("subject_id"),
        "count": len(items),
        "titles": [(x.get("title") or "")[:80] for x in items[:6]],
        "articles": [_row_article(x, marketplace) for x in items[:8]],
    }


def _filter_rows_for_ai_scope(rows: List[dict], scope: str, marketplace: str) -> List[dict]:
    sc = (scope or "all").strip().lower()
    if sc == "unlinked":
        return [r for r in rows if not r.get("linked")]
    if sc == "linked":
        return [r for r in rows if r.get("linked")]
    return list(rows)


def default_ai_system_prompt(marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    n = MAX_LINK_ITEMS
    common = (
        "ГЛАВНОЕ (приоритет №1): варианты ОДНОГО товара (1 шт / 2 шт / 3 шт / набор / комплект) "
        "— ВСЕГДА в одной связке, даже если сейчас в разных imtID. Никогда не дроби фасовки по разным связкам.\n"
        "\n"
        "ЖЁСТКИЕ ПРАВИЛА:\n"
        "1) Одна категория (subject_id) на связку — не смешивать разные категории.\n"
        f"2) Не более {n} карточек в одной связке. Если товаров в категории больше {n} — делить на несколько связок "
        f"(пример: 42 → {n} + 12). Набивай каждую связку по максимуму до {n}: не делай связки по 1–3 карточки, "
        f"если в ту же связку можно добавить другие логически близкие товары этой категории "
        f"(15 карточек в одной связке — нормально, если они подходят по смыслу).\n"
        "3) Сначала делить по НАЗНАЧЕНИЮ — разное назначение НИКОГДА вместе. "
        "Затем в пределах одного subjectID и ОДНОГО БРЕНДА — одна связка до лимита. "
        "НЕЛЬЗЯ смешивать в одной связке: Labello, Balea, ISANA, lavera, alverde, alviana, "
        "benecos, Cosnature, Denkmit, Sante, SUNDANCE — даже если категория одна. "
        "Внутри одного бренда: все оттенки/вкусы вместе (lavera 03+04); "
        "все гели для волос одного бренда вместе (Balea MEN Wet Look + Ultra Strong); "
        "фасовки 1/2/3/5 шт одной линейки — всегда вместе. "
        "Гели для душа: разные линейки ISANA (Urea ≠ Cream & Care) — разные связки. "
        "«Гигиеническая помада» = бальзам для губ. Не делить по объёму (мл/г) — только по бренду и линейке.\n"
        "4) Один бренд на связку; бренд из колонки и названия; "
        "IKEA не смешивать с ноунейм; ноунейм не смешивать с именованными брендами.\n"
        "5) IKEA и аксессуары-хранение (сумки, косметички, контейнеры) — можно разные SKU одного класса в одной связке.\n"
        "6) Запчасти телефонов — группировать по модели устройства (iPhone 14, Samsung A54…).\n"
        "7) Заполняй связки по максимуму внутри логической группы (до лимита).\n"
        "8) Анализируй ВСЕ товары, включая уже связанные — предлагай relocate/merge, если текущая связка неоптимальна.\n"
        "9) При сомнении — не объединяй.\n"
        f"10) НЕ объединяй весь бренд категории в одну связку (300 кремов Balea — это много связок, не одна). "
        f"В одной связке: фасовки 1/2/3 одной линейки + при необходимости другие близкие SKU той же категории, "
        f"но суммарно не более {n} карточек и без смешения несовместимых продуктов (п.3).\n"
        f"11) Итог: «красивые» связки — логично укомплектованные, до {n} карточек; товары категории распределены "
        "между несколькими связками без лишних мелких обрывков по 1–2 SKU.\n"
    )
    if mp == "wb":
        return (
            "Ты эксперт по связкам карточек Wildberries (WB). Связка = общий imtID.\n"
            + common
            + f"Верни ТОЛЬКО JSON-массив без markdown. Каждый элемент — одна связка (до {n} article_ids). Формат элемента:\n"
            '{"cluster_id":"c1","kind":"new_link"|"attach"|"relocate"|"merge_groups",'
            '"article_ids":["vendor_code",...],"target_group_id":null|123456789,'
            '"suggested_model_name":null,"normalized_product_name":"...","detected_brand":"...",'
            '"confidence":"high"|"medium"|"low","reason":"кратко по-русски"}.\n'
            "article_ids — vendor_code из списка. attach — 1 товар в существующую связку. "
            "relocate — перенести в другую связку. merge_groups — объединить несколько в target imtID. "
            "new_link — новая связка (target_group_id=null)."
        )
    return (
        "Ты эксперт по связкам карточек Ozon. Связка = одно «Название модели».\n"
        + common
        + "Верни ТОЛЬКО JSON-массив без markdown. Элемент:\n"
        '{"cluster_id":"c1","kind":"new_link"|"attach"|"relocate"|"merge_groups",'
        '"article_ids":["offer_id",...],"target_group_id":null|"имя модели",'
        '"suggested_model_name":"название модели","normalized_product_name":"...",'
        '"detected_brand":"...","confidence":"high"|"medium"|"low","reason":"кратко по-русски"}.\n'
        "article_ids — offer_id из списка."
    )


def resolve_ai_system_prompt(marketplace: str, custom: Optional[str] = None) -> str:
    text = (custom or "").strip()
    if not text:
        return default_ai_system_prompt(marketplace)
    return text.replace("{max_link_items}", str(MAX_LINK_ITEMS))


def _resolve_target_group_id(
    cl: dict,
    groups: List[dict],
    marketplace: str,
) -> Tuple[int, str]:
    mp = (marketplace or "").strip().lower()
    tgt_raw = cl.get("target_group_id") or cl.get("target_imt") or cl.get("target_group_label")
    if mp == "wb":
        tgt_imt = 0
        if tgt_raw is not None:
            try:
                tgt_imt = int(tgt_raw)
            except (TypeError, ValueError):
                for g in groups:
                    if str(g.get("group_label")) == str(tgt_raw):
                        tgt_imt = int(g.get("group_id") or 0)
                        break
        return tgt_imt, f"imtID {tgt_imt}" if tgt_imt else ""
    model = str(cl.get("suggested_model_name") or cl.get("target_group_id") or tgt_raw or "").strip()
    return 0, model


def _build_ai_candidate_entry(
    cl: dict,
    items: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
    seq: int,
    source: str = "ai",
) -> dict:
    mp = (marketplace or "").strip().lower()
    kind = str(cl.get("kind") or "new_link").strip()
    if kind not in ("new_link", "attach", "relocate", "merge_groups"):
        kind = "new_link"
    tgt_imt, tgt_label = _resolve_target_group_id(cl, groups, mp)
    group_by_id = {str(g.get("group_id")): g for g in groups if g.get("group_id")}
    prefix = "pack" if source == "pack" else "ai"
    entry: Dict[str, Any] = {
        "candidate_id": f"{prefix}-{mp}-{seq}",
        "kind": kind,
        "marketplace": mp,
        "category_label": _candidate_label(items, marketplace=mp),
        "count": len(items),
        "hint": str(cl.get("reason") or cl.get("hint") or "Подсказка ИИ")[:240],
        "items": items,
        "ai": True,
        "ai_source": source,
        "confidence": str(cl.get("confidence") or "medium").strip().lower(),
        "normalized_product_name": str(cl.get("normalized_product_name") or "")[:120],
        "detected_brand": str(cl.get("detected_brand") or _row_brand_extended(items[0]) if items else "")[:80],
    }
    if mp == "wb":
        entry["subject_id"] = items[0].get("subject_id") if items else None
        if kind in ("attach", "relocate", "merge_groups") and tgt_imt:
            gref = group_by_id.get(str(tgt_imt)) or {}
            entry["target_group_id"] = str(tgt_imt)
            entry["target_group_label"] = tgt_label
            entry["suggested_target_imt"] = tgt_imt
            entry["sample_items"] = (gref.get("items") or [])[:3]
            if kind == "relocate" and items:
                src_gid = str(items[0].get("link_group_id") or items[0].get("imt_id") or "")
                entry["source_group_id"] = src_gid
                entry["source_group_label"] = items[0].get("link_group_label") or src_gid
        else:
            entry["suggested_target_imt"] = _wb_target_imt(items)
    else:
        entry["category_key"] = items[0].get("category_key") if items else None
        model = str(cl.get("suggested_model_name") or tgt_label or "").strip()
        if kind in ("attach", "relocate", "merge_groups") and model:
            entry["target_group_label"] = model
            entry["suggested_model_name"] = model
            entry["sample_items"] = (group_by_id.get(model, {}).get("items") or [])[:3]
        else:
            entry["suggested_model_name"] = model or _suggested_model_name(items)
    return entry


def _validate_ai_cluster_items(items: List[dict], marketplace: str) -> Optional[str]:
    if not items:
        return "пусто"
    if len(items) > MAX_LINK_ITEMS:
        return f"более {MAX_LINK_ITEMS} товаров"
    if not _items_same_category(items, marketplace):
        return "разные категории"
    if not _items_brand_buckets_compatible(items):
        return "несовместимые бренды (IKEA/ноунейм)"
    if not _items_same_brand_key(items):
        return "разные бренды (Labello/Balea/ISANA/… — не смешивать)"
    if not _items_product_use_compatible(items):
        return "разное назначение (губы/помады/руки/волосы/тело/лицо)"
    return None


def _pick_merge_target_group(
    items: List[dict],
    cluster: List[dict],
    *,
    marketplace: str,
) -> Tuple[int, str]:
    """Куда собрать: imtID / модель с наибольшим числом товаров в кластере."""
    mp = (marketplace or "").strip().lower()
    scores: Counter[str] = Counter()
    for it in items:
        if mp == "wb":
            gid = str(it.get("link_group_id") or it.get("imt_id") or "").strip()
        else:
            gid = str(it.get("model_name") or it.get("link_group_id") or "").strip()
        if gid and it.get("linked"):
            scores[gid] += 1
    for c in cluster:
        if mp == "wb":
            tgt = str(c.get("target_group_id") or c.get("suggested_target_imt") or "").strip()
        else:
            tgt = str(
                c.get("suggested_model_name") or c.get("target_group_label") or c.get("target_group_id") or "",
            ).strip()
        if tgt:
            scores[tgt] += 3
    if not scores:
        return 0, ""
    best = scores.most_common(1)[0][0]
    if mp == "wb":
        try:
            return int(best), f"imtID {best}"
        except (TypeError, ValueError):
            return 0, ""
    return 0, best


def _merge_ai_suggestions_by_pack_key(
    suggestions: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
    start_seq: int = 0,
) -> Tuple[List[dict], int]:
    """Гарантирует: 1/2/3 шт одного товара — одна связка (не по разным imtID)."""
    mp = (marketplace or "").strip().lower()
    if not suggestions:
        return suggestions, 0

    pack_items: Dict[str, List[dict]] = {}
    pack_cids: Dict[str, set[str]] = {}
    cand_by_id = {str(c.get("candidate_id") or ""): c for c in suggestions if c.get("candidate_id")}

    for c in suggestions:
        cid = str(c.get("candidate_id") or "")
        for it in c.get("items") or []:
            pk = _product_pack_key(it, mp)
            if not pk:
                continue
            art = _row_article(it, mp)
            if not art:
                continue
            bucket = pack_items.setdefault(pk, [])
            if art not in {_row_article(x, mp) for x in bucket}:
                bucket.append(it)
            if cid:
                pack_cids.setdefault(pk, set()).add(cid)

    consumed: set[str] = set()
    merged_out: List[dict] = []
    seq = start_seq
    pack_merge_n = 0

    for pk, items in pack_items.items():
        if len(items) < 2 or not _items_are_pack_variants(items, marketplace=mp):
            continue
        cids = pack_cids.get(pk) or set()
        if len(cids) == 1:
            only = cand_by_id.get(next(iter(cids)))
            if only:
                pack_arts = {_row_article(x, mp) for x in items}
                cand_arts = {_row_article(x, mp) for x in only.get("items") or []}
                if pack_arts <= cand_arts:
                    continue

        cluster = [cand_by_id[cid] for cid in cids if cid in cand_by_id]
        if not cluster:
            continue
        tgt_imt, tgt_model = _pick_merge_target_group(items, cluster, marketplace=mp)
        seq += 1
        label = _candidate_label(items, marketplace=mp)
        if mp == "wb" and tgt_imt:
            cl: Dict[str, Any] = {
                "kind": "merge_groups",
                "target_group_id": tgt_imt,
                "reason": f"Фасовки 1/2/3 в одной связке · {label}",
                "confidence": "high",
                "normalized_product_name": _title_base_key(items[0].get("title") or ""),
            }
        elif mp == "ozon" and tgt_model:
            cl = {
                "kind": "merge_groups",
                "target_group_id": tgt_model,
                "suggested_model_name": tgt_model,
                "reason": f"Фасовки 1/2/3 в одной связке · {label}",
                "confidence": "high",
                "normalized_product_name": _title_base_key(items[0].get("title") or ""),
            }
        else:
            cl = {
                "kind": "new_link",
                "reason": f"Новая связка · фасовки 1/2/3 · {label}",
                "confidence": "high",
                "normalized_product_name": _title_base_key(items[0].get("title") or ""),
            }
        merged_out.append(
            _build_ai_candidate_entry(cl, items, groups, marketplace=mp, seq=seq, source="pack_merge")
        )
        consumed.update(cids)
        pack_merge_n += 1

    if not consumed:
        return suggestions, 0
    out = [c for c in suggestions if str(c.get("candidate_id") or "") not in consumed]
    out.extend(merged_out)
    return out, pack_merge_n


def _use_merge_category_key(c: dict, row: dict, *, marketplace: str) -> str:
    """Ключ для склейки по назначению: WB — subjectID (требование API при объединении imtID)."""
    mp = (marketplace or "").strip().lower()
    if mp == "wb":
        sid = int(row.get("subject_id") or c.get("subject_id") or 0)
        if sid:
            return f"wb:subject:{sid}"
        pid = int(row.get("parent_id") or 0)
        if pid:
            return f"wb:parent:{pid}"
    return _candidate_category_key(c, marketplace=mp) or _row_category_key(row, mp)


def _merge_ai_suggestions_by_use_bucket(
    suggestions: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
    start_seq: int = 0,
) -> Tuple[List[dict], int]:
    """Склеивает предложения с одним брендом и назначением в subjectID (оттенки lavera вместе)."""
    mp = (marketplace or "").strip().lower()
    if not suggestions:
        return suggestions, 0

    by_key: Dict[str, List[dict]] = {}
    passthrough: List[dict] = []

    for c in suggestions:
        items = c.get("items") or []
        if not items:
            passthrough.append(c)
            continue
        uses = {_row_use_bucket(it) for it in items}
        uses.discard("")
        if len(uses) != 1:
            passthrough.append(c)
            continue
        use = next(iter(uses))
        if use not in _USE_BUCKET_MERGEABLE:
            passthrough.append(c)
            continue
        brand = _row_brand_key(items[0])
        if not brand:
            passthrough.append(c)
            continue
        cat = _use_merge_category_key(c, items[0], marketplace=mp)
        merge_use = _use_bucket_merge_group(use)
        scope = _use_merge_scope_key(items[0], merge_use, marketplace=mp)
        if not scope:
            passthrough.append(c)
            continue
        by_key.setdefault(f"{cat}\x00{merge_use}\x00{scope}", []).append(c)

    consumed: set[str] = set()
    merged_out: List[dict] = []
    seq = start_seq
    use_merge_n = 0

    for bucket_key, cluster in by_key.items():
        if len(cluster) < 2:
            continue
        use = bucket_key.split("\x00")[-1]
        merge_use = bucket_key.split("\x00")[1] if "\x00" in bucket_key else use
        use_label = _USE_BUCKET_LABEL_RU.get(merge_use, merge_use)
        all_items: List[dict] = []
        seen: set[str] = set()
        for c in cluster:
            for it in c.get("items") or []:
                art = _row_article(it, mp)
                if not art or art in seen:
                    continue
                seen.add(art)
                all_items.append(it)
        if len(all_items) < 2 or not _items_product_use_compatible(all_items):
            continue
        if not _items_same_brand_key(all_items):
            continue
        brand_label = _row_brand_extended(all_items[0]) or _row_brand_key(all_items[0])
        for chunk in _split_items_to_bundles(all_items):
            if len(chunk) < 2:
                continue
            tgt_imt, tgt_model = _pick_merge_target_group(chunk, cluster, marketplace=mp)
            seq += 1
            if mp == "wb" and tgt_imt:
                cl: Dict[str, Any] = {
                    "kind": "merge_groups",
                    "target_group_id": tgt_imt,
                    "reason": f"{brand_label} · {use_label} · {len(chunk)} шт.",
                    "confidence": "high",
                    "normalized_product_name": use_label,
                    "detected_brand": brand_label,
                }
            elif mp == "ozon" and tgt_model:
                cl = {
                    "kind": "merge_groups",
                    "target_group_id": tgt_model,
                    "suggested_model_name": tgt_model,
                    "reason": f"По назначению ({use_label}) · {len(chunk)} шт.",
                    "confidence": "high",
                    "normalized_product_name": use_label,
                }
            else:
                cl = {
                    "kind": "new_link",
                    "reason": f"Новая связка · {use_label} · {len(chunk)} шт.",
                    "confidence": "high",
                    "normalized_product_name": use_label,
                }
            merged_out.append(
                _build_ai_candidate_entry(cl, chunk, groups, marketplace=mp, seq=seq, source="use_merge"),
            )
            use_merge_n += 1
        consumed.update(str(c.get("candidate_id") or "") for c in cluster if c.get("candidate_id"))

    if not consumed:
        return suggestions, 0
    out = passthrough + [c for c in suggestions if str(c.get("candidate_id") or "") not in consumed]
    out.extend(merged_out)
    return out, use_merge_n


def _bundle_bucket_use_scope(c: dict) -> str:
    items = c.get("items") or []
    if not items:
        return ""
    use = _row_use_bucket(items[0])
    return _use_bucket_merge_group(use) if use else ""


def _bundle_bucket_key(c: dict, *, marketplace: str) -> str:
    mp = (marketplace or "").strip().lower()
    cat = _candidate_category_key(c, marketplace=mp)
    kind = str(c.get("kind") or "new_link")
    items = c.get("items") or []
    brand = _row_brand_key(items[0]) if items else ""
    if not brand:
        brand = str(c.get("detected_brand") or "").strip().lower()
    merge_scope = _bundle_bucket_use_scope(c)
    if brand and merge_scope in _USE_MERGE_BRAND_SCOPE:
        return f"brand:{cat}:{brand}:{merge_scope}"
    if kind == "new_link":
        if brand:
            return f"new:{cat}:{brand}"
        label = str(
            c.get("suggested_model_name")
            or c.get("normalized_product_name")
            or "",
        ).strip().lower()
        if label:
            return f"new:{cat}:{label[:80]}"
        return f"new:{c.get('candidate_id')}"
    tgt = str(
        c.get("target_group_id")
        or c.get("suggested_target_imt")
        or c.get("suggested_model_name")
        or c.get("target_group_label")
        or "",
    ).strip()
    if brand:
        return f"tgt:{cat}:{brand}:{tgt}"
    return f"tgt:{cat}:{tgt}"


def _merged_apply_candidate(
    bucket: dict,
    moving_items: List[dict],
    op_map: Dict[str, dict],
    *,
    marketplace: str,
) -> dict:
    mp = (marketplace or "").strip().lower()
    ops = [op_map[oid] for oid in bucket.get("operations") or [] if oid in op_map]
    if not ops:
        return {}
    if len(ops) == 1 and len(moving_items) == len(ops[0].get("items") or []):
        return dict(ops[0])
    base = dict(ops[0])
    base["candidate_id"] = f"{bucket['bundle_id']}-apply"
    base["items"] = moving_items
    base["count"] = len(moving_items)
    if bucket.get("is_new_bundle"):
        base["kind"] = "new_link"
        if mp == "ozon" and bucket.get("suggested_model_name"):
            base["suggested_model_name"] = bucket["suggested_model_name"]
        if mp == "wb":
            base["suggested_target_imt"] = _wb_target_imt(moving_items)
        return base
    tgt = bucket.get("target_group_id") or bucket.get("suggested_target_imt")
    if mp == "wb" and moving_items:
        picked, _ = _pick_merge_target_group(
            moving_items,
            [op_map[oid] for oid in bucket.get("operations") or [] if oid in op_map],
            marketplace=mp,
        )
        if picked:
            tgt = picked
    base["kind"] = "merge_groups" if len(moving_items) > 1 else "attach"
    if mp == "wb" and tgt:
        base["target_group_id"] = str(tgt)
        base["suggested_target_imt"] = int(tgt)
        base["target_group_label"] = f"imtID {tgt}"
    if mp == "ozon":
        model = bucket.get("target_model_name") or bucket.get("suggested_model_name") or ""
        if model:
            base["suggested_model_name"] = model
            base["target_group_label"] = model
    return base


def consolidate_ai_bundle_previews(
    suggestions: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
) -> List[dict]:
    """Сводит операции ИИ в понятные «итоговые связки» для UI."""
    mp = (marketplace or "").strip().lower()
    if not suggestions:
        return []
    group_by_id = {str(g.get("group_id")): g for g in groups if g.get("group_id")}
    op_map = {str(c.get("candidate_id")): c for c in suggestions if c.get("candidate_id")}
    buckets: Dict[str, dict] = {}
    seq = 0

    for c in suggestions:
        key = _bundle_bucket_key(c, marketplace=mp)
        if key not in buckets:
            seq += 1
            kind = str(c.get("kind") or "new_link")
            is_new = kind == "new_link"
            tgt_id = str(c.get("target_group_id") or c.get("suggested_target_imt") or "").strip()
            tgt_model = str(
                c.get("suggested_model_name") or c.get("target_group_label") or "",
            ).strip()
            if mp == "wb":
                if is_new:
                    brand = _row_brand_extended((c.get("items") or [{}])[0])
                    if not brand:
                        brand = str(c.get("detected_brand") or "").strip()
                    bl = (
                        f"{brand} · {c.get('normalized_product_name') or 'новая связка'}"[:120]
                        if brand
                        else str(
                            c.get("suggested_model_name")
                            or c.get("normalized_product_name")
                            or c.get("category_label")
                            or f"Новая связка {seq}",
                        ).strip()
                    )
                else:
                    brand = str(c.get("detected_brand") or "").strip()
                    c_items = c.get("items") or []
                    if not brand and c_items:
                        brand = _row_brand_extended(c_items[0])
                    merge_scope = _bundle_bucket_use_scope(c)
                    if brand and merge_scope in _USE_MERGE_BRAND_SCOPE:
                        zone = _USE_BUCKET_LABEL_RU.get(merge_scope) or c.get("category_label") or ""
                        bl = f"{brand} · {zone}"[:120]
                    elif brand:
                        bl = f"{brand} · imtID {tgt_id}" if tgt_id else f"{brand} · {tgt_model or f'Связка {seq}'}"
                    else:
                        bl = f"imtID {tgt_id}" if tgt_id else (tgt_model or f"Связка {seq}")
            else:
                bl = (
                    tgt_model
                    if tgt_model and not is_new
                    else str(
                        c.get("suggested_model_name")
                        or c.get("normalized_product_name")
                        or f"Новая модель {seq}",
                    ).strip()
                )
            buckets[key] = {
                "bundle_id": f"bundle-{mp}-{seq}",
                "bundle_label": bl[:120],
                "category_label": c.get("category_label") or "",
                "is_new_bundle": is_new,
                "target_group_id": tgt_id,
                "target_model_name": tgt_model,
                "suggested_target_imt": c.get("suggested_target_imt"),
                "suggested_model_name": c.get("suggested_model_name"),
                "subject_id": c.get("subject_id"),
                "category_key": c.get("category_key"),
                "operations": [],
                "moving_by_art": {},
            }
        b = buckets[key]
        cid = str(c.get("candidate_id") or "")
        if cid and cid not in b["operations"]:
            b["operations"].append(cid)
        new_tgt = str(c.get("target_group_id") or c.get("suggested_target_imt") or "").strip()
        if new_tgt and not b.get("target_group_id"):
            b["target_group_id"] = new_tgt
            b["suggested_target_imt"] = c.get("suggested_target_imt")
        for it in c.get("items") or []:
            art = _row_article(it, mp)
            if art:
                b["moving_by_art"][art] = dict(it)

    out: List[dict] = []
    for b in buckets.values():
        moving_arts = set(b["moving_by_art"].keys())
        final_by_art: Dict[str, dict] = {}
        tgt_id = b["target_group_id"]
        is_new = b["is_new_bundle"]

        if not is_new and tgt_id and tgt_id in group_by_id:
            for it in group_by_id[tgt_id].get("items") or []:
                art = _row_article(it, mp)
                if not art:
                    continue
                if art in moving_arts:
                    final_by_art[art] = {**it, "role": "move", "moving": True}
                else:
                    final_by_art[art] = {**it, "role": "stay", "moving": False}

        for art, it in b["moving_by_art"].items():
            if art in final_by_art:
                final_by_art[art]["moving"] = True
                final_by_art[art]["role"] = "move" if final_by_art[art].get("linked") else "add"
            else:
                final_by_art[art] = {**it, "role": "add", "moving": True}

        items_list = list(final_by_art.values())
        if not items_list:
            continue
        if not _items_same_brand_key(items_list):
            continue

        item_chunks = _split_items_to_bundles(items_list) if len(items_list) > MAX_LINK_ITEMS else [items_list]
        for part_idx, chunk_items in enumerate(item_chunks):
            if len(chunk_items) < 2:
                continue
            chunk_moving = [x for x in chunk_items if x.get("moving")]
            chunk_moving_n = len(chunk_moving)
            chunk_stay_n = len(chunk_items) - chunk_moving_n
            if is_new:
                summary = f"Новая связка · {len(chunk_items)} товаров"
            else:
                parts = []
                if chunk_moving_n:
                    parts.append(f"+{chunk_moving_n} в связку")
                if chunk_stay_n:
                    parts.append(f"{chunk_stay_n} уже там")
                parts.append(f"итого {len(chunk_items)} шт.")
                summary = " · ".join(parts)
            if len(item_chunks) > 1:
                summary += f" · часть {part_idx + 1}/{len(item_chunks)}"

            chunk_apply = _merged_apply_candidate(b, chunk_moving, op_map, marketplace=mp)
            if not chunk_apply or not chunk_moving:
                continue

            bundle_id = b["bundle_id"] if len(item_chunks) == 1 else f"{b['bundle_id']}-p{part_idx + 1}"
            bundle_label = b["bundle_label"]
            if len(item_chunks) > 1:
                bundle_label = f"{bundle_label} ({part_idx + 1}/{len(item_chunks)})"[:120]

            out.append(
                {
                    "bundle_id": bundle_id,
                    "bundle_label": bundle_label,
                    "category_label": b["category_label"],
                    "is_new_bundle": is_new,
                    "target_group_id": b["target_group_id"],
                    "target_model_name": b["target_model_name"],
                    "suggested_target_imt": b.get("suggested_target_imt"),
                    "suggested_model_name": b.get("suggested_model_name"),
                    "item_count": len(chunk_items),
                    "moving_count": chunk_moving_n,
                    "stay_count": chunk_stay_n,
                    "summary": summary,
                    "items": chunk_items,
                    "operations": b["operations"],
                    "apply_candidate": chunk_apply,
                    "ai": True,
                }
            )

    out.sort(
        key=lambda x: (
            str(x.get("category_label") or "").lower(),
            str(x.get("bundle_label") or "").lower(),
        )
    )
    return out


def deterministic_pack_suggestions(
    rows: List[dict],
    *,
    marketplace: str,
    groups: Optional[List[dict]] = None,
    split_oversized: bool = True,
    start_seq: int = 0,
) -> Tuple[List[dict], set[str], int]:
    """Жёсткие связки по одинаковому базовому названию (фасовки 1/2/3 шт)."""
    mp = (marketplace or "").strip().lower()
    by_cat: Dict[str, List[dict]] = {}
    for r in rows:
        art = _row_article(r, mp)
        if not art:
            continue
        by_cat.setdefault(_row_category_key(r, mp), []).append(r)

    out: List[dict] = []
    used: set[str] = set()
    seq = start_seq

    for cat_rows in by_cat.values():
        by_base: Dict[str, List[dict]] = {}
        for r in cat_rows:
            base = _title_base_key(r.get("title") or "")
            if len(base) < 6:
                continue
            by_base.setdefault(base, []).append(r)

        for _base, items in by_base.items():
            if len(items) < 2:
                continue
            arts = [_row_article(x, mp) for x in items]
            if len(set(arts)) < 2:
                continue
            has_pack_hint = any(_PACK_RE.search(x.get("title") or "") for x in items)
            if not has_pack_hint:
                titles = [x.get("title") or "" for x in items]
                if not all(_titles_related_enough(_title_base_key(titles[0]), _title_base_key(t)) for t in titles[1:]):
                    continue
            err = _validate_ai_cluster_items(items, mp)
            if err:
                continue
            chunks = _split_items_to_bundles(items) if split_oversized else [items]
            for chunk in chunks:
                if len(chunk) < 2:
                    continue
                seq += 1
                gids = [
                    str(x.get("link_group_id") or x.get("imt_id") or x.get("model_name") or "")
                    for x in chunk
                ]
                gids = [g for g in gids if g]
                if gids and len(set(gids)) == 1 and all(x.get("linked") for x in chunk):
                    continue
                cl: Dict[str, Any] = {
                    "kind": "new_link",
                    "reason": f"Один продукт, разная фасовка · {_candidate_label(chunk, marketplace=mp)}",
                    "confidence": "high",
                    "normalized_product_name": _base,
                }
                if gids:
                    best_gid = Counter(gids).most_common(1)[0][0]
                    cl["target_group_id"] = best_gid
                    cl["kind"] = "merge_groups" if len(set(gids)) > 1 else "relocate"
                    cl["reason"] = f"Собрать фасовки 1/2/3 в одной связке · imtID {best_gid}"
                cand = _build_ai_candidate_entry(
                    cl, chunk, groups or [], marketplace=mp, seq=seq, source="pack",
                )
                out.append(cand)
                for a in [_row_article(x, mp) for x in chunk]:
                    if a:
                        used.add(a)
    return out, used, seq


async def _ai_suggest_batch(
    cat_rows: List[dict],
    cat_groups: List[dict],
    *,
    marketplace: str,
    openai_key: str,
    client: Any = None,
    category_label: str = "",
    system_prompt: Optional[str] = None,
) -> List[dict]:
    from app.core.openai_client import OpenAIClient

    mp = (marketplace or "").strip().lower()
    by_art = {_row_article(r, mp): r for r in cat_rows if _row_article(r, mp)}
    if len(by_art) < 2:
        return []
    products = [_compact_product_for_ai(r, mp) for r in cat_rows if _row_article(r, mp)]
    linked_groups = [_compact_group_for_ai(g, mp) for g in cat_groups if (g.get("items") or [])][:20]
    user = json.dumps(
        {
            "marketplace": mp,
            "category": category_label or _row_category_label(cat_rows[0], mp) if cat_rows else "",
            "product_count": len(products),
            "existing_linked_groups": linked_groups,
            "products": products,
        },
        ensure_ascii=False,
    )
    ai_client = client or OpenAIClient(openai_key)
    raw = await ai_client.generate(resolve_ai_system_prompt(mp, system_prompt), user)
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        log.warning("AI card links: invalid JSON: %s", text[:200])
        return []
    if not isinstance(parsed, list):
        return []
    out: List[dict] = []
    for cl in parsed:
        if not isinstance(cl, dict):
            continue
        ids = [str(x).strip() for x in (cl.get("article_ids") or cl.get("ids") or []) if str(x).strip()]
        items = [by_art[x] for x in ids if x in by_art]
        if not items:
            continue
        kind = str(cl.get("kind") or "new_link").strip()
        if kind == "attach" and len(items) != 1:
            continue
        if kind in ("new_link", "merge_groups") and len(items) < 2:
            continue
        if kind == "relocate" and len(items) < 1:
            continue
        if str(cl.get("confidence") or "").lower() == "low":
            continue
        err = _validate_ai_cluster_items(items, mp)
        if err:
            continue
        out.append({"cl": cl, "items": items})
    return out


async def ai_suggest_card_links(
    rows: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
    openai_key: str,
    include_linked: bool = True,
    scope: str = "all",
    batch_size: int = 60,
    max_products: int = 400,
    max_ai_batches: int = 12,
    deterministic_packs: bool = True,
    split_oversized: bool = True,
    system_prompt: Optional[str] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    cancel: Any = None,
) -> Tuple[List[dict], List[dict], dict]:
    """
    ИИ-кластеризация карточек (включая уже связанные).
    Возвращает (сырые предложения, итоговые связки для UI, meta).
    """
    mp = (marketplace or "").strip().lower()
    meta: Dict[str, Any] = {
        "batches": 0,
        "batches_planned": 0,
        "batches_run": 0,
        "batches_skipped": 0,
        "categories_total": 0,
        "ai_mode": "category",
        "ai_clusters": 0,
        "pack_clusters": 0,
        "total_catalog": len(rows),
        "pool_requested": len(_filter_rows_for_ai_scope(rows, scope if include_linked else "unlinked", mp)),
    }
    pool = _filter_rows_for_ai_scope(rows, scope if include_linked else "unlinked", mp)
    cap = int(max_products or 0)
    if cap > 0:
        pool = pool[: min(cap, 50000)]
    meta["analyzed"] = len(pool)
    meta["truncated"] = meta["pool_requested"] > len(pool)
    if len(pool) < 2 and not deterministic_packs:
        return [], [], meta

    out: List[dict] = []
    used_articles: set[str] = set()
    seq = 0

    if deterministic_packs:
        if progress_cb:
            progress_cb(0, 1, "Авто-фасовки 1/2/3 шт…")
        pack_rows = pool if include_linked else [r for r in pool if not r.get("linked")]
        packs, used_articles, seq = deterministic_pack_suggestions(
            pack_rows,
            marketplace=mp,
            groups=groups,
            split_oversized=split_oversized,
            start_seq=seq,
        )
        out.extend(packs)
        meta["pack_clusters"] = len(packs)

    ai_pool = [r for r in pool if _row_article(r, mp) not in used_articles]
    if len(ai_pool) >= 2:
        by_cat: Dict[str, List[dict]] = {}
        groups_by_cat: Dict[str, List[dict]] = {}
        for r in ai_pool:
            ck = _row_category_key(r, mp)
            by_cat.setdefault(ck, []).append(r)
        for g in groups:
            items = g.get("items") or []
            if not items:
                continue
            ck = _row_category_key(items[0], mp)
            groups_by_cat.setdefault(ck, []).append(g)

        bs = max(10, min(int(batch_size), 150))
        seen_clusters: set[frozenset] = set()
        batch_jobs = _build_ai_category_batch_jobs(
            by_cat,
            groups_by_cat,
            marketplace=mp,
            max_per_request=bs,
        )
        meta["categories_total"] = len(by_cat)
        meta["batches_planned"] = len(batch_jobs)
        batch_cap = int(max_ai_batches or 0)
        if batch_cap > 0 and len(batch_jobs) > batch_cap:
            batch_jobs = batch_jobs[:batch_cap]
            meta["batches_skipped"] = meta["batches_planned"] - batch_cap
        total_batches = len(batch_jobs)
        total_steps = max(total_batches, 1) + 2
        if progress_cb:
            progress_cb(1, total_steps, "Подготовка ИИ…")

        from app.core.openai_client import OpenAIClient

        ai_client = OpenAIClient(openai_key)
        sem = asyncio.Semaphore(AI_SUGGEST_PARALLEL)
        done_batches = 0
        batch_lock = asyncio.Lock()

        def _merge_batch_results(batch_results: List[dict]) -> None:
            nonlocal seq
            for br in batch_results:
                cl = br["cl"]
                items = br["items"]
                if split_oversized and len(items) > MAX_LINK_ITEMS:
                    chunks = _split_items_to_bundles(items)
                else:
                    chunks = [items]
                for chunk in chunks:
                    if len(chunk) < 2 and cl.get("kind") in ("new_link", "merge_groups"):
                        continue
                    key = frozenset(_row_article(x, mp) for x in chunk)
                    if key in seen_clusters:
                        continue
                    seen_clusters.add(key)
                    err = _validate_ai_cluster_items(chunk, mp)
                    if err:
                        continue
                    seq += 1
                    out.append(
                        _build_ai_candidate_entry(cl, chunk, groups, marketplace=mp, seq=seq, source="ai")
                    )
                    meta["ai_clusters"] += 1

        async def _run_batch(ck: str, batch: List[dict], cat_groups: List[dict], cat_label: str) -> None:
            nonlocal done_batches
            if cancel is not None and getattr(cancel, "cancelled", False):
                return
            if cancel is not None and hasattr(cancel, "raise_if_cancelled"):
                cancel.raise_if_cancelled()
            async with sem:
                if cancel is not None and hasattr(cancel, "raise_if_cancelled"):
                    cancel.raise_if_cancelled()
                try:
                    batch_results = await _ai_suggest_batch(
                        batch,
                        cat_groups,
                        marketplace=mp,
                        openai_key=openai_key,
                        client=ai_client,
                        category_label=cat_label,
                        system_prompt=system_prompt,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("AI batch failed (%s): %s", cat_label, e)
                    batch_results = []
            async with batch_lock:
                done_batches += 1
                meta["batches"] += 1
                meta["batches_run"] += 1
                step = 1 + done_batches
                pct = int(round(step / total_steps * 100))
                if progress_cb:
                    progress_cb(
                        step,
                        total_steps,
                        f"ИИ: {done_batches}/{total_batches} · {pct}% · {cat_label[:48]} · {len(batch)} тов.",
                    )
                _merge_batch_results(batch_results)

        if total_batches:
            if progress_cb:
                progress_cb(1, total_steps, f"ИИ: 0/{total_batches} категорий · 0%")
            tasks = [
                asyncio.create_task(_run_batch(ck, batch, cat_groups, cat_label))
                for ck, batch, cat_groups, cat_label in batch_jobs
            ]
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                meta["cancelled"] = True
                raise
            except Exception as e:
                log.warning("AI parallel batches failed: %s", e)

        if progress_cb:
            progress_cb(total_steps - 1, total_steps, "Сборка итоговых связок…")

    meta["suggestions_raw"] = len(out)
    out, pack_merge_n = _merge_ai_suggestions_by_pack_key(
        out, groups, marketplace=mp, start_seq=seq,
    )
    meta["pack_merges"] = pack_merge_n
    out, use_merge_n = _merge_ai_suggestions_by_use_bucket(
        out, groups, marketplace=mp, start_seq=seq + pack_merge_n,
    )
    meta["use_merges"] = use_merge_n
    bundles = consolidate_ai_bundle_previews(out, groups, marketplace=mp)
    meta["bundles"] = len(bundles)
    covered_arts = set()
    for b in bundles:
        for it in b.get("items") or []:
            art = _row_article(it, mp)
            if art:
                covered_arts.add(art)
    meta["products_in_bundles"] = len(covered_arts)
    meta["uncovered"] = max(0, meta["analyzed"] - len(covered_arts))
    return out, bundles, meta


def _enrich_wb_row(row: dict, subject_map: Dict[int, dict]) -> None:
    sid = int(row.get("subject_id") or 0)
    if sid and sid in subject_map:
        meta = subject_map[sid]
        row["parent_id"] = int(meta.get("parent_id") or 0)
        row["parent_name"] = str(meta.get("parent_name") or "")
        if not row.get("subject_name"):
            row["subject_name"] = str(meta.get("subject_name") or "")


async def _wb_subject_map(api_key: str) -> Dict[int, dict]:
    global _WB_SUBJECT_MAP_CACHE
    key = (api_key or "").strip()
    if not key:
        return {}
    now = time.time()
    cached = _WB_SUBJECT_MAP_CACHE.get(key)
    if cached and now - cached[0] < 600:
        return cached[1]
    client = WbContentClient(api_key)
    try:
        subjects = await client.list_subjects()
    except Exception as e:
        log.warning("WB subject map failed: %s", e)
        return {}
    out: Dict[int, dict] = {}
    for it in subjects:
        try:
            sid = int(it.get("subjectID") or it.get("subjectId") or 0)
        except (TypeError, ValueError):
            continue
        if not sid:
            continue
        try:
            pid = int(it.get("parentID") or it.get("parentId") or 0)
        except (TypeError, ValueError):
            pid = 0
        out[sid] = {
            "parent_id": pid,
            "parent_name": str(it.get("parentName") or "").strip(),
            "subject_name": str(it.get("subjectName") or "").strip(),
        }
    _WB_SUBJECT_MAP_CACHE[key] = (now, out)
    return out


_WB_SUBJECT_MAP_CACHE: Dict[str, Tuple[float, Dict[int, dict]]] = {}


def wb_merge_error_message(body: str) -> str:
    """Человекочитаемая ошибка WB moveNm."""
    text = (body or "").strip()
    low = text.lower()
    if "too many requests" in low or '"status": 429' in low or "limited by global limiter" in low:
        return (
            "Wildberries: слишком много запросов (лимит API). "
            "Подождите 1–2 минуты, не нажимайте «Связать» подряд — затем обновите каталог."
        )
    try:
        data = json.loads(text)
        err = str(data.get("errorText") or data.get("message") or data.get("detail") or "").strip()
        if not err and isinstance(data.get("title"), str):
            err = str(data.get("title") or "").strip()
    except json.JSONDecodeError:
        err = text
    low = err.lower()
    if "duplicate" in low:
        return (
            "WB отклонил повторный запрос. Подождите 1–2 минуты и обновите каталог — "
            "склейка могла уже уйти в обработку."
        )
    if "parent categor" in low:
        return (
            "WB не позволяет объединить товары из разных родительских категорий. "
            "Выберите карточки с одним предметом (subjectID) — используйте «Выбрать группу» на вкладке «Кандидаты»."
        )
    if "subject" in low or "предмет" in low:
        return f"WB: {err}"
    if err:
        return f"WB: {err}"
    return f"WB API: {text[:300]}"


async def _wb_fetch_row_by_nm(
    client: WbContentClient,
    *,
    nm_id: int,
    vendor_code: str = "",
    subject_map: Optional[Dict[int, dict]] = None,
) -> Optional[dict]:
    """Подгрузить одну карточку: сначала по артикулу (textSearch), иначе из каталога не найдётся."""
    vc = (vendor_code or "").strip()
    if vc:
        try:
            page = await client.list_cards(limit=20, text_search=vc)
            for card in page.get("cards") or []:
                if not isinstance(card, dict):
                    continue
                row = normalize_wb_card(card)
                if int(row.get("nm_id") or 0) == int(nm_id):
                    if subject_map:
                        _enrich_wb_row(row, subject_map)
                    return row
        except Exception:
            log.warning("WB card fetch by vendor_code=%s failed", vc[:40])
    return None


async def _wb_rows_for_merge(
    api_key: str,
    *,
    nm_ids: List[int],
    target_imt: int,
    rows: Optional[List[dict]] = None,
) -> List[dict]:
    """Подгружает карточки по nmID / imtID для проверки перед merge."""
    by_nm: Dict[int, dict] = {}
    vendor_by_nm: Dict[int, str] = {}
    for r in rows or []:
        try:
            nid = int(r.get("nm_id") or 0)
        except (TypeError, ValueError):
            nid = 0
        if nid:
            by_nm[nid] = dict(r)
            vc = str(r.get("vendor_code") or "").strip()
            if vc:
                vendor_by_nm[nid] = vc

    need_nm = [int(x) for x in nm_ids if int(x) not in by_nm]
    need_imt = int(target_imt or 0)
    has_imt = any(int(r.get("imt_id") or 0) == need_imt for r in by_nm.values())

    if not need_nm and (not need_imt or has_imt):
        return list(by_nm.values())

    client = WbContentClient(api_key)
    subject_map = await _wb_subject_map(api_key)

    for nid in need_nm:
        row = await _wb_fetch_row_by_nm(
            client,
            nm_id=int(nid),
            vendor_code=vendor_by_nm.get(int(nid), ""),
            subject_map=subject_map,
        )
        if row:
            by_nm[int(nid)] = row

    if need_imt and not any(int(r.get("imt_id") or 0) == need_imt for r in by_nm.values()):
        for r in rows or []:
            if int(r.get("imt_id") or 0) == need_imt:
                nid = int(r.get("nm_id") or 0)
                if nid:
                    by_nm[nid] = dict(r)
                    break

    return list(by_nm.values())


def validate_wb_merge_rows(
    rows: List[dict],
    *,
    target_imt: int,
    nm_ids: List[int],
) -> None:
    """Проверка до moveNm: один subjectID и одна родительская категория."""
    by_nm: Dict[int, dict] = {}
    for r in rows:
        try:
            nid = int(r.get("nm_id") or 0)
        except (TypeError, ValueError):
            continue
        if nid:
            by_nm[nid] = r

    ids = [int(x) for x in nm_ids if x is not None]
    if not ids:
        raise ValueError("nm_ids пуст")

    selected: List[dict] = []
    for nid in ids:
        row = by_nm.get(int(nid))
        if not row:
            raise ValueError(
                f"Карточка nmID {nid} не найдена — обновите каталог и выберите товары одной группы"
            )
        selected.append(row)

    subjects: Dict[int, str] = {}
    parents: Dict[int, str] = {}
    for row in selected:
        sid = int(row.get("subject_id") or 0)
        if not sid:
            raise ValueError(
                f"У карточки {row.get('vendor_code') or row.get('nm_id')} не определён предмет (subjectID)"
            )
        subjects[sid] = str(row.get("subject_name") or f"subjectID {sid}")
        pid = int(row.get("parent_id") or 0)
        if pid:
            parents[pid] = str(row.get("parent_name") or f"parentID {pid}")

    if len(subjects) > 1:
        labels = ", ".join(subjects.values())
        raise ValueError(
            f"Разные предметы WB: {labels}. Объединяйте только карточки одного subjectID."
        )
    if len(parents) > 1:
        labels = ", ".join(parents.values())
        raise ValueError(
            f"Разные родительские категории WB: {labels}. Выберите товары из одной категории."
        )

    want_subject = next(iter(subjects.keys()))
    want_parent = next(iter(parents.keys())) if parents else 0

    target_row = next((r for r in rows if int(r.get("imt_id") or 0) == int(target_imt)), None)
    if target_row:
        ts = int(target_row.get("subject_id") or 0)
        if ts and ts != want_subject:
            raise ValueError(
                f"target imtID {target_imt} относится к предмету «{target_row.get('subject_name')}», "
                f"а выбранные карточки — к «{subjects.get(want_subject)}»"
            )
        tp = int(target_row.get("parent_id") or 0)
        if want_parent and tp and tp != want_parent:
            raise ValueError(
                f"target imtID {target_imt} в другой родительской категории («{target_row.get('parent_name')}»)"
            )


async def fetch_wb_catalog(
    api_key: str,
    *,
    vendor_codes: Optional[List[str]] = None,
    text_search: Optional[str] = None,
    max_pages: int = 100,
    articles_only: bool = False,
) -> Tuple[List[dict], dict]:
    client = WbContentClient(api_key)
    subject_map = await _wb_subject_map(api_key)
    catalog_meta: dict = {"scope": "articles_only" if articles_only else "full"}
    codes = [str(v).strip() for v in (vendor_codes or []) if str(v).strip()]
    if articles_only and not codes:
        raise ValueError("articles_only: список артикулов пуст")
    raw = await client.list_cards_all(
        max_pages=max_pages,
        text_search=None if articles_only else text_search,
        vendor_codes=codes or None,
        meta_out=catalog_meta,
    )
    rows: List[dict] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        row = normalize_wb_card(c)
        _enrich_wb_row(row, subject_map)
        rows.append(row)
    if codes:
        rows, missing = filter_rows_by_articles(rows, codes, marketplace="wb")
        if articles_only:
            _apply_articles_scope_meta(catalog_meta, articles=codes, rows=rows, missing=missing)
        elif missing:
            catalog_meta["missing_articles"] = missing[:50]
            catalog_meta["missing_count"] = len(missing)
    catalog_meta["count"] = len(rows)
    return rows, catalog_meta


def build_catalog_payload(
    rows: List[dict],
    catalog_meta: dict,
    *,
    store_id: int,
    marketplace: str,
    articles_only: bool = False,
    suggestions: str = "none",
) -> dict:
    """Каталог + опциональные предложения связок (suggestions: none | review | all)."""
    mp = (marketplace or "wb").strip().lower()
    if mp == "wb":
        groups = group_wb_rows(rows)
    else:
        groups = group_ozon_rows(rows, articles_only=articles_only)
    apply_link_status(rows, groups)
    rows = sort_catalog_rows(rows, marketplace=mp)
    mode = (suggestions or "none").strip().lower()
    candidates: List[dict] = []
    attach: List[dict] = []
    review: List[dict] = []
    combine: List[dict] = []
    if mode in ("all", "full"):
        candidates = suggest_link_candidates(rows, marketplace=mp)
        attach = group_attach_suggestions(
            suggest_attach_to_groups(rows, groups, marketplace=mp),
            marketplace=mp,
        )
        review = suggest_review_linked_groups(groups, marketplace=mp) if not articles_only else []
        combine = suggest_combine_candidates(candidates, marketplace=mp)
    elif mode == "review" and not articles_only:
        review = suggest_review_linked_groups(groups, marketplace=mp)
    linked_groups = sum(1 for g in groups if g.get("linked"))
    unlinked_count = sum(1 for r in rows if not r.get("linked"))
    return {
        "store_id": store_id,
        "marketplace": mp,
        "count": len(rows),
        "unlinked_count": unlinked_count,
        "linked_groups": linked_groups,
        "max_link_items": MAX_LINK_ITEMS,
        "items": rows,
        "groups": groups,
        "candidates": candidates,
        "attach_suggestions": attach,
        "review_suggestions": review,
        "combine_suggestions": combine,
        "catalog_meta": catalog_meta,
    }


def build_wb_catalog_payload(
    rows: List[dict],
    catalog_meta: dict,
    *,
    store_id: int,
    articles_only: bool = False,
    suggestions: str = "none",
) -> dict:
    return build_catalog_payload(
        rows,
        catalog_meta,
        store_id=store_id,
        marketplace="wb",
        articles_only=articles_only,
        suggestions=suggestions,
    )


def build_ozon_catalog_payload(
    rows: List[dict],
    catalog_meta: dict,
    *,
    store_id: int,
    articles_only: bool = False,
    suggestions: str = "none",
) -> dict:
    return build_catalog_payload(
        rows,
        catalog_meta,
        store_id=store_id,
        marketplace="ozon",
        articles_only=articles_only,
        suggestions=suggestions,
    )


def _ozon_attrs_page_cursor(page: dict) -> Tuple[str, bool]:
    if not isinstance(page, dict):
        return "", False
    res = page.get("result")
    block = res if isinstance(res, dict) else page
    last_id = str(block.get("last_id") or "").strip()
    has_next = block.get("has_next")
    if has_next is None:
        has_next = bool(last_id)
    return last_id, bool(has_next)


async def _fetch_ozon_attributes_by_offers(
    client: OzonClient,
    offer_ids: List[str],
) -> Dict[str, List[dict]]:
    """Все страницы /v4/product/info/attributes для списка offer_id."""
    out: Dict[str, List[dict]] = {}
    oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    for i in range(0, len(oids), 100):
        batch = oids[i : i + 100]
        last_id = ""
        for _ in range(50):
            page = await client.product_info_attributes(offer_ids=batch, limit=1000, last_id=last_id)
            out.update(_parse_ozon_attributes_by_offer(page))
            next_id, has_next = _ozon_attrs_page_cursor(page)
            if not has_next or not next_id or next_id == last_id:
                break
            last_id = next_id
    return out


async def _fetch_ozon_info_by_offers(
    client: OzonClient,
    oids: List[str],
    listed_by_oid: Dict[str, dict],
) -> Tuple[Dict[str, dict], int]:
    """Детали товаров: info/list + повтор по product_id + fallback из list."""
    info_by_oid: Dict[str, dict] = {}
    pid_by_oid: Dict[str, int] = {}
    for oid, it in listed_by_oid.items():
        try:
            pid = int(it.get("product_id") or it.get("id") or 0)
        except (TypeError, ValueError):
            pid = 0
        if pid:
            pid_by_oid[oid] = pid

    for i in range(0, len(oids), 1000):
        batch = oids[i : i + 1000]
        for info in await client.product_info_list(offer_ids=batch):
            if not isinstance(info, dict):
                continue
            oid = str(info.get("offer_id") or info.get("offerId") or "").strip()
            if oid:
                info_by_oid[oid] = info

    missing = [oid for oid in oids if oid not in info_by_oid]
    if missing:
        retry_pids = [pid_by_oid[oid] for oid in missing if pid_by_oid.get(oid)]
        for i in range(0, len(retry_pids), 1000):
            for info in await client.product_info_list(product_ids=retry_pids[i : i + 1000]):
                if not isinstance(info, dict):
                    continue
                oid = str(info.get("offer_id") or info.get("offerId") or "").strip()
                if oid:
                    info_by_oid[oid] = info

    recovered = 0
    for oid in oids:
        if oid in info_by_oid:
            continue
        stub = listed_by_oid.get(oid)
        if stub:
            info_by_oid[oid] = stub
            recovered += 1
    return info_by_oid, recovered


async def fetch_ozon_catalog(
    client_id: str,
    api_key: str,
    *,
    offer_ids: Optional[List[str]] = None,
    max_pages: int = 30,
    meta_out: Optional[dict] = None,
    articles_only: bool = False,
) -> List[dict]:
    client = OzonClient(client_id, api_key, timeout_s=60.0)
    list_meta: dict = {}
    codes = [str(x).strip() for x in (offer_ids or []) if str(x).strip()]
    if articles_only and not codes:
        raise ValueError("articles_only: список артикулов пуст")
    listed = await client.list_products_all(
        max_pages=max_pages,
        offer_ids=codes or None,
        meta_out=list_meta,
    )
    if codes:
        oids = codes
    else:
        oids = []
        for it in listed:
            oid = str(it.get("offer_id") or "").strip()
            if oid:
                oids.append(oid)

    if not oids:
        return []

    listed_by_oid: Dict[str, dict] = {}
    for it in listed:
        if not isinstance(it, dict):
            continue
        oid = str(it.get("offer_id") or it.get("offerId") or "").strip()
        if oid:
            listed_by_oid[oid] = it

    info_by_oid, recovered_from_list = await _fetch_ozon_info_by_offers(client, oids, listed_by_oid)
    attrs_by_offer = await _fetch_ozon_attributes_by_offers(client, oids)

    skus: List[int] = []
    sku_set: set[int] = set()
    for info in info_by_oid.values():
        try:
            s = int(info.get("sku") or 0)
        except (TypeError, ValueError):
            s = 0
        if s:
            skus.append(s)
            sku_set.add(s)

    related_by_sku: Dict[int, List[int]] = {}
    if not articles_only:
        for i in range(0, len(skus), 200):
            batch = skus[i : i + 200]
            rel_items = await client.product_related_sku_get(batch)
            for ri in rel_items:
                try:
                    sku = int(ri.get("sku") or 0)
                except (TypeError, ValueError):
                    continue
                rel = ri.get("related_sku") or ri.get("related_skus") or ri.get("skus") or []
                parsed: List[int] = []
                for x in rel if isinstance(rel, list) else []:
                    try:
                        parsed.append(int(x))
                    except (TypeError, ValueError):
                        pass
                if sku:
                    related_by_sku[sku] = sorted(set(parsed))
    else:
        for sku in sku_set:
            related_by_sku[sku] = [sku]

    rows: List[dict] = []
    dropped = 0
    for oid in oids:
        info = info_by_oid.get(oid)
        if not info:
            dropped += 1
            continue
        try:
            sku = int(info.get("sku") or 0)
        except (TypeError, ValueError):
            sku = 0
        rel = related_by_sku.get(sku)
        if articles_only and sku:
            rel = [x for x in (rel or []) if x in sku_set] or [sku]
        rows.append(
            normalize_ozon_product(
                info,
                attrs=attrs_by_offer.get(oid),
                related_skus=rel,
            )
        )
    _enrich_ozon_model_from_peers(rows)
    if codes:
        rows, missing = filter_rows_by_articles(rows, codes, marketplace="ozon")
    else:
        missing = []
    if meta_out is not None:
        meta_out.update(list_meta)
        meta_out["scope"] = "articles_only" if articles_only else "full"
        meta_out["listed_count"] = len(oids)
        meta_out["info_count"] = len(info_by_oid)
        meta_out["recovered_from_list"] = recovered_from_list
        meta_out["dropped_count"] = dropped
        meta_out["count"] = len(rows)
        if articles_only and codes:
            _apply_articles_scope_meta(meta_out, articles=codes, rows=rows, missing=missing)
        elif missing:
            meta_out["missing_articles"] = missing[:50]
            meta_out["missing_count"] = len(missing)
    return rows


async def wb_merge_cards(
    api_key: str,
    *,
    target_imt: int,
    nm_ids: List[int],
    catalog_rows: Optional[List[dict]] = None,
) -> dict:
    target = int(target_imt)
    uniq: List[int] = []
    seen: set[int] = set()
    for x in nm_ids:
        nid = int(x)
        if nid in seen:
            continue
        seen.add(nid)
        uniq.append(nid)
    if not uniq:
        raise ValueError("nm_ids пуст")

    rows = await _wb_rows_for_merge(
        api_key,
        nm_ids=uniq,
        target_imt=target,
        rows=catalog_rows,
    )
    by_nm = {int(r.get("nm_id") or 0): r for r in rows if int(r.get("nm_id") or 0)}
    to_move = [nid for nid in uniq if int(by_nm.get(nid, {}).get("imt_id") or 0) != target]
    if not to_move:
        raise ValueError("Выбранные карточки уже в этой связке (imtID) — обновите каталог")

    validate_wb_merge_rows(rows, target_imt=target, nm_ids=to_move)
    validate_wb_link_capacity(rows, target_imt=target, nm_ids=to_move)
    client = WbContentClient(api_key)
    last: Optional[dict] = None
    for i in range(0, len(to_move), 30):
        batch = to_move[i : i + 30]
        last = await client.merge_cards(target_imt=target, nm_ids=batch)
        if i + 30 < len(to_move):
            await asyncio.sleep(2.0)
    return last or {}


async def wb_disconnect_cards(api_key: str, *, nm_ids: List[int]) -> dict:
    client = WbContentClient(api_key)
    return await client.disconnect_cards(nm_ids)


def _unique_ozon_model_name(*, offer_id: str, title: str = "") -> str:
    """Уникальное «Название модели» для разъединения на Ozon."""
    oid = (offer_id or "").strip()
    if not oid:
        raise ValueError("offer_id пуст")
    name = (title or "").strip()
    if name:
        base = name[:100].rstrip()
        return f"{base} ({oid})"[:250]
    return oid


async def ozon_unlink_cards(
    client_id: str,
    api_key: str,
    *,
    offer_ids: List[str],
    titles_by_offer: Optional[Dict[str, str]] = None,
) -> dict:
    """Разъединить на Ozon: у каждого offer_id своё «Название модели»."""
    oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    if not oids:
        raise ValueError("offer_ids пуст")
    titles = titles_by_offer or {}
    client = OzonClient(client_id, api_key, timeout_s=60.0)
    items = [
        {
            "offer_id": oid,
            "attributes": [
                {
                    "id": OZON_MODEL_ATTR_ID,
                    "values": [
                        {
                            "dictionary_value_id": 0,
                            "value": _unique_ozon_model_name(
                                offer_id=oid,
                                title=titles.get(oid, ""),
                            ),
                        }
                    ],
                }
            ],
        }
        for oid in oids[:100]
    ]
    result = await client.update_product_attributes(items)
    models = {
        oid: _unique_ozon_model_name(offer_id=oid, title=titles.get(oid, ""))
        for oid in oids[:100]
    }
    return {"api": result, "models": models}


async def ozon_link_by_model(
    client_id: str,
    api_key: str,
    *,
    offer_ids: List[str],
    model_name: str,
    catalog_rows: Optional[List[dict]] = None,
    qty_pack: bool = False,
    validate_only: bool = False,
) -> dict:
    model = (model_name or "").strip()
    if not model:
        raise ValueError("model_name пуст")
    oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    if not oids:
        raise ValueError("offer_ids пуст")
    if catalog_rows:
        validate_ozon_link_capacity(catalog_rows, model_name=model, offer_ids=oids)
    client = OzonClient(client_id, api_key, timeout_s=60.0)
    qty_attr_id: Optional[int] = None
    if catalog_rows and len(oids) >= 2:
        picked = [r for r in catalog_rows if str(r.get("offer_id") or "").strip() in set(oids)]
        if len(picked) >= 2:
            try:
                dc = int(picked[0].get("description_category_id") or 0)
                tid = int(picked[0].get("type_id") or 0)
            except (TypeError, ValueError):
                dc, tid = 0, 0
            if dc and tid:
                aspect_ids, attr_names, brand_ids = await _ozon_category_schema(client, dc, tid)
                validate_ozon_link_rows(
                    catalog_rows,
                    oids,
                    aspect_attr_ids=aspect_ids,
                    attr_names=attr_names,
                    brand_attr_ids=brand_ids,
                    qty_pack=qty_pack,
                )
                if qty_pack:
                    qty_attr_id = _ozon_qty_pack_attr_id(attr_names)
    if validate_only:
        return {"ok": True, "validated": True}
    items = []
    for idx, oid in enumerate(oids[:100]):
        attrs: List[dict] = [
            {
                "id": OZON_MODEL_ATTR_ID,
                "values": [{"dictionary_value_id": 0, "value": model}],
            }
        ]
        if qty_pack and qty_attr_id:
            attrs.append(
                {
                    "id": qty_attr_id,
                    "values": [{"dictionary_value_id": 0, "value": str(idx + 1)}],
                }
            )
        items.append({"offer_id": oid, "attributes": attrs})
    return await client.update_product_attributes(items)
