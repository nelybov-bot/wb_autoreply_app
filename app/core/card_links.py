"""Связки карточек WB и Ozon: выгрузка каталога, проверка групп, привязка."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from app.core.ozon_client import OzonClient
from app.core.wb_content_client import WbContentClient

OZON_MODEL_ATTR_ID = OzonClient.OZON_MODEL_ATTR_ID

_PACK_RE = re.compile(
    r"\b(\d+)\s*(шт|штук|уп|упак|pack|pcs)\b|"
    r"\bx\s*(\d+)\b|"
    r"\bкомплект\b|"
    r"\bнабор\b",
    re.IGNORECASE,
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


def _attr_value(attrs: List[dict], attr_id: int) -> str:
    for a in attrs or []:
        try:
            aid = int(a.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if aid != attr_id:
            continue
        vals = a.get("values") or []
        if vals and isinstance(vals[0], dict):
            return str(vals[0].get("value") or "").strip()
    return ""


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
    linked = bool(model_name) or len(rel) > 1
    group_label = model_name or (f"SKU {rel[0]}" if rel else None)
    return {
        "offer_id": offer_id,
        "sku": sku,
        "product_id": product_id,
        "title": str(info.get("name") or "").strip(),
        "photo_url": _ozon_primary_image(info),
        "model_name": model_name,
        "related_skus": rel,
        "linked": linked,
        "link_group_id": model_name or (str(rel[0]) if rel else None),
        "link_group_label": group_label,
    }


def _title_base_key(title: str) -> str:
    t = (title or "").lower().strip()
    t = _PACK_RE.sub(" ", t)
    t = re.sub(r"\s+", " ", t)
    return t[:120]


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
        for it in items:
            it["linked"] = linked
            it["link_group_id"] = imt
            it["link_group_label"] = f"imtID {imt}"
        groups.append(
            {
                "group_id": str(imt),
                "group_label": f"imtID {imt}",
                "marketplace": "wb",
                "linked": linked,
                "count": len(items),
                "subject_id": items[0].get("subject_id"),
                "subject_name": items[0].get("subject_name"),
                "items": items,
            }
        )
    return groups


def group_ozon_rows(rows: List[dict]) -> List[dict]:
    """Группы по model_name (9048) или кластеру related_sku."""
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
        linked = len(items) > 1 or any(len(x.get("related_skus") or []) > 1 for x in items)
        for it in items:
            it["linked"] = linked
            it["link_group_id"] = model
            it["link_group_label"] = model
        groups.append(
            {
                "group_id": model,
                "group_label": model,
                "marketplace": "ozon",
                "linked": linked,
                "count": len(items),
                "items": items,
            }
        )

    # Одиночные без model_name — по related_skus
    rel_map: Dict[Tuple[int, ...], List[dict]] = {}
    for r in orphan:
        rel = tuple(sorted(set(r.get("related_skus") or [])))
        if len(rel) > 1:
            rel_map.setdefault(rel, []).append(r)

    for rel, items in rel_map.items():
        label = f"related: {', '.join(str(x) for x in rel[:5])}"
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

    singles = [r for r in orphan if len(r.get("related_skus") or []) <= 1]
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


def suggest_link_candidates(rows: List[dict], *, marketplace: str) -> List[dict]:
    """Эвристика: похожие названия, ещё не в одной группе."""
    pool = [r for r in rows if not r.get("linked")]
    if marketplace == "wb":
        pool = [r for r in pool if int(r.get("imt_id") or 0)]
    buckets: Dict[str, List[dict]] = {}
    for r in pool:
        brand = (r.get("brand") or "").lower().strip()
        base = _title_base_key(r.get("title") or "")
        if not base:
            continue
        key = f"{brand}|{base}"
        if marketplace == "wb":
            key += f"|{r.get('subject_id')}"
        buckets.setdefault(key, []).append(r)

    out: List[dict] = []
    for key, items in buckets.items():
        if len(items) < 2:
            continue
        if marketplace == "wb":
            imts = {int(x.get("imt_id") or 0) for x in items}
            if len(imts) <= 1:
                continue
        if marketplace == "ozon":
            models = {(x.get("model_name") or "").strip() for x in items}
            if len(models) == 1 and list(models)[0]:
                continue
        out.append(
            {
                "hint_key": key,
                "marketplace": marketplace,
                "count": len(items),
                "items": items,
            }
        )
    out.sort(key=lambda x: -x["count"])
    return out[:50]


async def fetch_wb_catalog(
    api_key: str,
    *,
    vendor_codes: Optional[List[str]] = None,
    text_search: Optional[str] = None,
    max_pages: int = 20,
) -> List[dict]:
    client = WbContentClient(api_key)
    raw = await client.list_cards_all(
        max_pages=max_pages,
        text_search=text_search,
        vendor_codes=vendor_codes,
    )
    return [normalize_wb_card(c) for c in raw if isinstance(c, dict)]


async def fetch_ozon_catalog(
    client_id: str,
    api_key: str,
    *,
    offer_ids: Optional[List[str]] = None,
    max_pages: int = 15,
) -> List[dict]:
    client = OzonClient(client_id, api_key, timeout_s=60.0)
    listed = await client.list_products_all(max_pages=max_pages, offer_ids=offer_ids)
    if offer_ids:
        oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    else:
        oids = []
        for it in listed:
            oid = str(it.get("offer_id") or "").strip()
            if oid:
                oids.append(oid)

    if not oids:
        return []

    info_rows: List[dict] = []
    for i in range(0, len(oids), 100):
        batch = oids[i : i + 100]
        info_rows.extend(await client.product_info_list(offer_ids=batch))

    attrs_by_offer: Dict[str, List[dict]] = {}
    for i in range(0, len(oids), 100):
        batch = oids[i : i + 100]
        page = await client.product_info_attributes(offer_ids=batch, limit=1000)
        block = page.get("result") if isinstance(page.get("result"), dict) else page
        for it in block.get("items") or []:
            if not isinstance(it, dict):
                continue
            oid = str(it.get("offer_id") or "").strip()
            if oid:
                attrs_by_offer[oid] = it.get("attributes") or []

    skus: List[int] = []
    for it in info_rows:
        try:
            s = int(it.get("sku") or 0)
        except (TypeError, ValueError):
            s = 0
        if s:
            skus.append(s)

    related_by_sku: Dict[int, List[int]] = {}
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

    rows: List[dict] = []
    for info in info_rows:
        oid = str(info.get("offer_id") or "").strip()
        try:
            sku = int(info.get("sku") or 0)
        except (TypeError, ValueError):
            sku = 0
        rows.append(
            normalize_ozon_product(
                info,
                attrs=attrs_by_offer.get(oid),
                related_skus=related_by_sku.get(sku),
            )
        )
    return rows


async def wb_merge_cards(api_key: str, *, target_imt: int, nm_ids: List[int]) -> dict:
    client = WbContentClient(api_key)
    return await client.merge_cards(target_imt=int(target_imt), nm_ids=nm_ids)


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
    """Разъединить на Ozon: у каждого offer_id своё значение атрибута 9048."""
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
) -> dict:
    model = (model_name or "").strip()
    if not model:
        raise ValueError("model_name пуст")
    oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    if not oids:
        raise ValueError("offer_ids пуст")
    client = OzonClient(client_id, api_key, timeout_s=60.0)
    items = [
        {
            "offer_id": oid,
            "attributes": [
                {
                    "id": OZON_MODEL_ATTR_ID,
                    "values": [{"dictionary_value_id": 0, "value": model}],
                }
            ],
        }
        for oid in oids[:100]
    ]
    return await client.update_product_attributes(items)
