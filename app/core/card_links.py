"""Связки карточек WB и Ozon: выгрузка каталога, проверка групп, привязка."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from app.core.ozon_client import OzonClient
from app.core.wb_content_client import WbContentClient

log = logging.getLogger("card_links")

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
    cat_key, cat_label = _ozon_category_key(info)
    group_label = model_name or (f"SKU {rel[0]}" if rel else None)
    return {
        "offer_id": offer_id,
        "sku": sku,
        "product_id": product_id,
        "title": str(info.get("name") or "").strip(),
        "photo_url": _ozon_primary_image(info),
        "model_name": model_name,
        "related_skus": rel,
        "category_key": cat_key,
        "category_label": cat_label,
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
        label = f"категория {dc_i}, тип {tid_i}"
    return key, label or key


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
            key = f"wb:{sid}"
        else:
            key = f"oz:{it.get('category_key') or '0:0'}"
        buckets.setdefault(key, []).append(it)
    return [grp for grp in buckets.values() if len(grp) >= 2]


def _wb_target_imt(items: List[dict]) -> int:
    imts = [int(x.get("imt_id") or 0) for x in items if int(x.get("imt_id") or 0)]
    return imts[0] if imts else 0


def _candidate_label(items: List[dict], *, marketplace: str) -> str:
    if marketplace == "wb":
        name = str(items[0].get("subject_name") or "").strip()
        sid = int(items[0].get("subject_id") or 0)
        return name or (f"subjectID {sid}" if sid else "категория WB")
    label = str(items[0].get("category_label") or items[0].get("category_key") or "").strip()
    return label or "категория Ozon"


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
                "category_key": items[0].get("category_key"),
                "category_label": items[0].get("category_label"),
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
    """Новые связки: похожие несвязанные карточки в одной категории."""
    pool = [r for r in rows if not r.get("linked")]
    buckets: Dict[str, List[dict]] = {}
    for r in pool:
        brand = (r.get("brand") or "").lower().strip()
        base = _title_base_key(r.get("title") or "")
        if not base:
            continue
        if marketplace == "wb":
            sid = int(r.get("subject_id") or 0)
            if not sid:
                continue
            key = f"{brand}|{base}|s{sid}"
        else:
            cat = str(r.get("category_key") or "")
            key = f"{cat}|{base}"
        buckets.setdefault(key, []).append(r)

    out: List[dict] = []
    seq = 0
    for _key, raw_items in buckets.items():
        for items in _split_items_by_category(raw_items, marketplace=marketplace):
            if marketplace == "wb":
                imts = {int(x.get("imt_id") or 0) for x in items}
                if len(imts) <= 1:
                    continue
            else:
                models = {(x.get("model_name") or "").strip() for x in items}
                if len(models) == 1 and list(models)[0]:
                    continue
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
    out.sort(key=lambda x: -x["count"])
    return out[:80]


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
    out: List[dict] = []
    seq = 0
    seen: set[str] = set()
    for g in multi:
        ref_items = g.get("items") or []
        if not ref_items:
            continue
        ref = ref_items[0]
        ref_base = _title_base_key(ref.get("title") or "")
        if not ref_base:
            continue
        for u in unlinked:
            if marketplace == "wb":
                if int(u.get("subject_id") or 0) != int(g.get("subject_id") or ref.get("subject_id") or 0):
                    continue
                uid = str(u.get("nm_id") or u.get("vendor_code") or "")
                target_imt = int(g.get("group_id") or ref.get("imt_id") or 0)
            else:
                if str(u.get("category_key") or "") != str(ref.get("category_key") or ""):
                    continue
                uid = str(u.get("offer_id") or "")
                target_imt = None
            u_base = _title_base_key(u.get("title") or "")
            if not u_base:
                continue
            if u_base != ref_base and not (u_base in ref_base or ref_base in u_base):
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
                "hint": f"Добавить в «{g.get('group_label')}»",
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
    out.sort(key=lambda x: (x.get("target_group_label") or "", x.get("items", [{}])[0].get("title") or ""))
    return out[:120]


async def ai_suggest_card_links(
    rows: List[dict],
    groups: List[dict],
    *,
    marketplace: str,
    openai_key: str,
    max_items: int = 60,
) -> List[dict]:
    """ИИ-кластеризация несвязанных карточек (дополнение к эвристике)."""
    from app.core.openai_client import OpenAIClient

    unlinked = [r for r in rows if not r.get("linked")][:max(2, min(int(max_items), 80))]
    if len(unlinked) < 2:
        return []
    linked_samples = []
    for g in groups:
        if not g.get("linked") or len(g.get("items") or []) < 2:
            continue
        linked_samples.append(
            {
                "group": g.get("group_label"),
                "category": g.get("subject_name") or g.get("category_label"),
                "titles": [(x.get("title") or "")[:80] for x in (g.get("items") or [])[:4]],
            }
        )
        if len(linked_samples) >= 15:
            break

    compact = []
    for r in unlinked:
        compact.append(
            {
                "id": r.get("vendor_code") or r.get("offer_id"),
                "mp_id": r.get("nm_id") or r.get("sku"),
                "title": (r.get("title") or "")[:120],
                "category": r.get("subject_name") or r.get("category_label") or r.get("category_key"),
            }
        )

    system = (
        "Ты помощник по связкам карточек на маркетплейсе. "
        "Верни ТОЛЬКО JSON-массив без markdown. "
        "Каждый элемент: "
        '{"cluster_id":"c1","kind":"new_link"|"attach","article_ids":["..."],'
        '"target_group_label":null|"имя существующей связки",'
        '"suggested_model_name":"для Ozon",'
        '"reason":"кратко"}. '
        "Объединяй только товары одной категории и одной линейки (разные комплекты 1/2/3 шт — да, "
        "разные товары — нет). Для attach укажи target_group_label из примеров существующих связок."
    )
    user = json.dumps(
        {
            "marketplace": marketplace,
            "existing_linked_groups": linked_samples,
            "unlinked_products": compact,
        },
        ensure_ascii=False,
    )
    client = OpenAIClient(openai_key)
    raw = await client.generate(system, user)
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

    by_id = {
        str(r.get("vendor_code") or r.get("offer_id") or ""): r
        for r in unlinked
    }
    out: List[dict] = []
    for i, cl in enumerate(parsed):
        if not isinstance(cl, dict):
            continue
        ids = [str(x).strip() for x in (cl.get("article_ids") or cl.get("ids") or []) if str(x).strip()]
        items = [by_id[x] for x in ids if x in by_id]
        if len(items) < 1:
            continue
        kind = str(cl.get("kind") or "new_link").strip()
        if kind == "attach" and len(items) != 1:
            continue
        if kind == "new_link" and len(items) < 2:
            continue
        if marketplace == "wb":
            sids = {int(x.get("subject_id") or 0) for x in items}
            if len(sids) > 1:
                continue
        else:
            cats = {str(x.get("category_key") or "") for x in items}
            if len(cats) > 1:
                continue
        seq = i + 1
        entry: Dict[str, Any] = {
            "candidate_id": f"ai-{marketplace}-{seq}",
            "kind": kind,
            "marketplace": marketplace,
            "category_label": _candidate_label(items, marketplace=marketplace),
            "count": len(items),
            "hint": str(cl.get("reason") or "Подсказка ИИ")[:200],
            "items": items,
            "ai": True,
        }
        if kind == "attach":
            entry["target_group_label"] = cl.get("target_group_label")
            entry["suggested_model_name"] = cl.get("suggested_model_name")
            if marketplace == "wb":
                for g in groups:
                    if str(g.get("group_label")) == str(cl.get("target_group_label")):
                        entry["suggested_target_imt"] = int(g.get("group_id") or 0)
                        break
        else:
            entry["suggested_model_name"] = cl.get("suggested_model_name") or _suggested_model_name(items)
            if marketplace == "wb":
                entry["suggested_target_imt"] = _wb_target_imt(items)
        out.append(entry)
    return out[:40]


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
