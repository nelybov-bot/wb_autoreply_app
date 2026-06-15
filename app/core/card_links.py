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
        items = g.get("items") or []
        sample = items[0] if items else {}
        linked_samples.append(
            {
                "group_id": g.get("group_id"),
                "group_label": g.get("group_label"),
                "category": g.get("subject_name") or g.get("category_label"),
                "subject_id": g.get("subject_id") or sample.get("subject_id"),
                "sample_title": (sample.get("title") or "")[:80],
                "sample_article": sample.get("vendor_code") or sample.get("offer_id"),
                "count": len(items),
                "titles": [(x.get("title") or "")[:80] for x in items[:4]],
            }
        )
        if len(linked_samples) >= 15:
            break

    compact = []
    for r in unlinked:
        compact.append(
            {
                "article": r.get("vendor_code") or r.get("offer_id"),
                "mp_id": r.get("nm_id") or r.get("sku"),
                "title": (r.get("title") or "")[:120],
                "category": r.get("subject_name") or r.get("category_label") or r.get("category_key"),
                "subject_id": r.get("subject_id"),
                "current_imt_id": r.get("imt_id"),
            }
        )

    mp = (marketplace or "").strip().lower()
    if mp == "wb":
        system = (
            "Ты помощник по связкам карточек Wildberries (WB). "
            "Это ТОЛЬКО WB: не упоминай Ozon, offer_id, атрибут 9048, «название модели». "
            "Связка на WB — общий imtID (целое число). "
            "Верни ТОЛЬКО JSON-массив без markdown. Каждый элемент: "
            '{"cluster_id":"c1","kind":"new_link"|"attach","article_ids":["артикул продавца"],'
            '"target_group_id":null|123456789,"reason":"кратко по-русски"}. '
            "article_ids — vendor_code из списка. "
            "Для attach: target_group_id = imtID из existing_linked_groups.group_id. "
            "Для new_link: target_group_id = null, объединяй 2+ похожих товара одного subject_id. "
            "Разные родительские категории и subject_id не смешивать."
        )
    else:
        system = (
            "Ты помощник по связкам карточек Ozon. "
            "Это ТОЛЬКО Ozon: связка — одинаковый атрибут 9048 «Название модели». "
            "Верни ТОЛЬКО JSON-массив без markdown. Каждый элемент: "
            '{"cluster_id":"c1","kind":"new_link"|"attach","article_ids":["offer_id"],'
            '"target_group_id":null|"имя модели","suggested_model_name":"строка для 9048",'
            '"reason":"кратко по-русски"}. '
            "Для attach: target_group_id или suggested_model_name из existing_linked_groups. "
            "Для new_link: предложи общее suggested_model_name. Одна категория на кластер."
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
    group_by_id = {str(g.get("group_id")): g for g in groups if g.get("group_id")}
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
        if mp == "wb":
            sids = {int(x.get("subject_id") or 0) for x in items}
            if len(sids) > 1:
                continue
        else:
            cats = {str(x.get("category_key") or "") for x in items}
            if len(cats) > 1:
                continue
        seq = i + 1
        entry: Dict[str, Any] = {
            "candidate_id": f"ai-{mp}-{seq}",
            "kind": kind,
            "marketplace": mp,
            "category_label": _candidate_label(items, marketplace=mp),
            "count": len(items),
            "hint": str(cl.get("reason") or "Подсказка ИИ")[:200],
            "items": items,
            "ai": True,
        }
        if mp == "wb":
            tgt_raw = cl.get("target_group_id") or cl.get("target_imt") or cl.get("target_group_label")
            tgt_imt = 0
            if tgt_raw is not None:
                try:
                    tgt_imt = int(tgt_raw)
                except (TypeError, ValueError):
                    for g in groups:
                        if str(g.get("group_label")) == str(tgt_raw):
                            tgt_imt = int(g.get("group_id") or 0)
                            break
            if kind == "attach" and tgt_imt:
                gref = group_by_id.get(str(tgt_imt)) or {}
                entry["target_group_id"] = str(tgt_imt)
                entry["target_group_label"] = f"imtID {tgt_imt}"
                entry["suggested_target_imt"] = tgt_imt
                entry["sample_items"] = (gref.get("items") or [])[:3]
            else:
                entry["suggested_target_imt"] = _wb_target_imt(items)
        else:
            model = str(cl.get("suggested_model_name") or cl.get("target_group_id") or "").strip()
            if kind == "attach":
                entry["target_group_label"] = str(cl.get("target_group_id") or model)
                entry["suggested_model_name"] = model or str(cl.get("target_group_id") or "")
            else:
                entry["suggested_model_name"] = model or _suggested_model_name(items)
        out.append(entry)
    return out[:40]


def _enrich_wb_row(row: dict, subject_map: Dict[int, dict]) -> None:
    sid = int(row.get("subject_id") or 0)
    if sid and sid in subject_map:
        meta = subject_map[sid]
        row["parent_id"] = int(meta.get("parent_id") or 0)
        row["parent_name"] = str(meta.get("parent_name") or "")
        if not row.get("subject_name"):
            row["subject_name"] = str(meta.get("subject_name") or "")


async def _wb_subject_map(api_key: str) -> Dict[int, dict]:
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
    return out


def wb_merge_error_message(body: str) -> str:
    """Человекочитаемая ошибка WB moveNm."""
    text = (body or "").strip()
    try:
        data = json.loads(text)
        err = str(data.get("errorText") or data.get("message") or "").strip()
    except json.JSONDecodeError:
        err = text
    low = err.lower()
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


async def _wb_rows_for_merge(
    api_key: str,
    *,
    nm_ids: List[int],
    target_imt: int,
    rows: Optional[List[dict]] = None,
) -> List[dict]:
    """Подгружает карточки по nmID / imtID для проверки перед merge."""
    by_nm: Dict[int, dict] = {}
    for r in rows or []:
        try:
            nid = int(r.get("nm_id") or 0)
        except (TypeError, ValueError):
            nid = 0
        if nid:
            by_nm[nid] = r

    need_nm = [int(x) for x in nm_ids if int(x) not in by_nm]
    need_imt = int(target_imt or 0)
    has_imt = any(int(r.get("imt_id") or 0) == need_imt for r in by_nm.values())

    if not need_nm and (not need_imt or has_imt):
        return list(by_nm.values())

    client = WbContentClient(api_key)
    subject_map = await _wb_subject_map(api_key)

    for nid in need_nm:
        try:
            page = await client.list_cards(limit=20, nm_ids=[int(nid)])
        except Exception:
            continue
        for card in page.get("cards") or []:
            if not isinstance(card, dict):
                continue
            row = normalize_wb_card(card)
            _enrich_wb_row(row, subject_map)
            nm = int(row.get("nm_id") or 0)
            if nm:
                by_nm[nm] = row

    if need_imt and not any(int(r.get("imt_id") or 0) == need_imt for r in by_nm.values()):
        # Подгрузить одну карточку целевой связки по imt (через известный nm из запроса)
        for nid in nm_ids:
            row = by_nm.get(int(nid))
            if row and int(row.get("imt_id") or 0) == need_imt:
                break
        else:
            for row in list(by_nm.values()):
                if int(row.get("imt_id") or 0) == need_imt:
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
    max_pages: int = 20,
) -> List[dict]:
    client = WbContentClient(api_key)
    subject_map = await _wb_subject_map(api_key)
    raw = await client.list_cards_all(
        max_pages=max_pages,
        text_search=text_search,
        vendor_codes=vendor_codes,
    )
    rows: List[dict] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        row = normalize_wb_card(c)
        _enrich_wb_row(row, subject_map)
        rows.append(row)
    return rows


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


async def wb_merge_cards(
    api_key: str,
    *,
    target_imt: int,
    nm_ids: List[int],
    catalog_rows: Optional[List[dict]] = None,
) -> dict:
    rows = await _wb_rows_for_merge(
        api_key,
        nm_ids=nm_ids,
        target_imt=int(target_imt),
        rows=catalog_rows,
    )
    validate_wb_merge_rows(rows, target_imt=int(target_imt), nm_ids=nm_ids)
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
