"""
Мастер связок WB — пошаговая разметка и детерминированный план связок.

Не заменяет card_links.ai_suggest_card_links; отдельный конвейер.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter, defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app.core.card_links import (
    MAX_LINK_ITEMS,
    _IKEA_BRAND_RE,
    _PRODUCT_USE_PATTERNS,
    _USE_BUCKET_LABEL_RU,
    _facet_aware_pack_bins,
    _items_are_pack_variants,
    _items_same_brand_key,
    _product_pack_key,
    _row_article,
    _row_brand_extended,
    _row_brand_key,
    _row_category_label,
    _row_use_bucket,
    apply_link_status,
    fetch_wb_catalog,
    group_wb_rows,
    wb_merge_cards,
)

log = logging.getLogger("card_links_master")

MAX_MASTER_BUNDLE = 29
SMALL_BUNDLE_MIN = 3
SMALL_BUNDLE_MAX = 9

SEGMENT_COSMETIC = "cosmetic"
SEGMENT_HOME = "home"
SEGMENT_IKEA = "ikea"
SEGMENT_PARTS = "parts"
SEGMENT_UNKNOWN = "unknown"

STEP_LOAD = "load"
STEP_BRANDS = "brands"
STEP_SEGMENT = "segment"
STEP_CLASSIFY = "classify"
STEP_PLAN = "plan"
STEP_APPLY = "apply"

_COSMETIC_SUBJECT_RE = re.compile(
    r"крем|помад|шампун|маск|дезодорант|гель|бальзам|лосьон|сыворот|"
    r"тушь|тени|пудр|скраб|пилинг|шампун|кондиционер|ополаскивател|"
    r"патч|блеск|помад|духи|дезодор|мыло\s+космет",
    re.I,
)
_PARTS_SUBJECT_RE = re.compile(
    r"запчаст|стекл|шлейф|батаре|аккумулятор|чехол|плёнк|пленк|"
    r"защитн|диспле|камер|разъём|разъем|кабел|наушник|адаптер|"
    r"iphone|samsung|xiaomi|redmi|huawei|honor|realme",
    re.I,
)
_PHONE_MODEL_RE = re.compile(
    r"(?:iphone|айфон)\s*(\d{1,2}(?:\s*(?:pro|max|plus|mini|se))*)|"
    r"(?:galaxy|samsung)\s*([a-z]?\d{2,4}[a-z]?\d?)|"
    r"(?:redmi|xiaomi)\s*(\w+\s*\d+)|"
    r"(?:honor|huawei)\s*(\w+\s*\d+)|"
    r"\b([a-z]{1,2}\d{3,4}[a-z]?)\b",
    re.I,
)
_TITLE_BRUSH_RE = re.compile(r"щетк", re.I)
_TITLE_SPONGE_RE = re.compile(r"губк", re.I)
_SUBJECT_SPONGE_RE = re.compile(r"губк", re.I)


def _title_hash(title: str) -> str:
    return hashlib.sha256((title or "").encode("utf-8")).hexdigest()[:16]


def _normalize_phone_model(title: str, raw: str = "") -> str:
    s = (raw or "").strip().lower()
    if s:
        s = re.sub(r"\s+", " ", s)
        return s[:80]
    t = (title or "").lower()
    m = _PHONE_MODEL_RE.search(t)
    if not m:
        return ""
    for g in m.groups():
        if g:
            return re.sub(r"\s+", " ", g.strip().lower())[:80]
    return ""


def _detect_segment(row: dict) -> str:
    brand_k = _row_brand_key(row)
    title = str(row.get("title") or "")
    subj = " ".join(
        str(row.get(k) or "") for k in ("subject_name", "parent_name")
    ).lower()
    if _IKEA_BRAND_RE.search(brand_k) or _IKEA_BRAND_RE.search(title):
        return SEGMENT_IKEA
    if _PARTS_SUBJECT_RE.search(subj) or _PARTS_SUBJECT_RE.search(title):
        return SEGMENT_PARTS
    if _COSMETIC_SUBJECT_RE.search(subj) or _COSMETIC_SUBJECT_RE.search(title):
        return SEGMENT_COSMETIC
    return SEGMENT_HOME


def _cosmetic_type_label(row: dict) -> str:
    bucket = _row_use_bucket(row)
    if bucket:
        return _USE_BUCKET_LABEL_RU.get(bucket, bucket)
    t = (row.get("title") or "").lower()
    for key, pat in _PRODUCT_USE_PATTERNS:
        if re.search(pat, t, re.I):
            return _USE_BUCKET_LABEL_RU.get(key, key)
    return "косметика"


def _home_line_key(row: dict) -> str:
    title = str(row.get("title") or "").lower()
    if _TITLE_BRUSH_RE.search(title):
        return "brush"
    if _TITLE_SPONGE_RE.search(title):
        return "sponge"
    subj = str(row.get("subject_name") or "").lower()
    if _TITLE_BRUSH_RE.search(title) and _SUBJECT_SPONGE_RE.search(subj):
        return "brush_anomaly"
    return ""


def _ensure_row_metadata(rows: List[dict]) -> None:
    """Дозаполнить segment/brand/subtype для независимых шагов."""
    for r in rows:
        if not r.get("segment"):
            r["segment"] = _detect_segment(r)
        if not r.get("brand"):
            r["brand"] = _row_brand_extended(r) or _row_brand_key(r)
        seg = str(r.get("segment") or "")
        if seg == SEGMENT_COSMETIC and not r.get("subtype"):
            r["subtype"] = _cosmetic_type_label(r)
        elif seg == SEGMENT_PARTS and not r.get("phone_model"):
            m = _normalize_phone_model(str(r.get("title") or ""))
            if m:
                r["phone_model"] = m
        r["group_key"] = _group_key(r)


def _resolve_target_imt(bundle: dict, catalog_rows: List[dict]) -> int:
    target = int(bundle.get("target_imt") or 0)
    if target:
        return target
    imts = [int(r.get("imt_id") or 0) for r in catalog_rows if int(r.get("imt_id") or 0)]
    if imts:
        return Counter(imts).most_common(1)[0][0]
    return 0


def _group_key(row: dict) -> str:
    seg = str(row.get("segment") or SEGMENT_HOME)
    sid = int(row.get("subject_id") or 0)
    brand = _row_brand_key(row) or "nobrand"
    if seg == SEGMENT_IKEA:
        return f"ikea:{sid}"
    if seg == SEGMENT_COSMETIC:
        st = str(row.get("subtype") or _cosmetic_type_label(row))
        return f"cos:{sid}:{brand}:{st}"
    if seg == SEGMENT_PARTS:
        model = str(row.get("phone_model") or _normalize_phone_model(str(row.get("title") or "")))
        return f"parts:{sid}:{model or 'unknown'}"
    line = _home_line_key(row)
    if line:
        return f"home:{sid}:{line}:{brand}"
    return f"home:{sid}:{brand}"


def _row_to_item_dict(row: dict) -> dict:
    return {
        "nm_id": int(row.get("nm_id") or 0),
        "vendor_code": str(row.get("vendor_code") or ""),
        "title": str(row.get("title") or "")[:240],
        "subject_id": int(row.get("subject_id") or 0),
        "subject_name": str(row.get("subject_name") or ""),
        "parent_name": str(row.get("parent_name") or ""),
        "imt_id": int(row.get("imt_id") or 0),
        "linked": bool(row.get("linked")),
        "brand": _row_brand_extended(row) or _row_brand_key(row),
        "segment": str(row.get("segment") or ""),
        "subtype": str(row.get("subtype") or ""),
        "phone_model": str(row.get("phone_model") or ""),
        "bundle_id": str(row.get("bundle_id") or ""),
        "group_key": str(row.get("group_key") or ""),
        "status": str(row.get("status") or "pending"),
        "title_hash": _title_hash(str(row.get("title") or "")),
    }


async def master_step_load(
    api_key: str,
    *,
    max_pages: int = 100,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> Tuple[List[dict], List[dict], dict]:
    if not (api_key or "").strip():
        raise ValueError("Не задан API-ключ магазина WB")
    if progress_cb:
        progress_cb(0, 1, "Загрузка каталога WB…")
    rows, meta = await fetch_wb_catalog(api_key.strip(), max_pages=max_pages)
    groups = group_wb_rows(rows)
    apply_link_status(rows, groups)
    if progress_cb:
        progress_cb(1, 1, f"Загружено {len(rows)} карточек")
    return rows, groups, meta


def master_step_brands(rows: List[dict]) -> Tuple[List[dict], dict]:
    n = 0
    for r in rows:
        b = _row_brand_extended(r) or _row_brand_key(r)
        r["brand"] = b
        if b:
            n += 1
    return rows, {"branded": n, "total": len(rows)}


def master_step_segment(rows: List[dict]) -> Tuple[List[dict], dict]:
    counts: Counter[str] = Counter()
    for r in rows:
        seg = _detect_segment(r)
        r["segment"] = seg
        counts[seg] += 1
    return rows, dict(counts)


async def master_step_classify(
    rows: List[dict],
    *,
    openai_key: str = "",
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> Tuple[List[dict], dict]:
    """Тип косметики / модель запчастей. ИИ — только на пустые поля, батчами по subject."""
    from app.core.openai_client import OpenAIClient

    meta: dict = {"ai_batches": 0, "rule_filled": 0, "ai_filled": 0, "skipped": 0}
    _ensure_row_metadata(rows)

    for r in rows:
        seg = str(r.get("segment") or "")
        if seg == SEGMENT_COSMETIC:
            if not r.get("subtype"):
                r["subtype"] = _cosmetic_type_label(r)
                meta["rule_filled"] += 1
        elif seg == SEGMENT_PARTS:
            if not r.get("phone_model"):
                m = _normalize_phone_model(str(r.get("title") or ""))
                if m:
                    r["phone_model"] = m
                    meta["rule_filled"] += 1
        r["group_key"] = _group_key(r)

    need_ai: List[dict] = []
    for r in rows:
        seg = str(r.get("segment") or "")
        if seg == SEGMENT_PARTS and not r.get("phone_model"):
            need_ai.append(r)
        elif seg == SEGMENT_COSMETIC and str(r.get("subtype") or "") in ("", "косметика"):
            need_ai.append(r)

    if not need_ai or not (openai_key or "").strip():
        meta["skipped"] = len(need_ai)
        return rows, meta

    client = OpenAIClient(openai_key.strip())
    batches: Dict[int, List[dict]] = defaultdict(list)
    for r in need_ai:
        batches[int(r.get("subject_id") or 0)].append(r)
    total = len(batches)
    done = 0
    for sid, batch in batches.items():
        if len(batch) < 1:
            continue
        done += 1
        if progress_cb:
            progress_cb(done, max(total, 1), f"ИИ: subject {sid} ({len(batch)} шт)")
        payload = [
            {
                "nm_id": int(x.get("nm_id") or 0),
                "title": str(x.get("title") or "")[:120],
                "segment": str(x.get("segment") or ""),
            }
            for x in batch[:80]
        ]
        sys_p = (
            "Верни JSON-массив объектов: "
            '{"nm_id":123,"subtype":"крем для рук"} для косметики или '
            '{"nm_id":123,"phone_model":"samsung a52"} для запчастей. '
            "Только JSON, без markdown."
        )
        try:
            raw = await client.generate(sys_p, json.dumps(payload, ensure_ascii=False))
            text = (raw or "").strip()
            if text.startswith("```"):
                text = re.sub(r"^```\w*\n?", "", text)
                text = re.sub(r"\n?```$", "", text)
            parsed = json.loads(text)
            if isinstance(parsed, list):
                by_nm = {int(x.get("nm_id") or 0): x for x in batch}
                for item in parsed:
                    if not isinstance(item, dict):
                        continue
                    nid = int(item.get("nm_id") or 0)
                    if nid not in by_nm:
                        continue
                    row = by_nm[nid]
                    if str(row.get("segment")) == SEGMENT_PARTS:
                        m = str(item.get("phone_model") or "").strip().lower()
                        if m:
                            row["phone_model"] = m[:80]
                            meta["ai_filled"] += 1
                    elif str(row.get("segment")) == SEGMENT_COSMETIC:
                        st = str(item.get("subtype") or "").strip()
                        if st:
                            row["subtype"] = st[:80]
                            meta["ai_filled"] += 1
                meta["ai_batches"] += 1
        except Exception as e:
            log.warning("master classify AI subject %s: %s", sid, e)

    for r in rows:
        if str(r.get("segment")) == SEGMENT_PARTS and not r.get("phone_model"):
            r["phone_model"] = "unknown"
        if str(r.get("segment")) == SEGMENT_COSMETIC and not r.get("subtype"):
            r["subtype"] = _cosmetic_type_label(r)
        r["group_key"] = _group_key(r)

    return rows, meta


def _pack_group(items: List[dict], segment: str) -> List[List[dict]]:
    if len(items) <= MAX_MASTER_BUNDLE:
        if segment == SEGMENT_COSMETIC and len(items) >= 2:
            chunks = _facet_aware_pack_bins(items, marketplace="wb")
            return [c for c in chunks if c]
        return [items] if items else []
    if segment == SEGMENT_COSMETIC:
        return _facet_aware_pack_bins(items, marketplace="wb") or _split_chunks(items)
    return _split_chunks(items)


def _split_chunks(items: List[dict], size: int = MAX_MASTER_BUNDLE) -> List[List[dict]]:
    out: List[List[dict]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _can_merge_bundles(a: List[dict], b: List[dict], segment: str) -> bool:
    if len(a) + len(b) > MAX_MASTER_BUNDLE:
        return False
    combined = a + b
    if segment == SEGMENT_COSMETIC:
        return _items_same_brand_key(combined)
    if segment == SEGMENT_PARTS:
        models = {str(x.get("phone_model") or "") for x in combined}
        models.discard("")
        return len(models) <= 1
    return True


def _consolidate_small_bundles(
    bundles: List[List[dict]],
    segment: str,
    subject_id: int,
) -> List[List[dict]]:
    """Склеить пачки 3–9 в одном subject (мелкое к мелкому)."""
    small = [b for b in bundles if SMALL_BUNDLE_MIN <= len(b) <= SMALL_BUNDLE_MAX]
    large = [b for b in bundles if len(b) > SMALL_BUNDLE_MAX or len(b) < SMALL_BUNDLE_MIN]
    if len(small) < 2:
        return bundles
    small.sort(key=len, reverse=True)
    merged: List[List[dict]] = []
    used: Set[int] = set()
    for i, base in enumerate(small):
        if i in used:
            continue
        cur = list(base)
        used.add(i)
        for j, other in enumerate(small):
            if j in used or j == i:
                continue
            if _can_merge_bundles(cur, other, segment):
                cur.extend(other)
                used.add(j)
                if len(cur) >= MAX_MASTER_BUNDLE:
                    break
        merged.append(cur)
    for i, b in enumerate(small):
        if i not in used:
            merged.append(b)
    return large + merged


def _merge_bundle_dicts(a: dict, b: dict, segment: str) -> Optional[dict]:
    items_a = list(a.get("items") or [])
    items_b = list(b.get("items") or [])
    if not _can_merge_bundles(items_a, items_b, segment):
        return None
    nm_a = list(a.get("nm_ids") or [])
    nm_b = list(b.get("nm_ids") or [])
    if len(nm_a) + len(nm_b) > MAX_MASTER_BUNDLE:
        return None
    merged_items = items_a + items_b
    merged_nm = nm_a + nm_b
    imts = [int(x.get("imt_id") or 0) for x in merged_items if int(x.get("imt_id") or 0)]
    target = Counter(imts).most_common(1)[0][0] if imts else int(a.get("target_imt") or 0)
    out = dict(a)
    out["items"] = merged_items
    out["nm_ids"] = merged_nm
    out["item_count"] = len(merged_nm)
    out["sort_size"] = len(merged_nm)
    out["target_imt"] = target
    return out


def _consolidate_bundles_by_subject(bundles: List[dict], segment: str) -> List[dict]:
    """Мелкие пачки (3–9) в одном subject — склеить до 29 (дом / ikea)."""
    if segment not in (SEGMENT_HOME, SEGMENT_IKEA):
        return bundles
    by_subj: Dict[int, List[dict]] = defaultdict(list)
    for b in bundles:
        sid = int((b.get("items") or [{}])[0].get("subject_id") or 0)
        by_subj[sid].append(b)
    out: List[dict] = []
    for _sid, blist in by_subj.items():
        small = [b for b in blist if SMALL_BUNDLE_MIN <= int(b.get("item_count") or 0) <= SMALL_BUNDLE_MAX]
        rest = [b for b in blist if b not in small]
        small.sort(key=lambda x: -int(x.get("item_count") or 0))
        used: Set[str] = set()
        merged_small: List[dict] = []
        for base in small:
            bid = str(base.get("bundle_id") or "")
            if bid in used:
                continue
            cur = dict(base)
            used.add(bid)
            for other in small:
                oid = str(other.get("bundle_id") or "")
                if oid in used:
                    continue
                m = _merge_bundle_dicts(cur, other, segment)
                if m:
                    used.add(oid)
                    cur = m
                    if int(cur.get("item_count") or 0) >= MAX_MASTER_BUNDLE:
                        break
            merged_small.append(cur)
        out.extend(rest)
        out.extend(merged_small)
    return out


def master_step_plan(rows: List[dict]) -> Tuple[List[dict], List[dict], dict]:
    """
    План связок. Возвращает (обновлённые rows, bundles, meta).
    """
    _ensure_row_metadata(rows)
    by_gk: Dict[str, List[dict]] = defaultdict(list)
    for r in rows:
        gk = _group_key(r)
        r["group_key"] = gk
        by_gk[gk].append(r)

    bundles_out: List[dict] = []
    singles = 0
    seq = 0
    assigned_nm: Set[int] = set()

    # subject -> segment for consolidation
    subject_segments: Dict[int, str] = {}

    for gk, items in sorted(by_gk.items(), key=lambda x: x[0]):
        if len(items) < 2:
            for it in items:
                it["status"] = "solo"
                it["bundle_id"] = ""
            singles += len(items)
            continue
        seg = str(items[0].get("segment") or SEGMENT_HOME)
        sid = int(items[0].get("subject_id") or 0)
        subject_segments[sid] = seg
        chunks = _pack_group(items, seg)
        sid_chunks = _consolidate_small_bundles(chunks, seg, sid)
        for chunk in sid_chunks:
            if len(chunk) < 2:
                for it in chunk:
                    it["status"] = "solo"
                    it["bundle_id"] = ""
                singles += len(chunk)
                continue
            seq += 1
            bid = f"mb-{seq}"
            imts = [int(x.get("imt_id") or 0) for x in chunk if int(x.get("imt_id") or 0)]
            target_imt = Counter(imts).most_common(1)[0][0] if imts else 0
            cat = _row_category_label(chunk[0], marketplace="wb")
            brand = _row_brand_extended(chunk[0]) or _row_brand_key(chunk[0])
            subtype = str(chunk[0].get("subtype") or chunk[0].get("phone_model") or "")
            nm_ids = []
            for it in chunk:
                nid = int(it.get("nm_id") or 0)
                if nid in assigned_nm:
                    continue
                assigned_nm.add(nid)
                it["bundle_id"] = bid
                it["status"] = "planned"
                nm_ids.append(nid)
            if len(nm_ids) < 2:
                continue
            bundles_out.append(
                {
                    "bundle_id": bid,
                    "segment": seg,
                    "category_label": cat,
                    "brand": brand,
                    "subtype_label": subtype,
                    "item_count": len(nm_ids),
                    "target_imt": target_imt,
                    "nm_ids": nm_ids,
                    "items": [_row_to_item_dict(it) for it in chunk if int(it.get("nm_id") or 0) in nm_ids],
                    "sort_size": len(nm_ids),
                }
            )

    # Укрупнение мелких пачек (дом / ikea) в одном subject
    for seg in (SEGMENT_HOME, SEGMENT_IKEA):
        seg_b = [b for b in bundles_out if b.get("segment") == seg]
        if seg_b:
            consolidated = _consolidate_bundles_by_subject(seg_b, seg)
            bundles_out = [b for b in bundles_out if b.get("segment") != seg] + consolidated

    bundle_nm: Dict[int, str] = {}
    for b in bundles_out:
        bid = str(b.get("bundle_id") or "")
        for nid in b.get("nm_ids") or []:
            bundle_nm[int(nid)] = bid
    for r in rows:
        nid = int(r.get("nm_id") or 0)
        bid = bundle_nm.get(nid, "")
        if bid:
            r["bundle_id"] = bid
            r["status"] = "planned"
        elif r.get("status") != "solo":
            r["bundle_id"] = ""
            if r.get("group_key") and len(by_gk.get(r.get("group_key") or "", [])) >= 2:
                r["status"] = "pending"
            else:
                r["status"] = "solo"

    bundles_out.sort(key=lambda b: (-int(b.get("sort_size") or 0), str(b.get("category_label") or "")))

    covered = sum(1 for r in rows if r.get("status") == "planned")
    meta = {
        "total": len(rows),
        "bundles": len(bundles_out),
        "planned_items": covered,
        "singles": singles,
        "pending": len(rows) - covered - singles,
    }
    return out


def master_merge_bundles(
    rows: List[dict],
    bundles: List[dict],
    bundle_ids: List[str],
) -> Tuple[List[dict], List[dict], dict]:
    """Ручное объединение 2+ связок плана в одну (до 29 SKU)."""
    ids = [str(x).strip() for x in (bundle_ids or []) if str(x).strip()]
    uniq_ids = list(dict.fromkeys(ids))
    if len(uniq_ids) < 2:
        raise ValueError("Выберите минимум 2 связки для объединения")

    by_id = {str(b.get("bundle_id") or ""): b for b in bundles if str(b.get("bundle_id") or "")}
    selected = [by_id[bid] for bid in uniq_ids if bid in by_id]
    if len(selected) != len(uniq_ids):
        missing = [bid for bid in uniq_ids if bid not in by_id]
        raise ValueError(f"Связки не найдены в плане: {', '.join(missing[:5])}")

    seg = str(selected[0].get("segment") or SEGMENT_HOME)

    def _bundle_subject(b: dict) -> int:
        items = b.get("items") or []
        if items:
            return int(items[0].get("subject_id") or 0)
        nms = b.get("nm_ids") or []
        if nms:
            row = next((r for r in rows if int(r.get("nm_id") or 0) == int(nms[0])), None)
            if row:
                return int(row.get("subject_id") or 0)
        return 0

    want_subj = _bundle_subject(selected[0])
    if not want_subj:
        raise ValueError("Не определён subjectID у первой связки")

    for b in selected[1:]:
        if str(b.get("segment") or "") != seg:
            raise ValueError("Разные сегменты — объединяйте связки одного типа (косметика/дом/…)")
        if _bundle_subject(b) != want_subj:
            raise ValueError(
                "Разные предметы WB (subjectID) — объединяйте связки одной категории WB"
            )

    cur = dict(selected[0])
    for b in selected[1:]:
        merged = _merge_bundle_dicts(cur, b, seg)
        if not merged:
            raise ValueError(
                f"Нельзя объединить «{cur.get('bundle_id')}» и «{b.get('bundle_id')}»: "
                f"лимит {MAX_MASTER_BUNDLE} SKU или несовместимые бренд/модель"
            )
        cur = merged

    nm_ids = []
    seen_nm: Set[int] = set()
    for nid in cur.get("nm_ids") or []:
        n = int(nid)
        if n and n not in seen_nm:
            seen_nm.add(n)
            nm_ids.append(n)
    if len(nm_ids) < 2:
        raise ValueError("После объединения осталось меньше 2 товаров")

    seq_base = max(
        (int(re.sub(r"\D", "", str(b.get("bundle_id") or "")) or 0) for b in bundles),
        default=0,
    )
    new_id = f"mb-m{seq_base + 1}"
    while any(str(b.get("bundle_id") or "") == new_id for b in bundles):
        seq_base += 1
        new_id = f"mb-m{seq_base + 1}"

    brands = sorted({str(b.get("brand") or "").strip() for b in selected if str(b.get("brand") or "").strip()})
    cat = str(selected[0].get("category_label") or "")
    if not cat and selected[0].get("items"):
        cat = _row_category_label(selected[0]["items"][0], marketplace="wb")

    cur["bundle_id"] = new_id
    cur["segment"] = seg
    cur["nm_ids"] = nm_ids
    cur["item_count"] = len(nm_ids)
    cur["sort_size"] = len(nm_ids)
    cur["apply_status"] = "pending"
    cur["category_label"] = cat
    cur["brand"] = brands[0] if len(brands) == 1 else " · ".join(brands[:4])
    cur["items"] = [
        _row_to_item_dict(r)
        for r in rows
        if int(r.get("nm_id") or 0) in seen_nm
    ]
    imts = [int(x.get("imt_id") or 0) for x in cur["items"] if int(x.get("imt_id") or 0)]
    cur["target_imt"] = Counter(imts).most_common(1)[0][0] if imts else int(cur.get("target_imt") or 0)

    remove = set(uniq_ids)
    new_bundles = [b for b in bundles if str(b.get("bundle_id") or "") not in remove]
    new_bundles.append(cur)
    new_bundles.sort(key=lambda b: (-int(b.get("sort_size") or 0), str(b.get("category_label") or "")))

    for r in rows:
        if str(r.get("bundle_id") or "") in remove:
            nid = int(r.get("nm_id") or 0)
            if nid in seen_nm:
                r["bundle_id"] = new_id
                r["status"] = "planned"

    meta = {
        "merged_from": uniq_ids,
        "new_bundle_id": new_id,
        "item_count": len(nm_ids),
        "segment": seg,
        "category_label": cat,
    }
    return rows, new_bundles, meta


async def master_apply_bundles(
    api_key: str,
    bundles: List[dict],
    rows_by_nm: Dict[int, dict],
    *,
    bundle_ids: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> dict:
    """Применить план: крупные пачки первыми, moving-only disconnect."""
    import asyncio

    from app.core.net import HttpStatusError

    if not (api_key or "").strip():
        raise ValueError("Не задан API-ключ магазина WB")

    to_run = bundles
    if bundle_ids is not None:
        allow = {str(x) for x in bundle_ids if str(x).strip()}
        if allow:
            to_run = [b for b in bundles if str(b.get("bundle_id")) in allow]
        else:
            to_run = list(bundles)
    to_run = sorted(to_run, key=lambda b: -int(b.get("sort_size") or b.get("item_count") or 0))

    ok = 0
    fail = 0
    skipped = 0
    errors: List[dict] = []
    log_lines: List[str] = []
    applied_ids: List[str] = []
    skipped_ids: List[str] = []
    failed_ids: List[str] = []

    total = len(to_run)
    for i, b in enumerate(to_run):
        bid = str(b.get("bundle_id") or "")
        nm_ids = [int(x) for x in (b.get("nm_ids") or []) if int(x)]
        if len(nm_ids) < 2:
            fail += 1
            msg = f"Пачка {bid}: меньше 2 товаров"
            errors.append({"bundle_id": bid, "error": msg})
            log_lines.append(msg)
            failed_ids.append(bid)
            continue

        missing = [n for n in nm_ids if n not in rows_by_nm]
        if missing:
            fail += 1
            msg = f"Пачка {bid}: {len(missing)} nm_id нет в кэше — выполните шаг 1"
            errors.append({"bundle_id": bid, "error": msg, "missing_nm_ids": missing[:10]})
            log_lines.append(msg)
            failed_ids.append(bid)
            continue

        catalog_rows = [rows_by_nm[n] for n in nm_ids]
        target = _resolve_target_imt(b, catalog_rows)
        if not target:
            fail += 1
            msg = f"Пачка {bid}: не определён target imtID"
            errors.append({"bundle_id": bid, "error": msg})
            log_lines.append(msg)
            failed_ids.append(bid)
            continue

        to_move = [
            n for n in nm_ids
            if int(rows_by_nm[n].get("imt_id") or 0) != target
        ]
        if not to_move:
            skipped += 1
            skipped_ids.append(bid)
            log_lines.append(f"Пропуск · {bid}: уже в imtID {target}")
            continue

        if progress_cb:
            progress_cb(
                i + 1,
                total,
                f"Связка {bid} ({len(to_move)}/{len(nm_ids)} перенос) → imtID {target}",
            )
        try:
            await wb_merge_cards(
                api_key.strip(),
                target_imt=target,
                nm_ids=nm_ids,
                catalog_rows=catalog_rows,
                disconnect_first=True,
            )
            ok += 1
            applied_ids.append(bid)
            log_lines.append(
                f"OK · {bid} · {b.get('category_label', '')} · "
                f"{len(to_move)} перенос → imtID {target}"
            )
        except ValueError as e:
            err = str(e)[:300]
            if "уже в этой связке" in err.lower():
                skipped += 1
                skipped_ids.append(bid)
                log_lines.append(f"Пропуск · {bid}: {err}")
            else:
                fail += 1
                failed_ids.append(bid)
                errors.append({"bundle_id": bid, "error": err})
                log_lines.append(f"Ошибка · {bid}: {err}")
        except HttpStatusError as e:
            fail += 1
            failed_ids.append(bid)
            err = str(e.body or e)[:300]
            errors.append({"bundle_id": bid, "error": err, "status": int(getattr(e, "status", 0) or 0)})
            log_lines.append(f"Ошибка · {bid}: {err}")
        except Exception as e:
            fail += 1
            failed_ids.append(bid)
            err = str(e)[:300]
            errors.append({"bundle_id": bid, "error": err})
            log_lines.append(f"Ошибка · {bid}: {err}")
        if i < total - 1:
            await asyncio.sleep(2.0)

    return {
        "ok": ok,
        "fail": fail,
        "skipped": skipped,
        "errors": errors,
        "log": log_lines,
        "applied_bundle_ids": applied_ids,
        "skipped_bundle_ids": skipped_ids,
        "failed_bundle_ids": failed_ids,
    }


def master_coverage_stats(rows: List[dict], bundles: List[dict]) -> dict:
    total = len(rows)
    planned = sum(1 for r in rows if r.get("status") == "planned")
    solo = sum(1 for r in rows if r.get("status") == "solo")
    pending = total - planned - solo
    by_seg: Counter[str] = Counter()
    for r in rows:
        by_seg[str(r.get("segment") or "unknown")] += 1
    return {
        "total": total,
        "planned_items": planned,
        "singles": solo,
        "pending": pending,
        "bundles": len(bundles),
        "by_segment": dict(by_seg),
    }


async def run_master_step(
    db: Any,
    store_id: int,
    step: str,
    *,
    api_key: str,
    openai_key: str = "",
    max_pages: int = 100,
    bundle_ids: Optional[List[str]] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> dict:
    """Выполнить один шаг мастера; сохраняет в SQLite."""
    from app.db import utc_now_iso

    sid = int(store_id)
    st = db.clm_get_state(sid)
    steps = dict(st.get("steps") or {})

    if step == STEP_LOAD:
        db.clm_clear_store(sid)
        rows, groups, meta = await master_step_load(
            api_key, max_pages=max_pages, progress_cb=progress_cb,
        )
        for r in rows:
            r.setdefault("status", "pending")
        db.clm_save_items(sid, rows)
        steps[STEP_LOAD] = {"ok": True, "at": utc_now_iso(), "count": len(rows), "meta": meta}
        db.clm_set_state(sid, steps=steps, catalog_at=utc_now_iso())
        db.clm_append_log(sid, f"Загружено {len(rows)} карточек WB")
        cov = db.clm_coverage(sid)
        return {"step": step, "coverage": cov, "meta": meta}

    rows = db.clm_load_items(sid)
    if not rows:
        raise ValueError("Сначала выполните шаг «Загрузить WB»")

    if step == STEP_BRANDS:
        rows, meta = master_step_brands(rows)
        db.clm_save_items(sid, rows)
        steps[STEP_BRANDS] = {"ok": True, "at": utc_now_iso(), **meta}
        db.clm_set_state(sid, steps=steps)
        db.clm_append_log(sid, f"Бренды: {meta.get('branded')}/{meta.get('total')}")
        return {"step": step, "meta": meta, "coverage": db.clm_coverage(sid)}

    if step == STEP_SEGMENT:
        rows, meta = master_step_segment(rows)
        db.clm_save_items(sid, rows)
        steps[STEP_SEGMENT] = {"ok": True, "at": utc_now_iso(), "counts": meta}
        db.clm_set_state(sid, steps=steps)
        db.clm_append_log(sid, f"Сегменты: {meta}")
        return {"step": step, "meta": meta, "coverage": db.clm_coverage(sid)}

    if step == STEP_CLASSIFY:
        rows, meta = await master_step_classify(
            rows, openai_key=openai_key, progress_cb=progress_cb,
        )
        db.clm_save_items(sid, rows)
        steps[STEP_CLASSIFY] = {"ok": True, "at": utc_now_iso(), **meta}
        db.clm_set_state(sid, steps=steps)
        db.clm_append_log(sid, f"Тип/модель: ИИ батчей {meta.get('ai_batches', 0)}")
        return {"step": step, "meta": meta, "coverage": db.clm_coverage(sid)}

    if step == STEP_PLAN:
        rows, bundles, meta = master_step_plan(rows)
        db.clm_save_items(sid, rows)
        db.clm_save_bundles(sid, bundles)
        steps[STEP_PLAN] = {"ok": True, "at": utc_now_iso(), **meta}
        db.clm_set_state(sid, steps=steps)
        db.clm_append_log(
            sid,
            f"План: {meta.get('bundles')} связок, одиночек {meta.get('singles')}, в плане {meta.get('planned_items')} товаров",
        )
        return {"step": step, "meta": meta, "coverage": db.clm_coverage(sid)}

    if step == STEP_APPLY:
        bundles, _ = db.clm_load_bundles(sid, limit=100000, offset=0)
        if not bundles:
            raise ValueError("Нет плана связок — выполните шаг «План»")
        rows_by_nm = {int(r.get("nm_id") or 0): r for r in rows if int(r.get("nm_id") or 0)}
        result = await master_apply_bundles(
            api_key,
            bundles,
            rows_by_nm,
            bundle_ids=bundle_ids,
            progress_cb=progress_cb,
        )
        db.clm_set_bundle_apply_statuses(
            sid,
            applied=result.get("applied_bundle_ids") or [],
            skipped=result.get("skipped_bundle_ids") or [],
            failed=result.get("failed_bundle_ids") or [],
        )
        steps[STEP_APPLY] = {
            "ok": result.get("fail", 0) == 0,
            "at": utc_now_iso(),
            **{k: v for k, v in result.items() if k not in ("log",)},
        }
        db.clm_set_state(sid, steps=steps)
        for line in (result.get("log") or [])[-20:]:
            db.clm_append_log(sid, line, level="info" if line.startswith("OK") else "error")
        db.clm_append_log(
            sid,
            f"Применение: OK {result.get('ok')} · ошибок {result.get('fail')}",
            level="info" if result.get("fail") == 0 else "error",
        )
        return {"step": step, **result, "coverage": db.clm_coverage(sid)}

    raise ValueError(f"Неизвестный шаг: {step}")
