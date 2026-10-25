"""
Дозаполнение «недочётов» у связок Ozon (aspect/рекомендованные поля).

Порядок источников:
1) название + описание карточки (regex)
2) опционально OpenAI — из текста title/description/snippets
3) поиск в интернете, если в карточке нет данных
4) снова разбор / ИИ по сниппетам
5) логический вес: для жидкостей/гелей с известным объёмом мл ≈ 1 г/мл,
   вес оффера = объём × единиц (1/2/3 шт)

Запрещено менять: ТН ВЭД, бренд, габариты упаковки (Д×Ш×В), вес упаковки.
Разрешено: название цвета, объём мл, единиц в товаре, вес товара (г).

Ничего не запускает само: только функции propose / apply.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import aiohttp

from app.core.net import HttpStatusError
from app.core.openai_client import OpenAIClient
from app.core.ozon_client import OzonClient

log = logging.getLogger("ozon_link_deficits")

# --- field keys (внутренние) ---
FIELD_COLOR_NAME = "color_name"
FIELD_VOLUME_ML = "volume_ml"
FIELD_UNITS = "units"
FIELD_WEIGHT_G = "weight_g"

ALLOWED_FIELDS = (FIELD_COLOR_NAME, FIELD_VOLUME_ML, FIELD_UNITS, FIELD_WEIGHT_G)

FIELD_LABELS = {
    FIELD_COLOR_NAME: "Название цвета",
    FIELD_VOLUME_ML: "Объем, мл",
    FIELD_UNITS: "Единиц в одном товаре",
    FIELD_WEIGHT_G: "Вес товара, г",
}

# Имена атрибутов Ozon → поле (подстроки, lower)
_ATTR_NAME_TO_FIELD: List[Tuple[str, str]] = [
    ("название цвета", FIELD_COLOR_NAME),
    ("цвет товара", FIELD_COLOR_NAME),  # fallback only if color_name attr missing
    ("объем, мл", FIELD_VOLUME_ML),
    ("объём, мл", FIELD_VOLUME_ML),
    ("объем мл", FIELD_VOLUME_ML),
    ("единиц в одном товаре", FIELD_UNITS),
    ("количество в упаковке", FIELD_UNITS),
    ("кол-во в упаковке", FIELD_UNITS),
    ("вес товара, г", FIELD_WEIGHT_G),
    ("вес товара г", FIELD_WEIGHT_G),
    ("вес, г", FIELD_WEIGHT_G),
]

# Никогда не трогаем (имена атрибутов)
_FORBIDDEN_ATTR_NAME_RE = re.compile(
    r"тн\s*вэд|tn\s*ved|бренд|brand|"
    r"длина\s*упаков|ширина\s*упаков|высота\s*упаков|"
    r"вес\s*в\s*упаков|вес\s*с\s*упаков|габарит",
    re.I,
)

_VOLUME_RE = re.compile(
    r"(?<!\d)(\d+(?:[.,]\d+)?)\s*(мл|ml)\b",
    re.I,
)
_WEIGHT_RE = re.compile(
    r"(?<!\d)(\d+(?:[.,]\d+)?)\s*(г|гр|g|gram|грамм)\b(?!\s*/)",
    re.I,
)
# не путать с габаритами «21 см»
_UNITS_RE = re.compile(
    r"(?<!\d)(\d+)\s*(шт|штук|уп|упак|pcs|pc)\b",
    re.I,
)
_COLOR_RE = re.compile(
    r"(?:цвет(?:а|у)?|окрас(?:ка)?)\s*[:\-]?\s*"
    r"([а-яёa-z0-9\- ]{2,40}?)(?=,|\.|;|/|\(|$|\d\s*(?:см|мм|мл|г))",
    re.I,
)

ProgressCb = Optional[Callable[[int, int, str], None]]


@dataclass
class FieldProposal:
    field: str
    attr_id: int
    attr_name: str
    value: str
    source: str  # title | description | ai_text | web | ai_web | existing
    confidence: float = 1.0


@dataclass
class OfferDeficitPlan:
    offer_id: str
    title: str
    model_name: str
    description_category_id: int
    type_id: int
    proposals: List[FieldProposal] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)  # причины
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


def _norm_num(s: str) -> str:
    return str(s or "").strip().replace(",", ".").rstrip("0").rstrip(".") if "." in str(s).replace(",", ".") else str(s).strip()


def extract_facts_from_text(text: str) -> Dict[str, str]:
    """Достаёт факты из произвольного текста (title/description/snippet)."""
    t = str(text or "").strip()
    out: Dict[str, str] = {}
    if not t:
        return out

    m = _VOLUME_RE.search(t)
    if m:
        out[FIELD_VOLUME_ML] = _norm_num(m.group(1))

    m = _WEIGHT_RE.search(t)
    if m:
        # отсечь явные «см» контексты рядом — уже не в regex
        out[FIELD_WEIGHT_G] = _norm_num(m.group(1))

    m = _UNITS_RE.search(t)
    if m:
        out[FIELD_UNITS] = str(int(m.group(1)))

    m = _COLOR_RE.search(t)
    if m:
        color = re.sub(r"\s+", " ", m.group(1)).strip(" -–—")
        if color and len(color) >= 2:
            out[FIELD_COLOR_NAME] = color[:80]
    elif FIELD_COLOR_NAME not in out:
        # если в тексте явно «цвет X» не нашли — не угадываем одиночное прилагательное
        # (слишком шумно). Цвет из «Цвет товара» подставляется отдельно выше по пайплайну.
        pass

    return out


def merge_facts(*dicts: Dict[str, str]) -> Dict[str, str]:
    """Первый непустой выигрывает (приоритет источников задаёт порядок вызовов)."""
    out: Dict[str, str] = {}
    for d in dicts:
        for k, v in (d or {}).items():
            if k not in ALLOWED_FIELDS:
                continue
            val = str(v or "").strip()
            if val and k not in out:
                out[k] = val
    return out


# Косметика / уход на водной основе: плотность ≈ воды → г ≈ мл
_LIQUID_HINT_RE = re.compile(
    r"мл|\bml\b|крем|гель|шампун|бальзам|лосьон|тоник|сыворот|масло|мусс|"
    r"пенк|спрей|флюид|молочко|мыло|маск|паст|лак|кондиционер|дезодорант|"
    r"ополаскив|жидкост|эмульси|скраб|пилинг",
    re.I,
)


def _looks_liquid_product(title: str, *, has_volume_ml: bool) -> bool:
    t = str(title or "")
    if has_volume_ml and _VOLUME_RE.search(t):
        return True
    if has_volume_ml and _LIQUID_HINT_RE.search(t):
        return True
    return bool(_LIQUID_HINT_RE.search(t) and _VOLUME_RE.search(t))


def infer_weight_from_volume(
    *,
    title: str,
    volume_ml: str,
    units: str = "1",
) -> Optional[str]:
    """
    Вес товара (г) для фасовки: объём_мл × единиц.
    Для водных средств 1 мл ≈ 1 г (мусс/шампунь/мыло/крем…).
    """
    vol_s = str(volume_ml or "").strip()
    if not vol_s:
        return None
    if not _looks_liquid_product(title, has_volume_ml=True):
        return None
    try:
        vol = float(vol_s.replace(",", "."))
        units_n = int(float(str(units or "1").replace(",", ".")))
    except ValueError:
        return None
    if vol <= 0 or units_n <= 0 or units_n > 100:
        return None
    weight = int(round(vol * units_n))
    if weight < 1 or weight > 100_000:
        return None
    return str(weight)


def _attr_id(a: dict) -> int:
    for key in ("id", "attribute_id", "attributeId"):
        try:
            val = int(a.get(key) or 0)
        except (TypeError, ValueError):
            val = 0
        if val:
            return val
    return 0


def _attr_name(a: dict) -> str:
    return str(a.get("name") or a.get("attribute_name") or a.get("attributeName") or "").strip()


def _attr_value_text(a: dict) -> str:
    vals = a.get("values") or []
    if not isinstance(vals, list) or not vals:
        return ""
    first = vals[0]
    if isinstance(first, dict):
        text = str(first.get("value") or "").strip()
        if text:
            return text
        return str(first.get("dictionary_value") or "").strip()
    if first is not None:
        return str(first).strip()
    return ""


def is_forbidden_attr_name(name: str) -> bool:
    return bool(_FORBIDDEN_ATTR_NAME_RE.search(name or ""))


def map_attr_name_to_field(name: str) -> Optional[str]:
    if is_forbidden_attr_name(name):
        return None
    nl = (name or "").lower().replace("ё", "е")
    for needle, field in _ATTR_NAME_TO_FIELD:
        if needle in nl:
            # «цвет товара» — только если нет отдельного «название цвета» в схеме;
            # решаем на уровне resolve_schema_fields.
            return field
    return None


def resolve_schema_fields(schema_attrs: List[dict]) -> Dict[str, Tuple[int, str]]:
    """
    field_key -> (attr_id, attr_name).
    Предпочитаем «Название цвета» над «Цвет товара».
    """
    found: Dict[str, Tuple[int, str, int]] = {}  # field -> id, name, priority
    for a in schema_attrs or []:
        if not isinstance(a, dict):
            continue
        aid = _attr_id(a)
        nm = _attr_name(a)
        if not aid or not nm or is_forbidden_attr_name(nm):
            continue
        field = map_attr_name_to_field(nm)
        if not field:
            continue
        nl = nm.lower().replace("ё", "е")
        prio = 10
        if field == FIELD_COLOR_NAME:
            if "название цвета" in nl:
                prio = 100
            elif "цвет товара" in nl:
                prio = 40
            else:
                prio = 50
        if field == FIELD_UNITS:
            if "единиц в одном" in nl:
                prio = 100
            elif "количеств" in nl and "упаков" in nl:
                prio = 80
        prev = found.get(field)
        if not prev or prio > prev[2]:
            found[field] = (aid, nm, prio)
    return {k: (v[0], v[1]) for k, v in found.items()}


def current_attr_values(attrs: List[dict]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for a in attrs or []:
        if not isinstance(a, dict):
            continue
        aid = _attr_id(a)
        if not aid:
            continue
        val = _attr_value_text(a)
        if val:
            out[aid] = val
    return out


def _parse_attr_items(page: dict) -> List[dict]:
    if not isinstance(page, dict):
        return []
    res = page.get("result")
    if isinstance(res, dict):
        raw = res.get("items")
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
    if isinstance(res, list):
        return [x for x in res if isinstance(x, dict)]
    raw = page.get("items")
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    return []


def _attrs_last_id(page: dict) -> str:
    if not isinstance(page, dict):
        return ""
    res = page.get("result")
    block = res if isinstance(res, dict) else page
    return str(block.get("last_id") or block.get("lastId") or "").strip()


async def fetch_attributes_by_offers(
    client: OzonClient,
    offer_ids: List[str],
) -> Dict[str, dict]:
    """offer_id -> raw attributes item (name, attributes, category ids…)."""
    oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    out: Dict[str, dict] = {}
    for i in range(0, len(oids), 100):
        chunk = oids[i : i + 100]
        last_id = ""
        # один чанк offer_id обычно без пагинации, но на всякий
        for _ in range(20):
            page = await client.product_info_attributes(offer_ids=chunk, limit=100, last_id=last_id)
            for it in _parse_attr_items(page):
                oid = str(it.get("offer_id") or it.get("offerId") or "").strip()
                if oid:
                    out[oid] = it
            nid = _attrs_last_id(page)
            if not nid or nid == last_id:
                break
            last_id = nid
    return out


def _item_title(it: dict) -> str:
    return str(it.get("name") or it.get("title") or "").strip()


def _item_description(it: dict) -> str:
    for key in ("description", "rich_text", "richText", "annotation"):
        val = it.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    # иногда описание лежит в attributes
    for a in it.get("attributes") or []:
        if not isinstance(a, dict):
            continue
        nm = _attr_name(a).lower()
        if "аннотац" in nm or nm in ("описание", "description"):
            t = _attr_value_text(a)
            if t:
                return t
    return ""


def _item_category_ids(it: dict) -> Tuple[int, int]:
    try:
        dc = int(it.get("description_category_id") or it.get("descriptionCategoryId") or 0)
    except (TypeError, ValueError):
        dc = 0
    try:
        tid = int(it.get("type_id") or it.get("typeId") or 0)
    except (TypeError, ValueError):
        tid = 0
    return dc, tid


def _model_name_from_attrs(attrs: List[dict], model_attr_id: int = 9048) -> str:
    for a in attrs or []:
        if _attr_id(a) == model_attr_id:
            return _attr_value_text(a)
    return ""


async def web_search_snippets(query: str, *, limit: int = 5, timeout_s: float = 20.0) -> List[str]:
    """
    Лёгкий поиск через DuckDuckGo HTML (без ключей).
    При ошибке/блокировке — пустой список (fail-soft).
    """
    q = str(query or "").strip()
    if len(q) < 4:
        return []
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(q)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MarketAIDeficitBot/1.0)",
        "Accept-Language": "ru,en;q=0.8",
    }
    snippets: List[str] = []
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_s, connect=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status >= 400:
                    log.info("web_search HTTP %s for %r", resp.status, q[:80])
                    return []
                html = await resp.text(errors="ignore")
    except Exception as e:
        log.info("web_search failed: %s", e)
        return []

    # грубый разбор сниппетов
    for m in re.finditer(
        r'class="result__snippet[^"]*"[^>]*>(.*?)</a>|class="result__snippet[^"]*"[^>]*>(.*?)</td>',
        html,
        re.I | re.S,
    ):
        raw = m.group(1) or m.group(2) or ""
        text = re.sub(r"<[^>]+>", " ", raw)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) >= 20:
            snippets.append(text[:400])
        if len(snippets) >= limit:
            break
    if not snippets:
        # запасной паттерн
        for m in re.finditer(r"result__snippet[^>]*>([^<]{20,400})", html, re.I):
            snippets.append(re.sub(r"\s+", " ", m.group(1)).strip())
            if len(snippets) >= limit:
                break
    return snippets


async def ai_extract_fields(
    openai: OpenAIClient,
    *,
    title: str,
    description: str,
    snippets: List[str],
    needed: List[str],
) -> Dict[str, str]:
    """ИИ только извлекает из данного текста; иначе null. Не выдумывает."""
    if not needed:
        return {}
    need_labels = {k: FIELD_LABELS.get(k, k) for k in needed}
    system = (
        "Ты извлекаешь характеристики товара ТОЛЬКО из переданного текста. "
        "Нельзя выдумывать. Если в тексте нет явного значения — верни null для поля. "
        "Ответ — строго JSON-объект без markdown. "
        "Числа без единиц: volume_ml и weight_g и units — строки с числом. "
        "Учитывай фасовки: один и тот же товар может быть 1/2/3 шт — units бери из этой карточки."
    )
    user = json.dumps(
        {
            "title": title,
            "description": (description or "")[:2500],
            "web_snippets": snippets[:6],
            "need_fields": need_labels,
            "example": {"color_name": "бежевый", "volume_ml": "250", "units": "3", "weight_g": "175"},
        },
        ensure_ascii=False,
    )
    raw = await openai.generate(system, user)
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
    except Exception:
        log.info("ai_extract: bad JSON: %s", text[:200])
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, str] = {}
    for k in needed:
        v = data.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("null", "none", "нет", "-"):
            continue
        if k in (FIELD_VOLUME_ML, FIELD_WEIGHT_G, FIELD_UNITS):
            m = re.search(r"(\d+(?:[.,]\d+)?)", s)
            if not m:
                continue
            s = _norm_num(m.group(1))
            if k == FIELD_UNITS:
                try:
                    s = str(int(float(s)))
                except ValueError:
                    continue
        out[k] = s[:120]
    return out


def build_proposals_for_item(
    *,
    offer_id: str,
    title: str,
    description: str,
    model_name: str,
    dc: int,
    tid: int,
    attrs: List[dict],
    schema_fields: Dict[str, Tuple[int, str]],
    facts: Dict[str, str],
    sources: Dict[str, str],
    only_empty: bool = True,
) -> OfferDeficitPlan:
    plan = OfferDeficitPlan(
        offer_id=offer_id,
        title=title,
        model_name=model_name,
        description_category_id=dc,
        type_id=tid,
    )
    cur = current_attr_values(attrs)
    # если есть «Цвет товара» и нужен color_name — можно скопировать
    color_product_val = ""
    for a in attrs or []:
        nm = _attr_name(a).lower().replace("ё", "е")
        if "цвет товара" in nm and "название" not in nm:
            color_product_val = _attr_value_text(a)
            break
    if FIELD_COLOR_NAME not in facts and color_product_val:
        facts = {**facts, FIELD_COLOR_NAME: color_product_val}
        sources = {**sources, FIELD_COLOR_NAME: "color_product_attr"}

    for field_key, (aid, aname) in schema_fields.items():
        if field_key not in ALLOWED_FIELDS:
            continue
        existing = (cur.get(aid) or "").strip()
        if only_empty and existing:
            plan.skipped.append(f"{field_key}: already={existing}")
            continue
        val = (facts.get(field_key) or "").strip()
        if not val:
            plan.skipped.append(f"{field_key}: not_found")
            continue
        # не затирать другим значением при only_empty уже обработано
        plan.proposals.append(
            FieldProposal(
                field=field_key,
                attr_id=aid,
                attr_name=aname,
                value=val,
                source=sources.get(field_key, "unknown"),
                confidence=1.0 if sources.get(field_key) in ("title", "description", "color_product_attr") else 0.7,
            )
        )
    return plan


async def propose_link_deficit_fills(
    client_id: str,
    api_key: str,
    *,
    offer_ids: List[str],
    openai_key: str = "",
    use_web: bool = True,
    use_ai: bool = True,
    only_empty: bool = True,
    fields: Optional[List[str]] = None,
    progress: ProgressCb = None,
) -> dict:
    """
    Dry-run: предложения заполнения недочётов по offer_id.
    Не пишет в Ozon.
    """
    wanted = [f for f in (fields or list(ALLOWED_FIELDS)) if f in ALLOWED_FIELDS]
    oids = [str(x).strip() for x in offer_ids if str(x).strip()]
    if not oids:
        raise ValueError("offer_ids пуст")

    client = OzonClient(client_id, api_key, timeout_s=60.0)
    openai = OpenAIClient(openai_key) if (use_ai and openai_key.strip()) else None

    if progress:
        progress(0, max(len(oids), 1), "Загрузка атрибутов Ozon")
    by_item = await fetch_attributes_by_offers(client, oids)

    schema_cache: Dict[str, Dict[str, Tuple[int, str]]] = {}
    plans: List[OfferDeficitPlan] = []
    missing_offers = [o for o in oids if o not in by_item]

    for idx, oid in enumerate(oids, start=1):
        if progress:
            progress(idx - 1, len(oids), f"Разбор {oid}")
        it = by_item.get(oid)
        if not it:
            plans.append(
                OfferDeficitPlan(
                    offer_id=oid,
                    title="",
                    model_name="",
                    description_category_id=0,
                    type_id=0,
                    errors=["not_found_on_ozon"],
                )
            )
            continue

        title = _item_title(it)
        description = _item_description(it)
        attrs = [a for a in (it.get("attributes") or []) if isinstance(a, dict)]
        dc, tid = _item_category_ids(it)
        model = _model_name_from_attrs(attrs)
        schema_key = f"{dc}:{tid}"
        if schema_key not in schema_cache:
            schema_rows: List[dict] = []
            if dc and tid:
                try:
                    schema_rows = await client.description_category_attributes(
                        description_category_id=dc,
                        type_id=tid,
                    )
                except HttpStatusError as e:
                    log.info("schema failed %s: %s", schema_key, e)
            schema_cache[schema_key] = resolve_schema_fields(schema_rows)
        schema_fields = {
            k: v for k, v in schema_cache[schema_key].items() if k in wanted
        }
        if not schema_fields:
            plans.append(
                OfferDeficitPlan(
                    offer_id=oid,
                    title=title,
                    model_name=model,
                    description_category_id=dc,
                    type_id=tid,
                    skipped=["no_matching_schema_fields"],
                )
            )
            continue

        cur = current_attr_values(attrs)
        need: List[str] = []
        for field_key, (aid, _nm) in schema_fields.items():
            if only_empty and (cur.get(aid) or "").strip():
                continue
            need.append(field_key)

        sources: Dict[str, str] = {}
        facts_title = extract_facts_from_text(title)
        for k in facts_title:
            sources[k] = "title"
        facts_desc = extract_facts_from_text(description)
        for k in facts_desc:
            if k not in sources:
                sources[k] = "description"
        facts = merge_facts(facts_title, facts_desc)

        still = [k for k in need if k not in facts]
        snippets: List[str] = []
        if still and use_web:
            q = f"{title} характеристики"
            snippets = await web_search_snippets(q, limit=5)
            web_facts = extract_facts_from_text("\n".join(snippets))
            for k, v in web_facts.items():
                if k in still and k not in facts:
                    facts[k] = v
                    sources[k] = "web"
            still = [k for k in need if k not in facts]

        if still and openai is not None:
            try:
                ai_facts = await ai_extract_fields(
                    openai,
                    title=title,
                    description=description,
                    snippets=snippets,
                    needed=still,
                )
                src = "ai_web" if snippets else "ai_text"
                for k, v in ai_facts.items():
                    if k in still and k not in facts:
                        facts[k] = v
                        sources[k] = src
            except HttpStatusError as e:
                log.info("ai_extract skip: %s", e)

        # Вес: если явных грамм нет — из объёма (1 мл ≈ 1 г) × шт этой карточки
        if FIELD_WEIGHT_G in need and FIELD_WEIGHT_G not in facts:
            vol = facts.get(FIELD_VOLUME_ML) or ""
            if not vol:
                for field_key, (aid, _nm) in schema_fields.items():
                    if field_key == FIELD_VOLUME_ML:
                        vol = (cur.get(aid) or "").strip()
                        break
            units_val = facts.get(FIELD_UNITS) or ""
            if not units_val:
                for field_key, (aid, _nm) in schema_fields.items():
                    if field_key == FIELD_UNITS:
                        units_val = (cur.get(aid) or "").strip()
                        break
            if not units_val:
                units_val = extract_facts_from_text(title).get(FIELD_UNITS) or "1"
            inferred = infer_weight_from_volume(
                title=title,
                volume_ml=vol,
                units=units_val or "1",
            )
            if inferred:
                facts[FIELD_WEIGHT_G] = inferred
                sources[FIELD_WEIGHT_G] = "inferred_volume_x_units"

        plan = build_proposals_for_item(
            offer_id=oid,
            title=title,
            description=description,
            model_name=model,
            dc=dc,
            tid=tid,
            attrs=attrs,
            schema_fields=schema_fields,
            facts=facts,
            sources=sources,
            only_empty=only_empty,
        )
        plans.append(plan)

    if progress:
        progress(len(oids), len(oids), "Готово")

    with_props = sum(1 for p in plans if p.proposals)
    prop_count = sum(len(p.proposals) for p in plans)
    return {
        "ok": True,
        "dry_run": True,
        "offer_count": len(oids),
        "found_on_ozon": len(by_item),
        "missing_on_ozon": missing_offers,
        "plans_with_proposals": with_props,
        "proposal_count": prop_count,
        "plans": [p.to_dict() for p in plans],
    }


def proposals_to_update_items(plans: List[dict]) -> List[dict]:
    """Готовит payload для /v1/product/attributes/update (пачки по 100 снаружи)."""
    items: List[dict] = []
    for p in plans or []:
        oid = str(p.get("offer_id") or "").strip()
        props = p.get("proposals") or []
        if not oid or not props:
            continue
        attrs = []
        for pr in props:
            try:
                aid = int(pr.get("attr_id") or 0)
            except (TypeError, ValueError):
                aid = 0
            val = str(pr.get("value") or "").strip()
            if not aid or not val:
                continue
            if is_forbidden_attr_name(str(pr.get("attr_name") or "")):
                continue
            attrs.append(
                {
                    "id": aid,
                    "values": [{"dictionary_value_id": 0, "value": val}],
                }
            )
        if attrs:
            items.append({"offer_id": oid, "attributes": attrs})
    return items


async def apply_link_deficit_fills(
    client_id: str,
    api_key: str,
    *,
    plans: List[dict],
    pause_s: float = 0.4,
    progress: ProgressCb = None,
) -> dict:
    """Применяет proposals из propose_*(dry_run). Пишет только разрешённые атрибуты."""
    import asyncio

    items = proposals_to_update_items(plans)
    if not items:
        return {"ok": True, "updated": 0, "batches": 0, "results": []}

    client = OzonClient(client_id, api_key, timeout_s=60.0)
    results: List[dict] = []
    ok_n = fail_n = 0
    batches = (len(items) + 99) // 100
    for bi in range(0, len(items), 100):
        chunk = items[bi : bi + 100]
        if progress:
            progress(bi, len(items), f"Запись атрибутов {bi + 1}–{bi + len(chunk)}")
        try:
            api = await client.update_product_attributes(chunk)
            ok_n += len(chunk)
            results.append({"ok": True, "count": len(chunk), "api": api})
        except HttpStatusError as e:
            fail_n += len(chunk)
            results.append({"ok": False, "count": len(chunk), "error": str(e)[:400]})
        if bi + 100 < len(items) and pause_s > 0:
            await asyncio.sleep(pause_s)

    if progress:
        progress(len(items), len(items), "Готово")
    return {
        "ok": fail_n == 0,
        "updated": ok_n,
        "failed": fail_n,
        "batches": batches,
        "results": results,
    }
