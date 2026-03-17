"""
FastAPI-сервер веб-интерфейса WB Автоответчик.
Запуск: uvicorn app.web.server:app --reload
"""
from __future__ import annotations

import hmac
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Инициализация путей и логов до импорта app.db (который может использовать логи)
APP_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = str(APP_DIR / "data" / "reviews.db")
LOG_PATH = str(APP_DIR / "logs" / "app.log")
STATIC_DIR = Path(__file__).resolve().parent / "static"

from app.logging_config import setup_logging
setup_logging(LOG_PATH)

from app.db import Database, Store, ItemRow, PromptRow
from app.web import tasks as web_tasks

log = logging.getLogger("web")

app = FastAPI(title="WB Автоответчик", version="1.0")

def _parse_origins(value: str) -> list[str]:
    items = [x.strip() for x in (value or "").split(",")]
    return [x for x in items if x]


_cors_origins = _parse_origins(os.getenv("CORS_ORIGINS", "").strip())
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def _api_auth_middleware(request: Request, call_next):
    path = request.url.path or ""
    if path.startswith("/api/") and request.method.upper() != "OPTIONS":
        expected = (os.getenv("API_TOKEN") or "").strip()
        if not expected:
            return JSONResponse(
                status_code=503,
                content={"detail": "API_TOKEN не задан на сервере (переменная окружения)."},
            )

        auth = (request.headers.get("authorization") or "").strip()
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if not token or not hmac.compare_digest(token, expected):
            return JSONResponse(status_code=401, content={"detail": "Неавторизовано"})

    return await call_next(request)

_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(DB_PATH)
    return _db


# ---------- Pydantic models ----------
class StoreOut(BaseModel):
    id: int
    marketplace: str
    name: str
    active: bool
    business_id: Optional[int] = None
    client_id: Optional[str] = None


class StoreCreate(BaseModel):
    marketplace: str  # wb | yam | ozon
    name: str
    api_key: str
    active: bool = True
    business_id: Optional[int] = None
    client_id: Optional[str] = None


class StoreUpdate(BaseModel):
    name: Optional[str] = None
    api_key: Optional[str] = None
    active: Optional[bool] = None
    business_id: Optional[int] = None
    client_id: Optional[str] = None


class ItemOut(BaseModel):
    id: int
    store_id: int
    external_id: str
    item_type: str
    date: str
    rating: Optional[int]
    text: str
    author: str
    product_title: str
    status: str
    generated_text: str
    was_viewed: bool
    extra_json: str = ""


class PromptOut(BaseModel):
    id: int
    item_type: str
    rating_group: str
    prompt_text: str


class PromptUpdate(BaseModel):
    prompt_text: str


class LoadNewBody(BaseModel):
    store_ids: Optional[list[int]] = None  # null = все магазины


class GenerateBody(BaseModel):
    item_ids: list[int]


class SendBody(BaseModel):
    item_ids: list[int]


def _store_to_out(s: Store) -> StoreOut:
    return StoreOut(
        id=s.id,
        marketplace=s.marketplace,
        name=s.name,
        active=s.active,
        business_id=s.business_id,
        client_id=s.client_id,
    )


def _item_to_out(r: ItemRow) -> ItemOut:
    return ItemOut(
        id=r.id,
        store_id=r.store_id,
        external_id=r.external_id,
        item_type=r.item_type,
        date=r.date,
        rating=r.rating,
        text=r.text,
        author=r.author,
        product_title=r.product_title,
        status=r.status,
        generated_text=r.generated_text or "",
        was_viewed=r.was_viewed,
        extra_json=getattr(r, "extra_json", "") or "",
    )


# ---------- API: stores ----------
@app.get("/api/stores", response_model=list[StoreOut])
def api_list_stores(db: Database = Depends(get_db)):
    stores = db.list_stores()
    return [_store_to_out(s) for s in stores]


@app.post("/api/stores", response_model=StoreOut)
def api_create_store(body: StoreCreate, db: Database = Depends(get_db)):
    if body.marketplace == "wb":
        sid = db.upsert_store_wb(body.name, body.api_key, body.active)
    elif body.marketplace == "yam":
        if body.business_id is None:
            raise HTTPException(400, "business_id обязателен для Яндекс.Маркета")
        sid = db.upsert_store_yam(body.name, body.api_key, body.business_id, body.active)
    elif body.marketplace == "ozon":
        if not (body.client_id or "").strip():
            raise HTTPException(400, "client_id обязателен для Ozon")
        sid = db.upsert_store_ozon(body.name, body.api_key, body.client_id, body.active)
    else:
        raise HTTPException(400, "marketplace должен быть wb, yam или ozon")
    stores = [s for s in db.list_stores() if s.id == sid]
    if not stores:
        raise HTTPException(500, "Магазин не найден после создания")
    return _store_to_out(stores[0])


@app.get("/api/stores/{store_id}", response_model=StoreOut)
def api_get_store(store_id: int, db: Database = Depends(get_db)):
    stores = [s for s in db.list_stores() if s.id == store_id]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    return _store_to_out(stores[0])


@app.patch("/api/stores/{store_id}", response_model=StoreOut)
def api_update_store(store_id: int, body: StoreUpdate, db: Database = Depends(get_db)):
    stores = [s for s in db.list_stores() if s.id == store_id]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    s = stores[0]
    name = body.name if body.name is not None else s.name
    api_key = body.api_key if body.api_key is not None else s.api_key
    active = body.active if body.active is not None else s.active
    business_id = body.business_id if body.business_id is not None else s.business_id
    client_id = body.client_id if body.client_id is not None else (s.client_id or "")
    db.update_store(store_id, name, api_key, active, business_id=business_id, client_id=client_id)
    updated = [x for x in db.list_stores() if x.id == store_id][0]
    return _store_to_out(updated)


@app.delete("/api/stores/{store_id}")
def api_delete_store(store_id: int, db: Database = Depends(get_db)):
    stores = [s for s in db.list_stores() if s.id == store_id]
    if not stores:
        raise HTTPException(404, "Магазин не найден")
    db.delete_store(store_id)
    return {"ok": True}


# ---------- API: items ----------
@app.get("/api/items", response_model=list[ItemOut])
def api_list_items(
    item_type: str,  # review | question
    store_id: Optional[int] = None,
    db: Database = Depends(get_db),
):
    if store_id is not None:
        rows = db.list_items_for_ui(store_id, item_type)
    else:
        rows = db.list_items_for_ui_all(item_type)
    return [_item_to_out(r) for r in rows]


@app.get("/api/items/{item_id}", response_model=ItemOut)
def api_get_item(item_id: int, db: Database = Depends(get_db)):
    row = db.get_item_by_id(item_id)
    if not row:
        raise HTTPException(404, "Элемент не найден")
    return _item_to_out(row)


# ---------- API: settings ----------
@app.get("/api/settings")
def api_get_settings(db: Database = Depends(get_db)):
    keys = ["openai_key", "telegram_bot_token", "telegram_chat_id", "theme"]
    return {k: db.get_setting(k) or "" for k in keys}


@app.post("/api/settings")
def api_set_settings(body: dict[str, str], db: Database = Depends(get_db)):
    for k, v in body.items():
        db.set_setting(k, v or "")
    return {"ok": True}


# ---------- API: prompts ----------
@app.get("/api/prompts", response_model=list[PromptOut])
def api_list_prompts(db: Database = Depends(get_db)):
    return [PromptOut(id=p.id, item_type=p.item_type, rating_group=p.rating_group, prompt_text=p.prompt_text) for p in db.list_prompts()]


@app.patch("/api/prompts/{prompt_id}")
def api_update_prompt(prompt_id: int, body: PromptUpdate, db: Database = Depends(get_db)):
    db.update_prompt(prompt_id, body.prompt_text)
    return {"ok": True}


# ---------- API: long-running tasks ----------
@app.post("/api/load-new")
async def api_load_new(body: LoadNewBody, db: Database = Depends(get_db)):
    task_id = await web_tasks.run_load_new(db, body.store_ids)
    return {"task_id": task_id}


@app.post("/api/generate")
async def api_generate(body: GenerateBody, db: Database = Depends(get_db)):
    key = db.get_setting("openai_key") or ""
    if not key.strip():
        raise HTTPException(400, "Не задан OpenAI ключ в настройках")
    task_id = await web_tasks.run_generate(db, body.item_ids, key)
    return {"task_id": task_id}


@app.post("/api/send")
async def api_send(body: SendBody, db: Database = Depends(get_db)):
    task_id = await web_tasks.run_send(db, body.item_ids)
    return {"task_id": task_id}


@app.get("/api/tasks/{task_id}")
async def api_get_task(task_id: str):
    state = await web_tasks.get_task(task_id)
    if state is None:
        raise HTTPException(404, "Задача не найдена")
    return state


# ---------- API: stats ----------
@app.get("/api/stats")
def api_stats(db: Database = Depends(get_db)):
    return db.get_stats()


# ---------- API: log ----------
@app.get("/api/log")
def api_log_tail(db: Database = Depends(get_db)):
    try:
        with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            f.seek(max(0, f.seek(0, 2) - 100 * 1024))
            return {"text": f.read()}
    except FileNotFoundError:
        return {"text": ""}
    except Exception as e:
        log.warning("log tail failed: %s", e)
        return {"text": str(e)}


# ---------- Static SPA & PWA ----------
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/sw.js")
    def sw():
        return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript")
else:
    @app.get("/")
    def index():
        return {"message": "Static files not found. Create app/web/static/ with index.html"}
