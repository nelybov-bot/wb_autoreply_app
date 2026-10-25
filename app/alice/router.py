"""FastAPI router: webhook и health для навыка Алисы."""
from __future__ import annotations

import logging
from typing import Any, Optional

import aiohttp
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from app.db import Database

from .config import AliceWarehouseConfig, load_config
from .session_store import AliceSessionStore
from .tms_client import TmsClient
from .warehouse_service import WarehouseService

log = logging.getLogger("alice")

router = APIRouter(tags=["alice"])

_http_session: Optional[aiohttp.ClientSession] = None


def get_alice_config() -> AliceWarehouseConfig:
    return load_config()


def get_db_from_app(request: Request) -> Database:
    # Переиспользуем тот же Database, что и основной server
    from app.web.server import get_db

    return get_db()


async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(connect=4, total=8)
        )
    return _http_session


async def close_alice_http_session() -> None:
    global _http_session
    if _http_session is not None and not _http_session.closed:
        await _http_session.close()
    _http_session = None


def _alice_response(text: str, *, end_session: bool = False, version: str = "1.0") -> dict:
    return {
        "response": {
            "text": text[:1024],
            "end_session": bool(end_session),
        },
        "version": version or "1.0",
    }


def _extract_utterance(body: dict) -> str:
    req = body.get("request") if isinstance(body.get("request"), dict) else {}
    # SimpleUtterance
    cmd = (req.get("command") or "").strip()
    if cmd:
        return cmd
    orig = (req.get("original_utterance") or "").strip()
    if orig:
        return orig
    # ButtonPressed
    if req.get("type") == "ButtonPressed":
        payload = req.get("payload")
        if isinstance(payload, dict) and payload.get("text"):
            return str(payload["text"])
        return (req.get("command") or "").strip()
    return ""


def _extract_nlu_numbers(body: dict) -> list[int]:
    """Числа из request.nlu.entities (YANDEX.NUMBER) — Алиса часто так отдаёт артикул."""
    req = body.get("request") if isinstance(body.get("request"), dict) else {}
    nlu = req.get("nlu") if isinstance(req.get("nlu"), dict) else {}
    entities = nlu.get("entities") if isinstance(nlu.get("entities"), list) else []
    out: list[int] = []
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        if str(ent.get("type") or "") != "YANDEX.NUMBER":
            continue
        val = ent.get("value")
        if isinstance(val, bool):
            continue
        if isinstance(val, (int, float)):
            out.append(int(val))
        elif isinstance(val, dict) and "value" in val:
            try:
                out.append(int(val["value"]))
            except (TypeError, ValueError):
                pass
    return out


@router.get("/alice/health")
async def alice_health():
    return {"status": "ok", "service": "alice-warehouse"}


@router.post("/alice/webhook")
async def alice_webhook(
    request: Request,
    cfg: AliceWarehouseConfig = Depends(get_alice_config),
    db: Database = Depends(get_db_from_app),
    x_alice_secret: Optional[str] = Header(default=None, alias="X-Alice-Secret"),
):
    """
    Webhook навыка Яндекс Диалогов / Алисы.
    Протокол: https://yandex.ru/dev/dialogs/alice/doc/ru/request
    Ответ за < 4.5 с. Криптографической подписи у Диалогов нет —
    защита: приватный навык + опционально ALICE_SKILL_ID / ALICE_WEBHOOK_SECRET.
    """
    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(400, "Invalid body")

    version = str(body.get("version") or "1.0")
    session = body.get("session") if isinstance(body.get("session"), dict) else {}
    session_id = str(session.get("session_id") or "").strip()
    message_id = session.get("message_id", 0)
    skill_id = str(session.get("skill_id") or "").strip()
    is_new = bool(session.get("new"))

    if not session_id:
        return JSONResponse(_alice_response("Ошибка сессии.", version=version))

    # Опциональная защита webhook
    if cfg.webhook_secret:
        # Секрет можно передать заголовком или query ?secret=
        q_secret = request.query_params.get("secret") or ""
        provided = (x_alice_secret or q_secret or "").strip()
        if provided != cfg.webhook_secret:
            log.warning("alice_webhook_forbidden session=%s reason=bad_secret", session_id)
            raise HTTPException(403, "Forbidden")

    if cfg.skill_id and skill_id and skill_id != cfg.skill_id:
        log.warning(
            "alice_webhook_forbidden session=%s reason=skill_mismatch",
            session_id,
        )
        raise HTTPException(403, "Forbidden")

    utterance = _extract_utterance(body)
    nlu_numbers = _extract_nlu_numbers(body)
    request_id = f"{session_id}:{message_id}"

    store = AliceSessionStore(
        db,
        session_ttl=cfg.session_ttl_seconds,
        pending_ttl=cfg.pending_ttl_seconds,
        dedup_ttl=cfg.dedup_ttl_seconds,
    )
    http = await get_http_session()
    tms = TmsClient(
        base_url=cfg.tms_base_url,
        token=cfg.tms_api_token,
        operator=cfg.tms_operator,
        session=http,
    )
    service = WarehouseService(config=cfg, store=store, tms=tms)

    # Новая сессия без текста — приветствие
    if is_new and not utterance:
        return JSONResponse(_alice_response("Склад готов. Назовите артикул или команду.", version=version))

    reply = await service.handle(
        session_id=session_id,
        message_id=message_id,
        is_new=is_new,
        utterance=utterance,
        request_id=request_id,
        nlu_numbers=nlu_numbers,
    )
    return JSONResponse(
        _alice_response(reply.text, end_session=reply.end_session, version=version)
    )
