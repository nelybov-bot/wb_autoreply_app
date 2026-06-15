"""In-memory сессии диалога агента (один процесс)."""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

_SESSION_TTL_SEC = 6 * 3600
_MAX_MESSAGES = 40

_sessions: dict[str, "AgentSession"] = {}


@dataclass
class PendingAction:
    tool: str
    args: dict[str, Any]
    summary: str
    created_at: float = field(default_factory=time.time)


@dataclass
class AgentSession:
    session_id: str
    user_id: int
    username: str
    messages: list[dict[str, str]] = field(default_factory=list)
    pending: Optional[PendingAction] = None
    last_task_id: Optional[str] = None
    context: dict[str, Any] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)


def _prune() -> None:
    now = time.time()
    dead = [sid for sid, s in _sessions.items() if now - s.updated_at > _SESSION_TTL_SEC]
    for sid in dead:
        _sessions.pop(sid, None)


def get_or_create_session(*, user_id: int, username: str, session_id: Optional[str] = None) -> AgentSession:
    _prune()
    if session_id and session_id in _sessions:
        s = _sessions[session_id]
        if s.user_id == user_id:
            s.updated_at = time.time()
            return s
    sid = session_id or uuid.uuid4().hex[:16]
    s = AgentSession(session_id=sid, user_id=user_id, username=username)
    _sessions[sid] = s
    return s


def get_session_if_owner(session_id: str, *, user_id: int) -> Optional[AgentSession]:
    s = _sessions.get(session_id)
    if not s or s.user_id != user_id:
        return None
    s.updated_at = time.time()
    return s


def clear_session(session_id: str, *, user_id: int) -> bool:
    s = _sessions.get(session_id)
    if not s or s.user_id != user_id:
        return False
    _sessions.pop(session_id, None)
    return True


def append_message(session: AgentSession, role: str, content: str) -> None:
    session.messages.append({"role": role, "content": content})
    if len(session.messages) > _MAX_MESSAGES:
        session.messages = session.messages[-_MAX_MESSAGES:]
    session.updated_at = time.time()


def session_public_view(session: AgentSession) -> dict[str, Any]:
    return {
        "session_id": session.session_id,
        "messages": list(session.messages),
        "pending": (
            {"tool": session.pending.tool, "summary": session.pending.summary}
            if session.pending
            else None
        ),
    }
