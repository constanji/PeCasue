"""Agent (Copilot) HTTP surface."""
from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from server.agents.copilot import ask as copilot_ask
from server.agents.copilot import iter_copilot_ndjson
from server.agents.tools import list_drafts

router = APIRouter()


class CopilotHistoryTurn(BaseModel):
    role: str
    content: str

    @field_validator("role")
    @classmethod
    def role_ok(cls, v: str) -> str:
        r = (v or "").strip().lower()
        if r not in ("user", "assistant"):
            raise ValueError('history.role must be "user" or "assistant"')
        return r

    @field_validator("content")
    @classmethod
    def content_nonempty(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("history.content must not be empty")
        return s


class AskBody(BaseModel):
    task_id: str
    channel_id: Optional[str] = None
    run_id: Optional[str] = None
    verify_row_id: Optional[str] = None
    question: str
    history: Optional[List[CopilotHistoryTurn]] = Field(
        default=None,
        description=(
            "Prior user/assistant text from the same Copilot drawer; "
            "server applies windowing + soft-trim (agents-because–style)."
        ),
    )


class AskReply(BaseModel):
    answer: str
    thoughts: List[str]
    tool_calls: List[Dict[str, Any]]


@router.post("/agent/ask", response_model=AskReply)
async def agent_ask(
    body: AskBody,
    x_pecause_user_id: Optional[str] = Header(default=None),
) -> AskReply:
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="question is empty")
    ctx: Dict[str, Any] = {
        "task_id": body.task_id,
        "channel_id": body.channel_id,
        "run_id": body.run_id,
        "verify_row_id": body.verify_row_id,
        "user_id": x_pecause_user_id,
    }
    if body.history:
        ctx["conversation_history"] = [h.model_dump() for h in body.history]
    reply = copilot_ask(body.question, ctx)
    return AskReply(
        answer=reply.answer,
        thoughts=reply.thoughts,
        tool_calls=[
            {
                "name": c.name,
                "args": c.args,
                "result": c.result,
                "elapsed_ms": c.elapsed_ms,
            }
            for c in reply.tool_calls
        ],
    )


def _ndjson_chunk_bytes(question: str, ctx: Dict[str, Any]) -> Iterator[bytes]:
    for chunk in iter_copilot_ndjson(question, ctx):
        yield chunk.encode("utf-8")


@router.post("/agent/ask/stream")
async def agent_ask_stream(
    body: AskBody,
    x_pecause_user_id: Optional[str] = Header(default=None),
) -> StreamingResponse:
    """Incremental Copilot NDJSON stream: ``thought`` / ``tool_call`` / ``answer``lines, then ``done``."""
    q = body.question.strip()
    if not q:
        raise HTTPException(status_code=400, detail="question is empty")
    ctx = {
        "task_id": body.task_id,
        "channel_id": body.channel_id,
        "run_id": body.run_id,
        "verify_row_id": body.verify_row_id,
        "user_id": x_pecause_user_id,
    }
    if body.history:
        ctx["conversation_history"] = [h.model_dump() for h in body.history]
    return StreamingResponse(
        _ndjson_chunk_bytes(q, ctx),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/agent/drafts/{task_id}")
async def get_drafts(task_id: str) -> Dict[str, Any]:
    return list_drafts(task_id)
