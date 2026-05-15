"""Server-Sent Events streams.

    GET /tasks/{tid}/stream                          — task-level state updates
    GET /tasks/{tid}/channels/{ch}/logs/stream       — channel log tail
    GET /tasks/{tid}/logs/stream                     — task log tail (all channels)

Polling-based (1s) state diff and 0.7s log tail. Mirrors pingpong-master's
approach so existing client SSE plumbing can be re-used.
"""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from server.core.paths import get_channel_log_path, get_task_log_path
from server.core.pipeline_state import StateManager
from server.core.task_repo import TaskRepo

router = APIRouter()


@router.get("/tasks/{task_id}/stream")
async def task_state_stream(task_id: str, request: Request) -> StreamingResponse:
    if not TaskRepo.get_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")

    async def event_gen():
        last_hash = None
        try:
            while True:
                if await request.is_disconnected():
                    break
                state = StateManager.load_state(task_id)
                if state is None:
                    yield 'event: error\ndata: {"message":"task not found"}\n\n'
                    break
                payload = state.model_dump_json()
                cur_hash = hash(payload)
                if cur_hash != last_hash:
                    last_hash = cur_hash
                    yield f"event: state\ndata: {payload}\n\n"
                if state.current_step in ("COMPLETED", "FAILED"):
                    # send a final close marker, then exit
                    yield 'event: close\ndata: {"reason":"terminal"}\n\n'
                    break
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@router.get("/tasks/{task_id}/logs/stream")
async def task_log_stream(task_id: str, request: Request) -> StreamingResponse:
    if not TaskRepo.get_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
    log_path = get_task_log_path(task_id)

    async def event_gen():
        offset = 0
        # initial heartbeat
        yield ": connected\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                if log_path.exists():
                    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(offset)
                        chunk = f.read()
                        offset = f.tell()
                    if chunk:
                        for line in chunk.splitlines():
                            payload = json.dumps({"line": line}, ensure_ascii=False)
                            yield f"event: log\ndata: {payload}\n\n"
                await asyncio.sleep(0.7)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(event_gen(), media_type="text/event-stream")


@router.get("/tasks/{task_id}/channels/{channel_id}/logs/stream")
async def channel_log_stream(
    task_id: str, channel_id: str, request: Request
) -> StreamingResponse:
    if not TaskRepo.get_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
    log_path = get_channel_log_path(task_id, channel_id)

    async def event_gen():
        offset = 0
        yield ": connected\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                if log_path.exists():
                    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(offset)
                        chunk = f.read()
                        offset = f.tell()
                    if chunk:
                        for line in chunk.splitlines():
                            payload = json.dumps({"line": line}, ensure_ascii=False)
                            yield f"event: log\ndata: {payload}\n\n"
                await asyncio.sleep(0.7)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(event_gen(), media_type="text/event-stream")
