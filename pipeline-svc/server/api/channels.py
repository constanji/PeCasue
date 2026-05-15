"""Per-channel run endpoints.

    POST /tasks/{tid}/channels/{ch}/run        — kick off an async run
    POST /tasks/{tid}/channels/{ch}/confirm     — human audit: mark channel (and latest run) confirmed
    GET  /tasks/{tid}/channels/{ch}            — channel header
    GET  /tasks/{tid}/channels/{ch}/runs       — list of runs (latest first)
    GET  /tasks/{tid}/channels/{ch}/runs/{rid} — single run detail
    DELETE /tasks/{tid}/channels/{ch}/runs/{rid} — remove run + delete artifact dir
"""
from __future__ import annotations

import asyncio
import shutil
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException
from pydantic import BaseModel, Field

from server.core.channel_classifier import get_channel_def
from server.core.orchestrator import _recompute_task_status, run_channel
from server.core.paths import get_channel_run_dir
from server.core.task_repo import TaskRepo
from server.core.pipeline_state import (
    ChannelRunStatus,
    ChannelState,
    AuditEvent,
    StateManager,
    TaskState,
    utc_now_iso,
)

router = APIRouter()


def _catalog_channel_shell(channel_id: str) -> ChannelState:
    """Catalog channel not yet materialised in task state (e.g. zip upload skipped cn_jp)."""
    meta = get_channel_def(channel_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    return ChannelState(
        channel_id=channel_id,
        display_name=meta.display_name,
        entry_type=meta.entry_type,
    )


def _ensure_channel_registered(state: TaskState, channel_id: str) -> ChannelState:
    """Persist a catalog channel into ``state.channels`` so orchestrator can run it."""
    if channel_id in state.channels:
        return state.channels[channel_id]
    ch = _catalog_channel_shell(channel_id)
    state.channels[channel_id] = ch
    StateManager.save_state(state)
    return ch


class RunRequest(BaseModel):
    note: Optional[str] = None
    allocation_phase: Optional[str] = None
    allocation_options: Dict[str, Any] = Field(default_factory=dict)


class RunResponse(BaseModel):
    task_id: str
    channel_id: str
    run_id: Optional[str]
    accepted: bool


@router.get("/tasks/{task_id}/channels/{channel_id}")
async def get_channel(task_id: str, channel_id: str) -> Dict[str, Any]:
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    ch = state.channels.get(channel_id)
    if ch is None:
        ch = _catalog_channel_shell(channel_id)
    return ch.model_dump(mode="json")


@router.get("/tasks/{task_id}/channels/{channel_id}/runs")
async def list_channel_runs(task_id: str, channel_id: str) -> Dict[str, Any]:
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    ch = state.channels.get(channel_id)
    if ch is None:
        _catalog_channel_shell(channel_id)
        return {"task_id": task_id, "channel_id": channel_id, "runs": []}
    runs = [r.model_dump(mode="json") for r in reversed(ch.runs)]
    return {"task_id": task_id, "channel_id": channel_id, "runs": runs}


@router.get("/tasks/{task_id}/channels/{channel_id}/runs/{run_id}")
async def get_channel_run(task_id: str, channel_id: str, run_id: str) -> Dict[str, Any]:
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    if channel_id not in state.channels:
        if get_channel_def(channel_id) is None:
            raise HTTPException(status_code=404, detail="Channel not found")
        raise HTTPException(status_code=404, detail="Run not found")
    ch = state.channels[channel_id]
    for r in ch.runs:
        if r.run_id == run_id:
            return r.model_dump(mode="json")
    raise HTTPException(status_code=404, detail="Run not found")


@router.delete("/tasks/{task_id}/channels/{channel_id}/runs/{run_id}")
async def delete_channel_run(task_id: str, channel_id: str, run_id: str) -> Dict[str, Any]:
    """Remove one run from channel history and delete its ``runs/{run_id}/`` directory."""
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    ch = state.channels.get(channel_id)
    if ch is None:
        raise HTTPException(status_code=404, detail="Channel not found")

    idx = next((i for i, r in enumerate(ch.runs) if r.run_id == run_id), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Run not found")

    task_root = StateManager.get_task_dir(task_id).resolve()
    run_dir = get_channel_run_dir(task_id, channel_id, run_id).resolve()
    try:
        run_dir.relative_to(task_root)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid run directory")

    ch.runs.pop(idx)

    if ch.current_run_id == run_id:
        if ch.runs:
            latest = ch.runs[-1]
            ch.current_run_id = latest.run_id
            ch.status = latest.status
        else:
            ch.current_run_id = None
            ch.status = ChannelRunStatus.PENDING

    state.audit.append(
        AuditEvent(
            actor="machine",
            action="channel.run.delete",
            target=f"{channel_id}/{run_id}",
        )
    )
    StateManager.save_state(state)

    if run_dir.is_dir():
        shutil.rmtree(run_dir, ignore_errors=True)

    return {
        "deleted": True,
        "task_id": task_id,
        "channel_id": channel_id,
        "run_id": run_id,
    }


class CancelResponse(BaseModel):
    task_id: str
    channel_id: str
    cancelled: bool


@router.post("/tasks/{task_id}/channels/{channel_id}/cancel", response_model=CancelResponse)
async def cancel_channel_run(
    task_id: str,
    channel_id: str,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> CancelResponse:
    """Force-cancel a running channel: mark current run as FAILED and channel as FAILED.

    The background parser thread will eventually finish (or timeout), but this
    immediately releases the task from the 'stuck RUNNING' state in the UI.
    """
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    if channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Channel not found")

    ch = state.channels[channel_id]
    actor = x_pecause_user_email or x_pecause_user_id or "human"

    if ch.status != ChannelRunStatus.RUNNING:
        return CancelResponse(task_id=task_id, channel_id=channel_id, cancelled=False)

    # Mark current run as FAILED
    for r in reversed(ch.runs):
        if r.status == ChannelRunStatus.RUNNING:
            r.status = ChannelRunStatus.FAILED
            r.error = "用户手动中断"
            r.finished_at = utc_now_iso()
            break

    ch.status = ChannelRunStatus.FAILED

    state.audit.append(
        AuditEvent(
            actor=actor,
            action="channel.run.cancel",
            target=channel_id,
        )
    )
    _recompute_task_status(state)
    StateManager.save_state(state)

    TaskRepo.append_event(
        task_id,
        "channel.run.cancel",
        channel_id=channel_id,
        payload={"status": ChannelRunStatus.FAILED.value},
    )

    return CancelResponse(task_id=task_id, channel_id=channel_id, cancelled=True)


class ConfirmResponse(BaseModel):
    task_id: str
    channel_id: str
    status: str
    already: bool = False


@router.post("/tasks/{task_id}/channels/{channel_id}/confirm", response_model=ConfirmResponse)
async def confirm_channel(
    task_id: str,
    channel_id: str,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> ConfirmResponse:
    """Human audit: mark channel (and latest non-running run) as ``confirmed``.

    Updates task rollup via ``_recompute_task_status`` so the overview matrix
    reflects an issuance-approved channel (solid green dot in UI).
    """
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")

    _ensure_channel_registered(state, channel_id)
    ch = state.channels[channel_id]
    actor = x_pecause_user_email or x_pecause_user_id or "human"

    if ch.status == ChannelRunStatus.CONFIRMED:
        return ConfirmResponse(
            task_id=task_id, channel_id=channel_id, status="confirmed", already=True
        )

    if ch.status == ChannelRunStatus.RUNNING:
        raise HTTPException(status_code=400, detail="执行进行中，暂不可签发确认")

    if not ch.runs:
        raise HTTPException(status_code=400, detail="尚无运行记录，请先执行后再签发确认")

    if ch.status == ChannelRunStatus.FAILED:
        raise HTTPException(status_code=400, detail="最近运行失败，请先修复并重新执行后再签发确认")

    latest = ch.runs[-1]
    ch.status = ChannelRunStatus.CONFIRMED
    if latest.status != ChannelRunStatus.RUNNING:
        latest.status = ChannelRunStatus.CONFIRMED

    state.audit.append(
        AuditEvent(
            actor=actor,
            action="channel.confirm",
            target=channel_id,
            detail={"run_id": latest.run_id},
        )
    )
    _recompute_task_status(state)
    StateManager.save_state(state)

    TaskRepo.append_event(
        task_id,
        "channel.confirm",
        channel_id=channel_id,
        run_id=latest.run_id,
        payload={"status": ChannelRunStatus.CONFIRMED.value},
    )

    return ConfirmResponse(
        task_id=task_id, channel_id=channel_id, status="confirmed", already=False
    )


@router.post(
    "/tasks/{task_id}/channels/{channel_id}/run", response_model=RunResponse
)
async def trigger_channel_run(
    task_id: str,
    channel_id: str,
    background_tasks: BackgroundTasks,
    payload: Optional[RunRequest] = None,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> RunResponse:
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    _ensure_channel_registered(state, channel_id)

    actor = x_pecause_user_email or x_pecause_user_id or "machine"
    run_payload: Dict[str, Any] = {}
    if payload:
        run_payload = payload.model_dump(exclude_none=True)

    def _runner() -> None:
        # Use a fresh event loop in the background thread.
        asyncio.run(
            run_channel(task_id, channel_id, actor=actor, run_payload=run_payload or None)
        )

    background_tasks.add_task(_runner)
    return RunResponse(
        task_id=task_id, channel_id=channel_id, run_id=None, accepted=True
    )
