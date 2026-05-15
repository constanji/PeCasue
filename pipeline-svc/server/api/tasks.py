"""Task list / detail / timeline / delete.

Phase 1: read-only over SQLite + state.json. Creation lives in `upload.py`
(Phase 2). Pause/Resume/Terminate are deferred to Phase 3 once the orchestrator
exists.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.core.channel_classifier import get_channel_def, list_channel_defs
from server.core.pipeline_state import StateManager, TaskState
from server.core.task_repo import TaskRepo

router = APIRouter()


class TaskListResponse(BaseModel):
    tasks: list[Dict[str, Any]]


class TaskDeleteResponse(BaseModel):
    deleted: bool
    task_id: str


@router.get("/tasks", response_model=TaskListResponse)
async def list_tasks() -> TaskListResponse:
    return TaskListResponse(tasks=TaskRepo.list_tasks())


@router.get("/tasks/{task_id}")
async def get_task(task_id: str) -> Dict[str, Any]:
    summary = TaskRepo.get_task(task_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Task not found")

    state = StateManager.load_state(task_id)
    return {
        "summary": summary,
        "state": state.model_dump(mode="json") if state else None,
    }


@router.get("/tasks/{task_id}/timeline")
async def get_task_timeline(task_id: str, limit: int = 200) -> Dict[str, Any]:
    if not TaskRepo.get_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
    events = TaskRepo.get_timeline(task_id, limit=limit)
    return {"task_id": task_id, "events": events}


def _ordered_channel_ids(state: TaskState) -> List[str]:
    catalog = [d.channel_id for d in list_channel_defs()]
    seen = set()
    out: List[str] = []
    for cid in catalog:
        if cid in state.channels:
            out.append(cid)
            seen.add(cid)
    for cid in sorted(state.channels.keys()):
        if cid not in seen:
            out.append(cid)
    return out


def _verify_row_maps(
    verify_summary: Optional[Dict[str, Any]],
) -> Tuple[Optional[Any], Dict[str, Any]]:
    """run-level row_count hint (from metrics) + best-effort per-output-filename counts."""
    if not verify_summary:
        return None, {}
    metrics = verify_summary.get("metrics") or {}
    run_total = metrics.get("row_count")
    per: Dict[str, Any] = {}
    for row in verify_summary.get("rows") or []:
        fr = row.get("file_ref")
        if not fr:
            continue
        detail = row.get("detail") or {}
        rc = detail.get("row_count")
        if rc is None:
            continue
        base = str(fr).replace("\\", "/").split("/")[-1]
        per[base] = rc
    return run_total, per


def _artifact_row_count(
    filename: str,
    n_outputs: int,
    run_total: Any,
    per: Dict[str, Any],
) -> Any:
    if filename in per:
        return per[filename]
    if n_outputs == 1 and run_total is not None:
        return run_total
    return None


@router.get("/tasks/{task_id}/final-merge-inventory")
async def get_final_merge_inventory(task_id: str) -> Dict[str, Any]:
    """跨渠道汇总最近一次成功跑批产出（最终合并视图，只读）。"""
    state = StateManager.load_state(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")

    channels_out: List[Dict[str, Any]] = []
    for channel_id in _ordered_channel_ids(state):
        ch = state.channels[channel_id]
        meta = get_channel_def(channel_id)
        display_name = meta.display_name if meta else ch.display_name
        runs = ch.runs
        latest = runs[-1] if runs else None
        latest_run_payload: Optional[Dict[str, Any]] = None
        artifacts: List[Dict[str, Any]] = []
        if latest is not None:
            vs = latest.verify_summary
            run_total, per_file = _verify_row_maps(vs if isinstance(vs, dict) else None)
            outs = list(latest.output_files)
            n_out = len(outs)
            for fe in outs:
                name = fe.name
                artifacts.append(
                    {
                        "name": name,
                        "size": fe.size,
                        "created_at": fe.created_at,
                        "role": fe.role,
                        "sha256": fe.sha256,
                        "row_count": _artifact_row_count(
                            name, n_out, run_total, per_file
                        ),
                        "run_id": latest.run_id,
                    }
                )
            latest_run_payload = {
                "run_id": latest.run_id,
                "status": str(latest.status.value)
                if hasattr(latest.status, "value")
                else str(latest.status),
                "started_at": latest.started_at,
                "finished_at": latest.finished_at,
                "duration_seconds": latest.duration_seconds,
            }
        channels_out.append(
            {
                "channel_id": channel_id,
                "display_name": display_name,
                "entry_type": ch.entry_type,
                "channel_status": str(ch.status.value)
                if hasattr(ch.status, "value")
                else str(ch.status),
                "latest_run": latest_run_payload,
                "artifacts": artifacts,
            }
        )

    return {"task_id": task_id, "channels": channels_out}
