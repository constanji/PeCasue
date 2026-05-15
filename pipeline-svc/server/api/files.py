"""Run-output download + source file replace.

    GET  /tasks/{tid}/channels/{ch}/runs/{rid}/files/{name}
    GET  /tasks/{tid}/channels/{ch}/files                      — list extracted source files
    POST /tasks/{tid}/channels/{ch}/files/replace              — replace one extracted source file

After a source replace we mark **every** existing channel run as `is_dirty=true`
(advisory re-run) and append a `file.replaced` lifecycle event.
"""
from __future__ import annotations

import hashlib
import shutil
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Header, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from server.core.channel_prescan import scan_bill_prescan, scan_own_flow_prescan
from server.core.paths import (
    get_channel_run_dir,
    get_task_dir,
    get_task_extracted_dir,
    is_extracted_rel_path_parse_candidate,
    resolve_run_artifact_path,
)
from server.core.pipeline_state import (
    AuditEvent,
    ChannelRunHistory,
    ChannelRunStatus,
    StateManager,
    TaskState,
)
from server.core.task_logger import task_log
from server.core.task_repo import TaskRepo

router = APIRouter()


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_state_or_404(task_id: str) -> TaskState:
    state = StateManager.load_state(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return state


def _safe_join(root: Path, name: str) -> Path:
    p = (root / name).resolve()
    root_resolved = root.resolve()
    if root_resolved not in p.parents and p != root_resolved:
        raise HTTPException(status_code=400, detail="Invalid path")
    return p


@router.get("/tasks/{task_id}/channels/{channel_id}/runs/{run_id}/files/{name}")
async def download_run_file(
    task_id: str, channel_id: str, run_id: str, name: str
):
    state = _get_state_or_404(task_id)
    if channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Channel not found")
    ch = state.channels[channel_id]
    found = next((r for r in ch.runs if r.run_id == run_id), None)
    if found is None:
        raise HTTPException(status_code=404, detail="Run not found")

    run_dir = get_channel_run_dir(task_id, channel_id, run_id)
    file_path = resolve_run_artifact_path(run_dir, name)
    if file_path is None:
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path, filename=file_path.name)


@router.post("/tasks/{task_id}/channels/{channel_id}/runs/{run_id}/files/replace")
async def replace_run_output_file(
    task_id: str,
    channel_id: str,
    run_id: str,
    name: str = Form(...),
    file: UploadFile = File(...),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Replace one run artifact on disk and refresh ``output_files`` metadata (OnlyOffice 保存)."""
    if (
        not name
        or "\\" in name
        or ".." in Path(name.replace("\\", "/")).parts
        or Path(name.replace("\\", "/")).is_absolute()
    ):
        raise HTTPException(status_code=400, detail="Invalid file name")

    state = _get_state_or_404(task_id)
    if channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Channel not found")
    ch = state.channels[channel_id]
    found: ChannelRunHistory | None = next(
        (r for r in ch.runs if r.run_id == run_id), None
    )
    if found is None:
        raise HTTPException(status_code=404, detail="Run not found")

    allowed = {fe.name for fe in found.output_files}
    if name not in allowed:
        raise HTTPException(
            status_code=400,
            detail="File is not a registered output for this run",
        )

    run_dir = get_channel_run_dir(task_id, channel_id, run_id)
    target = resolve_run_artifact_path(run_dir, name)
    if target is None or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    old_sha = _sha256(target) if target.exists() and target.is_file() else None

    backup = target.with_suffix(target.suffix + f".bak.{uuid.uuid4().hex[:6]}")
    if target.exists() and target.is_file():
        shutil.copy2(target, backup)

    with open(target, "wb") as f:
        shutil.copyfileobj(file.file, f)

    new_sha = _sha256(target)
    new_size = target.stat().st_size

    for fe in found.output_files:
        if fe.name == name:
            fe.sha256 = new_sha
            fe.size = new_size
            break

    if found.status in (
        ChannelRunStatus.VERIFIED,
        ChannelRunStatus.VERIFIED_WITH_WARNING,
        ChannelRunStatus.PREVIEW_READY,
        ChannelRunStatus.CONFIRMED,
    ):
        found.status = ChannelRunStatus.EDITED

    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    state.audit.append(
        AuditEvent(
            actor=actor,
            action="run.output.replace",
            target=f"{channel_id}/{run_id}/{name}",
            detail={
                "old_sha256": old_sha,
                "new_sha256": new_sha,
                "backup_path": str(backup.name),
                "size": new_size,
            },
        )
    )
    StateManager.save_state(state)
    task_log(
        task_id,
        f"Run output replaced: {channel_id}/{run_id}/{name} (sha {new_sha[:8]})",
        channel=channel_id,
        level="WARNING",
    )
    TaskRepo.append_event(
        task_id,
        "run.output.replaced",
        channel_id=channel_id,
        payload={
            "run_id": run_id,
            "name": name,
            "old_sha256": old_sha,
            "new_sha256": new_sha,
            "actor": actor,
        },
    )

    return JSONResponse(
        {
            "task_id": task_id,
            "channel_id": channel_id,
            "run_id": run_id,
            "name": name,
            "old_sha256": old_sha,
            "new_sha256": new_sha,
            "backup_path": backup.name,
            "size": new_size,
            "run_status": found.status.value,
        }
    )


@router.get("/tasks/{task_id}/channels/{channel_id}/files")
async def list_channel_source_files(task_id: str, channel_id: str):
    state = _get_state_or_404(task_id)
    if channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Channel not found")
    extracted = get_task_extracted_dir(task_id, channel_id)
    if not extracted.exists():
        return {"task_id": task_id, "channel_id": channel_id, "files": []}
    files = []
    for p in sorted(extracted.rglob("*")):
        rel = p.relative_to(extracted).as_posix()
        if p.is_file() and is_extracted_rel_path_parse_candidate(rel):
            files.append(
                {
                    "rel_path": rel,
                    "size": p.stat().st_size,
                    "sha256": _sha256(p),
                }
            )
    return {"task_id": task_id, "channel_id": channel_id, "files": files}


@router.get("/tasks/{task_id}/channels/{channel_id}/prescan")
async def channel_prescan_endpoint(task_id: str, channel_id: str) -> dict:
    """Bill folder × bank_key counts + own-flow ``discovery.scan_inventory`` mirror."""
    state = StateManager.load_state(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found")
    extracted = get_task_extracted_dir(task_id, channel_id)
    if channel_id == "bill":
        return {"task_id": task_id, "channel_id": channel_id, **scan_bill_prescan(extracted)}
    if channel_id == "own_flow":
        return {"task_id": task_id, "channel_id": channel_id, **scan_own_flow_prescan(extracted)}
    return {
        "task_id": task_id,
        "channel_id": channel_id,
        "kind": "unsupported",
        "message": "当前渠道暂无预扫（已实现：bill / own_flow）。",
    }


@router.get("/tasks/{task_id}/channels/{channel_id}/files/download")
async def download_channel_source_file(
    task_id: str, channel_id: str, rel_path: str
):
    state = _get_state_or_404(task_id)
    if channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Channel not found")
    extracted = get_task_extracted_dir(task_id, channel_id)
    file_path = _safe_join(extracted, rel_path)
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=file_path.name)


@router.get("/tasks/{task_id}/channels/allocation_base/uploads/{name}")
async def download_allocation_merge_base_upload(task_id: str, name: str):
    """下载「分摊基数合并表」直接上传目录中的 .xlsx（成本出摊输入用）。"""
    _get_state_or_404(task_id)
    upload_dir = get_task_dir(task_id) / "channels" / "allocation_base" / "uploads"
    if not upload_dir.is_dir():
        raise HTTPException(status_code=404, detail="Upload directory not found")
    file_path = _safe_join(upload_dir, name)
    if not file_path.is_file() or not name.lower().endswith(".xlsx"):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=file_path.name)


@router.post("/tasks/{task_id}/channels/{channel_id}/files/replace")
async def replace_channel_source_file(
    task_id: str,
    channel_id: str,
    rel_path: str = Form(...),
    file: UploadFile = File(...),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    state = _get_state_or_404(task_id)
    if channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Channel not found")
    ch = state.channels[channel_id]
    extracted = get_task_extracted_dir(task_id, channel_id)
    target = _safe_join(extracted, rel_path)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="Target file not found")

    old_sha = _sha256(target)
    backup = target.with_suffix(target.suffix + f".bak.{uuid.uuid4().hex[:6]}")
    shutil.copy2(target, backup)
    with open(target, "wb") as f:
        shutil.copyfileobj(file.file, f)
    new_sha = _sha256(target)

    # Mark every existing run as dirty since a source file changed.
    for r in ch.runs:
        r.is_dirty = True

    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    state.audit.append(
        AuditEvent(
            actor=actor,
            action="file.replace",
            target=f"{channel_id}/{rel_path}",
            detail={
                "old_sha256": old_sha,
                "new_sha256": new_sha,
                "backup_path": str(backup),
                "size": target.stat().st_size,
            },
        )
    )
    StateManager.save_state(state)
    task_log(
        task_id,
        f"Source file replaced: {rel_path} (sha {old_sha[:8]} -> {new_sha[:8]})",
        channel=channel_id,
        level="WARNING",
    )
    TaskRepo.append_event(
        task_id,
        "file.replaced",
        channel_id=channel_id,
        payload={
            "rel_path": rel_path,
            "old_sha256": old_sha,
            "new_sha256": new_sha,
            "actor": actor,
        },
    )

    return JSONResponse(
        {
            "task_id": task_id,
            "channel_id": channel_id,
            "rel_path": rel_path,
            "old_sha256": old_sha,
            "new_sha256": new_sha,
            "backup_path": str(backup.relative_to(extracted)) if backup.is_relative_to(extracted) else str(backup),
            "is_changed": old_sha != new_sha,
            "advisory": "请重新执行该渠道以使用新文件",
        }
    )
