"""Compare endpoints — POST /compare, GET /compare/..., GET /compare/{cid}/report."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from server.compare.runner import (
    get_compare_meta,
    get_compare_report,
    list_compares,
    run_compare,
)
from server.core.paths import get_compare_dir

router = APIRouter()


class CompareSourceBody(BaseModel):
    kind: str = Field(default="run_output")
    channel_id: Optional[str] = None
    run_id: Optional[str] = None
    name: Optional[str] = None
    rel_path: Optional[str] = None
    staged_path: Optional[str] = None


class CompareCreateBody(BaseModel):
    task_id: str
    left: CompareSourceBody
    right: CompareSourceBody
    key_cols: List[str]
    compare_cols: Optional[List[str]] = None
    column_mapping: Optional[Dict[str, str]] = None
    numeric_tol: float = 0.01
    normalize_strings: bool = True
    note: Optional[str] = None


@router.post("/compare")
def compare_create(
    body: CompareCreateBody,
    x_pecause_user_id: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    try:
        meta = run_compare(
            task_id=body.task_id,
            left=body.left.model_dump(),
            right=body.right.model_dump(),
            key_cols=body.key_cols,
            compare_cols=body.compare_cols,
            column_mapping=body.column_mapping,
            numeric_tol=body.numeric_tol,
            normalize_strings=body.normalize_strings,
            note=body.note,
            actor=x_pecause_user_id,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"source not found: {exc}") from exc
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing field: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return meta


@router.get("/compare")
def compare_list(task_id: Optional[str] = None) -> Dict[str, Any]:
    return {"compares": list_compares(task_id)}


@router.get("/compare/{compare_id}")
def compare_get(compare_id: str, task_id: str) -> Dict[str, Any]:
    meta = get_compare_meta(task_id, compare_id)
    if not meta:
        raise HTTPException(status_code=404, detail="compare not found")
    return meta


@router.get("/compare/{compare_id}/report")
def compare_report(compare_id: str, task_id: str) -> Dict[str, Any]:
    payload = get_compare_report(task_id, compare_id)
    if not payload:
        raise HTTPException(status_code=404, detail="report not found")
    return payload


@router.get("/compare/{compare_id}/download")
def compare_download(compare_id: str, task_id: str):
    p = get_compare_dir(task_id, compare_id) / "report.xlsx"
    if not p.exists():
        raise HTTPException(status_code=404, detail="report.xlsx not found")
    return FileResponse(
        path=str(p),
        filename=f"compare_{compare_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.post("/compare/upload")
async def compare_upload(
    task_id: str = Form(...),
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    """Stage an external file under the task scope so it can be referenced by
    a subsequent ``POST /compare`` call as a source with kind=upload."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="missing filename")
    staging = get_compare_dir(task_id, "_uploads")
    staging.mkdir(parents=True, exist_ok=True)
    out_path = staging / file.filename
    content = await file.read()
    out_path.write_bytes(content)
    return {
        "task_id": task_id,
        "staged_path": str(out_path),
        "name": file.filename,
        "size": len(content),
    }
