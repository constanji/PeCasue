"""XLSX / CSV preview — light-weight server-rendered tables.

The frontend calls ``GET /preview/run-file`` to render the bottom-of-detail
preview pane. Returns at most ``MAX_ROWS`` × ``MAX_COLS`` cells per sheet to
keep responses small (the full file is still downloadable via
``/tasks/.../files/{name}``). Supports xlsx, xls, xlsm, csv.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, List

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.core.paths import get_channel_run_dir, resolve_run_artifact_path
from server.core.pipeline_state import StateManager

router = APIRouter()

MAX_ROWS = 200
MAX_COLS = 40
MAX_SHEETS = 8


class PreviewSheet(BaseModel):
    name: str
    headers: List[str]
    rows: List[List[Any]]
    total_rows: int
    total_cols: int
    truncated_rows: bool
    truncated_cols: bool


class PreviewResponse(BaseModel):
    task_id: str
    channel_id: str
    run_id: str
    filename: str
    kind: str  # xlsx | csv | unsupported
    sheets: List[PreviewSheet]
    error: str | None = None


def _serialize_value(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, float):
        if pd.isna(v):
            return ""
        # Avoid NaN / Infinity in JSON.
        if v != v or v == float("inf") or v == float("-inf"):  # noqa: PLR0124
            return ""
        return v
    if isinstance(v, (pd.Timestamp,)):
        return v.isoformat()
    if isinstance(v, (int, str, bool)):
        return v
    return str(v)


def _df_to_sheet(name: str, df: pd.DataFrame) -> PreviewSheet:
    total_rows, total_cols = df.shape
    truncated_cols = total_cols > MAX_COLS
    truncated_rows = total_rows > MAX_ROWS
    df = df.iloc[:MAX_ROWS, :MAX_COLS]
    headers = [str(c) for c in df.columns]
    rows: List[List[Any]] = []
    for _, row in df.iterrows():
        rows.append([_serialize_value(row[c]) for c in df.columns])
    return PreviewSheet(
        name=name,
        headers=headers,
        rows=rows,
        total_rows=total_rows,
        total_cols=total_cols,
        truncated_rows=truncated_rows,
        truncated_cols=truncated_cols,
    )


@router.get("/preview/run-file", response_model=PreviewResponse)
async def preview_run_file(
    task_id: str, channel_id: str, run_id: str, filename: str
) -> PreviewResponse:
    state = StateManager.load_state(task_id)
    if state is None or channel_id not in state.channels:
        raise HTTPException(status_code=404, detail="Task or channel not found")
    if not any(r.run_id == run_id for r in state.channels[channel_id].runs):
        raise HTTPException(status_code=404, detail="Run not found")

    run_dir = get_channel_run_dir(task_id, channel_id, run_id)
    file_path = resolve_run_artifact_path(run_dir, filename)
    if file_path is None:
        raise HTTPException(status_code=404, detail="File not found")

    suffix = file_path.suffix.lower()
    sheets: List[PreviewSheet] = []
    err: str | None = None
    kind = "unsupported"
    try:
        if suffix in (".xlsx", ".xls", ".xlsm"):
            kind = "xlsx"
            engine = "openpyxl" if suffix == ".xlsx" else None
            xls = pd.ExcelFile(file_path, engine=engine)
            for sn in xls.sheet_names[:MAX_SHEETS]:
                try:
                    sheets.append(
                        _df_to_sheet(sn, xls.parse(sn, dtype=object))
                    )
                except Exception as exc:  # noqa: BLE001
                    sheets.append(
                        PreviewSheet(
                            name=sn,
                            headers=["error"],
                            rows=[[f"sheet 解析失败: {exc}"]],
                            total_rows=1,
                            total_cols=1,
                            truncated_rows=False,
                            truncated_cols=False,
                        )
                    )
        elif suffix == ".csv":
            kind = "csv"
            try:
                df = pd.read_csv(file_path, encoding="utf-8-sig", dtype=object, nrows=MAX_ROWS + 1, on_bad_lines="skip")
            except Exception:
                df = pd.read_csv(file_path, encoding="gbk", dtype=object, nrows=MAX_ROWS + 1, on_bad_lines="skip")
            sheets.append(_df_to_sheet("csv", df))
        else:
            err = f"unsupported file type: {suffix or 'no-extension'}"
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"

    return PreviewResponse(
        task_id=task_id,
        channel_id=channel_id,
        run_id=run_id,
        filename=filename,
        kind=kind,
        sheets=sheets,
        error=err,
    )
