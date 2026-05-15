"""Compare orchestrator — turns a ``CompareRequest`` into persisted artefacts.

Sources can be:
    1. A run output file (``task_id`` / ``channel_id`` / ``run_id`` / ``name``)
    2. A source file (``task_id`` / ``channel_id`` / ``rel_path``)
    3. An external upload that has been staged into ``data/tasks/{tid}/compare/{cid}/uploads``
"""
from __future__ import annotations

import json
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from server.compare import tools as ctools
from server.core.paths import (
    get_channel_run_dir,
    get_compare_dir,
    get_task_extracted_dir,
    resolve_run_artifact_path,
)
from server.core.task_db import get_connection
from server.core.task_logger import task_log
from server.core.task_repo import TaskRepo


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CompareSource:
    kind: str  # "run_output" | "source_file" | "upload"
    path: Path
    label: str


def _resolve_source(task_id: str, payload: Dict[str, Any]) -> CompareSource:
    kind = payload.get("kind") or "run_output"
    if kind == "run_output":
        ch = payload["channel_id"]
        rid = payload["run_id"]
        name = payload["name"]
        run_dir = get_channel_run_dir(task_id, ch, rid)
        resolved = resolve_run_artifact_path(run_dir, name)
        if resolved is None:
            raise ValueError(f"run output not found: {ch}/{rid}/{name}")
        p = resolved
        return CompareSource(
            kind=kind, path=p, label=f"{ch}/{rid[:8]}/{name}"
        )
    if kind == "source_file":
        ch = payload["channel_id"]
        rel = payload["rel_path"]
        p = get_task_extracted_dir(task_id, ch) / rel
        return CompareSource(kind=kind, path=p, label=f"{ch}/source/{rel}")
    if kind == "upload":
        # Caller must have already staged it under compare_dir/uploads/<name>
        p = Path(payload["staged_path"])
        return CompareSource(kind=kind, path=p, label=f"upload/{p.name}")
    raise ValueError(f"unknown source kind: {kind}")


def _load_dataframe(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(p)
    suf = p.suffix.lower()
    if suf in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(p)
    if suf == ".csv":
        return pd.read_csv(p)
    if suf in {".tsv", ".txt"}:
        return pd.read_csv(p, sep="\t")
    if suf == ".json":
        return pd.read_json(p)
    # Fallback: try CSV
    return pd.read_csv(p)


def run_compare(
    *,
    task_id: str,
    left: Dict[str, Any],
    right: Dict[str, Any],
    key_cols: List[str],
    compare_cols: Optional[List[str]] = None,
    column_mapping: Optional[Dict[str, str]] = None,
    numeric_tol: float = 0.01,
    normalize_strings: bool = True,
    note: Optional[str] = None,
    actor: Optional[str] = None,
) -> Dict[str, Any]:
    """End-to-end compare. Persists ``meta.json`` + ``report.json`` +
    ``report.xlsx`` under ``data/tasks/{tid}/compare/{cid}/``."""
    cid = uuid.uuid4().hex[:12]
    out_dir = get_compare_dir(task_id, cid)
    out_dir.mkdir(parents=True, exist_ok=True)
    task_log(task_id, f"[compare] start {cid}", channel="compare")

    t0 = time.perf_counter()
    src_l = _resolve_source(task_id, left)
    src_r = _resolve_source(task_id, right)
    df_l = _load_dataframe(src_l.path)
    df_r = _load_dataframe(src_r.path)

    aligned_l, aligned_r, align_diag = ctools.align_columns(df_l, df_r, column_mapping)
    matches = ctools.match_rows(aligned_l, aligned_r, key_cols)
    matched_pairs = matches["matched"]

    diff_df = ctools.compute_cell_diff(
        aligned_l,
        aligned_r,
        matched_pairs,
        compare_cols=compare_cols,
        numeric_tol=numeric_tol,
        normalize_strings=normalize_strings,
    )
    summary = ctools.summarize_diff(
        diff_df,
        only_left=matches["only_left"],
        only_right=matches["only_right"],
        matched_count=len(matched_pairs),
    )

    only_left_df = (
        aligned_l.iloc[matches["only_left"]].reset_index(drop=True)
        if matches["only_left"]
        else aligned_l.head(0)
    )
    only_right_df = (
        aligned_r.iloc[matches["only_right"]].reset_index(drop=True)
        if matches["only_right"]
        else aligned_r.head(0)
    )

    meta = {
        "compare_id": cid,
        "task_id": task_id,
        "left": {"label": src_l.label, "kind": src_l.kind, "rows": int(len(df_l))},
        "right": {"label": src_r.label, "kind": src_r.kind, "rows": int(len(df_r))},
        "key_cols": key_cols,
        "compare_cols": compare_cols,
        "column_mapping": column_mapping or {},
        "tolerances": {"numeric": numeric_tol, "normalize_strings": normalize_strings},
        "alignment": align_diag,
        "summary": summary,
        "note": note,
        "created_by": actor,
        "created_at": _utc_now_iso(),
        "duration_ms": round((time.perf_counter() - t0) * 1000, 1),
    }
    (out_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    report_payload = {
        "summary": summary,
        "alignment": align_diag,
        "diff_rows": diff_df.head(500).to_dict(orient="records"),
        "diff_total": int(len(diff_df)),
        "only_left_preview": only_left_df.head(50).to_dict(orient="records"),
        "only_right_preview": only_right_df.head(50).to_dict(orient="records"),
    }
    (out_dir / "report.json").write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    ctools.render_report(
        out_dir / "report.xlsx",
        summary=summary,
        diff_df=diff_df,
        only_left_df=only_left_df,
        only_right_df=only_right_df,
        meta={
            "compare_id": cid,
            "task_id": task_id,
            "left": src_l.label,
            "right": src_r.label,
            "created_at": meta["created_at"],
        },
    )

    with get_connection() as conn:
        conn.execute(
            """INSERT INTO compare_runs
                 (compare_id, task_id, left_ref, right_ref, status,
                  report_path, summary_json, created_by, created_at, completed_at)
               VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?)""",
            (
                cid,
                task_id,
                src_l.label,
                src_r.label,
                str(out_dir / "report.xlsx"),
                json.dumps(summary, ensure_ascii=False),
                actor,
                meta["created_at"],
                _utc_now_iso(),
            ),
        )

    TaskRepo.append_event(
        task_id,
        "compare.completed",
        payload={
            "compare_id": cid,
            "diff_cells": summary["diff_cells"],
            "matched": summary["matched_rows"],
            "only_left": summary["only_left_rows"],
            "only_right": summary["only_right_rows"],
        },
    )
    task_log(
        task_id,
        f"[compare] done {cid} matched={summary['matched_rows']} diff_cells={summary['diff_cells']}",
        channel="compare",
    )
    return meta


def list_compares(task_id: Optional[str] = None) -> List[Dict[str, Any]]:
    sql = (
        "SELECT compare_id, task_id, left_ref, right_ref, status, "
        "       report_path, summary_json, created_by, created_at, completed_at "
        "FROM compare_runs"
    )
    params: tuple = ()
    if task_id:
        sql += " WHERE task_id = ?"
        params = (task_id,)
    sql += " ORDER BY created_at DESC LIMIT 200"
    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        try:
            d["summary"] = json.loads(d.pop("summary_json") or "{}")
        except Exception:
            d["summary"] = {}
        out.append(d)
    return out


def get_compare_meta(task_id: str, compare_id: str) -> Optional[Dict[str, Any]]:
    p = get_compare_dir(task_id, compare_id) / "meta.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def get_compare_report(task_id: str, compare_id: str) -> Optional[Dict[str, Any]]:
    p = get_compare_dir(task_id, compare_id) / "report.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))
