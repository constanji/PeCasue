"""Compare toolbox — pure functions invoked by ``CompareAgent`` and the
``POST /compare`` orchestrator. They map 1:1 onto the toolset in the plan §4.4
so they can be exposed as AgentScope tools later.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ---------- 1. align_columns ----------


def align_columns(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    mapping: Optional[Dict[str, str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Rename columns in ``df_b`` so they line up with ``df_a``.

    ``mapping`` is ``{ a_column: b_column }``; columns missing from either side
    are reported in the diagnostic dict. When ``mapping`` is ``None`` we treat
    columns of the same name as already aligned and surface the rest.
    """
    a_cols = list(df_a.columns)
    b_cols = list(df_b.columns)

    rename: Dict[str, str] = {}
    if mapping:
        for a, b in mapping.items():
            if b in df_b.columns and a in df_a.columns:
                rename[b] = a

    aligned_b = df_b.rename(columns=rename)
    common = [c for c in a_cols if c in aligned_b.columns]
    diag = {
        "common_columns": common,
        "left_only": [c for c in a_cols if c not in aligned_b.columns],
        "right_only": [c for c in aligned_b.columns if c not in a_cols],
        "rename_applied": rename,
    }
    return df_a[common], aligned_b[common], diag


# ---------- 2. match_rows ----------


def _normalise_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v).strip().lower()


def match_rows(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_cols: List[str],
    *,
    fuzzy: bool = False,
) -> Dict[str, Any]:
    """Inner/left/right match by ``key_cols``. Returns a structured payload."""
    if not key_cols:
        raise ValueError("key_cols is required")

    def _key_frame(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for c in key_cols:
            if c not in out.columns:
                out[c] = ""
            out[f"__k_{c}"] = out[c].map(_normalise_str)
        return out

    a = _key_frame(df_a).reset_index().rename(columns={"index": "idx_a"})
    b = _key_frame(df_b).reset_index().rename(columns={"index": "idx_b"})

    join_keys = [f"__k_{c}" for c in key_cols]
    merged = pd.merge(
        a[["idx_a", *join_keys]],
        b[["idx_b", *join_keys]],
        on=join_keys,
        how="outer",
        indicator=True,
    )

    matched = merged[merged["_merge"] == "both"][["idx_a", "idx_b"]]
    only_left = merged[merged["_merge"] == "left_only"]["idx_a"]
    only_right = merged[merged["_merge"] == "right_only"]["idx_b"]

    return {
        "matched": [
            {"left": int(left), "right": int(right)}
            for left, right in matched.itertuples(index=False, name=None)
        ],
        "only_left": [int(v) for v in only_left.dropna().tolist()],
        "only_right": [int(v) for v in only_right.dropna().tolist()],
        "fuzzy": fuzzy,
        "key_cols": key_cols,
    }


# ---------- 3. compute_cell_diff ----------


def _cell_equal(a: Any, b: Any, *, numeric_tol: float, normalize_strings: bool) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        try:
            return abs(float(a) - float(b)) <= numeric_tol
        except Exception:  # noqa: BLE001
            return a == b
    if normalize_strings:
        return _normalise_str(a) == _normalise_str(b)
    return a == b


def compute_cell_diff(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    matched_pairs: List[Dict[str, int]],
    compare_cols: Optional[List[str]] = None,
    *,
    numeric_tol: float = 0.01,
    normalize_strings: bool = True,
) -> pd.DataFrame:
    cols = compare_cols or [c for c in df_a.columns if c in df_b.columns]
    rows: List[Dict[str, Any]] = []
    for pair in matched_pairs:
        ia, ib = int(pair["left"]), int(pair["right"])
        if ia >= len(df_a) or ib >= len(df_b):
            continue
        ra = df_a.iloc[ia]
        rb = df_b.iloc[ib]
        for col in cols:
            va = ra.get(col)
            vb = rb.get(col)
            if not _cell_equal(va, vb, numeric_tol=numeric_tol, normalize_strings=normalize_strings):
                rows.append(
                    {
                        "left_index": ia,
                        "right_index": ib,
                        "column": col,
                        "left_value": None if pd.isna(va) else va,
                        "right_value": None if pd.isna(vb) else vb,
                    }
                )
    return pd.DataFrame(rows, columns=["left_index", "right_index", "column", "left_value", "right_value"])


# ---------- 4. summarize_diff ----------


def summarize_diff(
    diff_df: pd.DataFrame,
    *,
    only_left: List[int],
    only_right: List[int],
    matched_count: int,
) -> Dict[str, Any]:
    by_col: Dict[str, int] = {}
    if not diff_df.empty:
        by_col = diff_df.groupby("column").size().sort_values(ascending=False).to_dict()
    return {
        "matched_rows": matched_count,
        "only_left_rows": len(only_left),
        "only_right_rows": len(only_right),
        "diff_cells": int(len(diff_df)),
        "by_column": {k: int(v) for k, v in by_col.items()},
    }


# ---------- 5. render_report ----------


def render_report(
    out_xlsx_path,
    *,
    summary: Dict[str, Any],
    diff_df: pd.DataFrame,
    only_left_df: pd.DataFrame,
    only_right_df: pd.DataFrame,
    meta: Dict[str, Any],
) -> None:
    """Write a multi-sheet Excel report. The structure mirrors what most
    finance teams expect: a "概览" sheet, a "差异单元格" sheet, then per-side
    "仅左 / 仅右" rows."""
    with pd.ExcelWriter(str(out_xlsx_path), engine="openpyxl") as xw:
        meta_df = pd.DataFrame(
            [
                {"key": k, "value": str(v)}
                for k, v in meta.items()
            ]
            + [
                {"key": "matched_rows", "value": summary["matched_rows"]},
                {"key": "only_left_rows", "value": summary["only_left_rows"]},
                {"key": "only_right_rows", "value": summary["only_right_rows"]},
                {"key": "diff_cells", "value": summary["diff_cells"]},
            ]
        )
        meta_df.to_excel(xw, sheet_name="概览", index=False)
        diff_df.to_excel(xw, sheet_name="差异单元格", index=False)
        only_left_df.to_excel(xw, sheet_name="仅左", index=False)
        only_right_df.to_excel(xw, sheet_name="仅右", index=False)
        if summary.get("by_column"):
            pd.DataFrame(
                [{"column": k, "diff_count": v} for k, v in summary["by_column"].items()]
            ).to_excel(xw, sheet_name="按列分布", index=False)
