"""Merge latest special_* workbook outputs into one multi-sheet xlsx (align pingpong all.py)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from server.core.pipeline_state import StateManager
from server.parsers.base import ParseContext, ParseResult, VerifyRow, make_file_entry


# 合并写入 sheet 顺序（与 pingpong 多 sheet 一致）
_MERGE_OUTPUT_ORDER = ("内转", "ACH return", "OP入账表", "OP退票表")
def _run_status_str(st: Any) -> str:
    v = getattr(st, "value", None)
    if isinstance(v, str):
        return v
    return str(st)


def _latest_verified_output_xlsx(task_id: str, channel_id: str) -> Path | None:
    state = StateManager.load_state(task_id)
    if state is None:
        return None
    ch = state.channels.get(channel_id)
    if ch is None or not ch.runs:
        return None
    ok_status = {"verified", "verified_with_warning", "confirmed"}
    for run in reversed(ch.runs):
        if _run_status_str(run.status) not in ok_status:
            continue
        for fe in reversed(run.output_files):
            if fe.role != "output":
                continue
            if not fe.name.endswith(".xlsx"):
                continue
            p = Path(fe.path)
            if p.is_file():
                return p
    return None


def _read_named_sheet(book: Path, prefer: str) -> pd.DataFrame:
    """Read a named sheet from a workbook. Returns empty DataFrame if sheet not found."""
    xl = pd.ExcelFile(book)
    if prefer in xl.sheet_names:
        return xl.parse(prefer)
    return pd.DataFrame()


def run_special_bundle_merge(ctx: ParseContext) -> ParseResult:
    task_id = ctx.task_id
    missing: list[str] = []
    frames: dict[str, pd.DataFrame] = {}

    st_p = _latest_verified_output_xlsx(task_id, "special_transfer")
    ach_p = _latest_verified_output_xlsx(task_id, "special_ach_refund")

    if st_p:
        frames["内转"] = _read_named_sheet(st_p, "内转")
    else:
        frames["内转"] = pd.DataFrame()
        missing.append("special_transfer")

    if ach_p:
        frames["ACH return"] = _read_named_sheet(ach_p, "ACH return")
        if frames["ACH return"].empty:
            missing.append("special_ach_refund(empty sheet)")
    else:
        frames["ACH return"] = pd.DataFrame()

    tail_specs: tuple[tuple[str, str, str], ...] = (
        ("special_op_incoming", "OP入账表", "OP入账表"),
        ("special_op_refund", "OP退票表", "OP退票表"),
    )

    for source_ch, xl_sheet, out_sheet in tail_specs:
        p = _latest_verified_output_xlsx(task_id, source_ch)
        if p is None:
            missing.append(source_ch)
            frames[out_sheet] = pd.DataFrame()
            continue
        frames[out_sheet] = _read_named_sheet(p, xl_sheet)

    if frames["ACH return"].empty and ach_p is None:
        missing.append("special_ach_refund")

    out_path = ctx.output_dir / "内转_ACH_OP合并结果.xlsx"
    ctx.output_dir.mkdir(parents=True, exist_ok=True)

    nonempty = [(out_name, df) for out_name, df in frames.items() if not df.empty]
    if not nonempty:
        return ParseResult(
            output_files=[],
            verify_rows=[
                VerifyRow(
                    row_id="special_merge.no_inputs",
                    severity="pending",
                    summary=(
                        "没有可用的上游产物：请先在内转/ACH 与 OP 入账分区各自执行成功（或 OP 退票如有需要）。"
                        + (f" 缺产出渠道：{'、'.join(missing)}" if missing else "")
                    ),
                    rule_ref="special_merge.inputs",
                    detail={"missing_channels": missing},
                )
            ],
            warnings=[f"合并跳过：无任何非空上游 sheet ({', '.join(missing)})"]
            if missing
            else ["合并跳过：上游 sheet 均为空"],
            metrics={"merged_sheets": 0},
        )

    sheet_rows: list[VerifyRow] = []
    written = 0
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for out_name in _MERGE_OUTPUT_ORDER:
            df = frames.get(out_name, pd.DataFrame())
            if df.empty:
                continue
            safe_name = out_name[:31] if len(out_name) > 31 else out_name
            df.to_excel(writer, sheet_name=safe_name, index=False)
            written += 1
            sheet_rows.append(
                VerifyRow(
                    row_id=f"special_merge.sheet.{safe_name}",
                    severity="pass",
                    summary=f"sheet 「{safe_name}」:{len(df)} 行",
                    rule_ref="special_merge.write",
                    file_ref=out_path.name,
                )
            )

    outputs = [make_file_entry(out_path, role="output")]
    warns = [f"{m} 暂无已校验产出，跳过对应 sheet" for m in missing]
    metrics: dict[str, Any] = {"merged_sheets": written, "missing_channels": missing}
    summary = f"已写入 {written} 个 sheet" + ("（部分分区无产出已跳过）" if missing else "")

    return ParseResult(
        output_files=outputs,
        verify_rows=sheet_rows
        + [
            VerifyRow(
                row_id="special_merge.ok",
                severity="warning" if missing else "pass",
                summary=summary,
                rule_ref="special_merge.ok",
                detail=metrics,
            )
        ],
        warnings=warns,
        note="合并取自各渠道最近一次已校验/已签发 run 中的 xlsx 产物。",
        metrics=metrics,
    )
