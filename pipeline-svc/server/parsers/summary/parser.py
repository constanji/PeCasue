"""Final summary (cost_summary) parser — real implementation.

Aggregates the canonical outputs of every upstream channel into one
``final_summary.xlsx`` keyed by ``主体 / 分行维度 / 入账期间 / 类型``.
The upstream rows are read from each channel's most recent run output
(``data/tasks/{tid}/channels/{ch}/runs/{rid}/`` xlsx + csv files) so the
summary reflects the latest Human-approved state.

The orchestrator passes the per-task root via ``ctx.metadata["task_root"]``
so we don't have to re-derive it from runtime paths.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from server.parsers.base import (
    BaseParser,
    ParseContext,
    ParseResult,
    VerifyRow,
    make_file_entry,
)


UPSTREAM_CHANNELS = (
    "bill",
    "own_flow",
    "customer",
    "special",
    "cn_jp",
    "allocation_base",
)


def _latest_run_dir(task_root: Path, channel_id: str) -> Path | None:
    base = task_root / "channels" / channel_id / "runs"
    if not base.is_dir():
        return None
    runs = sorted(
        [p for p in base.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime
    )
    return runs[-1] if runs else None


def _read_canonical_outputs(run_dir: Path) -> List[pd.DataFrame]:
    out: List[pd.DataFrame] = []
    for p in sorted(run_dir.glob("*.csv")):
        try:
            out.append(pd.read_csv(p, encoding="utf-8-sig"))
        except Exception:
            try:
                out.append(pd.read_csv(p, encoding="gbk"))
            except Exception:
                continue
    if out:
        return out
    for p in sorted(run_dir.glob("*.xlsx")):
        try:
            out.append(pd.read_excel(p))
        except Exception:
            continue
    return out


class SummaryParser(BaseParser):
    channel_id = "summary"
    display_name = "最终汇总"
    output_filename = "final_summary.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        sources = self.list_source_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(
            ctx,
            sources=sources,
            extras={"upstream_channels": list(UPSTREAM_CHANNELS)},
        )
        outputs = [make_file_entry(manifest_path, role="manifest")]

        # Resolve task_root either from context or by walking up from output_dir
        task_root = ctx.metadata.get("task_root")
        if task_root:
            task_root = Path(task_root)
        else:
            task_root = ctx.output_dir.parent.parent.parent.parent

        verify_rows: List[VerifyRow] = []
        warnings: List[str] = []
        all_frames: List[pd.DataFrame] = []
        per_channel_stats: Dict[str, Dict[str, Any]] = {}

        for ch in UPSTREAM_CHANNELS:
            run_dir = _latest_run_dir(task_root, ch)
            if run_dir is None:
                verify_rows.append(
                    VerifyRow(
                        row_id=f"summary.upstream.{ch}",
                        severity="warning",
                        summary=f"上游渠道 {ch} 尚未产出可用 run",
                        rule_ref="summary.upstream.gate",
                    )
                )
                per_channel_stats[ch] = {"status": "missing"}
                continue
            frames = _read_canonical_outputs(run_dir)
            if not frames:
                verify_rows.append(
                    VerifyRow(
                        row_id=f"summary.upstream.{ch}",
                        severity="warning",
                        summary=f"{ch} 最近一次 run 没有可读取的 xlsx/csv",
                        rule_ref="summary.upstream.read",
                    )
                )
                per_channel_stats[ch] = {"status": "empty", "run": run_dir.name}
                continue
            row_total = sum(len(f) for f in frames)
            for f in frames:
                f = f.copy()
                f["__channel__"] = ch
                f["__run__"] = run_dir.name
                all_frames.append(f)
            verify_rows.append(
                VerifyRow(
                    row_id=f"summary.upstream.{ch}",
                    severity="pass",
                    summary=f"{ch}: 读取最新 run {run_dir.name} · {row_total} 行",
                    rule_ref="summary.upstream.gate",
                    detail={"run": run_dir.name, "row_count": row_total},
                )
            )
            per_channel_stats[ch] = {
                "status": "ready",
                "run": run_dir.name,
                "row_count": row_total,
            }

        if not all_frames:
            warnings.append("所有上游渠道均未产出，无法生成 final_summary。")
            return ParseResult(
                output_files=outputs,
                verify_rows=verify_rows,
                warnings=warnings,
                note="缺失上游渠道，请先在「渠道详情」执行各渠道。",
                metrics={"upstream_ready": 0, "upstream_missing": len(UPSTREAM_CHANNELS)},
            )

        # Long-format combined output (one row per upstream row, with channel tag).
        combined = pd.concat(all_frames, ignore_index=True, sort=False)
        long_xlsx = ctx.output_dir / "summary_long.xlsx"
        combined.to_excel(long_xlsx, index=False, engine="openpyxl")
        outputs.append(make_file_entry(long_xlsx, role="output"))

        # Try to aggregate by 主体 / 分行维度 / 入账期间 / 类型 → USD金额.
        agg_xlsx = ctx.output_dir / self.output_filename
        try:
            keys = [c for c in ("主体", "分行维度", "入账期间", "类型") if c in combined.columns]
            amount_col = next(
                (c for c in ("USD金额", "Amount", "Charge in Invoice CCY") if c in combined.columns),
                None,
            )
            if keys and amount_col is not None:
                combined[amount_col] = pd.to_numeric(combined[amount_col], errors="coerce")
                pivot = (
                    combined.groupby(keys + ["__channel__"], dropna=False)[amount_col]
                    .sum()
                    .reset_index()
                    .rename(columns={amount_col: "金额合计", "__channel__": "渠道"})
                )
                pivot.to_excel(agg_xlsx, index=False, engine="openpyxl")
                verify_rows.append(
                    VerifyRow(
                        row_id="summary.aggregate",
                        severity="pass",
                        summary=(
                            f"按 {' / '.join(keys)} 聚合 {amount_col} 完成，"
                            f"产出 {len(pivot)} 行"
                        ),
                        rule_ref="summary.aggregate",
                    )
                )
            else:
                # Fallback: per-channel row count summary
                per_channel = (
                    combined.groupby("__channel__")
                    .size()
                    .reset_index(name="row_count")
                    .rename(columns={"__channel__": "渠道"})
                )
                per_channel.to_excel(agg_xlsx, index=False, engine="openpyxl")
                verify_rows.append(
                    VerifyRow(
                        row_id="summary.aggregate",
                        severity="warning",
                        summary=(
                            "无法找到 主体/分行维度/入账期间/类型 + 金额列，"
                            "退化为按渠道行数统计。"
                        ),
                        rule_ref="summary.aggregate",
                    )
                )
                warnings.append("缺少标准映射列，最终汇总退化为渠道计数。")
            outputs.append(make_file_entry(agg_xlsx, role="output"))
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"聚合失败: {exc}")
            verify_rows.append(
                VerifyRow(
                    row_id="summary.aggregate",
                    severity="warning",
                    summary=f"聚合异常: {exc}"[:200],
                    rule_ref="summary.aggregate",
                )
            )

        ready = sum(1 for v in per_channel_stats.values() if v.get("status") == "ready")
        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note="读取每个上游渠道最近 run 的 csv/xlsx 输出，按主体/分行/期间/类型聚合金额。",
            metrics={
                "upstream_ready": ready,
                "upstream_missing": len(UPSTREAM_CHANNELS) - ready,
                "row_count": int(len(combined)),
                "per_channel": per_channel_stats,
            },
        )
