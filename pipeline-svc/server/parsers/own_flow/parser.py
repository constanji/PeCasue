"""Own-flow (自有流水) parser — real implementation.

Wraps the vendored ``own_flow_pkg.pipeline.run_pipeline(input_dir, period)``
which scans BOC / DBS / Citi / JPM / SCB / DB / PingPong-EU subfolders, applies
the 处理表 (processing book) rules, and returns a single DataFrame in the
canonical OUTPUT_COLUMNS order.

We persist the DataFrame as ``own_bank_statement_matched.xlsx`` and produce an
Allline-style **规则级**校验摘要 via ``matched_rule_verify.verify_structured``
(JSON ``own_flow_processing/current.json`` 或 ``rules/files/rules/处理表``）。
"""
from __future__ import annotations

import contextlib
import io
from typing import List

from server.core.paths import get_rules_files_dir
from server.core.task_logger import task_log

from server.parsers._legacy.own_flow_pkg.matched_rule_verify import (
    _EXPORT_OMITTED_FIN_MARK,
    compact_processing_verify_for_api,
    counts_from_verify_results,
    parse_rules_from_json,
    parse_rules_from_xlsx,
    verify_structured,
)
from server.parsers._legacy.own_flow_pkg.pipeline import run_pipeline
from server.parsers._legacy.own_flow_pkg.rules import all_rules
from server.parsers.base import (
    BaseParser,
    ParseContext,
    ParseResult,
    VerifyRow,
    make_file_entry,
)


def _load_processing_rules_for_verify() -> list[dict]:
    """优先 ``own_flow_processing/current.json``，其次 ``rules/files/rules/处理表``（csv 优先）。"""
    jp = get_rules_files_dir() / "own_flow_processing" / "current.json"
    if jp.is_file():
        parsed = parse_rules_from_json(jp)
        if parsed:
            return parsed
    stem = get_rules_files_dir() / "rules" / "处理表"
    xlsx_path = stem.with_suffix(".xlsx")
    if stem.with_suffix(".csv").exists() or xlsx_path.exists():
        try:
            return parse_rules_from_xlsx(xlsx_path)
        except (OSError, ValueError, KeyError):
            pass
    return []


class OwnFlowParser(BaseParser):
    channel_id = "own_flow"
    display_name = "自有流水"
    output_filename = "own_bank_statement_matched.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        sources = self.list_source_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=sources)

        verify_rows: List[VerifyRow] = []
        warnings: List[str] = []
        outputs = [make_file_entry(manifest_path, role="manifest")]
        metrics: dict[str, object] = {"source_count": len(sources)}

        if not sources:
            verify_rows.append(
                VerifyRow(
                    row_id="own_empty",
                    severity="pending",
                    summary="未发现自有流水源文件",
                    rule_ref="own_flow.directory.scan",
                )
            )
            return ParseResult(
                output_files=outputs,
                verify_rows=verify_rows,
                warnings=["自有流水目录为空"],
                note="Skip: empty extracted dir.",
                metrics=metrics,
            )

        period = (ctx.period or "").strip() or "202602"
        buf = io.StringIO()

        def _progress(msg: str) -> None:
            task_log(ctx.task_id, msg, channel=ctx.channel_id)

        try:
            with contextlib.redirect_stdout(buf):
                df = run_pipeline(ctx.extracted_dir, period, progress_log=_progress)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"run_pipeline 抛出 {type(exc).__name__}: {exc}")
            verify_rows.append(
                VerifyRow(
                    row_id="own_runtime_exception",
                    severity="warning",
                    summary=f"自有流水解析异常: {exc}"[:200],
                    rule_ref="own_flow.pipeline.run",
                )
            )
            log_path = ctx.output_dir / "legacy_stdout.log"
            log_path.write_text(buf.getvalue(), encoding="utf-8")
            outputs.append(make_file_entry(log_path, role="log"))
            return ParseResult(
                output_files=outputs,
                verify_rows=verify_rows,
                warnings=warnings,
                note="自有流水解析异常，请检查源目录与处理表/规则页配置。",
                metrics=metrics,
            )

        if df.empty:
            warn_msg = (
                "流水线未解析出任何明细行：请确认 extracted 目录下是否包含各银行解析器约定的子路径 "
                "（如 Citi CSV、DBS、JPM、DB、BOC、SCB、BO SH 等工作簿或文件夹）；仅顶层「自有流水」归类文件夹而没有对应渠道文件时输出为空。"
            )
            warnings.append(warn_msg)
            verify_rows.append(
                VerifyRow(
                    row_id="own_pipeline_no_rows",
                    severity="warning",
                    summary=warn_msg[:240],
                    rule_ref="own_flow.pipeline.empty_output",
                )
            )

        result_xlsx = ctx.output_dir / self.output_filename
        df.to_excel(result_xlsx, index=False, engine="openpyxl")
        outputs.append(make_file_entry(result_xlsx, role="output"))

        # CSV mirror for the compare module (which prefers row-oriented sources).
        result_csv = ctx.output_dir / "own_bank_statement_matched.csv"
        df.to_csv(result_csv, index=False, encoding="utf-8-sig")
        outputs.append(make_file_entry(result_csv, role="output"))

        # Allline 风格：逐条处理表规则 vs 汇总输出（备注 / 入账科目）
        status_map = {
            "通过": "pass",
            "警告": "warning",
            "待核算": "pending",
            "不适用": "pass",
        }
        try:
            rules_v = _load_processing_rules_for_verify()
            if not df.empty and rules_v:
                vdf = df.copy()
                if _EXPORT_OMITTED_FIN_MARK not in vdf.columns:
                    vdf[_EXPORT_OMITTED_FIN_MARK] = ""
                results = verify_structured(rules_v, vdf)
                compact = compact_processing_verify_for_api(results)
                cnt = counts_from_verify_results(results)
                eligible = max(cnt["total"] - cnt.get("na", 0), 1)
                metrics["own_flow_processing_verify"] = {
                    "schema": "pecause_ownflow_rule_verify_v1",
                    "counts": cnt,
                    "pass_rate": round(cnt["pass"] / eligible, 4),
                    "rules": compact,
                }
                for row in compact:
                    seq = row.get("规则序号")
                    rid = f"own.rule.{seq}" if seq is not None else f"own.rule.idx-{len(verify_rows)}"
                    verify_rows.append(
                        VerifyRow(
                            row_id=rid,
                            severity=status_map.get(str(row.get("状态") or ""), "pending"),
                            summary=str(row.get("说明") or ""),
                            rule_ref="own_flow.rule_verify",
                            detail={"rule": row},
                        )
                    )
            elif not df.empty and not rules_v:
                verify_rows.append(
                    VerifyRow(
                        row_id="own.no_verify_rules",
                        severity="pending",
                        summary="未能加载处理表规则（请配置 own_flow_processing/current.json 或 rules/files/rules/处理表），跳过规则级校验",
                        rule_ref="own_flow.rule_verify",
                    )
                )
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"规则级校验跳过: {exc}")

        log_path = ctx.output_dir / "legacy_stdout.log"
        log_path.write_text(buf.getvalue(), encoding="utf-8")
        outputs.append(make_file_entry(log_path, role="log"))

        metrics["row_count"] = int(len(df))
        metrics["processing_rules_total"] = int(len(all_rules()))
        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note="使用 allline/own_flow 真实流水线；校验 Tab 为逐条处理表规则对照汇总结果。",
            metrics=metrics,
        )
