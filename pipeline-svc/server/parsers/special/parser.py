"""Special-channel parsers — dedicated implementation (方案A).

Each of the 4 sub-channels now has its **own** processing logic ported from
pingpong-master, instead of sharing a generic dispatcher.

    * ``special_transfer``    — 内部转账 (FundType contains fundtransfer/channel-settle)
    * ``special_ach_refund``  — ACH 退款 (FundType contains ach return)
    * ``special_op_incoming`` — OP 入账 (单据子类型=退款退票(VA))
    * ``special_op_refund``   — OP 退票 (主站+B2B退票表)

Each parser:
    1. Scans ``extracted_dir`` for source files.
    2. Filters files by filename pattern matching.
    3. Calls the vendored ``vendor/special_channel/`` processing function.
    4. Writes per-sub-channel xlsx + csv.
    5. Returns ``ParseResult`` with verify rows.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from server.core.paths import get_rules_files_dir, get_task_extracted_dir, is_extracted_rel_path_parse_candidate
from server.core.task_logger import task_log
from server.parsers.base import (
    BaseParser,
    ParseContext,
    ParseResult,
    VerifyRow,
    make_file_entry,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_EXCEL_SUFFIXES = {".xlsx", ".xls", ".xlsm"}


def _excel_files(extracted_dir: Path) -> list[Path]:
    """Return sorted list of Excel files under *extracted_dir* (recursive)."""
    if not extracted_dir.exists():
        return []
    out: list[Path] = []
    for p in extracted_dir.rglob("*"):
        if not p.is_file():
            continue
        try:
            rel = p.relative_to(extracted_dir).as_posix()
        except ValueError:
            continue
        if not is_extracted_rel_path_parse_candidate(rel):
            continue
        if p.suffix.lower() not in _EXCEL_SUFFIXES:
            continue
        out.append(p)
    return sorted(out, key=lambda x: x.relative_to(extracted_dir).as_posix().lower())


def _match_files(files: list[Path], *patterns: str) -> list[Path]:
    """Return files whose name matches any of the given regex patterns (case-insensitive)."""
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    out: list[Path] = []
    for f in files:
        if any(rx.search(f.name) or rx.search(f.stem) for rx in compiled):
            out.append(f)
    return out


def _write_output(
    df: pd.DataFrame,
    output_dir: Path,
    basename: str,
    sheet_name: str,
) -> list:
    """Write DataFrame to xlsx + csv; return list of FileEntry."""
    from server.parsers.base import make_file_entry as _mfe

    xlsx_path = output_dir / f"{basename}.xlsx"
    csv_path = output_dir / f"{basename}.csv"
    output_dir.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    return [_mfe(xlsx_path, role="output"), _mfe(csv_path, role="output")]


# =========================================================================
# SpecialTransferParser — 内转
# =========================================================================

class SpecialTransferParser(BaseParser):
    channel_id = "special_transfer"
    display_name = "特殊渠道·内部转账"
    output_filename = "special_transfer.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        from vendor.special_channel.process_transfer import process_transfer

        files = _excel_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=files)
        outputs = [make_file_entry(manifest_path, role="manifest")]

        if not files:
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_transfer.empty",
                    severity="pending",
                    summary="未发现内转源文件",
                    rule_ref="special_transfer.directory.scan",
                )],
                warnings=["内转目录为空"],
                note="empty extracted dir",
                metrics={"source_count": 0},
            )

        # Only process files matching 内转/transfer
        matched_files = _match_files(files, r"内转", r"transfer")
        skipped = [f.name for f in files if f not in matched_files]
        if skipped:
            logger.info("special_transfer: 跳过非内转文件: %s", skipped)
        if not matched_files:
            warnings_list = [f"目录中无内转文件（共 {len(files)} 个文件已跳过）"]
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_transfer.no_match",
                    severity="warning",
                    summary=f"目录中无内转文件（共 {len(files)} 个文件已跳过）",
                    rule_ref="special_transfer.file_match",
                )],
                warnings=warnings_list,
                note="no matching files",
                metrics={"source_count": len(files), "matched_count": 0},
            )

        rules_files_dir = get_rules_files_dir()
        verify_rows: List[VerifyRow] = []
        all_dfs: list[pd.DataFrame] = []
        warnings: list[str] = []

        for f in matched_files:
            try:
                logger.info("special_transfer: 处理 %s ...", f.name)
                df = process_transfer(f, rules_files_dir=rules_files_dir)
                all_dfs.append(df)
                verify_rows.append(VerifyRow(
                    row_id=f"special_transfer.ok.{f.name}",
                    severity="pass",
                    summary=f"{f.name}: {len(df)} 行",
                    rule_ref="special_transfer.process",
                    file_ref=f.name,
                    detail={"row_count": len(df)},
                ))
            except Exception as exc:
                logger.warning("special_transfer: failed to process %s: %s", f.name, exc)
                warnings.append(f"{f.name}: 处理失败 — {exc}")
                verify_rows.append(VerifyRow(
                    row_id=f"special_transfer.err.{f.name}",
                    severity="warning",
                    summary=f"{f.name}: 处理失败 — {exc}"[:200],
                    rule_ref="special_transfer.process",
                    file_ref=f.name,
                ))

        df_transfer = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

        # ── ACH return：同步读取 special_ach_refund 目录，合并写入第二 sheet ──
        from vendor.special_channel.process_ach_return import process_ach_return
        ach_dir = get_task_extracted_dir(ctx.task_id, "special_ach_refund")
        ach_files = _excel_files(ach_dir)
        ach_matched = _match_files(ach_files, r"ach\s*return", r"achreturn", r"退款.*ach", r"ach.*退款")
        ach_dfs: list[pd.DataFrame] = []
        for f in ach_matched:
            try:
                df_ach = process_ach_return(f, rules_files_dir=rules_files_dir)
                ach_dfs.append(df_ach)
                verify_rows.append(VerifyRow(
                    row_id=f"special_transfer.ach.ok.{f.name}",
                    severity="pass",
                    summary=f"ACH return · {f.name}: {len(df_ach)} 行",
                    rule_ref="special_transfer.ach_return.process",
                    file_ref=f.name,
                    detail={"row_count": len(df_ach)},
                ))
            except Exception as exc:
                logger.warning("special_transfer: ACH return 处理失败 %s: %s", f.name, exc)
                warnings.append(f"ACH return · {f.name}: 处理失败 — {exc}")
                verify_rows.append(VerifyRow(
                    row_id=f"special_transfer.ach.err.{f.name}",
                    severity="warning",
                    summary=f"ACH return · {f.name}: 处理失败 — {exc}"[:200],
                    rule_ref="special_transfer.ach_return.process",
                    file_ref=f.name,
                ))
        df_ach_merged = pd.concat(ach_dfs, ignore_index=True) if ach_dfs else pd.DataFrame()

        # 写合并工作簿（内转 + ACH return 两张 sheet）
        if not df_transfer.empty or not df_ach_merged.empty:
            xlsx_path = ctx.output_dir / "special_transfer.xlsx"
            csv_path = ctx.output_dir / "special_transfer.csv"
            ctx.output_dir.mkdir(parents=True, exist_ok=True)
            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                if not df_transfer.empty:
                    df_transfer.to_excel(writer, sheet_name="内转", index=False)
                if not df_ach_merged.empty:
                    df_ach_merged.to_excel(writer, sheet_name="ACH return", index=False)
            from server.parsers.base import make_file_entry as _mfe
            outputs.append(_mfe(xlsx_path, role="output"))
            # csv 仅导出内转（主数据）
            if not df_transfer.empty:
                df_transfer.to_csv(csv_path, index=False, encoding="utf-8-sig")
                outputs.append(_mfe(csv_path, role="output"))

        transfer_rows = len(df_transfer) if not df_transfer.empty else 0
        ach_rows = len(df_ach_merged) if not df_ach_merged.empty else 0
        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note=(
                f"内转：{transfer_rows} 行，ACH return：{ach_rows} 行"
                f"（扫描内转文件 {len(files)} 个，匹配 {len(matched_files)}；"
                f"扫描 ACH 文件 {len(ach_files)} 个，匹配 {len(ach_matched)}）"
            ),
            metrics={
                "source_count": len(files),
                "matched_count": len(matched_files),
                "row_count": transfer_rows,
                "ach_row_count": ach_rows,
            },
        )


# =========================================================================
# SpecialAchRefundParser — ACH 退款
# =========================================================================

class SpecialAchRefundParser(BaseParser):
    channel_id = "special_ach_refund"
    display_name = "特殊渠道·ACH 退款"
    output_filename = "special_ach_refund.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        from vendor.special_channel.process_ach_return import process_ach_return

        files = _excel_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=files)
        outputs = [make_file_entry(manifest_path, role="manifest")]

        if not files:
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_ach_refund.empty",
                    severity="pending",
                    summary="未发现 ACH 退款源文件",
                    rule_ref="special_ach_refund.directory.scan",
                )],
                warnings=["ACH 退款目录为空"],
                note="empty extracted dir",
                metrics={"source_count": 0},
            )

        # Only process files matching ACH return
        matched_files = _match_files(files, r"ach\s*return", r"achreturn", r"退款.*ach", r"ach.*退款")
        skipped = [f.name for f in files if f not in matched_files]
        if skipped:
            logger.info("special_ach_refund: 跳过非ACH退款文件: %s", skipped)
        if not matched_files:
            warnings_list = [f"目录中无 ACH 退款文件（共 {len(files)} 个文件已跳过）"]
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_ach_refund.no_match",
                    severity="warning",
                    summary=f"目录中无 ACH 退款文件（共 {len(files)} 个文件已跳过）",
                    rule_ref="special_ach_refund.file_match",
                )],
                warnings=warnings_list,
                note="no matching files",
                metrics={"source_count": len(files), "matched_count": 0},
            )

        rules_files_dir = get_rules_files_dir()
        verify_rows: List[VerifyRow] = []
        all_dfs: list[pd.DataFrame] = []
        warnings: list[str] = []

        for f in matched_files:
            try:
                logger.info("special_ach_refund: 处理 %s ...", f.name)
                df = process_ach_return(f, rules_files_dir=rules_files_dir)
                all_dfs.append(df)
                verify_rows.append(VerifyRow(
                    row_id=f"special_ach_refund.ok.{f.name}",
                    severity="pass",
                    summary=f"{f.name}: {len(df)} 行",
                    rule_ref="special_ach_refund.process",
                    file_ref=f.name,
                    detail={"row_count": len(df)},
                ))
            except Exception as exc:
                logger.warning("special_ach_refund: failed to process %s: %s", f.name, exc)
                warnings.append(f"{f.name}: 处理失败 — {exc}")
                verify_rows.append(VerifyRow(
                    row_id=f"special_ach_refund.err.{f.name}",
                    severity="warning",
                    summary=f"{f.name}: 处理失败 — {exc}"[:200],
                    rule_ref="special_ach_refund.process",
                    file_ref=f.name,
                ))

        if all_dfs:
            merged = pd.concat(all_dfs, ignore_index=True)
            outputs.extend(_write_output(merged, ctx.output_dir, "special_ach_refund", "ACH return"))

        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note=f"ACH退款：专用 parser，{sum(len(d) for d in all_dfs)} 行（扫描 {len(files)} 文件，匹配 {len(matched_files)}）",
            metrics={
                "source_count": len(files),
                "matched_count": len(matched_files),
                "row_count": sum(len(d) for d in all_dfs),
            },
        )


# =========================================================================
# SpecialOpRefundParser — OP 退票
# =========================================================================

class SpecialOpRefundParser(BaseParser):
    channel_id = "special_op_refund"
    display_name = "特殊渠道·OP 退票"
    output_filename = "special_op_refund.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        from vendor.special_channel.process_op_refund import process_op_refund

        files = _excel_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=files)
        outputs = [make_file_entry(manifest_path, role="manifest")]

        if not files:
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_op_refund.empty",
                    severity="pending",
                    summary="未发现 OP 退票源文件",
                    rule_ref="special_op_refund.directory.scan",
                )],
                warnings=["OP 退票目录为空"],
                note="empty extracted dir",
                metrics={"source_count": 0},
            )

        rules_files_dir = get_rules_files_dir()
        verify_rows: List[VerifyRow] = []
        warnings: list[str] = []

        # Match main (主站退票) and B2B files
        main_files = _match_files(files, r"主站退票表.*bu", r"主站.*退票")
        b2b_files = _match_files(files, r"b2b退票表.*bu", r"b2b.*退票", r"b2b.*bu")

        if not main_files:
            warnings.append("未找到主站退票表 BU 文件")
            verify_rows.append(VerifyRow(
                row_id="special_op_refund.no_main",
                severity="warning",
                summary="未找到主站退票表 BU 文件",
                rule_ref="special_op_refund.file_match",
            ))
        if not b2b_files:
            warnings.append("未找到 B2B 退票表 BU 文件")
            verify_rows.append(VerifyRow(
                row_id="special_op_refund.no_b2b",
                severity="warning",
                summary="未找到 B2B 退票表 BU 文件",
                rule_ref="special_op_refund.file_match",
            ))

        if main_files and b2b_files:
            try:
                df = process_op_refund(
                    main_files[0], b2b_files[0], rules_files_dir=rules_files_dir
                )
                outputs.extend(_write_output(df, ctx.output_dir, "special_op_refund", "OP退票表"))
                verify_rows.append(VerifyRow(
                    row_id="special_op_refund.ok",
                    severity="pass",
                    summary=f"OP退票表: {len(df)} 行",
                    rule_ref="special_op_refund.process",
                    detail={"row_count": len(df)},
                ))
            except Exception as exc:
                logger.warning("special_op_refund: failed: %s", exc)
                warnings.append(f"OP退票处理失败 — {exc}")
                verify_rows.append(VerifyRow(
                    row_id="special_op_refund.err",
                    severity="warning",
                    summary=f"OP退票处理失败 — {exc}"[:200],
                    rule_ref="special_op_refund.process",
                ))

        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note="OP退票：专用 parser（主站+B2B）",
            metrics={"source_count": len(files)},
        )


# =========================================================================
# SpecialOpIncomingParser — OP 入账
# =========================================================================

class SpecialOpIncomingParser(BaseParser):
    channel_id = "special_op_incoming"
    display_name = "特殊渠道·OP 入账"
    output_filename = "special_op_incoming.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        from vendor.special_channel.process_op_incoming import process_op_incoming

        files = _excel_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=files)
        outputs = [make_file_entry(manifest_path, role="manifest")]

        if not files:
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_op_incoming.empty",
                    severity="pending",
                    summary="未发现 OP 入账源文件",
                    rule_ref="special_op_incoming.directory.scan",
                )],
                warnings=["OP 入账目录为空"],
                note="empty extracted dir",
                metrics={"source_count": 0},
            )

        # Only process files that look like OP 入账 files (not 退票表)
        matched_files = _match_files(files, r"福贸入账", r"入账.*bu", r"入账表.*bu")
        # Explicitly exclude 退票 files
        matched_files = [f for f in matched_files if "退票" not in f.name]
        skipped = [f.name for f in files if f not in matched_files]
        if skipped:
            logger.info("special_op_incoming: 跳过非入账文件: %s", skipped)
        if not matched_files:
            warnings_list = [f"目录中无 OP 入账文件（共 {len(files)} 个文件已跳过：{', '.join(skipped)}）"]
            return ParseResult(
                output_files=outputs,
                verify_rows=[VerifyRow(
                    row_id="special_op_incoming.no_match",
                    severity="warning",
                    summary=f"目录中无 OP 入账文件（共 {len(files)} 个文件已跳过）",
                    rule_ref="special_op_incoming.file_match",
                    detail={"skipped": skipped},
                )],
                warnings=warnings_list,
                note="no matching files",
                metrics={"source_count": len(files), "matched_count": 0},
            )

        rules_files_dir = get_rules_files_dir()
        verify_rows: List[VerifyRow] = []
        all_dfs: list[pd.DataFrame] = []
        warnings: list[str] = []

        task_log(
            ctx.task_id,
            f"OP 入账：目录内 {len(files)} 个表格文件，文件名匹配待处理 {len(matched_files)} 个 …",
            channel=ctx.channel_id,
        )

        nm = len(matched_files)
        for i, f in enumerate(matched_files, start=1):
            try:
                logger.info("special_op_incoming: 处理 %s ...", f.name)
                task_log(
                    ctx.task_id,
                    f"  [{i}/{nm}] OP 入账：正在解析 {f.name}（读取 Excel · 筛选子类型 …）…",
                    channel=ctx.channel_id,
                )
                df = process_op_incoming(f, rules_files_dir=rules_files_dir)
                n = len(df)
                all_dfs.append(df)
                if n == 0:
                    wmsg = f"{f.name}: 解析结果为 0 行（表可能为空、列结构不匹配或全部被规则过滤）"
                    warnings.append(wmsg)
                    verify_rows.append(
                        VerifyRow(
                            row_id=f"special_op_incoming.empty_rows.{f.name}",
                            severity="warning",
                            summary=f"{f.name}: 0 行（无有效数据，不应视为成功）",
                            rule_ref="special_op_incoming.process",
                            file_ref=f.name,
                            detail={"row_count": 0},
                        )
                    )
                else:
                    verify_rows.append(
                        VerifyRow(
                            row_id=f"special_op_incoming.ok.{f.name}",
                            severity="pass",
                            summary=f"{f.name}: {n} 行",
                            rule_ref="special_op_incoming.process",
                            file_ref=f.name,
                            detail={"row_count": n},
                        )
                    )
                task_log(
                    ctx.task_id,
                    f"  [{i}/{nm}] OP 入账：完成 {f.name} · {n} 行",
                    channel=ctx.channel_id,
                )
            except Exception as exc:
                logger.warning("special_op_incoming: failed to process %s: %s", f.name, exc)
                task_log(
                    ctx.task_id,
                    f"  [{i}/{nm}] OP 入账：失败 {f.name} — {exc}",
                    channel=ctx.channel_id,
                )
                warnings.append(f"{f.name}: 处理失败 — {exc}")
                verify_rows.append(VerifyRow(
                    row_id=f"special_op_incoming.err.{f.name}",
                    severity="warning",
                    summary=f"{f.name}: 处理失败 — {exc}"[:200],
                    rule_ref="special_op_incoming.process",
                    file_ref=f.name,
                ))

        non_empty = [d for d in all_dfs if not d.empty]
        if non_empty:
            task_log(
                ctx.task_id,
                f"OP 入账：合并 {len(non_empty)} 份非空结果，写入 special_op_incoming …",
                channel=ctx.channel_id,
            )
            merged = pd.concat(non_empty, ignore_index=True)
            outputs.extend(_write_output(merged, ctx.output_dir, "special_op_incoming", "OP入账表"))

        total_rows = sum(len(d) for d in all_dfs)
        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note=f"OP入账：专用 parser，{total_rows} 行（扫描 {len(files)} 文件，匹配 {len(matched_files)}）",
            metrics={
                "source_count": len(files),
                "matched_count": len(matched_files),
                "row_count": total_rows,
            },
        )


# =========================================================================
# SpecialMergeBundleParser — 多 sheet 合并
# =========================================================================


class SpecialMergeBundleParser(BaseParser):
    """Merge latest special_* run outputs into one multi-sheet xlsx."""
    channel_id = "special_merge"
    display_name = "特殊来源·合并导出"
    output_filename = "内转_ACH_OP合并结果.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        from server.parsers.special.merge_bundle import run_special_bundle_merge

        phase = (ctx.metadata.get("allocation_phase") or "").strip().lower()
        if phase == "merge":
            return run_special_bundle_merge(ctx)

        return ParseResult(
            output_files=[],
            verify_rows=[
                VerifyRow(
                    row_id="special_merge.section",
                    severity="pending",
                    summary="请在本页切换到「合并」分区后点击执行合并",
                    rule_ref="special_merge.section",
                )
            ],
            warnings=[],
            note="special_merge 仅用于合并步骤；请使用 allocation_phase=merge 触发。",
        )


# =========================================================================
# FinalMergeParser — 最终合并（所有大渠道 → 成本汇总）
# =========================================================================


class FinalMergeParser(BaseParser):
    """Collect all verified channel outputs and produce cost summary."""
    channel_id = "final_merge"
    display_name = "最终合并"
    output_filename = "成本汇总_合并.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        from server.parsers.special.merge_final_bundle import run_final_merge

        # 直接执行最终合并（不需要 allocation_phase 参数）
        return run_final_merge(ctx)
