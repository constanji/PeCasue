"""Allocation QuickBI / CitiHK / merge — thin wrappers around allline ``分摊基数`` code."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from server.core.paths import (
    get_allocation_bundle_root,
    get_rules_allocation_citihk_mapping_csv_path,
    get_rules_allocation_citihk_pphk_template_path,
    get_rules_allocation_quickbi_template_path,
    get_task_dir,
    get_vendored_allline_allocation_root,
)
from server.core.task_logger import task_log
from server.parsers.allocation.cost_allocate_bridge import run_cost_allocate_with_pingpong
from server.parsers.allocation.merge_tables import merge_allocation_workbooks
from server.parsers.base import ParseContext, ParseResult, VerifyRow, make_file_entry


def _verify_note_from_logs(logs: list[str], *, tail: int | None) -> str | None:
    """校验面板 note：步骤间空行分隔（配合前端 whitespace-pre-wrap）。"""
    if not logs:
        return None
    chunk = logs[-tail:] if tail is not None and tail > 0 else logs
    return "\n\n".join(chunk)


def _quickbi_scan_metrics(scan: Any) -> dict[str, Any]:
    """来自 bases_folder.scan_bases_folder：源文件份数与选用文件的估算行数。"""
    return {
        "quickbi_inbound_sources": scan.quickbi_source_counts[0],
        "quickbi_outbound_sources": scan.quickbi_source_counts[1],
        "quickbi_va_sources": scan.quickbi_source_counts[2],
        "quickbi_inbound_rows": scan.quickbi_selected_rows[0],
        "quickbi_outbound_rows": scan.quickbi_selected_rows[1],
        "quickbi_va_rows": scan.quickbi_selected_rows[2],
    }


def allocation_pkg_root() -> Path:
    """默认指向 ``pipeline-svc/vendor/allocation_bundle/allline``；可用 ``PECAUSE_ALLLINE_ALLOCATION_ROOT`` 覆盖。"""
    env = os.environ.get("PECAUSE_ALLLINE_ALLOCATION_ROOT", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return get_vendored_allline_allocation_root()


def _cost_allocation_parent(pkg: Path) -> Path:
    """含 ``cost_allocation`` 包的目录：优先 vendored bundle，其次旧版 ``script/分摊``。"""
    vendored = get_allocation_bundle_root()
    if (vendored / "cost_allocation").is_dir():
        return vendored
    legacy = pkg.parent.parent / "分摊"
    if (legacy / "cost_allocation").is_dir():
        return legacy
    return vendored


def _log(ctx: ParseContext, msg: str) -> None:
    task_log(ctx.task_id, msg, channel=ctx.channel_id)


def _ensure_quickbi_path() -> None:
    pkg = allocation_pkg_root()
    if str(pkg) not in sys.path:
        sys.path.insert(0, str(pkg))


def _task_period_to_yyyymm(period: object) -> str:
    """任务期次 -> YYYYMM，仅 CitiHK 输出「月份」覆盖时使用（QuickBI 保留源表月份）。"""
    if period is None:
        return ""
    s = str(period).strip()
    if not s:
        return ""
    if len(s) == 6 and s.isdigit():
        return s
    if len(s) >= 7 and s[4] in "-/" and s[:4].isdigit() and s[5:7].isdigit():
        return s[:4] + s[5:7]
    return ""


def run_quickbi_phase(ctx: ParseContext, opts: dict[str, Any]) -> ParseResult:
    action = str(opts.get("action") or "simulated").strip().lower()

    _ensure_quickbi_path()
    try:
        from ui_services import bases_folder as bf  # type: ignore
        from ui_services import quickbi_service as qb  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.import",
                    severity="warning",
                    summary=f"无法加载 allline 分摊基数 QuickBI 模块：{exc}",
                    rule_ref="allocation.quickbi.import",
                )
            ],
            warnings=[str(exc)],
            note="QuickBI 依赖缺失（确认 pipeline-svc/vendor/allocation_bundle/allline 已随仓库同步，或设置 PECAUSE_ALLLINE_ALLOCATION_ROOT）",
        )

    tpl = get_rules_allocation_quickbi_template_path()
    if not tpl.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.template",
                    severity="pending",
                    summary="规则库缺少 QuickBI 模版：请在「规则 → 分摊基数模版」上传「收付款成本分摊基数表模版.xlsx」",
                    rule_ref="allocation.quickbi.template",
                )
            ],
            warnings=[f"模版不存在：{tpl}"],
        )

    if action == "convert_csv":
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.csv",
                    severity="pending",
                    summary=(
                        "已不再提供「QuickBI 转 CSV」。请直接在渠道目录上传入金 / 出金 / VA 对应的 .csv 文件。"
                    ),
                    rule_ref="allocation.quickbi.csv",
                )
            ],
            warnings=[],
        )

    if action == "excel_step_c":
        logs: list[str] = []

        def progress(msg: str) -> None:
            logs.append(msg)
            _log(ctx, msg)

        outputs: list = []
        detail_name = str(opts.get("detail_workbook_name") or "").strip()
        candidates: list[Path] = []
        if detail_name:
            p = (ctx.extracted_dir / detail_name).resolve()
            if p.is_file():
                candidates.append(p)
        if not candidates:
            st = ctx.metadata.get("allocation_task_state") or {}
            step_a = st.get("quickbi_step_a_workbook")
            if isinstance(step_a, str) and Path(step_a).is_file():
                candidates.append(Path(step_a))
        if not candidates:
            for p in sorted(
                ctx.extracted_dir.glob("*.xlsx"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            ):
                if not p.name.startswith("~$"):
                    candidates.append(p)
                    break
            for p in sorted(
                ctx.extracted_dir.glob("*.xlsm"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            ):
                if not p.name.startswith("~$"):
                    candidates.append(p)
                    break
        if not candidates:
            return ParseResult(
                verify_rows=[
                    VerifyRow(
                        row_id="allocation.quickbi.excel_c",
                        severity="pending",
                        summary=(
                            "步骤 c：找不到刷新后的明细 xlsx（请上传或在 allocation_options 指定 detail_workbook_name）"
                        ),
                        rule_ref="allocation.quickbi.excel_c",
                    )
                ],
                warnings=[],
            )
        src = candidates[0]
        summary_path = qb.build_excel_formula_summary_with_progress(src, progress=progress)
        sp = Path(summary_path).resolve()
        dest = ctx.output_dir / sp.name
        shutil.copy2(sp, dest)
        outputs.append(make_file_entry(dest, role="output"))
        patch = {
            "quickbi_summary_workbook": str(dest.resolve()),
            "quickbi_merge_source": str(dest.resolve()),
            "quickbi_workbook": str(src.resolve()),
            "quickbi_ready": True,
        }
        return ParseResult(
            output_files=outputs,
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.excel_c",
                    severity="pass",
                    summary=f"步骤 c：已生成 Excel 公式口径汇总表 {dest.name}",
                    rule_ref="allocation.quickbi.excel_c",
                )
            ],
            warnings=[],
            note=_verify_note_from_logs(logs, tail=60),
            metrics={"allocation_state_patch": patch, "lines": logs},
        )

    scan = bf.scan_bases_folder(ctx.extracted_dir)
    qb_metrics = _quickbi_scan_metrics(scan)
    for note in scan.scan_notes:
        _log(ctx, f"[QuickBI scan] {note}")
    for w in scan.warnings:
        _log(ctx, f"[QuickBI scan] {w}")

    if scan.quickbi_inbound is None or scan.quickbi_outbound is None or scan.quickbi_va is None:
        missing = []
        if scan.quickbi_inbound is None:
            missing.append("入金")
        if scan.quickbi_outbound is None:
            missing.append("出金")
        if scan.quickbi_va is None:
            missing.append("VA")
        summary_extra = scan.scan_notes[0] if scan.scan_notes else ""
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.sources",
                    severity="pending",
                    summary=(
                        "QuickBI 三件套不齐（须各一份 CSV）："
                        + "、".join(missing)
                        + (f"。{summary_extra}" if summary_extra else "")
                    ),
                    rule_ref="allocation.quickbi.sources",
                    detail=qb_metrics,
                )
            ],
            warnings=list(scan.warnings),
            note="请上传符合命名的 QuickBI 源文件：入金 / 出金 / VA **各一份 .csv**（文件名含「入金」「出金」或 VA 类 QuickBI 导出关键字）。",
            metrics=qb_metrics,
        )

    paths = qb.QuickBIPaths(
        inbound=scan.quickbi_inbound,
        outbound=scan.quickbi_outbound,
        va=scan.quickbi_va,
        template=tpl,
        output_root=ctx.output_dir,
    )

    logs: list[str] = []

    def progress(msg: str) -> None:
        logs.append(msg)
        _log(ctx, msg)

    outputs: list = []

    if action == "simulated":
        result = qb.build_quickbi_workbook(
            paths,
            prefer_csv=True,
            output_dir=ctx.output_dir,
            enrich_final_bu=True,
            progress=progress,
        )
        wb = Path(result.workbook_path)
        outputs.append(make_file_entry(wb, role="output"))
        soc = result.summary_only_path
        if soc is not None and Path(soc).is_file():
            outputs.append(make_file_entry(Path(soc).resolve(), role="output"))
        soc_ok = soc is not None and Path(soc).is_file()
        merge_src = str(Path(soc).resolve()) if soc_ok else str(wb.resolve())
        patch = {
            "quickbi_workbook": str(wb.resolve()),
            "quickbi_summary_workbook": merge_src,
            "quickbi_merge_source": merge_src,
            "quickbi_ready": True,
        }
        return ParseResult(
            output_files=outputs,
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.simulated",
                    severity="pass",
                    summary=(
                        f"模拟公式 BU：已生成 {wb.name}，并附 {soc.name if soc and Path(soc).is_file() else '—'} 三表汇总轻量版 "
                        f"(入金 {result.rows_in} / 出金 {result.rows_out} / VA {result.rows_va})"
                    ),
                    rule_ref="allocation.quickbi.simulated",
                    detail={"workbook": str(wb)},
                )
            ],
            warnings=list(scan.warnings),
            note=_verify_note_from_logs(logs, tail=60),
            metrics={"allocation_state_patch": patch, "lines": logs, **qb_metrics},
        )

    if action == "excel_step_a":
        result = qb.build_quickbi_workbook(
            paths,
            prefer_csv=True,
            output_dir=ctx.output_dir,
            enrich_final_bu=False,
            progress=progress,
        )
        wb = Path(result.workbook_path)
        outputs.append(make_file_entry(wb, role="output"))
        patch = {
            "quickbi_step_a_workbook": str(wb.resolve()),
            "quickbi_ready": False,
            "quickbi_merge_source": None,
            "quickbi_summary_workbook": None,
        }
        return ParseResult(
            output_files=outputs,
            verify_rows=[
                VerifyRow(
                    row_id="allocation.quickbi.excel_a",
                    severity="pass",
                    summary=(
                        "步骤 a：已生成不含 BU 的明细；请用 Excel 打开刷新公式、保存后上传到本渠道目录，再执行步骤 c"
                    ),
                    rule_ref="allocation.quickbi.excel_a",
                )
            ],
            warnings=list(scan.warnings),
            note=_verify_note_from_logs(logs, tail=60),
            metrics={"allocation_state_patch": patch, "lines": logs, **qb_metrics},
        )

    return ParseResult(
        verify_rows=[
            VerifyRow(
                row_id="allocation.quickbi.action",
                severity="warning",
                summary=f"未知 QuickBI action：{action}",
                rule_ref="allocation.quickbi.action",
            )
        ],
        warnings=[],
    )


def run_citihk_phase(ctx: ParseContext, opts: dict[str, Any]) -> ParseResult:
    action = str(opts.get("action") or "build").strip().lower()
    with_details = bool(opts.get("with_details", False))

    mapping = get_rules_allocation_citihk_mapping_csv_path()
    template = get_rules_allocation_citihk_pphk_template_path()
    if not mapping.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.mapping",
                    severity="pending",
                    summary=(
                        f"缺少 CitiHK 账户 mapping CSV：请将「账户对应主体分行mapping表.csv」放入 "
                        f"{mapping.parent}"
                    ),
                    rule_ref="allocation.citihk.mapping",
                )
            ],
            warnings=[],
        )
    if not template.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.template",
                    severity="pending",
                    summary="缺少 PPHK 模版 xlsx：请在规则页上传分摊基数模版（CitiHK）",
                    rule_ref="allocation.citihk.template",
                )
            ],
            warnings=[],
        )

    if action == "convert_csv":
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.csv",
                    severity="pending",
                    summary="已不再提供「CitiHK 转 CSV」。请按约定命名在目录中放置源表后再执行构建。",
                    rule_ref="allocation.citihk.csv",
                )
            ],
            warnings=[],
        )

    _ensure_quickbi_path()
    try:
        from ui_services import bases_folder as bf  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.import",
                    severity="warning",
                    summary=f"无法加载 allline bases_folder：{exc}",
                    rule_ref="allocation.citihk.import",
                )
            ],
            warnings=[str(exc)],
        )

    scan = bf.scan_bases_folder(ctx.extracted_dir)
    for note in scan.scan_notes:
        _log(ctx, f"[allocation scan] {note}")
    cdir = scan.citihk_dir
    if cdir is None:
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.dir",
                    severity="pending",
                    summary=(
                        "未识别到可用的 CitiHK 目录：需在某一文件夹（推荐 …/extracted/allocation_base/CITIHK/）"
                        "内放置恰好 3 个 2-Inbound*.csv（或 3 个 xlsx）、以及 4outbound 与资金流 slip 约定文件名。"
                    ),
                    rule_ref="allocation.citihk.dir",
                )
            ],
            warnings=list(scan.warnings),
        )

    logs: list[str] = []

    def progress(msg: str) -> None:
        logs.append(msg)
        _log(ctx, msg)

    outputs: list = []

    pkg = allocation_pkg_root()
    build_py = pkg / "CitiHKLine" / "build_citihk_bases.py"
    if not build_py.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.cli",
                    severity="warning",
                    summary=f"找不到 build_citihk_bases.py：{build_py}",
                    rule_ref="allocation.citihk.cli",
                )
            ],
            warnings=[],
        )

    out_workbook = ctx.output_dir / "CITIHK_PPHK_基数.xlsx"
    cmd = [
        sys.executable,
        "-u",
        str(build_py),
        "--citihk-dir",
        str(cdir.resolve()),
        "--mapping",
        str(mapping.resolve()),
        "--out",
        str(out_workbook.resolve()),
        "--template",
        str(template.resolve()),
        "--workers",
        "0",
    ]
    if with_details:
        cmd.append("--with-details")

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    ym = _task_period_to_yyyymm(getattr(ctx, "period", None))
    if ym:
        env["PECAUSE_CITIHK_REPORT_MONTH"] = ym
    ca_parent = _cost_allocation_parent(pkg)
    extra_paths = [str(pkg / "CitiHKLine"), str(pkg), str(ca_parent)]
    prev = env.get("PYTHONPATH", "")
    sep = os.pathsep
    env["PYTHONPATH"] = sep.join([*extra_paths, prev]).strip(sep)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        cwd=str(pkg / "CitiHKLine"),
    )
    if proc.stdout:
        for line in proc.stdout:
            progress(line.rstrip())
    code = proc.wait()
    if code != 0:
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.build",
                    severity="warning",
                    summary=f"CitiHK build 退出码 {code}",
                    rule_ref="allocation.citihk.build",
                )
            ],
            warnings=logs[-20:],
            note=_verify_note_from_logs(logs, tail=80),
            metrics={"lines": logs, "exit_code": code},
        )

    if not out_workbook.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.citihk.missing_out",
                    severity="warning",
                    summary="构建结束但未找到输出 xlsx",
                    rule_ref="allocation.citihk.missing_out",
                )
            ],
            warnings=logs[-10:],
        )

    outputs.append(make_file_entry(out_workbook, role="output"))
    patch = {
        "citihk_workbook": str(out_workbook.resolve()),
        "citihk_ready": True,
    }
    return ParseResult(
        output_files=outputs,
        verify_rows=[
            VerifyRow(
                row_id="allocation.citihk.build",
                severity="pass",
                summary=f"CitiHK 基数表已生成：{out_workbook.name}",
                rule_ref="allocation.citihk.build",
            )
        ],
        warnings=list(scan.warnings),
        note=_verify_note_from_logs(logs, tail=80),
        metrics={"allocation_state_patch": patch, "lines": logs},
    )


def run_merge_phase(ctx: ParseContext, opts: dict[str, Any]) -> ParseResult:
    alloc = ctx.metadata.get("allocation_task_state") or {}
    if not isinstance(alloc, dict):
        alloc = {}

    qb_raw = (
        alloc.get("quickbi_summary_workbook")
        or alloc.get("quickbi_merge_source")
        or alloc.get("quickbi_workbook")
    )
    ch_raw = alloc.get("citihk_workbook")
    qb_path = Path(qb_raw) if isinstance(qb_raw, str) and qb_raw.strip() else None
    ch_path = Path(ch_raw) if isinstance(ch_raw, str) and ch_raw.strip() else None
    has_qb = qb_path is not None and qb_path.is_file()
    has_ch = ch_path is not None and ch_path.is_file()

    merge_qb_replaced_with_summary = False
    if has_qb and qb_path is not None and "三表汇总" not in qb_path.name:
        summary_sibling = qb_path.parent / "收付款基数_QuickBI_三表汇总.xlsx"
        if summary_sibling.is_file():
            qb_path = summary_sibling
            merge_qb_replaced_with_summary = True

    if not has_qb and not has_ch:
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.merge.both_missing",
                    severity="pending",
                    summary=(
                        "合并缺少两侧产出：任务状态中无有效的 QuickBI 与 CitiHK 文件路径。"
                        "请至少成功执行一侧（模拟 QuickBI 或 CitiHK 构建），或确认产出文件仍在原路径。"
                    ),
                    rule_ref="allocation.merge.both_missing",
                )
            ],
            warnings=[],
        )

    out_name = str(opts.get("output_filename") or "收付款基数_合并_out.xlsx").strip()
    if not out_name.lower().endswith(".xlsx"):
        out_name += ".xlsx"
    out_path = ctx.output_dir / out_name

    logs: list[str] = []

    def progress(msg: str) -> None:
        logs.append(msg)
        _log(ctx, msg)

    progress(
        f"QuickBI：{str(qb_path.resolve()) if has_qb else '（未就绪/缺失，仅输出另一侧可合并数据或提示为空）'}"
    )
    if merge_qb_replaced_with_summary:
        progress(
            "（合并侧已从完整 QuickBI_out 切换为同目录「收付款基数_QuickBI_三表汇总.xlsx」）"
        )
    progress(
        f"CitiHK：{str(ch_path.resolve()) if has_ch else '（未就绪/缺失，跳过 CitiHK 笔数并入）'}"
    )

    try:
        stats = merge_allocation_workbooks(
            qb_path if has_qb else None,
            ch_path if has_ch else None,
            out_path,
        )
    except Exception as exc:  # noqa: BLE001
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.merge.fail",
                    severity="warning",
                    summary=f"合并失败：{exc}",
                    rule_ref="allocation.merge.fail",
                )
            ],
            warnings=[str(exc)],
            note=_verify_note_from_logs(logs, tail=None),
        )

    progress(f"合并完成 → {out_path}")
    if stats.get("missing_quickbi"):
        progress("提示：未使用 QuickBI 产出；若仅有 CitiHK，则汇总表为 CitiHK 可并入部分。")
    if stats.get("missing_citihk"):
        progress("提示：未使用 CitiHK 产出；汇总表仅为 QuickBI 汇总（VA 仅随 QuickBI）。")

    outputs = [make_file_entry(out_path.resolve(), role="output")]
    patch: dict[str, Any] = {
        "merge_output": str(out_path.resolve()),
        "merge_output_name": out_name,
        "merge_output_is_upload": False,
        "merge_output_uploaded_at": None,
        "merge_stats": stats,
    }
    summary = (
        f"入金汇总 {stats.get('final_in_rows')} 行 · "
        f"出金汇总 {stats.get('final_out_rows')} 行 · VA {stats.get('va_rows')} 行"
    )

    detail = {str(k): stats[k] for k in stats}
    verify_rows: list = []
    partial_msgs: list[str] = []
    if stats.get("missing_quickbi"):
        partial_msgs.append(
            "缺少 QuickBI 中间表：本次合并未并入 QuickBI 入金/出金/VA 汇总（若仅有 CitiHK，输出仅反映 CitiHK 可合并部分）"
        )
    if stats.get("missing_citihk"):
        partial_msgs.append(
            "缺少 CitiHK 产出：未并入笔数/交易量类列，汇总表等同 QuickBI 侧（或仅含 QuickBI 可提供 sheet）"
        )
    if partial_msgs:
        verify_rows.append(
            VerifyRow(
                row_id="allocation.merge.partial",
                severity="warning",
                summary=" · ".join(partial_msgs),
                rule_ref="allocation.merge.partial",
                detail=detail,
            )
        )
    verify_rows.append(
        VerifyRow(
            row_id="allocation.merge.ok",
            severity="pass",
            summary=summary,
            rule_ref="allocation.merge.ok",
            detail=detail,
        )
    )

    return ParseResult(
        output_files=outputs,
        verify_rows=verify_rows,
        warnings=list(partial_msgs),
        note=_verify_note_from_logs(logs, tail=None),
        metrics={"allocation_state_patch": patch},
    )


def _find_latest_summary_workbook(task_id: str) -> Path | None:
    """返回最新的成本汇总文件：优先直接上传的覆盖文件，其次扫描 final_merge runs。"""
    # 优先直接上传的覆盖文件
    upload_dir = get_task_dir(task_id) / "channels" / "final_merge" / "uploads"
    if upload_dir.is_dir():
        candidates = list(upload_dir.glob("*.xlsx"))
        if candidates:
            return max(candidates, key=lambda p: p.stat().st_mtime)

    runs_dir = get_task_dir(task_id) / "channels" / "final_merge" / "runs"
    if not runs_dir.is_dir():
        return None
    latest: Path | None = None
    latest_mtime = -1.0
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        for p in run_dir.glob("成本汇总_*_汇总.xlsx"):
            try:
                mt = p.stat().st_mtime
            except OSError:
                continue
            if mt > latest_mtime:
                latest = p
                latest_mtime = mt
    return latest


def run_cost_allocate_phase(ctx: ParseContext, opts: dict[str, Any]) -> ParseResult:
    action = str(opts.get("action") or "build").strip().lower()
    if action != "build":
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.action",
                    severity="warning",
                    summary=f"未知 cost_allocate action：{action}",
                    rule_ref="allocation.cost_allocate.action",
                )
            ],
            warnings=[],
        )

    summary_path = None
    raw = opts.get("summary_workbook")
    if isinstance(raw, str) and raw.strip():
        p = Path(raw).expanduser().resolve()
        if p.is_file():
            summary_path = p
    if summary_path is None:
        summary_path = _find_latest_summary_workbook(ctx.task_id)
    if summary_path is None:
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.summary",
                    severity="pending",
                    summary=(
                        "未找到成本汇总文件：请先执行「最终合并」产出 成本汇总_*_汇总.xlsx，"
                        "或在 allocation_options.summary_workbook 显式指定。"
                    ),
                    rule_ref="allocation.cost_allocate.summary",
                )
            ],
            warnings=[],
        )
    task_log(ctx.task_id, f"cost_allocate 输入汇总：{summary_path.name}", channel=ctx.channel_id)

    # 检查是否有直接上传/合并生成的基数合并表（用作 BASES_PATH，提供入金/出金/VA基数数据）
    alloc = ctx.metadata.get("allocation_task_state") or {}
    merge_base: Path | None = None
    merge_out_raw = alloc.get("merge_output")
    if isinstance(merge_out_raw, str) and merge_out_raw.strip():
        p = Path(merge_out_raw)
        if p.is_file() and p.suffix.lower() == ".xlsx":
            merge_base = p
            task_log(ctx.task_id, f"cost_allocate BASES_PATH：检测到基数合并表 {p.name}", channel=ctx.channel_id)

    return run_cost_allocate_with_pingpong(ctx=ctx, summary_workbook=summary_path, merge_base_workbook=merge_base)
