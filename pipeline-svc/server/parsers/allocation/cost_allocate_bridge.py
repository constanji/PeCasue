"""在任务 run 目录内执行 ``cost_allocate.main``，并产出 ParseResult。

脚本路径（优先）：
1. 环境变量 ``PECAUSE_COST_ALLOCATE_SCRIPT``
2. ``pipeline-svc/vendor/allocation_cost/cost_allocate.py``（随仓库分发）
3. 过渡期回退：仓库上级目录的 ``pingpong-master/script/allocation/cost_allocate.py``
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from server.core.paths import get_rules_cost_allocate_workbook_path
from server.core.task_logger import task_log
from server.parsers.base import ParseContext, ParseResult, VerifyRow, make_file_entry

_RX_SUMMARY_PERIOD = re.compile(r"成本汇总_(\d{6})_汇总\.xlsx$", re.IGNORECASE)


def _pipeline_svc_root() -> Path:
    # server/parsers/allocation/cost_allocate_bridge.py -> …/pipeline-svc
    return Path(__file__).resolve().parents[3]


def _resolve_cost_allocate_script_path() -> Path | None:
    env = (os.environ.get("PECAUSE_COST_ALLOCATE_SCRIPT") or "").strip()
    if env:
        p = Path(env).expanduser().resolve()
        if p.is_file():
            return p
    vendored = _pipeline_svc_root() / "vendor" / "allocation_cost" / "cost_allocate.py"
    if vendored.is_file():
        return vendored.resolve()
    legacy = _pipeline_svc_root().parent / "pingpong-master" / "script" / "allocation" / "cost_allocate.py"
    if legacy.is_file():
        return legacy.resolve()
    return None


def _summary_period_from_name(summary_path: Path) -> int | None:
    m = _RX_SUMMARY_PERIOD.match(summary_path.name)
    if not m:
        return None
    return int(m.group(1))


def _period_yyyymm_from_ctx(period: Optional[str]) -> int | None:
    if period is None:
        return None
    s = str(period).strip().replace("-", "").replace("/", "")
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) >= 6:
        return int(digits[:6])
    return None


def _import_cost_allocate_module(script_path: Path) -> Any:
    name = "_pecause_cost_allocate_runtime"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载 cost_allocate：{script_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_cost_allocate_with_pingpong(
    *,
    ctx: ParseContext,
    summary_workbook: Path,
    merge_base_workbook: Optional[Path] = None,
) -> ParseResult:
    script = _resolve_cost_allocate_script_path()
    if script is None:
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.script",
                    severity="pending",
                    summary=(
                        "未找到 cost_allocate.py：确认 pipeline-svc/vendor/allocation_cost/cost_allocate.py "
                        "已同步，或设置 PECAUSE_COST_ALLOCATE_SCRIPT 指向该文件。"
                    ),
                    rule_ref="allocation.cost_allocate.script",
                )
            ],
            warnings=[],
        )

    # TEMPLATE_PATH 始终指向规则库完整模板（含 inbound 成本 等输出结构 sheet）
    template_path = get_rules_cost_allocate_workbook_path()
    if not template_path.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.template",
                    severity="pending",
                    summary=f"规则库缺少分摊模版文件：{template_path}",
                    rule_ref="allocation.cost_allocate.template",
                )
            ],
            warnings=[],
        )

    # BASES_PATH：若用户上传了基数表，则用于 load_bases / load_mapping；否则回退规则库模板
    bases_path: Path | None = None
    if merge_base_workbook is not None and merge_base_workbook.is_file():
        bases_path = merge_base_workbook
        task_log(ctx.task_id, f"cost_allocate BASES_PATH：使用上传基数合并表 {bases_path.name}", channel=ctx.channel_id)
    elif merge_base_workbook is not None:
        task_log(
            ctx.task_id,
            f"cost_allocate：指定基数合并表 {merge_base_workbook} 不存在，基数读取回退规则库模板",
            channel=ctx.channel_id,
        )

    summary_path = summary_workbook.resolve()
    if not summary_path.is_file():
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.summary_path",
                    severity="pending",
                    summary=f"成本汇总文件不存在：{summary_path}",
                    rule_ref="allocation.cost_allocate.summary_path",
                )
            ],
            warnings=[],
        )

    period_file = _summary_period_from_name(summary_path)
    period_ctx = _period_yyyymm_from_ctx(ctx.period)
    # 任务设定的期次优先；文件名期次仅在任务未设期次时作备用。
    if period_ctx is not None:
        period = period_ctx
        if period_file is not None and period_file != period_ctx:
            task_log(
                ctx.task_id,
                (
                    f"cost_allocate：汇总文件名期次为 {period_file}，与任务 period={ctx.period!r}（解析为 {period_ctx}）"
                    "不一致；已以任务期次为准。"
                ),
                channel=ctx.channel_id,
            )
    elif period_file is not None:
        period = period_file
    else:
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.period",
                    severity="pending",
                    summary=(
                        "无法确定分摊期次：请使用「成本汇总_YYYYMM_汇总.xlsx」命名汇总文件，"
                        "或在任务 metadata 中填写 period（如 202602）。"
                    ),
                    rule_ref="allocation.cost_allocate.period",
                )
            ],
            warnings=[],
        )

    # 前置校验：读汇总文件实际 month 列，若数据期次与 period 不符则报错（而非静默产出全零）
    try:
        _sm_check = pd.read_excel(summary_path, sheet_name=0, engine="calamine", usecols=["month"])
        actual_months = set(
            int(m)
            for m in pd.to_numeric(_sm_check["month"], errors="coerce").dropna().unique()
        )
        if actual_months and period not in actual_months:
            actual_str = "、".join(str(m) for m in sorted(actual_months))
            return ParseResult(
                verify_rows=[
                    VerifyRow(
                        row_id="allocation.cost_allocate.period_mismatch",
                        severity="warning",
                        summary=(
                            f"汇总文件「{summary_path.name}」实际数据期次为 {actual_str}，"
                            f"与当前分摊期次 {period} 不符——分摊将产出全零。"
                            "请重新执行「最终合并」以生成正确期次的汇总文件，再执行分摊。"
                        ),
                        rule_ref="allocation.cost_allocate.period_mismatch",
                    )
                ],
                warnings=[f"汇总文件期次 {actual_str} ≠ 分摊期次 {period}，请重新执行最终合并。"],
            )
    except Exception:
        pass  # 读取失败时跳过校验，交给后续 cost_allocate.main 自行处理

    out_dir = ctx.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        mod = _import_cost_allocate_module(script)
    except Exception as e:
        task_log(ctx.task_id, f"cost_allocate 载入脚本失败：{e}", channel=ctx.channel_id)
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.import",
                    severity="warning",
                    summary=f"载入 cost_allocate 失败：{e}",
                    rule_ref="allocation.cost_allocate.import",
                )
            ],
            warnings=[str(e)],
        )

    # 与 pingpong 脚本顶部常量对齐：输出与附属 json 写入当前 run 目录
    mod.BASE_DIR = out_dir
    mod.TEMPLATE_PATH = template_path
    # BASES_PATH：若为上传基数表则单独覆盖，否则与 TEMPLATE_PATH 相同
    mod.BASES_PATH = bases_path if bases_path is not None else template_path
    mod.SUMMARY_PATH = summary_path
    mod.PERIOD = int(period)
    mod.OUT_PATH = out_dir / f"成本分摊_{period}_输出.xlsx"
    mod.SPECIAL_RULE_LOG_PATH = out_dir / f"成本分摊_{period}_特殊规则命中.json"

    try:
        mod.main()
    except Exception as e:
        task_log(ctx.task_id, f"Run FAILED: {e}", channel=ctx.channel_id)
        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.cost_allocate.run",
                    severity="warning",
                    summary=str(e),
                    rule_ref="allocation.cost_allocate.run",
                )
            ],
            warnings=[str(e)],
        )

    output_files: list = []
    if mod.OUT_PATH.is_file():
        output_files.append(make_file_entry(mod.OUT_PATH, role="output"))
    unmapped = out_dir / "_allocate_unmapped_bills.json"
    if unmapped.is_file():
        output_files.append(make_file_entry(unmapped, role="auxiliary"))
    if mod.SPECIAL_RULE_LOG_PATH.is_file():
        output_files.append(make_file_entry(mod.SPECIAL_RULE_LOG_PATH, role="auxiliary"))

    return ParseResult(
        output_files=output_files,
        verify_rows=[
            VerifyRow(
                row_id="allocation.cost_allocate.ok",
                severity="pass",
                summary=f"成本分摊已写出：{mod.OUT_PATH.name}（期次 {period}）",
                rule_ref="allocation.cost_allocate.ok",
            )
        ],
        warnings=[],
        metrics={"cost_allocate_period": period, "script": str(script)},
    )
