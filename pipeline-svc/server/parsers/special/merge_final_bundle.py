"""Final merge — collect all verified channel outputs and run cost_summary."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from server.core.pipeline_state import StateManager
from server.core.paths import get_rules_files_dir
from server.core.task_logger import task_log
from server.parsers.base import ParseContext, ParseResult, VerifyRow, make_file_entry
from vendor.special_channel.common import lookup_usd_fx_rate_series, load_usd_fx_rates_from_csv
from vendor.special_channel.cost_summary import CostSummaryInput, run_cost_summary

_logger = logging.getLogger(__name__)

# 渠道 ID → 日志用中文名
_CH_LABEL: dict[str, str] = {
    "bill": "账单",
    "own_flow": "自有流水",
    "customer": "客资流水",
    "special_transfer": "特殊来源·内转",
    "special_ach_refund": "特殊来源·ACH退票",
    "special_op_refund": "特殊来源·OP退票",
    "special_op_incoming": "特殊来源·OP入账",
    "cn_jp": "境内&日本通道",
}

# Channels whose latest verified xlsx we try to collect
_COLLECT_CHANNELS = (
    "bill",
    "own_flow",
    "customer",
    "special_transfer",
    "special_ach_refund",
    "special_op_refund",
    "special_op_incoming",
    "cn_jp",
)


def _run_status_str(st: Any) -> str:
    v = getattr(st, "value", None)
    return str(v) if isinstance(v, str) else str(st)


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


def _infer_period_from_task(task_id: str) -> str:
    """Try to infer YYYYMM period from task state."""
    try:
        state = StateManager.load_state(task_id)
        if state:
            # period 直接在 TaskState 顶层
            period = getattr(state, "period", None)
            if period:
                s = str(period).strip().replace("-", "").replace("/", "")
                digits = "".join(c for c in s if c.isdigit())
                if len(digits) >= 6:
                    return digits[:6]
    except Exception:
        pass
    return "202602"


def run_final_merge(ctx: ParseContext) -> ParseResult:
    """Collect all verified channel outputs and produce a combined cost summary."""
    task_id = ctx.task_id
    channel_id = ctx.channel_id
    missing: list[str] = []
    inputs = CostSummaryInput()

    task_log(
        task_id,
        "最终合并：从任务状态中依次查找各渠道「最近一次 verified/confirmed」产出的 output xlsx（不是边跑边追加同一文件）；"
        "路径收集完毕后一次性送入成本汇总，明细层纵向 concat，再写出明细/汇总/合并三本 xlsx。",
        channel=channel_id,
    )

    # Map channel_id -> attribute on CostSummaryInput
    _channel_map = {
        "bill": "bill",
        "own_flow": "own",
        "customer": "cust",
        "special_transfer": "special_transfer",
        "special_ach_refund": "special_ach_refund",
        "special_op_refund": "special_op_refund",
        "special_op_incoming": "special_op_incoming",
        "cn_jp": "cn_jp",
    }

    for ch_id, attr in _channel_map.items():
        label = _CH_LABEL.get(ch_id, ch_id)
        task_log(task_id, f"正在解析渠道引用：「{label}」（{ch_id}）…", channel=channel_id)
        p = _latest_verified_output_xlsx(task_id, ch_id)
        if p:
            setattr(inputs, attr, p)
            _logger.info("final_merge: %s → %s", ch_id, p.name)
            task_log(
                task_id,
                f"「{label}」已挂载参与汇总：{p.name}",
                channel=channel_id,
            )
        else:
            missing.append(ch_id)
            _logger.info("final_merge: %s → 无已校验产出", ch_id)
            task_log(
                task_id,
                f"「{label}」无可用已校验产出（skipped）",
                level="WARNING",
                channel=channel_id,
            )

    # Load FX rates
    task_log(task_id, "正在加载规则目录中的汇率 CSV …", channel=channel_id)
    rules_files_dir = get_rules_files_dir()
    fx_df = load_usd_fx_rates_from_csv(rules_files_dir)
    if not fx_df.empty:
        # 直接使用上传的汇率文件中最新月份的数据，不依赖 fx_month_label
        target_month = str(fx_df["month"].max())
        task_log(task_id, f"汇率月份：使用汇率 CSV 中最新月份 {target_month}", channel=channel_id)
        month_str_series = fx_df["month"].astype(str)
        month_rates = fx_df[month_str_series == target_month]
        inputs.fx_map = dict(zip(
            month_rates["currency"].str.upper(),
            month_rates["usd_rate"],
        ))
        inputs.fx_map["USD"] = 1.0
    else:
        inputs.fx_map = {"USD": 1.0}

    period = _infer_period_from_task(task_id)
    output_dir = ctx.output_dir

    task_log(
        task_id,
        f"开始成本汇总（period={period}）；各渠道 DataFrame 加载与合并见后续逐条日志。",
        channel=channel_id,
    )

    def _detail(msg: str) -> None:
        task_log(task_id, msg, channel=channel_id)

    result = run_cost_summary(inputs, output_dir, period=period, on_step=_detail)

    outputs = []
    if result.get("success"):
        for key in ("detail_path", "summary_path", "combined_path"):
            p = result.get(key)
            if p:
                outputs.append(make_file_entry(Path(p), role="output"))
                task_log(task_id, f"已生成产出文件：{Path(p).name}", channel=channel_id)

    verify_rows = []
    if result.get("success"):
        verify_rows.append(VerifyRow(
            row_id="final_merge.ok",
            severity="pass" if not result.get("errors") else "warning",
            summary=(
                f"成本汇总完成：明细 {result['detail_rows']} 行，"
                f"模板口径 {result['summary_rows']} 行，"
                f"总 USD ${result['total_usd']:,.2f}，"
                f"加载 {len(result['loaded_channels'])} 个渠道"
            ),
            rule_ref="final_merge.summary",
            detail=result,
        ))
    else:
        verify_rows.append(VerifyRow(
            row_id="final_merge.fail",
            severity="warning",
            summary=f"成本汇总失败：{result.get('error', 'unknown')}",
            rule_ref="final_merge.summary",
        ))

    warnings = []
    if missing:
        warnings.append(f"以下渠道无已校验产出已跳过：{', '.join(missing)}")
    for e in result.get("errors", []):
        warnings.append(str(e))

    return ParseResult(
        output_files=outputs,
        verify_rows=verify_rows,
        warnings=warnings,
        note=f"最终合并（period={period}）：{len(result.get('loaded_channels', []))}/{len(_channel_map)} 渠道",
        metrics={
            "loaded_channels": result.get("loaded_channels", []),
            "missing_channels": missing,
            "detail_rows": result.get("detail_rows", 0),
            "summary_rows": result.get("summary_rows", 0),
            "total_usd": result.get("total_usd", 0),
        },
    )