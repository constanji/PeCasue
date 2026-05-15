"""Customer-flow (客资流水) parser — real implementation.

Customer-funded channels (Coupang, Lazada, Souche …) ship per-channel xls/
csv files with heterogeneous layouts. Both upstream projects converge on the
same canonical schema (Account / Description / Volume / Charge / CCY / 来源
文件) before mapping. We re-use the shared dispatcher to walk every input
file, extract canonical rows by alias, and emit ``customer_canonical.xlsx``.

Verify rows surface per file:
    * ``pass`` if ≥ 6 canonical columns matched.
    * ``warning`` if 3-5 matched (likely a new layout — Agent should propose
      an alias rule).
    * ``warning`` if < 3 matched or read error.
"""
from __future__ import annotations

from typing import List

from server.core.task_logger import task_log
from server.parsers._shared.dispatcher import (
    SLIM_COLUMN_ALIASES,
    normalize_file,
    write_canonical,
    write_canonical_xlsx,
)
from server.parsers.customer.enriched import try_write_customer_flow_output
from server.parsers.base import (
    BaseParser,
    ParseContext,
    ParseResult,
    VerifyRow,
    make_file_entry,
)


def _build_verify(channel: str, outcomes) -> List[VerifyRow]:
    rows: List[VerifyRow] = []
    for o in outcomes:
        if o.error:
            rows.append(
                VerifyRow(
                    row_id=f"{channel}.read.{o.file.name}",
                    severity="warning",
                    summary=f"{o.file.name}: 读取失败 — {o.error}"[:200],
                    rule_ref=f"{channel}.dispatcher.read",
                    file_ref=o.file.name,
                )
            )
            continue
        sev = (
            "pass"
            if o.matched_columns >= max(6, len(SLIM_COLUMN_ALIASES) - 4)
            else "warning"
        )
        rows.append(
            VerifyRow(
                row_id=f"{channel}.match.{o.file.name}",
                severity=sev,
                summary=(
                    f"{o.file.name}{' / ' + o.sheet if o.sheet else ''}: "
                    f"命中 {o.matched_columns} 列, "
                    f"未识别 {len(o.missing_canonical)} 列, 行数 {len(o.rows)}"
                ),
                rule_ref=f"{channel}.dispatcher.column_alias",
                file_ref=o.file.name,
                detail={
                    "missing": o.missing_canonical,
                    "sheet": o.sheet,
                    "row_count": len(o.rows),
                },
            )
        )
    return rows


class CustomerParser(BaseParser):
    channel_id = "customer"
    display_name = "客资流水"
    output_filename = "customer_canonical.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        sources = self.list_source_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=sources)
        outputs = [make_file_entry(manifest_path, role="manifest")]

        if not sources:
            return ParseResult(
                output_files=outputs,
                verify_rows=[
                    VerifyRow(
                        row_id="customer_empty",
                        severity="pending",
                        summary="未发现客资流水源文件",
                        rule_ref="customer.directory.scan",
                    )
                ],
                warnings=["客资流水目录为空"],
                note="empty extracted dir",
                metrics={"source_count": 0},
            )

        task_log(
            ctx.task_id,
            f"客资流水解析：输入文件 {len(sources)} 个，开始逐文件归一化…",
            channel=ctx.channel_id,
        )
        outcomes = []
        for idx, p in enumerate(sources, start=1):
            task_log(
                ctx.task_id,
                f"  [{idx}/{len(sources)}] 正在处理 {p.name} …",
                channel=ctx.channel_id,
            )
            o = normalize_file(p)
            hint = (
                "Pingpong·渠道对账单"
                if (o.sheet and ("渠道对账单" in str(o.sheet)))
                else "通用 SLIM"
            )
            task_log(
                ctx.task_id,
                f"  [{idx}/{len(sources)}] 完成 {p.name} · {hint} · "
                f"sheet={o.sheet or '—'} · 行数={len(o.rows)} · 命中列={o.matched_columns}",
                channel=ctx.channel_id,
            )
            outcomes.append(o)
        all_rows = []
        for o in outcomes:
            all_rows.extend(o.rows)

        result_xlsx = ctx.output_dir / self.output_filename
        result_csv = ctx.output_dir / "customer_canonical.csv"
        write_canonical_xlsx(all_rows, result_xlsx)
        write_canonical(all_rows, result_csv)
        outputs.append(make_file_entry(result_xlsx, role="midfile"))
        outputs.append(make_file_entry(result_csv, role="midfile"))
        task_log(
            ctx.task_id,
            f"客资：canonical 表已写出（{len(all_rows):,} 行），开始做规则 enrichment…",
            channel=ctx.channel_id,
        )

        flow_out = ctx.output_dir / "customer_flow_output.xlsx"
        warnings: List[str] = []

        def _enrich_prog(msg: str) -> None:
            task_log(ctx.task_id, msg, channel=ctx.channel_id)

        enrich_warn, enrich_n, fx_rate_notes = try_write_customer_flow_output(
            sources,
            flow_out,
            progress_log=_enrich_prog,
        )
        if enrich_warn:
            warnings.append(enrich_warn)
        else:
            outputs.append(make_file_entry(flow_out, role="output"))
            task_log(
                ctx.task_id,
                f"客资规则明细：已写入 customer_flow_output.xlsx · {enrich_n} 行（rules/files/mapping + fx）。",
                channel=ctx.channel_id,
            )
            if fx_rate_notes:
                warnings.extend(fx_rate_notes)

        for o in outcomes:
            if o.error:
                warnings.append(f"{o.file.name}: {o.error}")
            elif o.matched_columns < 3:
                warnings.append(
                    f"{o.file.name}: 仅命中 {o.matched_columns} 列，疑似新格式"
                )

        verify_rows = _build_verify("customer", outcomes)
        if fx_rate_notes and not enrich_warn:
            verify_rows.append(
                VerifyRow(
                    row_id="customer.fx.unmatched_rates",
                    severity="warning",
                    summary="；".join(fx_rate_notes),
                    rule_ref="customer.enriched.fx_rate_lookup",
                    detail={
                        "hint": "请在 rules/files/fx 的折算率表中补充对应币种（或重新导入汇率 Excel），并重跑客资渠道",
                    },
                )
            )
        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note=(
                "customer_canonical.xlsx/csv：跨渠道 SLIM 英文中间表（Debit−Credit 原币种）。"
                "customer_flow_output.xlsx：读取 rules/files/mapping 下三张客资表与 fx/各种货币对美元折算率，"
                "按 pingpong-master/script/customer/all.py 同口径生成中文明细（入账期间、主体、分行维度、USD金额、入账科目、费项、类型等）；"
                "若仅为单列币种折算率且无「日期」，USD 折算账期优先 RuleStore FX.meta.fx_month_label，其次取自账单 BillDate。"
            ),
            metrics={
                "source_count": len(sources),
                "row_count": len(all_rows),
                "enriched_row_count": enrich_n if not enrich_warn else 0,
                "warning_files": sum(1 for o in outcomes if o.matched_columns < 6),
            },
        )
