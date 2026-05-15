"""Allocation-base channel — QuickBI / CitiHK / merge phases + legacy inventory scan."""
from __future__ import annotations

from server.parsers.allocation.inventory import run_allocation_inventory
from server.parsers.allocation.ops import (
    run_citihk_phase,
    run_cost_allocate_phase,
    run_merge_phase,
    run_quickbi_phase,
)
from server.parsers.base import BaseParser, ParseContext, ParseResult, VerifyRow


class AllocationBaseParser(BaseParser):
    channel_id = "allocation_base"
    display_name = "分摊基数"
    output_filename = "allocation_base.csv"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        phase = (ctx.metadata.get("allocation_phase") or "").strip().lower()
        opts = ctx.metadata.get("allocation_options") or {}
        if not isinstance(opts, dict):
            opts = {}

        if not phase:
            return run_allocation_inventory(ctx=ctx)
        if phase == "quickbi":
            return run_quickbi_phase(ctx, opts)
        if phase == "citihk":
            return run_citihk_phase(ctx, opts)
        if phase == "merge":
            return run_merge_phase(ctx, opts)
        if phase == "cost_allocate":
            return run_cost_allocate_phase(ctx, opts)

        return ParseResult(
            verify_rows=[
                VerifyRow(
                    row_id="allocation.phase",
                    severity="warning",
                    summary=f"未知的分摊基数分区 allocation_phase={phase!r}",
                    rule_ref="allocation.phase",
                )
            ],
            warnings=[],
            note="请在请求体中指定 allocation_phase：quickbi | citihk | merge | cost_allocate（留空则执行库存扫描）。",
        )
