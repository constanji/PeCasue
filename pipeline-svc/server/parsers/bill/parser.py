"""Bill (账单) parser — real implementation.

Wraps the vendored ``server.parsers._legacy.zhangdan.all.generate_summary``
which iterates ``extracted_dir/<bank-folder>/`` and dispatches each bank to
its module (``citi``, ``jpm``, ``ewb`` …). The legacy code returns a single
``{bank}_summary.xlsx`` aggregating all rows after column alignment + template
mapping.

Outputs registered with the orchestrator:
    - ``bill_summary.xlsx`` — final aligned + mapped result (Human download).
    - ``manifest.json``      — per-source-file inventory.
    - ``per-bank xlsx``      — one mid-file per bank for traceability.

Verify rows surface:
    - ``bill.bank.{key}`` — pass / warning per bank folder (dispatch result).
    - ``bill.unmatched``  — warning for folders we could not map to any bank.
"""
from __future__ import annotations

import contextlib
import io
import os
from typing import Iterable, List

from server.parsers._legacy.zhangdan import all as _legacy
from server.parsers.bill.log_report import bill_merge_report_payload
from server.parsers.base import (
    BaseParser,
    ParseContext,
    ParseResult,
    VerifyRow,
    make_file_entry,
)


def _bank_folders(extracted_dir) -> Iterable[str]:
    if not extracted_dir.exists():
        return []
    return [p.name for p in extracted_dir.iterdir() if p.is_dir()]


class BillParser(BaseParser):
    channel_id = "bill"
    display_name = "账单"
    output_filename = "bill_summary.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        sources = self.list_source_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(
            ctx,
            sources=sources,
            extras={"bank_folders": list(_bank_folders(ctx.extracted_dir))},
        )

        # legacy.generate_summary writes per-bank xlsx into midfile_dir AND a
        # final aggregated xlsx at output_file. We capture stdout to surface
        # the per-bank dispatch lines as verify rows + warnings.
        midfile_dir = ctx.output_dir / "midfile"
        midfile_dir.mkdir(parents=True, exist_ok=True)
        result_path = ctx.output_dir / self.output_filename

        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                _legacy.generate_summary(
                    str(ctx.extracted_dir), str(result_path), midfile_dir=str(midfile_dir)
                )
        except Exception as exc:  # noqa: BLE001
            return ParseResult(
                output_files=[make_file_entry(manifest_path, role="manifest")],
                verify_rows=[
                    VerifyRow(
                        row_id="bill_legacy_exception",
                        severity="warning",
                        summary=f"账单解析异常: {type(exc).__name__}: {exc}",
                        rule_ref="bill.legacy.generate_summary",
                    )
                ],
                warnings=[f"legacy.generate_summary 抛出 {exc}"],
                note="账单解析异常，请检查源文件目录结构是否符合按银行分文件夹的约定。",
                metrics={"source_count": len(sources)},
            )
        log = buf.getvalue()

        # Surface per-bank verify rows by parsing legacy stdout markers.
        verify_rows: List[VerifyRow] = []
        warnings: List[str] = []
        for line in log.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") and "]" in stripped:
                bank_key = stripped.split("]", 1)[0].strip("[").lower()
                msg = stripped.split("]", 1)[1].strip()
                sev = "pass"
                if "未提取到数据" in msg or "失败" in msg or "异常" in msg:
                    sev = "warning"
                    warnings.append(stripped)
                elif "成功提取" not in msg and "开始解析" not in msg:
                    # generic info line — not a verify entry
                    continue
                verify_rows.append(
                    VerifyRow(
                        row_id=f"bill.bank.{bank_key}.{len(verify_rows)}",
                        severity=sev,
                        summary=msg[:200],
                        rule_ref=f"bill.bank.{bank_key}",
                    )
                )
            elif "未匹配到任何已知的银行" in stripped:
                warnings.append(stripped)
                verify_rows.append(
                    VerifyRow(
                        row_id=f"bill.unmatched.{len(verify_rows)}",
                        severity="warning",
                        summary=stripped[:200],
                        rule_ref="bill.folder.match",
                    )
                )

        outputs = [make_file_entry(manifest_path, role="manifest")]
        if result_path.exists():
            outputs.append(make_file_entry(result_path, role="output"))
        else:
            warnings.append("legacy.generate_summary 未生成最终汇总 xlsx。")
        for f in sorted(midfile_dir.glob("*_temp.xlsx")):
            outputs.append(make_file_entry(f, role="midfile"))

        # Persist the captured legacy log next to the outputs for traceability.
        log_path = ctx.output_dir / "legacy_stdout.log"
        log_path.write_text(log, encoding="utf-8")
        outputs.append(make_file_entry(log_path, role="log"))

        if not sources:
            verify_rows.append(
                VerifyRow(
                    row_id="bill_empty",
                    severity="pending",
                    summary="未发现账单源文件",
                    rule_ref="bill.directory.scan",
                )
            )

        bill_report = bill_merge_report_payload(log)

        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note="使用 zhangdan 银行分发器；绿区优先读随包「模版.xlsx」，若无则回退 rules/files/mapping 与 fx CSV（与前端规则上传一致）。",
            metrics={
                "source_count": len(sources),
                "bank_folder_count": len(list(_bank_folders(ctx.extracted_dir))),
                "output_exists": result_path.exists(),
                "bill_merge_report": bill_report,
            },
        )
