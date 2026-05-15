"""Domestic + Japan (境内 & 日本通道) parser — passthrough.

CnJp files are **not** column-normalised.  Each source file is copied
directly to the output directory unchanged (``shutil.copy2``), matching
pingpong-master's behaviour which simply ``shutil.copy2(source, output)``.
"""
from __future__ import annotations

import shutil
from typing import List

from server.parsers.base import (
    BaseParser,
    ParseContext,
    ParseResult,
    VerifyRow,
    make_file_entry,
)


class CnJpParser(BaseParser):
    channel_id = "cn_jp"
    display_name = "境内 & 日本通道"
    output_filename = "cn_jp_canonical.xlsx"

    def parse(self, *, ctx: ParseContext) -> ParseResult:
        sources = self.list_source_files(ctx.extracted_dir)
        manifest_path = self.write_manifest(ctx, sources=sources)
        outputs = [make_file_entry(manifest_path, role="manifest")]
        if not sources:
            return ParseResult(
                output_files=outputs,
                verify_rows=[
                    VerifyRow(
                        row_id="cn_jp_empty",
                        severity="pending",
                        summary="未发现境内 / 日本源文件",
                        rule_ref="cn_jp.directory.scan",
                    )
                ],
                warnings=["境内 / 日本目录为空"],
                note="empty extracted dir",
                metrics={"source_count": 0},
            )

        verify_rows: List[VerifyRow] = []
        warnings: List[str] = []
        copied = 0

        for src in sources:
            dest = ctx.output_dir / src.name
            try:
                ctx.output_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
                copied += 1
                verify_rows.append(
                    VerifyRow(
                        row_id=f"cn_jp.copy.{src.name}",
                        severity="pass",
                        summary=f"{src.name}: 透传完成 ({dest.stat().st_size} bytes)",
                        rule_ref="cn_jp.passthrough",
                        file_ref=src.name,
                    )
                )
                outputs.append(make_file_entry(dest, role="output"))
            except Exception as exc:
                warnings.append(f"{src.name}: 透传失败 — {exc}")
                verify_rows.append(
                    VerifyRow(
                        row_id=f"cn_jp.copy_err.{src.name}",
                        severity="warning",
                        summary=f"{src.name}: 透传失败 — {exc}"[:200],
                        rule_ref="cn_jp.passthrough",
                        file_ref=src.name,
                    )
                )

        return ParseResult(
            output_files=outputs,
            verify_rows=verify_rows,
            warnings=warnings,
            note=f"境内 & 日本通道：透传模式，复制 {copied}/{len(sources)} 个文件。",
            metrics={"source_count": len(sources), "copied_count": copied},
        )