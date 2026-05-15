"""Legacy allocation scan: bucket sources and emit flat allocation_base.csv (PeCause Phase-3 preview)."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from server.parsers.base import BaseParser, ParseContext, ParseResult, VerifyRow, make_file_entry


def _classify(name: str) -> str:
    """与 vendor ``bases_folder`` 关键字口径对齐（QuickBI：入金/出金/VA；CitiHK：Inbound 等）。"""
    n = name.lower()
    if "入金" in name or "finance_channel_inbound" in n or ("inbound_sm" in n and "quickbi" in n):
        return "quickbi_inbound"
    if "出金" in name or "finance_channel_outbound" in n or ("outbound_sm" in n and "quickbi" in n):
        return "quickbi_outbound"
    if (
        "finance_channel_valid_va" in n
        or "valid_va_sm" in n
        or "vaads_quickbi" in n
        or ("VA" in name and "quickbi" in n)
    ):
        return "quickbi_va"
    if n.startswith("2-inbound"):
        return "citihk_inbound"
    if "inbound" in n and "finance_channel" not in n:
        return "citihk_inbound"
    if n.startswith("4outbound"):
        return "citihk_outbound"
    if "outbound" in n and "finance_channel" not in n:
        return "citihk_outbound"
    if "资金流slip" in name or "资金流slip" in n.replace(" ", "") or "资金流" in name:
        return "citihk_slip"
    return "other"


def _amount_columns(cols) -> List[str]:
    out: List[str] = []
    for c in cols:
        s = str(c).strip().lower()
        if s in {"amount", "usd金额", "usd_amount", "总金额", "金额"}:
            out.append(c)
        elif "金额" in str(c) or "amount" in s:
            out.append(c)
    return out


def _branch_column(cols):
    for c in cols:
        s = str(c).strip().lower()
        if s in {"branch", "branch_name", "分行", "分行维度"}:
            return c
    return None


def _channel_column(cols):
    for c in cols:
        s = str(c).strip().lower()
        if s in {"channel", "渠道", "channel_name", "channel name"}:
            return c
    return None


def _read_any(path: Path) -> List[pd.DataFrame]:
    suf = path.suffix.lower()
    if suf in (".xlsx", ".xls", ".xlsm"):
        try:
            xls = pd.ExcelFile(path)
            return [xls.parse(s) for s in xls.sheet_names]
        except Exception:
            return []
    if suf == ".csv":
        try:
            return [pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")]
        except Exception:
            try:
                return [pd.read_csv(path, encoding="gbk", on_bad_lines="skip")]
            except Exception:
                return []
    return []


def run_allocation_inventory(*, ctx: ParseContext, output_filename: str = "allocation_base.csv") -> ParseResult:
    sources = [
        p for p in BaseParser.list_source_files(ctx.extracted_dir) if not p.name.startswith("~$")
    ]
    manifest_path = ctx.output_dir / "manifest_sources.txt"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("\n".join(sorted(str(p.relative_to(ctx.extracted_dir)) for p in sources)), encoding="utf-8")
    outputs = [make_file_entry(manifest_path, role="manifest")]

    if not sources:
        return ParseResult(
            output_files=outputs,
            verify_rows=[
                VerifyRow(
                    row_id="allocation_empty",
                    severity="pending",
                    summary="未上传 QuickBI 入金/出金/VA 与 CitiHK 数据",
                    rule_ref="allocation.directory.scan",
                )
            ],
            warnings=["分摊基数目录为空"],
            note="empty extracted dir",
            metrics={"source_count": 0},
        )

    rows: List[Dict[str, Any]] = []
    verify_rows: List[VerifyRow] = []
    warnings: List[str] = []
    bucket_counts: Dict[str, int] = {}

    for src in sources:
        try:
            rel = src.relative_to(ctx.extracted_dir).as_posix()
        except ValueError:
            rel = src.name
        kind = _classify(src.name)
        bucket_counts[kind] = bucket_counts.get(kind, 0) + 1
        dfs = _read_any(src)
        if not dfs:
            warnings.append(f"{rel}: 无法读取或为空")
            verify_rows.append(
                VerifyRow(
                    row_id=f"allocation.read.{rel}",
                    severity="warning",
                    summary=f"{rel}: 读取失败 / 空表",
                    rule_ref="allocation.read",
                    file_ref=rel,
                )
            )
            continue

        file_total = 0
        file_amount = 0.0
        for df in dfs:
            if df.empty:
                continue
            amt_cols = _amount_columns(df.columns)
            br_col = _branch_column(df.columns)
            ch_col = _channel_column(df.columns)
            for _, raw in df.iterrows():
                rec: Dict[str, Any] = {
                    "Source File": rel,
                    "Bucket": kind,
                    "Branch": "" if br_col is None else (raw.get(br_col) or ""),
                    "Channel": "" if ch_col is None else (raw.get(ch_col) or ""),
                }
                primary_amount = 0.0
                for ac in amt_cols:
                    v = raw.get(ac)
                    if isinstance(v, (int, float)) and not pd.isna(v):
                        rec[str(ac)] = float(v)
                        if not primary_amount:
                            primary_amount = float(v)
                rec["Amount"] = primary_amount
                rows.append(rec)
                file_total += 1
                file_amount += primary_amount
        verify_rows.append(
            VerifyRow(
                row_id=f"allocation.{kind}.{rel}",
                severity="pass",
                summary=f"{rel} → {kind}: {file_total} 行 · 金额合计 {file_amount:,.2f}",
                rule_ref=f"allocation.bucket.{kind}",
                file_ref=rel,
                detail={"row_count": file_total, "bucket": kind},
            )
        )

    result_csv = ctx.output_dir / output_filename
    fieldnames = ["Source File", "Bucket", "Branch", "Channel", "Amount"]
    with open(result_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    outputs.append(make_file_entry(result_csv, role="output"))

    result_xlsx = ctx.output_dir / "allocation_base.xlsx"
    pd.DataFrame(rows, columns=fieldnames).to_excel(result_xlsx, index=False, engine="openpyxl")
    outputs.append(make_file_entry(result_xlsx, role="output"))

    if "other" in bucket_counts:
        warnings.append(f"{bucket_counts['other']} 个文件未识别为 QuickBI / CitiHK 标准命名")

    return ParseResult(
        output_files=outputs,
        verify_rows=verify_rows,
        warnings=warnings,
        note="分摊基数库存扫描（未执行 QuickBI/CitiHK 流水线）；请选择分区运行完整构建。",
        metrics={
            "source_count": len(sources),
            "row_count": len(rows),
            "buckets": bucket_counts,
        },
    )
