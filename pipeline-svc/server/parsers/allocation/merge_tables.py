"""Merge QuickBI summary workbook with CitiHK base workbook (allline 5_04 logic, headless)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(path)
        if sheet_name in xl.sheet_names:
            return xl.parse(sheet_name)
    except Exception:
        pass
    return pd.DataFrame()


def read_citihk_raw_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    """Read a CITIHK sheet that may have a comment/instruction row before the real header.

    The CITIHK workbook often has row 0 as a note (e.g. "境内渠道收款的需要手工添加交易量数据"),
    row 1 as the actual header (月份, 主体, 渠道名称, ...), and data from row 2 onward.
    This function detects the real header row by looking for known column names.
    """
    known_headers = {"月份", "主体", "渠道名称", "大账号"}
    try:
        xl = pd.ExcelFile(path)
        if sheet_name in xl.sheet_names:
            raw = xl.parse(sheet_name, header=None, dtype=object)
            if len(raw) == 0:
                return pd.DataFrame()
            # Scan first 5 rows to find the real header row
            header_row = 0
            for ri in range(min(5, len(raw))):
                row_vals = {str(v).strip() for v in raw.iloc[ri] if pd.notna(v)}
                if len(known_headers & row_vals) >= 3:
                    header_row = ri
                    break
            raw.columns = [
                str(c).strip() if pd.notna(c) else f"col_{i}"
                for i, c in enumerate(raw.iloc[header_row])
            ]
            raw = raw.iloc[header_row + 1:].reset_index(drop=True)
            return raw
    except Exception:
        pass
    return pd.DataFrame()


def read_citihk_inbound(path: Path) -> pd.DataFrame:
    df = read_citihk_raw_sheet(path, "入金笔数")
    if not df.empty:
        return df
    df = read_sheet(path, "入金基数_明细")
    if not df.empty:
        return df
    df = read_sheet(path, "入金")
    if not df.empty:
        return df
    return read_sheet(path, "入金汇总")


def read_citihk_outbound(path: Path) -> pd.DataFrame:
    df = read_citihk_raw_sheet(path, "出金笔数")
    if not df.empty:
        return df
    df = read_sheet(path, "出金基数_明细")
    if not df.empty:
        return df
    df = read_sheet(path, "出金")
    if not df.empty:
        return df
    return read_sheet(path, "出金汇总")


def aggregate_citihk(ch_df: pd.DataFrame, count_col: str, group_keys: list[str]) -> pd.DataFrame:
    if ch_df.empty or count_col not in ch_df.columns:
        return pd.DataFrame()
    # Excel 偶发重复表头列名，会导致 ch_df["主体"] 为二维子表，groupby 报 Grouper not 1-dimensional
    if ch_df.columns.duplicated().any():
        ch_df = ch_df.loc[:, ~ch_df.columns.duplicated(keep="first")].copy()
    existing_keys = [k for k in group_keys if k in ch_df.columns]
    if not existing_keys:
        return pd.DataFrame()
    ch_df = ch_df.copy()
    ch_df[count_col] = pd.to_numeric(ch_df[count_col], errors="coerce").fillna(0)
    return ch_df.groupby(existing_keys, dropna=False)[count_col].sum().reset_index()


def align_and_append(base: pd.DataFrame, addon: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return addon
    if addon.empty:
        return base
    aligned = addon.reindex(columns=base.columns)
    for c in base.columns:
        if c in ("入金笔数", "入金交易量", "出金笔数", "出金交易量", "va数"):
            aligned[c] = pd.to_numeric(aligned[c], errors="coerce").fillna(0)
    return pd.concat([base, aligned], ignore_index=True)


def merge_allocation_workbooks(
    quickbi_path: Path | None,
    citihk_path: Path | None,
    output_path: Path,
) -> dict[str, int | str | bool]:
    """合并 QuickBI 与 CitiHK 工作簿。

    任一路径可为 ``None`` 或不存在：缺失侧视为无数据并入，仍写出汇总表（可能仅含单侧），
    并在返回中包含 ``missing_quickbi`` / ``missing_citihk`` 供上游提示。
    若两侧均无法提供任何可用数据则抛出 ``ValueError``。
    """
    qb_p = quickbi_path.expanduser().resolve() if quickbi_path else None
    ch_p = citihk_path.expanduser().resolve() if citihk_path else None
    if qb_p is not None and not qb_p.is_file():
        qb_p = None
    if ch_p is not None and not ch_p.is_file():
        ch_p = None

    qb_agg_in = pd.DataFrame()
    qb_agg_out = pd.DataFrame()
    qb_agg_va = pd.DataFrame()
    if qb_p is not None:
        qb_agg_in = read_sheet(qb_p, "入金汇总")
        if qb_agg_in.empty:
            qb_agg_in = read_sheet(qb_p, "入金")
        qb_agg_out = read_sheet(qb_p, "出金汇总")
        if qb_agg_out.empty:
            qb_agg_out = read_sheet(qb_p, "出金")
        qb_agg_va = read_sheet(qb_p, "VA汇总")
        if qb_agg_va.empty:
            qb_agg_va = read_sheet(qb_p, "VA")

    ch_in_raw = pd.DataFrame()
    ch_out_raw = pd.DataFrame()
    if ch_p is not None:
        ch_in_raw = read_citihk_inbound(ch_p)
        ch_out_raw = read_citihk_outbound(ch_p)
        if ch_in_raw.columns.duplicated().any():
            ch_in_raw = ch_in_raw.loc[:, ~ch_in_raw.columns.duplicated(keep="first")].copy()
        if ch_out_raw.columns.duplicated().any():
            ch_out_raw = ch_out_raw.loc[:, ~ch_out_raw.columns.duplicated(keep="first")].copy()

    missing_quickbi = qb_p is None
    missing_citihk = ch_p is None

    if qb_p is not None and qb_agg_in.empty and qb_agg_out.empty:
        try:
            xl = pd.ExcelFile(qb_p)
            avail = ", ".join(xl.sheet_names)
        except Exception:
            avail = "?"
        raise ValueError(f"在 {qb_p.name} 中未找到入金/出金汇总 sheet。可用: [{avail}]")

    in_grp_keys = ["月份", "主体", "渠道名称", "大账号", "最终bu"]
    if not qb_agg_in.empty:
        if "最终 bu" in qb_agg_in.columns and "最终bu" not in qb_agg_in.columns:
            in_grp_keys = ["月份", "主体", "渠道名称", "大账号", "最终 bu"]
    elif not ch_in_raw.empty and "最终 bu" in ch_in_raw.columns:
        in_grp_keys = ["月份", "主体", "渠道名称", "大账号", "最终 bu"]

    ch_agg_in = aggregate_citihk(ch_in_raw, "入金笔数", in_grp_keys)
    ch_agg_out = aggregate_citihk(ch_out_raw, "出金笔数", in_grp_keys)

    for df_agg, vol_col in ((ch_agg_in, "入金交易量"), (ch_agg_out, "出金交易量")):
        if not df_agg.empty and vol_col not in df_agg.columns:
            df_agg[vol_col] = 0

    for df_agg in (ch_agg_in, ch_agg_out):
        if "最终bu" in df_agg.columns:
            if "最终 bu" in qb_agg_in.columns or "最终 bu" in qb_agg_out.columns:
                df_agg.rename(columns={"最终bu": "最终 bu"}, inplace=True)

    final_in = align_and_append(qb_agg_in, ch_agg_in)
    final_out = align_and_append(qb_agg_out, ch_agg_out)
    final_va = qb_agg_va

    if final_in.empty and final_out.empty and final_va.empty:
        raise ValueError(
            "合并无可用数据：请至少提供一侧可用的 QuickBI 汇总中间表（含入金/出金或 VA）"
            "或 CitiHK 构建产出。"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        if not final_in.empty:
            final_in.to_excel(writer, sheet_name="入金汇总", index=False)
        if not final_out.empty:
            final_out.to_excel(writer, sheet_name="出金汇总", index=False)
        if not final_va.empty:
            final_va.to_excel(writer, sheet_name="VA汇总", index=False)

    return {
        "output_path": str(output_path.resolve()),
        "qb_in_rows": len(qb_agg_in),
        "qb_out_rows": len(qb_agg_out),
        "ch_in_rows": len(ch_agg_in),
        "ch_out_rows": len(ch_agg_out),
        "final_in_rows": len(final_in),
        "final_out_rows": len(final_out),
        "va_rows": len(final_va),
        "source_file": qb_p.name if qb_p is not None else "",
        "citihk_file": ch_p.name if ch_p is not None else "",
        "missing_quickbi": missing_quickbi,
        "missing_citihk": missing_citihk,
    }
