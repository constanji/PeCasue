"""Summary sheets: aggregated by key columns (matching template 汇总 pattern)."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.workbook.workbook import Workbook as WB

from .quickbi_io import (
    COLS_INBOUND,
    COLS_OUTBOUND,
    COLS_VA,
    FINAL_BU_COL,
    coerce_numeric_for_excel,
)

_LOG = logging.getLogger("quickbi.line")

SRC_IN = "\u5165\u91d1"
SRC_OUT = "\u51fa\u91d1"
SRC_VA = "VA"
SUM_IN = "\u5165\u91d1\u6c47\u603b"
SUM_OUT = "\u51fa\u91d1\u6c47\u603b"
SUM_VA = "VA\u6c47\u603b"

# ---- Aggregation keys ----
_KEY_IN_OUT = [COLS_INBOUND[0], COLS_INBOUND[1], COLS_INBOUND[2], COLS_INBOUND[3],
               COLS_INBOUND[5], COLS_INBOUND[6]]
# Template VA个数 key: 月份, 渠道名称, 主体, 最终bu
_KEY_VA = [COLS_VA[0], COLS_VA[2], COLS_VA[1]]  # 月份, 渠道名称, 主体

_VAL_IN = [COLS_INBOUND[7], COLS_INBOUND[8]]   # 入金笔数, 入金交易量
_VAL_OUT = [COLS_OUTBOUND[7], COLS_OUTBOUND[8]]  # 出金笔数, 出金交易量
_VAL_VA = [COLS_VA[7]]  # va数

_HDR_IN = (
    "\u6708\u4efd",
    "\u4e3b\u4f53",
    "\u6e20\u9053\u540d\u79f0",
    "\u5927\u8d26\u53f7",
    "\u4e1a\u52a1\u7cfb\u7edf",
    "\u5ba2\u6237kyc\u56fd\u5bb6",
    "\u5165\u91d1\u7b14\u6570",
    "\u5165\u91d1\u4ea4\u6613\u91cf",
    "\u6700\u7ec8 bu",
)
_HDR_OUT = (
    "\u6708\u4efd",
    "\u4e3b\u4f53",
    "\u6e20\u9053\u540d\u79f0",
    "\u5927\u8d26\u53f7",
    "\u4e1a\u52a1\u7cfb\u7edf",
    "\u5ba2\u6237kyc\u56fd\u5bb6",
    "\u51fa\u91d1\u7b14\u6570",
    "\u51fa\u91d1\u4ea4\u6613\u91cf",
    "\u6700\u7ec8 bu",
)
_HDR_VA = (
    "\u6708\u4efd",
    "\u6e20\u9053\u540d\u79f0",
    "\u4e3b\u4f53",
    "va\u6570",
    "\u6700\u7ec8 bu",
)


def _safe_num(v: object) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _safe_str(v: object) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _get_final_bu(df: pd.DataFrame) -> pd.Series:
    """必须来自单独的「最终 bu」列，不得用表格最后一列（否则常与「是否B2B」混淆）。"""
    return df[FINAL_BU_COL].map(_safe_str)


def _drop_no_count_bu_for_summary(df: pd.DataFrame) -> pd.DataFrame:
    """汇总表不统计最终 BU 为「不取数」的记录，明细表保留原行。"""
    if FINAL_BU_COL not in df.columns:
        return df.copy()
    return df[_get_final_bu(df) != "不取数"].copy()


def aggregate_inbound(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 入金 data by key columns, summing numeric values (matching template pattern)."""
    df = _drop_no_count_bu_for_summary(df)
    df["__bu"] = _get_final_bu(df)
    keys = _KEY_IN_OUT + ["__bu"]
    for vc in _VAL_IN:
        df[vc] = df[vc].map(_safe_num)
    agg = df.groupby(keys, as_index=False)[_VAL_IN].sum()
    agg = agg.rename(columns={"__bu": FINAL_BU_COL})
    return agg


def aggregate_outbound(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 出金 data by key columns, summing numeric values."""
    df = _drop_no_count_bu_for_summary(df)
    df["__bu"] = _get_final_bu(df)
    keys = _KEY_IN_OUT + ["__bu"]
    for vc in _VAL_OUT:
        df[vc] = df[vc].map(_safe_num)
    agg = df.groupby(keys, as_index=False)[_VAL_OUT].sum()
    agg = agg.rename(columns={"__bu": FINAL_BU_COL})
    return agg


def aggregate_va(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate VA data by key columns, summing numeric values.

    QuickBI VA exports do not contain final BU, but build_shoufukuan_bases enriches it from
    the template mapping before summary generation.
    """
    df = _drop_no_count_bu_for_summary(df)
    if FINAL_BU_COL not in df.columns:
        df[FINAL_BU_COL] = ""
    df["__bu"] = _get_final_bu(df)
    for vc in _VAL_VA:
        df[vc] = df[vc].map(_safe_num)
    agg = df.groupby(_KEY_VA + ["__bu"], as_index=False)[_VAL_VA].sum()
    agg = agg.rename(columns={"__bu": FINAL_BU_COL})
    return agg


def _fill_agg_summary_ws(
    ws,
    *,
    headers: tuple[str, ...],
    agg_df: pd.DataFrame,
    col_order: list[str],
) -> None:
    """Write aggregated DataFrame to worksheet."""
    numeric = set(_VAL_IN) | set(_VAL_OUT) | set(_VAL_VA)
    for c, h in enumerate(headers, start=1):
        ws.cell(row=1, column=c).value = h
    for ri in range(len(agg_df)):
        rr = ri + 2
        r = agg_df.iloc[ri]
        for c, col_name in enumerate(col_order, start=1):
            val = r[col_name]
            if col_name in numeric:
                ws.cell(row=rr, column=c).value = coerce_numeric_for_excel(val)
            else:
                ws.cell(row=rr, column=c).value = _safe_str(val) if val is not None else None


def append_summary_sheets_to_workbook(
    wb: WB,
    *,
    df_in: pd.DataFrame | None = None,
    df_out: pd.DataFrame | None = None,
    df_va: pd.DataFrame | None = None,
    n_in: int = 0,
    n_out: int = 0,
    n_va: int = 0,
) -> None:
    """Add aggregated summary sheets. If DataFrames provided, do groupby aggregation."""
    for title in (SUM_IN, SUM_OUT, SUM_VA):
        if title in wb.sheetnames:
            wb.remove(wb[title])

    if df_in is not None and len(df_in) > 0:
        agg_in = aggregate_inbound(df_in)
        ws_in = wb.create_sheet(SUM_IN)
        col_order = _KEY_IN_OUT + [FINAL_BU_COL] + _VAL_IN
        _fill_agg_summary_ws(ws_in, headers=_HDR_IN, agg_df=agg_in, col_order=col_order)
        _LOG.info("%s: %d rows aggregated -> %d rows", SUM_IN, len(df_in), len(agg_in))
    elif n_in > 0:
        _LOG.warning("%s: no DataFrame provided, skipping", SUM_IN)

    if df_out is not None and len(df_out) > 0:
        agg_out = aggregate_outbound(df_out)
        ws_out = wb.create_sheet(SUM_OUT)
        col_order = _KEY_IN_OUT + [FINAL_BU_COL] + _VAL_OUT
        _fill_agg_summary_ws(ws_out, headers=_HDR_OUT, agg_df=agg_out, col_order=col_order)
        _LOG.info("%s: %d rows aggregated -> %d rows", SUM_OUT, len(df_out), len(agg_out))
    elif n_out > 0:
        _LOG.warning("%s: no DataFrame provided, skipping", SUM_OUT)

    if df_va is not None and len(df_va) > 0:
        agg_va = aggregate_va(df_va)
        ws_va = wb.create_sheet(SUM_VA)
        col_order = _KEY_VA + _VAL_VA + [FINAL_BU_COL]
        _fill_agg_summary_ws(ws_va, headers=_HDR_VA, agg_df=agg_va, col_order=col_order)
        _LOG.info("%s: %d rows aggregated -> %d rows", SUM_VA, len(df_va), len(agg_va))
    elif n_va > 0:
        _LOG.warning("%s: no DataFrame provided, skipping", SUM_VA)


def append_summary_sheets_via_load_save(
    path: Path,
    *,
    df_in: pd.DataFrame | None = None,
    df_out: pd.DataFrame | None = None,
    df_va: pd.DataFrame | None = None,
    n_in: int = 0,
    n_out: int = 0,
    n_va: int = 0,
) -> None:
    """After zip fast path: inject aggregated summaries."""
    from .summary_sheets_zip import append_summary_sheets_zip
    append_summary_sheets_zip(
        path,
        df_in=df_in, df_out=df_out, df_va=df_va,
        n_in=n_in, n_out=n_out, n_va=n_va,
    )


def write_external_narrow_workbook(
    path: Path,
    *,
    main_book_filename: str,
    n_in: int,
    n_out: int,
    n_va: int,
) -> None:
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    # For external narrow, keep the formula-reference approach (cross-workbook)
    _COL_IN_OUT = ("A", "B", "C", "D", "F", "G", "H", "I", "W")
    _COL_VA = ("A", "C", "B", "H", "T")

    def _xref(book: str, sh: str, col: str, row: int) -> str:
        return "='[%s]%s'!%s%d" % (book, sh, col, row)

    def _fill(
        ws,
        *,
        src_sheet: str,
        headers: tuple[str, ...],
        src_cols: tuple[str, ...],
        nrows: int,
    ) -> None:
        for c, h in enumerate(headers, start=1):
            ws.cell(row=1, column=c).value = h
        for i in range(nrows):
            r = 2 + i
            for c, letter in enumerate(src_cols, start=1):
                ws.cell(row=r, column=c).value = _xref(
                    main_book_filename, src_sheet, letter, r
                )

    wb = Workbook()
    d = wb.active
    wb.remove(d)
    ws_i = wb.create_sheet(SRC_IN)
    _fill(ws_i, src_sheet=SRC_IN, headers=_HDR_IN, src_cols=_COL_IN_OUT, nrows=n_in)
    ws_o = wb.create_sheet(SRC_OUT)
    _fill(ws_o, src_sheet=SRC_OUT, headers=_HDR_OUT, src_cols=_COL_IN_OUT, nrows=n_out)
    ws_v = wb.create_sheet(SRC_VA)
    _fill(ws_v, src_sheet=SRC_VA, headers=_HDR_VA, src_cols=_COL_VA, nrows=n_va)
    wb.save(path)
    wb.close()
