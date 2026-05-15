"""OP 入账表专用处理逻辑。

Ported from pingpong-master/script/other/process_op_incoming.py.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .common import (
    OP_INCOMING_SUBTYPE,
    OpBranchKey,
    build_op_bank_entity_to_branch_dim,
    lookup_usd_fx_rate_series,
    safe_numeric,
    yyyymm_from_date_series,
)

logger = logging.getLogger(__name__)


def process_op_incoming(src_path: Path, *, rules_files_dir: Path) -> pd.DataFrame:
    """Read an OP 入账 source xlsx and return a canonicalised DataFrame.

    Parameters
    ----------
    src_path:
        Path to the source Excel file (福贸入账 BU).
    rules_files_dir:
        Path to ``rules/files`` for FX rates.
    """
    bank_entity_to_branch = build_op_bank_entity_to_branch_dim()

    logger.info("process_op_incoming: 读取 %s ...", src_path.name)
    df = pd.read_excel(src_path, sheet_name=0, dtype=object)
    logger.info("  原始行数=%d, 列数=%d", len(df), len(df.columns))
    logger.info("  列名=%s", [str(c) for c in df.columns[:25]])

    # Key columns (by position index for stability)
    col_date = df.columns[5]      # 订单日期
    col_channel = df.columns[6]
    col_product = df.columns[7]
    col_process = df.columns[8]
    col_desc = df.columns[9]      # 单据子类型
    col_entity = df.columns[18]   # 公司主体
    col_bank = df.columns[19]     # 银行/通道
    col_ccy = df.columns[21]      # 币种
    col_amt = df.columns[22]      # 金额

    logger.info("  col_desc=%r (idx=9), 用作单据子类型筛选", col_desc)
    # Show unique values of 单据子类型 for debugging
    if len(df) > 0:
        desc_vals = df[col_desc].astype(str).str.strip().value_counts().head(10)
        logger.info("  col_desc 前10值: %s", dict(desc_vals))

    # Fee column: the original pingpong script used "Unnamed: 23" (legacy unnamed column),
    # but in newer files column 23 is named "银行入账手续费" (all zeros).
    # The actual fee for 退款退票(VA) is in "原退款手续费" (typically col 26).
    # Try multiple candidates in priority order:
    fee_ccy_amt = pd.Series([0.0] * len(df), index=df.index)
    fee_source = "none"
    for _fc in ("Unnamed: 23", "原退款手续费"):
        if _fc in df.columns:
            candidate = safe_numeric(df[_fc])
            if candidate.fillna(0).ne(0).any():
                fee_ccy_amt = candidate
                fee_source = _fc
                break
    # Fallback: try column at position 23 if it has non-zero data
    if fee_source == "none" and len(df.columns) > 23:
        candidate = safe_numeric(df[df.columns[23]])
        if candidate.fillna(0).ne(0).any():
            fee_ccy_amt = candidate
            fee_source = str(df.columns[23])
    logger.info("  fee 列来源: %s (非零行=%d)", fee_source, (fee_ccy_amt.fillna(0) != 0).sum())

    # Filter: 单据子类型 = 退款退票（VA）AND fee != 0
    m_desc = df[col_desc].astype(str).str.strip().eq(OP_INCOMING_SUBTYPE)
    m_fee = fee_ccy_amt.fillna(0) != 0
    n_desc_match = m_desc.sum()
    n_fee_match = m_fee.sum()
    n_both = (m_desc & m_fee).sum()
    logger.info("  筛选: 单据子类型='%s' 命中=%d行, fee!=0 命中=%d行, 交集=%d行",
                OP_INCOMING_SUBTYPE, n_desc_match, n_fee_match, n_both)
    df = df.loc[m_desc & m_fee].copy()
    fee_ccy_amt = fee_ccy_amt.loc[df.index]
    logger.info("  筛选后行数=%d", len(df))

    bank_norm = df[col_bank].astype(str).str.strip().str.upper()
    entity_norm = pd.Series(["PPHK"] * len(df), index=df.index)

    branch = []
    for b, e in zip(bank_norm.tolist(), entity_norm.tolist()):
        v = bank_entity_to_branch.get(OpBranchKey(bank=b, entity=e), "")
        if not v:
            if e == "PPHK" and b in {"CITI", "JPM", "SCB", "DBS", "DB"}:
                v = f"{b}HK"
            elif e == "PPHK" and b == "BOC":
                v = "BOCHK"
            else:
                v = b
        branch.append(v)

    period = yyyymm_from_date_series(df[col_date])
    ccy_upper = df[col_ccy].astype(str).str.strip().str.upper()
    rate = lookup_usd_fx_rate_series(period, ccy_upper, rules_files_dir)
    fee_usd = (fee_ccy_amt * rate).where(rate.notna(), fee_ccy_amt)

    src_name = src_path.name
    out = pd.DataFrame(
        {
            "统计期间": period,
            "类型_统计维度": "",
            "USD金额": fee_usd,
            "方向": "inbound",
            "主体": "PPHK",
            "分行维度": branch,
            ">>": "",
            "订单日期": pd.to_datetime(df[col_date], errors="coerce").dt.date,
            "业务渠道": df[col_channel],
            "产品": df[col_product],
            "数据处理": df[col_process],
            "交易描述": df[col_desc],
            "公司主体": "PPHK",
            "银行/通道": df[col_bank],
            "入账币种": df[col_ccy],
            "入账金额": safe_numeric(df[col_amt]),
            "费用USD": fee_usd,
            "OUTBOUND_REFUND_FEE": 0,
            "入账金额(含费)": safe_numeric(df[col_amt]) + fee_ccy_amt.fillna(0),
            "入账币种(含费)": df[col_ccy],
            "是否1": 1.0,
            "来源文件": src_name,
        }
    )
    out["类型_统计维度"] = out["统计期间"].astype(str) + "_" + out["入账币种"].astype(str)

    out = out.sort_values(
        ["统计期间", "主体", "分行维度", "订单日期"], kind="mergesort"
    ).reset_index(drop=True)
    return out