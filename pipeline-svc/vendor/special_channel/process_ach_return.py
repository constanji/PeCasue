"""ACH 退款（ACH return）专用处理逻辑。

Ported from pingpong-master/script/other/process_ach_return.py.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .common import (
    lookup_usd_fx_rate_series,
    normalize_subject_from_account_entity,
    safe_numeric,
    yyyymm_from_date_series,
)

logger = logging.getLogger(__name__)


def process_ach_return(src_path: Path, *, rules_files_dir: Path) -> pd.DataFrame:
    """Read an ACH return source xlsx and return a canonicalised DataFrame.

    Parameters
    ----------
    src_path:
        Path to the source Excel file.
    rules_files_dir:
        Path to ``rules/files`` so FX rates can be loaded.
    """
    logger.info("process_ach_return: 读取 %s ...", src_path.name)
    df = pd.read_excel(src_path, sheet_name=0, dtype=object)
    logger.info("  原始行数=%d, 列数=%d", len(df), len(df.columns))

    bill_date = df["BillDate"] if "BillDate" in df.columns else df.iloc[:, 3]
    currency = df["Currency"] if "Currency" in df.columns else df.iloc[:, 12]
    extra_fee = df["Extra Fee"] if "Extra Fee" in df.columns else df.iloc[:, 27]
    channel = df["Channel"] if "Channel" in df.columns else df.iloc[:, 5]
    fund_type = df["FundType"] if "FundType" in df.columns else None

    period = yyyymm_from_date_series(bill_date)
    ccy = currency.astype(str).str.strip()
    fee = safe_numeric(extra_fee)
    ccy_upper = ccy.astype(str).str.strip().str.upper()

    # Filter: FundType contains 'ach return' or 'achreturn' AND Extra Fee != 0
    if fund_type is not None:
        ft = fund_type.astype(str).str.lower()
        m_ft = ft.str.contains("ach return", na=False) | ft.str.contains("achreturn", na=False)
    else:
        m_ft = pd.Series([True] * len(df), index=df.index)
    m_fee = fee.fillna(0) != 0
    n_ft = m_ft.sum()
    n_fee = m_fee.sum()
    n_both = (m_ft & m_fee).sum()
    logger.info("  筛选: ach_return=%d, fee!=0=%d, 交集=%d", n_ft, n_fee, n_both)
    df = df.loc[m_ft & m_fee].copy()
    period = period.loc[df.index]
    ccy = ccy.loc[df.index]
    logger.info("  筛选后行数=%d", len(df))
    fee = fee.loc[df.index]
    ccy_upper = ccy_upper.loc[df.index]
    rate = lookup_usd_fx_rate_series(period, ccy_upper, rules_files_dir)

    # Normalise 账户主体
    account_entity_col = "账户主体" if "账户主体" in df.columns else df.columns[0]
    df[account_entity_col] = normalize_subject_from_account_entity(df[account_entity_col])
    entity = df[account_entity_col]
    ch_norm = df[channel.name].astype(str).str.strip()

    # 分行维度: prefer channel name without spaces; DBS by entity
    branch = ch_norm.astype(str).str.replace(" ", "", regex=False)
    ent_norm = entity.astype(str).str.strip().str.upper()
    ch_upper = ch_norm.astype(str).str.strip().str.upper()
    branch.loc[ch_upper.eq("DBS") & ent_norm.eq("PPHK")] = "DBSHK"
    branch.loc[ch_upper.eq("DBS") & ent_norm.eq("MANA PAYMENT SG")] = "DBSSG"

    out = df.copy()
    out.insert(0, "统计期间", period)
    out.insert(1, "类型_统计维度", period.astype(str) + "_" + ccy)
    usd = (-fee * rate).where(rate.notna(), -fee)
    out.insert(2, "USD", usd.round(6))
    out.insert(3, "分行维度", branch)
    out.insert(4, "方向", "inbound")
    out["来源文件"] = src_path.name
    return out