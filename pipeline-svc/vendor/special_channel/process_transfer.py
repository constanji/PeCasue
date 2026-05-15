"""内转（internal transfer / channel-settle-in）专用处理逻辑。

Ported from pingpong-master/script/other/process_transfer.py.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .common import (
    GMO_TAX_DIVISOR,
    XENDIT_ENTITY_LABEL,
    lookup_usd_fx_rate_series,
    normalize_subject_from_account_entity,
    safe_numeric,
    yyyymm_from_date_series,
)

logger = logging.getLogger(__name__)


def process_transfer(src_path: Path, *, rules_files_dir: Path) -> pd.DataFrame:
    """Read a 内转 source xlsx and return a canonicalised DataFrame.

    Parameters
    ----------
    src_path:
        Path to the source Excel file (e.g. "内转渠道账单.xlsx").
    rules_files_dir:
        Path to ``rules/files`` so FX rates can be loaded.
    """
    logger.info("process_transfer: 读取 %s ...", src_path.name)
    df = pd.read_excel(src_path, sheet_name=0, dtype=object)
    logger.info("  原始行数=%d, 列数=%d", len(df), len(df.columns))

    bill_date = df["BillDate"] if "BillDate" in df.columns else df.iloc[:, 3]
    currency = df["Currency"] if "Currency" in df.columns else df.iloc[:, 12]
    extra_fee = df["Extra Fee"] if "Extra Fee" in df.columns else df.iloc[:, 27]
    channel = df["Channel"] if "Channel" in df.columns else df.iloc[:, 5]
    trade_channel = df["tradeChannel"] if "tradeChannel" in df.columns else None
    fund_type = df["FundType"] if "FundType" in df.columns else None

    period = yyyymm_from_date_series(bill_date)
    ccy = currency.astype(str).str.strip()
    fee = safe_numeric(extra_fee)

    # Filtering rules:
    # - FundType contains 'fundtransfer' AND Extra Fee != 0
    # - tradeChannel=GMO AND Extra Fee != 0
    # - FundType contains 'channel-settle' AND Extra Fee != 0
    if fund_type is not None:
        ft = fund_type.astype(str).str.lower()
        m_ft = ft.str.contains("fundtransfer", na=False)
        m_settle = ft.str.contains("channel-settle", na=False)
    else:
        m_ft = pd.Series([True] * len(df), index=df.index)
        m_settle = pd.Series([False] * len(df), index=df.index)
    m_fee = fee.fillna(0) != 0
    if trade_channel is not None:
        tc0 = df[trade_channel.name].astype(str).str.strip().str.upper()
        m_gmo = tc0.eq("GMO")
    else:
        m_gmo = pd.Series([False] * len(df), index=df.index)
    n_ft = m_ft.sum()
    n_gmo = m_gmo.sum()
    n_settle = m_settle.sum()
    n_fee = m_fee.sum()
    n_combined = ((m_ft | m_gmo | m_settle) & m_fee).sum()
    logger.info("  筛选: fundtransfer=%d, GMO=%d, channel-settle=%d, fee!=0=%d, 交集=%d",
                n_ft, n_gmo, n_settle, n_fee, n_combined)
    df = df.loc[(m_ft | m_gmo | m_settle) & m_fee].copy()
    period = period.loc[df.index]
    logger.info("  筛选后行数=%d", len(df))
    ccy = ccy.loc[df.index]
    fee = fee.loc[df.index]

    ch_norm = df[channel.name].astype(str).str.strip()
    ch_upper = ch_norm.str.upper()

    # Normalise 账户主体
    account_entity_col = "账户主体" if "账户主体" in df.columns else df.columns[0]
    df[account_entity_col] = normalize_subject_from_account_entity(df[account_entity_col])

    # 主体/类型列: unified 'others'; XENDIT rows get special label
    entity_out = pd.Series(["others"] * len(df), index=df.index)
    entity_out.loc[ch_upper.eq("XENDIT")] = XENDIT_ENTITY_LABEL

    # 分行维度
    branch = ch_norm.str.replace(" ", "", regex=False)
    branch.loc[ch_upper.eq("XENDIT")] = "Xendit-ID"
    if trade_channel is not None:
        tc = df[trade_channel.name].astype(str).str.strip().str.upper()
        branch.loc[tc.eq("GMO")] = "GMO"

    # Supplementary branch mapping for bare bank codes
    cur_upper = df[currency.name].astype(str).str.strip().str.upper()
    branch.loc[(ch_upper.eq("CITI")) & (cur_upper.eq("PLN"))] = "CITIPL"
    branch.loc[ch_upper.eq("CITI") & ~cur_upper.eq("PLN")] = "CITIHK"
    branch.loc[ch_upper.eq("JPM")] = "JPMHK"
    branch.loc[ch_upper.eq("SCB")] = "SCBHK"
    branch.loc[ch_upper.eq("DBS")] = "DBSHK"

    out = df.copy()
    out.insert(0, "统计期间", period)
    out.insert(1, "类型_统计维度", period.astype(str) + "_" + ccy)

    # USD conversion
    fee_num = safe_numeric(fee)
    cur_upper2 = df[currency.name].astype(str).str.strip().str.upper()
    rate = lookup_usd_fx_rate_series(period, cur_upper2, rules_files_dir)
    usd_abs = (fee_num.abs() * rate).where(rate.notna(), fee_num.abs())
    if trade_channel is not None:
        tc1 = df[trade_channel.name].astype(str).str.strip().str.upper()
        # GMO: divide by 1.1 then apply FX
        mask_gmo_rate = tc1.eq("GMO") & rate.notna()
        mask_gmo_norate = tc1.eq("GMO") & rate.isna()
        usd_abs.loc[mask_gmo_rate] = (
            fee_num.loc[mask_gmo_rate] / GMO_TAX_DIVISOR * rate.loc[mask_gmo_rate]
        )
        usd_abs.loc[mask_gmo_norate] = fee_num.loc[mask_gmo_norate] / GMO_TAX_DIVISOR
    out.insert(2, "USD(折算值)", usd_abs.round(6))

    # 渠道列: GMO rows → "GMO"
    channel_out = ch_norm.copy()
    if trade_channel is not None:
        tc2 = df[trade_channel.name].astype(str).str.strip().str.upper()
        channel_out.loc[tc2.eq("GMO")] = "GMO"
    out.insert(3, "方向", channel_out)
    out.insert(4, "分行维度", branch)
    out.insert(5, "主体", entity_out)
    out["来源文件"] = src_path.name
    return out