"""OP 退票表专用处理逻辑。

Ported from pingpong-master/script/other/process_op_refund.py.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .common import (
    OpBranchKey,
    build_op_bank_entity_to_branch_dim,
    lookup_usd_fx_rate_series,
    safe_numeric,
    yyyymm_from_date_series,
)

logger = logging.getLogger(__name__)


def _read_op_refund_file(path: Path, source_name: str) -> pd.DataFrame:
    logger.info("_read_op_refund_file: 读取 %s (%s) ...", path.name, source_name)
    df = pd.read_excel(path, sheet_name=0, dtype=object)
    logger.info("  原始行数=%d, 列数=%d", len(df), len(df.columns))
    df["__source_name__"] = source_name
    df["来源文件"] = path.name
    return df


def _build_output(
    df: pd.DataFrame,
    bank_entity_to_branch: dict[OpBranchKey, str],
    *,
    rules_files_dir: Path,
) -> pd.DataFrame:
    # Column positions (stable across OP退票表 BU files)
    col_date = df.columns[4]
    col_channel = df.columns[5]
    col_product = df.columns[6]
    col_process = df.columns[7]
    col_entity = df.columns[18]
    col_bank = df.columns[19]
    col_refund_ccy = df.columns[22]
    col_refund_amt = df.columns[23]
    col_fee_ccy = df.columns[24]
    col_fee_amt = df.columns[25]

    fee_amt = safe_numeric(df[col_fee_amt])
    period = yyyymm_from_date_series(df[col_date])
    fee_ccy_upper = df[col_fee_ccy].astype(str).str.strip().str.upper()
    rate = lookup_usd_fx_rate_series(period, fee_ccy_upper, rules_files_dir)
    usd_amt = (fee_amt * rate).where(rate.notna(), fee_amt)

    out = pd.DataFrame(
        {
            "统计期间": period,
            "类型_统计维度": "",
            "USD金额": usd_amt.round(6),
            "方向": "outbound",
            "主体": "PPHK",
            "分行维度": "",
            ">>": "",
            "订单日期": pd.to_datetime(df[col_date], errors="coerce").dt.date,
            "业务渠道": df[col_channel],
            "产品": df[col_product],
            "数据处理": df[col_process],
            "公司主体": "PPHK",
            "银行/通道": df[col_bank],
            "退票入账币种": df[col_refund_ccy],
            "退票入账金额": safe_numeric(df[col_refund_amt]),
            "退票手续费币种": df[col_fee_ccy],
            "退票手续费金额": fee_amt,
            "统计期间说明": "",
            "数据来源": df["__source_name__"],
            "来源文件": df["来源文件"],
        }
    )

    # 分行维度映射
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
    out["分行维度"] = branch

    # 类型_统计维度: period_feeCCY
    fee_ccy = df[col_fee_ccy].astype(str).str.strip()
    out["类型_统计维度"] = out["统计期间"].astype(str) + "_" + fee_ccy

    # Filter: 退票手续费金额 > 0
    out = out.loc[out["退票手续费金额"].fillna(0) > 0].reset_index(drop=True)
    return out


def process_op_refund(
    main_path: Path,
    b2b_path: Path,
    *,
    rules_files_dir: Path,
) -> pd.DataFrame:
    """Process OP 退票表 (main + B2B) and return combined DataFrame.

    Parameters
    ----------
    main_path:
        Path to 主站退票表 BU xlsx.
    b2b_path:
        Path to B2B退票表 BU xlsx (first sheet is mapping, second is 流水).
    rules_files_dir:
        Path to ``rules/files`` for FX rates.
    """
    bank_entity_to_branch = build_op_bank_entity_to_branch_dim()

    df_main = _read_op_refund_file(main_path, "PPHK 主站退票表")

    # B2B: first sheet is BU mapping, actual data in second sheet
    xl = pd.ExcelFile(b2b_path)
    sheet_idx = 1 if len(xl.sheet_names) > 1 else 0
    df_b2b = pd.read_excel(b2b_path, sheet_name=sheet_idx, dtype=object)
    df_b2b["__source_name__"] = "PPHK B2B退票表"
    df_b2b["来源文件"] = b2b_path.name

    out_main = _build_output(df_main, bank_entity_to_branch, rules_files_dir=rules_files_dir)
    out_b2b = _build_output(df_b2b, bank_entity_to_branch, rules_files_dir=rules_files_dir)
    out = pd.concat([out_main, out_b2b], ignore_index=True)

    out = out.sort_values(
        ["统计期间", "主体", "分行维度", "订单日期"], kind="mergesort"
    ).reset_index(drop=True)
    return out