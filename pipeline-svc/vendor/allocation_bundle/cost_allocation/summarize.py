#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把分摊长表归纳为模板 `分摊结果` sheet 风格的 8 个块。

输出块：
  1) inbound         : BU x month，来自长表 成本类型 == inbound
  2) outbound        : BU x month，来自长表 成本类型 == outbound
  3) others          : BU x month，来自长表 成本类型 == others
  4) VA成本          : BU x month，来自长表 成本类型 == va
  5) 收款通道成本    : BU x month，来自长表 成本类型 == 收款通道成本
  6) 合计            : 以上 5 块逐单元相加（含「整体(残差)」行，与长表成本金额一致）
  7) 整体分摊金额    : 把 BU=整体 的「合计」值按 (各BU(入+出)笔数 / (全BU-整体) 笔数) 再分摊回各 BU
  8) 合计分摊整体后  : 合计 + 整体分摊金额
"""
from __future__ import annotations

import pandas as pd

from .bases import Bases


BU_WHOLE = "整体"
# allocate 后未进主站…整体列的金额（分母为 0、未识别分摊方式等），须计入分块合计，否则分项与长表成本不符
RESIDUAL_LABEL = "整体(残差)"
RESIDUAL_COL = "整体(残差)"


def _sum_inout(bases: Bases, *, month: str, bu: str | None = None) -> float:
    """(入金笔数 + 出金笔数) 在给定 month/bu 下的合计。"""
    return (
        bases.sum_base("in", "count", month=month, bu=bu)
        + bases.sum_base("out", "count", month=month, bu=bu)
    )


def _pivot_block(
    df_long: pd.DataFrame,
    cost_type: str,
    bu_list: list,
    months: list,
    *,
    match_mode: str = "lower",
    display_name: str | None = None,
) -> pd.DataFrame:
    """按 (成本类型, BU) x month 透视；末行追加 '合计'。"""
    col = df_long["成本类型"].astype(str)
    if match_mode == "exact":
        mask = col == cost_type
    else:
        mask = col.str.lower() == cost_type.lower()
    sub = df_long[mask]
    has_res = RESIDUAL_COL in sub.columns
    label = display_name or cost_type
    rows = []
    for bu in bu_list:
        rec = {"成本类型": label, "BU": bu}
        total = 0.0
        for m in months:
            if bu == RESIDUAL_LABEL:
                msub = sub["month"].astype(str) == m
                v = float(sub.loc[msub, RESIDUAL_COL].sum()) if has_res else 0.0
            else:
                v = float(sub.loc[sub["month"].astype(str) == m, bu].sum())
            rec[m] = v
            total += v
        rec["合计"] = total
        rows.append(rec)
    total_rec = {"成本类型": label, "BU": "合计"}
    for m in months:
        total_rec[m] = sum(r[m] for r in rows)
    total_rec["合计"] = sum(r["合计"] for r in rows)
    rows.append(total_rec)
    return pd.DataFrame(rows, columns=["成本类型", "BU", *months, "合计"])


def _add_blocks(name, blocks, bu_list, months):
    """逐 (BU, month) 单元把若干块相加（仅 BU 行，不含块自己的合计行）。"""
    rows = []
    for i, bu in enumerate(bu_list):
        rec = {"成本类型": name, "BU": bu}
        total = 0.0
        for m in months:
            v = sum(float(b.iloc[i][m]) for b in blocks)
            rec[m] = v
            total += v
        rec["合计"] = total
        rows.append(rec)
    total_rec = {"成本类型": name, "BU": "合计"}
    for m in months:
        total_rec[m] = sum(r[m] for r in rows)
    total_rec["合计"] = sum(r["合计"] for r in rows)
    rows.append(total_rec)
    return pd.DataFrame(rows, columns=["成本类型", "BU", *months, "合计"])


def _build_whole_allocation(he_ji, bases, bu_list, months):
    """把「合计」块里 BU=整体 的月度合计按 (各BU笔数 / (全-整体笔数)) 再分摊回各 BU。
    展示行「整体(残差)」不参与基数比例，对应月列恒为 0。
    """
    whole_row = he_ji.loc[he_ji["BU"] == BU_WHOLE].iloc[0]

    rows = []
    for bu in bu_list:
        if bu == RESIDUAL_LABEL:
            rec = {"成本类型": "整体分摊金额", "BU": bu}
            for m in months:
                rec[m] = 0.0
            rec["合计"] = 0.0
            rows.append(rec)
            continue
        rec = {"成本类型": "整体分摊金额", "BU": bu}
        total = 0.0
        for m in months:
            whole = float(whole_row[m])
            denom = _sum_inout(bases, month=m) - _sum_inout(bases, month=m, bu=BU_WHOLE)
            numer = _sum_inout(bases, month=m, bu=bu)
            v = (whole / denom * numer) if denom else 0.0
            rec[m] = v
            total += v
        rec["合计"] = total
        rows.append(rec)
    total_rec = {"成本类型": "整体分摊金额", "BU": "合计"}
    for m in months:
        total_rec[m] = sum(r[m] for r in rows)
    total_rec["合计"] = sum(r["合计"] for r in rows)
    rows.append(total_rec)
    return pd.DataFrame(rows, columns=["成本类型", "BU", *months, "合计"])


def summarize(df_long, bases, bu_list, *, months=None):
    """从长表 + 基数产出 8 个块，返回 sheet_name → DataFrame。"""
    if months is None:
        months = sorted(df_long["month"].dropna().astype(str).unique().tolist())

    full_bu = list(bu_list) + [BU_WHOLE, RESIDUAL_LABEL]

    inbound = _pivot_block(df_long, "inbound", full_bu, months)
    outbound = _pivot_block(df_long, "outbound", full_bu, months)
    others = _pivot_block(df_long, "others", full_bu, months)
    va = _pivot_block(df_long, "va", full_bu, months, display_name="VA成本")
    pc = _pivot_block(
        df_long, "收款通道成本", full_bu, months, match_mode="exact",
    )

    he_ji = _add_blocks("合计", [inbound, outbound, others, va, pc], full_bu, months)

    whole_alloc = _build_whole_allocation(he_ji, bases, full_bu, months)

    # 合计分摊整体后 = 合计 + 整体分摊金额（按 BU、month 逐单元相加）
    final = _add_blocks(
        "合计分摊整体后",
        [he_ji[he_ji["BU"].isin(full_bu)].reset_index(drop=True),
         whole_alloc[whole_alloc["BU"].isin(full_bu)].reset_index(drop=True)],
        full_bu,
        months,
    )

    return {
        "inbound": inbound,
        "outbound": outbound,
        "others": others,
        "VA成本": va,
        "收款通道成本": pc,
        "合计": he_ji,
        "整体分摊金额": whole_alloc,
        "合计分摊整体后": final,
    }


__all__ = ["summarize"]
