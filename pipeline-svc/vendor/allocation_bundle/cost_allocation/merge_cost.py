#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""读取「成本数据合并_八来源.xlsx」，按 (账单渠道, 主体, month, 成本类型) 聚合 USD 金额。"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .bases import MappingInfo, norm_channel, norm_entity, norm_month


# 合并表里 `类型` 的取值：inbound / Inbound / outbound / others / va / charge / 收款通道成本
# 纳入 5 类：inbound / outbound / others / va / 收款通道成本；charge 与空类型不参与分摊。
_VALID_TYPES = {"inbound", "outbound", "others", "va", "收款通道成本"}


@dataclass
class CostRow:
    bill_channel: str  # 账单渠道（分行维度）
    mapped_channel: str  # mapping 对应渠道（如 CITI-HK）
    entity: str
    month: str
    cost_type: str  # inbound/outbound/others
    cost: float
    method: str  # 分摊方式


def _norm_type(s: object) -> str:
    t = str(s or "").strip()
    tl = t.lower()
    if tl in _VALID_TYPES:
        return tl
    if t == "收款通道成本":
        return "收款通道成本"
    return ""


def load_cost_long(
    merged_path: Path,
    mapping: MappingInfo,
    *,
    default_month: str = "",
) -> tuple[list[CostRow], pd.DataFrame, pd.DataFrame, dict]:
    """返回 (聚合后的成本行列表, 未映射明细, 聚合前长表, 重归类统计)。

    - 按 (账单渠道, 主体, month, 成本类型) 聚合 USD 金额；
    - 按 mapping 填入 `mapped_channel` 与 `method`；
    - `mapping` 里找不到账单渠道的行会进入 `未映射` 表，但仍保留原渠道名称；
    - 若账单渠道命中「统一收款通道渠道清单」，则 inbound/outbound/others/va 全部
      重归为 `收款通道成本`；统计结果返回在第 4 元素。
    """
    merged_path = merged_path.expanduser().resolve()
    if not merged_path.is_file():
        raise FileNotFoundError(f"合并表不存在: {merged_path}")
    df = pd.read_excel(merged_path, sheet_name=0)
    need = {"主体", "分行维度", "入账期间", "USD金额", "类型", "入账科目"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"合并表缺少列: {sorted(miss)}")

    df = df[df["入账科目"].astype(str).str.strip() == "成本"].copy()
    df["_type"] = df["类型"].map(_norm_type)
    df = df[df["_type"] != ""].copy()

    df["_month"] = df["入账期间"].map(norm_month)
    if default_month:
        dm = norm_month(default_month)
        df.loc[df["_month"].eq(""), "_month"] = dm

    df["_bill"] = df["分行维度"].map(norm_channel)
    df["_entity"] = df["主体"].map(norm_entity)
    df["_cost"] = pd.to_numeric(df["USD金额"], errors="coerce").fillna(0.0)

    # 过滤掉账单渠道为空的（几乎不会出现）
    df = df[df["_bill"].ne("")].copy()

    # 重分类前按类型合计（与「合并表直接 sum 类型列」对账；入账科目已为成本、且已排除 charge/空类型）
    sums_pre = df.groupby("_type", dropna=False)["_cost"].sum()

    # 收款通道成本 重归类（按「渠道级」清单）：
    # 业务规则（来自 files/rules/收款通道渠道清单.csv）：命中清单的分行不区分
    # inbound/outbound/others/VA成本，成本总额一律归入 `收款通道成本`。
    reclass_stats: dict = {
        "命中渠道": [],
        "命中行数": 0,
        "命中金额": 0.0,
        "清单中未出现渠道": sorted(mapping.pc_unified_channels) if mapping.pc_unified_channels else [],
    }
    if mapping.pc_unified_channels:
        reclassify_mask = df["_bill"].isin(mapping.pc_unified_channels) & df["_type"].isin(
            {"inbound", "outbound", "others", "va"}
        )
        if reclassify_mask.any():
            hit = df.loc[reclassify_mask]
            reclass_stats["命中渠道"] = sorted(hit["_bill"].unique().tolist())
            reclass_stats["命中行数"] = int(reclassify_mask.sum())
            reclass_stats["命中金额"] = float(hit["_cost"].sum())
            df.loc[reclassify_mask, "_type"] = "收款通道成本"
        hit_channels = set(df.loc[df["_bill"].isin(mapping.pc_unified_channels), "_bill"].unique())
        reclass_stats["清单中未出现渠道"] = sorted(mapping.pc_unified_channels - hit_channels)

    # 行级重分类后、与聚合成本 sheet 同口径
    sums_post = df.groupby("_type", dropna=False)["_cost"].sum()
    reclass_stats["重分类前_各类型"] = {str(k): float(v) for k, v in sums_pre.items()}
    reclass_stats["重分类后_各类型"] = {str(k): float(v) for k, v in sums_post.items()}
    pre_pc = reclass_stats["重分类前_各类型"]
    post_pc = reclass_stats["重分类后_各类型"]
    to_pc = float(
        (pre_pc.get("inbound", 0) - post_pc.get("inbound", 0))
        + (pre_pc.get("outbound", 0) - post_pc.get("outbound", 0))
        + (pre_pc.get("others", 0) - post_pc.get("others", 0))
        + (pre_pc.get("va", 0) - post_pc.get("va", 0))
    )
    reclass_stats["自入出他VA划入收款"] = to_pc
    reclass_stats["重分类后_收款应增加额"] = float(
        post_pc.get("收款通道成本", 0) - pre_pc.get("收款通道成本", 0)
    )

    g = (
        df.groupby(["_bill", "_entity", "_month", "_type"], dropna=False, as_index=False)[
            "_cost"
        ].sum()
    )

    rows: list[CostRow] = []
    unmapped_rows = []
    for _, r in g.iterrows():
        bill = str(r["_bill"])
        entity = str(r["_entity"])
        mapped = mapping.channel_for_bill(bill)
        method = mapping.method_for(bill, r["_type"], entity=entity)
        row = CostRow(
            bill_channel=bill,
            mapped_channel=mapped,
            entity=entity,
            month=str(r["_month"]),
            cost_type=str(r["_type"]),
            cost=float(r["_cost"]),
            method=method,
        )
        rows.append(row)
        if not mapped or not method:
            unmapped_rows.append(
                {
                    "账单渠道": bill,
                    "主体": row.entity,
                    "month": row.month,
                    "成本类型": row.cost_type,
                    "成本金额": row.cost,
                    "对应渠道(mapping)": mapped,
                    "分摊方式(mapping)": method,
                }
            )

    unmapped = pd.DataFrame(unmapped_rows)
    agg = g.rename(
        columns={
            "_bill": "账单渠道",
            "_entity": "主体",
            "_month": "month",
            "_type": "成本类型",
            "_cost": "成本金额",
        }
    )
    return rows, unmapped, agg, reclass_stats


__all__ = ["CostRow", "load_cost_long"]
