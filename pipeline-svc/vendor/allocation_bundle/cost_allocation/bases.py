#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""加载分摊基数与 mapping。

基数列坐标（与原模板公式 $G/$H/$J/$K/$M/$N 对应）：
- G 笔数、H 交易量、J month、K BU、M 渠道-分行、N 主体（主体.1）
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


# ----- 文本规整 -------------------------------------------------------------


def _strip(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip()


def norm_month(v: object) -> str:
    """把 month 规整为 YYYYMM 字符串。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if hasattr(v, "year") and hasattr(v, "month"):
        return "%04d%02d" % (v.year, v.month)
    s = _strip(v)
    if not s or s.lower() == "nan":
        return ""
    if s.replace(".", "", 1).isdigit() and "." in s:
        try:
            f = float(s)
            if f == int(f):
                s = str(int(f))
        except ValueError:
            pass
    if len(s) == 6 and s.isdigit():
        return s
    if len(s) == 7 and s[4] == "-" and s[:4].isdigit() and s[5:7].isdigit():
        return s[:4] + s[5:7]
    return s


def norm_entity(s: object) -> str:
    return _strip(s).upper()


def norm_channel(s: object) -> str:
    """渠道-分行：大写，CITIKHK → CITIHK 等容错。"""
    b = _strip(s).upper()
    if b == "CITIKHK":
        return "CITIHK"
    return b


# ----- 基数 sheet -----------------------------------------------------------


# 原模板基数 sheet 的 N 列（第 14 列）是「主体」，pandas 读出后叫 `主体.1`；
# 另外出金笔数 sheet 右侧有若干无关列（Unnamed / 日期样列），在加载时按位置截断。
_BASE_USECOLS = ["month", "BU", "渠道-分行", "主体.1"]


def _load_base_sheet(
    path: Path,
    sheet: str,
    *,
    count_col: str,
    vol_col: str,
) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet, header=1)
    need = set(_BASE_USECOLS) | {count_col, vol_col}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(
            f"基数 sheet {sheet!r} 缺少列: {sorted(miss)}；实际列: {list(df.columns)}"
        )
    out = pd.DataFrame(
        {
            "month": df["month"].map(norm_month),
            "BU": df["BU"].map(_strip),
            "channel": df["渠道-分行"].map(norm_channel),
            "entity": df["主体.1"].map(norm_entity),
            "count": pd.to_numeric(df[count_col], errors="coerce").fillna(0.0),
            "volume": pd.to_numeric(df[vol_col], errors="coerce").fillna(0.0),
        }
    )
    out = out[out["month"].ne("") & out["BU"].ne("")].reset_index(drop=True)
    return out


def _load_va_sheet(path: Path) -> pd.DataFrame:
    """加载 `VA个数` sheet：列结构 ≈ 入金/出金笔数，但只有 PP有效VA数 一个数量列。"""
    df = pd.read_excel(path, sheet_name="VA个数", header=1)
    count_col = None
    for c in df.columns:
        if "有效VA" in str(c) or "VA数" in str(c):
            count_col = c
            break
    if count_col is None:
        raise ValueError(f"VA个数 sheet 找不到 VA 数量列；实际列: {list(df.columns)}")
    # `主体` 可能是 `主体` 或 `主体.1`
    entity_col = "主体.1" if "主体.1" in df.columns else ("主体" if "主体" in df.columns else None)
    if entity_col is None:
        raise ValueError(f"VA个数 sheet 找不到主体列；实际列: {list(df.columns)}")
    bu_col = "BU.1" if "BU.1" in df.columns else ("BU" if "BU" in df.columns else None)
    ch_col = "渠道-分行"
    if ch_col not in df.columns:
        raise ValueError(f"VA个数 sheet 缺少 渠道-分行 列；实际列: {list(df.columns)}")
    out = pd.DataFrame(
        {
            "month": df["month"].map(norm_month),
            "BU": df[bu_col].map(_strip),
            "channel": df[ch_col].map(norm_channel),
            "entity": df[entity_col].map(norm_entity),
            "va_count": pd.to_numeric(df[count_col], errors="coerce").fillna(0.0),
        }
    )
    out = out[out["month"].ne("") & out["BU"].ne("")].reset_index(drop=True)
    return out


@dataclass
class Bases:
    """入金/出金/VA 基数表，支持按任意维度组合聚合。"""

    inbound: pd.DataFrame
    outbound: pd.DataFrame
    va: pd.DataFrame

    @classmethod
    def load(cls, path: Path) -> "Bases":
        path = path.expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"基数模板不存在: {path}")
        inb = _load_base_sheet(path, "入金笔数", count_col="入金笔数", vol_col="入金交易量")
        outb = _load_base_sheet(path, "出金笔数", count_col="出金笔数", vol_col="出金交易量")
        va = _load_va_sheet(path)
        return cls(inbound=inb, outbound=outb, va=va)

    @staticmethod
    def _sum(
        df: pd.DataFrame,
        field: str,
        *,
        month: str | None,
        entity: str | None = None,
        channel: str | None = None,
        bu: str | None = None,
    ) -> float:
        if df.empty:
            return 0.0
        m = pd.Series(True, index=df.index)
        if month is not None and month != "":
            m &= df["month"].eq(norm_month(month))
        if entity is not None and entity != "":
            m &= df["entity"].eq(norm_entity(entity))
        if channel is not None and channel != "":
            m &= df["channel"].eq(norm_channel(channel))
        if bu is not None and bu != "":
            m &= df["BU"].eq(_strip(bu))
        s = df.loc[m, field]
        return float(s.sum()) if len(s) else 0.0

    def sum_base(
        self,
        direction: str,
        field: str,
        *,
        month: str | None,
        entity: str | None = None,
        channel: str | None = None,
        bu: str | None = None,
    ) -> float:
        """direction ∈ {in, out}；field ∈ {count, volume}"""
        d = direction.lower()
        f = field.lower()
        if d not in ("in", "out"):
            raise ValueError(f"direction 需为 in/out，得到 {direction!r}")
        if f not in ("count", "volume"):
            raise ValueError(f"field 需为 count/volume，得到 {field!r}")
        df = self.inbound if d == "in" else self.outbound
        return self._sum(df, f, month=month, entity=entity, channel=channel, bu=bu)

    def sum_va(
        self,
        *,
        month: str | None,
        entity: str | None = None,
        channel: str | None = None,
        bu: str | None = None,
    ) -> float:
        """PP 有效 VA 数按给定维度求和。"""
        return self._sum(self.va, "va_count", month=month, entity=entity, channel=channel, bu=bu)


# ----- mapping --------------------------------------------------------------


@dataclass
class MappingInfo:
    """mapping sheet + 收款通道成本覆盖表 + 统一收款通道渠道清单。"""

    bill_to_channel: dict[str, str]
    methods: dict[str, dict[str, str]]
    bu_list: list[str]
    # 收款通道成本 sheet：{(账单渠道大写, 主体大写): 分摊基数}
    pc_methods: dict[tuple[str, str], str]
    # 统一列表：命中的账单渠道（大写）无论 inbound/outbound/others/va，
    # 成本总额一律归入 `收款通道成本`。来自 script/分摊/files/rules/收款通道渠道清单.csv。
    pc_unified_channels: set[str]

    def channel_for_bill(self, bill: str) -> str:
        return self.bill_to_channel.get(norm_channel(bill), "")

    def is_pc_channel(self, bill: str) -> bool:
        return norm_channel(bill) in self.pc_unified_channels

    def method_for(self, bill: str, cost_type: str, entity: str = "") -> str:
        ct = (cost_type or "").strip().lower()
        if ct == "收款通道成本":
            key = (norm_channel(bill), norm_entity(entity))
            m = self.pc_methods.get(key, "")
            if m:
                return m
            # 统一清单命中但 pc_methods 未覆盖 → 回落到 mapping 的 other分摊方式
            # （没有起量的新接入渠道，业务上按「其他」处理；若 mapping 也无，返回空）
            k = norm_channel(bill)
            if k in self.methods:
                return self.methods[k].get("other", "")
            return ""
        k = norm_channel(bill)
        if k not in self.methods:
            return ""
        m = self.methods[k]
        if ct == "inbound":
            return m.get("in", "")
        if ct == "outbound":
            return m.get("out", "")
        if ct == "others":
            return m.get("other", "")
        if ct == "va":
            return m.get("va", "")
        return ""


def load_mapping(path: Path, *, pc_channels_csv: Path | None = None) -> MappingInfo:
    path = path.expanduser().resolve()
    raw = pd.read_excel(path, sheet_name="mapping", header=None)
    # 表头位于第 2 行（索引 1）；数据从第 3 行起
    hdr_row = 1
    df = pd.read_excel(path, sheet_name="mapping", header=hdr_row)

    def _pick(col_candidates: list[str]) -> str:
        for c in col_candidates:
            if c in df.columns:
                return c
        raise ValueError(f"mapping 缺少列: {col_candidates}")

    col_bill = _pick(["账单渠道"])
    col_ch = _pick(["对应渠道"])
    col_in = _pick(["入金分摊方式"])
    col_out = _pick(["出金分摊方式"])
    col_other = _pick(["other分摊方式"])
    col_va = _pick(["VA分摊"])

    bill_to_channel: dict[str, str] = {}
    methods: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        bill = norm_channel(r.get(col_bill))
        if not bill:
            continue
        ch = _strip(r.get(col_ch))
        if bill not in bill_to_channel:
            bill_to_channel[bill] = ch
        methods[bill] = {
            "in": _strip(r.get(col_in)),
            "out": _strip(r.get(col_out)),
            "other": _strip(r.get(col_other)),
            "va": _strip(r.get(col_va)),
        }

    # BU 清单：mapping 首列自顶向下直到「合计」前，全部非空值即为 BU 列名
    bu_list: list[str] = []
    first_col = raw.iloc[:, 0]
    for v in first_col:
        s = _strip(v)
        if s in ("", "bu", "BU"):
            continue
        if s == "合计":
            break
        bu_list.append(s)

    pc_methods = _load_pc_methods(path)
    pc_unified_channels = _load_pc_unified_channels(pc_channels_csv)
    return MappingInfo(
        bill_to_channel=bill_to_channel,
        methods=methods,
        bu_list=bu_list,
        pc_methods=pc_methods,
        pc_unified_channels=pc_unified_channels,
    )


def _load_pc_unified_channels(path: Path | None) -> set[str]:
    """读「统一收款通道渠道清单」CSV；空集表示该功能关闭。"""
    if path is None:
        return set()
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        return set()
    df = pd.read_csv(p, dtype=str).fillna("")
    col = "分行维度" if "分行维度" in df.columns else df.columns[0]
    return {norm_channel(x) for x in df[col].tolist() if _strip(x)}


def _load_pc_methods(path: Path) -> dict[tuple[str, str], str]:
    """加载「收款通道成本」sheet 的 (账单渠道, 主体) → 分摊基数 覆盖映射。"""
    try:
        df = pd.read_excel(path, sheet_name="收款通道成本", header=1)
    except (ValueError, KeyError):
        return {}
    # 该 sheet 合并了两列同名「渠道名称（分行维度）」，pandas 读出后第二列带 .1 后缀
    bill_col = "渠道名称（分行维度）.1" if "渠道名称（分行维度）.1" in df.columns else "渠道名称（分行维度）"
    need = {bill_col, "主体", "分摊基数"}
    miss = need - set(df.columns)
    if miss:
        return {}
    out: dict[tuple[str, str], str] = {}
    for _, r in df.iterrows():
        bill = norm_channel(r.get(bill_col))
        entity = norm_entity(r.get("主体"))
        method = _strip(r.get("分摊基数"))
        if not bill or not entity or not method:
            continue
        out.setdefault((bill, entity), method)
    return out


__all__ = [
    "Bases",
    "MappingInfo",
    "load_mapping",
    "norm_month",
    "norm_entity",
    "norm_channel",
]
