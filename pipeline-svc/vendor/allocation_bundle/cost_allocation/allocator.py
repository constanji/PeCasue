#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把原模板的 IFS+SUMIFS 分摊分支翻译为 Python。

**默认**（`fallback_whole_pool=True`）：当「渠道+主体+月」下分母为 0 时，改按 **全公司、同月、同方式**
的基数池分摊，与《收付款分摊逻辑说明》§3.1「未起量」、以及常见**答案表**中「主站/福贸**占大头、
整体(残差) 较小」的口径一致（否则分母为 0 的通道整笔会堆在残差，与参考表差异很大）。

**严格**（`fallback_whole_pool=False`）：与 **IFS 单格** 一致，分母为 0 则各 BU=0、差额进 整体(残差)。

「固费分摊」始终只按「月」、在全 BU 上按笔数（公式仅 $J 对 月列）。
"""
from __future__ import annotations

from .bases import Bases

METHODS = (
    "入金笔数",
    "出金笔数",
    "入金交易量",
    "出金交易量",
    "总笔数",
    "总交易量",
    "固费分摊",
    "VA个数",
)

# method → 基数 sheet 方向 / 字段 (或 ("sum", ...) 表示 in+out)
_METHOD_SPEC: dict[str, tuple] = {
    "入金笔数": ("in", "count"),
    "出金笔数": ("out", "count"),
    "入金交易量": ("in", "volume"),
    "出金交易量": ("out", "volume"),
    "总笔数": ("both", "count"),
    "总交易量": ("both", "volume"),
    "VA个数": ("va", None),
}


def _sum_by_spec(
    bases: Bases,
    spec: tuple,
    *,
    month: str,
    entity: str | None,
    channel: str | None,
    bu: str | None,
) -> float:
    kind, field = spec
    if kind == "va":
        return bases.sum_va(month=month, entity=entity, channel=channel, bu=bu)
    if kind == "both":
        return bases.sum_base(
            "in", field, month=month, entity=entity, channel=channel, bu=bu
        ) + bases.sum_base(
            "out", field, month=month, entity=entity, channel=channel, bu=bu
        )
    return bases.sum_base(kind, field, month=month, entity=entity, channel=channel, bu=bu)


def _den_num(
    bases: Bases,
    method: str,
    *,
    month: str,
    entity: str,
    channel: str,
    bu: str | None,
) -> float:
    """按分摊方式聚合基数；bu=None 返回分母（全 BU），否则返回该 BU 的分子。"""
    m = method.strip()
    spec = _METHOD_SPEC.get(m)
    if spec is not None:
        return _sum_by_spec(bases, spec, month=month, entity=entity, channel=channel, bu=bu)
    if m == "固费分摊":
        # 与原公式一致：只按 month 过滤，不限主体与渠道
        return bases.sum_base(
            "in", "count", month=month, entity=None, channel=None, bu=bu
        ) + bases.sum_base(
            "out", "count", month=month, entity=None, channel=None, bu=bu
        )
    return 0.0


def allocate(
    cost: float,
    method: str,
    *,
    month: str,
    entity: str,
    channel: str,
    bases: Bases,
    bu_list: list[str],
    fallback_whole_pool: bool = True,
) -> dict[str, float]:
    """返回 {BU: 金额}。见模块 docstring。"""
    m = (method or "").strip()
    try:
        c = float(cost)
    except (TypeError, ValueError):
        c = 0.0
    out = {b: 0.0 for b in bu_list}
    if not m or m not in METHODS:
        return out

    # 一级：按渠道/主体口径
    denom = _den_num(bases, m, month=month, entity=entity, channel=channel, bu=None)
    use_entity: str | None = entity
    use_channel: str | None = channel
    if (denom == 0 or not (denom == denom)) and fallback_whole_pool and m != "固费分摊":
        # 二级 fallback：只按 month 过滤（全公司同方式池）
        denom = _den_num(bases, m, month=month, entity="", channel="", bu=None)
        use_entity = None
        use_channel = None

    if denom == 0 or not (denom == denom):
        return out

    spec = _METHOD_SPEC.get(m)
    if m == "固费分摊":
        for b in bu_list:
            num = _den_num(bases, m, month=month, entity=entity, channel=channel, bu=b)
            if num:
                out[b] = c * (num / denom)
        return out

    if spec is None:
        return out

    for b in bu_list:
        num = _sum_by_spec(
            bases, spec, month=month, entity=use_entity, channel=use_channel, bu=b
        )
        if num == 0:
            continue
        out[b] = c * (num / denom)
    return out


__all__ = ["allocate", "METHODS"]
