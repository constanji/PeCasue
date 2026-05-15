"""单元格与规则匹配。"""

from __future__ import annotations

import re
from typing import Any

from .rules import MatchKind, OwnFlowRule


def _cell_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


# 德意志 DB 日报：与账户 mapping「支行简称」一致为 DB-HK / DB-KR / DB-TH
_DB_REGION_HINTS: dict[str, tuple[str, ...]] = {
    "DB-HK": ("DBHONGKONG", "DEUTHKH"),
    "DB-KR": ("DBSEOUL", "DEUTKRS"),
    "DB-TH": ("DBBANGKOK", "DEUTTHB"),
}


def _db_row_matches_region(row: dict[str, Any], region: str) -> bool:
    r = _normalize_db_region_tag(region)
    hints = _DB_REGION_HINTS.get(r, ())
    if not hints:
        return False
    bb = _cell_str(row.get("Bank/Branch")).upper()
    swift = _cell_str(row.get("Bank/Branch SWIFT Code") or row.get("Bank Key")).upper()
    blob = f"{bb} {swift}"
    return any(h in blob for h in hints)


def _normalize_db_region_tag(region: str) -> str:
    """处理表「文件」列可为 DBKR/DBTH 或 DB-KR/DB-TH，统一为 mapping 支行简称。"""
    s = (region or "").strip().upper().replace(" ", "")
    aliases = {
        "DBHK": "DB-HK",
        "DBKR": "DB-KR",
        "DBTH": "DB-TH",
        "DB-HK": "DB-HK",
        "DB-KR": "DB-KR",
        "DB-TH": "DB-TH",
    }
    return aliases.get(s, s)


def _is_db_charge_movement_rule(rule: OwnFlowRule) -> bool:
    """DB-KR/DB-TH：处理表写 Charge/ charge → 日报无费项描述列时，约定表示「该分行下 Sum of Transaction amount 非零的变动行」。"""
    if rule.file_group != "db" or not rule.db_region:
        return False
    if _normalize_db_region_tag(rule.db_region) not in ("DB-KR", "DB-TH"):
        return False
    return rule.kind == "iregex" and rule.pattern.strip().lower() == r"^charge$"


def match_value(kind: MatchKind, pattern: str, value: Any) -> bool:
    s = _cell_str(value)
    if not s and kind != "iexact":
        return False
    p = pattern
    if kind == "icontains":
        return p.lower() in s.lower()
    if kind == "equals":
        return s == p
    if kind == "istartswith":
        return s.lower().startswith(p.lower())
    if kind == "iexact":
        return s.lower() == p.lower()
    if kind == "iregex":
        return re.search(p, s, re.IGNORECASE) is not None
    return False


# CITI 原始 209 列里同名的列（不同交易方的 Name/Address 等），pandas 读入后会加 .1/.2/... 后缀。
# 规则里写的是裸列名（例如 Name/Address），匹配时应扫描所有后缀变体。
_CITI_MULTI_COLS = frozenset({"Name/Address", "Address Line 1", "Address Line 2", "Address Line 3", "Address Line 4"})


def _iter_column_values(row: dict[str, Any], col: str) -> list[Any]:
    """返回 row 中 col 对应的所有值——对于 CITI 多列重名场景，同时返回 col、col.1、col.2 …… 全部。"""
    vals: list[Any] = []
    if col in row:
        vals.append(row.get(col))
    if col in _CITI_MULTI_COLS:
        prefix = f"{col}."
        for k in row.keys():
            if isinstance(k, str) and k.startswith(prefix) and k[len(prefix):].isdigit():
                vals.append(row.get(k))
    return vals


def _money_to_float(v: Any) -> float | None:
    """与 pipeline._as_float 一致：含 $ 千分位、括号负数的可解析为浮点。"""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ("nan", "none", "null", "-"):
            return None
        s = s.replace(",", "")
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].replace(",", "")
        for sym in ("$", "€", "£", "¥", "￥"):
            s = s.replace(sym, "")
        s = s.strip()
        if not s:
            return None
    else:
        if isinstance(v, float) and v != v:  # NaN
            return None
        try:
            x = float(v)
        except (TypeError, ValueError, OverflowError):
            return None
        if x != x:
            return None
        return x
    try:
        x = float(s)  # type: ignore[has-type]
    except (ValueError, TypeError):
        return None
    if x != x:
        return None
    return x


def _row_abs_amount(row: dict[str, Any]) -> float | None:
    """用于规则 max_abs_amount 的 |金额|：与 BOC 等 pipeline 同口径，支持 Amount/带 $ 的 Debit。"""
    for k in (
        "Transaction Amount",
        "Amount",
        "Debit Amount",
        "Credit Amount",
        "Debit",
        "Credit",
    ):
        v = row.get(k)
        if v is None:
            continue
        x = _money_to_float(v)
        if x is None or abs(x) < 1e-12:
            continue
        return abs(x)
    return None


def row_matches_rule(row: dict[str, Any], rule: OwnFlowRule) -> bool:
    if rule.account_name_icontains:
        an = _cell_str(row.get("Account Name")) or _cell_str(row.get("Account"))
        if rule.account_name_icontains.lower() not in an.lower():
            return False
    if rule.db_region:
        if not _db_row_matches_region(row, rule.db_region):
            return False
        if _is_db_charge_movement_rule(rule):
            return True
    if rule.max_abs_amount is not None:
        amt = _row_abs_amount(row)
        if amt is None or amt > float(rule.max_abs_amount) + 1e-9:
            return False
    col = rule.column
    values = _iter_column_values(row, col)
    if not values:
        return False
    # BOC：处理表常用「ACH-」前缀，源数据可能为「ACH」开头无连字符
    if col == "Description" and rule.kind == "istartswith":
        pat = (rule.pattern or "").strip().lower()
        if pat.startswith("ach"):
            for val in values:
                s = _cell_str(val).lower()
                if s.startswith(pat):
                    return True
                if pat.startswith("ach-") and s.startswith("ach") and not s.startswith("ach-"):
                    return True
    for val in values:
        if match_value(rule.kind, rule.pattern, val):
            return True
    return False


def first_matching_rule(row: dict[str, Any], rules: list[OwnFlowRule]) -> OwnFlowRule | None:
    for rule in sorted(rules, key=lambda r: r.priority):
        if row_matches_rule(row, rule):
            return rule
    return None


def first_matching_rule_db(
    row: dict[str, Any],
    rules: list[OwnFlowRule],
    resolved_entity: str,
) -> OwnFlowRule | None:
    """德意志 DB：同一 file_group 下多主体子目录；仅当规则「主体」与当前子目录解析主体一致时才命中。"""
    re_u = (resolved_entity or "").strip().upper()
    for rule in sorted(rules, key=lambda r: r.priority):
        ed = (rule.entity_default or "*").strip()
        if ed not in ("*", "各主体", "其他主体", "") and str(ed).lower() != "nan":
            if re_u != ed.upper():
                continue
        if row_matches_rule(row, rule):
            return rule
    return None


def first_matching_rule_dbs(
    row: dict[str, Any],
    rules: list[OwnFlowRule],
    resolved_entity: str,
) -> OwnFlowRule | None:
    """DBS 同一 file_group 下多主体共存：仅当账户 mapping 解析出的主体与规则「主体」一致时才命中该条规则。"""
    re_u = (resolved_entity or "").strip().upper()
    for rule in sorted(rules, key=lambda r: r.priority):
        ed = (rule.entity_default or "*").strip()
        if ed not in ("*", "各主体", "其他主体", "") and str(ed).lower() != "nan":
            if re_u != ed.upper():
                continue
        if row_matches_rule(row, rule):
            return rule
    return None
