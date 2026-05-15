"""根据路径判断文件分组（对应业务表「文件」列）。"""

from __future__ import annotations

import re
from pathlib import Path


def classify_citi_csv_name(path: Path) -> str:
    n = path.name.lower()
    if "pphk-other" in n or "pphk_other" in n:
        return "ppbk_other"
    if re.search(r"(?<![a-z])pphk", n):
        return "ppbk_main"
    return "citi_other"


def ppeu_sheet_to_channel(sheet_name: str) -> str | None:
    """PPEU 工作表名 → 处理表「渠道」列常用取值（与 rules 一致）。"""
    s = sheet_name.strip().lower()
    if s in ("citi", "city"):
        return "CITI"
    if s == "bgl":
        return "BGL"
    if "banking circle" in s:
        return "Banking Circle"
    if "barclays" in s:
        return "Barclays"
    if "queen" in s:
        return "Queen Bee"
    return None


def ppeu_sheet_to_group(sheet_name: str) -> str:
    s = sheet_name.strip().lower()
    # CITI（部分文件误写为 CITY）
    if s in ("citi", "city"):
        return "ppeu_citi"
    if s == "bgl":
        return "ppeu_bgl"
    # 仅「Banking Circle」工作表（避免其它含 banking 的 sheet 误入）
    if "banking circle" in s:
        return "ppeu_bc"
    if "barclays" in s:
        return "ppeu_barclays"
    # Queen Bee（与 CITI 同组规则；金额字段仍走 pipeline 的取负/符号取反逻辑）
    if "queen" in s:
        return "ppeu_citi"
    return ""


def classify_path(path: Path) -> str | None:
    """返回 file_group，无法识别则 None。"""
    p = path.resolve()
    parts_lower = [x.lower() for x in p.parts]
    name = p.name.lower()

    if "2026.02citi流水" in str(p) or "citi流水" in str(p):
        if name.endswith(".csv"):
            return classify_citi_csv_name(p)
        return None

    if "ppeu" in name and name.endswith(".xlsx") and "bc&bgl" in name.replace(" ", "").lower():
        return "ppeu_workbook"

    if "jpm" in name and (name.endswith(".xls") or name.endswith(".xlsx")):
        return "jpm"

    if "boc" in str(p).lower() and name.endswith(".xlsx"):
        return "boc"

    if "scb" in name and name.endswith(".xlsx"):
        return "scb"

    if "bosh" in name:
        return "bosh"

    if "db" in name and "自有流水" in name and name.endswith(".xlsx"):
        return "db"

    if "db流水" in str(p) or "db" in parts_lower:
        if name.endswith(".csv"):
            return "db_csv"

    return None
