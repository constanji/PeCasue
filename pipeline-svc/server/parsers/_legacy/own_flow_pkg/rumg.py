"""三菱 UFJ 等 RUMG 导出「全明細」类 CSV（日文、cp932）：首行为账户信息，明细行首列为 \"2\"。"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any


def parse_rumg_statement(path: Path) -> tuple[list[str], list[list[str]]]:
    """
    返回 (首行账户字段, 明细行列表)。
    明细行：第一列为 \"2\"；列为 日期、摘要、…、金额、…、残高 等。
    """
    raw = path.expanduser().resolve().read_bytes()
    text: str | None = None
    for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = raw.decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if r]
    if not rows:
        return [], []
    header = rows[0]
    data = [r for r in rows[1:] if r and str(r[0]).strip() == "2"]
    return header, data


def rumg_row_to_blue(header: list[str], row: list[str]) -> dict[str, Any]:
    """映射为与 CITI_RAW_KEYS 一致的蓝区字段。"""
    branch = header[2].strip() if len(header) > 2 else ""
    acct_no = header[6].strip() if len(header) > 6 else ""
    acct_name = header[7].strip() if len(header) > 7 else ""

    date = row[1].strip() if len(row) > 1 else ""
    desc = row[2].strip() if len(row) > 2 else ""
    extra = row[3].strip() if len(row) > 3 else ""

    amt_str = ""
    if len(row) > 4:
        amt_str = str(row[4]).replace(",", "").strip()
    amt: Any = float("nan")
    if amt_str:
        try:
            amt = float(amt_str)
        except ValueError:
            amt = float("nan")

    td = f"{desc} {extra}".strip() if extra else desc

    return {
        "Branch Name": branch,
        "Account Number": acct_no,
        "Account": acct_name,
        "Merchant ID": "",
        # RUMG 全明細为日元；「チャージ」行类型由处理表备注与 template_enrich 决定（默认 others）
        "Account Currency": "JPY",
        "Last Entry Date": date,
        "Transaction Amount": amt,
        "Product Type": "",
        "Transaction Description": td,
        "Payment Details": extra,
    }
