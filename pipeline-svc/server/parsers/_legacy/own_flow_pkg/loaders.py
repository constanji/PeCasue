"""读取各渠道导出（编码与表头行容错）。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def read_csv_auto(path: Path) -> pd.DataFrame:
    path = path.expanduser().resolve()
    raw = path.read_bytes()[:4]
    encodings = ["utf-16-le", "utf-16", "utf-8-sig", "utf-8", "cp1252", "latin-1"]
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        encodings = ["utf-16", "utf-16-le", "utf-16-be"] + [e for e in encodings if e not in ("utf-16",)]

    last_err: Exception | None = None
    for enc in encodings:
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                on_bad_lines="skip",
                engine="python",
            )
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"无法读取 CSV {path}: {last_err}")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def read_citi_csv(path: Path) -> pd.DataFrame:
    df = read_csv_auto(path)
    return normalize_columns(df)


def read_excel_first_sheet(path: Path, header: int = 0) -> pd.DataFrame:
    path = path.expanduser().resolve()
    eng = "openpyxl" if path.suffix.lower() == ".xlsx" else "xlrd"
    df = pd.read_excel(path, sheet_name=0, header=header, engine=eng)
    return normalize_columns(df)


def drop_boc_balance_column(df: pd.DataFrame) -> pd.DataFrame:
    """BOC 导出中的 Balance 仅为对账参考，不参与规则匹配与标准列导出。"""
    to_drop = [c for c in df.columns if str(c).strip().lower() == "balance"]
    if not to_drop:
        return df
    return df.drop(columns=to_drop)


def read_ppeu_sheet(path: Path, sheet_name: str, header: int = 13) -> pd.DataFrame:
    path = path.expanduser().resolve()
    df = pd.read_excel(path, sheet_name=sheet_name, header=header, engine="openpyxl")
    return normalize_columns(df)


def read_jpm_details(path: Path, sheet: str = "Details") -> pd.DataFrame:
    path = path.expanduser().resolve()
    eng = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
    df = pd.read_excel(path, sheet_name=sheet, engine=eng)
    df = normalize_columns(df)
    if "Payment Details" not in df.columns:
        df["Payment Details"] = ""
    return df


def row_dict(df: pd.DataFrame, idx: int) -> dict[str, Any]:
    return {str(k): v for k, v in df.iloc[idx].items()}


def all_row_dicts(df: pd.DataFrame) -> list[dict[str, Any]]:
    """批量转 list[dict]，比逐行 row_dict 快 10 倍以上。"""
    cols = [str(c) for c in df.columns]
    records = df.to_dict("records")
    if cols == list(df.columns):
        return records
    return [{cols[i]: v for i, (_, v) in enumerate(row.items())} for row in records]
