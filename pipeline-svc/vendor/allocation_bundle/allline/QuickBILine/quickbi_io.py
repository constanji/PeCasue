from __future__ import annotations

from pathlib import Path

import pandas as pd

SHEET0 = "Sheet0"

_M = "\u6708\u4efd"
_E = "\u4e3b\u4f53"
_CH = "\u6e20\u9053\u540d\u79f0"
_AC = "\u5927\u8d26\u53f7"
_DP = "\u7ef4\u62a4\u4eba\u90e8\u95e8"
_BS = "\u4e1a\u52a1\u7cfb\u7edf"
_KY = "\u5ba2\u6237kyc\u56fd\u5bb6"
_MT = "\u7ef4\u62a4\u4eba"
_B2 = "\u662f\u5426B2B\u673a\u6784\u7528\u6237"
_INN = "\u5165\u91d1\u7b14\u6570"
_INV = "\u5165\u91d1\u4ea4\u6613\u91cf"
_OUTN = "\u51fa\u91d1\u7b14\u6570"
_OUTV = "\u51fa\u91d1\u4ea4\u6613\u91cf"
INBOUND_COUNT_COL = _INN
INBOUND_VOL_COL = _INV
OUTBOUND_COUNT_COL = _OUTN
OUTBOUND_VOL_COL = _OUTV
_VA = "va\u6570"

FINAL_BU_COL = "\u6700\u7ec8 bu"

COLS_IN_OUT = [_M, _E, _CH, _AC, _DP, _BS, _KY, _MT, _B2]
MONTH_COL = _M
ENTITY_COL = _E
CHANNEL_COL = _CH
# QuickBI 往往只导出 11 列（无「最终 bu」）；读入 BASE 后再内存附加 FINAL_BU_COL。
COLS_INBOUND_BASE = COLS_IN_OUT[:7] + [_INN, _INV] + COLS_IN_OUT[7:]
COLS_OUTBOUND_BASE = COLS_IN_OUT[:7] + [_OUTN, _OUTV] + COLS_IN_OUT[7:]
# 后续 zip/汇总与模板仍按 12 列顺序（… + 最终 bu）
COLS_INBOUND = COLS_INBOUND_BASE + [FINAL_BU_COL]
COLS_OUTBOUND = COLS_OUTBOUND_BASE + [FINAL_BU_COL]
COLS_VA = COLS_IN_OUT[:7] + [_VA]


def _strip(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip()


def month_to_yyyymm(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = _strip(v)
    if not s or s.lower() == "nan":
        return ""
    if len(s) == 6 and s.isdigit():
        return s
    if len(s) >= 7 and s[4] == "-" and s[:4].isdigit() and s[5:7].isdigit():
        return s[:4] + s[5:7]
    return s


def _rename_final_bu_header_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """「最终bu」无空格别名 -> ``FINAL_BU_COL``。"""
    alias_no_space = "\u6700\u7ec8bu"
    if FINAL_BU_COL not in df.columns and alias_no_space in df.columns:
        df = df.rename(columns={alias_no_space: FINAL_BU_COL})
    return df


def read_quickbi_table(path: Path, cols: list[str]) -> pd.DataFrame:
    path = Path(path)
    name = path.name
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig",
        )
        return _ensure_columns(df, cols, name)
    df = pd.read_excel(path, sheet_name=SHEET0, dtype=object)
    return _ensure_columns(df, cols, name)


def _ensure_columns(df: pd.DataFrame, required: list[str], src_name: str) -> pd.DataFrame:
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError("%s missing cols %s have %s" % (src_name, miss, list(df.columns)))
    out = df[required].copy()
    for c in required:
        out[c] = out[c].map(lambda x: "" if pd.isna(x) else x)
    return out


def _read_io_with_optional_final_bu(path: Path, base_cols: list[str]) -> pd.DataFrame:
    path = Path(path).resolve()
    name = path.name
    if path.suffix.lower() == ".csv":
        raw = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig",
        )
    else:
        raw = pd.read_excel(path, sheet_name=SHEET0, dtype=object)
    raw = _rename_final_bu_header_if_needed(raw)
    bu_saved: pd.Series | None = None
    if FINAL_BU_COL in raw.columns:
        bu_saved = raw[FINAL_BU_COL].map(lambda x: "" if pd.isna(x) else x)
    df = _ensure_columns(raw, base_cols, name)
    out = df.copy()
    if bu_saved is not None:
        out[FINAL_BU_COL] = bu_saved.values
    else:
        out[FINAL_BU_COL] = ""
    return out


def read_quickbi_inbound(path: Path) -> pd.DataFrame:
    return _read_io_with_optional_final_bu(path, COLS_INBOUND_BASE)


def read_quickbi_outbound(path: Path) -> pd.DataFrame:
    return _read_io_with_optional_final_bu(path, COLS_OUTBOUND_BASE)


def read_quickbi_va(path: Path) -> pd.DataFrame:
    return read_quickbi_table(path, COLS_VA)


def coerce_numeric_for_excel(val: object) -> object:
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if isinstance(val, float) and pd.isna(val):
            return None
        return val
    s = _strip(val)
    if not s:
        return None
    try:
        f = float(s.replace(",", ""))
        if f == int(f):
            return int(f)
        return f
    except ValueError:
        return val
