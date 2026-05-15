"""从「完整账号mapping.xlsx」或「huilv」类原模板解析，写入 ``PIPELINE_DATA_DIR/rules/files``（xlsx + csv）；抛出可读错误。"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_fx_rate_value(x: Any) -> float | None:
    """
    汇率保留 6 位有效数字（与 Python 的 .6g 一致），
    例如 0.00773111192731873 → 0.00773111。
    Excel 常见「1,964.90」字符串需先去掉千分位逗号。
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, str):
        x = x.replace(",", "").replace("\u00a0", "").strip()
        if not x:
            return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v:  # NaN
        return None
    return float(f"{v:.6g}")

def _files_root() -> Path:
    """规则根目录：默认 ``get_rules_files_dir()``；可通过 ``OWN_FLOW_FILES_ROOT`` 覆盖。"""
    env = os.environ.get("OWN_FLOW_FILES_ROOT", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    from server.core.paths import get_rules_files_dir

    return get_rules_files_dir()


def _mapping_dir() -> Path:
    return _files_root() / "mapping"


def _fx_dir() -> Path:
    return _files_root() / "fx"


def _rules_dir() -> Path:
    return _files_root() / "rules"

ACCOUNT_NAME = "账户对应主体分行mapping表"
FEE_NAME = "账单及自有流水费项mapping表"
FX_SHEET_NAME = "各种货币对美元折算率"
RULES_SHEET = "处理表"
# 新版模版常用「自有流水处理表」，表头仍为「数据源 / 渠道」扫描定位。
RULES_SHEET_CANDIDATES = ("处理表", "自有流水处理表")
SPECIAL_NAME = "特殊来源主体分行mapping"

# 客资流水模版工作表（与 allline customer_flow / pingpong 模版一致）
CUSTOMER_MAPPING_SHEET = "客资流水MAPPING"
CUSTOMER_FEE_MAPPING_SHEET = "客资流水费项mapping表"
CUSTOMER_BRANCH_MAPPING_SHEET = "客资流水分行mapping"


class MappingImportError(Exception):
    """解析或校验失败时抛出，message 可直接展示给用户。"""


def _ensure_dirs() -> None:
    _mapping_dir().mkdir(parents=True, exist_ok=True)
    _fx_dir().mkdir(parents=True, exist_ok=True)
    _rules_dir().mkdir(parents=True, exist_ok=True)


def _save_df(df: pd.DataFrame, stem: Path) -> None:
    """同时写入 .xlsx 与 .csv（utf-8-sig）。"""
    df.to_excel(stem.with_suffix(".xlsx"), index=False, engine="openpyxl")
    df.to_csv(stem.with_suffix(".csv"), index=False, encoding="utf-8-sig")


def _strip_column_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: str(c).strip() if c is not None else "" for c in df.columns})


def _fx_rate_col_from_columns(cols: list[Any]) -> str | None:
    """优先「兑USD汇率」，否则「对美元折算率」。"""
    stripped = {str(c).strip(): c for c in cols}
    if "兑USD汇率" in stripped:
        return str(stripped["兑USD汇率"]).strip()
    if "对美元折算率" in stripped:
        return str(stripped["对美元折算率"]).strip()
    for c in cols:
        s = str(c).strip()
        if s in ("兑USD汇率", "对美元折算率"):
            return s
    return None


def _is_iso_4217_code(s: str) -> bool:
    t = s.strip().upper()
    return len(t) == 3 and t.isalpha()


def _is_fx_currency_token(s: str) -> bool:
    """ISO 4217 三位码及常见加密资产 ticker（如 USDT、USDC），不含数字以免误吸账号。"""
    t = str(s).strip().upper().split()[0]
    return 3 <= len(t) <= 12 and t.isalpha()


def _column_mostly_fx_codes(s: pd.Series) -> bool:
    vals = s.dropna().head(50)
    if len(vals) < 3:
        return False
    ok = 0
    for v in vals:
        if _is_fx_currency_token(str(v)):
            ok += 1
    return ok / max(len(vals), 1) >= 0.6


def _code_column_for_fx_table(df: pd.DataFrame) -> str | None:
    if "货币代码" in df.columns and _column_mostly_fx_codes(df["货币代码"]):
        return "货币代码"
    if "货币名称" in df.columns and _column_mostly_fx_codes(df["货币名称"]):
        # 如「202601-03 货币兑美元汇率表」将 ISO 三字母列误标为「货币名称」
        return "货币名称"
    return None


def _name_col_after_code(df: pd.DataFrame, code_col: str) -> str | None:
    """标准表：货币代码 + 货币名称；误标时 ISO 在「货币名称」列、中文在右侧列（如 Unnamed:1）。"""
    if code_col == "货币代码" and "货币名称" in df.columns:
        return "货币名称"
    if code_col != "货币名称":
        return None
    # 本列表头为「货币名称」、单元格为三位代码时，中文名在右邻列
    skip = {
        "货币单位",
        "对美元折算率",
        "兑USD汇率",
        "日期",
        "日期_货币名称",
    }
    cols = list(df.columns)
    try:
        i = cols.index("货币名称")
    except ValueError:
        return None
    for j in range(i + 1, len(cols)):
        cj = str(cols[j]).strip()
        if cj in skip or cj == "货币代码":
            continue
        if cj == "货币名称":
            continue
        if cj.startswith("日期") and cj != "日期_货币名称":
            continue
        return cols[j]
    return None


def parse_usd_fx_columnar_dataframe(data: pd.DataFrame) -> pd.DataFrame | None:
    """
    货币兑美元列式表：首列为 ISO / 加密资产 ticker（列名可能为「货币名称」或「货币代码」），
    汇率为「兑USD汇率」或「对美元折算率」。代码长度 3–12 位纯字母（含 USDT、ETH 等）。

    去重：同一货币有有效「日期」时取**最大日期**行（同日多行取**更靠表底**行）；无有效日期则取该货币在表中**最末**一行。
    """
    df = _strip_column_names(data)
    code_col = _code_column_for_fx_table(df)
    rate_key = _fx_rate_col_from_columns(list(df.columns))
    if code_col is None or rate_key is None:
        return None
    name_col = _name_col_after_code(df, code_col)
    has_date = "日期" in df.columns

    recs: list[dict[str, Any]] = []
    for pos, (_, r) in enumerate(df.iterrows()):
        c = r.get(code_col)
        if c is None or (isinstance(c, float) and pd.isna(c)):
            continue
        code_s = str(c).strip().upper().split()[0]
        if not _is_fx_currency_token(code_s):
            continue
        rate_val = normalize_fx_rate_value(r.get(rate_key))
        if rate_val is None:
            continue
        nm = ""
        if name_col and name_col in df.columns:
            v = r.get(name_col)
            nm = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
        un = r.get("货币单位", "")
        un = str(un).strip() if un is not None and not (isinstance(un, float) and pd.isna(un)) else ""
        d_raw = r.get("日期") if has_date else None
        recs.append(
            {
                "货币代码": code_s,
                "货币名称": nm,
                "货币单位": un,
                "对美元折算率": rate_val,
                "_d": d_raw,
                "_pos": pos,
            }
        )
    if not recs:
        return None

    t = pd.DataFrame(recs)
    t["dnum"] = pd.to_numeric(t["_d"], errors="coerce")
    picks: list[pd.Series] = []
    for _, g in t.groupby("货币代码", sort=False):
        if g["dnum"].notna().any():
            g2 = g[g["dnum"].notna()].copy()
            # 最新日期；同日多行取**更靠表底**的一行
            g2 = g2.sort_values(["dnum", "_pos"], ascending=[False, False])
            picks.append(g2.iloc[0])
        else:
            picks.append(g.sort_values("_pos", ascending=True).iloc[-1])
    out = pd.DataFrame(picks)
    return out[["货币代码", "货币名称", "货币单位", "对美元折算率"]].reset_index(drop=True)


def _read_excel_workbook(path: Path) -> pd.ExcelFile:
    path = path.expanduser().resolve()
    if not path.exists():
        raise MappingImportError(f"文件不存在: {path}")
    if path.suffix.lower() not in (".xlsx", ".xlsm"):
        raise MappingImportError(f"需要 Excel 文件（.xlsx），当前: {path.suffix}")
    try:
        return pd.ExcelFile(path, engine="openpyxl")
    except Exception as e:
        raise MappingImportError(f"无法打开 Excel（可能损坏或格式不对）: {path.name} — {e}") from e


def _read_usd_fx_sheet_columnar(path: Path, sheet_name: str) -> pd.DataFrame | None:
    """第 0 行即表头，与现用「货币兑美元汇率表」xlsx 同结构。"""
    raw = pd.read_excel(path, sheet_name=sheet_name, header=0, engine="openpyxl")
    return parse_usd_fx_columnar_dataframe(_strip_column_names(raw))


def extract_fx_from_usd_rate_columnar_workbook(path: Path) -> pd.DataFrame | None:
    """工作簿中首个可解析的货币兑美元工作表。汇率列优先「兑USD汇率」；数值 6 位有效数字在 normalize 中已处理。"""
    xl = _read_excel_workbook(path)
    for sheet_name in xl.sheet_names:
        out = _read_usd_fx_sheet_columnar(path, sheet_name)
        if out is not None and not out.empty:
            return out
    return None


def extract_processing_rules_from_workbook(path: Path) -> pd.DataFrame:
    xl = _read_excel_workbook(path)
    last_detail = ""
    for sheet_name in RULES_SHEET_CANDIDATES:
        if sheet_name not in xl.sheet_names:
            continue
        raw = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")
        header_idx = None
        for i in range(len(raw)):
            if str(raw.iloc[i, 0]).strip() == "数据源" and str(raw.iloc[i, 1]).strip() == "渠道":
                header_idx = i
                break
        if header_idx is None:
            last_detail = f"「{sheet_name}」中找不到表头行（数据源 / 渠道）。"
            continue
        cols = ["数据源", "渠道", "主体", "文件", "表头", "处理", "备注", "入账科目", "说明"]
        df = raw.iloc[header_idx + 1 :].copy()
        df.columns = cols[: len(df.columns)]
        df = df[df["数据源"].fillna("").astype(str).str.strip() == "自有流水"].reset_index(drop=True)
        df = df.dropna(subset=["表头", "处理"], how="all")
        if df.empty:
            last_detail = f"「{sheet_name}」中无「自有流水」有效规则行。"
            continue
        return df
    raise MappingImportError(
        last_detail
        or f"工作簿中缺少处理表（尝试工作表：{', '.join(RULES_SHEET_CANDIDATES)}）。"
    )


def load_customer_mapping_dataframe(path: Path) -> pd.DataFrame:
    """Read 客资流水MAPPING sheet (subject / channel-type columns preserved as-is)."""
    xl = _read_excel_workbook(path)
    if CUSTOMER_MAPPING_SHEET not in xl.sheet_names:
        raise MappingImportError(f"文件中缺少工作表「{CUSTOMER_MAPPING_SHEET}」。")
    df = pd.read_excel(path, sheet_name=CUSTOMER_MAPPING_SHEET, header=0, engine="openpyxl")
    df = _strip_column_names(df).dropna(how="all")
    if df.empty:
        raise MappingImportError(f"「{CUSTOMER_MAPPING_SHEET}」解析后无有效数据行。")
    return df


def load_customer_fee_mapping_dataframe(path: Path) -> pd.DataFrame:
    xl = _read_excel_workbook(path)
    if CUSTOMER_FEE_MAPPING_SHEET not in xl.sheet_names:
        raise MappingImportError(f"文件中缺少工作表「{CUSTOMER_FEE_MAPPING_SHEET}」。")
    df = pd.read_excel(path, sheet_name=CUSTOMER_FEE_MAPPING_SHEET, header=0, engine="openpyxl")
    df = _strip_column_names(df).dropna(how="all")
    if df.empty:
        raise MappingImportError(f"「{CUSTOMER_FEE_MAPPING_SHEET}」解析后无有效数据行。")
    return df


def load_customer_branch_mapping_dataframe(path: Path) -> pd.DataFrame:
    xl = _read_excel_workbook(path)
    if CUSTOMER_BRANCH_MAPPING_SHEET not in xl.sheet_names:
        raise MappingImportError(f"文件中缺少工作表「{CUSTOMER_BRANCH_MAPPING_SHEET}」。")
    df = pd.read_excel(path, sheet_name=CUSTOMER_BRANCH_MAPPING_SHEET, header=0, engine="openpyxl")
    df = _strip_column_names(df).dropna(how="all")
    if df.empty:
        raise MappingImportError(f"「{CUSTOMER_BRANCH_MAPPING_SHEET}」解析后无有效数据行。")
    return df


def load_account_mapping_dataframe(path: Path) -> pd.DataFrame:
    """Read account→entity/branch sheet without writing disk."""
    xl = _read_excel_workbook(path)
    if ACCOUNT_NAME not in xl.sheet_names:
        raise MappingImportError(f"文件中缺少工作表「{ACCOUNT_NAME}」。")
    df = pd.read_excel(path, sheet_name=ACCOUNT_NAME, header=0, engine="openpyxl")
    df = df.dropna(how="all")
    if "银行账号" not in df.columns:
        raise MappingImportError(f"「{ACCOUNT_NAME}」缺少必需列「银行账号」。")
    return df


def load_fee_mapping_dataframe(path: Path) -> pd.DataFrame:
    """Read bill / own-flow fee mapping sheet without writing disk."""
    xl = _read_excel_workbook(path)
    if FEE_NAME not in xl.sheet_names:
        raise MappingImportError(f"文件中缺少工作表「{FEE_NAME}」。")
    df = pd.read_excel(path, sheet_name=FEE_NAME, header=1, engine="openpyxl")
    df = df.dropna(how="all")
    if "渠道" not in df.columns:
        raise MappingImportError(f"「{FEE_NAME}」缺少必需列「渠道」。")
    if "费用项名称" not in df.columns and "费用项名称.1" not in df.columns:
        raise MappingImportError(f"「{FEE_NAME}」缺少「费用项名称」列。")
    return df


def load_fx_rates_standard_dataframe(path: Path) -> pd.DataFrame:
    """Four-column USD FX table from full mapping workbook or first parsable sheet."""
    xl = _read_excel_workbook(path)
    df = None
    if FX_SHEET_NAME in xl.sheet_names:
        df = _read_usd_fx_sheet_columnar(path, FX_SHEET_NAME)
    if df is None or df.empty:
        df = extract_fx_from_usd_rate_columnar_workbook(path)
    if df is None or df.empty:
        raise MappingImportError(_fx_import_error_detail())
    return df


def load_fx_standalone_workbook_dataframe(path: Path, sheet_name: str | int = 0) -> pd.DataFrame:
    """Single-purpose FX workbook (e.g. ``202601-03货币兑美元汇率表.xlsx``)."""
    xl = _read_excel_workbook(path)
    df = None
    if isinstance(sheet_name, str) and sheet_name in xl.sheet_names:
        df = _read_usd_fx_sheet_columnar(path, sheet_name)
    elif isinstance(sheet_name, int) and 0 <= sheet_name < len(xl.sheet_names):
        df = _read_usd_fx_sheet_columnar(path, xl.sheet_names[sheet_name])
    if df is None or df.empty:
        df = extract_fx_from_usd_rate_columnar_workbook(path)
    if df is None or df.empty:
        raise MappingImportError(_fx_import_error_detail())
    return df


def import_account_from_workbook(path: Path) -> int:
    df = load_account_mapping_dataframe(path)
    _ensure_dirs()
    _save_df(df, _mapping_dir() / ACCOUNT_NAME)
    return len(df)


def import_fee_from_workbook(path: Path) -> int:
    df = load_fee_mapping_dataframe(path)
    _ensure_dirs()
    _save_df(df, _mapping_dir() / FEE_NAME)
    return len(df)


def _fx_import_error_detail() -> str:
    return (
        "未识别为货币兑美元列式表（与 files 下「202601-03货币兑美元汇率表」同结构："
        "首列为 ISO 三字母、列名可能为「货币名称」；含「兑USD汇率」或「对美元折算率」；"
        "同一货币多行时优先取「日期」最大行，无有效日期则取表中最下方一行）。"
    )


def import_fx_from_full_mapping_workbook(path: Path) -> int:
    df = load_fx_rates_standard_dataframe(path)
    _ensure_dirs()
    _save_df(df, _fx_dir() / "各种货币对美元折算率")
    return len(df)


def import_fx_from_huilv(path: Path, sheet_name: str | int = 0) -> int:
    """单表导入；`sheet_name` 为工作表名或下标时先只读该表，失败再扫全簿（与 Step2 仅汇率一致）。"""
    df = load_fx_standalone_workbook_dataframe(path, sheet_name)
    _ensure_dirs()
    _save_df(df, _fx_dir() / "各种货币对美元折算率")
    return len(df)


def import_rules_from_workbook(path: Path) -> int:
    df = extract_processing_rules_from_workbook(path)
    _ensure_dirs()
    _save_df(df, _rules_dir() / "处理表")
    return len(df)


def import_full_mapping_workbook(path: Path, *, preserve_special: bool = True) -> dict[str, Any]:
    """
    从「完整账号mapping.xlsx」提取账户 / 费项 / 汇率 / 处理表，写入 files（xlsx+csv）。
    不覆盖「特殊来源主体分行mapping」（preserve_special=True 时始终不删不改）。
    """
    stats: dict[str, Any] = {}
    stats["账户行数"] = import_account_from_workbook(path)
    stats["费项行数"] = import_fee_from_workbook(path)
    stats["汇率行数"] = import_fx_from_full_mapping_workbook(path)
    stats["备注规则行数"] = import_rules_from_workbook(path)
    stats["特殊来源"] = "已保留（未修改）" if preserve_special else "未处理"
    return stats


def persist_account_mapping(df: pd.DataFrame) -> None:
    """写入账户 mapping（xlsx + csv）。"""
    _ensure_dirs()
    _save_df(df, _mapping_dir() / ACCOUNT_NAME)


def persist_fee_mapping(df: pd.DataFrame) -> None:
    """写入费项 mapping（xlsx + csv）。"""
    _ensure_dirs()
    _save_df(df, _mapping_dir() / FEE_NAME)


def persist_fx_rates(df: pd.DataFrame) -> None:
    """写入四列汇率表（xlsx + csv）。"""
    _ensure_dirs()
    _save_df(df, _fx_dir() / "各种货币对美元折算率")


def persist_rules(df: pd.DataFrame) -> None:
    """写入处理表（xlsx + csv）。"""
    _ensure_dirs()
    _save_df(df, _rules_dir() / "处理表")
    try:
        from .rules import invalidate_rules_cache

        invalidate_rules_cache()
    except Exception:
        pass


_MONTH_ZH_RE = re.compile(r"(20\d{2})\s*年\s*(1[0-2]|0?[1-9])\s*月")
_MONTH_ISO_RE = re.compile(
    r"(20\d{2})[-/](1[0-2]|0?[1-9])(?:[-/](?:0?[1-9]|[12]\d|3[01]))?"
)
_MONTH_YM_COMPACT_RE = re.compile(r"(20\d{2})(1[0-2]|0[1-9])(?!\d)")


def _zh_month_label(year: int, month: int) -> str:
    return f"{year}年{month}月"


def infer_fx_period_label(*, workbook_path: Path, hint_filenames: list[str | None] | None = None) -> str | None:
    """从上传文件名、路径 stem、工作表标题猜测汇率所属月份（展示用）。"""
    parts: list[str] = []
    if hint_filenames:
        for raw in hint_filenames:
            if raw:
                parts.append(Path(raw).stem)
    parts.append(workbook_path.stem)
    try:
        xf = pd.ExcelFile(workbook_path)
        parts.extend(list(xf.sheet_names))
    except Exception:
        pass
    for blob in parts:
        if not blob:
            continue
        blob_s = str(blob).strip()
        m = _MONTH_ZH_RE.search(blob_s)
        if m:
            return _zh_month_label(int(m.group(1)), int(m.group(2)))
        m = _MONTH_ISO_RE.search(blob_s)
        if m:
            return _zh_month_label(int(m.group(1)), int(m.group(2)))
        m = _MONTH_YM_COMPACT_RE.search(blob_s.replace(" ", ""))
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            if 1 <= mo <= 12:
                return _zh_month_label(y, mo)
    return None


def detect_huilv_style(path: Path) -> bool:
    """是否像 huilv：仅含加工合并类单表，且无「账户对应主体分行mapping表」。"""
    try:
        xl = _read_excel_workbook(path)
    except MappingImportError:
        return False
    names = set(xl.sheet_names)
    if ACCOUNT_NAME in names or FEE_NAME in names:
        return False
    if FX_SHEET_NAME in names:
        return False
    return any("加工" in n or "合并" in n for n in xl.sheet_names)
