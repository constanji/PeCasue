"""Generic xlsx/csv → canonical row dispatcher.

Used by ``customer`` / ``special`` / ``cn_jp`` channels — these don't have a
single upstream entry point in either ``allline`` or ``pingpong``; instead
each bank ships a slightly different sheet/column layout. The dispatcher
walks every xlsx/csv/xls file in ``extracted_dir``, picks the canonical
columns by alias, and emits a single normalised CSV/XLSX (the SLIM_COLUMN_
ALIASES list is the same one allline/zhangdan/citi.py exposes for Citi
Service Activity exports — it covers ~90% of bank fee statements).

If a file's columns don't match any canonical name, we still emit a verify
row tagged ``warning`` so the Human can decide to add a new alias to the
rule book or hand the file off to the UnknownChannelStructurer agent.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# Canonical schema (kept identical to allline/zhangdan/citi.SLIM_COLUMN_ALIASES
# so downstream mappers / template-fillers can stay shared).
SLIM_COLUMN_ALIASES: List[Tuple[str, List[str]]] = [
    ("Account", [
        "Account", "Cuenta", "账号", "Account No", "Reference", "DEPOSIT ACCOUNT",
        "商户编号", "商户号", "店铺编号", "卖家编号", "Seller ID", "Client ID", "客户端账号",
        "账户主体",
    ]),
    ("Period", [
        "Period", "Periodo", "Date", "Posting Date", "入账期间", "Invoice Period",
        "账单周期", "结算周期", "账期", "统计周期", "BillDate", "ValueDate",
    ]),
    ("Description", [
        "Description", "DESCRIPTION", "Product Description", "描述", "Service Description",
        "费用名称", "费项", "费用类型", "项目名称", "账单类型", "费用说明",
        "Transaction Description",
    ]),
    ("Pricing Method", ["Pricing Method", "Service Type", "Unidades"]),
    ("Unit Price", ["Unit Price", "Unit\nPrice", "UNIT PRICE", "Avg Per Item", "Precio Unitario"]),
    ("Unit Price CCY", ["Unit Price CCY", "Precio Unitario CCY"]),
    ("Volume", ["Volume", "VOLUME", "Volumen", "Item Count", "笔数", "交易量", "订单笔数"]),
    ("Charge in Invoice CCY", [
        "Charge in Invoice CCY",
        "TOTAL CHARGE",
        "Amount",
        "Charge for\nService",
        "Importe de la Comisión",
        "账单金额", "应收金额", "含税金额", "结算金额", "手续费金额", "交易金额",
    ]),
    ("Invoice CCY", [
        "Invoice CCY",
        "TOTAL CHARGE CCY",
        "Tariff CCY",
        "Curr",
        "Aviso de Comisiones CCY",
        "币种", "货币", "结算币种",
        "Currency",
    ]),
    ("Taxable", ["Taxable", "Aplica Impuestos"]),
]

OPTIONAL_COLUMNS: List[Tuple[str, List[str]]] = [
    ("Branch", ["Branch", "Branch Name", "分行维度"]),
    ("Currency", ["Currency", "CCY"]),
]

CANONICAL_OUTPUT = [name for name, _ in SLIM_COLUMN_ALIASES] + ["Branch", "来源文件"]

# pingpong-master/script/customer/all.py — 原始 workbook 工作表「渠道对账单」，列名与银行 SLIM 模版不同。
_PINGPONG_CUSTOMER_REQUIRED_COLS = frozenset(
    {"账户主体", "Transaction Description", "Currency"}
)
_PINGPONG_CUSTOMER_AMOUNT_COLS = frozenset({"Debit Amount", "Credit Amount"})


def _pick_pingpong_customer_sheet(xls: pd.ExcelFile) -> str | None:
    names = list(xls.sheet_names)
    if "渠道对账单" in names:
        return "渠道对账单"
    for n in names:
        ns = str(n)
        if "渠道" in ns and "账单" in ns:
            return n
    return None


def _pingpong_cell_str(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, (pd.Timestamp,)):
        return val.isoformat()[:10]
    s = str(val).strip()
    if s.lower() in ("none", "nan", "nat"):
        return ""
    return s


def _try_pingpong_customer_statement(path: Path) -> Optional[FileParseOutcome]:
    """若为本渠道 Pingpong 客资原始表（渠道对账单），映射为 SLIM canonical 行。"""
    suf = path.suffix.lower()
    if suf not in (".xlsx", ".xls", ".xlsm"):
        return None
    engine = "xlrd" if suf == ".xls" else "openpyxl"
    try:
        xls = pd.ExcelFile(path, engine=engine)
    except Exception:
        return None
    sheet = _pick_pingpong_customer_sheet(xls)
    if sheet is None:
        return None

    df: Optional[pd.DataFrame] = None
    used_hdr: Optional[int] = None
    for hdr in range(12):
        try:
            # Use xls.parse() instead of pd.read_excel() to avoid reopening
            # the file on each iteration — this saves ~200MB per extra read.
            cand = xls.parse(sheet, header=hdr)
        except Exception:
            continue
        if cand is None or cand.empty:
            del cand
            continue
        cols_raw = [str(c).strip() for c in cand.columns]
        colset = set(cols_raw)
        compact_map = {str(c).strip().replace(" ", ""): str(c).strip() for c in cand.columns}

        def _has_col(name: str) -> bool:
            if name in colset:
                return True
            key = name.replace(" ", "")
            return key in compact_map

        if not all(_has_col(c) for c in _PINGPONG_CUSTOMER_REQUIRED_COLS):
            del cand
            continue
        if not any(_has_col(c) for c in _PINGPONG_CUSTOMER_AMOUNT_COLS):
            del cand
            continue
        df = cand
        used_hdr = hdr
        break
    xls.close()
    if df is None:
        return None

    cols = [str(c).strip() for c in df.columns]
    colset = set(cols)
    compact_rev = {str(c).strip().replace(" ", ""): str(c).strip() for c in df.columns}

    def col_one(options: tuple[str, ...]) -> str | None:
        for o in options:
            if o in colset:
                return o
            compact = o.replace(" ", "")
            if compact in compact_rev:
                return compact_rev[compact]
        return None

    c_debit = col_one(("Debit Amount", "DebitAmount"))
    c_credit = col_one(("Credit Amount", "CreditAmount"))

    rows_out: List[Dict[str, Any]] = []
    for _, raw in df.iterrows():
        if raw.isna().all():
            continue
        debit = pd.to_numeric(raw.get(c_debit) if c_debit else None, errors="coerce")
        credit = pd.to_numeric(raw.get(c_credit) if c_credit else None, errors="coerce")
        d_val = 0.0 if debit is None or pd.isna(debit) else float(debit)
        c_val = 0.0 if credit is None or pd.isna(credit) else float(credit)
        charge = d_val - c_val

        bill_dt = raw.get("BillDate")
        val_dt = raw.get("ValueDate")
        period_raw = bill_dt
        if period_raw is None or (isinstance(period_raw, float) and pd.isna(period_raw)):
            period_raw = val_dt
        period_s = _pingpong_cell_str(period_raw)

        rec: Dict[str, Any] = {"来源文件": path.name}
        for canon, _ in SLIM_COLUMN_ALIASES:
            rec[canon] = ""
        for canon, _ in OPTIONAL_COLUMNS:
            rec[canon] = ""

        rec["Account"] = _pingpong_cell_str(raw.get("账户主体"))
        rec["Period"] = period_s
        rec["Description"] = _pingpong_cell_str(raw.get("Transaction Description"))
        rec["Pricing Method"] = _pingpong_cell_str(raw.get("FundType"))
        rec["Charge in Invoice CCY"] = charge
        rec["Invoice CCY"] = _pingpong_cell_str(raw.get("Currency"))
        rec["Branch"] = _pingpong_cell_str(raw.get("地区"))
        rec["Currency"] = rec["Invoice CCY"]

        rows_out.append(rec)

    matched = len(SLIM_COLUMN_ALIASES) if rows_out else 0
    missing_total = [
        c
        for c, _ in SLIM_COLUMN_ALIASES
        if c
        not in {
            "Account",
            "Period",
            "Description",
            "Pricing Method",
            "Charge in Invoice CCY",
            "Invoice CCY",
        }
    ]
    return FileParseOutcome(
        file=path,
        rows=rows_out,
        matched_columns=matched,
        missing_canonical=missing_total,
        sheet=f"{sheet}(header={used_hdr})",
    )


def _pick_column(cols: Sequence[str], aliases: Sequence[str]) -> Optional[str]:
    norm = {str(c).strip(): c for c in cols if c is not None}
    for alias in aliases:
        if alias in norm:
            return norm[alias]
    # Loose match (lower-case, strip whitespace).
    lo = {str(c).strip().lower(): c for c in cols if c is not None}
    for alias in aliases:
        if alias.lower() in lo:
            return lo[alias.lower()]
    return None


@dataclass
class FileParseOutcome:
    file: Path
    rows: List[Dict[str, Any]]
    matched_columns: int
    missing_canonical: List[str]
    error: Optional[str] = None
    sheet: Optional[str] = None


def _read_excel_frames(
    path: Path,
    *,
    only_sheets: Optional[Sequence[str]] = None,
) -> Iterable[Tuple[str, pd.DataFrame]]:
    suf = path.suffix.lower()
    engine = "xlrd" if suf == ".xls" else "openpyxl"
    try:
        xls = pd.ExcelFile(path, engine=engine)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Excel 打开失败: {exc}") from exc
    names_all = list(xls.sheet_names)
    if only_sheets is not None:
        want = {str(s).strip() for s in only_sheets}
        names = [n for n in names_all if str(n).strip() in want]
        if not names:
            names = names_all
    else:
        names = names_all
    for name in names:
        best_df: Optional[pd.DataFrame] = None
        best_score = -1
        for hdr in range(10):
            try:
                df_try = xls.parse(name, header=hdr)
            except Exception:
                continue
            if df_try is None or df_try.empty:
                continue
            cols = [str(c).strip() for c in df_try.columns]
            score = sum(
                1 for _, aliases in SLIM_COLUMN_ALIASES if _pick_column(cols, aliases) is not None
            )
            if score > best_score:
                best_score = score
                best_df = df_try
            if score >= 6:
                break
        if best_df is not None and best_score >= 1:
            yield name, best_df


def _read_dataframe(
    path: Path,
    *,
    only_sheets: Optional[Sequence[str]] = None,
) -> Iterable[Tuple[str, pd.DataFrame]]:
    suf = path.suffix.lower()
    if suf in (".xlsx", ".xls", ".xlsm"):
        yield from _read_excel_frames(path, only_sheets=only_sheets)
    elif suf == ".csv":
        try:
            yield "", pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
        except Exception:
            yield "", pd.read_csv(path, encoding="gbk", on_bad_lines="skip")


def normalize_file(path: Path) -> FileParseOutcome:
    ping = _try_pingpong_customer_statement(path)
    if ping is not None:
        if ping.rows:
            return ping
        # 识别到 Pingpong 工作表但无数据行：不再走通用 SLIM（避免误匹配其它 sheet）
        if ping.sheet and not ping.error:
            return ping

    # pingpong-master/script/customer/all.py 只读「渠道对账单」。若通用 SLIM 扫整簿，
    # 其它 sheet（汇总/透视等）常被弱匹配到 1～2 列而整表输出，行数会远大于答案表。
    sheet_only: Optional[str] = None
    try:
        suf = path.suffix.lower()
        if suf in (".xlsx", ".xls", ".xlsm"):
            engine = "xlrd" if suf == ".xls" else "openpyxl"
            # Read sheet names only (low memory) — close immediately.
            xls_try = pd.ExcelFile(path, engine=engine)
            sheet_only = _pick_pingpong_customer_sheet(xls_try)
            xls_try.close()
            del xls_try
    except Exception:
        sheet_only = None

    rows_out: List[Dict[str, Any]] = []
    matched_total = 0
    missing_total: List[str] = []
    last_sheet: Optional[str] = None
    try:
        only = [sheet_only] if sheet_only else None
        for sheet_name, df in _read_dataframe(path, only_sheets=only):
            if df is None or df.empty:
                continue
            cols = [str(c).strip() for c in df.columns]
            mapping: Dict[str, str] = {}
            for canon, aliases in SLIM_COLUMN_ALIASES:
                src = _pick_column(cols, aliases)
                if src is not None:
                    mapping[canon] = src
            for canon, aliases in OPTIONAL_COLUMNS:
                src = _pick_column(cols, aliases)
                if src is not None:
                    mapping[canon] = src
            if len(mapping) < 3:
                # Almost no canonical columns matched — likely the wrong sheet.
                continue
            matched_total = max(matched_total, len(mapping))
            missing_total = [
                c for c, _ in SLIM_COLUMN_ALIASES if c not in mapping
            ]
            last_sheet = sheet_name
            for _, raw in df.iterrows():
                rec: Dict[str, Any] = {"来源文件": path.name}
                for canon, src in mapping.items():
                    val = raw.get(src)
                    rec[canon] = "" if pd.isna(val) else val
                # Fill canonical columns we didn't find with empty string so
                # downstream merge stays tabular.
                for canon, _ in SLIM_COLUMN_ALIASES:
                    rec.setdefault(canon, "")
                for canon, _ in OPTIONAL_COLUMNS:
                    rec.setdefault(canon, "")
                rows_out.append(rec)
    except Exception as exc:  # noqa: BLE001
        return FileParseOutcome(
            file=path,
            rows=[],
            matched_columns=0,
            missing_canonical=[c for c, _ in SLIM_COLUMN_ALIASES],
            error=str(exc),
            sheet=last_sheet,
        )
    return FileParseOutcome(
        file=path,
        rows=rows_out,
        matched_columns=matched_total,
        missing_canonical=missing_total,
        sheet=last_sheet,
    )


def write_canonical(rows: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = CANONICAL_OUTPUT
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def write_canonical_xlsx(rows: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=CANONICAL_OUTPUT)
    df.to_excel(out_path, index=False, engine="openpyxl")
