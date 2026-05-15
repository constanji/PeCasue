"""客资流水「渠道对账单」→ 与 pingpong-master ``script/customer/all.py`` 对齐的明细输出。

规则来源（与前端上传一致）：
    ``PIPELINE_DATA_DIR/rules/files/mapping/`` 下侧车 CSV/xlsx
    ``PIPELINE_DATA_DIR/rules/files/fx/各种货币对美元折算率.csv``
可选读取 RuleStore ``FX`` 的 ``meta.fx_month_label``（如 ``2026年3月``）作为无「日期」列时的默认账期。
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from server.core.paths import get_rules_files_dir
from server.parsers._legacy.own_flow_pkg.mapping_import import (
    CUSTOMER_BRANCH_MAPPING_SHEET,
    CUSTOMER_FEE_MAPPING_SHEET,
    CUSTOMER_MAPPING_SHEET,
    normalize_fx_rate_value,
)
from server.parsers._shared.dispatcher import _pick_pingpong_customer_sheet
from server.rules import store as rule_store
from server.rules.schema import RuleKind

# ---- 与 pingpong-master/script/customer/all.py 常量对齐 ----
K_COST = "成本"
K_NONCOST = "非成本"
K_SELF_FEE = "自有费用"
K_RECEIVED_COST = "收单成本"
K_DUP_BILL = "与账单重复"
K_VCC_COST = "VCC成本"
K_TAX = "税"
K_QUDAO = "渠道"
CHANNELS_EXCLUDE_TAX_SELF_FEE = frozenset({"DBS", "CITI", "KBANK"})

STEM_CUSTOMER_MAPPING = "客资流水MAPPING"
STEM_CUSTOMER_FEE = "客资流水费项mapping表"
STEM_CUSTOMER_BRANCH = "客资流水分行mapping"
FX_FILENAME = "各种货币对美元折算率.csv"

OUT_COLUMNS: List[str] = [
    "账户主体",
    "账户BU",
    "ValueDate",
    "Channel",
    "地区",
    "MerchantId",
    "Currency",
    "Credit Amount",
    "Debit Amount",
    "Transaction Description",
    "Extra Information",
    "Payment Detail",
    "Payee Name",
    "Drawee Name",
    "FundType",
    "Remark-description",
    "USD金额",
    "入账期间",
    "主体",
    "分行维度",
    "费项",
    "类型",
    "备注",
    "入账科目",
]

OUT_MAP = {
    "成本": K_COST,
    "非成本": K_NONCOST,
    "自有费用": K_SELF_FEE,
    "收单成本": K_RECEIVED_COST,
    "收到成本": K_RECEIVED_COST,
    "与账单重复": K_DUP_BILL,
    "渠道账单重复": K_DUP_BILL,
    "VCC成本": K_VCC_COST,
    "税": K_TAX,
}


def _norm_key_text(s: Any) -> str:
    if s is None:
        return ""
    t = str(s).strip()
    t = t.replace("\u00a0", " ")
    t = re.sub(r"\s+", " ", t)
    return t.upper()


def _norm_combo(channel: Any, desc: Any) -> str:
    return f"{_norm_key_text(channel)}-{_norm_key_text(desc)}"


def _period_yyyymm_for_fx(bill_raw: Any, value_raw: Any) -> Tuple[str, str]:
    def _try(v: Any) -> Tuple[Optional[str], Optional[str]]:
        if v is None:
            return None, None
        s = str(v).strip()
        if s in ("", "None", "nan", "NaT"):
            return None, None
        dt = pd.to_datetime(v, errors="coerce")
        if pd.isna(dt):
            return None, None
        return dt.strftime("%Y-%m"), dt.strftime("%Y%m")

    p, y = _try(bill_raw)
    if p:
        return p, y or ""
    p, y = _try(value_raw)
    if p:
        return p, y or ""
    v = value_raw if value_raw not in (None, "") else bill_raw
    s = str(v).strip() if v is not None else ""
    period = s[:7] if len(s) >= 7 else s
    yyyymm = (
        s.replace("-", "")[:6] if len(s.replace("-", "")) >= 6 else s
    )
    return period, yyyymm


def get_branch_dimension(
    channel: Any,
    std_sub: Any,
    acc_sub: Any,
    region: Any,
    subject_map: Dict[str, str],
    branch_map: Dict[str, str],
) -> str:
    channel_str = str(channel).strip().upper() if pd.notna(channel) else ""
    region_str = str(region).strip().upper() if pd.notna(region) else ""
    std = str(std_sub).strip() if pd.notna(std_sub) else ""
    acc = str(acc_sub).strip() if pd.notna(acc_sub) else ""

    if channel_str == "CITI":
        if std == "PPEU":
            if region_str == "PL":
                return "CITIPL"
            if region_str in ("LU", "GB", "DE"):
                return "CITIEU"
            return "CITI" + region_str
        if region_str in ("LU", "DE"):
            return "CITIEU"
        return "CITI" + region_str
    if channel_str == "SCB":
        return "SCB" + region_str
    if channel_str == "DB":
        return "DB" + region_str
    if channel_str == "DBS":
        return "DBS" + region_str
    if channel_str == "XENDIT":
        return "Xendit-" + region_str

    std_m = subject_map.get(acc, acc)
    keys_to_try = [
        f"{channel_str}_{std_m}",
        f"{channel_str}_{std}",
        f"{channel_str}_{acc}",
    ]
    seen: set[str] = set()
    for k in keys_to_try:
        if k in seen:
            continue
        seen.add(k)
        if k in branch_map:
            return branch_map[k]
    return channel_str


def _has_rule18_tax_fee(r: Dict[str, Any]) -> bool:
    cols = (
        "Transaction Description",
        "Extra Information",
        "Payment Detail",
        "Payee Name",
        "Drawee Name",
        "Remark-description",
    )
    for col in cols:
        s = str(r.get(col) or "")
        sl = s.lower()
        if "税" in s:
            return True
        if "withhold" in sl:
            return True
        if re.search(r"(?<![A-Z0-9])PPH(?![A-Z0-9])", s.upper()):
            return True
        if re.search(r"(?<![a-z0-9-])tax(?![a-z0-9-])", sl):
            return True
    return False


def assign_entry_subject(
    r: Dict[str, Any],
    ch_u: str,
    acc_sub: str,
    std_sub: str,
    br_dim: str,
    fundtype: Any,
    bu: Any,
    desc: Any,
    remark: Any,
) -> str:
    ft = str(fundtype or "").strip().lower()
    bu = str(bu or "").strip()
    std = str(std_sub or "").strip()
    acc = str(acc_sub or "").strip()
    br = str(br_dim or "").strip()
    d = str(desc or "").strip()
    rm = str(remark or "").strip()
    d_u = d.upper()

    def is_billing() -> bool:
        return ft.startswith("billing")

    sc = ""

    if bu == "ACQ":
        sc = K_RECEIVED_COST
    if ch_u == "JPM" and bu == "VCC":
        sc = K_VCC_COST
    if ch_u == "JPM" and (std == "PPHK" or acc == "PPHK") and bu == "SMB":
        sc = K_DUP_BILL
    if (
        (std == "PPHK" or acc == "PPHK")
        and br == "CITIUS"
        and ft == "charge"
        and bu == "VCC"
        and "云游send-ICA扣费" not in rm
    ):
        sc = K_VCC_COST
    if ch_u == "JPM" and std == "MANA AU" and is_billing():
        sc = K_DUP_BILL
    if ch_u == "CITI" and br in ("CITIID", "CITITH") and is_billing():
        sc = K_DUP_BILL
    if ch_u == "CITI" and br in ("CITIPH", "CITINZ") and (
        ("TAX" in d_u) or ("WITHHOLD" in d_u)
    ):
        sc = K_NONCOST
    if ch_u == "CITI" and std in ("PPEU", "PPUS", "PPI") and is_billing():
        sc = K_DUP_BILL
    if ch_u == "CITI" and std in ("MANA AU", "BRSG"):
        sc = K_NONCOST
    if ch_u == "RAKUTEN" and ("－Ｈ３１９" in d):
        sc = K_NONCOST
    if ch_u == "XENDIT" and std == "MANA-ID" and is_billing():
        sc = K_DUP_BILL
    if ch_u == "XENDIT" and std == "PPUS":
        sc = K_DUP_BILL
    if ch_u == "IBC" and (std == "PPUS" or acc == "PPUS"):
        sc = K_NONCOST

    if (
        sc == ""
        and ch_u not in CHANNELS_EXCLUDE_TAX_SELF_FEE
        and _has_rule18_tax_fee(r)
    ):
        sc = K_SELF_FEE
    if sc == "":
        sc = K_COST
    return sc


def _fee_lookup_combo(
    ch_u: str,
    fee_text: str,
    fee_map: Dict[str, str],
    fee_patterns: List[Tuple[re.Pattern[str], str]],
) -> str:
    if not fee_text:
        return ""
    k = _norm_combo(ch_u, fee_text)
    v = fee_map.get(k, "")
    if v:
        return v
    for rgx, typ in fee_patterns:
        if rgx.match(k):
            return typ
    return ""


def resolve_fee_type_non_dbs(
    ch_u: str,
    fee_item: str,
    fee_map: Dict[str, str],
    fee_patterns: List[Tuple[re.Pattern[str], str]],
    fee_channel_keys: set[str],
) -> str:
    nk = _norm_key_text(ch_u)
    if nk not in fee_channel_keys:
        return "others"
    return _fee_lookup_combo(ch_u, fee_item, fee_map, fee_patterns) or "others"


def _dbs_cell_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""
    if isinstance(v, (int, np.integer)):
        return str(int(v))
    if isinstance(v, float):
        if v == int(v):
            return str(int(v))
    s = str(v).strip()
    if s.lower() in ("none", "nan"):
        return ""
    return s


def dbs_resolve_fee_type(td: Any, extra: Any) -> str:
    td_raw = _dbs_cell_str(td)
    ex_raw = _dbs_cell_str(extra)
    td_u = re.sub(r"\s+", " ", td_raw).strip().upper()
    ex_u = re.sub(r"\s+", " ", ex_raw).strip().upper()

    if (
        any(x in td_u for x in ("495", "508", "506"))
        or re.search(r"TD.OUTGOING", ex_u)
        or re.search(r"TD.OUTGNG", ex_u)
    ):
        return "outbound"
    if td_u == "TRANSFER":
        return "outbound"
    if td_u == "INWARD TELEGRAPHIC TRANSFER COMM IB CHARGES":
        return "inbound"
    if "VA CHARGE" in ex_u:
        return "others"
    if "FPS FEE" in ex_u:
        return "outbound"
    if "INWARD TELEGRAPHIC TRANSFER" in td_u:
        return "inbound"
    if "INWARD TELEGRAPHIC TRANSFER" in ex_u:
        return "inbound"
    if "OUTWARD TELEGRAPHIC TRANSFER" in ex_u:
        return "outbound"
    if "FPS COMMISSION REBATE" in ex_u:
        return "outbound"
    if re.search(r"TD.INCMG TT FEE.", ex_u):
        return "inbound"
    if (
        re.search(r"TD.FAST PYMT CHG.", ex_u)
        or re.search(r"TD.REMITTANCE FEE.", ex_u)
        or re.search(r"TD.TT FEE", ex_u)
    ):
        if "PING PONG" in ex_u:
            return "inbound"
        return "outbound"
    return "others"


def resolve_fee_type(
    subject_code: Any,
    ch_u: str,
    fee_item: str,
    r: Dict[str, Any],
    channel_type_map: Dict[str, str],
    fee_map: Dict[str, str],
    fee_patterns: List[Tuple[re.Pattern[str], str]],
    fee_channel_keys: set[str],
) -> str:
    if str(subject_code).strip() != K_COST:
        return ""
    if ch_u == "DBS":
        return dbs_resolve_fee_type(
            r.get("Transaction Description", ""),
            r.get("Extra Information", ""),
        )
    ct = channel_type_map.get(_norm_key_text(ch_u), "")
    if ct == K_QUDAO:
        return "收款通道成本"
    return resolve_fee_type_non_dbs(
        ch_u, fee_item, fee_map, fee_patterns, fee_channel_keys
    )


def _normalize_fx_sheet_month(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (int, np.integer)):
        s = str(int(val))
        return s if len(s) == 6 and s.isdigit() else None
    if isinstance(val, float) and not np.isnan(val) and abs(val - round(val)) < 1e-9:
        s = str(int(round(val)))
        return s if len(s) == 6 and s.isdigit() else None
    s = str(val).strip()
    s = re.sub(r"\.0$", "", s)
    if re.fullmatch(r"\d{6}", s):
        return s
    ts = pd.to_datetime(val, errors="coerce")
    if pd.notna(ts):
        return ts.strftime("%Y%m")
    return None


def _zh_month_meta_to_yyyymm(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    m = re.search(r"(20\d{2})年(\d{1,2})月", str(label).strip())
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}"
    m2 = re.search(r"(20\d{2})[-/](\d{1,2})", str(label))
    if m2:
        mo = int(m2.group(2))
        if 1 <= mo <= 12:
            return f"{m2.group(1)}{mo:02d}"
    return None


def _fx_meta_default_yyyymm() -> Optional[str]:
    try:
        t = rule_store.load_rule(RuleKind.FX)
        lab = (t.meta or {}).get("fx_month_label") if t.meta else None
        return _zh_month_meta_to_yyyymm(str(lab) if lab else None)
    except Exception:
        return None


def _infer_yyyymm_from_workbook(path: Path, sheet: str) -> Optional[str]:
    try:
        df = pd.read_excel(path, sheet_name=sheet, header=0, engine="openpyxl")
    except Exception:
        return None
    best: Optional[pd.Timestamp] = None
    for col in ("BillDate", "ValueDate"):
        if col not in df.columns:
            continue
        s = pd.to_datetime(df[col], errors="coerce")
        mx = s.max()
        if pd.notna(mx) and (best is None or mx > best):
            best = mx
    if best is not None and pd.notna(best):
        return best.strftime("%Y%m")
    return None


def _load_branch_mapping_csv(path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path, header=None, encoding="utf-8-sig")
    colnames = [str(c).strip() if pd.notna(c) else "" for c in raw.iloc[0]]
    start = 1
    if start < len(raw):
        row1_a = str(raw.iloc[start, 0]) if pd.notna(raw.iloc[start, 0]) else ""
        if "默认分行维度" in row1_a or row1_a.startswith("如果有新的渠道"):
            start += 1
    df = raw.iloc[start:].copy()
    df.columns = colnames
    return df.dropna(how="all").reset_index(drop=True)


def _read_mapping_table(stem: str, sheet_name: str) -> pd.DataFrame:
    root = get_rules_files_dir() / "mapping"
    csv_p = root / f"{stem}.csv"
    xlsx_p = root / f"{stem}.xlsx"
    if stem == STEM_CUSTOMER_BRANCH and csv_p.exists():
        return _load_branch_mapping_csv(csv_p)
    if csv_p.exists():
        df = pd.read_csv(csv_p, encoding="utf-8-sig")
        return df.dropna(how="all").reset_index(drop=True)
    if xlsx_p.exists():
        return pd.read_excel(
            xlsx_p, sheet_name=sheet_name, header=0, engine="openpyxl"
        ).dropna(how="all")
    raise FileNotFoundError(f"缺少 mapping：{csv_p.name} 或 {xlsx_p.name}")


def _build_subject_and_channel_maps(df_map: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    subject_map: Dict[str, str] = {}
    channel_type_map: Dict[str, str] = {}
    try:
        c0, c1 = df_map.columns[0], df_map.columns[1]
        tmp = df_map[[c0, c1]].dropna(how="any")
        subject_map = dict(
            zip(tmp[c0].astype(str).str.strip(), tmp[c1].astype(str).str.strip())
        )
    except Exception:
        subject_map = {}
    try:
        c_name, c_type = df_map.columns[4], df_map.columns[5]
        for _, row in df_map.iterrows():
            n, t = row.get(c_name), row.get(c_type)
            if pd.isna(n) or pd.isna(t):
                continue
            channel_type_map[_norm_key_text(str(n).strip())] = str(t).strip()
    except Exception:
        pass
    return subject_map, channel_type_map


def _build_fee_maps(
    df_fee: pd.DataFrame,
) -> Tuple[Dict[str, str], List[Tuple[re.Pattern[str], str]], set[str]]:
    fee_map: Dict[str, str] = {}
    fee_patterns: List[Tuple[re.Pattern[str], str]] = []
    combo_col = df_fee.columns[2] if len(df_fee.columns) > 2 else None
    type_col = df_fee.columns[3] if len(df_fee.columns) > 3 else None
    channel_col = df_fee.columns[0] if len(df_fee.columns) > 0 else None
    # 与模版列「费项（Transaction Description去除前后空字符串）」一致：第 2 列（0-based index 1）
    fee_name_col = df_fee.columns[1] if len(df_fee.columns) > 1 else None

    for _, row in df_fee.iterrows():
        typ = row.get(type_col) if type_col is not None else None
        if pd.isna(typ):
            continue
        typ_s = str(typ).strip()
        if not typ_s or typ_s.lower() == "nan":
            continue

        combo_raw = row.get(combo_col) if combo_col is not None else None
        combo_raw_s = ""
        if combo_col is not None and pd.notna(combo_raw):
            s = str(combo_raw).strip()
            if s and s.lower() != "nan":
                combo_raw_s = s
        # 模版里「组合」常为公式 =channel&"-"&费项；导出 CSV / 未缓存公式时该列为空，需与Excel一致用 channel+费项
        # 合成键；与 _fee_lookup_combo 中 _norm_combo(ch_u, fee_item) 对齐。
        if not combo_raw_s and channel_col is not None and fee_name_col is not None:
            chv = row.get(channel_col)
            fev = row.get(fee_name_col)
            if pd.notna(chv) and pd.notna(fev) and str(fev).strip():
                combo_raw_s = _norm_combo(chv, fev)
        if not combo_raw_s:
            continue

        fee_map[_norm_key_text(combo_raw_s)] = typ_s
        if "?" in combo_raw_s:
            pat = (
                "^"
                + re.escape(_norm_key_text(combo_raw_s)).replace("\\?", ".")
                + "$"
            )
            try:
                fee_patterns.append((re.compile(pat), typ_s))
            except re.error:
                pass

    fee_channel_keys: set[str] = set()
    if channel_col is not None:
        for _, row in df_fee.iterrows():
            chv = row.get(channel_col)
            if pd.notna(chv):
                fee_channel_keys.add(_norm_key_text(str(chv).strip()))
    return fee_map, fee_patterns, fee_channel_keys


def _build_branch_map(df_branch: pd.DataFrame) -> Dict[str, str]:
    branch_map: Dict[str, str] = {}
    cols = list(df_branch.columns)
    for _, row in df_branch.iterrows():
        try:
            channel_val = row.get(cols[1])
            account_val = row.get(cols[2])
            branch_val = row.get(cols[4])
            if pd.notna(channel_val) and pd.notna(account_val) and pd.notna(branch_val):
                key = f"{str(channel_val).strip().upper()}_{str(account_val).strip()}"
                branch_map[key] = str(branch_val).strip()
        except Exception:
            pass
        try:
            if len(cols) > 10:
                channel_val = row.get(cols[8])
                account_val = row.get(cols[9])
                branch_val = row.get(cols[10])
                if pd.notna(channel_val) and pd.notna(account_val) and pd.notna(branch_val):
                    key = f"{str(channel_val).strip().upper()}_{str(account_val).strip()}"
                    branch_map[key] = str(branch_val).strip()
        except Exception:
            pass
    return branch_map


def _load_fx_rates_monthly(
    default_yyyymm: str,
) -> Dict[str, float]:
    fx_path = get_rules_files_dir() / "fx" / FX_FILENAME
    if not fx_path.exists():
        raise FileNotFoundError(f"缺少汇率文件：{FX_FILENAME}")
    df_fx = pd.read_csv(fx_path, encoding="utf-8-sig").dropna(how="all")

    rate_col = None
    for name in ("兑USD汇率", "对美元折算率"):
        if name in df_fx.columns:
            rate_col = name
            break
    if rate_col is None:
        c = _guess_fx_rate_column(df_fx.columns)
        if c:
            rate_col = c
    if rate_col is None:
        raise ValueError("汇率表缺少「兑USD汇率」或「对美元折算率」列")

    code_col = "货币代码" if "货币代码" in df_fx.columns else None
    name_col = "货币名称" if "货币名称" in df_fx.columns else None

    out: Dict[str, float] = {}

    if "日期" in df_fx.columns:
        for _, row in df_fx.iterrows():
            p = _normalize_fx_sheet_month(row.get("日期"))
            if p is None:
                continue
            for lab_col in (code_col, name_col):
                if not lab_col:
                    continue
                raw_name = row.get(lab_col)
                if raw_name is None or (isinstance(raw_name, float) and np.isnan(raw_name)):
                    continue
                curr = str(raw_name).strip().upper()
                if not curr:
                    continue
                rv = normalize_fx_rate_value(row.get(rate_col))
                if rv is None or rv == 0:
                    continue
                out[f"{p}_{curr}"] = float(rv)
        return out

    # 无「日期」：整表视为 default_yyyymm 月度汇率（前端单独上传的折算率 CSV）
    for _, row in df_fx.iterrows():
        rv = normalize_fx_rate_value(row.get(rate_col))
        if rv is None or rv == 0:
            continue
        if code_col:
            c = row.get(code_col)
            if pd.notna(c) and str(c).strip():
                cu = str(c).strip().upper()
                out[f"{default_yyyymm}_{cu}"] = float(rv)
        if name_col:
            n = row.get(name_col)
            if pd.notna(n) and str(n).strip():
                nu = str(n).strip().upper()
                out[f"{default_yyyymm}_{nu}"] = float(rv)
    return out


def _guess_fx_rate_column(columns: Any) -> Optional[str]:
    cols = [str(c).strip() for c in list(columns)]
    for c in cols:
        if "美元" in c or "USD" in c.upper() or "折算" in c:
            return c
    return cols[-1] if cols else None


def _lookup_fx_usd(
    fx_rates: Dict[str, float],
    yyyymm: str,
    curr: str,
    preferred_ym: Optional[str] = None,
) -> tuple[float, bool]:
    """返回 (对美元汇率乘数, 是否在表中匹配到该币种)。

    未匹配时返回 (0.0, False)（与历史行为一致：USD金额按 0 计）。
    fallback 优先顺序：preferred_ym（fx_month_label）→ 字典序最新月份。
    """
    curr_u = str(curr or "").strip().upper()
    if curr_u == "USD" or curr_u == "":
        return 1.0, True
    rate_key = f"{yyyymm}_{curr_u}"
    rate_used = fx_rates.get(rate_key)
    if rate_used is not None:
        return float(rate_used or 0.0), True
    # fallback: 先尝试 preferred_ym（来自 fx_month_label），再取字典序最大
    if preferred_ym:
        preferred_key = f"{preferred_ym}_{curr_u}"
        preferred_val = fx_rates.get(preferred_key)
        if preferred_val is not None:
            return float(preferred_val or 0.0), True
    fallback_keys = [k for k in fx_rates.keys() if k.endswith(f"_{curr_u}")]
    if fallback_keys:
        fallback_keys.sort()
        return float(fx_rates[fallback_keys[-1]] or 0.0), True
    return 0.0, False


def _row_dict_from_headers(headers: List[str], row: Tuple[Any, ...]) -> Dict[str, Any]:
    r = dict(zip(headers, row))
    for k0, v0 in list(r.items()):
        if isinstance(k0, str) and " " in k0:
            k1 = k0.replace(" ", "")
            if k1 not in r:
                r[k1] = v0
    return r


def build_enriched_rows_for_workbook(
    path: Path,
    *,
    subject_map: Dict[str, str],
    channel_type_map: Dict[str, str],
    fee_map: Dict[str, str],
    fee_patterns: List[Tuple[re.Pattern[str], str]],
    fee_channel_keys: set[str],
    branch_map: Dict[str, str],
    fx_rates_monthly: Dict[str, float],
    default_yyyymm: str,
    progress_log: Optional[Callable[[str], None]] = None,
    progress_every: int = 25_000,
) -> Tuple[List[Dict[str, Any]], set[tuple[str, str]]]:
    import openpyxl

    misses: set[tuple[str, str]] = set()

    with pd.ExcelFile(path, engine="openpyxl") as xls:
        sheet = _pick_pingpong_customer_sheet(xls)
    if sheet is None:
        return [], misses

    if progress_log:
        progress_log(
            f"  规则明细：打开「渠道对账单」sheet「{sheet}」({path.name})，开始逐行映射…"
        )

    wb_raw = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws_raw = wb_raw[sheet]
        headers: List[str] = []
        out_rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(ws_raw.iter_rows(values_only=True)):
            if idx == 0:
                headers = [str(c).strip() if c else "" for c in row]
                continue
            if all(c is None for c in row):
                continue
            r = _row_dict_from_headers(headers, row)

            acc_sub = str(r.get("账户主体", "") or "").strip()
            if acc_sub == "None":
                acc_sub = ""
            std_sub = subject_map.get(acc_sub, acc_sub)

            channel = str(r.get("Channel", "") or "").strip()
            if channel == "None":
                channel = ""
            channel_raw = channel.upper().strip()
            channel = channel_raw

            region = str(r.get("地区", "") or "").strip()
            if region == "None":
                region = ""
            region = region.upper().strip()

            branch_dim = get_branch_dimension(
                channel, std_sub, acc_sub, region, subject_map, branch_map
            )

            desc = str(r.get("Transaction Description", "") or "").strip()
            if desc == "None":
                desc = ""

            fundtype = str(r.get("FundType", "") or "").strip()
            if fundtype.lower() == "none":
                fundtype = ""
            bu_now = str(r.get("账户BU", "") or "").strip()
            if bu_now == "None":
                bu_now = ""
            remark = str(r.get("Remark-description", "") or "").strip()
            if remark == "None":
                remark = ""

            payee = str(r.get("Payee Name", "") or "").strip()
            if payee == "None":
                payee = ""
            drawee = str(r.get("Drawee Name", "") or "").strip()
            if drawee == "None":
                drawee = ""

            if channel_raw in ("CITI", "JPM"):
                fee_item_out = desc.strip()
            else:
                fee_item_out = ""

            subject_code = assign_entry_subject(
                r,
                channel_raw,
                acc_sub,
                std_sub,
                branch_dim,
                fundtype,
                bu_now,
                desc,
                remark,
            )
            subject_code = OUT_MAP.get(str(subject_code).strip(), subject_code)
            try:
                subject_code = str(subject_code).replace("\u00a0", " ").strip()
            except Exception:
                pass

            fee_type = resolve_fee_type(
                subject_code,
                channel_raw,
                fee_item_out,
                r,
                channel_type_map,
                fee_map,
                fee_patterns,
                fee_channel_keys,
            )

            if (
                (std_sub.strip() == "PPHK" or acc_sub.strip() == "PPHK")
                and branch_dim.strip() == "CITIUS"
                and "云游send-ICA扣费" in str(r.get("Remark-description", "") or "")
            ):
                fee_type = "others"

            vdate = r.get("ValueDate", "")
            if vdate is None or str(vdate).strip() == "None":
                vdate = ""
            bill_date = r.get("BillDate", "")
            if bill_date is None or str(bill_date).strip() == "None":
                bill_date = ""
            period, yyyymm = _period_yyyymm_for_fx(bill_date, vdate)
            yyyymm_s = str(yyyymm).strip()
            if len(yyyymm_s) < 6 or not yyyymm_s.isdigit():
                yyyymm_s = str(default_yyyymm).strip()

            curr = str(r.get("Currency", "") or "").strip()
            if curr == "None":
                curr = ""
            c_amt = (
                float(r.get("Credit Amount"))
                if r.get("Credit Amount") not in [None, "", "None"]
                else 0.0
            )
            d_amt = (
                float(r.get("Debit Amount"))
                if r.get("Debit Amount") not in [None, "", "None"]
                else 0.0
            )

            rate_used, fx_hit = _lookup_fx_usd(
                fx_rates_monthly, yyyymm_s, curr, preferred_ym=default_yyyymm or None
            )
            if curr and curr.upper() not in ("USD", "") and not fx_hit:
                misses.add((curr.upper(), yyyymm_s))
            usd_amt = (d_amt - c_amt) * float(rate_used)

            extra_info = str(r.get("Extra Information", "") or "")
            if extra_info == "None":
                extra_info = ""
            pay_detail = str(r.get("Payment Detail", "") or "")
            if pay_detail == "None":
                pay_detail = ""
            merch_id = str(r.get("MerchantId", "") or "")
            if merch_id == "None":
                merch_id = ""

            out_rows.append(
                {
                    "账户主体": acc_sub,
                    "账户BU": bu_now,
                    "ValueDate": vdate,
                    "Channel": channel,
                    "地区": region,
                    "MerchantId": merch_id,
                    "Currency": curr,
                    "Credit Amount": c_amt,
                    "Debit Amount": d_amt,
                    "Transaction Description": desc,
                    "Extra Information": extra_info,
                    "Payment Detail": pay_detail,
                    "Payee Name": payee,
                    "Drawee Name": drawee,
                    "FundType": fundtype,
                    "Remark-description": remark,
                    "USD金额": usd_amt,
                    "入账期间": period,
                    "主体": std_sub,
                    "分行维度": branch_dim,
                    "费项": fee_item_out,
                    "类型": fee_type,
                    "备注": "",
                    "入账科目": subject_code,
                }
            )
            nrow = len(out_rows)
            if (
                progress_log
                and progress_every > 0
                and nrow % progress_every == 0
            ):
                progress_log(
                    f"  规则明细映射：{path.name} · 已累计 {nrow:,} 行（大单请耐心等待）…"
                )
        return out_rows, misses
    finally:
        wb_raw.close()


@dataclass
class CustomerEnrichRules:
    subject_map: Dict[str, str]
    channel_type_map: Dict[str, str]
    fee_map: Dict[str, str]
    fee_patterns: List[Tuple[re.Pattern[str], str]]
    fee_channel_keys: set[str]
    branch_map: Dict[str, str]
    fx_rates_monthly: Dict[str, float]
    default_yyyymm: str


def load_customer_enrich_rules(source_paths: List[Path]) -> CustomerEnrichRules:
    df_map = _read_mapping_table(STEM_CUSTOMER_MAPPING, CUSTOMER_MAPPING_SHEET)
    df_fee = _read_mapping_table(STEM_CUSTOMER_FEE, CUSTOMER_FEE_MAPPING_SHEET)
    df_branch = _read_mapping_table(STEM_CUSTOMER_BRANCH, CUSTOMER_BRANCH_MAPPING_SHEET)

    subject_map, channel_type_map = _build_subject_and_channel_maps(df_map)
    fee_map, fee_patterns, fee_channel_keys = _build_fee_maps(df_fee)
    branch_map = _build_branch_map(df_branch)

    default_yyyymm = _fx_meta_default_yyyymm() or ""
    if not default_yyyymm and source_paths:
        for p in source_paths:
            try:
                with pd.ExcelFile(p, engine="openpyxl") as xls:
                    sheet = _pick_pingpong_customer_sheet(xls)
                if sheet:
                    default_yyyymm = _infer_yyyymm_from_workbook(p, sheet) or ""
                    if default_yyyymm:
                        break
            except Exception:
                continue
    if not default_yyyymm:
        default_yyyymm = pd.Timestamp.utcnow().strftime("%Y%m")

    fx_rates_monthly = _load_fx_rates_monthly(default_yyyymm)
    return CustomerEnrichRules(
        subject_map=subject_map,
        channel_type_map=channel_type_map,
        fee_map=fee_map,
        fee_patterns=fee_patterns,
        fee_channel_keys=fee_channel_keys,
        branch_map=branch_map,
        fx_rates_monthly=fx_rates_monthly,
        default_yyyymm=default_yyyymm,
    )


def try_write_customer_flow_output(
    source_paths: List[Path],
    out_xlsx: Path,
    *,
    progress_log: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[str], int, List[str]]:
    """写入 ``customer_flow_output.xlsx``（与 master 列集合对齐）。

    返回 (警告或 None, 行数, 未匹配汇率说明列表)。
    """
    paths = [p for p in source_paths if p.exists() and p.is_file()]
    if not paths:
        return "未找到客资源文件，跳过客资规则明细导出。", 0, []
    if progress_log:
        progress_log("客资规则明细：读取 rules/files 下三张客资 mapping 与 FX 折算率表…")

    try:
        rules = load_customer_enrich_rules(paths)
    except Exception as exc:
        return f"客资规则明细跳过（mapping/汇率加载失败）：{exc}", 0, []

    if progress_log:
        progress_log(
            f"客资规则明细：mapping/fx 已就绪，默认账期 {rules.default_yyyymm}，"
            "开始按源表逐行 enrichment（十几万行时每 25000 行打印进度）…"
        )

    all_rows: List[Dict[str, Any]] = []
    all_misses: set[tuple[str, str]] = set()
    total_p = len(paths)
    for i, p in enumerate(paths, start=1):
        if progress_log:
            progress_log(
                f"客资规则明细：[{i}/{total_p}] enrich {p.name} …"
            )
        rows, misses = build_enriched_rows_for_workbook(
            p,
            subject_map=rules.subject_map,
            channel_type_map=rules.channel_type_map,
            fee_map=rules.fee_map,
            fee_patterns=rules.fee_patterns,
            fee_channel_keys=rules.fee_channel_keys,
            branch_map=rules.branch_map,
            fx_rates_monthly=rules.fx_rates_monthly,
            default_yyyymm=rules.default_yyyymm,
            progress_log=progress_log,
        )
        all_rows.extend(rows)
        all_misses |= misses

    if not all_rows:
        return "未解析到「渠道对账单」数据行，跳过客资规则明细导出。", 0, []

    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    if progress_log:
        progress_log(
            f"客资规则明细：行映射完成 · 共 {len(all_rows):,} 行，正在写入 {out_xlsx.name} …"
        )
    pd.DataFrame(all_rows, columns=OUT_COLUMNS).to_excel(
        out_xlsx, index=False, engine="openpyxl"
    )
    fx_msgs: List[str] = []
    if all_misses:
        parts = sorted(f"{c}（账期 {y}）" for c, y in sorted(all_misses))
        fx_msgs.append(
            "未匹配到 rules/files/fx 中的币种汇率，USD金额已按 0 计：" + "；".join(parts)
        )
    return None, len(all_rows), fx_msgs
