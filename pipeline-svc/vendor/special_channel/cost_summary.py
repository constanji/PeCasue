"""成本汇总：合并所有大渠道的最终产物。

Ported from pingpong-master/script/allocation/cost_summary.py.
Loads bill / own / customer / special (内转+ACH+OP退票+OP入账) / cn_jp
from their latest verified xlsx outputs and produces:
  1. 明细表 (所有渠道纵向 concat)
  2. 模板口径汇总表 (按 RECONCILIATION_TEMPLATE_ROWS 逐行汇总；
     CITIHK+PPHK：首行为空账号残差 + 模板内硬编码大账号行（与 pingpong-master 一致）；
     CITISG/CITI-SG+PPHK：按明细出现的大账号扩行，首行空账号)
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_PERIOD = "202602"

COL_CH_TOP = "渠道"
COL_CH_BRANCH = "渠道名称（分行维度）"
COST_BUCKETS = ["inbound", "outbound", "others", "VA", "收款通道成本"]

# Template reconciliation rows — (渠道, 渠道名称/分行维度, 主体, 账号)
RECONCILIATION_TEMPLATE_ROWS = [
    ("CITI", "CITIHK", "PPHK", None),
    ("CITI", "CITIAU", "PPHK", None),
    ("CITI", "CITINZ", "PPHK", None),
    ("CITI", "CITITH", "PPHK", None),
    ("CITI", "CITIID", "PPHK", None),
    ("CITI", "CITISG", "PPHK", None),
    ("CITI", "CITIAE", "PPHK", None),
    ("CITI", "CITIGB", "PPHK", None),
    ("CITI", "CITIEU", "PPHK", None),
    ("CITI", "CITIUS", "PPHK", None),
    ("CITI", "CITIPH", "PPHK", None),
    ("CITI", "CITIUS", "PPUS", None),
    ("CITI", "CITIMX", "PPUS", None),
    ("CITI", "CITICA", "PPUS", None),
    ("CITI", "CITIEU", "PPEU", None),
    ("CITI", "CITIPL", "PPEU", None),
    ("CITI", "CITIHK", "PPGT", None),
    ("CITI", "CITIAU", "PPGT", None),
    ("CITI", "CITIID", "PPGT", None),
    ("CITI", "CITINZ", "PPGT", None),
    ("CITI", "CITIAU", "MANA AU", None),
    ("CITI", "CITIHK", "PPI", None),
    ("CITI", "CITIJP", "PPJP", None),
    ("CITI", "CITISG", "BRSG", None),
    ("JPM", "JPMHK", "PPHK", None),
    ("JPM", "JPMUS", "PPUS", None),
    ("JPM", "JPMEU", "PPEU", None),
    ("JPM", "JPMHK", "PPGT", None),
    ("JPM", "JPMHK", "MANA AU", None),
    ("JPM", "JPMHK", "PPI", None),
    ("JPM", "JPMSG", "BRSG", None),
    ("DB", "DBHK", "PPHK", None),
    ("DB", "DBTH", "PPHK", None),
    ("DB", "DBKR", "PPHK", None),
    ("DB", "DBHK", "PPGT", None),
    ("DB", "DBUS", "PPUS", None),
    ("DB", "DBNL", "PPEU", None),
    ("DB", "DBEU", "PPEU", None),
    ("XENDIT", "Xendit-ID", "MANA-ID", None),
    ("XENDIT", "Xendit-TH", "PPHK", None),
    ("XENDIT", "Xendit-MY", "PPHK", None),
    ("XENDIT", "Xendit-VN", "PPHK", None),
    ("SCB", "SCBHK", "PPHK", None),
    ("SCB", "SCBAE", "PPHK", None),
    ("SCB", "SCBAE", "PPGT", None),
    ("SCB", "SCBTH", "PPGT", None),
    ("SCB", "SCBSG", "BRSG", None),
    ("SCB", "SCBHK", "PPGT", None),
    ("EWB", "EWB-US", "PPUS", None),
    ("MONOOVA", "MONOOVA", "MANA AU", None),
    ("Beepay", "Beepay", "PPHK", None),
    ("E-Commerce", "E-Commerce", "PPHK", None),
    ("KCB", "KCB", "PPHK", None),
    ("Helipay", "Helipay", "PPHK", None),
    ("Newpay", "Newpay", "PPHK", None),
    ("Shcepp", "Shcepp", "PPHK", None),
    ("Yeepay", "Yeepay", "PPHK", None),
    ("Sumpay", "Sumpay", "PPHK", None),
    ("YIWUPAY", "YIWUPAY", "PPHK", None),
    ("MUFG", "MUFG", "PPJP", None),
    ("Rakuten", "Rakuten", "PPJP", None),
    ("JNRCB", "JNRCB", "PPHK", None),
    ("Ninepay", "Ninepay", "PPHK", None),
    ("SZBANK", "SZBANK", "PPHK", None),
    ("BOC", "BOC", "PPHK", None),
    ("BOC", "BOC", "PPGT", None),
    ("BOC", "BOCSG", "BRSG", None),
    ("BOC", "BOCUS", "PPUS", None),
    ("Barclays", "Barclays", "PPEU", None),
    ("Barclays", "Barclays", "PPHK", None),
    ("BC", "BC-EU", "PPEU", None),
    ("BGL", "BGL-EU", "PPEU", None),
    ("BOSH", "BOSH", "PPHK", None),
    ("Queen Bee", "Queen Bee", "PPUS", None),
    ("BBVA", "BBVA", "PPHK", None),
    ("BCM", "BCM", "PPHK", None),
    ("CMB", "CMB", "PPHK", None),
    ("CMFB", "CMFB", "PPHK", None),
    ("CNCB", "CNCB", "PPHK", None),
    ("DOKU", "DOKU", "PPHK", None),
    ("Flutterwave", "Flutterwave", "PPHK", None),
    ("DBS", "DBSHK", "PPHK", None),
    ("DBS", "DBSSG", "BRSG", None),
    ("DBS", "DBSHK", "PPI", None),
    ("HXBANK", "HXBANK", "PPHK", None),
    ("KORAPAY", "KORAPAY", "PPHK", None),
    ("MYBANK", "MYBANK", "PPHK", None),
    ("SHRCB", "SHRCB", "PPHK", None),
    ("SPDB", "SPDB", "PPHK", None),
    ("Payso", "Payso", "PPHK", None),
    ("Tranglo", "Tranglo", "PPHK", None),
    ("VERTO", "VERTO", "PPHK", None),
    ("CIMB", "CIMB", "PPMY", None),
    ("VietinBank", "VietinBank", "PPGT", None),
    ("KBANK", "KBANK", "PPGT", None),
    ("GMO", "GMO", "PPJP", None),
    ("BAOKIM", "BAOKIM", "PPGT", None),
    ("ABC", "ABC", "PPHK", None),
    ("AFFIN", "AFFIN", "PPMY", None),
    ("BCA", "BCA", "PT First Money", None),
    ("BNI", "BNI", "PT First Money", None),
    ("CEB", "CEB", "PPHK", None),
    ("CHB", "CHB", "PPHK", None),
    ("CIB", "CIB", "PPHK", None),
    ("DANDELION", "DANDELION", "PPHK", None),
    ("ESUNBANK", "ESUNBANK", "PPHK", None),
    ("STP", "STP", "PPHK", None),
    ("TERRAPAY", "TERRAPAY", "PPHK", None),
    ("FINCRA", "FINCRA", "PPHK", None),
    ("HFBANK", "HFBANK", "PPHK", None),
    ("HZBANK", "HZBANK", "PPHK", None),
    ("IBC", "IBC", "PPUS", None),
    ("LHPP", "LHPP", "PPGT", None),
    ("Maybank", "Maybank", "PPMY", None),
    ("Multigate", "Multigate", "PPHK", None),
    ("NBCB", "NBCB", "PPHK", None),
    ("NETBANK", "NETBANK", "PPHK", None),
    ("PAB", "PAB", "PPHK", None),
    ("Payermax", "Payermax", "PPHK", None),
    ("PPCBank", "PPCBank", "PPGT", None),
    ("MacaoBank", "MacaoBank", "PPHK", None),
    ("METACOMP", "METACOMP", "NM", None),
    ("Czbank", "Czbank", "PPHK", None),
    ("Lithic", "Lithic", "PPUS", None),
    ("form3", "form3", "PPEU", None),
    ("clearbank", "clearbank", "PPUK", None),
    ("Transfermate", "Transfermate", "PPHK", None),
    ("苏宁银行", "苏宁银行", "杭州乒乓", None),
    ("FACILITAPAY", "FACILITAPAY", "PPHK", None),
    ("ICB", "ICB", "PPHK", None),
    ("KORAPAY", "KORAPAY", "PPGT", None),
    ("chinapay", "chinapay", "PPHK", None),
    ("ICBC", "ICBC", "PPHK", None),
    ("Orient", "Orient", "PPHK", None),
    ("XMB", "XMB", "PPHK", None),
    ("Geoswift", "Geoswift", "PPHK", None),
    ("Rakuten", "Rakuten", "ASIA PAY", None),
    ("CIMB", "CIMB", "BRSG", None),
    ("SPDB", "SPDB", "Mana AU", None),
    ("ABC", "ABC", "Mana AU", None),
    # CITIHK+PPHK 分账号：与 pingpong-master 一致为模板硬编码大账号；首行空账号残差在列表最前 (CITIHK,PPHK,None)。
    ("CITI", "CITIHK", "PPHK", 1044102168),
    ("CITI", "CITIHK", "PPHK", 1065249002),
    ("CITI", "CITIHK", "PPHK", 1065249029),
    ("CITI", "CITIHK", "PPHK", 1065249037),
    ("CITI", "CITIHK", "PPHK", 1065249045),
    ("CITI", "CITIHK", "PPHK", 1065249053),
    ("CITI", "CITIHK", "PPHK", 1065249061),
    ("CITI", "CITIHK", "PPHK", 1065249096),
    ("CITI", "CITIHK", "PPHK", 1065249126),
    ("CITI", "CITIHK", "PPHK", 1065249835),
    ("BOB", "BOB", "PPHK", None),
    ("BOSH", "BOSH", "PPUS", None),
    ("HZBANK", "HZBANK", "MANA AU", None),
    ("LusoBank", "LusoBank", "PPHK", None),
    ("DB", "DBHK", "PPI", None),
    ("CHINAUMS", "CHINAUMS", "PPHK", None),
    ("HASHKEY", "HASHKEY", "PPI", None),
    ("BCA", "BCA", "MANA-ID", None),
]

# 收款通道统一清单 — 不区分 inbound/outbound/others/VA
_FORCE_RCV_BRANCH_RAW: tuple[str, ...] = (
    "BAOKIM", "FACILITAPAY", "FACILITAP", "Hanpass", "HECTO", "ICB",
    "KORAPAY", "Payermax", "Payso", "QBC", "TRANSFERMATE", "TRANSFERM",
    "Unionpay", "VERTO", "WXRCB", "XENDIT", "Dandelion", "tranglo",
    "Ninepay", "TERRAPAY", "Mastercard", "METACOMP", "PINGPONG_SETTLE",
    "PINGPONG_", "STP", "Multigate", "FINCRA", "LHPP", "Flutterwave",
    "Flutterwa", "HASHKEY", "PAB", "XMB", "DOKU", "Xendit-ID", "Xendit-TH",
    "Xendit-MY", "Xendit-VN", "Beepay", "E-Commerce", "Helipay", "Newpay",
    "Shcepp", "Yeepay", "Sumpay", "YIWUPAY", "chinapay", "ICBC", "Orient",
    "Geoswift", "Queen Bee", "CHINAUMS",
)

def _norm_force_key(k: str) -> str:
    return k.strip().casefold().replace("-", "").replace(" ", "").replace("_", "")


FORCE_RECEIVABLE_CHANNEL_BRANCH_KEYS: frozenset[str] = frozenset(
    _norm_force_key(k) for k in _FORCE_RCV_BRANCH_RAW if k.strip()
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_usd_col(columns: list[str]) -> str | None:
    for c in columns:
        if str(c).startswith("USD"):
            return c
    return None


def _pick_col(df: pd.DataFrame, *names: str, label: str) -> str:
    for name in names:
        if name in df.columns:
            return name
    raise RuntimeError(f"{label} 缺少列 {names}，实际列: {list(df.columns)}")


def _pick_optional_col(df: pd.DataFrame, *names: str) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def _resolve_own_usd_column(df: pd.DataFrame) -> str:
    for name in ("USD金额", "USD"):
        if name in df.columns:
            return name
    c = _find_usd_col(list(df.columns))
    if c:
        return c
    raise RuntimeError(f"缺少 USD/USD金额 列，实际列: {list(df.columns)}")


def normalize_period(val, default: str = DEFAULT_PERIOD) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return default
    if isinstance(val, (int, np.integer)):
        return str(int(val))
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y%m")
    s = str(val).strip()
    if re.fullmatch(r"\d{6}", s):
        return s
    ts = pd.to_datetime(val, errors="coerce")
    if pd.notna(ts):
        return ts.strftime("%Y%m")
    return default


def to_usd_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def is_ledger_subject_strict_cost(val) -> bool:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return False
    return str(val).strip() == "成本"


def _norm_match_key(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    # 去除连字符和空格再比较，避免 "EWB-US" vs "EWBUS" 之类的命名差异
    return str(val).strip().casefold().replace("-", "").replace(" ", "").replace("_", "")


def _norm_account_key(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    s = s.replace(",", "")
    num = pd.to_numeric(s, errors="coerce")
    if pd.notna(num):
        f = float(num)
        if abs(f - round(f)) < 1e-9:
            return str(int(round(f)))
        return str(f)
    return s


def _br_m_is_citisg_variant(br_m: str) -> bool:
    """分行规范化键为 CITISG / CITI-SG 等时均为 True（去分隔与空白后为 citisg）。"""
    s = (br_m or "").strip().casefold()
    if not s:
        return False
    return re.sub(r"[-_\s]+", "", s) == "citisg"


def _expand_citisg_pphk_skeleton(
    skeleton: pd.DataFrame, d: pd.DataFrame, col_ch: str, col_br: str
) -> pd.DataFrame:
    """
    将 CITI-SG/CITISG + PPHK 的模板行替换为与 pingpong-master 一致：
    首行空账号（残差），其余为明细 (PPHK+对应分行) 下出现的大账号；无则仅空账号一行。
    下游 cost_allocate 才有 per-account 行可走 SR_CITI_SG_OUT_ACCT，否则 outbound 会整笔走 SR_CITI_SG_OUT_ZT。
    """
    pphk = _norm_match_key("PPHK")
    m_br = skeleton[col_br].map(lambda x: _br_m_is_citisg_variant(_norm_match_key(x)))
    m_en = skeleton["主体"].map(_norm_match_key) == pphk
    m = m_br & m_en
    if not m.any():
        return skeleton

    first = skeleton.loc[m].iloc[0]
    ch0, br0, ent0 = first[col_ch], first[col_br], first["主体"]

    d_sub = d[(d["_en_m"] == pphk) & (d["_br_m"].map(_br_m_is_citisg_variant))]
    accs = d_sub.loc[d_sub["_ac"].notna(), "_ac"].drop_duplicates()
    acc_list = sorted(accs.dropna().unique(), key=str)

    new_rows: list[dict] = [{col_ch: ch0, col_br: br0, "主体": ent0, "账号": np.nan}]
    for a in acc_list:
        new_rows.append({col_ch: ch0, col_br: br0, "主体": ent0, "账号": a})

    sk = skeleton.reset_index(drop=True)
    m = sk[col_br].map(lambda x: _br_m_is_citisg_variant(_norm_match_key(x))) & (
        sk["主体"].map(_norm_match_key) == pphk
    )
    out_list: list[dict] = []
    inserted = False
    for i in range(len(sk)):
        if bool(m.iloc[i]):
            if not inserted:
                for nr in new_rows:
                    out_list.append(nr)
                inserted = True
            continue
        row = sk.iloc[i]
        out_list.append(
            {col_ch: row[col_ch], col_br: row[col_br], "主体": row["主体"], "账号": row["账号"]}
        )
    return pd.DataFrame(out_list, columns=[col_ch, col_br, "主体", "账号"])


def cost_type_bucket(raw: str) -> str:
    t = str(raw).strip()
    if not t or t.lower() == "nan":
        return "others"
    if t == "收款通道成本":
        return "收款通道成本"
    if t == "VA":
        return "VA"
    tl = t.lower()
    if tl == "inbound":
        return "inbound"
    if tl == "outbound":
        return "outbound"
    return "others"


def _account_from_row(r: pd.Series) -> str:
    for k in ("MerchantId", "Account Reference", "关联大账号"):
        if k in r.index and pd.notna(r[k]) and str(r[k]).strip():
            return str(r[k]).strip()
    return ""


# ---------------------------------------------------------------------------
# Channel loaders — each reads from a specific xlsx path
# ---------------------------------------------------------------------------

def load_bill(path: Path) -> pd.DataFrame:
    """Load bill channel output. Sheet: '账单'."""
    df = pd.read_excel(path, sheet_name="账单")
    df = df[df["入账科目"].map(is_ledger_subject_strict_cost)].copy()
    out = pd.DataFrame({
        "主体": df["主体"],
        "分行维度": df["分行维度"],
        "账号": df["Account"].astype(str),
        "入账期间": df["入账期间"].map(lambda x: normalize_period(x, DEFAULT_PERIOD)),
        "USD金额": to_usd_numeric(df["USD金额"]),
        "类型": df["类型"],
        "入账科目": df["入账科目"],
    })
    out.insert(0, "数据来源", "账单")
    return out


def load_own(path: Path) -> pd.DataFrame:
    """Load own-flow channel output. Sheet: 'Sheet1'."""
    df = pd.read_excel(path, sheet_name="Sheet1")
    df = df[df["入账科目"].map(is_ledger_subject_strict_cost)].copy()
    usd_col = _resolve_own_usd_column(df)
    out = pd.DataFrame({
        "主体": df["主体"],
        "分行维度": df["分行维度"],
        "账号": df["Account Number"].astype(str),
        "入账期间": DEFAULT_PERIOD,
        "USD金额": to_usd_numeric(df[usd_col]),
        "类型": df["类型"],
        "入账科目": df["入账科目"],
    })
    out.insert(0, "数据来源", "自有流水")
    return out


def load_cust(path: Path) -> pd.DataFrame:
    """Load customer channel output. Sheet: 'Sheet1' or first sheet."""
    xl = pd.ExcelFile(path)
    sheet = "Sheet1" if "Sheet1" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    mask = df["入账科目"].map(is_ledger_subject_strict_cost)
    df = df[mask].copy()
    ent_col = "主体" if "主体" in df.columns else "账户主体"
    if "MerchantId" in df.columns:
        mid = df["MerchantId"]
        id_ok = mid.notna() & ~(mid.astype(str).str.strip().isin(("", "nan", "None", "<NA>")))
        acct = mid.astype(str).str.strip().where(id_ok, np.nan)
        if "账户主体" in df.columns:
            acct = acct.fillna(df["账户主体"].astype(str).str.strip())
        else:
            acct = acct.fillna("")
        acct = acct.astype(str)
    else:
        acct = (
            df["账户主体"].astype(str) if "账户主体" in df.columns
            else df[ent_col].astype(str)
        )
    out = pd.DataFrame({
        "主体": df[ent_col],
        "分行维度": df["分行维度"],
        "账号": acct,
        "入账期间": df["入账期间"].map(lambda x: normalize_period(x, DEFAULT_PERIOD)),
        "USD金额": to_usd_numeric(df["USD金额"]),
        "类型": df["类型"],
        "入账科目": df["入账科目"],
    })
    out.insert(0, "数据来源", "客资流水")
    return out


def load_neizhuan(path: Path, fx_map: dict[str, float]) -> pd.DataFrame:
    """Load 内转 sheet from special_transfer xlsx."""
    df = pd.read_excel(path, sheet_name="内转")
    usd_col = _find_usd_col(list(df.columns))
    if not usd_col:
        raise RuntimeError("内转 sheet 未找到 USD 列")
    period_col = _pick_col(df, "入账期间", "统计期间", label="内转")
    type_col = _pick_col(df, "类型", "方向", label="内转")
    entity_col = _pick_col(df, "账户主体", "主体", label="内转")
    raw = to_usd_numeric(df[usd_col])
    curr = df["Currency"].astype(str).str.strip().str.upper().replace({"NAN": "", "NONE": ""}) if "Currency" in df.columns else pd.Series("", index=df.index)
    ef = to_usd_numeric(df["Extra Fee"]) if "Extra Fee" in df.columns else pd.Series(0.0, index=df.index)
    abs_r = raw.abs().to_numpy()
    abs_e = ef.abs().to_numpy()
    mx = np.maximum(abs_r, abs_e)
    mx = np.where(mx > 1e-12, mx, 1.0)
    rel = np.minimum(abs_r, abs_e) / mx
    abs_e_pos = abs_e > 1e-6
    same_local = abs_e_pos & (
        np.isclose(abs_r, abs_e, rtol=0.02, atol=1.0)
        | ((np.maximum(abs_r, abs_e) > 1e5) & (rel > 0.95))
    )
    is_usd = curr.eq("") | curr.eq("USD")
    same_local_sr = pd.Series(same_local, index=df.index)
    rate = curr.map(fx_map)
    usd_amt = raw.copy().astype(float)
    mask_conv = (~is_usd) & same_local_sr
    usd_amt.loc[mask_conv] = (raw.loc[mask_conv] * rate.loc[mask_conv]).astype(float)
    out = pd.DataFrame({
        "主体": df[entity_col],
        "分行维度": df["分行维度"],
        "账号": df.apply(_account_from_row, axis=1),
        "入账期间": df[period_col].map(lambda x: normalize_period(x, DEFAULT_PERIOD)),
        "USD金额": usd_amt,
        "类型": df[type_col],
        "入账科目": "成本",
    })
    out.insert(0, "数据来源", "内转")
    return out


def load_ach(path: Path) -> pd.DataFrame:
    """Load ACH return sheet from special_ach_refund xlsx."""
    df = pd.read_excel(path, sheet_name="ACH return")
    if "USD" not in df.columns:
        raise RuntimeError("ACH return 缺少 USD 列")
    period_col = _pick_col(df, "入账期间", "统计期间", label="ACH return")
    type_col = _pick_col(df, "类型", "方向", label="ACH return")
    entity_col = _pick_col(df, "账户主体", "主体", label="ACH return")
    out = pd.DataFrame({
        "主体": df[entity_col],
        "分行维度": df["分行维度"],
        "账号": df.apply(_account_from_row, axis=1),
        "入账期间": df[period_col].map(lambda x: normalize_period(x, DEFAULT_PERIOD)),
        "USD金额": to_usd_numeric(df["USD"]),
        "类型": df[type_col],
        "入账科目": "成本",
    })
    out.insert(0, "数据来源", "ACH return")
    return out


def load_op_sheet(path: Path, sheet: str, label: str) -> pd.DataFrame:
    """Load OP 退票/入账 sheet from special_op_refund/incoming xlsx."""
    df = pd.read_excel(path, sheet_name=sheet)
    if "主体" not in df.columns and "公司主体" not in df.columns:
        df = pd.read_excel(path, sheet_name=sheet, header=1)
    entity_col = _pick_col(df, "主体", "公司主体", label=sheet)
    bank_col = _pick_optional_col(df, "银行/通道名称", "银行/通道")
    acc = (
        df[bank_col].fillna("").astype(str)
        if bank_col
        else pd.Series("", index=df.index, dtype=object)
    )
    period_col = _pick_col(df, "入账期间", "统计期间", label=sheet)
    type_col = _pick_col(df, "类型", "方向", label=sheet)
    out = pd.DataFrame({
        "主体": df[entity_col],
        "分行维度": df["分行维度"],
        "账号": acc,
        "入账期间": df[period_col].map(lambda x: normalize_period(x, DEFAULT_PERIOD)),
        "USD金额": to_usd_numeric(df["USD金额"]),
        "类型": type_col,
        "入账科目": "成本",
    })
    # fix: type_col should be column name string, not the column itself
    out["类型"] = df[type_col]
    out.insert(0, "数据来源", label)
    return out


def load_jp(path: Path) -> pd.DataFrame:
    """Load 境内日本通道 xlsx."""
    xl = pd.ExcelFile(path)
    sheet = "Sheet1" if "Sheet1" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    mask = df["入账科目"].map(is_ledger_subject_strict_cost)
    df = df[mask].copy()
    ch_col = "渠道名称" if "渠道名称" in df.columns else "分行维度"
    out = pd.DataFrame({
        "主体": df["主体"],
        "分行维度": df[ch_col],
        "账号": df[ch_col].astype(str),
        "入账期间": df["入账期间"].map(lambda x: normalize_period(x, DEFAULT_PERIOD)),
        "USD金额": to_usd_numeric(df["USD金额"]),
        "类型": df["类型"],
        "入账科目": df["入账科目"],
    })
    out.insert(0, "数据来源", "境内日本通道")
    return out


# ---------------------------------------------------------------------------
# Reconciliation summary builder
# ---------------------------------------------------------------------------

def build_reconciliation_summary(detail: pd.DataFrame) -> tuple[pd.DataFrame, float, int]:
    """
    按模板行逐行汇总：CITIHK+PPHK 的大账号行由 RECONCILIATION_TEMPLATE_ROWS 硬编码（与 pingpong-master 一致），
    首条 CITIHK+PPHK+None 为残差行；CITISG/CITI-SG+PPHK 在汇总前按明细动态展开大账号骨架；
    模板「账号」为空行：汇总该 (分行,主体) 下明细但排除同组合已列出的账号，避免重复。
    """
    skeleton = pd.DataFrame(
        RECONCILIATION_TEMPLATE_ROWS,
        columns=[COL_CH_TOP, COL_CH_BRANCH, "主体", "账号"],
    )
    d = detail.copy()
    d["_br_m"] = d["分行维度"].map(_norm_match_key)
    d["_en_m"] = d["主体"].map(_norm_match_key)
    d["_ac"] = d["账号"].map(_norm_account_key)
    d["_bucket"] = d["类型"].map(cost_type_bucket)
    _u = d["_br_m"].isin(FORCE_RECEIVABLE_CHANNEL_BRANCH_KEYS)
    d.loc[_u, "_bucket"] = "收款通道成本"

    skeleton = _expand_citisg_pphk_skeleton(skeleton, d, COL_CH_TOP, COL_CH_BRANCH)

    explicit_accounts: dict[tuple[str, str], set[str]] = {}
    for _, r in skeleton.iterrows():
        br_m = _norm_match_key(r[COL_CH_BRANCH])
        en_m = _norm_match_key(r["主体"])
        ak = _norm_account_key(r["账号"])
        if ak is not None:
            explicit_accounts.setdefault((br_m, en_m), set()).add(ak)

    matched_any = pd.Series(False, index=d.index)
    rows: list[dict] = []
    month_val = int(DEFAULT_PERIOD)

    for _, r in skeleton.iterrows():
        br_key = _norm_match_key(r[COL_CH_BRANCH])
        en_key = _norm_match_key(r["主体"])
        tac = _norm_account_key(r["账号"])
        m = (d["_br_m"] == br_key) & (d["_en_m"] == en_key)
        if tac is not None:
            m &= d["_ac"] == tac
        else:
            listed = explicit_accounts.get((br_key, en_key))
            if listed:
                m &= ~d["_ac"].isin(listed)
        matched_any |= m
        sub = d.loc[m]
        bucket_sums = sub.groupby("_bucket", dropna=False)["USD金额"].sum()
        total = float(sub["USD金额"].sum())
        src = sub["数据来源"].astype(str).str.strip()
        src = src[src.ne("") & src.str.lower().ne("nan")]
        src_str = ";".join(sorted(src.unique())) if len(src) else ""
        row_dict: dict = {
            COL_CH_TOP: r[COL_CH_TOP],
            COL_CH_BRANCH: r[COL_CH_BRANCH],
            "主体": r["主体"],
            "账号": r["账号"],
            "数据来源": src_str,
            "month": month_val,
            "总成本": total,
        }
        for c in COST_BUCKETS:
            row_dict[c] = float(bucket_sums.get(c, 0.0))
        rows.append(row_dict)

    out = pd.DataFrame(rows)
    order = [COL_CH_TOP, COL_CH_BRANCH, "主体", "账号", "数据来源", "month", "总成本"] + COST_BUCKETS
    out = out[order]

    unmatched = d.loc[~matched_any]
    unmatched_usd = float(unmatched["USD金额"].sum()) if len(unmatched) else 0.0
    return out, unmatched_usd, int(len(unmatched))


# ---------------------------------------------------------------------------
# Main merge entry point
# ---------------------------------------------------------------------------

@dataclass
class CostSummaryInput:
    """Paths to channel outputs (all optional — missing channels are skipped)."""
    bill: Path | None = None
    own: Path | None = None
    cust: Path | None = None
    special_transfer: Path | None = None  # has sheets: 内转 / ACH return
    special_ach_refund: Path | None = None
    special_op_refund: Path | None = None  # sheet: OP退票表
    special_op_incoming: Path | None = None  # sheet: OP入账表
    cn_jp: Path | None = None
    fx_map: dict[str, float] | None = None  # currency -> 1 unit = X USD


def run_cost_summary(
    inputs: CostSummaryInput,
    output_dir: Path,
    *,
    period: str = DEFAULT_PERIOD,
    on_step: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Run the cost summary merge and write output files.

    ``on_step`` optional human-visible progress hook (UI / task log).
    Returns a dict with metrics and paths.
    """
    global DEFAULT_PERIOD
    DEFAULT_PERIOD = period

    fx_map = inputs.fx_map or {"USD": 1.0}
    parts: list[pd.DataFrame] = []
    loaded: list[str] = []
    errors: list[str] = []

    def _say(msg: str) -> None:
        if on_step:
            try:
                on_step(msg)
            except Exception:
                pass

    def _try_load(name: str, func, *args):
        _say(f"正在读取并入池：{name} …")
        try:
            df = func(*args)
            if df is not None and not df.empty:
                parts.append(df)
                loaded.append(name)
                logger.info("cost_summary: %s loaded %d rows", name, len(df))
                _say(f"「{name}」已加入合并池：{len(df)} 行")
            else:
                logger.info("cost_summary: %s empty", name)
                _say(f"「{name}」结果为空，跳过")
        except Exception as exc:
            logger.warning("cost_summary: %s failed: %s", name, exc)
            errors.append(f"{name}: {exc}")
            _say(f"「{name}」读取失败：{exc}")

    if inputs.bill:
        _try_load("账单", load_bill, inputs.bill)
    if inputs.own:
        _try_load("自有流水", load_own, inputs.own)
    if inputs.cust:
        _try_load("客资流水", load_cust, inputs.cust)
    if inputs.special_transfer:
        _try_load("内转（特殊来源）", load_neizhuan, inputs.special_transfer, fx_map)
    if inputs.special_ach_refund:
        _try_load("ACH 退票（特殊来源）", load_ach, inputs.special_ach_refund)
    if inputs.special_op_refund:
        _try_load("OP退票表", load_op_sheet, inputs.special_op_refund, "OP退票表", "OP退票表")
    if inputs.special_op_incoming:
        _try_load("OP入账表", load_op_sheet, inputs.special_op_incoming, "OP入账表", "OP入账表")
    if inputs.cn_jp:
        _try_load("境内&日本通道", load_jp, inputs.cn_jp)

    if not parts:
        return {
            "success": False,
            "error": "没有成功加载任何渠道数据",
            "loaded": loaded,
            "errors": errors,
        }

    _say(f"纵向合并各渠道明细：共 {len(parts)} 段 → concat …")
    detail = pd.concat(parts, ignore_index=True)
    for c in ("类型", "入账科目"):
        detail[c] = detail[c].replace({np.nan: ""}).astype(str)
    detail["账号"] = detail["账号"].replace({"nan": ""})

    output_dir.mkdir(parents=True, exist_ok=True)

    _say(f"合并后总行数：{len(detail)}，正在写入明细 xlsx …")
    # Write detail
    detail_path = output_dir / f"成本汇总_{period}_明细.xlsx"
    with pd.ExcelWriter(detail_path, engine="openpyxl") as w:
        detail.to_excel(w, sheet_name="明细", index=False)

    _say("正在按模板口径汇总（核对表）…")
    # Write summary
    recon, unmatched_usd, unmatched_n = build_reconciliation_summary(detail)
    summary_path = output_dir / f"成本汇总_{period}_汇总.xlsx"
    with pd.ExcelWriter(summary_path, engine="openpyxl") as w:
        recon.to_excel(w, sheet_name="模板口径汇总", index=False)

    _say("正在写出「明细 + 汇总」合并工作簿 …")
    # Write combined workbook
    combined_path = output_dir / f"成本汇总_{period}_合并.xlsx"
    with pd.ExcelWriter(combined_path, engine="openpyxl") as w:
        detail.to_excel(w, sheet_name="明细", index=False)
        recon.to_excel(w, sheet_name="模板口径汇总", index=False)

    _say(f"成本汇总文件已写出（period={period}）")

    return {
        "success": True,
        "detail_path": str(detail_path),
        "summary_path": str(summary_path),
        "combined_path": str(combined_path),
        "detail_rows": len(detail),
        "summary_rows": len(recon),
        "unmatched_rows": unmatched_n,
        "unmatched_usd": round(unmatched_usd, 2),
        "total_usd": round(float(detail["USD金额"].sum()), 2),
        "total_cost": round(float(recon["总成本"].sum()), 2),
        "loaded_channels": loaded,
        "errors": errors,
    }