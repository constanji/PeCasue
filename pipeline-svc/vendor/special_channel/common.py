"""Shared utilities for special-channel processing.

Ported from pingpong-master/script/other/_common.py with adaptations:
- FX rates loaded from ``rules/files/fx/各种货币对美元折算率.csv`` (pipeline-svc
  RuleStore) instead of a local 模版.xlsx.
- Source-file discovery is removed — the caller passes file paths explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

XENDIT_ENTITY_LABEL = "收款通道成本"
OP_INCOMING_SUBTYPE = "退款退票（VA）"
GMO_TAX_DIVISOR = 1.1

ACCOUNT_ENTITY_TO_SUBJECT: Dict[str, str] = {
    "PPMY": "PPMY",
    "MANAID": "MANA-ID",
    "MANA-ID": "MANA-ID",
    "PPHK": "PPHK",
    "PT FIRST MONEY": "PT First Money",
    "MANA PAYMENT SG": "BRSG",
    "PPI": "PPI",
    "PPGT": "PPGT",
    "PPEU": "PPEU",
    "MANAAU": "MANA AU",
    "MANA AU": "MANA AU",
    "PPUS": "PPUS",
    "PPUK": "PPUK",
    "PPJP": "PPJP",
    "NEXT MOUNTAIN": "NM",
    "NM": "NM",
}

OP_BANK_ENTITY_TO_BRANCH_DIM: Dict[tuple[str, str], str] = {
    ("CITI", "PPHK"): "CITIHK",
    ("JPM", "PPHK"): "JPMHK",
    ("SCB", "PPHK"): "SCBHK",
    ("DBS", "PPHK"): "DBSHK",
    ("BOC", "PPHK"): "BOCHK",
    ("DB", "PPHK"): "DBHK",
    ("CITI", "PPEU"): "CITIEU",
    ("CITI", "PPUS"): "CITIUS",
}

# ---------------------------------------------------------------------------
# Dataclass for OP branch key
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OpBranchKey:
    bank: str
    entity: str


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------


def safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------


def yyyymm_from_date_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.strftime("%Y%m")


# ---------------------------------------------------------------------------
# Subject normalisation
# ---------------------------------------------------------------------------


def normalize_subject_from_account_entity(s: pd.Series) -> pd.Series:
    """Map raw account-entity names to standardised subject names."""
    raw = s.astype(str).fillna("").str.strip()
    key = raw.str.upper()
    mapped = key.map(ACCOUNT_ENTITY_TO_SUBJECT)
    return mapped.fillna(raw)


# ---------------------------------------------------------------------------
# OP branch mapping
# ---------------------------------------------------------------------------


def build_op_bank_entity_to_branch_dim() -> dict[OpBranchKey, str]:
    return {
        OpBranchKey(bank=bank, entity=entity): branch
        for (bank, entity), branch in OP_BANK_ENTITY_TO_BRANCH_DIM.items()
    }


# ---------------------------------------------------------------------------
# FX rate loading — from rules/files/fx CSV
# ---------------------------------------------------------------------------

_FX_CSV_NAME = "各种货币对美元折算率.csv"


def _fx_csv_path(rules_files_dir: Path) -> Path:
    return rules_files_dir / "fx" / _FX_CSV_NAME


def load_usd_fx_rates_from_csv(rules_files_dir: Path) -> pd.DataFrame:
    """Load FX rates from ``rules/files/fx/各种货币对美元折算率.csv``.

    Returns a DataFrame with columns ``[month, currency, usd_rate]``.
    If the file does not exist or is empty, returns an empty DataFrame.
    """
    fx_path = _fx_csv_path(rules_files_dir)
    if not fx_path.exists():
        return pd.DataFrame(columns=["month", "currency", "usd_rate"])

    df = pd.read_csv(fx_path, encoding="utf-8-sig").dropna(how="all")
    if df.empty:
        return pd.DataFrame(columns=["month", "currency", "usd_rate"])

    # Detect columns
    rate_col: Optional[str] = None
    for name in ("兑USD汇率", "对美元折算率"):
        if name in df.columns:
            rate_col = name
            break
    if rate_col is None:
        # Try heuristic
        for c in df.columns:
            cs = str(c).strip()
            if "折算" in cs or "USD" in cs.upper():
                rate_col = cs
                break
    if rate_col is None:
        return pd.DataFrame(columns=["month", "currency", "usd_rate"])

    code_col: Optional[str] = None
    for name in ("货币代码", "货币名称", "currency", "Currency"):
        if name in df.columns:
            code_col = name
            break
    if code_col is None:
        # Assume first column with 3-letter strings
        code_col = df.columns[0]

    month_col: Optional[str] = None
    for name in ("月份", "month", "期间", "日期"):
        if name in df.columns:
            month_col = name
            break

    codes = df[code_col].astype(str).str.strip().str.upper()
    rates_num = pd.to_numeric(df[rate_col], errors="coerce")

    if month_col is not None:
        months = pd.to_numeric(df[month_col], errors="coerce")
        out = pd.DataFrame({
            "month": months,
            "currency": codes,
            "usd_rate": rates_num,
        })
        out = out.loc[
            out["currency"].str.len().eq(3) & out["usd_rate"].notna() & out["month"].notna()
        ].copy()
        out["month"] = out["month"].astype(int).astype(str)
    else:
        # No month column — treat as a single-period table
        out = pd.DataFrame({
            "month": pd.Series([""] * len(df)),
            "currency": codes,
            "usd_rate": rates_num,
        })
        out = out.loc[out["currency"].str.len().eq(3) & out["usd_rate"].notna()].copy()

    out = out.drop_duplicates(["month", "currency"], keep="last").reset_index(drop=True)
    return out


def _get_fx_preferred_yyyymm() -> Optional[str]:
    """从 FX RuleStore meta 读取目标月份（YYYYMM）。失败时返回 None。"""
    try:
        from server.rules.store import get_fx_preferred_yyyymm
        return get_fx_preferred_yyyymm()
    except Exception:
        return None


def lookup_usd_fx_rate_series(
    period_series: pd.Series,
    currency_series: pd.Series,
    rules_files_dir: Path,
    preferred_ym: Optional[str] = None,
) -> pd.Series:
    """Look up per-row USD FX rates by (period, currency).

    fallback 优先顺序：preferred_ym（fx_month_label）→ CSV 中最新月份。
    当 preferred_ym 未传入时自动从 RuleStore FX meta 读取。
    """
    rates = load_usd_fx_rates_from_csv(rules_files_dir)
    if rates.empty:
        return pd.Series(
            [float("nan")] * len(period_series),
            index=period_series.index,
            dtype="float64",
        )

    if preferred_ym is None:
        preferred_ym = _get_fx_preferred_yyyymm()

    lookup = rates.copy()
    lookup["key"] = lookup["month"].astype(str) + "|" + lookup["currency"].astype(str)
    exact_map = dict(zip(lookup["key"].tolist(), lookup["usd_rate"].tolist()))

    # 优先月份 fallback（fx_month_label），其次最新月份
    if preferred_ym:
        preferred_df = lookup[lookup["month"].astype(str) == preferred_ym].drop_duplicates(
            ["currency"], keep="last"
        )
        preferred_map: dict[str, float] = dict(
            zip(preferred_df["currency"].tolist(), preferred_df["usd_rate"].tolist())
        )
    else:
        preferred_map = {}

    latest = (
        lookup.sort_values(["currency", "month"], kind="mergesort")
        .drop_duplicates(["currency"], keep="last")
    )
    latest_map: dict[str, float] = dict(
        zip(latest["currency"].tolist(), latest["usd_rate"].tolist())
    )

    # preferred_map 优先；若 preferred_ym 未在 CSV 中找到则退回 latest_map
    fallback_map = preferred_map if preferred_map else latest_map

    period_norm = period_series.astype(str).str.strip()
    currency_norm = currency_series.astype(str).str.strip().str.upper()
    keys = period_norm + "|" + currency_norm
    out = keys.map(exact_map)
    fallback = currency_norm.map(fallback_map)
    out = out.where(out.notna(), fallback)
    return pd.to_numeric(out, errors="coerce")