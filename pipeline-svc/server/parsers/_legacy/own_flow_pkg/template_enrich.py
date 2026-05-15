"""根据 ``rules/files`` 下 fx + mapping 表填充绿区：USD（汇率）、主体/分行（账户 mapping）、类型（费项 mapping）。"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from .mapping_import import (
    _code_column_for_fx_table,
    _fx_rate_col_from_columns,
    normalize_fx_rate_value,
)

from .special_source_mapping import load_special_source_mapping

# 支行简称 → 账单及自有流水费项mapping表「渠道」
# 仅映射到「账单及自有流水费项mapping表」中实际存在的「渠道」值
_CITI_BRANCH_TO_CHANNEL: dict[str, str] = {
    "CITIAE": "CITI-AE",
    "CITIAU": "CITI-AU",
    "CITICA": "CITI-CA",
    "CITIEU": "CITI-EU",
    "CITIGB": "CITI-EU",
    "CITIHK": "CITI-HK",
    "CITIID": "CITI-ID",
    "CITIINBENGALURU": "CITI-ID",
    "CITIINMUMBAI": "CITI-ID",
    "CITIJP": "CITI-JP",
    "CITIMX": "CITI-MX",
    "CITINZ": "CITI-NZ",
    "CITIPH": "CITI-PH",
    "CITIPHTaguig": "CITI-PH",
    "CITIPL": "CITI-PL",
    "CITISG": "CITI-SG",
    "CITITH": "CITI-TH",
    "CITIUS": "CITI-US",
}
_JPM_BRANCH_TO_CHANNEL: dict[str, str] = {
    "JPMUS": "JPM-US",
    "JPMHK": "JPM-HK",
    "JPMSG": "JPM-SG",
    "JPMEU": "JPM-EU",
}


def _canonical_fee_mapping_channel(raw: Any) -> str:
    """费项 mapping 「渠道」与账户侧 fee_channel 对齐：**去首尾空白 → 大写 → 去掉 - / _ / 空格**。

    使 ``BOC-US``、``BOC US``、``BOCUS`` 与 mapping 表中写法互相匹配；
    ``CITI-HK`` 与 ``CITIHK`` 同理（仅用于索引/匹配键，不改变费用项文本子串）。
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    return str(raw).strip().upper().replace("-", "").replace("_", "").replace(" ", "")


def _norm_account_key(v: Any) -> str:
    """与 mapping 表「银行账号」对齐：去掉空格、Excel 浮点账号（1041783008.0）等。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, (int, np.integer)):
        return str(int(v))
    if isinstance(v, float):
        if abs(v - round(v)) < 1e-9:
            return str(int(round(v)))
        return str(v).strip()
    t = str(v).strip().replace("\u00a0", "").replace(" ", "")
    # 字符串形式的数字账号被读成 float 再转回 str 时常见 "....0"
    if re.fullmatch(r"\d+\.0+", t):
        t = t.split(".")[0]
    return t


def _account_key_variants(v: Any) -> list[str]:
    """同一账号多种写法（纯数字、去前导零、大小写）用于建索引与查找。"""
    base = _norm_account_key(v)
    if not base:
        return []
    seen: set[str] = set()
    out: list[str] = []

    def add(x: str) -> None:
        if x and x not in seen:
            seen.add(x)
            out.append(x)

    add(base)
    add(base.upper())
    digits = re.sub(r"\D", "", base)
    if digits:
        add(digits)
        stripped = digits.lstrip("0") or "0"
        if stripped != digits:
            add(stripped)
    return out


_FX_TABLE_STEM = "各种货币对美元折算率"


def _fx_dirs_ordered() -> list[Path]:
    """``OWN_FLOW_FILES_ROOT/fx``（可选）→ ``PIPELINE_DATA_DIR/rules/files/fx``。"""
    dirs: list[Path] = []
    env_root = os.environ.get("OWN_FLOW_FILES_ROOT", "").strip()
    if env_root:
        dirs.append(Path(env_root).expanduser().resolve() / "fx")
    try:
        from server.core.paths import get_rules_files_dir

        dirs.append(get_rules_files_dir() / "fx")
    except Exception:
        pass

    seen: set[Path] = set()
    out: list[Path] = []
    for d in dirs:
        try:
            r = d.resolve()
        except OSError:
            continue
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out


def _filter_df_by_preferred_month(df: pd.DataFrame, preferred_ym: str) -> pd.DataFrame:
    """将含多月数据的 DataFrame 过滤到指定 YYYYMM 月份。找不到则返回原 DataFrame。"""
    date_col: str | None = None
    for c in df.columns:
        if str(c).strip() in ("日期", "month", "期间", "Month", "月份"):
            date_col = str(c)
            break
    if date_col is None:
        return df

    def _to_yyyymm(v: object) -> str:
        if v is None:
            return ""
        try:
            if pd.isna(v):  # type: ignore[arg-type]
                return ""
        except Exception:
            pass
        try:
            return str(int(float(str(v).strip())))
        except Exception:
            return str(v).strip()

    month_vals = df[date_col].apply(_to_yyyymm)
    mask = month_vals == preferred_ym
    return df[mask].copy() if mask.any() else df


def _load_fx_from_dir(fx_dir: Path, preferred_ym: str | None = None) -> dict[str, float] | None:
    stem = fx_dir / _FX_TABLE_STEM
    if stem.with_suffix(".csv").exists():
        try:
            df = pd.read_csv(stem.with_suffix(".csv"), encoding="utf-8-sig")
            if preferred_ym:
                df = _filter_df_by_preferred_month(df, preferred_ym)
            fx = _fx_rates_from_dataframe(df)
            if fx:
                return fx
        except Exception:
            pass
    if stem.with_suffix(".xlsx").exists():
        try:
            df = pd.read_excel(stem.with_suffix(".xlsx"), engine="openpyxl")
            if preferred_ym:
                df = _filter_df_by_preferred_month(df, preferred_ym)
            fx = _fx_rates_from_dataframe(df)
            if fx:
                return fx
        except Exception:
            pass
    return None


def _rules_mapping_stem(name: str) -> Path:
    from server.core.paths import get_rules_files_dir

    return get_rules_files_dir() / "mapping" / name


def _read_rules_mapping_dataframe(stem: Path, *, fee_mapping: bool = False) -> pd.DataFrame | None:
    """仅从 ``rules/files/mapping/{stem}`` 读取 ``.csv`` 或 ``.xlsx``（不再回退整本「模版」工作簿）。"""
    csv_path = stem.with_suffix(".csv")
    if csv_path.is_file():
        for hdr in (0, 1) if fee_mapping else (0,):
            try:
                df = pd.read_csv(
                    csv_path,
                    encoding="utf-8-sig",
                    header=hdr,
                    low_memory=False,
                )
                if df is not None and not df.empty:
                    if not fee_mapping or "渠道" in df.columns:
                        return df
            except Exception:
                pass
    xlsx_path = stem.with_suffix(".xlsx")
    if xlsx_path.is_file():
        for hdr in (0, 1) if fee_mapping else (0,):
            try:
                df = pd.read_excel(xlsx_path, header=hdr, engine="openpyxl")
                if df is not None and not df.empty:
                    if not fee_mapping or "渠道" in df.columns:
                        return df
            except Exception:
                pass
    return None


def _fx_rates_from_dataframe(df: pd.DataFrame) -> dict[str, float]:
    """与 mapping_import 列式表一致；优先「兑USD汇率」列；首列可为误标的「货币名称」。"""
    rates: dict[str, float] = {}
    d = df.rename(
        columns={c: str(c).strip() if c is not None else "" for c in df.columns}
    )
    code_col = _code_column_for_fx_table(d)
    rate_col = _fx_rate_col_from_columns(list(d.columns))
    if code_col is None or rate_col is None:
        return rates
    for _, row in d.iterrows():
        c = row.get(code_col)
        r = row.get(rate_col)
        if c is None or (isinstance(c, float) and pd.isna(c)):
            continue
        code = str(c).strip().upper().split()[0] if c is not None else ""
        if len(code) != 3 or not code.isalpha():
            continue
        v = normalize_fx_rate_value(r)
        if v is None:
            continue
        rates[code] = v
    return rates


def _get_fx_preferred_yyyymm() -> str | None:
    """从 FX RuleStore meta 读取目标月份（YYYYMM）。失败时返回 None。"""
    try:
        from server.rules.store import get_fx_preferred_yyyymm
        return get_fx_preferred_yyyymm()
    except Exception:
        return None


def _load_fx_map() -> dict[str, float]:
    """仅 ``rules/files/fx``（及 OWN_FLOW_FILES_ROOT/fx）；不再有「模版」工作簿内嵌 sheet 回退。

    优先使用 RuleStore FX meta 中记录的 fx_month_label 对应月份；
    若未设置则回退为文件中最新月份（旧行为）。
    """
    preferred_ym = _get_fx_preferred_yyyymm()
    for fx_dir in _fx_dirs_ordered():
        fx = _load_fx_from_dir(fx_dir, preferred_ym=preferred_ym)
        if fx:
            return fx
    return {}


def _branch_to_fee_channel(bank: Any, branch: Any) -> str | None:
    """账户表「银行/通道简称 + 支行简称」→ 费项 mapping「渠道」。"""
    b = str(branch).strip() if branch is not None and not (isinstance(branch, float) and pd.isna(branch)) else ""
    if not b:
        return None
    if b in _CITI_BRANCH_TO_CHANNEL:
        return _CITI_BRANCH_TO_CHANNEL[b]
    if b in _JPM_BRANCH_TO_CHANNEL:
        return _JPM_BRANCH_TO_CHANNEL[b]
    bk = str(bank).strip() if bank is not None and not (isinstance(bank, float) and pd.isna(bank)) else ""
    if bk.upper() == "CITI" and b.upper().startswith("CITI"):
        return _CITI_BRANCH_TO_CHANNEL.get(b)
    return None


def _load_account_lookup() -> dict[str, dict[str, Any]]:
    """按账号多列、多写法建索引（输出列仅 Account Number，合并自 Account/Merchant 等）。"""
    stem = _rules_mapping_stem("账户对应主体分行mapping表")
    df = _read_rules_mapping_dataframe(stem, fee_mapping=False)
    if df is None:
        return {}
    if "银行账号" not in df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    key_cols = [c for c in ("银行账号", "银行账号.1", "系统账号") if c in df.columns]
    bank_col = "银行/通道简称" if "银行/通道简称" in df.columns else None
    branch_col = "支行简称" if "支行简称" in df.columns else None
    entity_col = "主体1" if "主体1" in df.columns else None
    nature_col = "账户性质" if "账户性质" in df.columns else None

    records = df.to_dict("records")
    for row in records:
        bank_val = row.get(bank_col) if bank_col else None
        branch_val = row.get(branch_col) if branch_col else None
        fee_ch = _branch_to_fee_channel(bank_val, branch_val)
        info = {
            "主体1": row.get(entity_col) if entity_col else None,
            "支行简称": branch_val,
            "fee_channel": fee_ch,
        }
        if nature_col:
            info["账户性质"] = row.get(nature_col)
        for col in key_cols:
            for key in _account_key_variants(row.get(col)):
                if key not in out:
                    out[key] = info
    return out


def _lookup_account_row(
    row: Any,
    acc_map: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """用 Account / Account Number 与 mapping「银行账号」匹配（账单/自有流水）。"""
    for col in ("Account", "Account Number"):
        try:
            v = row[col] if hasattr(row, "__getitem__") else getattr(row, col, None)
        except (KeyError, TypeError):
            v = None
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        if isinstance(v, str) and not str(v).strip():
            continue
        for key in _account_key_variants(v):
            if key in acc_map:
                return acc_map[key]
    return None


class _FeeIndex:
    """预建索引：按 channel 分桶 + 预计算 stripped pattern，避免全表遍历和重复计算。"""

    __slots__ = ("_by_channel", "_all_tuples", "valid_channels")

    def __init__(self, patterns: list[dict[str, Any]], channels: set[str]) -> None:
        self.valid_channels = channels
        by_ch: dict[str, list[tuple[str, str, str]]] = {}
        all_tuples: list[tuple[str, str, str]] = []
        for item in patterns:
            ch = item["channel"]
            pat = item["pattern"]
            typ = item["typ"]
            base = _fee_pattern_strip_channel_suffix(ch, pat) or ""
            t = (pat, base, typ)
            all_tuples.append(t)
            by_ch.setdefault(ch, []).append(t)
        self._all_tuples = all_tuples
        self._by_channel = by_ch

    def _get_tuples(
        self,
        *,
        exact: str | None,
        channel_set: frozenset[str] | None,
        prefix: str | None,
    ) -> list[tuple[str, str, str]]:
        if exact:
            ex = _canonical_fee_mapping_channel(exact)
            return self._by_channel.get(ex, []) if ex else []
        if channel_set is not None:
            out: list[tuple[str, str, str]] = []
            for ch in channel_set:
                ck = _canonical_fee_mapping_channel(ch)
                if ck:
                    out.extend(self._by_channel.get(ck, []))
            return out
        if prefix:
            px = prefix.upper()
            out = []
            for ch, items in self._by_channel.items():
                # mapping 渠道键已规范化（无「-」）；仍用前缀缩小范围（如 CITI、citi-xx → CITIXX）
                if str(ch).upper().startswith(px):
                    out.extend(items)
            return out
        return self._all_tuples

    @staticmethod
    def _base_match_variants(base: str) -> tuple[str, ...]:
        """去掉渠道后缀后的主串；兼容「JJJKJ,-BGL」→「JJJKJ,」与流水中「JJJKJ」不一致的情况。"""
        if not base:
            return ()
        b = base.strip()
        out: list[str] = [b]
        if b.endswith(","):
            t = b.rstrip(",").strip()
            if t and t not in out:
                out.append(t)
        return tuple(out)

    def match(
        self,
        u: str,
        *,
        exact: str | None,
        channel_set: frozenset[str] | None,
        prefix: str | None,
    ) -> str:
        if not u:
            return ""
        for pat, base, typ in self._get_tuples(exact=exact, channel_set=channel_set, prefix=prefix):
            if pat in u:
                return typ
            for b in self._base_match_variants(base):
                if b and b in u:
                    return typ
        return ""

    def match_cascade(
        self,
        fee_text: str,
        *,
        fee_exact: str | None,
        ch_set: frozenset[str] | None,
        prefix_loose: str | None,
    ) -> str:
        u = fee_text.upper() if fee_text else ""
        if not u:
            return ""

        prefix_strict = None if fee_exact else prefix_loose
        if ch_set is not None:
            prefix_strict = None

        # 匹配顺序与范围：
        # 1) 账户精确渠道（略）
        # 2) 指定 channel_set 时仅在对应渠道下做子串匹配
        # 3) 无 channel_set 时按 prefix（如 CITI）限定渠道名前缀
        # 4) 仅在「未限定任何渠道/前缀」时全表子串匹配（如部分未挂 file_group 的解析）
        # 已限定 barclays / CITI-EU 等集合时，**不得**在失败后回退到全表，否则「INT」等会命中他行规则，误出 others
        attempts: list[tuple[str | None, frozenset[str] | None, str | None]] = []
        if fee_exact:
            attempts.append((fee_exact, ch_set, prefix_strict))
        attempts.append((None, ch_set, prefix_strict))
        if prefix_loose is not None:
            attempts.append((None, None, prefix_loose))
        if ch_set is None and prefix_loose is None:
            attempts.append((None, None, None))

        seen: set[tuple[Any, Any, Any]] = set()
        for ex, cs, px in attempts:
            key = (ex, cs, px)
            if key in seen:
                continue
            seen.add(key)
            t = self.match(u, exact=ex, channel_set=cs, prefix=px)
            if t:
                return t
        return ""


def _load_fee_patterns() -> _FeeIndex:
    stem = _rules_mapping_stem("账单及自有流水费项mapping表")
    fm = _read_rules_mapping_dataframe(stem, fee_mapping=True)
    if fm is None:
        return _FeeIndex([], set())
    fm = fm.dropna(how="all")
    rows: list[dict[str, Any]] = []
    channels: set[str] = set()
    for _, r in fm.iterrows():
        ch = r.get("渠道")
        if ch is None or (isinstance(ch, float) and pd.isna(ch)):
            continue
        channel = _canonical_fee_mapping_channel(ch)
        if not channel:
            continue
        channels.add(channel)
        typ = r.get("打标分类（新）")
        if typ is None or (isinstance(typ, float) and pd.isna(typ)):
            continue
        type_str = str(typ).strip()
        for col in ("费用项名称", "费用项名称.1"):
            pat = r.get(col)
            if pat is None or (isinstance(pat, float) and pd.isna(pat)):
                continue
            s = str(pat).strip()
            if not s:
                continue
            rows.append(
                {
                    "channel": channel,
                    "pattern": s.upper(),
                    "typ": type_str,
                }
            )
    rows.sort(key=lambda x: len(x["pattern"]), reverse=True)
    return _FeeIndex(rows, channels)


def _fee_channel_prefix_for_file_group(fg: str) -> str | None:
    """无账户命中时，用 file_group 限定费项渠道前缀。"""
    if fg in ("ppbk_main", "ppbk_other"):
        return "CITI"
    if fg == "citi_other":
        return "CITI"
    if fg == "ppeu_citi":
        return "CITI"
    if fg == "ppeu_bgl":
        return "BGL"
    if fg == "jpm":
        return "JPM"
    if fg == "boc":
        return "BOC"
    if fg == "scb":
        return "SCB"
    if fg == "db":
        return None
    if fg == "dbs":
        return "DBS"
    return None


def _fee_channel_set_for_file_group(fg: str) -> frozenset[str] | None:
    """精确渠道集合（优先于前缀）。"""
    if fg == "ppeu_citi":
        return frozenset({"CITI-EU"})
    if fg == "ppeu_bgl":
        return frozenset({"BGL-EU"})
    if fg == "ppeu_bc":
        return frozenset({"BC"})
    if fg == "ppeu_barclays":
        return frozenset({"barclays", "Barclays"})
    if fg == "db":
        return frozenset({"DB", "DB-HK", "DBS-HK", "DBS-SG"})
    if fg == "dbs":
        return frozenset({"DBS", "DBS-SG", "DBS-HK"})
    return None


def _channel_matches(
    fee_channel: str,
    *,
    exact: str | None,
    channel_set: frozenset[str] | None,
    prefix: str | None,
) -> bool:
    if exact:
        return fee_channel == exact
    if channel_set is not None:
        return fee_channel in channel_set
    if prefix:
        return fee_channel.startswith(prefix)
    return True


def _cell_nonempty(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    if isinstance(v, str) and not str(v).strip():
        return False
    return True


def _combined_fee_match_text_bill(row: Any) -> str:
    """账单「账单」表：费项 + Description 合并后做费项 mapping 子串匹配。"""
    parts: list[str] = []
    for key in ("费项", "Description"):
        try:
            v = row[key] if hasattr(row, "__getitem__") else getattr(row, key, None)
        except (KeyError, TypeError):
            v = None
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            parts.append(s)
    raw = " ".join(parts)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw.upper()


def _combined_fee_match_text(row: Any) -> str:
    """费项 mapping：用 description + Payment Details 等合并后再做子串匹配（与模板「费项=描述」一致，并覆盖附言）。"""
    parts: list[str] = []
    for key in (
        "费项",
        "Transaction Description",
        "Payment Details",
        "Product Type",
        "mark（财务）",
    ):
        try:
            v = row[key] if hasattr(row, "__getitem__") else getattr(row, key, None)
        except (KeyError, TypeError):
            v = None
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            parts.append(s)
    raw = " ".join(parts)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw.upper()


def _fee_pattern_strip_channel_suffix(channel: str, pattern_upper: str) -> str | None:
    """
    费项 mapping 中 BGL-EU / BC / barclays 等渠道在「费用项名称」后加 `-BGL`、`-BC`、`-Barclays` 等后缀区分；
    流水原文通常不含该后缀，故全串未命中时再尝试去掉后缀后的主串做子串匹配。
    """
    c = _canonical_fee_mapping_channel(channel)
    p = pattern_upper
    if c in ("BGLEU", "BGL"):
        if p.endswith("-BGL"):
            return p[:-4]
    elif c == "BC":
        if p.endswith("-BC"):
            return p[:-3]
    elif c in ("BARCLAYS", "BARCLAY"):
        if p.endswith("-BARCLAYS"):
            return p[:-9]
    return None


def _match_fee_type(
    fee_index: _FeeIndex,
    desc: str,
    *,
    exact: str | None,
    channel_set: frozenset[str] | None,
    prefix: str | None,
) -> str:
    u = desc.upper() if desc else ""
    return fee_index.match(u, exact=exact, channel_set=channel_set, prefix=prefix)


def _match_fee_type_cascade(
    fee_index: _FeeIndex,
    fee_text: str,
    *,
    fee_exact: str | None,
    ch_set: frozenset[str] | None,
    prefix_loose: str | None,
) -> str:
    return fee_index.match_cascade(
        fee_text, fee_exact=fee_exact, ch_set=ch_set, prefix_loose=prefix_loose,
    )


def _infer_accounting_subject(row: Any, typ: str) -> str:
    """规则未命中时不自动推断入账科目，保持为空。"""
    return ""


def _resolve_accounting_subject(row: Any, typ: str) -> str:
    """保留规则已写入的入账科目；否则用推断。"""
    try:
        cur = row.get("入账科目", "")  # type: ignore[union-attr]
    except (AttributeError, TypeError):
        cur = ""
    if _cell_nonempty(cur) and str(cur).strip().lower() != "nan":
        return str(cur).strip()
    inf = _infer_accounting_subject(row, typ)
    return inf if inf else ""


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, str) and not str(v).strip():
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _normalize_ccy(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip().upper()
    if not s:
        return ""
    m = re.match(r"^[A-Z]{3}$", s)
    if m:
        return s
    return s


def _fx_rate_for_ccy(ccy: str, fx: dict[str, float]) -> float | None:
    """币种代码在「对美元折算率」表中的汇率；CNH 无独立行时回退 CNY。"""
    if not ccy:
        return None
    if ccy in fx:
        return fx[ccy]
    if ccy == "CNH" and "CNY" in fx:
        return fx["CNY"]
    return None


def enrich_bill_dataframe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    全渠道「账单」总表：按 ``rules/files/fx`` + ``rules/files/mapping`` 填充绿区需匹配信息。

    - USD金额：Charge in Invoice CCY × 对美元折算率（Invoice CCY）
    - 主体 / 分行维度：账户对应主体分行mapping表（银行账号）
    - 类型：账单及自有流水费项mapping表（渠道 + 费用项名称子串）
    - 入账科目：不修改（allin 已按特殊账号与「成本」处理）
    """
    fx = _load_fx_map()
    acc_map = _load_account_lookup()
    fee_index = _load_fee_patterns()
    valid_fee_channels = fee_index.valid_channels

    if df.empty:
        return df

    records = df.to_dict("records")
    usd_list: list[Any] = []
    ent_list: list[Any] = []
    branch_list: list[Any] = []
    type_list: list[Any] = []
    acc_nature_list: list[Any] = []

    fg = ""
    for row in records:
        amt = _as_float(row.get("Charge in Invoice CCY"))
        ccy = _normalize_ccy(row.get("Invoice CCY"))
        usd_val: Any = np.nan
        if amt is not None and not (isinstance(amt, float) and np.isnan(amt)):
            if not ccy or ccy == "USD":
                usd_val = amt
            else:
                rate = _fx_rate_for_ccy(ccy, fx)
                if rate is not None:
                    usd_val = round(amt * rate, 6)
                else:
                    usd_val = np.nan

        acc_info = _lookup_account_row_dict(row, acc_map)

        ent = row.get("主体", "")
        br = row.get("分行维度", "")
        if acc_info is not None:
            e1 = acc_info.get("主体1")
            if e1 is not None and not (isinstance(e1, float) and pd.isna(e1)) and str(e1).strip():
                ent = str(e1).strip()
            b1 = acc_info.get("支行简称")
            if b1 is not None and not (isinstance(b1, float) and pd.isna(b1)) and str(b1).strip():
                br = str(b1).strip()

        fee_exact: str | None = None
        if acc_info and acc_info.get("fee_channel"):
            fc = str(acc_info["fee_channel"])
            fc_c = _canonical_fee_mapping_channel(fc)
            if fc_c and fc_c in valid_fee_channels:
                fee_exact = fc_c

        ch_set = _fee_channel_set_for_file_group(fg)
        if ch_set is not None:
            fee_exact = None
        prefix_loose = _fee_channel_prefix_for_file_group(fg)

        parts: list[str] = []
        for key in ("费项", "Description"):
            v = row.get(key)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                s = str(v).strip()
                if s and s.lower() != "nan":
                    parts.append(s)
        fee_text = re.sub(r"\s+", " ", " ".join(parts)).strip().upper()

        typ = fee_index.match_cascade(
            fee_text,
            fee_exact=fee_exact,
            ch_set=ch_set,
            prefix_loose=prefix_loose,
        )

        usd_list.append(usd_val)
        ent_list.append(ent if _cell_nonempty(ent) else np.nan)
        branch_list.append(br if _cell_nonempty(br) else np.nan)
        type_list.append(typ if typ else np.nan)
        if acc_info is not None:
            nv = acc_info.get("账户性质")
            if nv is None or (isinstance(nv, float) and pd.isna(nv)):
                acc_nature_list.append(np.nan)
            else:
                s = str(nv).strip()
                acc_nature_list.append(s if s else np.nan)
        else:
            acc_nature_list.append(np.nan)

    out = df.copy()
    out["USD金额"] = usd_list
    out["主体"] = ent_list
    out["分行维度"] = branch_list
    out["类型"] = type_list
    out["账户性质"] = acc_nature_list
    return out


def _combined_fee_match_text_dict(row: dict[str, Any]) -> str:
    """与 _combined_fee_match_text 相同逻辑，但接受 dict（避免 Series 开销）。"""
    parts: list[str] = []
    for key in ("费项", "Transaction Description", "Payment Details", "Product Type", "mark（财务）"):
        v = row.get(key)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            parts.append(s)
    raw = " ".join(parts)
    return re.sub(r"\s+", " ", raw).strip().upper()


def _lookup_account_row_dict(row: dict[str, Any], acc_map: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """dict 版本的 _lookup_account_row，避免 Series 取值开销。"""
    for col in ("Account", "Account Number"):
        v = row.get(col)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        if isinstance(v, str) and not v.strip():
            continue
        for key in _account_key_variants(v):
            if key in acc_map:
                return acc_map[key]
    return None


def enrich_own_flow_dataframe(
    df: pd.DataFrame,
    *,
    progress_log: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """填充 USD金额、主体、分行维度、类型（费项 mapping 逐级放宽）、入账科目（规则优先，空则按类型/描述推断）。"""
    def lg(msg: str) -> None:
        if progress_log:
            progress_log(msg)

    lg("模版 enrich：载入 rules/files（fx/汇率、mapping/账户与费项）…")
    fx = _load_fx_map()
    acc_map = _load_account_lookup()
    fee_index = _load_fee_patterns()
    valid_fee_channels = fee_index.valid_channels

    if "__file_group" not in df.columns:
        df["__file_group"] = ""

    sp_src = load_special_source_mapping()

    records = df.to_dict("records")

    usd_list: list[Any] = []
    ent_list: list[Any] = []
    branch_list: list[Any] = []
    type_list: list[Any] = []
    subj_list: list[str] = []
    acc_nature_list: list[str] = []

    for row in records:
        fg = str(row.get("__file_group", "") or "")

        amt = _as_float(row.get("Transaction Amount"))
        ccy = _normalize_ccy(row.get("Account Currency"))
        usd_val: Any = ""
        if amt is not None and not (isinstance(amt, float) and np.isnan(amt)):
            if not ccy or ccy == "USD":
                usd_val = amt
            else:
                rate = _fx_rate_for_ccy(ccy, fx)
                if rate is not None:
                    usd_val = round(amt * rate, 6)
                else:
                    usd_val = ""
        usd_list.append(usd_val)

        acc_info = _lookup_account_row_dict(row, acc_map)

        ent = row.get("主体", "")
        br = row.get("分行维度", "")
        # 若规则已明确 entity_override（比如 JPM PPUS EFT DEBIT → PPHK），则主体不再按账户 mapping 覆盖
        # 避免 pandas 将缺失读成 NaN 后 bool(nan)==True
        ent_locked = row.get("__entity_locked") is True
        # 同理，若规则显式指定了 branch_override（JPM EFT DEBIT → JPMHK），分行维度也不应再被账户 mapping 覆盖
        br_locked = row.get("__branch_locked") is True
        if acc_info is not None:
            if not ent_locked:
                e1 = acc_info.get("主体1")
                if e1 is not None and not (isinstance(e1, float) and pd.isna(e1)) and str(e1).strip():
                    ent = str(e1).strip()
            if not br_locked:
                b1 = acc_info.get("支行简称")
                if b1 is not None and not (isinstance(b1, float) and pd.isna(b1)) and str(b1).strip():
                    br = str(b1).strip()

        if not br_locked:
            if fg in sp_src and (not br or not str(br).strip()):
                br = sp_src[fg][1]
            elif fg == "boc" and (not br or not str(br).strip()):
                br = "BOCUS"
            elif fg == "bosh" and (not br or not str(br).strip()):
                br = "BOSH"

        fee_exact: str | None = None
        if acc_info and acc_info.get("fee_channel"):
            fc = str(acc_info["fee_channel"])
            fc_c = _canonical_fee_mapping_channel(fc)
            if fc_c and fc_c in valid_fee_channels:
                fee_exact = fc_c

        ch_set = _fee_channel_set_for_file_group(fg)
        if ch_set is not None:
            fee_exact = None
        prefix_loose = _fee_channel_prefix_for_file_group(fg)

        fee_text = _combined_fee_match_text_dict(row)
        typ = fee_index.match_cascade(
            fee_text,
            fee_exact=fee_exact,
            ch_set=ch_set,
            prefix_loose=prefix_loose,
        )
        # RUMG：摘要「チャージ」行按打标分类强制为 others（处理表备注/入账科目 charge/成本 仅作标识）
        if fg == "rumg":
            td_only = str(row.get("Transaction Description", "") or "").strip()
            if td_only == "チャージ":
                typ = "others"

        # DB：规则备注 charge 时强制「类型」为 charge（费项 cascade 可能先从公司名等误匹配）
        if fg == "db":
            rm = str(row.get("备注", "") or "").strip().lower()
            if rm == "charge":
                typ = "charge"

        # DBS：处理表命中 RAPID FEE（备注=charge）时，打标分类归为 others
        if fg == "dbs":
            rm_d = str(row.get("备注", "") or "").strip().lower()
            if rm_d == "charge":
                typ = "others"

        cur_subj = row.get("入账科目", "")
        if _cell_nonempty(cur_subj) and str(cur_subj).strip().lower() != "nan":
            subj = str(cur_subj).strip()
        else:
            subj = ""

        # RUMG：备注 charge 而入账科目在传递中丢失时，兜底为「成本」（与处理表一致）
        if not subj and fg == "rumg":
            rm_r = str(row.get("备注", "") or "").strip().lower()
            if rm_r == "charge":
                subj = "成本"
        # DB-KR/DB-TH：处理表 Charge→charge 成本；若入账科目在传递中丢失，按类型兜底
        if not subj and fg == "db" and typ == "charge":
            subj = "成本"

        # 渠道为 DB/DBS 系（分行维度以 DB/DBS 开头，如 DBKR/DBTH/DBS…）且入账科目为成本 → 类型标为 others
        if subj == "成本":
            br_norm = str(br or "").strip().upper()
            if br_norm.startswith("DB"):
                typ = "others"

        if acc_info is not None:
            nv = acc_info.get("账户性质")
            if nv is None or (isinstance(nv, float) and pd.isna(nv)):
                acc_nature_list.append("")
            else:
                acc_nature_list.append(str(nv).strip())
        else:
            acc_nature_list.append("")

        # PPHK + 渠道 DB：输出去掉分行维度中的连字符（DB-KR→DBKR、DB-TH→DBTH）
        ch_out = str(row.get("渠道", "") or "").strip()
        if str(ent).strip() == "PPHK" and ch_out.upper() == "DB" and br:
            sbr = str(br).strip()
            if "-" in sbr:
                br = sbr.replace("-", "")

        ent_list.append(ent)
        branch_list.append(br)
        type_list.append(typ)
        subj_list.append(subj)

    df = df.copy()
    df["USD金额"] = usd_list
    df["主体"] = ent_list
    df["分行维度"] = branch_list
    df["类型"] = type_list
    df["入账科目"] = subj_list
    df["账户性质"] = acc_nature_list
    lg(f"模版 enrich：完成（{len(records)} 行回填 USD/主体/分行/类型/入账科目）")
    return df


def default_template_path() -> Path:
    """保留给旧调用方；enrich 已只读 ``rules/files/mapping`` 与 ``rules/files/fx``，不再依赖整本模版 xlsx。"""
    try:
        from server.core.paths import get_rules_files_dir

        return get_rules_files_dir() / "mapping"
    except Exception:
        from pathlib import Path

        return Path()
