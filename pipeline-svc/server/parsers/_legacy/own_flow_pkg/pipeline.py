"""自有流水：扫描各渠道文件，全量汇总输出明细（规则命中时填充备注/入账科目/主体覆盖）。"""

from __future__ import annotations

import re
import threading
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from .classify import classify_citi_csv_name, ppeu_sheet_to_group
from .constants import CITI_RAW_KEYS, OUTPUT_COLUMNS
from .discovery import (
    iter_citi_csv_files,
    resolve_boc_path,
    resolve_bosh_path,
    resolve_deutsche_db_flow_dirs,
    resolve_dbs_flow_dirs,
    resolve_jpm_path,
    resolve_ppeu_workbook_path,
    resolve_rumg_csv_paths,
    resolve_scb_path,
)
from .loaders import (
    all_row_dicts,
    drop_boc_balance_column,
    normalize_columns,
    read_citi_csv,
    read_excel_first_sheet,
    read_jpm_details,
    read_ppeu_sheet,
    row_dict,
)
from .match import (
    _iter_column_values,
    first_matching_rule,
    first_matching_rule_db,
    first_matching_rule_dbs,
    row_matches_rule,
)
from .rules import OwnFlowRule, rules_for_file_group

# BOC：SW- 规则仅当 |原币金额|≤100 时命中成本（与处理表 bo1 搭配）
_BOC_SW_MAX_ABS_AMOUNT = 100.0
from .special_source_mapping import load_special_source_mapping
from .template_enrich import enrich_own_flow_dataframe

# 与页面「执行完整流水线」中写 Excel 共用，避免与另一入口交错。
pipeline_execution_lock = threading.RLock()

# 业务约定：下列 file_group 在写入前对 Transaction Amount 执行「取负」——即**符号取反**（x → -x；
# 例如原值为 -2 则变为 +2，原为 +5 则变为 -5）。与处理表自有流水列用语「取负」一致；
# **不是**「一律记成负数」或忽略原符号。
# 注意：ppeu_bc / ppeu_barclays 的金额符号已在 _ppeu_fill_blue_aliases 按 Debit± / Transaction± 处理，
# 若仍在此取反会导致双重取反（例如原 TA=-247.4 → 应为 +247.4，再取反会变成 -247.4）。
_NEGATE_TRANSACTION_AMOUNT_GROUPS = frozenset(
    {
        "citi_other",
        "ppbk_main",
        "ppbk_other",
        "ppeu_citi",
        "ppeu_bgl",
        "boc",
        "db",
    }
)


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or s.lower() in ("nan", "none", "null", "-"):
            return None
        s = s.replace(",", "")
        if s.startswith("(") and s.endswith(")"):
            s = "-" + s[1:-1].replace(",", "")
        for sym in ("$", "€", "£", "¥", "￥"):
            s = s.replace(sym, "")
        s = s.strip()
        if not s:
            return None
    else:
        if isinstance(v, float) and pd.isna(v):
            return None
        try:
            x = float(v)
        except (TypeError, ValueError, OverflowError):
            return None
        if x != x:
            return None
        return x
    try:
        x = float(s)  # type: ignore[has-type]
    except (ValueError, TypeError):
        return None
    if x != x:
        return None
    return x


def _negate_transaction_amount_if_applicable(amt: Any, file_group: str) -> Any:
    """若该 file_group 需「取负」，则对金额做符号取反（-amt）。"""
    if file_group not in _NEGATE_TRANSACTION_AMOUNT_GROUPS:
        return amt
    x = _as_float(amt)
    if x is None:
        return amt
    return -x


def _cell_nonempty(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    if isinstance(v, str) and not str(v).strip():
        return False
    return True


def _ppeu_get_by_aliases(row: dict[str, Any], aliases: tuple[str, ...]) -> Any | None:
    """按别名顺序取第一个非空（列名大小写不敏感）。"""
    idx = {str(k).strip().casefold(): k for k in row}
    for a in aliases:
        orig = idx.get(a.casefold())
        if orig is None:
            continue
        v = row.get(orig)
        if _cell_nonempty(v):
            return v
    return None


def _ppeu_account_fields(row: dict[str, Any]) -> dict[str, Any]:
    """PPEU：从各行读取 Account Number / Account / Merchant ID（列名别名）。

    汇总输出「Account Number」列时按子表约定取优先级（见 _coalesce_account_number）：
    CITI→Account Number；BGL→Account；Banking Circle / Barclays→MerchantId。
    """
    out: dict[str, Any] = {}
    v = _ppeu_get_by_aliases(
        row,
        (
            "account number",
            "账号",
            "帳號",
        ),
    )
    if v is not None:
        out["Account Number"] = v
    v = _ppeu_get_by_aliases(
        row,
        (
            "account",
            "account name",
            "账户",
            "帐户",
            "账户名称",
            "帳戶",
        ),
    )
    if v is not None:
        out["Account"] = v
    v = _ppeu_get_by_aliases(
        row,
        (
            "merchantid",
            "merchant id",
            "merchant_id",
            "商户id",
            "商户号",
        ),
    )
    if v is not None:
        out["Merchant ID"] = v
    return out


def _usd_amount_nonempty(v: Any) -> bool:
    """USD 金额有有效值（含 0）；空字符串 / NaN / None 为无。"""
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    if isinstance(v, str) and not str(v).strip():
        return False
    return True


# PPEU 各子表「账号」所在列不同，合并到输出列 Account Number 时的优先顺序
_PPEU_ACCOUNT_KEY_ORDER: dict[str, tuple[str, ...]] = {
    "ppeu_citi": ("Account Number", "Merchant ID", "Account"),
    "ppeu_bgl": ("Account", "Account Number", "Merchant ID"),
    "ppeu_bc": ("Merchant ID", "Account Number", "Account"),
    "ppeu_barclays": ("Merchant ID", "Account Number", "Account"),
}
_DEFAULT_ACCOUNT_KEY_ORDER = ("Account Number", "Merchant ID", "Account")


def _coalesce_account_number(blue: dict[str, Any], *, file_group: str = "") -> Any:
    """将蓝区账号字段合并为输出列 Account Number（非 PPEU 子表时默认先 Account Number）。"""
    order = _PPEU_ACCOUNT_KEY_ORDER.get(file_group, _DEFAULT_ACCOUNT_KEY_ORDER)
    for key in order:
        v = blue.get(key)
        if _cell_nonempty(v):
            return v
    return ""


def _fee_item_from_blue(blue: dict[str, Any]) -> Any:
    """模版规定：费项 = 即 description → 与蓝区 Transaction Description 一致。"""
    v = blue.get("Transaction Description")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return v


def _default_entity_for_unmatched(file_group: str, path: Path) -> str:
    """无规则命中时输出「主体」：按渠道/file_group 给默认，与处理表脱钩（全量汇总）。"""
    if file_group in ("ppbk_main", "ppbk_other"):
        return "PPHK"
    if file_group == "citi_other":
        return _entity_from_citi_other_path(path)
    if file_group == "ppeu_citi":
        return "PPEU"
    if file_group == "ppeu_bgl":
        return "BGL"
    if file_group == "ppeu_bc":
        return "Banking Circle"
    if file_group == "ppeu_barclays":
        return "Barclays"
    if file_group == "boc":
        return "PPUS"
    if file_group == "bosh":
        return "PPHK"
    if file_group == "rumg":
        return "PPJP"
    if file_group == "dbs":
        return ""
    return ""


def _entity_from_citi_other_path(path: Path) -> str:
    s = path.stem.upper()
    for token in (
        "PPGT",
        "PPUS",
        "PPDT",
        "PPJP",
        "PPEU",
        "PPHK",
        "PPHK-OTHER",
        "MANA",
        "BIG ROCKET",
        "FLC3",
        "LIGHTYEAR",
        "PP CAYMAN",
        "PP SEA",
        "PPESOP",
    ):
        if token.replace(" ", "") in s.replace(" ", ""):
            return token.split()[0] if "-" in token else token
    return "其他"


_sp_src_cache: dict[str, tuple[str, str]] | None = None


def _get_sp_src() -> dict[str, tuple[str, str]]:
    global _sp_src_cache
    if _sp_src_cache is None:
        _sp_src_cache = load_special_source_mapping()
    return _sp_src_cache


def _entity_for_unmatched_row(file_group: str, path: Path) -> str:
    sp = _get_sp_src()
    if file_group in sp:
        return sp[file_group][0]
    return _default_entity_for_unmatched(file_group, path)


def _branch_for_file_group(file_group: str) -> str:
    sp = _get_sp_src()
    if file_group in sp:
        return sp[file_group][1]
    if file_group == "boc":
        return "BOCUS"
    if file_group == "bosh":
        return "BOSH"
    return ""


def _default_channel_for_file_group(file_group: str) -> str:
    """无规则命中时输出「渠道」：与处理表常用取值一致，便于校验与汇总。"""
    if file_group in ("ppbk_main", "ppbk_other", "citi_other", "ppeu_citi"):
        return "CITI"
    if file_group == "ppeu_bgl":
        return "BGL"
    if file_group == "ppeu_bc":
        return "Banking Circle"
    if file_group == "ppeu_barclays":
        return "Barclays"
    if file_group == "boc":
        return "BOC"
    if file_group == "jpm":
        return "JPM"
    if file_group == "scb":
        return "SCB"
    if file_group == "bosh":
        return "BOSH"
    if file_group == "rumg":
        return "RUMG"
    if file_group == "dbs":
        return "DBS"
    if file_group in ("db", "db_csv"):
        return "DB"
    return ""


def _output_row(
    blue: dict[str, Any],
    path: Path,
    period: str,
    rule: OwnFlowRule | None,
    *,
    file_group: str = "",
) -> dict[str, Any]:
    if rule is not None:
        ent = rule.entity_override or rule.entity_default
        if ent == "*":
            ent = _entity_from_citi_other_path(path)
        remark = rule.remark
        acct_subj = rule.accounting_subject or ""
    else:
        ent = _entity_for_unmatched_row(file_group, path)
        remark = ""
        acct_subj = ""
    rec = {k: "" for k in OUTPUT_COLUMNS}
    for k in OUTPUT_COLUMNS:
        if k == "Account Number":
            rec[k] = _coalesce_account_number(blue, file_group=file_group)
        elif k in blue:
            rec[k] = blue[k]
    if "Transaction Amount" in rec:
        rec["Transaction Amount"] = _negate_transaction_amount_if_applicable(
            rec["Transaction Amount"], file_group
        )
    rec["来源文件"] = path.name
    rec["USD金额"] = ""
    rec["入账期间"] = period
    rec["主体"] = ent
    rec["渠道"] = (
        str(rule.channel).strip()
        if rule is not None and getattr(rule, "channel", None) and str(rule.channel).strip()
        else _default_channel_for_file_group(file_group)
    )
    # 若规则显式指定了 entity_override（例如 JPM PPUS EFT DEBIT → PPHK），
    # 需要锁定该主体，防止下游按"账户 mapping" 再覆盖回去。
    rec["__entity_locked"] = bool(rule is not None and rule.entity_override)
    # 规则若显式指定 branch_override（例如 JPM EFT DEBIT → JPMHK），分行维度直接以规则为准并锁定。
    if rule is not None and rule.branch_override:
        rec["分行维度"] = rule.branch_override
        rec["__branch_locked"] = True
    else:
        rec["分行维度"] = _branch_for_file_group(file_group)
        rec["__branch_locked"] = False
        if file_group in ("db", "db_csv"):
            db_r = _db_region_from_blue(blue)
            if db_r:
                rec["分行维度"] = db_r
                rec["__branch_locked"] = True
    rec["费项"] = _fee_item_from_blue(blue)
    rec["类型"] = ""
    rec["备注"] = remark
    rec["入账科目"] = acct_subj
    rec["__file_group"] = file_group
    rec["file_group"] = file_group
    if rule is not None:
        rec["matched_rule_kind"] = rule.kind
        rec["matched_rule_pattern"] = rule.pattern
    else:
        rec["matched_rule_kind"] = np.nan
        rec["matched_rule_pattern"] = np.nan
    return rec


def _citi_row_to_blue(row: dict[str, Any]) -> dict[str, Any]:
    blue: dict[str, Any] = {k: row.get(k) for k in CITI_RAW_KEYS}
    for v in _iter_column_values(row, "Name/Address"):
        if _cell_nonempty(v):
            blue["Name/Address"] = str(v).strip()
            break
    return blue


def process_rumg_csvs(root: Path, period: str) -> list[dict[str, Any]]:
    """
    三菱系全明細 .csv：根目录含 rumg 的文件，或 mufg+自有/流水 目录下递归的 csv（见 discovery.resolve_rumg_csv_paths）。
    """
    from .rumg import parse_rumg_statement, rumg_row_to_blue

    out: list[dict[str, Any]] = []
    root = root.expanduser().resolve()
    if not root.is_dir():
        return out
    for path in resolve_rumg_csv_paths(root):
        try:
            header, data_rows = parse_rumg_statement(path)
        except Exception:
            continue
        if not header or not data_rows:
            continue
        rules = rules_for_file_group("rumg")
        for r in data_rows:
            blue = rumg_row_to_blue(header, r)
            rd = dict(blue)
            rule = first_matching_rule(rd, rules) if rules else None
            out.append(_output_row(blue, path, period, rule, file_group="rumg"))
    return out


def process_citi_csvs(root: Path, period: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    paths = iter_citi_csv_files(root)
    if not paths:
        return out
    for path in paths:
        fg = classify_citi_csv_name(path)
        rules = rules_for_file_group(fg)
        try:
            df = read_citi_csv(path)
        except Exception:
            continue
        for rd in all_row_dicts(df):
            rule = first_matching_rule(rd, rules) if rules else None
            blue = _citi_row_to_blue(rd)
            out.append(_output_row(blue, path, period, rule, file_group=fg))
    return out


def process_ppeu_workbook(path: Path, period: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    path = path.expanduser().resolve()
    if not path.exists():
        return out
    xl = pd.ExcelFile(path, engine="openpyxl")
    for sheet in xl.sheet_names:
        # 仅 CITI / BGL / Banking Circle / Barclays（见 classify.ppeu_sheet_to_group）
        fg = ppeu_sheet_to_group(sheet)
        if not fg:
            continue
        rules = rules_for_file_group(fg)
        try:
            df = read_ppeu_sheet(path, sheet, header=13)
        except Exception:
            continue
        if "mark（财务）" not in df.columns:
            continue
        for rd in all_row_dicts(df):
            rule = first_matching_rule(rd, rules) if rules else None
            blue = {k: rd.get(k) for k in CITI_RAW_KEYS}
            blue["mark（财务）"] = rd.get("mark（财务）", "")
            if not blue.get("Branch Name"):
                blue["Branch Name"] = rd.get("Channel")
            for k, v in _ppeu_account_fields(rd).items():
                blue[k] = v
            _ppeu_fill_blue_aliases(rd, blue, fg)
            out.append(_output_row(blue, path, period, rule, file_group=fg))
    return out


def _jpm_to_blue(row: dict[str, Any]) -> dict[str, Any]:
    """Debit Amount 取正，Credit Amount 取负（绝对值带符号）。"""
    debit = _as_float(row.get("Debit Amount"))
    credit = _as_float(row.get("Credit Amount"))
    if debit is not None and debit != 0:
        amt: Any = abs(debit)
    elif credit is not None and credit != 0:
        amt = -abs(credit)
    else:
        amt = np.nan
    return {
        "Branch Name": row.get("Bank Name"),
        "Account Number": row.get("Account Number"),
        "Account": row.get("Account Name") or row.get("Account"),
        "Merchant ID": row.get("Merchant ID"),
        "Account Currency": row.get("Currency"),
        "Last Entry Date": row.get("Transaction Date"),
        "Transaction Amount": amt,
        "Product Type": "",
        "Transaction Description": row.get("Description") or row.get("Transaction Description"),
        "Payment Details": row.get("Payment Details", ""),
    }


def process_jpm(path: Path, period: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rules = rules_for_file_group("jpm")
    try:
        df = read_jpm_details(path)
    except Exception:
        return out
    for rd in all_row_dicts(df):
        rd["Description"] = rd.get("Description") or rd.get("Transaction Description")
        rule = first_matching_rule(rd, rules) if rules else None
        blue = _jpm_to_blue(rd)
        out.append(_output_row(blue, path, period, rule, file_group="jpm"))
    return out


def _normalize_boc_row_dict(rd: dict[str, Any]) -> dict[str, Any]:
    """统一 Description 等列名，便于处理表 istartswith（ACH/SW）命中。"""
    rd = dict(rd)
    idx = {str(k).strip().casefold(): k for k in rd}

    def get_ci(*names: str):
        for n in names:
            k = idx.get(n.casefold())
            if k is None:
                continue
            v = rd.get(k)
            if _cell_nonempty(v):
                return v
        return None

    desc = get_ci("description", "描述", "transaction description", "narration", "memo")
    if desc is not None:
        rd["Description"] = desc
    return rd


def _boc_row_abs_amount(rd: dict[str, Any]) -> float | None:
    """用于 SW- 金额阈值：取借/贷绝对值或 Transaction Amount。"""
    d = _as_float(rd.get("Debit"))
    c = _as_float(rd.get("Credit"))
    if d is not None and abs(d) > 1e-12:
        return abs(d)
    if c is not None and abs(c) > 1e-12:
        return abs(c)
    return _as_float(rd.get("Transaction Amount"))


def _boc_first_matching_rule(rd: dict[str, Any], rules: list[OwnFlowRule]) -> OwnFlowRule | None:
    """BOC：SW- 规则额外要求 |金额|≤100；其余规则不变。"""
    for rule in sorted(rules, key=lambda r: r.priority):
        if not row_matches_rule(rd, rule):
            continue
        pat = (rule.pattern or "").strip().lower()
        if rule.column == "Description" and rule.kind == "istartswith" and pat.startswith("sw-"):
            amt = _boc_row_abs_amount(rd)
            if amt is None or abs(amt) > _BOC_SW_MAX_ABS_AMOUNT:
                continue
        return rule
    return None


def _boc_to_blue(row: dict[str, Any]) -> dict[str, Any]:
    """BOC：优先 Debit / Credit（支行模板里常为带符号的支出/入账）；没有再回退 Amount。

    若先读 Amount：部分文件中 Amount 仅为正数额度、Debit 才为负数，会先得到 +TA，
    经 ``_negate_transaction_amount_if_applicable``（boc）取反后在汇总里会变成负数，看起来像「没有取负」。
    Debit/Credit 常带 $ 与千分位，由 ``_as_float`` 解析。
    """
    debit = _as_float(row.get("Debit"))
    credit = _as_float(row.get("Credit"))
    ta: float | None = None
    if debit is not None and abs(debit) > 1e-12:
        ta = debit
    elif credit is not None and abs(credit) > 1e-12:
        ta = credit
    else:
        ta = _as_float(row.get("Amount"))
    ccy = row.get("Account Currency") or row.get("Currency")
    return {
        "Branch Name": "",
        "Account Number": row.get("Account Number"),
        "Account": row.get("Account"),
        "Merchant ID": row.get("Merchant ID"),
        "Account Currency": ccy if ccy is not None and str(ccy).strip() else "",
        "Last Entry Date": row.get("Value Date"),
        "Transaction Amount": ta,
        "Product Type": row.get("Type"),
        "Transaction Description": row.get("Description"),
        "Payment Details": "",
    }


def _ppeu_fill_blue_aliases(rd: dict[str, Any], blue: dict[str, Any], file_group: str) -> None:
    """PPEU：源表列名常为 Currency / Transaction / Entry Date（与 CITI 标准列名不一致时补齐）。"""
    idx = {str(k).strip().casefold(): k for k in rd}

    def get_ci(*names: str):
        for n in names:
            k = idx.get(n.casefold())
            if k is None:
                continue
            v = rd.get(k)
            if _cell_nonempty(v):
                return v
        return None

    ccy = get_ci("account currency", "currency", "currency code", "curr", "ccy", "货币")
    if ccy is not None:
        blue["Account Currency"] = ccy
    if file_group == "ppeu_barclays":
        # Barclays 源表：业务类型在 Transaction Type（如 Charges and Other Expenses），
        # 长叙述在 Transaction Details；勿将 Transaction Details 误填进 Transaction Description。
        tt = get_ci("transaction type")
        if tt is not None:
            blue["Transaction Description"] = str(tt).strip()
        tdet = get_ci("transaction details")
        if tdet is not None:
            blue["Payment Details"] = str(tdet).strip()
    else:
        td = get_ci(
            "transaction description",
            "description",
            "transaction details",
            "transaction",
            "narrative",
            "customer ref",
            "bank refer",
            "customerref",
        )
        if td is not None:
            blue["Transaction Description"] = str(td).strip()
    ld = get_ci("last entry date", "entry date", "post date", "value date", "transaction date")
    if ld is not None:
        blue["Last Entry Date"] = ld
    if file_group != "ppeu_barclays":
        payment_details = get_ci("payment details", "extra information", "extra inform", "payment or narrative")
        if payment_details is not None and _cell_nonempty(payment_details):
            blue["Payment Details"] = str(payment_details).strip()

    if file_group == "ppeu_bgl":
        debit = _as_float(rd.get("Debit Amount"))
        credit = _as_float(rd.get("Credit Amount"))
        ta = _as_float(rd.get("Transaction Amount"))
        if debit is not None and abs(debit) > 1e-12:
            blue["Transaction Amount"] = abs(debit)
        elif credit is not None and abs(credit) > 1e-12:
            blue["Transaction Amount"] = -abs(credit)
        elif ta is not None:
            blue["Transaction Amount"] = ta
    elif file_group in ("ppeu_bc", "ppeu_barclays"):
        # BC / Barclays 模版：Debit Amount=对原金额一元「+」、Transaction Amount=一元「-」（非取绝对值）。
        # 输出统一写入 blue「Transaction Amount」列，对应模版 Transaction Amount 列语义 → -x。
        ta = _as_float(rd.get("Transaction Amount"))
        if ta is not None:
            blue["Transaction Amount"] = -ta
        else:
            debit = _as_float(rd.get("Debit Amount"))
            credit = _as_float(rd.get("Credit Amount"))
            if debit is not None and abs(debit) > 1e-12:
                blue["Transaction Amount"] = -debit
            elif credit is not None and abs(credit) > 1e-12:
                blue["Transaction Amount"] = -credit


def process_boc(path: Path, period: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rules = rules_for_file_group("boc")
    df = drop_boc_balance_column(read_excel_first_sheet(path, header=0))
    for rd in all_row_dicts(df):
        rd = _normalize_boc_row_dict(rd)
        rule = _boc_first_matching_rule(rd, rules) if rules else None
        blue = _boc_to_blue(rd)
        out.append(_output_row(blue, path, period, rule, file_group="boc"))
    return out


def _scb_signed_amount(amt: Any, flag: Any) -> float | None:
    """SCB 源数据 Transaction Amount 为无符号绝对值，方向写在 Debit/Credit Flag 列：
    - D（借方=付出）→ +abs(amt) 视作成本
    - C（贷方=入账）→ -abs(amt) 视作收入/非成本
    与映射表「交易金额取正（按借贷方向签名）」及 JPM/DBS/BOSH 的口径保持一致。
    """
    x = _as_float(amt)
    if x is None:
        return None
    f = str(flag).strip().upper() if flag is not None else ""
    if f.startswith("D"):
        return abs(x)
    if f.startswith("C"):
        return -abs(x)
    return x


def _scb_to_blue(row: dict[str, Any]) -> dict[str, Any]:
    amt = _scb_signed_amount(
        row.get("Transaction Amount"),
        row.get("Debit/Credit Flag") or row.get("Debit Credit Flag") or row.get("DC Flag"),
    )
    return {
        "Branch Name": "",
        "Account Number": row.get("Account Number"),
        "Account": row.get("Account"),
        "Merchant ID": row.get("Merchant ID"),
        "Account Currency": row.get("Account Currency"),
        "Last Entry Date": row.get("Post Date") or row.get("Value Date"),
        "Transaction Amount": amt if amt is not None else row.get("Transaction Amount"),
        "Product Type": row.get("Transaction Type"),
        "Transaction Description": row.get("Transaction Description"),
        "Payment Details": "",
    }


def process_scb(path: Path, period: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rules = rules_for_file_group("scb")
    df = read_excel_first_sheet(path, header=0)
    for rd in all_row_dicts(df):
        # 过滤无金额行（SCB 源表常含大量仅头信息的空行）
        if _as_float(rd.get("Transaction Amount")) is None:
            continue
        rule = first_matching_rule(rd, rules) if rules else None
        blue = _scb_to_blue(rd)
        out.append(_output_row(blue, path, period, rule, file_group="scb"))
    return out


def process_bosh(path: Path, period: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rules = rules_for_file_group("bosh")
    df = pd.read_excel(path, sheet_name=0, header=5, engine="openpyxl")
    df = normalize_columns(df)
    if "Transaction Description" not in df.columns and "摘要" in df.columns:
        df = df.rename(columns={"摘要": "Transaction Description"})
    for rd in all_row_dicts(df):
        rule = first_matching_rule(rd, rules) if rules else None
        dr = _as_float(rd.get("借方发生额"))
        cr = _as_float(rd.get("贷方发生额"))
        if dr is not None and dr != 0:
            bosh_amt: Any = abs(dr)
        elif cr is not None and cr != 0:
            bosh_amt = -abs(cr)
        else:
            bosh_amt = np.nan
        blue = {
            "Branch Name": "",
            "Account Number": rd.get("Account Number") or rd.get("账号") or rd.get("帐户"),
            "Account": rd.get("Account") or rd.get("账户名称"),
            "Merchant ID": rd.get("Merchant ID") or rd.get("商户号"),
            "Account Currency": "",
            "Last Entry Date": rd.get("记账日期"),
            "Transaction Amount": bosh_amt,
            "Product Type": "",
            "Transaction Description": rd.get("Transaction Description") or rd.get("摘要"),
            "Payment Details": rd.get("备注") or rd.get("交易用途"),
        }
        out.append(_output_row(blue, path, period, rule, file_group="bosh"))
    return out


# ────────────────────────── DB 流水（日报 CSV） ──────────────────────────

_DB_ENTITY_MAP: dict[str, str] = {
    "PPHK": "PPHK",
    "PPUS": "PPUS",
    "PPGT": "PPGT",
    "PPI": "PPI",
    "BRSG": "BRSG",
    "PPEU": "PPEU",
}


def _db_region_from_blue(blue: dict[str, Any]) -> str:
    """由 DB 日报行推断分行维度，与账户 mapping「支行简称」一致：DB-HK / DB-KR / DB-TH。"""
    bb = str(blue.get("Bank/Branch", "") or "").upper()
    swift = str(
        blue.get("Bank/Branch SWIFT Code", "") or blue.get("Bank Key", "") or ""
    ).upper()
    blob = f"{bb} {swift}"
    if "DBHONGKONG" in blob or "DEUTHKH" in blob:
        return "DB-HK"
    if "DBSEOUL" in blob or "DEUTKRS" in blob:
        return "DB-KR"
    if "DBBANGKOK" in blob or "DEUTTHB" in blob:
        return "DB-TH"
    return ""


def _db_csv_to_blue(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "Branch Name": row.get("Bank/Branch", ""),
        "Account Number": row.get("Account Number", ""),
        "Account": row.get("Account Name", ""),
        "Merchant ID": "",
        "Account Currency": row.get("Account Currency", ""),
        "Last Entry Date": row.get("Closing Book Date", ""),
        "Transaction Amount": row.get("Sum of Transaction amount", ""),
        "Product Type": "",
        "Transaction Description": row.get("Company Long Name", ""),
        "Payment Details": "",
        # 规则匹配 / 分行维度：与 match._DB_REGION_HINTS 一致
        "Bank/Branch": row.get("Bank/Branch", ""),
        "Bank Key": row.get("Bank Key", ""),
        "Bank/Branch SWIFT Code": row.get("Bank/Branch SWIFT Code", ""),
    }


def process_db_csvs(root: Path, period: str) -> list[dict[str, Any]]:
    """扫描 root 下德意志 DB *DB流水*/ 子目录，按主体子目录读取日报 CSV。"""
    out: list[dict[str, Any]] = []
    db_dirs = resolve_deutsche_db_flow_dirs(root)
    if not db_dirs:
        return out
    rules = rules_for_file_group("db")

    for db_dir in db_dirs:
        for entity_dir in sorted(db_dir.iterdir()):
            if not entity_dir.is_dir():
                continue
            entity = _DB_ENTITY_MAP.get(entity_dir.name.upper(), entity_dir.name)
            csvs = sorted(entity_dir.glob("*.csv"))
            for csv_path in csvs:
                try:
                    df = pd.read_csv(csv_path, encoding="utf-8-sig")
                except Exception:
                    try:
                        df = pd.read_csv(csv_path, encoding="utf-8")
                    except Exception:
                        continue
                df.columns = [str(c).strip() for c in df.columns]
                if "Sum of Transaction amount" not in df.columns:
                    continue
                for rd in all_row_dicts(df):
                    amt = _as_float(rd.get("Sum of Transaction amount"))
                    if amt is None or amt == 0:
                        continue
                    blue = _db_csv_to_blue(rd)
                    match_row = {**rd, **blue}
                    rule = (
                        first_matching_rule_db(match_row, rules, entity) if rules else None
                    )
                    rec = _output_row(blue, csv_path, period, rule, file_group="db")
                    rec["主体"] = entity
                    out.append(rec)
    return out


# ────────────────────────── DBS 星展（对账单 xls/xlsx） ──────────────────────────


def _dbs_ccy_from_filename(name: str) -> str:
    m = re.search(r"_([A-Z]{3})_", name.upper().replace(" ", ""))
    return m.group(1) if m else ""


def _dbs_extract_account_number_from_banner(s: str) -> str:
    m = re.search(r"(\d{6,})", s)
    return m.group(1) if m else ""


def _dbs_account_title_from_banner(s: str) -> str:
    s = str(s).strip()
    u = s.upper()
    if " - HOUSE " in u:
        return s[: u.index(" - HOUSE ")].strip()
    return s


def _dbs_signed_amount(debit: Any, credit: Any) -> float | None:
    """Debit 列取正，Credit 列取负（与模版映射一致）。"""
    d = _as_float(debit)
    c = _as_float(credit)
    if d is not None and abs(d) > 1e-12 and c is not None and abs(c) > 1e-12:
        return abs(d) - abs(c)
    if d is not None and abs(d) > 1e-12:
        return abs(d)
    if c is not None and abs(c) > 1e-12:
        return -abs(c)
    return None


def _dbs_sheet_to_blues(df: pd.DataFrame, path: Path) -> list[dict[str, Any]]:
    """解析 DBS 导出：表头行为 Date / Value Date / … / Debit / Credit / Running Balance，明细在表头之下。"""
    blues: list[dict[str, Any]] = []
    acct = ""
    title = ""
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        if len(row) < 2:
            continue
        s0 = str(row.iloc[0]).strip().lower()
        if "account details" in s0:
            banner = str(row.iloc[1])
            acct = _dbs_extract_account_number_from_banner(banner)
            title = _dbs_account_title_from_banner(banner)
            break

    header_idx: int | None = None
    for i in range(len(df)):
        c0 = str(df.iat[i, 0]).strip().lower() if len(df.columns) else ""
        if c0 == "date":
            header_idx = i
            break
    if header_idx is None:
        return blues

    hdr = df.iloc[header_idx]
    col_names = [str(hdr.iloc[j]).strip() if j < len(hdr) and pd.notna(hdr.iloc[j]) else "" for j in range(len(hdr))]

    j_date = next((j for j, n in enumerate(col_names) if n.lower() == "date"), None)
    j_val = next((j for j, n in enumerate(col_names) if "value date" in n.lower()), None)
    j_deb = next((j for j, n in enumerate(col_names) if n.lower() == "debit"), None)
    j_cred = next((j for j, n in enumerate(col_names) if n.lower() == "credit"), None)
    j_td = [j for j, n in enumerate(col_names) if "transaction description" in n.lower()]
    if j_deb is None or j_cred is None:
        return blues

    ccy = _dbs_ccy_from_filename(path.name)

    for i in range(header_idx + 1, len(df)):
        row = df.iloc[i]
        c0 = str(row.iloc[0]).strip() if len(row) else ""
        if not c0 or c0.lower().startswith("printed"):
            break
        debit = row.iloc[j_deb] if j_deb < len(row) else None
        credit = row.iloc[j_cred] if j_cred < len(row) else None
        amt = _dbs_signed_amount(debit, credit)
        if amt is None:
            continue
        parts: list[str] = []
        for j in j_td:
            if j < len(row):
                v = row.iloc[j]
                if _cell_nonempty(v):
                    parts.append(str(v).strip())
        desc = " ".join(parts)
        last_dt = ""
        if j_val is not None and j_val < len(row) and _cell_nonempty(row.iloc[j_val]):
            last_dt = str(row.iloc[j_val]).strip()
        elif j_date is not None and j_date < len(row) and _cell_nonempty(row.iloc[j_date]):
            last_dt = str(row.iloc[j_date]).strip()

        blues.append(
            {
                "Branch Name": "DBS",
                "Account Number": acct,
                "Account": title,
                "Merchant ID": "",
                "Account Currency": ccy,
                "Last Entry Date": last_dt,
                "Transaction Amount": amt,
                "Product Type": "",
                "Transaction Description": desc,
                "Payment Details": "",
            }
        )
    return blues


def process_dbs_workbooks(root: Path, period: str) -> list[dict[str, Any]]:
    """扫描 root 下 *DBS流水*/ 中的 .xls / .xlsx，解析明细并入标准列（file_group=dbs，金额不再取反）。"""
    from .template_enrich import _load_account_lookup, _lookup_account_row_dict

    out: list[dict[str, Any]] = []
    dbs_dirs = resolve_dbs_flow_dirs(root)
    acc_map = _load_account_lookup()
    rules = rules_for_file_group("dbs")
    for ddir in dbs_dirs:
        paths = sorted(ddir.glob("*.xls")) + sorted(ddir.glob("*.xlsx"))
        for path in paths:
            try:
                eng = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
                raw = pd.read_excel(path, sheet_name=0, header=None, engine=eng)
            except Exception:
                continue
            for blue in _dbs_sheet_to_blues(raw, path):
                rd = dict(blue)
                info = _lookup_account_row_dict(rd, acc_map)
                ent_resolved = ""
                if info:
                    e1 = info.get("主体1")
                    if e1 is not None and not (isinstance(e1, float) and pd.isna(e1)) and str(e1).strip():
                        ent_resolved = str(e1).strip()
                rule = first_matching_rule_dbs(rd, rules, ent_resolved) if rules else None
                out.append(_output_row(blue, path, period, rule, file_group="dbs"))
    return out


def run_pipeline(
    root: Path,
    period: str,
    *,
    progress_log: Callable[[str], None] | None = None,
) -> pd.DataFrame:
    """汇总自有流水各渠道；``progress_log`` 写入可读的阶段日志（任务/channel 面板）。"""
    def lg(msg: str) -> None:
        if progress_log:
            progress_log(msg)

    global _sp_src_cache
    _sp_src_cache = None
    root = root.expanduser().resolve()
    from .rules import all_rules

    lg(f"自有流水：入账期间={period}，处理表规则条数={len(all_rules())}")
    lg("自有流水：扫原始明细 …")

    rows: list[dict[str, Any]] = []
    n = len(rows)
    rows.extend(process_citi_csvs(root, period))
    lg(f"  · CITI CSV：+{len(rows) - n} 行")
    n = len(rows)
    rows.extend(process_rumg_csvs(root, period))
    lg(f"  · RUMG：+{len(rows) - n} 行")

    ppeu = resolve_ppeu_workbook_path(root)
    if ppeu is not None and ppeu.exists():
        n = len(rows)
        rows.extend(process_ppeu_workbook(ppeu, period))
        lg(f"  · PPEU（{ppeu.name}）：+{len(rows) - n} 行")
    else:
        lg("  · PPEU：未发现")

    jpm = resolve_jpm_path(root)
    if jpm is not None and jpm.exists():
        n = len(rows)
        rows.extend(process_jpm(jpm, period))
        lg(f"  · JPM：+{len(rows) - n} 行")
    else:
        lg("  · JPM：未发现")

    boc = resolve_boc_path(root)
    if boc is not None and boc.exists():
        n = len(rows)
        rows.extend(process_boc(boc, period))
        lg(f"  · BOC：+{len(rows) - n} 行")
    else:
        lg("  · BOC：未发现")

    scb = resolve_scb_path(root)
    if scb is not None and scb.exists():
        n = len(rows)
        rows.extend(process_scb(scb, period))
        lg(f"  · SCB（{scb.name}）：+{len(rows) - n} 行")
    else:
        lg("  · SCB：未发现")

    bosh = resolve_bosh_path(root)
    if bosh is not None and bosh.exists():
        n = len(rows)
        rows.extend(process_bosh(bosh, period))
        lg(f"  · BO SH：+{len(rows) - n} 行")
    else:
        lg("  · BO SH：未发现")

    n = len(rows)
    rows.extend(process_db_csvs(root, period))
    lg(f"  · DB 日报 CSV：+{len(rows) - n} 行")
    n = len(rows)
    rows.extend(process_dbs_workbooks(root, period))
    lg(f"  · DBS 流水：+{len(rows) - n} 行")

    if not rows:
        lg("自有流水：无明细行，跳过模版 enrich")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    lg(f"自有流水：原始明细合计 {len(rows)} 行，进入模版 enrich（汇率 / 账户 mapping / 费项打标）…")
    df = pd.DataFrame(rows)
    df = enrich_own_flow_dataframe(df, progress_log=progress_log)
    if "__file_group" in df.columns:
        df = df.drop(columns=["__file_group"])
    if "USD金额" in df.columns:
        before_usd = len(df)
        df = df.loc[df["USD金额"].map(_usd_amount_nonempty)].reset_index(drop=True)
        lg(f"自有流水：筛除无 USD 金额行 {before_usd - len(df)} 条，保留 {len(df)} 行")
    lg("自有流水：汇总完成")
    return df[OUTPUT_COLUMNS]
