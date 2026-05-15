"""用 ``PIPELINE_DATA_DIR/rules/files`` 侧车表填充账单绿区。

汇率、账户 mapping、费项 mapping 均从前端上传后同步到 rules/files 的 CSV 读取；
不再依赖随包 ``模版.xlsx``。
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from server.core.paths import get_rules_files_dir
from server.parsers._legacy.own_flow_pkg.mapping_import import normalize_fx_rate_value

TEMPLATE_COLUMNS = [
    "Account",
    "Description",
    "Pricing Method",
    "Volume",
    "Unit\nPrice",
    "Unit Price CCY",
    "Charge in Invoice CCY",
    "Invoice CCY",
    "汇率",
    "Taxable",
    "TAX",
    "来源文件",
    "USD金额",
    "入账期间",
    "主体",
    "分行维度",
    "费项",
    "类型",
    "入账科目",
]


def _mapping_dir() -> Path:
    return get_rules_files_dir() / "mapping"


def _fx_path() -> Path:
    return get_rules_files_dir() / "fx" / "各种货币对美元折算率.csv"


def _load_rate_dict_from_fx_csv() -> Dict[str, float]:
    p = _fx_path()
    if not p.exists():
        raise FileNotFoundError(f"缺少汇率文件: {p}")
    df = pd.read_csv(p, encoding="utf-8-sig").dropna(how="all")

    # 多月数据时按 fx_month_label 指定月份过滤，避免最新月覆盖目标月
    if "日期" in df.columns:
        preferred_ym: str | None = None
        try:
            from server.rules.store import get_fx_preferred_yyyymm
            preferred_ym = get_fx_preferred_yyyymm()
        except Exception:
            pass

        def _to_yyyymm(v: object) -> str:
            try:
                if pd.isna(v):  # type: ignore[arg-type]
                    return ""
            except Exception:
                pass
            try:
                return str(int(float(str(v).strip())))
            except Exception:
                return str(v).strip()

        month_vals = df["日期"].apply(_to_yyyymm)
        unique_months = [m for m in month_vals.unique() if len(m) == 6 and m.isdigit()]
        if unique_months:
            target = preferred_ym if (preferred_ym and preferred_ym in unique_months) else max(unique_months)
            df = df[month_vals == target].copy()

    col_rate = None
    for name in ("兑USD汇率", "对美元折算率"):
        if name in df.columns:
            col_rate = name
            break
    if col_rate is None:
        for c in df.columns:
            if "折算" in str(c) or "USD" in str(c).upper():
                col_rate = str(c)
                break
    if col_rate is None:
        raise ValueError("汇率 CSV 缺少可识别的汇率列")
    out: Dict[str, float] = {"USD": 1.0}
    for _, row in df.iterrows():
        rv = normalize_fx_rate_value(row.get(col_rate))
        if rv is None or float(rv) == 0:
            continue
        if "货币代码" in df.columns:
            c = row.get("货币代码")
            if pd.notna(c) and str(c).strip():
                out[str(c).strip().upper()] = float(rv)
        if "货币名称" in df.columns:
            n = row.get("货币名称")
            if pd.notna(n) and str(n).strip():
                out[str(n).strip().upper()] = float(rv)
    return out


# 不含标准银行账号的特殊通道银行（BARCLAYS、EWB、XENDIT、IBC、MONOOVA）
# 可在 rules/files/mapping/账单特殊银行mapping.csv 中配置：bank_key,主体,分行维度
# CSV 存在时优先使用，不存在时使用下方内置默认值。
_SPECIAL_BANK_CSV = "账单特殊银行mapping.csv"

# 内置默认值（与 master 答案保持一致）；CSV 可覆盖这些值
_DEFAULT_SPECIAL_BANK_ENTITY: Dict[str, str] = {
    "BARCLAYS": "PPEU",
    "EWB": "PPUS",
    "IBC": "PPUS",
    "MONOOVA": "Mana AU",
    "XENDIT": "MANA-ID",
}
_DEFAULT_SPECIAL_BANK_BRANCH: Dict[str, str] = {
    "BARCLAYS": "Barclays",
    "EWB": "EWB-US",
    "IBC": "IBC",
    "MONOOVA": "MONOOVA",
    "XENDIT": "Xendit-ID",
}


def _load_special_bank_maps() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """加载特殊通道银行→主体/分行维度映射。

    先以内置默认值初始化，再用 ``rules/files/mapping/账单特殊银行mapping.csv`` 覆盖
    （CSV 不存在或读取失败时内置默认值仍生效）。

    CSV 格式（UTF-8-sig，三列）::

        bank_key,主体,分行维度
        BARCLAYS,PPEU,Barclays
        EWB,PPUS,EWB-US
        ...
    """
    # 从内置默认值出发
    ent: Dict[str, Any] = dict(_DEFAULT_SPECIAL_BANK_ENTITY)
    bra: Dict[str, Any] = dict(_DEFAULT_SPECIAL_BANK_BRANCH)

    p = _mapping_dir() / _SPECIAL_BANK_CSV
    if not p.exists():
        return ent, bra
    try:
        df = pd.read_csv(p, encoding="utf-8-sig").dropna(how="all")
        for _, row in df.iterrows():
            key = str(row.get("bank_key", "") or "").strip().upper()
            if not key:
                continue
            ent[key] = row.get("主体") if pd.notna(row.get("主体")) else None
            bra[key] = row.get("分行维度") if pd.notna(row.get("分行维度")) else None
    except Exception:
        pass
    return ent, bra


def _load_account_maps() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    p = _mapping_dir() / "账户对应主体分行mapping表.csv"
    if not p.exists():
        raise FileNotFoundError(f"缺少账户 mapping: {p.name}")
    df = pd.read_csv(p, encoding="utf-8-sig").dropna(how="all")
    if "银行账号" not in df.columns:
        raise ValueError("账户 mapping CSV 缺少列「银行账号」")
    ent_col = "主体1" if "主体1" in df.columns else None
    br_col = "支行简称" if "支行简称" in df.columns else None
    if ent_col is None or br_col is None:
        raise ValueError("账户 mapping CSV 缺少「主体1」或「支行简称」")
    acc_to_entity: Dict[str, Any] = {}
    acc_to_branch: Dict[str, Any] = {}
    for _, row in df.iterrows():
        acc_val = str(row["银行账号"]).strip()
        if not acc_val or acc_val.lower() == "nan":
            continue
        acc_to_entity[acc_val] = row[ent_col] if pd.notna(row[ent_col]) else None
        acc_to_branch[acc_val] = row[br_col] if pd.notna(row[br_col]) else None
    return acc_to_entity, acc_to_branch


def _load_fee_maps() -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    p = _mapping_dir() / "账单及自有流水费项mapping表.csv"
    if not p.exists():
        raise FileNotFoundError(f"缺少费项 mapping: {p.name}")
    df = pd.read_csv(p, encoding="utf-8-sig").dropna(how="all")
    fee_to_type: Dict[str, Any] = {}
    fee_to_type_by_entity: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        if len(row) <= 10:
            continue
        entity_name = re.sub(r"\s+", " ", str(row.iloc[1])).strip()
        fee_name = re.sub(r"\s+", " ", str(row.iloc[8])).strip()
        fee_type = row.iloc[10]
        if fee_name and fee_name != "nan" and pd.notna(fee_type):
            fee_to_type[fee_name] = fee_type
            if entity_name and entity_name != "nan":
                ent = entity_name.strip().upper()
                fee_to_type_by_entity.setdefault(ent, {})[fee_name] = fee_type
    return fee_to_type, fee_to_type_by_entity


def enrich_bill_final_dataframe(final_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """对已通过 ``align_columns`` 汇总后的 ``final_df`` 写入绿区列并重排 ``TEMPLATE_COLUMNS``。

    返回 ``(df, warnings)``；warnings 含 Invoice CCY 在折算率表中无匹配时的提示。
    """
    rate_dict = _load_rate_dict_from_fx_csv()
    acc_to_entity, acc_to_branch = _load_account_maps()
    fee_to_type, fee_to_type_by_entity = _load_fee_maps()
    special_mapping_entity, special_mapping_branch = _load_special_bank_maps()

    def match_acc(acc: Any, mapping: Dict[str, Any]) -> Any:
        acc_str = str(acc).strip()
        if acc_str in mapping:
            return mapping[acc_str]
        acc_no_zero = acc_str.lstrip("0")
        if acc_no_zero in mapping:
            return mapping[acc_no_zero]
        acc_no_space = acc_str.replace(" ", "")
        for k, v in mapping.items():
            k_str = str(k)
            if k_str.replace(" ", "") == acc_no_space:
                return v
        for k, v in mapping.items():
            k_str = str(k)
            if k_str and (k_str in acc_str or acc_str in k_str):
                return v
        for k, v in mapping.items():
            k_str = str(k).replace(" ", "")
            if k_str and (k_str in acc_no_space or acc_no_space in k_str):
                return v
        return None

    def get_entity(row: pd.Series) -> Any:
        bank = row.get("__Bank__")
        if bank in special_mapping_entity:
            return special_mapping_entity[bank]
        return match_acc(row["Account"], acc_to_entity)

    def get_branch(row: pd.Series) -> Any:
        bank = row.get("__Bank__")
        if bank in special_mapping_branch:
            return special_mapping_branch[bank]
        return match_acc(row["Account"], acc_to_branch)

    work = final_df.copy()
    work["主体"] = work.apply(get_entity, axis=1)
    work["分行维度"] = work.apply(get_branch, axis=1)

    missing_invoice_ccy: set[str] = set()

    def get_tax(row: pd.Series) -> float:
        charge = row["Charge in Invoice CCY"]
        if pd.isna(charge):
            return 0.0
        branch = str(row["分行维度"]).strip().upper()
        taxable = str(row["Taxable"]).strip().upper()
        bank = str(row.get("__Bank__", "")).strip().upper()
        if bank == "CITI" and branch == "CITIMX" and taxable == "Y":
            return float(charge) * 0.16
        if bank == "XENDIT":
            return float(charge) * (11 / 12) * 0.12
        return 0.0

    work["TAX"] = work.apply(get_tax, axis=1)

    def calc_usd(row: pd.Series) -> Any:
        ccy = str(row["Invoice CCY"]).strip().upper()
        charge = row["Charge in Invoice CCY"]
        tax = row["TAX"]
        if pd.isna(charge):
            return None
        bank = str(row.get("__Bank__", "")).strip().upper()
        rate = None
        if bank == "SCB":
            fx = row.get("汇率")
            if pd.notna(fx):
                try:
                    rate = float(fx)
                except (TypeError, ValueError):
                    rate = None
        if rate is None:
            rate = rate_dict.get(ccy)
        if rate is not None:
            try:
                return (float(charge) + float(tax)) * rate
            except (TypeError, ValueError):
                pass
        if ccy and ccy not in ("USD", "", "NAN"):
            missing_invoice_ccy.add(ccy)
        return None

    work["USD金额"] = work.apply(calc_usd, axis=1)
    work["费项"] = work["Description"]

    def match_type_row(row: pd.Series) -> Any:
        fee_str = re.sub(r"\s+", " ", str(row.get("费项"))).strip()
        bank = str(row.get("__Bank__", "")).strip().upper()
        entity = str(row.get("主体", "")).strip().upper()
        if bank == "JPM" and entity and entity != "NAN":
            ent_map = fee_to_type_by_entity.get(entity)
            if ent_map:
                if fee_str in ent_map:
                    return ent_map[fee_str]
                for k, v in ent_map.items():
                    if k.lower() == fee_str.lower():
                        return v
        if fee_str in fee_to_type:
            return fee_to_type[fee_str]
        for k, v in fee_to_type.items():
            if k.lower() == fee_str.lower():
                return v
        return None

    work["类型"] = work.apply(match_type_row, axis=1)

    special_expense_accounts = {
        "236993019",
        "236993027",
        "237086007",
        "38380028",
        "115140019",
    }

    def get_entry_subject(row: pd.Series) -> str:
        acc = str(row["Account"]).strip().lstrip("0")
        if acc in special_expense_accounts:
            return "费用"
        return "成本"

    work["入账科目"] = work.apply(get_entry_subject, axis=1)

    if "__Bank__" in work.columns:
        work = work.drop(columns=["__Bank__"])
    warns: List[str] = []
    if missing_invoice_ccy:
        warns.append(
            "账单绿区：以下 Invoice CCY 在 rules/files/fx 无折算率，USD金额为空 — "
            + ", ".join(sorted(missing_invoice_ccy))
        )
    return work[TEMPLATE_COLUMNS], warns
