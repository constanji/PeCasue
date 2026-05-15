"""Import pipeline static rules from Excel into JSON snapshots + CSV sidecars.

CSV / xlsx 侧车文件位于 ``PIPELINE_DATA_DIR/rules/files`` 下的 ``mapping``、``fx``、``rules`` 子目录。
写入形态：**同一内容的 CSV（utf-8-sig）+ xlsx** 置于 ``mapping/`` / ``fx/`` / ``rules/`` 子目录，
与 allline ``files/mapping``、``files/fx`` 约定一致；RuleStore 里另有 JSON 版本快照。
legacy ``own_flow`` 通过 ``OWN_FLOW_FILES_ROOT=$PIPELINE_DATA_DIR/rules/files`` 复用这些路径。

特殊来源主体分行：启动时若尚无磁盘文件则写入默认两行（boc/bosh）到 ``mapping/特殊来源主体分行mapping.{csv,xlsx}``，
并填充空的 RuleStore；运行时 ``load_special_source_mapping()`` 会优先读该目录。
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from server.core.paths import ensure_data_directories, get_rules_files_dir
from server.parsers._legacy.own_flow_pkg.mapping_import import (
    ACCOUNT_NAME,
    CUSTOMER_BRANCH_MAPPING_SHEET,
    CUSTOMER_FEE_MAPPING_SHEET,
    CUSTOMER_MAPPING_SHEET,
    FEE_NAME,
    MappingImportError,
    SPECIAL_NAME,
    extract_processing_rules_from_workbook,
    load_account_mapping_dataframe,
    load_customer_branch_mapping_dataframe,
    load_customer_fee_mapping_dataframe,
    load_customer_mapping_dataframe,
    load_fee_mapping_dataframe,
    load_fx_rates_standard_dataframe,
    load_fx_standalone_workbook_dataframe,
)
from server.rules import store as rule_store
from server.rules.schema import RuleKind, RuleTable


def write_utf8_sig_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _rule_table_to_dataframe(table: RuleTable) -> pd.DataFrame:
    """RuleTable -> DataFrame while preserving declared column order."""
    cols = [str(c).strip() for c in (table.columns or []) if str(c).strip()]
    rows = list(table.rows or [])
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    merged_cols = list(cols)
    for c in df.columns:
        cs = str(c).strip()
        if cs and cs not in merged_cols:
            merged_cols.append(cs)
    if not merged_cols:
        return pd.DataFrame(rows)
    return df.reindex(columns=merged_cols)


def sync_rule_table_sidecars(kind: RuleKind, table: RuleTable) -> dict[str, Any] | None:
    """Persist RuleTable to runtime sidecars consumed by parsers."""
    ensure_data_directories()
    stem: Path | None = None
    if kind == RuleKind.FX:
        stem = get_rules_files_dir() / "fx" / "各种货币对美元折算率"
    elif kind == RuleKind.ACCOUNT_MAPPING:
        stem = get_rules_files_dir() / "mapping" / ACCOUNT_NAME
    elif kind == RuleKind.FEE_MAPPING:
        stem = get_rules_files_dir() / "mapping" / FEE_NAME
    elif kind == RuleKind.SPECIAL_BRANCH_MAPPING:
        stem = get_rules_files_dir() / "mapping" / SPECIAL_NAME
    elif kind == RuleKind.OWN_FLOW_PROCESSING:
        stem = get_rules_files_dir() / "rules" / "处理表"
    elif kind == RuleKind.CUSTOMER_MAPPING:
        stem = get_rules_files_dir() / "mapping" / "客资流水MAPPING"
    elif kind == RuleKind.CUSTOMER_FEE_MAPPING:
        stem = get_rules_files_dir() / "mapping" / "客资流水费项mapping表"
    elif kind == RuleKind.CUSTOMER_BRANCH_MAPPING:
        stem = get_rules_files_dir() / "mapping" / "客资流水分行mapping"
    if stem is None:
        return None

    df = _rule_table_to_dataframe(table)
    csv_abs = stem.with_suffix(".csv")
    xlsx_abs = stem.with_suffix(".xlsx")
    write_utf8_sig_csv(df, csv_abs)
    df.to_excel(xlsx_abs, index=False, engine="openpyxl")

    if kind == RuleKind.SPECIAL_BRANCH_MAPPING:
        from server.parsers._legacy.own_flow_pkg.special_source_mapping import (
            invalidate_special_source_mapping_cache,
        )

        invalidate_special_source_mapping_cache()

    if kind == RuleKind.OWN_FLOW_PROCESSING:
        from server.parsers._legacy.own_flow_pkg.rules import invalidate_rules_cache

        invalidate_rules_cache()

    rel_csv = csv_abs.relative_to(get_rules_files_dir().parent).as_posix()
    rel_xlsx = xlsx_abs.relative_to(get_rules_files_dir().parent).as_posix()
    return {"csv_relative": rel_csv, "xlsx_relative": rel_xlsx, "rows": int(len(df))}


_OWN_FLOW_IMPORT_SCOPES = frozenset(
    {"account_mapping", "fee_mapping", "fx", "own_flow_processing"}
)
_CUSTOMER_IMPORT_SCOPES = frozenset(
    {"customer_mapping", "customer_fee_mapping", "customer_branch_mapping"}
)


def dataframe_to_rule_table(
    df: pd.DataFrame,
    *,
    note: str | None = None,
    meta: dict[str, Any] | None = None,
) -> RuleTable:
    cols = [str(c).strip() if c is not None else "" for c in df.columns]
    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        row: dict[str, Any] = {}
        for c in cols:
            v = r.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                row[c] = ""
            elif isinstance(v, (np.integer,)):
                row[c] = int(v)
            elif isinstance(v, (np.floating, float)):
                row[c] = float(v)
            else:
                row[c] = str(v).strip() if isinstance(v, str) else v
        rows.append(row)
    return RuleTable(columns=cols, rows=rows, note=note, meta=meta)


def _normalize_fx_month_label(raw: str | None) -> str | None:
    """Accept ``YYYY-MM`` / ``YYYY年M月`` / passthrough text → canonical 「YYYY年M月」."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = re.fullmatch(r"(20\d{2})-(\d{1,2})", s)
    if m:
        return f"{m.group(1)}年{int(m.group(2))}月"
    m2 = re.fullmatch(r"(20\d{2})[/_:](\d{1,2})", s)
    if m2:
        return f"{m2.group(1)}年{int(m2.group(2))}月"
    m3 = re.fullmatch(r"(20\d{2})年(\d{1,2})月", s)
    if m3:
        return f"{m3.group(1)}年{int(m3.group(2))}月"
    return s


def _fx_meta_for_import(explicit_label: str | None) -> dict[str, Any] | None:
    """Manual ``fx_month_label`` wins; otherwise keep existing RuleKind.FX meta."""
    norm = _normalize_fx_month_label(explicit_label)
    if norm:
        return {"fx_month_label": norm}
    prev = rule_store.load_rule(RuleKind.FX)
    prev_lab = (prev.meta or {}).get("fx_month_label") if prev.meta else None
    if prev_lab:
        pl = str(prev_lab).strip()
        if pl:
            return {"fx_month_label": pl}
    return None


def import_fx_standalone_file(
    path: Path,
    *,
    author: str | None,
    note: str | None,
    fx_month_label: str | None = None,
) -> dict[str, Any]:
    ensure_data_directories()
    df = load_fx_standalone_workbook_dataframe(path, 0)
    csv_abs = get_rules_files_dir() / "fx" / "各种货币对美元折算率.csv"
    write_utf8_sig_csv(df, csv_abs)
    fx_meta = _fx_meta_for_import(fx_month_label)
    table = dataframe_to_rule_table(df, meta=fx_meta)
    entry = rule_store.save_rule(RuleKind.FX, table, author=author, note=note or "Excel 导入 · 汇率表")
    return {
        "kind": RuleKind.FX.value,
        "rows": len(df),
        "csv_relative": "rules/files/fx/各种货币对美元折算率.csv",
        "entry": entry.model_dump(mode="json"),
    }


def _persist_mapping_df_sidecars(df: pd.DataFrame, stem: str) -> Path:
    """Write utf-8-sig CSV + xlsx next to each other under ``mapping/``."""
    csv_abs = get_rules_files_dir() / "mapping" / f"{stem}.csv"
    write_utf8_sig_csv(df, csv_abs)
    df.to_excel(csv_abs.with_suffix(".xlsx"), index=False, engine="openpyxl")
    return csv_abs


def import_own_flow_template_bundle(
    path: Path,
    *,
    author: str | None,
    note: str | None,
    scope: str | None = None,
    fx_month_label: str | None = None,
) -> dict[str, Any]:
    """Parse ``模版*.xlsx`` — 可按 scope 仅覆盖某一类自有流水规则。

    * ``scope`` 为 ``None`` / ``""`` / ``"all"``：账户 + 费项 + 汇率 + 处理表。
    * 否则须为 ``account_mapping`` | ``fee_mapping`` | ``fx`` | ``own_flow_processing`` 之一。
    """
    ensure_data_directories()
    base_note = note or "Excel 导入 · 模版工作簿"
    imported: dict[str, Any] = {}
    row_counts: dict[str, int] = {}

    sel = (scope or "").strip().lower()
    if sel in ("", "all"):
        targets = set(_OWN_FLOW_IMPORT_SCOPES)
    elif sel in _OWN_FLOW_IMPORT_SCOPES:
        targets = {sel}
    else:
        raise ValueError(
            f"无效的 scope={scope!r}；可选 all 或 "
            + ", ".join(sorted(_OWN_FLOW_IMPORT_SCOPES))
        )

    if "account_mapping" in targets:
        acc_df = load_account_mapping_dataframe(path)
        _persist_mapping_df_sidecars(acc_df, ACCOUNT_NAME)
        acc_table = dataframe_to_rule_table(acc_df)
        imported["account_mapping"] = rule_store.save_rule(
            RuleKind.ACCOUNT_MAPPING, acc_table, author=author, note=base_note
        ).model_dump(mode="json")
        row_counts["account_mapping"] = len(acc_df)

    if "fee_mapping" in targets:
        fee_df = load_fee_mapping_dataframe(path)
        _persist_mapping_df_sidecars(fee_df, FEE_NAME)
        fee_table = dataframe_to_rule_table(fee_df)
        imported["fee_mapping"] = rule_store.save_rule(
            RuleKind.FEE_MAPPING, fee_table, author=author, note=base_note
        ).model_dump(mode="json")
        row_counts["fee_mapping"] = len(fee_df)

    fx_info: dict[str, Any] | None = None
    if "fx" in targets:
        try:
            fx_df = load_fx_rates_standard_dataframe(path)
            fx_csv = get_rules_files_dir() / "fx" / "各种货币对美元折算率.csv"
            write_utf8_sig_csv(fx_df, fx_csv)
            fx_df.to_excel(fx_csv.with_suffix(".xlsx"), index=False, engine="openpyxl")
            fx_meta = _fx_meta_for_import(fx_month_label)
            fx_table = dataframe_to_rule_table(fx_df, meta=fx_meta)
            fx_entry = rule_store.save_rule(RuleKind.FX, fx_table, author=author, note=base_note)
            imported["fx"] = fx_entry.model_dump(mode="json")
            row_counts["fx"] = len(fx_df)
            fx_info = {"rows": len(fx_df), "skipped": False}
        except MappingImportError as e:
            imported["fx"] = None
            fx_info = {"skipped": True, "reason": str(e)}

    if "own_flow_processing" in targets:
        proc_df = extract_processing_rules_from_workbook(path)
        proc_csv = get_rules_files_dir() / "rules" / "处理表.csv"
        write_utf8_sig_csv(proc_df, proc_csv)
        proc_df.to_excel(proc_csv.with_suffix(".xlsx"), index=False, engine="openpyxl")
        proc_table = dataframe_to_rule_table(proc_df)
        imported["own_flow_processing"] = rule_store.save_rule(
            RuleKind.OWN_FLOW_PROCESSING, proc_table, author=author, note=base_note
        ).model_dump(mode="json")
        row_counts["own_flow_processing"] = len(proc_df)

    return {
        "imported": imported,
        "row_counts": row_counts,
        "csv_roots_relative": "rules/files/mapping|fx|rules",
        "fx": fx_info,
        "scope": sel or "all",
    }


def import_customer_flow_template_bundle(
    path: Path,
    *,
    author: str | None,
    note: str | None,
    scope: str | None = None,
) -> dict[str, Any]:
    """Parse 模版.xlsx 中三张客资 mapping；可按 scope 仅覆盖其中一类 RuleStore + mapping 侧车。"""
    ensure_data_directories()
    base_note = note or "Excel 导入 · 客资流水模版"
    imported: dict[str, Any] = {}
    row_counts: dict[str, int] = {}

    sel = (scope or "").strip().lower()
    if sel in ("", "all"):
        targets = set(_CUSTOMER_IMPORT_SCOPES)
    elif sel in _CUSTOMER_IMPORT_SCOPES:
        targets = {sel}
    else:
        raise ValueError(
            f"无效的 scope={scope!r}；可选 all 或 "
            + ", ".join(sorted(_CUSTOMER_IMPORT_SCOPES))
        )

    jobs = [
        (
            "customer_mapping",
            RuleKind.CUSTOMER_MAPPING,
            CUSTOMER_MAPPING_SHEET,
            load_customer_mapping_dataframe,
        ),
        (
            "customer_fee_mapping",
            RuleKind.CUSTOMER_FEE_MAPPING,
            CUSTOMER_FEE_MAPPING_SHEET,
            load_customer_fee_mapping_dataframe,
        ),
        (
            "customer_branch_mapping",
            RuleKind.CUSTOMER_BRANCH_MAPPING,
            CUSTOMER_BRANCH_MAPPING_SHEET,
            load_customer_branch_mapping_dataframe,
        ),
    ]
    for key, kind, stem, loader in jobs:
        if key not in targets:
            continue
        df = loader(path)
        _persist_mapping_df_sidecars(df, stem)
        table = dataframe_to_rule_table(df)
        imported[key] = rule_store.save_rule(
            kind, table, author=author, note=base_note
        ).model_dump(mode="json")
        row_counts[key] = len(df)

    return {
        "imported": imported,
        "row_counts": row_counts,
        "csv_roots_relative": "rules/files/mapping",
        "scope": sel or "all",
    }


DEFAULT_SPECIAL_BRANCH_ROWS: list[dict[str, str]] = [
    {"file_group": "boc", "主体": "PPUS", "分行维度": "BOCUS"},
    {"file_group": "bosh", "主体": "PPHK", "分行维度": "BOSH"},
]


def ensure_special_branch_defaults() -> None:
    """若 ``rules/files/mapping`` 下无特殊来源文件，则写入默认表并同步空 RuleStore。"""
    from server.parsers._legacy.own_flow_pkg.special_source_mapping import (
        invalidate_special_source_mapping_cache,
    )

    ensure_data_directories()
    mapping_dir = get_rules_files_dir() / "mapping"
    csv_path = mapping_dir / f"{SPECIAL_NAME}.csv"
    xlsx_path = mapping_dir / f"{SPECIAL_NAME}.xlsx"
    touched = False
    if not csv_path.exists():
        write_utf8_sig_csv(pd.DataFrame(DEFAULT_SPECIAL_BRANCH_ROWS), csv_path)
        touched = True
    if not xlsx_path.exists():
        df_x = pd.read_csv(csv_path, encoding="utf-8-sig")
        df_x.to_excel(xlsx_path, index=False, engine="openpyxl")
        touched = True
    tab = rule_store.load_rule(RuleKind.SPECIAL_BRANCH_MAPPING)
    if not tab.rows:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        rule_store.save_rule(
            RuleKind.SPECIAL_BRANCH_MAPPING,
            dataframe_to_rule_table(df),
            author="system",
            note="startup seed · 特殊来源主体分行",
        )
        touched = True
    if touched:
        invalidate_special_source_mapping_cache()


def import_upload_bytes(
    data: bytes,
    filename: str,
    *,
    mode: str,
    author: str | None,
    note: str | None,
    scope: str | None = None,
    fx_month_label: str | None = None,
) -> dict[str, Any]:
    suffix = Path(filename or "upload.xlsx").suffix.lower()
    if suffix not in (".xlsx", ".xlsm"):
        suffix = ".xlsx"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(data)
        p = Path(tmp.name)
    try:
        if mode == "fx_standalone":
            return import_fx_standalone_file(
                p, author=author, note=note, fx_month_label=fx_month_label
            )
        if mode == "own_flow_template":
            return import_own_flow_template_bundle(
                p,
                author=author,
                note=note,
                scope=scope,
                fx_month_label=fx_month_label,
            )
        if mode == "customer_flow_template":
            return import_customer_flow_template_bundle(
                p, author=author, note=note, scope=scope
            )
        raise ValueError(f"unknown import mode: {mode}")
    finally:
        p.unlink(missing_ok=True)
