from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import subprocess
from typing import Callable, Literal

import pandas as pd
from openpyxl import Workbook, load_workbook

from QuickBILine.channel_mapping import apply_channel_name_mapping
from QuickBILine.filters import (
    filter_inbound_outbound_all_months,
    filter_va_all_months,
    pop_inbound_channel_preserve_mask,
    reclassify_ppus_citi_us_ach_debit_outbound_to_inbound,
)
from QuickBILine.final_bu import enrich_final_bu_from_template
from QuickBILine.paths import (
    DEFAULT_OUTPUT_QUICKBI,
    DEFAULT_QUICKBI_INBOUND,
    DEFAULT_QUICKBI_OUTBOUND,
    DEFAULT_QUICKBI_VA,
    DEFAULT_SHOUFUKUAN_TEMPLATE,
)
from QuickBILine.quickbi_io import (
    COLS_INBOUND,
    COLS_INBOUND_BASE,
    COLS_OUTBOUND,
    COLS_OUTBOUND_BASE,
    COLS_VA,
    FINAL_BU_COL,
    read_quickbi_inbound,
    read_quickbi_outbound,
    read_quickbi_va,
)
from QuickBILine.quickbi_xlsx_to_csv import _load_read_xlsx_rows, _one
from QuickBILine.summary_sheets import (
    SUM_IN,
    SUM_OUT,
    SUM_VA,
    aggregate_inbound,
    aggregate_outbound,
    aggregate_va,
)
from QuickBILine.va_branch import filter_va_branches
from QuickBILine.write_template import fill_shoufukuan_workbook

Mode = Literal["simulated", "excel"]
Progress = Callable[[str], None] | None

LINE_ROOT = Path(__file__).resolve().parent.parent
QUICKBI_FILES_DIR = LINE_ROOT / "files" / "quickbi"
# 仅作 ``new_run_dir`` 回退根目录；PeCause 流水线传入 ``ctx.output_dir``（与 tasks 渠道 runs 对齐）
DEFAULT_QUICKBI_OUTPUT_ROOT = LINE_ROOT / "files" / "default_quickbi_runs"


@dataclass(frozen=True)
class QuickBIPaths:
    inbound: Path = DEFAULT_QUICKBI_INBOUND
    outbound: Path = DEFAULT_QUICKBI_OUTBOUND
    va: Path = DEFAULT_QUICKBI_VA
    template: Path = DEFAULT_SHOUFUKUAN_TEMPLATE
    output_root: Path = DEFAULT_QUICKBI_OUTPUT_ROOT


@dataclass(frozen=True)
class SourceStatus:
    label: str
    path: Path
    exists: bool
    xlsx_path: Path
    xlsx_exists: bool
    csv_path: Path
    csv_exists: bool
    selected_path: Path
    selected_is_csv: bool
    rows: int | None


@dataclass(frozen=True)
class PreparedData:
    inbound: pd.DataFrame
    outbound: pd.DataFrame
    va: pd.DataFrame


@dataclass(frozen=True)
class BuildResult:
    run_dir: Path
    workbook_path: Path
    summary_path: Path | None
    """仅含入金汇总 / 出金汇总 / VA汇总 三 sheet，体积小，便于预览或二次处理。"""
    summary_only_path: Path | None
    rows_in: int
    rows_out: int
    rows_va: int


def resolve_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def new_run_dir(root: str | Path, prefix: str = "run") -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = resolve_path(root) / f"{prefix}_{stamp}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def count_csv_data_rows(path: Path) -> int | None:
    """数据行数（不含表头）；非 CSV 或不存在则返回 None。"""
    path = Path(path)
    if path.suffix.lower() != ".csv" or not path.is_file():
        return None
    with path.open("r", encoding="utf-8-sig", errors="ignore") as fh:
        total = sum(1 for _ in fh)
    return max(total - 1, 0)


def _count_csv_rows(path: Path) -> int | None:
    """兼容旧名，等价于 ``count_csv_data_rows``。"""
    return count_csv_data_rows(path)


def _xlsx_csv_pair(configured: Path) -> tuple[Path, Path]:
    """Return (xlsx_path, csv_path) for a configured QuickBI source path."""
    c = resolve_path(configured)
    if c.suffix.lower() == ".csv":
        return c.with_suffix(".xlsx"), c
    return c, c.with_suffix(".csv")


def inspect_sources(paths: QuickBIPaths, *, prefer_csv: bool = True) -> list[SourceStatus]:
    items = [
        ("入金", resolve_path(paths.inbound)),
        ("出金", resolve_path(paths.outbound)),
        ("VA", resolve_path(paths.va)),
    ]
    out: list[SourceStatus] = []
    for label, configured in items:
        xlsx_path, csv_path = _xlsx_csv_pair(configured)
        xlsx_exists = xlsx_path.is_file()
        csv_exists = csv_path.is_file()
        has_any = xlsx_exists or csv_exists
        if prefer_csv and csv_exists:
            selected = csv_path
        elif xlsx_exists:
            selected = xlsx_path
        else:
            selected = csv_path
        out.append(
            SourceStatus(
                label=label,
                path=configured,
                exists=has_any,
                xlsx_path=xlsx_path,
                xlsx_exists=xlsx_exists,
                csv_path=csv_path,
                csv_exists=csv_exists,
                selected_path=selected,
                selected_is_csv=selected.suffix.lower() == ".csv",
                rows=_count_csv_rows(csv_path) if csv_exists else None,
            )
        )
    return out


def convert_missing_to_csv(paths: QuickBIPaths, *, overwrite: bool = False) -> list[Path]:
    read_xlsx_rows = _load_read_xlsx_rows()
    converted: list[Path] = []
    pairs = [
        (resolve_path(paths.inbound), COLS_INBOUND_BASE),
        (resolve_path(paths.outbound), COLS_OUTBOUND_BASE),
        (resolve_path(paths.va), COLS_VA),
    ]
    for src, cols in pairs:
        xlsx_path, csv_path = _xlsx_csv_pair(src)
        if not xlsx_path.is_file():
            continue
        if csv_path.is_file() and not overwrite:
            continue
        converted.append(_one(xlsx_path, cols, None, read_xlsx_rows))
    return converted


def convert_missing_to_csv_with_progress(
    paths: QuickBIPaths,
    *,
    overwrite: bool = False,
    progress: Progress = None,
) -> list[Path]:
    read_xlsx_rows = _load_read_xlsx_rows()
    converted: list[Path] = []
    pairs = [
        ("入金", resolve_path(paths.inbound), COLS_INBOUND_BASE),
        ("出金", resolve_path(paths.outbound), COLS_OUTBOUND_BASE),
        ("VA", resolve_path(paths.va), COLS_VA),
    ]
    for label, src, cols in pairs:
        xlsx_path, csv_path = _xlsx_csv_pair(src)
        if not xlsx_path.is_file():
            if progress:
                progress(f"{label}: 未找到 XLSX，跳过转换（若仅有 CSV 可直接读取）-> {xlsx_path.name}")
            continue
        if csv_path.is_file() and not overwrite:
            if progress:
                progress(f"{label}: 已存在 CSV，跳过转换 -> {csv_path.name}")
            continue
        if progress:
            progress(f"{label}: 开始转换 xlsx -> csv")
        out = _one(xlsx_path, cols, None, read_xlsx_rows)
        converted.append(out)
        if progress:
            progress(f"{label}: 转换完成 -> {out.name}")
    return converted


# 渠道映射来源文件（可选）；存在时使用，否则仅依赖模版内 mapping（逻辑不变）
_CHANNEL_MAP_WORKBOOK = LINE_ROOT / "files" / "quickbi" / "成本分摊基数+输出模板(2).xlsx"


def load_filtered_data(
    paths: QuickBIPaths,
    *,
    prefer_csv: bool = True,
    enrich_final_bu: bool = True,
    progress: Progress = None,
) -> PreparedData:
    statuses = inspect_sources(paths, prefer_csv=prefer_csv)
    src = {s.label: s.selected_path for s in statuses}
    if progress:
        progress("开始读取 QuickBI 入金/出金/VA 源文件")
    d_in = filter_inbound_outbound_all_months(read_quickbi_inbound(src["入金"]))
    d_out = filter_inbound_outbound_all_months(read_quickbi_outbound(src["出金"]))
    d_in, d_out = reclassify_ppus_citi_us_ach_debit_outbound_to_inbound(d_in, d_out)
    if progress:
        progress(
            "入金/出金读取并过滤完成：入金 %d 行，出金 %d 行（含将出金源中 PPUS+CITI_US_ACH_DEBIT 并入入金）"
            % (len(d_in), len(d_out))
        )
    d_va = filter_va_branches(filter_va_all_months(read_quickbi_va(src["VA"])))
    if progress:
        progress(f"VA 读取并过滤完成：{len(d_va)} 行")
    # 渠道名称映射（原始渠道名 → 渠道-分行标准名）
    tpl = resolve_path(paths.template)
    mw = _CHANNEL_MAP_WORKBOOK if _CHANNEL_MAP_WORKBOOK.is_file() else None
    if progress:
        progress("开始应用渠道名称映射")
    d_in, _in_preserve = pop_inbound_channel_preserve_mask(d_in)
    d_in = apply_channel_name_mapping(
        d_in,
        template=tpl,
        is_outbound=False,
        mapping_workbook=mw,
        preserve_channel_mask=_in_preserve,
    )
    d_out = apply_channel_name_mapping(d_out, template=tpl, is_outbound=True, mapping_workbook=mw)
    if progress:
        progress("渠道名称映射完成")
    if enrich_final_bu:
        if progress:
            progress("开始按模拟公式填充最终BU")
        d_in, d_out, d_va = enrich_final_bu_from_template(
            template=tpl,
            df_in=d_in,
            df_out=d_out,
            df_va=d_va,
        )
        if progress:
            progress("模拟公式最终BU填充完成")
    return PreparedData(d_in, d_out, d_va)


def build_quickbi_workbook(
    paths: QuickBIPaths,
    *,
    prefer_csv: bool = True,
    output_dir: str | Path | None = None,
    enrich_final_bu: bool = True,
    progress: Progress = None,
) -> BuildResult:
    """生成 QuickBI_out.xlsx。

    enrich_final_bu=True（默认）: 模拟公式自动填充最终BU（CLI 一步到位模式）。
    enrich_final_bu=False: 不填充最终BU，写入原始数据到 out 表，等待用户在 Excel 中刷新公式后再生成汇总（Streamlit UI 分步模式）。
    """
    run_dir = resolve_path(output_dir) if output_dir else new_run_dir(paths.output_root, "quickbi")
    run_dir.mkdir(parents=True, exist_ok=True)
    if progress:
        progress(f"本次输出目录：{run_dir}")
    data = load_filtered_data(
        paths,
        prefer_csv=prefer_csv,
        enrich_final_bu=enrich_final_bu,
        progress=progress,
    )
    workbook_path = run_dir / DEFAULT_OUTPUT_QUICKBI.name
    if enrich_final_bu:
        if progress:
            progress("开始写入 QuickBI_out 明细表（入金/出金/VA）+ 入金汇总/出金汇总/VA汇总")
    else:
        if progress:
            progress("开始写入 QuickBI_out 明细表（入金/出金/VA）——不含汇总，等待 Excel 刷新公式后再生成")
    fill_shoufukuan_workbook(
        resolve_path(paths.template),
        workbook_path,
        df_in=data.inbound,
        df_out=data.outbound,
        df_va=data.va,
        include_summary_sheets=enrich_final_bu,
    )
    summary_only_path: Path | None = None
    if enrich_final_bu:
        summary_only_path = run_dir / "\u6536\u4ed8\u6b3e\u57fa\u6570_QuickBI_\u4e09\u8868\u6c47\u603b.xlsx"
        if progress:
            progress(f"写入轻量三表汇总（无模版明细）：{summary_only_path.name}")
        write_summary_workbook(summary_only_path, data)
    if progress:
        if enrich_final_bu:
            progress(f"QuickBI_out 完整文件生成完成（含入金汇总/出金汇总/VA汇总）：{workbook_path}")
        else:
            progress(f"QuickBI_out 明细表生成完成（不含汇总，请用 Excel 打开刷新公式后保存）：{workbook_path}")
    summary_path: Path | None = None
    return BuildResult(
        run_dir=run_dir,
        workbook_path=workbook_path,
        summary_path=summary_path,
        summary_only_path=summary_only_path,
        rows_in=len(data.inbound),
        rows_out=len(data.outbound),
        rows_va=len(data.va),
    )


def build_simulated_formula_summary(
    paths: QuickBIPaths,
    *,
    prefer_csv: bool = True,
    output_dir: str | Path,
    progress: Progress = None,
) -> Path:
    data = load_filtered_data(
        paths,
        prefer_csv=prefer_csv,
        enrich_final_bu=True,
        progress=progress,
    )
    summary_path = resolve_path(output_dir) / "收付款基数_QuickBI_out_三表汇总_模拟公式.xlsx"
    if progress:
        progress("开始生成模拟公式口径三表汇总")
    return write_summary_workbook(summary_path, data)


def _write_df_sheet(wb: Workbook, title: str, df: pd.DataFrame) -> None:
    ws = wb.create_sheet(title)
    for c, col in enumerate(df.columns, start=1):
        ws.cell(row=1, column=c).value = str(col)
    for r_idx, row in enumerate(df.itertuples(index=False, name=None), start=2):
        for c_idx, val in enumerate(row, start=1):
            if pd.isna(val):
                ws.cell(row=r_idx, column=c_idx).value = None
            else:
                ws.cell(row=r_idx, column=c_idx).value = val


def write_summary_workbook(path: str | Path, data: PreparedData) -> Path:
    path = resolve_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    wb.remove(wb.active)
    _write_df_sheet(wb, SUM_IN, aggregate_inbound(data.inbound))
    _write_df_sheet(wb, SUM_OUT, aggregate_outbound(data.outbound))
    _write_df_sheet(wb, SUM_VA, aggregate_va(data.va))
    wb.save(path)
    wb.close()
    return path


def _read_detail_sheet_with_formula_bu(
    workbook_path: Path,
    *,
    sheet_name: str,
    base_cols: list[str],
    final_bu_col_idx: int,
) -> pd.DataFrame:
    wb = load_workbook(workbook_path, data_only=True, read_only=True)
    try:
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"{workbook_path.name} missing sheet {sheet_name!r}")
        ws = wb[sheet_name]
        records: list[dict[str, object]] = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            base_values = list(row[: len(base_cols)])
            if not any(v not in (None, "") for v in base_values):
                continue
            rec = {col: ("" if val is None else val) for col, val in zip(base_cols, base_values)}
            bu_val = row[final_bu_col_idx - 1] if len(row) >= final_bu_col_idx else ""
            rec[FINAL_BU_COL] = "" if bu_val is None else bu_val
            records.append(rec)
        return pd.DataFrame(records, columns=base_cols + [FINAL_BU_COL])
    finally:
        wb.close()


def load_excel_formula_data(workbook_path: str | Path) -> PreparedData:
    path = resolve_path(workbook_path)
    _validate_xlsx_path(path)
    d_in = _read_detail_sheet_with_formula_bu(
        path,
        sheet_name="入金",
        base_cols=COLS_INBOUND_BASE,
        final_bu_col_idx=23,
    )
    d_out = _read_detail_sheet_with_formula_bu(
        path,
        sheet_name="出金",
        base_cols=COLS_OUTBOUND_BASE,
        final_bu_col_idx=23,
    )
    d_va = _read_detail_sheet_with_formula_bu(
        path,
        sheet_name="VA",
        base_cols=COLS_VA,
        final_bu_col_idx=20,
    )
    return PreparedData(d_in, d_out, d_va)


def _validate_xlsx_path(path: Path) -> None:
    if path.exists() and path.is_dir():
        raise ValueError(f"请选择具体 Excel 文件，而不是目录：{path}")
    if not path.is_file():
        raise FileNotFoundError(f"找不到 Excel 文件：{path}")
    if path.suffix.lower() not in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        raise ValueError(f"请选择 .xlsx/.xlsm/.xltx/.xltm 文件：{path}")


def final_bu_counts(data: PreparedData) -> dict[str, int]:
    def _count(df: pd.DataFrame) -> int:
        if FINAL_BU_COL not in df.columns:
            return 0
        return int((df[FINAL_BU_COL].astype(str).str.strip() != "").sum())

    return {"入金": _count(data.inbound), "出金": _count(data.outbound), "VA": _count(data.va)}


def build_excel_formula_summary(workbook_path: str | Path) -> Path:
    path = resolve_path(workbook_path)
    _validate_xlsx_path(path)
    data = load_excel_formula_data(path)
    counts = final_bu_counts(data)
    total_rows = len(data.inbound) + len(data.outbound) + len(data.va)
    if total_rows > 0 and sum(counts.values()) == 0:
        raise ValueError(
            "三张明细表的最终BU全部为空——文件中公式尚未计算缓存值。\n"
            "请先用 Excel 打开此文件，等待公式刷新完成后保存，再重新生成汇总表。"
        )
    summary_path = path.with_name(path.stem + "_对外表.xlsx")
    return write_summary_workbook(summary_path, data)


def build_excel_formula_summary_with_progress(
    workbook_path: str | Path,
    *,
    progress: Progress = None,
) -> Path:
    path = resolve_path(workbook_path)
    _validate_xlsx_path(path)
    if progress:
        progress("开始读取 Excel 已刷新后的最终BU明细")
    data = load_excel_formula_data(path)
    counts = final_bu_counts(data)
    if progress:
        progress(f"最终BU非空行数：入金 {counts['入金']}，出金 {counts['出金']}，VA {counts['VA']}")
    total_rows = len(data.inbound) + len(data.outbound) + len(data.va)
    total_bu = counts["入金"] + counts["出金"] + counts["VA"]
    if total_rows > 0 and total_bu == 0:
        raise ValueError(
            "三张明细表的最终BU全部为空——文件中公式尚未计算缓存值。\n"
            "请先用 Excel 打开此文件，等待公式刷新完成后保存，再重新生成汇总表。\n"
            f"（文件：{path.name}）"
        )
    if total_rows > 0 and total_bu < total_rows * 0.5:
        pct = int(100 * total_bu / total_rows)
        if progress:
            progress(
                f"⚠️ 警告：最终BU填充率仅 {pct}%（{total_bu}/{total_rows} 行）——"
                "可能未完整刷新，请确认 Excel 已重算并保存。继续生成汇总…"
            )
    if progress:
        progress("开始生成 Excel 公式口径汇总表")
    summary_path = path.with_name(path.stem + "_对外表.xlsx")
    out = write_summary_workbook(summary_path, data)
    if progress:
        progress(f"Excel 公式口径汇总表生成完成：{out}")
    return out


def open_workbook_for_refresh(workbook_path: str | Path) -> None:
    path = resolve_path(workbook_path)
    _validate_xlsx_path(path)
    subprocess.Popen(["open", str(path)])


def template_status(path: str | Path) -> dict[str, object]:
    p = resolve_path(path)
    info: dict[str, object] = {
        "path": str(p),
        "exists": p.is_file(),
        "size_mb": None,
        "has_mapping": False,
        "sheets": [],
    }
    if not p.is_file():
        return info
    info["size_mb"] = round(p.stat().st_size / (1024 * 1024), 2)
    wb = load_workbook(p, read_only=True, data_only=False)
    try:
        sheets = list(wb.sheetnames)
        info["sheets"] = sheets
        info["has_mapping"] = "mapping" in sheets
    finally:
        wb.close()
    return info
