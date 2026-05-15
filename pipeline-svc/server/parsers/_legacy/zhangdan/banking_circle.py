"""Banking Circle（Banking Circle S.A.）渠道账单。

「BC账单」指 Banking Circle，非巴克莱（Barclays）。

- 仅处理目录内 *.xlsx；*.pdf 不解析，仅打印跳过说明。
- 中间列与 all.py 中与 barclays 相同的字段映射对齐，供并入 bill_merged。
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


def _norm_header(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())


def _build_header_index(df: pd.DataFrame) -> dict[str, str]:
    out: dict[str, str] = {}
    for c in df.columns:
        out[_norm_header(c)] = str(c).strip()
    return out


def _pick_column(idx: dict[str, str], *candidates: str) -> str | None:
    for raw in candidates:
        k = _norm_header(raw)
        if k in idx:
            return idx[k]
    kn = _norm_header(candidates[0]).replace(" ", "") if candidates else ""
    for hk, orig in idx.items():
        if hk.replace(" ", "") == kn or kn in hk.replace(" ", ""):
            return orig
    return None


def _to_float(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "-"):
        return None
    # 单元格常为 "£12.34" / "35 GBP" / 千分位 "1,234.56"
    s_lo = s.lower().replace(",", "")
    for suf in (" gbp", " eur", " usd"):
        i = s_lo.find(suf)
        if i != -1:
            s = s[:i].strip()
            s_lo = s.lower().replace(",", "")
            break
    s = re.sub(r"^[€£\$¥₹]", "", s).strip()
    try:
        return float(s)
    except ValueError:
        m = re.search(r"-?\d[\d,.]*(?:\.\d+)?", s.replace(",", ""))
        if not m:
            return None
        try:
            return float(m.group(0).replace(",", ""))
        except ValueError:
            return None


def _format_period_cell(v: Any) -> str:
    """February, 2026 -> February 2026，供入账期间展示。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return ""
    return re.sub(r"\s*,\s*", " ", s)


def parse_banking_circle_sheet(df: pd.DataFrame, *, source_name: str) -> list[dict[str, Any]]:
    idx = _build_header_index(df)
    acc_c = _pick_column(idx, "Account number", "account number")
    per_c = _pick_column(idx, "Period", "period")
    ft_c = _pick_column(idx, "Fee type", "fee type")
    desc_c = _pick_column(idx, "description", "Description")
    vol_c = _pick_column(idx, "Volume/Value", "volume/value", "Volume value")
    fee_c = _pick_column(idx, "Fee", "fee")
    sum_c = _pick_column(idx, "Sum", "sum")
    tot_c = _pick_column(idx, "Total Amount", "total amount", "Total")
    ccy_c = _pick_column(idx, "Currency", "Invoice CCY", "CCY", "currency")

    missing = [n for n, c in (("Account number", acc_c), ("Period", per_c)) if c is None]
    if missing:
        print(
            f"  [BANKING_CIRCLE] 表 {source_name} 缺少必要列 {missing}；"
            f"当前表头: {list(df.columns)[:20]}{'…' if len(df.columns) > 20 else ''}"
        )
        return []
    if not desc_c:
        print(f"  [BANKING_CIRCLE] 表 {source_name} 缺少 description 列，无法用费项描述对齐模板。")
        return []

    rows: list[dict[str, Any]] = []
    default_ccy = "GBP"
    # 表头占 Excel 第 1 行时，首条数据行号约等于 2 + DataFrame 行序号
    skip_no_account = skip_no_charge = skip_zero = 0
    skip_samples: list[str] = []

    def _note_skip(kind: str, excel_row: int, detail: str) -> None:
        nonlocal skip_no_account, skip_no_charge, skip_zero
        if kind == "no_account":
            skip_no_account += 1
        elif kind == "no_charge":
            skip_no_charge += 1
        else:
            skip_zero += 1
        if len(skip_samples) < 6:
            skip_samples.append(f"Excel≈第{excel_row}行 [{kind}] {detail}")

    for i, (_, r) in enumerate(df.iterrows()):
        excel_row = i + 2
        ref = str(r.get(acc_c, "") or "").strip()
        if not ref or ref.lower() == "nan":
            _note_skip("no_account", excel_row, f"Account 空或无效")
            continue
        period = _format_period_cell(r.get(per_c))
        ft = str(r.get(ft_c, "") or "").strip() if ft_c else ""
        desc_raw = str(r.get(desc_c, "") or "").strip()
        parts = [p for p in (ft, desc_raw) if p and p.lower() != "nan"]
        description = " | ".join(parts) if parts else desc_raw

        volume_v = _to_float(r.get(vol_c)) if vol_c else None
        unit_price_v = _to_float(r.get(fee_c)) if fee_c else None
        charge = None
        if tot_c:
            charge = _to_float(r.get(tot_c))
        if charge is None and sum_c:
            charge = _to_float(r.get(sum_c))
        if charge is None:
            raw_tot = tot_c and r.get(tot_c)
            raw_sum = sum_c and r.get(sum_c)
            _note_skip(
                "no_charge",
                excel_row,
                f"账号={ref[:16]} Total/Sum不可解析 Total={raw_tot!r} Sum={raw_sum!r}",
            )
            continue
        if abs(charge) < 1e-12:
            _note_skip("zero", excel_row, f"账号={ref[:16]} 金额为0")
            continue

        ccy = default_ccy
        if ccy_c:
            raw_ccy = str(r.get(ccy_c, "") or "").strip().upper()
            if raw_ccy and raw_ccy != "NAN":
                ccy = raw_ccy

        pm = ft if ft else None
        rows.append(
            {
                "Reference": ref,
                "Date": period,
                "DESCRIPTION": description,
                "VOLUME": volume_v,
                "UNIT PRICE": unit_price_v,
                "TOTAL CHARGE": charge,
                "TOTAL CHARGE CCY": ccy,
                "Source": source_name,
                "Pricing Method": pm,
                "Taxable": "N",
            }
        )
    skipped = skip_no_account + skip_no_charge + skip_zero
    if skipped:
        lines = "; ".join(skip_samples[:4])
        more = f" 等共{skipped}条" if len(skip_samples) < skipped else ""
        print(
            f"  [BANKING_CIRCLE] {source_name}：表内共 {len(df)} 行，"
            f"写出 {len(rows)} 条；跳过 {skipped} 条"
            f"（无账号 {skip_no_account} / 无金额 {skip_no_charge} / 零金额 {skip_zero}）。"
            f"示例：{lines}{more}"
        )
    return rows


def main(input_folder: str, output_excel: str) -> None:
    print(
        "\n[BANKING_CIRCLE] 渠道：Banking Circle（非 Barclays）；"
        "仅合并 Excel 行；同目录 PDF 不解析，仅记录日志。"
    )
    folder = Path(input_folder)
    if not folder.is_dir():
        print(f"[BANKING_CIRCLE] 目录不存在: {input_folder}")
        return

    for p in sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF")):
        print(f"  [BANKING_CIRCLE] 跳过 PDF（当前仅处理 Excel 渠道明细）: {p.name}")

    patterns = ("*.xlsx", "*.XLSX", "*.xlsm", "*.XLSM")
    files: list[Path] = []
    for pat in patterns:
        files.extend(folder.glob(pat))
    seen: set[str] = set()
    xlsx_files: list[Path] = []
    for p in sorted(files, key=lambda q: q.name.lower()):
        k = str(p.resolve())
        if k not in seen:
            seen.add(k)
            xlsx_files.append(p)

    if not xlsx_files:
        print(f"[BANKING_CIRCLE] 未找到可处理的 Excel（*.xlsx）：{input_folder}")
        return

    all_rows: list[dict[str, Any]] = []
    for fp in xlsx_files:
        try:
            raw = pd.read_excel(fp, sheet_name=0, engine="openpyxl")
        except Exception as e:
            print(f"  [BANKING_CIRCLE] 读取失败 {fp.name}: {e}")
            continue
        if raw is None or raw.empty:
            print(f"  [BANKING_CIRCLE] 空表跳过: {fp.name}")
            continue
        raw.columns = [str(c).strip() for c in raw.columns]
        sheet_rows = parse_banking_circle_sheet(raw, source_name=fp.name)
        print(f"  [BANKING_CIRCLE] {fp.name}：解析 {len(sheet_rows)} 条明细行")
        all_rows.extend(sheet_rows)

    if not all_rows:
        print("[BANKING_CIRCLE] 未能从任何 Excel 提取到有效明细。")
        return

    cols = [
        "Reference",
        "Date",
        "DESCRIPTION",
        "VOLUME",
        "UNIT PRICE",
        "TOTAL CHARGE",
        "TOTAL CHARGE CCY",
        "Source",
        "Pricing Method",
        "Taxable",
    ]
    df = pd.DataFrame(all_rows)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    try:
        with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
        print(f"[BANKING_CIRCLE] 已写入中间表 {len(df)} 行 -> {output_excel}")
    except Exception as e:
        print(f"[BANKING_CIRCLE] 写出 Excel 失败: {e}")
