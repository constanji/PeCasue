import os
import glob
import io
from pathlib import Path
from typing import Any
import math
import numpy as np
import pandas as pd
import re

'Citi Service Activity：标红字段 + 多语言/Debit Advice 列别名。'

SLIM_COLUMN_ALIASES: list[tuple[str, list[str]]] = [('Account', ['Account', 'Cuenta']), ('Period', ['Period', 'Periodo']), ('Product Description', ['Product Description', 'Producto / Servicio']), ('Pricing Method', ['Pricing Method', 'Unidades']), ('Unit Price', ['Unit Price', 'Precio Unitario']), ('Unit Price CCY', ['Unit Price CCY', 'Precio Unitario CCY']), ('Volume', ['Volume', 'Volumen']), ('Charge in Price CCY', ['Charge in Price CCY', 'Importe de la Comisión CCY', 'Importe de la Comisión']), ('Price CCY', ['Price CCY', 'Precio (CCY)']), ('Charge in Invoice CCY', ['Charge in Invoice CCY', 'Charge in Debit Advice CCY', 'Cargos en Avisio de Comisiones CCY']), ('Invoice CCY', ['Invoice CCY', 'Debit Advice CCY', 'Aviso de Comisiones CCY']), ('Taxable', ['Taxable', 'Aplica Impuestos'])]

BRANCH_OPTIONAL: tuple[str, list[str]] = ('Branch', ['Branch'])

SLIM_OUTPUT_ORDER = [name for (name, _) in SLIM_COLUMN_ALIASES]

DEDUP_COLUMNS = SLIM_OUTPUT_ORDER.copy()

DEDUP_COLUMNS_LEGACY = ['Invoice No.', 'Invoice Date', 'Account', 'Service Code', 'Product Code', 'Product Description', 'Pricing Method', 'Unit Price', 'Unit Price CCY', 'Volume', 'Charge in Price CCY', 'Charge in Invoice CCY', 'Invoice CCY']

GREEN_COLUMNS = ['日期_货币名称', 'USD金额', '入账期间', '主体', '分行维度', '费项', '类型', '入账科目', '备注1']

LEGACY_COLUMN_ALIASES: list[tuple[str, list[str]]] = [('Region', ['Region', 'Región']), ('Branch', ['Branch']), ('Invoice No.', ['Invoice No.', 'Debit Advice No.', 'No. de Aviso de Comisiones']), ('Invoice Date', ['Invoice Date', 'Debit Advice Date', 'Fecha de Aviso de Comisiones']), ('Account', ['Account', 'Cuenta']), ('Customer#', ['Customer#', 'No. de Cliente']), ('Customer Name', ['Customer Name', 'Razón Social']), ('Period', ['Period', 'Periodo']), ('Service Code', ['Service Code', 'Código de Servicio']), ('Service Description', ['Service Description', 'Descripción del Servicio']), ('Product Code', ['Product Code', 'Código de Producto']), ('Product Description', ['Product Description', 'Producto / Servicio']), ('Pricing Method', ['Pricing Method', 'Unidades']), ('Unit Price', ['Unit Price', 'Precio Unitario']), ('Unit Price CCY', ['Unit Price CCY', 'Precio Unitario CCY']), ('Volume', ['Volume', 'Volumen']), ('Charge in Price CCY', ['Charge in Price CCY', 'Importe de la Comisión CCY', 'Importe de la Comisión']), ('Price CCY', ['Price CCY', 'Precio (CCY)']), ('Charge in Invoice CCY', ['Charge in Invoice CCY', 'Charge in Debit Advice CCY', 'Cargos en Avisio de Comisiones CCY']), ('Invoice CCY', ['Invoice CCY', 'Debit Advice CCY', 'Aviso de Comisiones CCY']), ('Price to Invoice Rate', ['Price to Invoice Rate', 'Price to Debit Advice Rate', 'Tarifa de Aviso de Comisiones']), ('Branch CCY', ['Branch CCY', 'México CCY']), ('Invoice to Branch Rate', ['Invoice to Branch Rate', 'Debit Advice to Branch Rate', 'Tasa de Aviso de Comisiones']), ('Taxable', ['Taxable', 'Aplica Impuestos'])]

OPTIONAL_TAX_COLUMNS: list[tuple[str, list[str]]] = [('Tax Amount', ['Tax Amount']), ('Tax Rate', ['Tax Rate'])]

SERVICE_ACTIVITY_SHEET_NAMES = ('Service Activity Detail', 'Detalle de Actividad x Servicio')

LEGACY_BASE_OUTPUT_ORDER = ['Region', 'Branch', 'Invoice No.', 'Invoice Date', 'Account', 'Customer#', 'Customer Name', 'Period', 'Service Code', 'Service Description', 'Product Code', 'Product Description', 'Pricing Method', 'Unit Price', 'Unit Price CCY', 'Volume', 'Charge in Price CCY', 'Price CCY', 'Charge in Invoice CCY', 'Invoice CCY', 'Price to Invoice Rate', 'Branch CCY', 'Invoice to Branch Rate', 'Taxable', 'Tax Amount'] + GREEN_COLUMNS

# 密码常量
CITI_XLS_PASSWORD = 'Pp618618@'

def _pick_column(columns: list[str], canon: str, aliases: list[str]) -> str | None:
    for name in [canon] + aliases:
        if name in columns:
            return name
    return None

def dataframe_to_canonical(raw: pd.DataFrame, *, legacy_full: bool=False) -> tuple[pd.DataFrame | None, str | None]:
    cols = list(raw.columns)
    out: dict[str, Any] = {}
    if legacy_full:
        for (canon, aliases) in LEGACY_COLUMN_ALIASES:
            src = _pick_column(cols, canon, aliases)
            if src is None:
                return (None, f'缺少列（无法映射到 {canon}），当前列: {cols[:12]}...')
            out[canon] = raw[src]
        for (canon, aliases) in OPTIONAL_TAX_COLUMNS:
            src = _pick_column(cols, canon, aliases)
            if src is not None:
                out[canon] = raw[src]
        return (pd.DataFrame(out), None)
    for (canon, aliases) in SLIM_COLUMN_ALIASES:
        src = _pick_column(cols, canon, aliases)
        if src is None:
            return (None, f'缺少列（无法映射到 {canon}），当前列: {cols[:20]}...')
        out[canon] = raw[src]
    (b_canon, b_aliases) = BRANCH_OPTIONAL
    src_b = _pick_column(cols, b_canon, b_aliases)
    if src_b is not None:
        out['Branch'] = raw[src_b]
    return (pd.DataFrame(out), None)

def dedupe_service_rows(df: pd.DataFrame, *, legacy_full: bool=False) -> pd.DataFrame:
    if df.empty:
        return df
    subset_name = DEDUP_COLUMNS_LEGACY if legacy_full else DEDUP_COLUMNS
    subset = [c for c in subset_name if c in df.columns]
    if not subset:
        return df
    return df.drop_duplicates(subset=subset, keep='first').reset_index(drop=True)

def build_output_dataframe(df: pd.DataFrame, period_yyyymm: str, *, legacy_full: bool=False, with_mapping_cols: bool=False, with_source: bool=False) -> pd.DataFrame:
    src = df.copy()
    if 'Tax Rate' in src.columns:
        src = src.drop(columns=['Tax Rate'])
    if legacy_full:
        out = src
        if 'Tax Amount' not in out.columns:
            out['Tax Amount'] = np.nan
        for c in LEGACY_BASE_OUTPUT_ORDER:
            if c not in out.columns:
                out[c] = np.nan
        out['入账期间'] = period_yyyymm
        cols = [c for c in LEGACY_BASE_OUTPUT_ORDER if c in out.columns]
    else:
        out = src.drop(columns=['Branch'], errors='ignore')
        for c in SLIM_OUTPUT_ORDER:
            if c not in out.columns:
                out[c] = np.nan
        cols = list(SLIM_OUTPUT_ORDER)
        if with_mapping_cols:
            for c in ('主体', '分行维度'):
                if c not in out.columns:
                    out[c] = np.nan
            cols = cols + ['主体', '分行维度']
    take = [c for c in cols if c in out.columns]
    result = out[take].copy()
    if with_source and '_source_file' in src.columns:
        result['_source_file'] = src['_source_file'].values
    return result

def write_service_activity_workbook(path: Path, df: pd.DataFrame) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = df.columns.tolist()
    r0 = pd.DataFrame([np.full(len(cols), np.nan)], columns=cols)
    r1 = pd.DataFrame([['Service Activity Detail'] + [np.nan] * (len(cols) - 1)], columns=cols)
    header_row = pd.DataFrame([cols], columns=cols)
    body = pd.DataFrame(df.to_numpy(), columns=cols)
    combined = pd.concat([r0, r1, header_row, body], ignore_index=True)
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        combined.to_excel(writer, sheet_name='加工合并', index=False, header=False)

def load_mapping_table(mapping_xlsx: Path) -> pd.DataFrame:
    mapping_xlsx = mapping_xlsx.expanduser().resolve()
    df = pd.read_excel(mapping_xlsx, sheet_name='MAPPING', header=0)
    df = df.rename(columns={'渠道名称': '分行维度'})
    return df

def _norm_branch(s: object) -> str:
    if pd.isna(s):
        return ''
    t = str(s).strip()
    t = ' '.join(t.split())
    return t

def apply_branch_mapping(df: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'Branch' not in df.columns:
        return df
    lut = {}
    for (_, r) in mapping.iterrows():
        b = _norm_branch(r.get('Branch', ''))
        if not b:
            continue
        subj = r.get('主体', '')
        ch = r.get('分行维度', '')
        lut[b] = (subj, ch)
    subjects: list[object] = []
    channels: list[object] = []
    for v in df['Branch'].map(_norm_branch):
        pair = lut.get(v, ('', ''))
        subjects.append(pair[0])
        channels.append(pair[1])
    df['主体'] = subjects
    df['分行维度'] = channels
    return df

def _norm_id_cell(v: object) -> str:
    if v is None or pd.isna(v):
        return ''
    if isinstance(v, float):
        if math.isnan(v):
            return ''
        if v == int(v):
            return str(int(round(v)))
        return str(v).strip()
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    s = re.sub('\\s+', '', s)
    return s

def normalize_identifiers(df: pd.DataFrame, *, legacy_full: bool=False) -> pd.DataFrame:
    df = df.copy()
    cols = ('Account',)
    if legacy_full:
        cols = ('Invoice No.', 'Account', 'Customer#')
    for col in cols:
        if col in df.columns:
            df[col] = df[col].map(_norm_id_cell)
    return df

def _all_expected_header_labels(legacy_full: bool) -> set[str]:
    pairs = LEGACY_COLUMN_ALIASES if legacy_full else SLIM_COLUMN_ALIASES
    labels: set[str] = set()
    for canon, aliases in pairs:
        labels.add(canon)
        for a in aliases:
            labels.add(a)
    return labels

def _score_header_row(row: pd.Series, labels: set[str]) -> int:
    n = 0
    for v in row:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        t = str(v).strip()
        if not t:
            continue
        if t in labels:
            n += 1
            continue
        t2 = " ".join(t.split())
        if t2 in labels:
            n += 1
    return n

def _find_service_activity_header_row(preview: pd.DataFrame, *, legacy_full: bool) -> int:
    if preview is None or len(preview) == 0:
        return 1
    labels = _all_expected_header_labels(legacy_full)
    n_cols = len(SLIM_COLUMN_ALIASES) if not legacy_full else len(LEGACY_COLUMN_ALIASES)
    min_ok = max(6, min(8, n_cols - 2))
    best_i = 1
    best_score = -1
    for i in range(min(len(preview), 30)):
        sc = _score_header_row(preview.iloc[i], labels)
        if sc > best_score:
            best_score = sc
            best_i = i
    if best_score >= min_ok:
        return int(best_i)
    return 1

def _open_excel(path: Path, password: str | None) -> tuple[pd.ExcelFile | None, str | None]:
    try:
        return pd.ExcelFile(path), None
    except Exception as e1:
        first = str(e1)
    if not password or not str(password).strip():
        return None, f"无法打开: {first}"
    try:
        import msoffcrypto
    except ImportError:
        return None, f"无法打开: {first}；加密文件需 pip install msoffcrypto-tool"
    try:
        with open(path, "rb") as f:
            office = msoffcrypto.OfficeFile(f)
            office.load_key(password=str(password))
            buf = io.BytesIO()
            office.decrypt(buf)
        buf.seek(0)
        return pd.ExcelFile(buf), None
    except Exception as e2:
        return None, f"无法打开: {first}；解密失败: {e2}"

def _sheet_name_is_service_activity(name: str) -> bool:
    s = " ".join(str(name).strip().split())
    for want in SERVICE_ACTIVITY_SHEET_NAMES:
        if s == want or s.lower() == want.lower():
            return True
    low = s.lower()
    if "service activity" in low and "detail" in low:
        return True
    if "detalle" in low and "actividad" in low and "servicio" in low:
        return True
    return False

def _pick_service_sheet(xl: pd.ExcelFile) -> str | None:
    names = xl.sheet_names
    if not names:
        return None
    if _sheet_name_is_service_activity(names[0]):
        return names[0]
    for want in SERVICE_ACTIVITY_SHEET_NAMES:
        if want in names:
            return want
    for name in names:
        low = " ".join(str(name).lower().split())
        if "service activity" in low and "detail" in low:
            return name
        if "detalle" in low and "actividad" in low and "servicio" in low:
            return name
    return None

def read_service_activity_table(path: Path, *, legacy_full: bool=False, password: str | None = None) -> tuple[pd.DataFrame | None, str | None]:
    path = path.expanduser().resolve()
    xl, err = _open_excel(path, password)
    if err:
        return (None, err)
    sheet = _pick_service_sheet(xl)
    if not sheet:
        return (None, f'无 Service Activity 表，仅有: {xl.sheet_names[:8]}')
    try:
        preview = pd.read_excel(xl, sheet_name=sheet, header=None, nrows=35, dtype=object)
    except Exception as e:
        return (None, f'读取失败: {e}')
    header_row = _find_service_activity_header_row(preview, legacy_full=legacy_full)
    try:
        raw = pd.read_excel(xl, sheet_name=sheet, header=header_row, dtype=object)
    except Exception as e:
        return (None, f'读取失败: {e}')
    raw = raw.copy()
    raw.columns = pd.Index([str(c).strip() if c is not None else "" for c in raw.columns])
    raw = raw.dropna(how="all")
    (df, err) = dataframe_to_canonical(raw, legacy_full=legacy_full)
    if err:
        return (None, err)
    df = df.copy()
    df['_source_file'] = str(path)
    return (df, None)

def iter_invoice_folder(root: Path, recursive: bool=True, *, legacy_full: bool=False, password: str | None = None) -> tuple[list[pd.DataFrame], list[tuple[str, str]]]:
    root = root.expanduser().resolve()
    pattern = '**/*' if recursive else '*'
    files = sorted((p for p in root.glob(pattern) if p.suffix.lower() in ('.xls', '.xlsx') and (not p.name.startswith('~$')) and ('清单' not in p.name) and ('ENCRYPTED' not in p.name)))
    ok: list[pd.DataFrame] = []
    errors: list[tuple[str, str]] = []
    for p in files:
        (df, err) = read_service_activity_table(p, legacy_full=legacy_full, password=password)
        if err:
            errors.append((str(p), err))
            continue
        if df is not None and (not df.empty):
            ok.append(df)
    return (ok, errors)

def read_named_sheet(path: Path, sheet_name: str, *, legacy_full: bool=False, password: str | None = None) -> tuple[pd.DataFrame | None, str | None]:
    path = path.expanduser().resolve()
    xl, err = _open_excel(path, password)
    if err:
        return (None, err)
    try:
        raw = pd.read_excel(xl, sheet_name=sheet_name, header=1)
    except Exception as e:
        return (None, str(e))
    (df, err) = dataframe_to_canonical(raw, legacy_full=legacy_full)
    if err:
        return (None, err)
    df = df.copy()
    df['_source_file'] = f'{path}::{sheet_name}'
    return (df, None)

def main(input_folder: str, output_excel: str) -> None:
    input_dir = Path(input_folder).expanduser().resolve()
    out = Path(output_excel).expanduser().resolve()
    
    # 强制带上写死的密码
    password = CITI_XLS_PASSWORD

    dfs, folder_errs = iter_invoice_folder(input_dir, recursive=True, legacy_full=False, password=password)
    if folder_errs:
        for p, e in folder_errs[:20]:
            print(f"  {p}: {e}")
            
    if not dfs:
        print("未读到任何 Service Activity 行。")
        return
        
    merged = pd.concat(dfs, ignore_index=True)
    merged = normalize_identifiers(merged, legacy_full=False)
    merged = dedupe_service_rows(merged, legacy_full=False)
    
    final_df = build_output_dataframe(
        merged,
        "202602",
        legacy_full=False,
        with_mapping_cols=False,
        with_source=True,
    )
    
    try:
        write_service_activity_workbook(out, final_df)
        print(f"成功提取了 {len(final_df)} 条明细记录，并已保存至: {out}")
    except Exception as e:
        print(f"保存 Excel 时出错: {e}")

if __name__ == "__main__":
    main(r"E:\2月成本分摊模拟\202602\2026.02账单\citi账单\2026.02 citi invoice", r"e:\Desktop\demo\citi_summary.xlsx")
