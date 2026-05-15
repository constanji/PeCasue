from __future__ import annotations

import os
import pandas as pd
from pathlib import Path
import tempfile
import shutil

# Banks are vendored as siblings inside the package; relative-import them so
# this module can be loaded as `server.parsers._legacy.zhangdan.all`.
from . import (
    banking_circle,
    barclays,
    citi,
    db,
    ewb,
    ibc,
    jpm,
    monoova,
    scb,
    xendit,
)
from .folder_match import BANK_FOLDER_ALIASES, folder_matches_bank as _folder_matches_bank

# 目标汇总模板字段
TEMPLATE_COLUMNS = [
    'Account', 'Description', 'Pricing Method', 'Volume', 'Unit\nPrice',
    'Unit Price CCY', 'Charge in Invoice CCY', 'Invoice CCY', '汇率', 'Taxable', 'TAX',
    '来源文件', 'USD金额', '入账期间', '主体', '分行维度', '费项', '类型', '入账科目'
]

# 各个银行对应的映射规则
BANK_MAPPINGS = {
    'banking_circle': {
        'Reference': 'Account',
        'DESCRIPTION': 'Description',
        'Pricing Method': 'Pricing Method',
        'VOLUME': 'Volume',
        'UNIT PRICE': 'Unit\nPrice',
        'TOTAL CHARGE': 'Charge in Invoice CCY',
        'TOTAL CHARGE CCY': 'Invoice CCY',
        'Source': '来源文件',
        'Date': '入账期间',
        'Taxable': 'Taxable',
    },
    'barclays': {
        'Reference': 'Account',
        'DESCRIPTION': 'Description',
        'VOLUME': 'Volume',
        'UNIT PRICE': 'Unit\nPrice',
        'TOTAL CHARGE': 'Charge in Invoice CCY',
        'TOTAL CHARGE CCY': 'Invoice CCY',
        'Source': '来源文件',
        'Date': '入账期间'
    },
    'citi': {
        'Account': 'Account',
        'Product Description': 'Description',
        'Pricing Method': 'Pricing Method',
        'Volume': 'Volume',
        'Unit Price': 'Unit\nPrice',
        'Unit Price CCY': 'Unit Price CCY',
        'Charge in Invoice CCY': 'Charge in Invoice CCY',
        'Invoice CCY': 'Invoice CCY',
        'Taxable': 'Taxable',
        '_source_file': '来源文件',
        'Period': '入账期间'
    },
    'db': {
        'Account No': 'Account',
        'Product Description': 'Description',
        'Item Count': 'Volume',
        'Avg Per Item': 'Unit\nPrice',
        'Amount': 'Charge in Invoice CCY',
        'Tariff CCY': 'Invoice CCY',
        'Source': '来源文件',
        'Service Type': 'Pricing Method'
    },
    'ewb': {
        'Reference': 'Account',
        'DESCRIPTION': 'Description',
        'VOLUME': 'Volume',
        'UNIT PRICE': 'Unit\nPrice',
        'TOTAL CHARGE': 'Charge in Invoice CCY',
        'TOTAL CHARGE CCY': 'Invoice CCY',
        'source': '来源文件',
        'Date': '入账期间'
    },
    'ibc': {
        'Account': 'Account',
        'Description': 'Description',
        'Pricing Method': 'Pricing Method',
        'Volume': 'Volume',
        'Unit\nPrice': 'Unit\nPrice',
        'Unit Price CCY': 'Unit Price CCY',
        'Charge in Invoice CCY': 'Charge in Invoice CCY',
        'Invoice CCY': 'Invoice CCY',
        'Taxable': 'Taxable',
        '来源文件': '来源文件',
        'Invoice Period': '入账期间'
    },
    'jpm': {
        'DEPOSIT ACCOUNT': 'Account',
        'Description': 'Description',
        'Volume': 'Volume',
        'Unit\nPrice': 'Unit\nPrice',
        'Charge for\nService': 'Charge in Invoice CCY',
        'Curr': 'Invoice CCY',
        'Source': '来源文件'
    },
    'monoova': {
        'Reference': 'Account',
        'DESCRIPTION': 'Description',
        'VOLUME': 'Volume',
        'UNIT PRICE': 'Unit\nPrice',
        'TOTAL CHARGE': 'Charge in Invoice CCY',
        'TOTAL CHARGE CCY': 'Invoice CCY',
        'source': '来源文件',
        'Date': '入账期间'
    },
    'scb': {
        'Account': 'Account',
        'Description': 'Description',
        'Pricing Method': 'Pricing Method',
        'Volume': 'Volume',
        'Unit\nPrice': 'Unit\nPrice',
        'Unit Price CCY': 'Unit Price CCY',
        'Charge in Invoice CCY': 'Charge in Invoice CCY',
        'Invoice CCY': 'Invoice CCY',
        '汇率': '汇率',
        'Taxable': 'Taxable',
        '来源文件': '来源文件'
    },
    'xendit': {
        'Account': 'Account',
        'Description': 'Description',
        'Pricing Method': 'Pricing Method',
        'Volume': 'Volume',
        'Unit\nPrice': 'Unit\nPrice',
        'Unit Price CCY': 'Unit Price CCY',
        'Charge in Invoice CCY': 'Charge in Invoice CCY',
        'Invoice CCY': 'Invoice CCY',
        'Taxable': 'Taxable',
        '来源文件': '来源文件',
        'Invoice Period': '入账期间'
    }
}

# 银行对应的处理函数
BANK_SCRIPTS = {
    'banking_circle': banking_circle.main,
    'barclays': barclays.main,
    'citi': citi.main,
    'db': db.process_all,
    'ewb': ewb.main,
    'ibc': ibc.process_pdfs,
    'jpm': jpm.process_all,
    'monoova': monoova.main,
    'scb': scb.extract_scb_pdfs,
    'xendit': xendit.process_all
}


def _bill_arc_skip(p: Path) -> bool:
    return "__MACOSX" in {x.casefold() for x in p.parts}


def _first_bank_key_folder(folder_name: str) -> str | None:
    for bank_key in BANK_SCRIPTS.keys():
        if _folder_matches_bank(folder_name, bank_key):
            return bank_key
    return None


def iter_bill_bank_input_dirs(root_dir: str | os.PathLike[str]) -> list[tuple[Path, str]]:
    """枚举银行账单输入目录（支持 2026.03账单/CITI账单 等多层嵌套），与 channel_prescan / UI 预扫一致。"""
    root_path = Path(root_dir).expanduser().resolve()
    if not root_path.is_dir():
        return []

    matched_paths: list[tuple[Path, str]] = []
    root = root_path
    for folder in sorted(root.rglob("*")):
        if not folder.is_dir() or _bill_arc_skip(folder):
            continue
        try:
            folder.relative_to(root)
        except ValueError:
            continue

        bk = _first_bank_key_folder(folder.name)
        if bk is None:
            continue

        nested_same_bank = False
        p = folder.parent
        while p != root:
            try:
                p.relative_to(root)
            except ValueError:
                break
            pb = _first_bank_key_folder(p.name)
            if pb == bk:
                nested_same_bank = True
                break
            p = p.parent

        if nested_same_bank:
            continue
        matched_paths.append((folder, bk))

    seen: set[str] = set()
    out: list[tuple[Path, str]] = []
    for folder, bk in matched_paths:
        key = str(folder.resolve())
        if key in seen:
            continue
        seen.add(key)
        out.append((folder, bk))
    out.sort(key=lambda t: str(t[0]))
    return out


def process_bank_folder(bank_key, input_dir, output_excel, folder_name: str | None = None):
    """
    folder_name: 用户侧子文件夹名（如「BC账单」走 banking_circle，巴克莱走 barclays）。
    """
    fn_label = (folder_name or Path(input_dir).name).strip() or input_dir
    func = BANK_SCRIPTS.get(bank_key)
    if func:
        try:
            print(
                f"\n[{bank_key.upper()}] 开始解析 — 子文件夹「{fn_label}」"
                f"（脚本 key={bank_key!r}）"
            )
            func(input_dir, output_excel)
            if os.path.exists(output_excel):
                # 读取生成的excel文件
                try:
                    if bank_key == 'citi':
                        # citi 的输出格式中，前两行可能是空行或标题，我们手动找到真实的表头
                        df_raw = pd.read_excel(output_excel, sheet_name='加工合并', header=None)
                        # 遍历前 10 行，寻找包含 'Account' 的行作为真正的列名
                        header_idx = 0
                        for idx, row in df_raw.head(10).iterrows():
                            if 'Account' in row.values or 'Account No' in row.values or 'Invoice No.' in row.values:
                                header_idx = idx
                                break
                        df = df_raw.iloc[header_idx + 1:].copy()
                        df.columns = df_raw.iloc[header_idx].values
                    else:
                        df = pd.read_excel(output_excel)
                    if df is not None and not df.empty:
                        df.columns = [str(c).strip() for c in df.columns]
                        if bank_key == 'ewb':
                            for old, new in (
                                ('UNIT\nPRICE', 'UNIT PRICE'),
                                ('Unit Price', 'UNIT PRICE'),
                            ):
                                if old in df.columns and new not in df.columns:
                                    df = df.rename(columns={old: new})
                    return df
                except Exception as e:
                    print(f"[{bank_key.upper()}] 子文件夹「{fn_label}」读取中间结果失败: {e}")
        except Exception as e:
            print(f"[{bank_key.upper()}] 子文件夹「{fn_label}」执行脚本异常: {e}")
    return None

def align_columns(df, bank_key):
    mapping = BANK_MAPPINGS.get(bank_key, {})
    # 重命名列
    df = df.rename(columns=mapping)
    
    # 补充缺失的目标列
    for col in TEMPLATE_COLUMNS:
        if col not in df.columns:
            df[col] = None
            
    # 特殊处理：如果没有“费项”，则用“Description”填充
    if '费项' in df.columns and 'Description' in df.columns:
        # DB有专门的Product Description映射到了费项，其他如果没有则用Description填充
        df['费项'] = df['费项'].fillna(df['Description'])
    
    # 根据用户要求，费项统一取 Description
    if bank_key == 'citi' and 'Description' in df.columns:
        df['Description'] = df['Description'].str.strip()
        
    df['费项'] = df['Description']
    
    # DB 银行：我们可能把 Service Description 和 Product Description 合并或者保留
    # if bank_key == 'db':
    #     desc = df['Description'].fillna('')
    #     feixiang = df['费项'].fillna('')
    #     df['Description'] = desc.astype(str) + " - " + feixiang.astype(str)
    #     df['Description'] = df['Description'].str.replace(r'^ - | - $', '', regex=True)
    #     df['Description'] = df['Description'].str.replace(r' - None|None - ', '', regex=True)

    # 过滤多余的列，仅保留模板中存在的列，并按照模板顺序排序
    df = df[TEMPLATE_COLUMNS]
    return df


def generate_summary(root_dir, output_file, midfile_dir=None):
    """
    :param root_dir: 各银行子文件夹根目录
    :param output_file: 最终合并的 xlsx 路径
    :param midfile_dir: 若提供，将各银行中间结果 `{银行}_temp.xlsx` 写入此目录并保留；否则使用系统临时目录并在结束后删除
    """
    root_path = Path(root_dir)
    if not root_path.exists():
        print(f"根目录 {root_dir} 不存在。")
        return

    all_dfs = []
    use_midfile = midfile_dir is not None and str(midfile_dir).strip() != ""
    if use_midfile:
        temp_dir = os.path.abspath(os.path.expanduser(str(midfile_dir).strip()))
        os.makedirs(temp_dir, exist_ok=True)
        print(f"各银行中间 xlsx 将保存至: {temp_dir}")
    else:
        temp_dir = tempfile.mkdtemp(prefix="bank_temp_")

    try:
        jobs = iter_bill_bank_input_dirs(root_path)
        if not jobs:
            # 兼容历史上「银行文件夹直接在账单根下一层」的布局
            for folder in root_path.iterdir():
                if not folder.is_dir() or _bill_arc_skip(folder):
                    continue
                matched_bank = None
                for bank_key in BANK_SCRIPTS.keys():
                    if _folder_matches_bank(folder.name, bank_key):
                        matched_bank = bank_key
                        break
                if matched_bank:
                    jobs.append((folder, matched_bank))

        for idx, (folder, matched_bank) in enumerate(jobs):
            folder_display = folder.name
            temp_excel = os.path.join(temp_dir, f"{matched_bank}_{idx}_temp.xlsx")
            df = process_bank_folder(
                matched_bank,
                str(folder),
                temp_excel,
                folder_name=folder.name,
            )

            if df is not None and not df.empty:
                df_aligned = align_columns(df, matched_bank)
                df_aligned['__Bank__'] = matched_bank.upper()
                all_dfs.append(df_aligned)
                print(
                    f"[{matched_bank.upper()}] 成功提取并对齐 {len(df_aligned)} 条数据。"
                    f"（子文件夹「{folder_display}」）"
                )
            else:
                print(
                    f"[{matched_bank.upper()}] 未提取到数据。"
                    f"（子文件夹「{folder_display}」）"
                )

        if not all_dfs:
            print("\n未能从任何文件夹中提取到数据。")
            return

        final_df = pd.concat(all_dfs, ignore_index=True)

        # 统一使用上传的 rules/files 侧车表（汇率 CSV、账户/费项 mapping CSV）填充绿区。
        try:
            from .rules_files_enrichment import enrich_bill_final_dataframe

            print("正在使用 rules/files/mapping + fx 侧车表填充 USD金额、主体、分行维度、类型…")
            final_df, fx_warns = enrich_bill_final_dataframe(final_df)
            for _w in fx_warns:
                print(_w)
        except Exception as e:
            print(f"rules/files 账单绿区填充失败（仍将输出 SLIM 对齐列，绿区多为空）: {e}")

        # 确保移除辅助列 __Bank__
        if '__Bank__' in final_df.columns:
            final_df = final_df.drop(columns=['__Bank__'])

        # 汇总结果写入独立 xlsx，不修改模版文件；表头与 TEMPLATE_COLUMNS 一致，工作表名与模版「账单」一致便于对照
        try:
            final_df = final_df.fillna("")
            with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                final_df.to_excel(writer, sheet_name="账单", index=False)
            print(f"\n[完成] 所有银行数据汇总完成，已保存至: {output_file}")
        except Exception as e:
            print(f"保存汇总 Excel 时出错: {e}")
            final_df.to_excel(output_file, index=False)
            print(f"已使用备用方式保存至: {output_file}")
            
    finally:
        if not use_midfile:
            shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="各大银行账单汇总工具")
    parser.add_argument("--input", "-i", type=str, default=r"e:\\Desktop\\demo", help="总文件夹入口")
    parser.add_argument("--output", "-o", type=str, default=r"e:\\Desktop\\demo\\汇总结果.xlsx", help="输出的汇总Excel路径")
    
    args = parser.parse_args()
    generate_summary(args.input, args.output)
