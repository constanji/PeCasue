import os
import re
import pandas as pd
import pdfplumber

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_DIR = os.path.join(CURRENT_DIR, "SCB账单")
OUTPUT_EXCEL = os.path.join(CURRENT_DIR, "2026.2SCB.xlsx")

"""
    渣打银行账单解析工具
    """


def _scb_password_candidates(filename: str) -> list[str]:
    """
    电邮/附件名常见形如 "PING7000 AE_1935854_....pdf" —— 打开密码通常即 **PING+数字** 整段。
    旧逻辑 "PING" + filename[2:6] 在文件名以 PING 开头时会把 [2:6] 取成 "NG70" 等，得到
    错误密码 *PINGNG70*，从而 Pdfminer: PDFPasswordIncorrect。
    """
    base = os.path.basename(filename)
    cands: list[str] = []
    m = re.search(r"(?i)PING(\d+)", base)
    if m:
        cands.append(f"PING{m.group(1)}")
    if len(base) >= 6:
        legacy = "PING" + base[2:6]
        if legacy not in cands:
            cands.append(legacy)
    cands.append("")
    # 去重且保持顺序
    seen: set[str] = set()
    out: list[str] = []
    for p in cands:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _is_passwordish_exception(e: BaseException) -> bool:
    s = (str(e) or "").lower()
    t = type(e).__name__.lower()
    return (
        "password" in s
        or "password" in t
        or "encrypted" in s
        or "decrypt" in s
        or "pdfminer" in t
    )


def _open_scb_pdf(pdf_path: str, filename: str):
    """依次尝试多组密码打开（含加密）PDF，返回 (pdf, password_used)。"""
    last_err: Exception | None = None
    for password in _scb_password_candidates(filename):
        pdf = None
        try:
            pdf = pdfplumber.open(pdf_path, password=password)
            if password:
                print(f"已用文件名推导密码打开（仅提示长度: {len(password)}）")
            return pdf, password
        except Exception as e:
            if pdf is not None:
                try:
                    pdf.close()
                except Exception:
                    pass
            if _is_passwordish_exception(e):
                last_err = e
                continue
            raise
    if last_err is not None:
        raise last_err
    raise RuntimeError("无法打开 PDF")


def extract_fx_rate_from_text(text):
    """
    从 Settlement Summary 提取折算率，如：
    Fx Conversion Rate as at 31/01/26
    AED 1 = USD 0.272255528
    返回：(账单币种, 1 单位该币种折合的 USD) 元组；未找到则 None。
    """
    if not text:
        return None
    m = re.search(
        r'Fx\s+Conversion\s+Rate[^\n]*(?:\n\s*)?([A-Z]{3})\s*1\s*=\s*USD\s+([\d,.]+)',
        text,
        re.IGNORECASE | re.MULTILINE,
    )
    if m:
        return (m.group(1).upper(), float(m.group(2).replace(',', '')))
    m = re.search(r'\b([A-Z]{3})\s*1\s*=\s*USD\s+([\d,.]+)\b', text)
    if m:
        return (m.group(1).upper(), float(m.group(2).replace(',', '')))
    return None


def extract_scb_pdfs(input_dir, output_file):
    all_rows = []
    failed_items: list[str] = []

    if not os.path.exists(input_dir):
        print(f"文件夹不存在: {input_dir}")
        return

    for filename in os.listdir(input_dir):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(input_dir, filename)
            print(f"==================================================")
            print(f"正在处理：{filename}")

            try:
                pdf, _pw_used = _open_scb_pdf(pdf_path, filename)
                with pdf:
                    full_text = "\n".join(
                        (p.extract_text() or "") for p in pdf.pages
                    )
                    fx_pair = extract_fx_rate_from_text(full_text)
                    file_fx_rate = fx_pair[1] if fx_pair else None
                    if fx_pair is not None:
                        print(f"提取汇率: 1 {fx_pair[0]} = USD {fx_pair[1]}")

                    for page_num, page in enumerate(pdf.pages, 1):
                        text = page.extract_text()
                        if not text:
                            continue
                            
                        lines = text.split('\n')
                        in_activity_summary = False
                        current_account = ""
                        current_product = ""
                        
                        invoice_ccy = "AED"  # Default
                        
                        for i, line in enumerate(lines):
                            line = line.strip()
                            if not line:
                                continue
                                
                            if line.startswith("ACTIVITY SUMMARY FOR"):
                                in_activity_summary = True
                                # 尝试获取下一行作为 Account
                                if i + 1 < len(lines):
                                    account_line = lines[i+1].strip()
                                    if re.match(r'^\d{8,}\s*\(.*?\)', account_line):
                                        current_account = account_line
                                continue
                                
                            if not in_activity_summary:
                                continue
                                
                            # 尝试获取账单基础货币 (如 AMOUNT 下方的 (AED))
                            if re.match(r'^\([A-Z]{3}\)$', line):
                                invoice_ccy = line.strip('()')
                                continue
                                
                            # Detail line match (Pricing Method, CCY, Unit Price, Volume, Charge)
                            # 匹配如 "Transaction Fee AED 36.75 389 14,295.75"
                            # 或者 "Monthly Charges (** Charge Waived **) AED 0.00 1 0.00"
                            # 注意: Unit Price CCY (如 AED/USD) 总是3个大写字母
                            match = re.match(r'^(.*?)\s+([A-Z]{3})\s+([\d,.]+)\s+([\d,]+)\s+([\d,.]+)$', line)
                            if match:
                                pricing_method = match.group(1).strip()
                                ccy = match.group(2)
                                unit_price = float(match.group(3).replace(',', ''))
                                volume = int(match.group(4).replace(',', ''))
                                charge = float(match.group(5).replace(',', ''))
                                
                                row = {
                                    'Account': current_account,
                                    'Description': current_product,
                                    'Pricing Method': pricing_method,
                                    'Volume': volume,
                                    'Unit\nPrice': unit_price,
                                    'Unit Price CCY': ccy,
                                    'Charge in Invoice CCY': charge,
                                    'Invoice CCY': invoice_ccy,
                                    '汇率': file_fx_rate,
                                    'Taxable': '',
                                    '来源文件': filename
                                }
                                all_rows.append(row)
                                print(f"提取明细: {current_product} -> {pricing_method} | {volume} x {unit_price} {ccy} = {charge}")
                            # Skip Headers and Metadata
                            elif line.isupper() and not "ACTIVITY SUMMARY" in line and not "PRODUCT CCY" in line and not "(AED)" in line and not "(USD)" in line:
                                # 这是大类（如 CASH-PAYMENTS），忽略
                                pass
                            elif "Net Charges" in line or "Total Amount Due" in line or "Charge Details" in line or "PRODUCT CCY" in line or line == current_account:
                                # 忽略汇总行和表头
                                pass
                            else:
                                # 剩余的即为 Product (Description)
                                current_product = line

                # 本 PDF 是否贡献了至少一行明细
                file_rows = [r for r in all_rows if r.get("来源文件") == filename]
                if not file_rows:
                    hint = (
                        "无明细行：可能无 ACTIVITY SUMMARY 块、或版式与脚本正则不符；"
                        f"若曾为加密 PDF，已尝试自文件名解析 PING+数字 等密码（末次密码长度={len(_pw_used) if _pw_used else 0}）"
                    )
                    line = f"[SCB] 警告: {filename} — {hint}"
                    print(line)
                    failed_items.append(line)

            except Exception as e:
                s = str(e) or ""
                if "password" in s.lower() or "encrypted" in s.lower() or "decrypt" in s.lower():
                    extra = "（PDF 可能需正确密码或文件已加密）"
                else:
                    extra = ""
                line = (
                    f"[SCB] 警告: {filename} 处理失败 — {type(e).__name__}: {e!r}{extra}"
                )
                print(line)
                failed_items.append(line)

    if all_rows:
        # 直接使用固定的表头
        columns = ['Account', 'Description', 'Pricing Method', 'Volume', 'Unit\nPrice', 'Unit Price CCY', 'Charge in Invoice CCY', 'Invoice CCY', '汇率', 'Taxable', '来源文件']
            
        df_result = pd.DataFrame(all_rows, columns=columns)
        
        # 写入数据
        df_result.to_excel(output_file, index=False)
        print(f"\n==================================================")
        print(f"全部完成！共提取 {len(all_rows)} 条记录，已成功生成汇总文件 {output_file}")
    else:
        print("\n没有提取到任何数据")
    if failed_items:
        print("\n[SCB] 未成功解析或零明细的文件清单：")
        for it in failed_items:
            print("  -", it)

if __name__ == "__main__":
    extract_scb_pdfs(PDF_DIR, OUTPUT_EXCEL)
