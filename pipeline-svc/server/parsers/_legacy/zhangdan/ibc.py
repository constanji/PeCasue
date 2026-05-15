import io
import os
import re

import fitz
import pandas as pd

"""
    芝加哥国际银行账单解析工具
    优先 PaddleOCR；若未安装/不可用，则回退 Tesseract + pytesseract（需本机安装 tesseract 可执行文件）。
"""

# Paddle 懒加载
ocr = None
_OCR_INIT_ERROR: str | None = None
_TESSERACT_CHECKED: bool = False
_TESSERACT_OK: bool = False


def _ensure_paddle_ocr():
    global ocr, _OCR_INIT_ERROR
    if ocr is not None:
        return ocr
    if _OCR_INIT_ERROR is not None:
        return None
    try:
        from paddleocr import PaddleOCR

        ocr = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)
        return ocr
    except Exception as e:  # noqa: BLE001
        _OCR_INIT_ERROR = f"{type(e).__name__}: {e}"
        return None


def _tesseract_usable() -> bool:
    """本机 tesseract 可执行文件可用（pytesseract 仅为封装）。"""
    global _TESSERACT_CHECKED, _TESSERACT_OK
    if _TESSERACT_CHECKED:
        return _TESSERACT_OK
    _TESSERACT_CHECKED = True
    try:
        import pytesseract

        pytesseract.get_tesseract_version()
        _TESSERACT_OK = True
    except Exception:
        _TESSERACT_OK = False
    return _TESSERACT_OK


def _text_from_paddle(ocr_inst, img_bytes: bytes) -> str:
    result = ocr_inst.ocr(img_bytes, cls=True)
    if not result:
        return ""
    buf = []
    for line in result:
        if not line:
            continue
        for word in line:
            if word and len(word) > 1:
                buf.append(word[1][0])
    return " ".join(buf) + " "


def _text_from_tesseract(img_bytes: bytes) -> str:
    from PIL import Image
    import pytesseract

    im = Image.open(io.BytesIO(img_bytes))
    return pytesseract.image_to_string(im, lang="eng") + "\n"


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_DIR = os.path.join(CURRENT_DIR, "IBC账单")
EXCEL_TEMPLATE = os.path.join(CURRENT_DIR, "2026.2IBC.xlsx")


def extract_info_from_text(text, filename):
    print(f"正在分析提取内容...")

    # 尝试从文件名提取 Account 和 Invoice Period
    account = ""
    invoice_period = ""
    # 匹配例如 MSA2-202601
    filename_match = re.search(r"^([A-Za-z0-9]+)-(\d{6})", filename)
    if filename_match:
        account = filename_match.group(1)
        invoice_period = filename_match.group(2)

    # 从文本中提取 Invoice # 和 Date
    invoice_no = ""
    issuing_date = ""
    # 匹配例如 S2202601 2026/01/31 $7,591.50
    inv_match = re.search(
        r"([A-Z0-9]+)\s+(\d{4}/\d{2}/\d{2})\s+\$([\d,.]+)", text
    )
    if inv_match:
        invoice_no = inv_match.group(1)
        issuing_date = inv_match.group(2)

    # 提取各项 Fee 费用
    fee_pattern = r"([A-Za-z\s]+?Fee):\s*\$([\d,.]+)(?:\s*\(?Count:\s*(\d+)\s*@\s*\$([\d,.]+)\)?)?"
    fees = re.findall(fee_pattern, text)

    rows = []
    for fee in fees:
        desc = fee[0].strip()
        charge = float(fee[1].replace(",", ""))

        volume = int(fee[2]) if fee[2] else ""
        unit_price = float(fee[3].replace(",", "")) if fee[3] else ""

        row = {
            "Account": account,
            "Issuing Date": issuing_date,
            "Invoice Period": invoice_period,
            "Invoice No": invoice_no,
            "Description": desc,
            "Pricing Method": "",
            "Volume": volume,
            "Unit\nPrice": unit_price,
            "Unit Price CCY": "USD" if unit_price != "" else "",
            "Charge in Invoice CCY": charge,
            "Invoice CCY": "USD",
            "Taxable": "",
            "来源文件": filename,
        }
        rows.append(row)

    return rows


def process_pdfs(input_dir, output_file):
    all_rows = []

    paddle = _ensure_paddle_ocr()
    if paddle is not None:
        ocr_mode = "paddle"
        ocr_inst = paddle
    elif _tesseract_usable():
        ocr_mode = "tesseract"
        ocr_inst = None
        err = _OCR_INIT_ERROR or "unknown"
        print(
            f"[IBC] PaddleOCR 不可用（{err}），已改用 Tesseract-OCR + pytesseract。"
            " 识别率可能略低于 Paddle，英文账单通常仍可用。"
        )
    else:
        err = _OCR_INIT_ERROR or "unknown"
        print(
            f"[IBC] 已跳过: 无可用 OCR。Paddle 原因: {err}；"
            "Tesseract: 本机未检测到 tesseract（请安装: macOS `brew install tesseract`，"
            "并确保在 PATH 中）。"
            " 若需 Paddle，请用 Python 3.12/3.11 venv 并安装 paddlepaddle + paddleocr<3。"
        )
        return

    if not os.path.exists(input_dir):
        print(f"[错误] 文件夹不存在: {input_dir}")
        return

    for filename in os.listdir(input_dir):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(input_dir, filename)
            print(f"==================================================")
            print(f"正在处理：{filename}")

            doc = fitz.open(pdf_path)
            full_text = ""
            for page_num, page in enumerate(doc, 1):
                print(f"识别第 {page_num} 页（引擎={ocr_mode}）...")
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                img_bytes = pix.tobytes("png")
                if ocr_mode == "paddle":
                    full_text += _text_from_paddle(ocr_inst, img_bytes)
                else:
                    full_text += _text_from_tesseract(img_bytes)

            rows = extract_info_from_text(full_text, filename)
            if rows:
                print(f"[成功] 成功从 {filename} 提取 {len(rows)} 条费用记录。")
            else:
                print(f"[警告] 未从 {filename} 提取到任何费用记录。")
            all_rows.extend(rows)

    if all_rows:
        try:
            df_template = pd.read_excel(EXCEL_TEMPLATE)
            columns = df_template.columns.tolist()
        except Exception as e:
            print("读取模板失败，将使用默认列名。错误:", e)
            columns = [
                "Account",
                "Issuing Date",
                "Invoice Period",
                "Invoice No",
                "Description",
                "Pricing Method",
                "Volume",
                "Unit\nPrice",
                "Unit Price CCY",
                "Charge in Invoice CCY",
                "Invoice CCY",
                "Taxable",
                "来源文件",
            ]

        df_result = pd.DataFrame(all_rows, columns=columns)

        df_result.to_excel(output_file, index=False)
        print(f"\n==================================================")
        print(
            f"[完成] 全部完成！共提取 {len(all_rows)} 条记录，已成功写入模板 {output_file}"
        )
    else:
        print("\n[警告] 没有提取到任何数据")


if __name__ == "__main__":
    process_pdfs(PDF_DIR, EXCEL_TEMPLATE)
