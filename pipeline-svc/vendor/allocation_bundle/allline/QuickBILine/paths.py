from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_FILES = _ROOT / "files"
_QUICKBI = _FILES / "quickbi"

# PeCause：任务输入在 ``data/tasks/{tid}/extracted/allocation_base``；模版在 ``data/rules/files/allocation``。
# 此处仅为历史默认文件名占位（CLI）；流水线由 ``server.parsers.allocation.ops`` 传入真实路径。
DEFAULT_QUICKBI_INBOUND = _QUICKBI / "finance_channel_inbound_default.xlsx"
DEFAULT_QUICKBI_OUTBOUND = _QUICKBI / "finance_channel_outbound_default.xlsx"
DEFAULT_QUICKBI_VA = _QUICKBI / "finance_channel_valid_va_default.xlsx"
DEFAULT_SHOUFUKUAN_TEMPLATE = _QUICKBI / "收付款成本分摊基数表模版.xlsx"
DEFAULT_OUTPUT_QUICKBI = _FILES / "default_quickbi_out" / "收付款基数_QuickBI_out.xlsx"
