"""自有流水：目标列与数据源标识。"""

from __future__ import annotations

DATA_SOURCE = "自有流水"

# 与 模版.xlsx「自有流水」表第 2 行英文表头一致（绿区说明：费项=即 description）
OUTPUT_COLUMNS = [
    "Branch Name",
    "Account Number",
    "Account Currency",
    "Last Entry Date",
    "Transaction Amount",
    "Product Type",
    "Transaction Description",
    "Payment Details",
    # CITI：多列 Name/Address 折叠后的首条非空，便于 Queen Bee 等规则校验
    "Name/Address",
    "来源文件",
    "USD金额",
    "入账期间",
    "主体",
    "渠道",
    "分行维度",
    "费项",
    "类型",
    "备注",
    "入账科目",
    "账户性质",
    # PPEU 各子表规则匹配列（与流水线 first_matching_rule 一致；非 PPEU 行为空）
    "mark（财务）",
    # 流水线诊断列（供核对命中处理表；与 own_flow.matched_rule_verify 语义一致：rule 非空即命中）
    "file_group",
    "matched_rule_kind",
    "matched_rule_pattern",
]

# 从各渠道原始行读取的字段（含 Account / Merchant ID，输出时合并进 Account Number）
CITI_RAW_KEYS = [
    "Branch Name",
    "Account Number",
    "Account",
    "Merchant ID",
    "Account Currency",
    "Last Entry Date",
    "Transaction Amount",
    "Product Type",
    "Transaction Description",
    "Payment Details",
]
