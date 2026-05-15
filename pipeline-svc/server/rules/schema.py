"""Static-rules schema (plan §4.3 / §6.1).

Seven canonical kinds, all stored as flexible {columns, rows} tables; the
password book is special and never returned in plaintext to non-admin callers.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class RuleKind(str, Enum):
    ACCOUNT_MAPPING = "account_mapping"
    FEE_MAPPING = "fee_mapping"
    FX = "fx"
    OWN_FLOW_PROCESSING = "own_flow_processing"
    SPECIAL_BRANCH_MAPPING = "special_branch_mapping"
    RESULT_TEMPLATE = "result_template"
    PASSWORD_BOOK = "password_book"
    # 模版.xlsx · 客资流水三张 mapping（与 Allline / pingpong 工作表名一致）
    CUSTOMER_MAPPING = "customer_mapping"
    CUSTOMER_FEE_MAPPING = "customer_fee_mapping"
    CUSTOMER_BRANCH_MAPPING = "customer_branch_mapping"


# Default columns per kind. Frontend can add / remove freely; this just seeds
# brand-new rule tables with sensible headers.
DEFAULT_COLUMNS: Dict[RuleKind, List[str]] = {
    RuleKind.ACCOUNT_MAPPING: ["银行", "账号", "主体", "分行", "币种", "备注"],
    RuleKind.FEE_MAPPING: ["银行", "原始描述", "费项分类", "入账科目", "备注"],
    RuleKind.FX: ["货币对", "汇率(USD)", "生效期次", "来源", "备注"],
    RuleKind.OWN_FLOW_PROCESSING: [
        "数据源",
        "渠道",
        "主体",
        "文件",
        "表头",
        "处理",
        "备注",
        "入账科目",
        "说明",
    ],
    RuleKind.SPECIAL_BRANCH_MAPPING: ["渠道", "主体", "分行", "对端", "备注"],
    RuleKind.RESULT_TEMPLATE: ["模板名", "字段", "默认值", "是否必填", "校验规则"],
    RuleKind.PASSWORD_BOOK: ["scope", "pattern", "password", "备注", "expires_at"],
    RuleKind.CUSTOMER_MAPPING: ["（上传模版「客资流水MAPPING」导入）"],
    RuleKind.CUSTOMER_FEE_MAPPING: ["（上传模版「客资流水费项mapping表」导入）"],
    RuleKind.CUSTOMER_BRANCH_MAPPING: ["（上传模版「客资流水分行mapping」导入）"],
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RuleTable(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    note: Optional[str] = None
    meta: Optional[Dict[str, Any]] = Field(
        default=None,
        description="扩展字段（如汇率 fx_month_label，由用户在规则页手动选择或导入时传入）",
    )


class RuleManifestEntry(BaseModel):
    kind: RuleKind
    version: int = 1
    updated_at: str = Field(default_factory=_utc_now)
    updated_by: Optional[str] = None
    rows_count: int = 0


class RuleManifest(BaseModel):
    entries: Dict[str, RuleManifestEntry] = Field(default_factory=dict)
    created_at: str = Field(default_factory=_utc_now)
    updated_at: str = Field(default_factory=_utc_now)


class RuleVersionRow(BaseModel):
    version: int
    snapshot_path: str
    author: Optional[str]
    note: Optional[str]
    created_at: str
