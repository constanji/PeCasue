"""Rule CRUD endpoints (plan §4.3 / §6.3)."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, Header, HTTPException, UploadFile

from pydantic import BaseModel

from server.core.paths import (
    ensure_data_directories,
    get_rules_allocation_citihk_pphk_template_path,
    get_rules_allocation_quickbi_template_path,
    get_rules_cost_allocate_workbook_path,
)
from server.parsers._legacy.own_flow_pkg.mapping_import import MappingImportError
from server.rules import excel_import as rule_excel_import
from server.rules import store as rule_store
from server.rules.password_book import (
    load_book as pb_load,
    upsert_book as pb_upsert,
)
from server.rules.schema import DEFAULT_COLUMNS, RuleKind, RuleManifest, RuleTable

router = APIRouter()


class AllocationTemplatesStatus(BaseModel):
    quickbi_present: bool
    citihk_present: bool
    cost_allocate_workbook_present: bool


@router.get("/rules/allocation-templates/status", response_model=AllocationTemplatesStatus)
async def allocation_templates_status() -> AllocationTemplatesStatus:
    """磁盘是否已有分摊基数 / 成本分摊所需固定路径模版 xlsx（不参与 RuleStore 版本）。"""
    ensure_data_directories()
    qb = get_rules_allocation_quickbi_template_path()
    hk = get_rules_allocation_citihk_pphk_template_path()
    ca = get_rules_cost_allocate_workbook_path()
    return AllocationTemplatesStatus(
        quickbi_present=qb.is_file(),
        citihk_present=hk.is_file(),
        cost_allocate_workbook_present=ca.is_file(),
    )


@router.post("/rules/allocation-templates/upload")
async def allocation_templates_upload(
    template_kind: str = Form(...),
    file: UploadFile = File(...),
):
    """写入固定路径：quickbi / citihk PPHK 模版，或 ``allocation/成本分摊基数+输出模板.xlsx``。"""
    ensure_data_directories()
    kind = (template_kind or "").strip().lower()
    if kind == "quickbi":
        dest = get_rules_allocation_quickbi_template_path()
    elif kind == "citihk":
        dest = get_rules_allocation_citihk_pphk_template_path()
    elif kind in ("cost_allocate", "cost_allocate_workbook"):
        dest = get_rules_cost_allocate_workbook_path()
        kind = "cost_allocate"
    else:
        raise HTTPException(
            status_code=400,
            detail="template_kind must be quickbi, citihk, or cost_allocate",
        )
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    suffix = Path(file.filename or "upload.xlsx").suffix.lower()
    if suffix not in (".xlsx", ".xlsm"):
        raise HTTPException(status_code=400, detail="需要 .xlsx / .xlsm")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return {"ok": True, "template_kind": kind}


def _is_admin(role: Optional[str]) -> bool:
    return (role or "").upper() == "ADMIN"


def _own_flow_table_drop_match_condition(table: RuleTable) -> RuleTable:
    """Legacy UI column; matching DSL lives in「处理」(see matched_rule_verify)."""
    if "match_condition" not in table.columns:
        return table
    cols = [c for c in table.columns if c != "match_condition"]
    rows = [{k: v for k, v in dict(r).items() if k != "match_condition"} for r in table.rows]
    return RuleTable(columns=cols, rows=rows, note=table.note, meta=table.meta)


@router.get("/rules/manifest", response_model=RuleManifest)
async def get_manifest() -> RuleManifest:
    return rule_store.load_manifest()


@router.get("/rules/{kind}")
async def get_rule(
    kind: RuleKind,
    x_pecause_user_role: Optional[str] = Header(default=None),
):
    if kind == RuleKind.PASSWORD_BOOK:
        rows = pb_load(mask=not _is_admin(x_pecause_user_role))
        return {
            "kind": kind.value,
            "table": {
                "columns": list(DEFAULT_COLUMNS[kind]),
                "rows": rows,
                "note": None,
            },
            "is_password_book": True,
            "masked": not _is_admin(x_pecause_user_role),
        }
    table = rule_store.load_rule(kind)
    if kind == RuleKind.OWN_FLOW_PROCESSING:
        table = _own_flow_table_drop_match_condition(table)
    return {"kind": kind.value, "table": table.model_dump(mode="json")}


class PutRuleBody(BaseModel):
    table: RuleTable
    note: Optional[str] = None


@router.put("/rules/{kind}")
async def put_rule(
    kind: RuleKind,
    body: PutRuleBody,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
):
    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    if kind == RuleKind.PASSWORD_BOOK:
        rows = body.table.rows
        merged = pb_upsert(rows, preserve_unchanged_passwords=True)
        return {
            "kind": kind.value,
            "rows_count": len(merged),
            "is_password_book": True,
        }
    table_in = (
        _own_flow_table_drop_match_condition(body.table)
        if kind == RuleKind.OWN_FLOW_PROCESSING
        else body.table
    )
    entry = rule_store.save_rule(kind, table_in, author=actor, note=body.note)
    sidecars = rule_excel_import.sync_rule_table_sidecars(kind, table_in)
    return {
        "kind": kind.value,
        "entry": entry.model_dump(mode="json"),
        "sidecars": sidecars,
    }


@router.post("/rules/import/fx")
async def rules_import_fx(
    file: UploadFile = File(...),
    note: Optional[str] = Form(None),
    fx_month_label: Optional[str] = Form(
        None,
        description="所属月份：YYYY-MM 或「YYYY年M月」；不传则沿用当前 fx 规则 meta",
    ),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
):
    """Standalone FX workbook → ``RuleKind.FX`` JSON + ``rules/files/fx/*.csv``."""
    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        return rule_excel_import.import_upload_bytes(
            data,
            file.filename or "upload.xlsx",
            mode="fx_standalone",
            author=actor,
            note=note,
            fx_month_label=fx_month_label,
        )
    except MappingImportError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/rules/import/own_flow_template")
async def rules_import_own_flow_template(
    file: UploadFile = File(...),
    note: Optional[str] = Form(None),
    scope: Optional[str] = Form(
        None,
        description="可选：all（默认）| account_mapping | fee_mapping | fx | own_flow_processing",
    ),
    fx_month_label: Optional[str] = Form(
        None,
        description="写入 fx 规则时的所属月份（scope 含 fx 或 all 时生效）",
    ),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
):
    """模版工作簿导入；默认写入四类自有流水相关规则，亦可 scope 仅覆盖其中一类。"""
    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        return rule_excel_import.import_upload_bytes(
            data,
            file.filename or "upload.xlsx",
            mode="own_flow_template",
            author=actor,
            note=note,
            scope=scope,
            fx_month_label=fx_month_label,
        )
    except MappingImportError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/rules/import/customer_flow_template")
async def rules_import_customer_flow_template(
    file: UploadFile = File(...),
    note: Optional[str] = Form(None),
    scope: Optional[str] = Form(
        None,
        description=(
            "可选：all（默认）| customer_mapping | customer_fee_mapping | "
            "customer_branch_mapping"
        ),
    ),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
):
    """客资模版三张 mapping → RuleStore + ``rules/files/mapping/*.csv``。"""
    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        return rule_excel_import.import_upload_bytes(
            data,
            file.filename or "upload.xlsx",
            mode="customer_flow_template",
            author=actor,
            note=note,
            scope=scope,
        )
    except MappingImportError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/rules/{kind}/versions")
async def get_versions(kind: RuleKind):
    if kind == RuleKind.PASSWORD_BOOK:
        return {"kind": kind.value, "versions": [], "note": "password_book is not versioned"}
    return {"kind": kind.value, "versions": rule_store.list_versions(kind)}


@router.post("/rules/{kind}/sync-sidecars")
async def sync_rule_sidecars_endpoint(kind: RuleKind):
    """将当前 RuleStore 快照重新写入 ``rules/files/*`` 侧车（执行通道读的文件）。"""
    if kind == RuleKind.PASSWORD_BOOK:
        raise HTTPException(status_code=400, detail="password_book has no sidecar files")
    table = rule_store.load_rule(kind)
    sidecars = rule_excel_import.sync_rule_table_sidecars(kind, table)
    return {"kind": kind.value, "sidecars": sidecars}


class RollbackBody(BaseModel):
    target_version: int
    note: Optional[str] = None


@router.post("/rules/{kind}/rollback")
async def rollback_rule(
    kind: RuleKind,
    body: RollbackBody,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
):
    if kind == RuleKind.PASSWORD_BOOK:
        raise HTTPException(status_code=400, detail="password_book is not versioned")
    actor = x_pecause_user_email or x_pecause_user_id or "anonymous"
    try:
        entry = rule_store.rollback(
            kind, target_version=body.target_version, author=actor, note=body.note
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    table = rule_store.load_rule(kind)
    sidecars = rule_excel_import.sync_rule_table_sidecars(kind, table)
    return {
        "kind": kind.value,
        "entry": entry.model_dump(mode="json"),
        "sidecars": sidecars,
    }
