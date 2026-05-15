"""MapperAgent — migrated from pingpong-master.

The pingpong implementation hands transactions + a rule book to an LLM and
asks it to map ``主体 / 分行维度 / 费项 / 类型 / 入账科目``. For Phase 7 we
keep the same async surface but apply the rules deterministically against the
PeCause rule store. When ``PIPELINE_LLM_ENABLED=1`` we additionally invoke a
ReActAgent for any row the deterministic step could not resolve, so the LLM
acts as a fallback rather than the source of truth.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from server.agents.config import llm_enabled
from server.rules import store as rule_store
from server.rules.schema import RuleKind


def _index_rules(kind: RuleKind, key_col: str) -> Dict[str, Dict[str, Any]]:
    table = rule_store.load_rule(kind)
    out: Dict[str, Dict[str, Any]] = {}
    for row in table.rows:
        k = str(row.get(key_col, "")).strip()
        if k:
            out[k] = row
    return out


class _MappingRow(BaseModel):
    index: int
    entity: Optional[str] = Field(default=None, alias="主体")
    branch: Optional[str] = Field(default=None, alias="分行维度")
    fee_item: Optional[str] = Field(default=None, alias="费项")
    category_type: Optional[str] = Field(default=None, alias="类型")
    subject: Optional[str] = Field(default=None, alias="入账科目")

    class Config:
        populate_by_name = True


class _MappingResponse(BaseModel):
    mappings: List[_MappingRow] = Field(default_factory=list)


class MapperAgent:
    """Apply business mapping rules to bank transaction rows."""

    def __init__(self) -> None:
        self._llm_enabled = llm_enabled()
        self._react = None
        if self._llm_enabled:
            try:
                from agentscope.agent import ReActAgent  # type: ignore

                from server.agents.config import get_chat_model

                model, formatter = get_chat_model()
                self._react = ReActAgent(
                    name="BusinessMapper",
                    sys_prompt=(
                        "You are an expert financial controller categorising bank "
                        "transactions against an internal rule book. Always return "
                        "structured JSON, one mapping per input index."
                    ),
                    model=model,
                    formatter=formatter,
                )
            except Exception:
                self._react = None

    async def apply_business_rules(
        self,
        rows: List[Dict[str, Any]],
        rules_context: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        account_idx = _index_rules(RuleKind.ACCOUNT_MAPPING, key_col="账户号")
        fee_idx = _index_rules(RuleKind.FEE_MAPPING, key_col="原始描述")
        out: List[Dict[str, Any]] = []
        unresolved: List[Dict[str, Any]] = []
        for i, raw in enumerate(rows):
            acct = str(raw.get("Account") or raw.get("账户号") or "").strip()
            desc = str(raw.get("Description") or raw.get("描述") or "").strip()
            acc_row = account_idx.get(acct, {})
            fee_row = fee_idx.get(desc, {})
            mapped = {
                "index": i,
                "主体": acc_row.get("主体"),
                "分行维度": acc_row.get("分行维度"),
                "费项": desc,
                "类型": fee_row.get("类型"),
                "入账科目": fee_row.get("入账科目") or "费用",
                "_resolved": bool(acc_row) and bool(fee_row),
            }
            out.append(mapped)
            if not mapped["_resolved"]:
                unresolved.append(
                    {"index": i, "Account": acct, "Description": desc}
                )

        # If LLM is on and we have unresolved rows, give it the rule book +
        # the unresolved rows and merge any structured suggestions back in.
        if self._react is not None and unresolved:
            try:
                from agentscope.message import Msg  # type: ignore

                ctx = rules_context or _format_rules_context(account_idx, fee_idx)
                prompt = (
                    "Categorize the following unresolved transactions using the "
                    "rule book. Return strict JSON: {\"mappings\":[{...}]}\n\n"
                    f"RULES:\n{ctx}\n\n"
                    f"TRANSACTIONS:\n{json.dumps(unresolved, ensure_ascii=False)}"
                )
                resp = await self._react(
                    Msg("user", prompt, "user"),
                    structured_model=_MappingResponse,
                )
                meta = getattr(resp, "metadata", None)
                parsed: List[Dict[str, Any]] = []
                if isinstance(meta, dict):
                    parsed = meta.get("mappings", []) or []
                elif isinstance(meta, _MappingResponse):
                    parsed = [m.model_dump(by_alias=True) for m in meta.mappings]
                for item in parsed:
                    idx = item.get("index")
                    if not isinstance(idx, int) or idx < 0 or idx >= len(out):
                        continue
                    target = out[idx]
                    for key in ("主体", "分行维度", "类型", "入账科目"):
                        val = item.get(key)
                        if val and not target.get(key):
                            target[key] = val
                    target["_resolved"] = True
                    target["_resolved_by"] = "llm"
            except Exception:
                pass

        return out


def _format_rules_context(
    account_idx: Dict[str, Dict[str, Any]],
    fee_idx: Dict[str, Dict[str, Any]],
) -> str:
    lines: List[str] = ["# 账户 → 主体/分行维度"]
    for k, v in list(account_idx.items())[:50]:
        lines.append(f"- {k} → 主体={v.get('主体')} 分行维度={v.get('分行维度')}")
    lines.append("\n# 费项 → 类型/入账科目")
    for k, v in list(fee_idx.items())[:50]:
        lines.append(f"- {k} → 类型={v.get('类型')} 入账科目={v.get('入账科目')}")
    return "\n".join(lines)
