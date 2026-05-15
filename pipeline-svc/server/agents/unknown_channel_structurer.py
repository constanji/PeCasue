"""UnknownChannelStructurer — migrated from pingpong-master.

Phase 7 strategy: same as PasswordAgent — keep the public contract intact and
provide a deterministic best-effort structurer that uses simple keyword/column
heuristics. LLM enrichment is opt-in via `PIPELINE_LLM_ENABLED=1`.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from server.agents.config import llm_enabled


class StructuredRow(BaseModel):
    Account: str = ""
    Description: str = ""
    Pricing_Method: str = Field(default="", alias="Pricing Method")
    Volume: str = ""
    Unit_Price: str = Field(default="", alias="Unit Price")
    Unit_Price_CCY: str = Field(default="", alias="Unit Price CCY")
    Charge_in_Invoice_CCY: str = Field(default="", alias="Charge in Invoice CCY")
    Invoice_CCY: str = Field(default="", alias="Invoice CCY")
    Taxable: str = ""
    source_file: str = Field(default="", alias="来源文件")

    class Config:
        populate_by_name = True


class UnknownChannelStructureResponse(BaseModel):
    rows: List[StructuredRow] = Field(default_factory=list)
    confidence: float = 0.0
    reasoning: str = ""


_NUMERIC = re.compile(r"^-?\d+(?:\.\d+)?$")


def _heuristic(channel_key: str, payload: Dict[str, Any]) -> UnknownChannelStructureResponse:
    rows_in: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        for k in ("rows", "items", "records"):
            if isinstance(payload.get(k), list):
                rows_in.extend(payload[k])
    out: List[StructuredRow] = []
    for raw in rows_in[:200]:
        if not isinstance(raw, dict):
            continue
        row = StructuredRow(
            Account=str(raw.get("Account") or raw.get("account") or ""),
            Description=str(raw.get("Description") or raw.get("desc") or raw.get("description") or ""),
        )
        if "Pricing Method" in raw:
            row.Pricing_Method = str(raw["Pricing Method"])
        for k in ("Volume", "volume"):
            if k in raw:
                row.Volume = str(raw[k])
                break
        for k in ("Unit Price", "unit_price", "price"):
            if k in raw:
                row.Unit_Price = str(raw[k])
                break
        for k in ("Charge in Invoice CCY", "charge"):
            if k in raw:
                row.Charge_in_Invoice_CCY = str(raw[k])
                break
        for k in ("Invoice CCY", "invoice_ccy", "ccy"):
            if k in raw:
                row.Invoice_CCY = str(raw[k])
                break
        for k in ("来源文件", "source_file", "file"):
            if k in raw:
                row.source_file = str(raw[k])
                break
        out.append(row)
    return UnknownChannelStructureResponse(
        rows=out,
        confidence=0.5 if out else 0.1,
        reasoning=f"启发式提取 {len(out)} 行（无 LLM 后处理）",
    )


class UnknownChannelStructurer:
    def __init__(self) -> None:
        self._llm_enabled = llm_enabled()
        self._react = None
        if self._llm_enabled:
            try:
                from agentscope.agent import ReActAgent  # type: ignore

                from server.agents.config import get_chat_model

                model, formatter = get_chat_model()
                self._react = ReActAgent(
                    name="UnknownChannelStructurer",
                    sys_prompt=(
                        "You are a financial document analyst that organises raw "
                        "extracted content into structured fee rows."
                    ),
                    model=model,
                    formatter=formatter,
                )
            except Exception:
                self._react = None

    async def structure_content(
        self,
        channel_key: str,
        extracted_payload: Dict[str, Any],
    ) -> UnknownChannelStructureResponse:
        baseline = _heuristic(channel_key, extracted_payload)
        if self._react is None or baseline.confidence >= 0.5:
            return baseline
        try:
            from agentscope.message import Msg  # type: ignore

            response = await self._react(
                Msg(
                    "system",
                    f"CHANNEL: {channel_key}\n\nPAYLOAD:\n{extracted_payload}",
                    "user",
                ),
                structured_model=UnknownChannelStructureResponse,
            )
            meta = getattr(response, "metadata", None)
            if isinstance(meta, dict):
                return UnknownChannelStructureResponse(**meta)
            if isinstance(meta, UnknownChannelStructureResponse):
                return meta
        except Exception:
            pass
        return baseline
