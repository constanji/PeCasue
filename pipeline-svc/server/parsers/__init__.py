"""Parser registry: maps a channel_id to its concrete parser class.

Phase 3 ships **placeholder parsers** that emit a real manifest.json + a
representative csv per channel so the rest of the pipeline (status machine,
download, replace, compare, observe) can be exercised end-to-end. Real allline
/ pingpong-master implementations are migrated incrementally in Phase 3.x.
"""
from __future__ import annotations

from typing import Dict, Type

from server.parsers.base import BaseParser
from server.parsers.bill.parser import BillParser
from server.parsers.own_flow.parser import OwnFlowParser
from server.parsers.customer.parser import CustomerParser
from server.parsers.special.parser import (
    FinalMergeParser,
    SpecialAchRefundParser,
    SpecialMergeBundleParser,
    SpecialOpIncomingParser,
    SpecialOpRefundParser,
    SpecialTransferParser,
)
from server.parsers.cn_jp.parser import CnJpParser
from server.parsers.allocation.parser import AllocationBaseParser
from server.parsers.summary.parser import SummaryParser

_REGISTRY: Dict[str, Type[BaseParser]] = {
    BillParser.channel_id: BillParser,
    OwnFlowParser.channel_id: OwnFlowParser,
    CustomerParser.channel_id: CustomerParser,
    SpecialTransferParser.channel_id: SpecialTransferParser,
    SpecialAchRefundParser.channel_id: SpecialAchRefundParser,
    SpecialOpIncomingParser.channel_id: SpecialOpIncomingParser,
    SpecialOpRefundParser.channel_id: SpecialOpRefundParser,
    SpecialMergeBundleParser.channel_id: SpecialMergeBundleParser,
    FinalMergeParser.channel_id: FinalMergeParser,
    CnJpParser.channel_id: CnJpParser,
    AllocationBaseParser.channel_id: AllocationBaseParser,
    SummaryParser.channel_id: SummaryParser,
}


def get_parser(channel_id: str) -> Type[BaseParser] | None:
    return _REGISTRY.get(channel_id)


def list_parsers() -> Dict[str, Type[BaseParser]]:
    return dict(_REGISTRY)
