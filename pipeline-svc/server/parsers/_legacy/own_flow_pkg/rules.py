"""自有流水处理规则（priority 越小越先匹配）。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

MatchKind = Literal["icontains", "equals", "istartswith", "iexact", "iregex"]


@dataclass(frozen=True)
class OwnFlowRule:
    rule_id: str
    priority: int
    channel: str
    entity_default: str  # * 表示不限制；输出主体时再用
    file_group: str
    column: str
    kind: MatchKind
    pattern: str
    remark: str
    accounting_subject: str = ""
    entity_override: str | None = None
    # 显式覆盖"分行维度"（例如 JPM EFT DEBIT：主体 PPUS→PPHK，同时分行维度应为 JPMHK）
    branch_override: str | None = None
    # 可选：行级 Account Name 须包含（大小写不敏感）
    account_name_icontains: str | None = None
    # 可选：金额绝对值上限（取负/取正后的 |x|；超过则不命中，用于「BILLING DIRECT DEBIT 金额 ≤ 300」「BOC SW- ≤ 100」等阈值规则）
    max_abs_amount: float | None = None
    # 德意志 DB 日报：处理表「文件」列为 DBHK/DBKR/DBTH 时，按 Bank/Branch、SWIFT 与行匹配
    db_region: str | None = None


def _hardcoded_rules() -> list[OwnFlowRule]:
    r: list[OwnFlowRule] = []

    def add(
        rule: OwnFlowRule,
    ) -> None:
        r.append(rule)

    # --- CITI PPHK 主文件 ---
    add(OwnFlowRule("c1", 10, "CITI", "PPHK", "ppbk_main", "Transaction Description", "icontains", "BILLING", "billing"))
    add(OwnFlowRule("c2", 11, "CITI", "PPHK", "ppbk_main", "Payment Details", "icontains", "charge", "charge", "成本"))
    add(OwnFlowRule("c3", 12, "CITI", "PPHK", "ppbk_main", "Payment Details", "icontains", "worldlink", "worldlink", "成本"))
    # Queen Bee 必须早于 Payment Details 含 charge/worldlink 的规则，否则会先命中 CITI 而主体仍为 PPHK
    add(
        OwnFlowRule(
            "c4",
            5,
            "Queen Bee",
            "PPUS",
            "ppbk_main",
            "Name/Address",
            "icontains",
            "Queen Bee",
            "Queen Bee",
            "成本",
            entity_override="PPUS",
        )
    )

    # --- CITI PPHK other ---
    add(OwnFlowRule("c5", 20, "CITI", "PPHK", "ppbk_other", "Transaction Description", "icontains", "BILLING", "billing"))
    add(OwnFlowRule("c6", 21, "CITI", "PPHK", "ppbk_other", "Transaction Description", "iexact", "FT DEBIT", "charge", "成本"))
    add(
        OwnFlowRule(
            "c7",
            22,
            "CITI",
            "PPHK",
            "ppbk_other",
            "Transaction Description",
            "iexact",
            "X-BORDER WIRE FT CHARGE",
            "charge",
            "成本",
        )
    )

    # --- CITI 其他主体 ---
    # PPUS-USD 等：小额 BILLING DIRECT DEBIT（金额阈值）须先于「含 BILLING」泛匹配，避免误标为 billing 丢成本
    add(
        OwnFlowRule(
            "c9",
            25,
            "CITI",
            "PPUS",
            "citi_other",
            "Transaction Description",
            "iexact",
            "BILLING DIRECT DEBIT",
            "charge",
            "成本",
            max_abs_amount=300.0,
        )
    )
    add(OwnFlowRule("c8", 30, "CITI", "*", "citi_other", "Transaction Description", "icontains", "BILLING", "billing"))

    # --- PPEU：mark（财务），按渠道分子组 ---
    add(OwnFlowRule("p1", 40, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "iexact", "BILLING INVOICE PAID", "billing"))
    add(OwnFlowRule("p2", 41, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "iexact", "CASH BILLING INVOICE PAID", "billing"))
    add(OwnFlowRule("p3", 42, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "iregex", r"^charge$", "charge", "成本"))
    add(OwnFlowRule("p4", 43, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "icontains", "INCOMING PAYMENT CHARGES", "charge", "成本"))
    add(OwnFlowRule("p5", 44, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "icontains", "ACCOUNTS MAINTENANCE CHARGE", "charge", "成本"))
    add(
        OwnFlowRule(
            "p6",
            45,
            "CITI",
            "PPEU",
            "ppeu_citi",
            "mark（财务）",
            "icontains",
            "SWIFTEM",
            "charge",
            "成本",
        )
    )
    add(OwnFlowRule("p7", 46, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "icontains", "LOCAL PAYMENTS CHARGES", "charge", "成本"))
    add(OwnFlowRule("p8", 47, "CITI", "PPEU", "ppeu_citi", "mark（财务）", "icontains", "INTERNAL DEBIT", "charge", "成本"))

    add(OwnFlowRule("b1", 50, "BGL", "PPEU", "ppeu_bgl", "mark（财务）", "icontains", "Commission", "charge", "成本"))
    add(OwnFlowRule("b2", 51, "BGL", "PPEU", "ppeu_bgl", "mark（财务）", "icontains", "Virtual Account", "charge", "成本"))
    add(OwnFlowRule("b3", 52, "BGL", "PPEU", "ppeu_bgl", "mark（财务）", "icontains", "bank fee", "charge", "成本"))
    add(OwnFlowRule("b4", 53, "BGL", "PPEU", "ppeu_bgl", "mark（财务）", "icontains", "Connexis Fee", "charge", "成本"))

    add(OwnFlowRule("bc1", 60, "Banking Circle", "PPEU", "ppeu_bc", "mark（财务）", "icontains", "Bank charges", "charge", "成本"))
    # Barclays：与 pingpong-master script/own/all.py ba1 一致为 Bank charges；另补含charges 以覆盖中文 mark
    add(OwnFlowRule("ba1", 61, "Barclays", "PPEU", "ppeu_barclays", "mark（财务）", "icontains", "Bank charges", "billing", ""))
    add(OwnFlowRule("ba1b", 62, "Barclays", "PPEU", "ppeu_barclays", "mark（财务）", "icontains", "含charges", "billing", ""))

    # --- BOC ---
    add(
        OwnFlowRule(
            "bo1",
            70,
            "BOC",
            "PPUS",
            "boc",
            "Description",
            "istartswith",
            "SW-",
            "charge",
            "成本",
            max_abs_amount=100.0,
        )
    )
    add(OwnFlowRule("bo2", 71, "BOC", "PPUS", "boc", "Description", "istartswith", "ACH-", "charge", "成本"))

    # --- 德意志 DB 日报：DB-KR / DB-TH 非零变动（处理表 Charge → iregex ^charge$ + db_region）---
    add(
        OwnFlowRule(
            "db_kr",
            10,
            "DB",
            "PPHK",
            "db",
            "Transaction Description",
            "iregex",
            r"^charge$",
            "charge",
            "成本",
            db_region="DB-KR",
        )
    )
    add(
        OwnFlowRule(
            "db_th",
            11,
            "DB",
            "PPHK",
            "db",
            "Transaction Description",
            "iregex",
            r"^charge$",
            "charge",
            "成本",
            db_region="DB-TH",
        )
    )

    # --- DBS 星展（RAPID FEE 等，与处理表「筛选含RAPID FEE」一致）---
    add(
        OwnFlowRule(
            "dbs1",
            10,
            "DBS",
            "BRSG",
            "dbs",
            "Transaction Description",
            "icontains",
            "RAPID FEE",
            "charge",
            "成本",
        )
    )

    # --- JPM：同一文件内按账户名 + Description；EFT 优先于 SERVICE FEE ---
    add(
        OwnFlowRule(
            "j1",
            1,
            "JPM",
            "PPUS",
            "jpm",
            "Description",
            "iexact",
            "EFT DEBIT",
            "charge",
            "成本",
            entity_override="PPHK",
            branch_override="JPMHK",
            account_name_icontains="PingPong Global Solutions",
        )
    )
    add(
        OwnFlowRule(
            "j2",
            2,
            "JPM",
            "PPUS",
            "jpm",
            "Description",
            "icontains",
            "SERVICE FEE",
            "billing",
            "",
            account_name_icontains="PingPong Global Solutions",
        )
    )
    add(
        OwnFlowRule(
            "j3",
            10,
            "JPM",
            "PPI",
            "jpm",
            "Description",
            "icontains",
            "SERVICE CHARGE",
            "charge",
            "成本",
            account_name_icontains="INTELLIGENCE",
        )
    )
    add(
        OwnFlowRule(
            "j4",
            11,
            "JPM",
            "PPGT",
            "jpm",
            "Description",
            "icontains",
            "SERVICE CHARGE",
            "billing",
            "",
            account_name_icontains="TECHNOLOGY LIM",
        )
    )
    add(
        OwnFlowRule(
            "j5",
            12,
            "JPM",
            "BRSG",
            "jpm",
            "Description",
            "icontains",
            "SERVICE CHARGE",
            "billing",
            "",
            account_name_icontains="MANA PAYMENT (SINGAPORE)",
        )
    )

    # --- JPM PPEU：TRANSACTION CHARGES ---
    add(
        OwnFlowRule(
            "j6",
            13,
            "JPM",
            "PPEU",
            "jpm",
            "Description",
            "icontains",
            "TRANSACTION CHARGES",
            "billing",
            "",
            account_name_icontains="PingPong Europe",
        )
    )

    # --- SCB：模版「bill / FEE」= Transaction Description 含 bill 或 fee（非紧邻的 bill+fee）---
    add(OwnFlowRule("s1", 100, "SCB", "*", "scb", "Transaction Description", "iregex", r"(?i)(bill|fee)", "billing"))

    # --- BOSH ---
    add(OwnFlowRule("h1", 110, "BOSH", "PPHK", "bosh", "Transaction Description", "icontains", "手续费", "charge", "成本"))

    # --- MUFG（RUMG 文件组，与上传处理表一致）---
    add(
        OwnFlowRule(
            "r1",
            100,
            "MUFG",
            "PPJP",
            "rumg",
            "Transaction Description",
            "icontains",
            "チャージ",
            "charge",
            "成本",
        )
    )

    r.sort(key=lambda x: (x.file_group, x.priority))
    return r


def _processing_bundle_mtime() -> float:
    """处理表任一来源更新时间较大值（缓存失效键）。"""
    mt = 0.0
    try:
        from server.core.paths import get_rules_files_dir

        stem = get_rules_files_dir() / "rules" / "处理表"
        for p in (stem.with_suffix(".csv"), stem.with_suffix(".xlsx")):
            if p.exists():
                mt = max(mt, p.stat().st_mtime)
        jp = get_rules_files_dir() / "own_flow_processing" / "current.json"
        if jp.is_file():
            mt = max(mt, jp.stat().st_mtime)
    except Exception:
        pass
    return mt


_rules_cache: tuple[tuple[float, str], list[OwnFlowRule]] | None = None


def _own_flow_rules_source() -> str:
    """``PIPELINE_OWN_FLOW_RULES_SOURCE``：

    - ``auto``（默认）：① ``own_flow_processing/current.json``（≥10 条）→ ② ``rules/处理表`` → ③硬编码。
    - ``embedded``：仅用 ``_hardcoded_rules()``，不读上传 JSON / 处理表文件（逻辑锁定在代码库）。
    """
    v = (os.environ.get("PIPELINE_OWN_FLOW_RULES_SOURCE") or "auto").strip().lower()
    if v in ("embedded", "builtin", "hardcoded", "code"):
        return "embedded"
    return "auto"


def invalidate_rules_cache() -> None:
    """处理表保存后调用，使下次 all_rules() 重新读盘。"""
    global _rules_cache
    _rules_cache = None


def all_rules() -> list[OwnFlowRule]:
    """加载顺序见 ``_own_flow_rules_source()``；结果按 (bundle mtime, source) 缓存。"""
    global _rules_cache

    source = _own_flow_rules_source()
    key = (_processing_bundle_mtime(), source)
    if _rules_cache is not None and _rules_cache[0] == key:
        return _rules_cache[1]

    if source == "embedded":
        hard = _hardcoded_rules()
        _rules_cache = (key, hard)
        return hard

    loaded: list[OwnFlowRule] = []

    try:
        from server.core.paths import get_rules_files_dir

        jp = get_rules_files_dir() / "own_flow_processing" / "current.json"
        if jp.is_file():
            from .rules_from_xlsx import load_rules_from_processing_json

            loaded = load_rules_from_processing_json(jp)
    except Exception:
        loaded = []

    if len(loaded) >= 10:
        _rules_cache = (key, loaded)
        return loaded

    try:
        from server.core.paths import get_rules_files_dir

        stem = get_rules_files_dir() / "rules" / "处理表"
        xlsx_path = stem.with_suffix(".xlsx")
        if stem.with_suffix(".csv").exists() or xlsx_path.exists():
            from .rules_from_xlsx import load_rules_from_processing_xlsx

            loaded = load_rules_from_processing_xlsx(xlsx_path)
    except Exception:
        loaded = []

    if len(loaded) >= 10:
        _rules_cache = (key, loaded)
        return loaded

    hard = _hardcoded_rules()
    _rules_cache = (key, hard)
    return hard


def rules_for_file_group(fg: str) -> list[OwnFlowRule]:
    return sorted([x for x in all_rules() if x.file_group == fg], key=lambda x: x.priority)
