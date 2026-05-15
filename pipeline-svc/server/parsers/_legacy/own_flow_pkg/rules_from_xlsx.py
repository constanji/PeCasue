"""从 files/rules/处理表.xlsx 或 PeCause rules JSON 解析为 OwnFlowRule（运行时替代硬编码）。"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path

import pandas as pd

from .match import _normalize_db_region_tag
from .rules import MatchKind, OwnFlowRule

_COL_MAP: dict[str, str] = {
    "Transaction Description": "Transaction Description",
    "Payment Details": "Payment Details",
    "Name/Address": "Name/Address",
    "Description": "Description",
    "mark（财务）": "mark（财务）",
}


def _ensure_barclays_dual_mark_rules(out: list[OwnFlowRule]) -> None:
    """Barclays：pingpong-master 用 ``Bank charges``，中文模版常见 ``含charges``；只配一条时会漏另一半。"""
    bar = [
        r
        for r in out
        if r.file_group == "ppeu_barclays"
        and r.column == "mark（财务）"
        and r.kind == "icontains"
    ]
    if not bar:
        return
    pats = {(r.pattern or "").strip().casefold() for r in bar}
    if "含charges" in pats and "bank charges" in pats:
        return
    base = min(bar, key=lambda r: r.priority)
    p0 = (base.pattern or "").strip()
    if p0.casefold() == "bank charges":
        out.append(
            replace(
                base,
                rule_id=f"{base.rule_id}_cn",
                pattern="含charges",
                priority=base.priority + 1,
            )
        )
    elif p0 == "含charges":
        out.append(
            replace(
                base,
                rule_id=f"{base.rule_id}_en",
                pattern="Bank charges",
                priority=base.priority + 1,
            )
        )


def _infer_file_group(channel: str, file_tag: str) -> str:
    ch = (channel or "").strip()
    ft = (file_tag or "").strip().lower()
    if not ft and ch == "SCB":
        return "scb"
    if ch == "BOSH" or "bosh" in ft:
        return "bosh"
    if ch == "BOC" or ft == "boc":
        return "boc"
    if ch == "JPM" or "jpm" in ft:
        return "jpm"
    if ch == "SCB" or "scb" in ft:
        return "scb"
    if ch == "Queen Bee":
        return "ppbk_main"
    if "ppeu" in ft:
        if ch == "CITI":
            return "ppeu_citi"
        if ch == "BGL":
            return "ppeu_bgl"
        if "banking" in ch.lower():
            return "ppeu_bc"
        if "barclays" in ch.lower():
            return "ppeu_barclays"
    if "pphk" in ft and "other" in ft:
        return "ppbk_other"
    if ft in ("pphk", "pphk主文件") or (ch == "CITI" and "pphk" in ft and "other" not in ft):
        return "ppbk_main"
    if "其他文件" in ft or ft == "其他文件":
        return "citi_other"
    # PPUS-USD 等 CITI 非 PPHK 主文件：与 classify_citi_csv_name 一致 → citi_other
    if ch == "CITI" and "ppus" in ft.replace(" ", ""):
        return "citi_other"
    if ch == "CITI" and ft == "pphk":
        return "ppbk_main"
    if "dbs" in ft and "流水" in ft:
        return "dbs"
    ch_upper = (channel or "").strip().upper()
    if ch_upper == "RUMG" or "rumg" in ft.replace(" ", ""):
        return "rumg"
    if ch_upper == "MUFG" or "mufg" in ft.replace(" ", ""):
        return "rumg"
    if (ch or "").strip().upper() == "DB":
        return "db"
    return ""


_AMOUNT_CEIL_RE = re.compile(r"[（(]\s*金额[低小≤<]+于?\s*(\d+(?:\.\d+)?)\s*[）)]")


def _extract_amount_ceiling(processing: str) -> tuple[str, float | None]:
    """从「处理」字符串尾部抽取金额阈值，如 `筛选全等BILLING DIRECT DEBIT（金额低于300）`
    返回剥离阈值后的处理文本与数值；若无阈值则返回 (原文, None)。"""
    if not processing:
        return processing, None
    m = _AMOUNT_CEIL_RE.search(processing)
    if not m:
        return processing, None
    ceil = float(m.group(1))
    cleaned = (processing[: m.start()] + processing[m.end() :]).strip()
    return cleaned, ceil


def _parse_processing_to_kind_pattern(processing: str) -> tuple[MatchKind, str]:
    s, _ = _extract_amount_ceiling((processing or "").strip())
    if not s or str(s).lower() == "nan":
        raise ValueError("empty processing")
    if "需人为" in s or "人为判断" in s:
        raise ValueError("skip")
    _scompact = s.replace(" ", "")
    if "那条" in s or "金额100多" in _scompact:
        raise ValueError("skip")
    if s.startswith("筛选含"):
        pat = s[len("筛选含") :].strip().strip('""\u201c\u201d')
        return "icontains", pat
    m = re.match(r'筛选["\u201c](.+?)["\u201d]开头', s)
    if m:
        return "istartswith", m.group(1)
    if s.startswith("筛选全等"):
        pat = s[len("筛选全等") :].strip()
        if not pat:
            raise ValueError("empty after 筛选全等")
        return "iexact", pat
    if "/" in s and not s.startswith("筛选"):
        parts = [p.strip() for p in s.split("/") if p.strip()]
        rx = "|".join(re.escape(p) for p in parts)
        return "iregex", rf"(?i)({rx})"
    sl = s.lower()
    if sl == "charge" or s == "Charge":
        return "iregex", r"^charge$"
    return "icontains", s


# 与 Streamlit「添加规则」向导共用的条件类型键
PROCESSING_MODE_ICONTAINS = "icontains"
PROCESSING_MODE_ISTARTSWITH = "istartswith"
PROCESSING_MODE_IREGEX_OR = "iregex_or"
PROCESSING_MODE_IEXACT = "iexact"

PROCESSING_MODE_LABELS: dict[str, str] = {
    PROCESSING_MODE_ICONTAINS: "子串包含",
    PROCESSING_MODE_ISTARTSWITH: "以前缀",
    PROCESSING_MODE_IREGEX_OR: "多段命中一段",
    PROCESSING_MODE_IEXACT: "全命中（整格相等）",
}


def format_processing_condition(mode: str, content: str) -> str:
    """将向导中的「条件类型 + 输入内容」转为「处理」列存储格式（与 _parse_processing_to_kind_pattern 互逆）。

    - 子串包含 → ``筛选含…``
    - 以前缀 → ``筛选"…"开头``
    - 多段命中一段 → ``a/b/c``（可用换行或 / 分隔多段）
    - 全命中 → ``筛选全等…``（整格与内容一致，忽略大小写比较见 match.iexact）
    """
    t = (content or "").strip()
    if mode == PROCESSING_MODE_ICONTAINS:
        if not t:
            return ""
        return f"筛选含{t}"
    if mode == PROCESSING_MODE_ISTARTSWITH:
        if not t:
            return ""
        return f'筛选"{t}"开头'
    if mode == PROCESSING_MODE_IREGEX_OR:
        parts = re.split(r"[/\n\r]+", t)
        parts = [p.strip() for p in parts if p.strip()]
        if not parts:
            return ""
        return "/".join(parts)
    if mode == PROCESSING_MODE_IEXACT:
        if not t:
            return ""
        return f"筛选全等{t}"
    return ""


def _entity_from_row(主体: str) -> str:
    t = (主体 or "").strip()
    if t in ("各主体", "其他主体", "*", "") or str(t).lower() == "nan":
        return "*"
    return t


def _jpm_account_icontains(ent: str, proc: str) -> str | None:
    pu = proc.upper()
    e = ent.upper().strip()
    if "EFT" in pu or "SERVICE FEE" in pu:
        return "PingPong Global Solutions"
    if "TRANSACTION CHARGES" in pu:
        return "PingPong Europe"
    if "SERVICE CHARGE" in pu:
        if e == "PPI":
            return "INTELLIGENCE"
        if e == "PPGT":
            return "TECHNOLOGY LIM"
        if e == "BRSG":
            return "MANA PAYMENT (SINGAPORE)"
    return None


def _jpm_entity_override(ent: str, proc: str, note: str) -> str | None:
    if "EFT" not in proc.upper():
        return None
    if ent.upper() == "PPUS" and "PPHK" in (note or ""):
        return "PPHK"
    return None


def _jpm_branch_override(ent: str, proc: str, note: str) -> str | None:
    """JPM EFT DEBIT：主体由 PPUS 调整为 PPHK 时，分行维度也应由 JPMUS 改为 JPMHK。"""
    ov = _jpm_entity_override(ent, proc, note)
    if ov == "PPHK":
        return "JPMHK"
    return None


def rules_from_processing_dataframe(df: pd.DataFrame) -> list[OwnFlowRule]:
    """将处理表 DataFrame（含「数据源」「渠道」等列）转为 OwnFlowRule 列表。"""
    out: list[OwnFlowRule] = []
    idx = 0
    for _, row in df.iterrows():
        src = str(row.get("数据源", "")).strip()
        if src != "自有流水":
            continue
        channel = str(row.get("渠道", "") or "").strip()
        file_tag = str(row.get("文件", "") or "").strip()
        if str(file_tag).lower() == "nan":
            file_tag = ""
        col_raw = str(row.get("表头", "") or "").strip()
        proc = str(row.get("处理", "") or "").strip()
        remark = str(row.get("备注", "") or "").strip()
        acct_subj = str(row.get("入账科目", "") or "").strip()
        note = str(row.get("说明", "") or "").strip()
        主体 = str(row.get("主体", "") or "").strip()

        if remark.lower() == "nan":
            remark = ""
        if acct_subj.lower() == "nan":
            acct_subj = ""
        if note.lower() == "nan":
            note = ""

        fg = _infer_file_group(channel, file_tag)
        if not fg:
            continue

        # JPM EFT DEBIT：处理表若写成"EFT DEBIT"（无前缀），语义上应按整格相等而非 contains
        if fg == "jpm" and proc.strip().upper() == "EFT DEBIT":
            proc = "筛选全等EFT DEBIT"

        if (not col_raw or col_raw.lower() == "nan") and fg == "bosh" and proc and "手续费" in proc:
            col_raw = "Transaction Description"
        # RUMG（三菱 UFJ 日文全明細）规则默认不写「表头」，摘要并入 Transaction Description
        if (not col_raw or col_raw.lower() == "nan") and fg == "rumg":
            col_raw = "Transaction Description"
        if not col_raw or col_raw.lower() == "nan":
            continue

        ch_st = (channel or "").strip()

        try:
            kind, pattern = _parse_processing_to_kind_pattern(proc)
        except ValueError:
            continue
        _, amt_ceiling = _extract_amount_ceiling(proc)

        col = _COL_MAP.get(col_raw, col_raw)
        ent_def = _entity_from_row(主体)
        ent_ov = _jpm_entity_override(主体, proc, note)
        # Queen Bee：与硬编码 c4 一致——输出主体恒为 PPUS（来源文件名常为 PPHK，处理表「主体」列易被误填为 PPHK）
        if ch_st == "Queen Bee":
            ent_def = "PPUS"
            ent_ov = "PPUS"
        br_ov = _jpm_branch_override(主体, proc, note) if fg == "jpm" else None
        if ch_st == "Queen Bee":
            br_ov = "Queen Bee"
        acc_ic = None
        if fg == "jpm":
            acc_ic = _jpm_account_icontains(主体, proc)

        db_reg: str | None = None
        if fg == "db":
            ft_norm = (file_tag or "").strip().upper().replace(" ", "")
            canon = _normalize_db_region_tag(ft_norm)
            if canon in ("DB-HK", "DB-KR", "DB-TH"):
                db_reg = canon

        rule_id = f"x{idx}"
        pri = idx * 10 + 10
        if fg == "jpm":
            pu = proc.upper()
            if "EFT" in pu:
                pri = 1
            elif "SERVICE FEE" in pu:
                pri = 2
            elif "TRANSACTION CHARGES" in pu:
                pri = 13
            elif "SERVICE CHARGE" in pu:
                pri = 10
            else:
                pri = 20
        # Queen Bee：须早于同文件「Payment Details 含 charge」等规则，否则主体无法改为 PPUS
        if ch_st == "Queen Bee":
            pri = 5
        idx += 1

        out.append(
            OwnFlowRule(
                rule_id,
                pri,
                channel,
                ent_def,
                fg,
                col,
                kind,
                pattern,
                remark,
                acct_subj,
                entity_override=ent_ov,
                branch_override=br_ov,
                account_name_icontains=acc_ic,
                max_abs_amount=amt_ceiling,
                db_region=db_reg,
            )
        )

    _ensure_barclays_dual_mark_rules(out)
    out.sort(key=lambda x: (x.file_group, x.priority))
    return out


def load_rules_from_processing_xlsx(path: Path) -> list[OwnFlowRule]:
    """解析处理表（优先同路径 .csv，否则 .xlsx）中「自有流水」行。"""
    stem = path.parent / path.stem
    if stem.with_suffix(".csv").exists():
        df = pd.read_csv(stem.with_suffix(".csv"), encoding="utf-8-sig")
    else:
        df = pd.read_excel(path, engine="openpyxl")
    return rules_from_processing_dataframe(df)


def load_rules_from_processing_json(path: Path) -> list[OwnFlowRule]:
    """解析 ``own_flow_processing/current.json``（columns + rows）。"""
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows = raw.get("rows") or []
    if not rows:
        return []
    df = pd.DataFrame(rows)
    return rules_from_processing_dataframe(df)
