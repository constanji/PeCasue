"""自有流水：入账期无关的路径解析 + 目录清单扫描 + 未纳入解析的告警。

设计目标：
- 与「YYYY.MM」目录名解耦：用「目录名含 citi+流水、CITI自有流水」等规则匹配；
- CITI：在 **任意深度** 查找名称含 citi+流水的目录，其下递归 *.csv（可与「2026.03自有流水」等多包一层）；
- PPEU / SCB / MUFG：见 resolve_ppeu_workbook_path、resolve_scb_path、resolve_rumg_csv_paths；
- BOC/DB 的 PDF 在扫描中认领为已识别（行数 0+说明），避免误报，入汇总仍以 Excel/CSV 为准；
- 凡未被认领的数据文件（含「孤儿」pdf）进 warnings。
"""

from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any

import pandas as pd

from .classify import ppeu_sheet_to_channel, ppeu_sheet_to_group
from .constants import CITI_RAW_KEYS

# 规则「表头」列仅使用这些与流水线/处理表一致的列名（扫描列名与此求交，避免无关列刷屏）
_RULE_HEADER_ALLOW: set[str] = set(CITI_RAW_KEYS) | {
    "mark（财务）",
    "Description",
    "Name/Address",
    "Channel",
    "Transaction Description",
    "Payment Details",
}
from .loaders import (
    drop_boc_balance_column,
    normalize_columns,
    read_citi_csv,
    read_excel_first_sheet,
    read_jpm_details,
    read_ppeu_sheet,
)
from .rumg import parse_rumg_statement


def _path_skipped_for_scan(p: Path) -> bool:
    """跳过 macOS 压缩垃圾目录与 Office 锁文件路径。"""
    pl = {x.casefold() for x in p.parts}
    return "__macosx" in pl


def is_deutsche_db_flow_dir(name: str) -> bool:
    """德意志 DB 流水目录；排除名称中含 dbs 的星展目录。"""
    n = name.lower()
    if "流水" not in n or "db" not in n:
        return False
    if "dbs" in n:
        return False
    return True


# ────────────────────────── 入账期无关：与 pipeline 共用 ──────────────────────────


def resolve_citi_csv_dir(root: Path) -> Path | None:
    """任意深度：名称同时含 citi 与「流水」的目录（含 CITI自有流水）；用于兼容旧入口。"""
    root = root.expanduser().resolve()
    if not root.is_dir():
        return None
    for name in ("2026.02CITI流水", "2026.02citi流水"):
        p = root / name
        if p.is_dir():
            return p
    for d in sorted(root.rglob("*")):
        if not d.is_dir() or _path_skipped_for_scan(d):
            continue
        ld = d.name.lower()
        if "citi" in ld and "流水" in d.name:
            return d
    return None


def iter_citi_csv_files(root: Path) -> list[Path]:
    """所有「citi+流水」命名目录下的 *.csv（目录可嵌套在「2026.03自有流水」等外层之下），路径去重后排序。"""
    root = root.expanduser().resolve()
    if not root.is_dir():
        return []
    seen: set[str] = set()
    out: list[Path] = []

    def add_csv_tree(base: Path) -> None:
        for f in sorted(base.rglob("*.csv")):
            if not f.is_file() or _path_skipped_for_scan(f):
                continue
            k = str(f.resolve())
            if k not in seen:
                seen.add(k)
                out.append(f)

    for name in ("2026.02CITI流水", "2026.02citi流水"):
        p = root / name
        if p.is_dir():
            add_csv_tree(p)

    for d in sorted(root.rglob("*")):
        if not d.is_dir() or _path_skipped_for_scan(d):
            continue
        ld = d.name.lower()
        if "citi" not in ld or "流水" not in d.name:
            continue
        add_csv_tree(d)

    return sorted(out, key=lambda x: str(x))


def resolve_ppeu_workbook_path(root: Path) -> Path | None:
    root = root.expanduser().resolve()
    if not root.is_dir():
        return None
    legacy = root / "2026.02 PPEU（BC&BGL&CITI&Barclays)自有流水.xlsx"
    if legacy.exists():
        return legacy
    globs = []
    for ext in ("*.xlsx", "*.xlsm"):
        globs.extend(sorted(root.rglob(ext)))
    pats = (
        "*ppeu*bc*bgl*自有流水*.xlsx",
        "*ppeu*自有流水*.xlsx",
        "*ppeu*自有*.xlsx",
        "*ppeu*自有*.xlsm",
    )
    matched: list[Path] = []
    for f in globs:
        if not f.is_file() or _path_skipped_for_scan(f):
            continue
        nl = f.name.lower()
        if any(fnmatch.fnmatch(nl, p) for p in pats):
            matched.append(f)
    if matched:
        return sorted(matched, key=lambda p: p.name.lower())[0]
    for f in sorted(globs):
        if _path_skipped_for_scan(f):
            continue
        nl = f.name.lower()
        if "ppeu" in nl and "自有" in f.name:
            return f
    return None


def resolve_jpm_path(root: Path) -> Path | None:
    root = root.expanduser().resolve()
    legacy = root / "2026.02JPM自有流水.xls"
    if legacy.exists():
        return legacy
    for f in sorted(root.rglob("*.xls")) + sorted(root.rglob("*.xlsx")):
        if not f.is_file() or _path_skipped_for_scan(f):
            continue
        nl = f.name.lower()
        if "jpm" in nl and "自有流水" in f.name:
            return f
    return None


def resolve_boc_path(root: Path) -> Path | None:
    root = root.expanduser().resolve()
    if not root.is_dir():
        return None
    legacy = root / "2026.02BOC流水" / "BOC_Feb 2026.xlsx"
    if legacy.exists():
        return legacy
    for d in sorted(root.rglob("*")):
        if not d.is_dir() or _path_skipped_for_scan(d):
            continue
        if "boc" not in d.name.lower() or "流水" not in d.name:
            continue
        cands = sorted(d.glob("*.xlsx"))
        if not cands:
            continue
        named = [f for f in cands if "boc" in f.stem.lower()]
        return named[0] if named else cands[0]
    return None


def resolve_scb_path(root: Path) -> Path | None:
    root = root.expanduser().resolve()
    if not root.is_dir():
        return None
    legacy = root / "2026.2SCB流水" / "SCB202602流水.xlsx"
    if legacy.exists():
        return legacy
    for d in sorted(root.rglob("*")):
        if not d.is_dir() or _path_skipped_for_scan(d):
            continue
        if "scb" not in d.name.lower() or "流水" not in d.name:
            continue
        for f in sorted(d.glob("*.xlsx")) + sorted(d.glob("*.xls")):
            if not _path_skipped_for_scan(f):
                return f
    for f in sorted(root.rglob("*.xlsx")) + sorted(root.rglob("*.xls")):
        if not f.is_file() or _path_skipped_for_scan(f):
            continue
        nl = f.name.lower()
        if "scb" in nl and "流水" in f.name:
            return f
    return None


def resolve_bosh_path(root: Path) -> Path | None:
    root = root.expanduser().resolve()
    legacy = root / "202602BOSH自有流水.xlsx"
    if legacy.exists():
        return legacy
    for f in sorted(root.rglob("*.xlsx")):
        if not f.is_file() or _path_skipped_for_scan(f):
            continue
        nl = f.name.lower()
        if fnmatch.fnmatch(nl, "*bosh*自有流水*.xlsx"):
            return f
    return None


def resolve_dbs_flow_dirs(root: Path) -> list[Path]:
    root = root.expanduser().resolve()
    if not root.is_dir():
        return []
    return sorted(
        d
        for d in root.rglob("*")
        if d.is_dir()
        and not _path_skipped_for_scan(d)
        and "dbs" in d.name.lower()
        and "流水" in d.name
    )


def resolve_deutsche_db_flow_dirs(root: Path) -> list[Path]:
    root = root.expanduser().resolve()
    if not root.is_dir():
        return []
    return sorted(
        d
        for d in root.rglob("*")
        if d.is_dir() and not _path_skipped_for_scan(d) and is_deutsche_db_flow_dir(d.name)
    )


def resolve_rumg_csv_paths(root: Path) -> list[Path]:
    """
    三菱系全明細 CSV：① 任意深度文件名含 rumg 的 .csv；② 任意深度目录名含 mufg 且含流水或自有时递归 *.csv
    （如 2026.03自有流水/2026.3mufg自有流水/MEISAI*.csv）。
    """
    root = root.expanduser().resolve()
    if not root.is_dir():
        return []
    out: set[Path] = set()
    for p in root.rglob("*.csv"):
        if not p.is_file() or _path_skipped_for_scan(p):
            continue
        if "rumg" in p.name.lower():
            out.add(p.resolve())
    for d in root.rglob("*"):
        if not d.is_dir() or _path_skipped_for_scan(d):
            continue
        ld = d.name.lower()
        if "mufg" not in ld:
            continue
        if "流水" not in ld and "自有" not in ld:
            continue
        for f in d.rglob("*.csv"):
            if f.is_file() and not _path_skipped_for_scan(f):
                out.add(f.resolve())
    return sorted(out)


def walk_candidate_data_files(root: Path) -> list[Path]:
    """递归列出 csv/xlsx/xls/pdf，排除 Excel 临时锁文件。"""
    root = root.expanduser().resolve()
    out: list[Path] = []
    if not root.is_dir():
        return out
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if _path_skipped_for_scan(p):
            continue
        if p.name.startswith("~$") or p.name.startswith("._"):
            continue
        if p.suffix.lower() in (".csv", ".xlsx", ".xls", ".pdf"):
            out.append(p)
    return sorted(out)


def _rel(root: Path, p: Path) -> str:
    try:
        return str(p.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(p)


def _dbs_parse_row_count(path: Path) -> int:
    """与 pipeline._dbs_sheet_to_blues 一致；延迟 import 避免循环依赖。"""
    from .pipeline import _dbs_sheet_to_blues

    try:
        eng = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
        raw = pd.read_excel(path, sheet_name=0, header=None, engine=eng)
    except Exception:
        return -1
    return len(_dbs_sheet_to_blues(raw, path))


def scan_inventory(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """扫描 root：返回 (已识别来源行, 未纳入解析告警行)。

    每条「已识别」含：来源、文件、行数、列数、路径（绝对路径，供后续比对）。
    每条「告警」含：相对路径、原因、说明。
    """
    root = root.expanduser().resolve()
    sources: list[dict[str, Any]] = []
    handled: set[str] = set()

    def add_handled(p: Path) -> None:
        handled.add(str(p.resolve()))

    # ── CITI（子目录内 csv 一并纳入，如 2026.03流水/*.csv）──
    for p in iter_citi_csv_files(root):
        add_handled(p)
        try:
            df = read_citi_csv(p)
            sources.append(
                {
                    "来源": "CITI",
                    "文件": p.name,
                    "行数": len(df),
                    "列数": len(df.columns),
                    "列名": [str(c).strip() for c in df.columns],
                    "路径": str(p),
                }
            )
        except Exception as e:
            sources.append(
                {"来源": "CITI", "文件": p.name, "行数": f"错误: {e}", "列数": "-", "路径": str(p)}
            )

    # ── RUMG / MUFG 明細（rumg 或 mufg 自有/流水 目录下 csv）──
    for p in resolve_rumg_csv_paths(root):
        add_handled(p)
        try:
            _, data_rows = parse_rumg_statement(p)
            sources.append(
                {
                    "来源": "RUMG",
                    "文件": p.name,
                    "行数": len(data_rows),
                    "列数": "-",
                    "列名": list(CITI_RAW_KEYS),
                    "路径": str(p),
                }
            )
        except Exception as e:
            sources.append({"来源": "RUMG", "文件": p.name, "行数": f"错误: {e}", "列数": "-", "路径": str(p)})

    # ── PPEU ──
    ppeu = resolve_ppeu_workbook_path(root)
    if ppeu is not None:
        add_handled(ppeu)
        try:
            xl = pd.ExcelFile(ppeu, engine="openpyxl")
            for sheet in xl.sheet_names:
                fg = ppeu_sheet_to_group(sheet)
                if not fg:
                    continue
                try:
                    df = read_ppeu_sheet(ppeu, sheet, header=13)
                    sources.append(
                        {
                            "来源": f"PPEU/{sheet}",
                            "文件": ppeu.name,
                            "行数": len(df),
                            "列数": len(df.columns),
                            "列名": [str(c).strip() for c in df.columns],
                            "路径": str(ppeu),
                        }
                    )
                except Exception as e:
                    sources.append(
                        {
                            "来源": f"PPEU/{sheet}",
                            "文件": ppeu.name,
                            "行数": f"错误: {e}",
                            "列数": "-",
                            "路径": str(ppeu),
                        }
                    )
        except Exception as e:
            sources.append({"来源": "PPEU", "文件": ppeu.name, "行数": f"错误: {e}", "列数": "-", "路径": str(ppeu)})

    # ── JPM ──
    jpm = resolve_jpm_path(root)
    if jpm is not None:
        add_handled(jpm)
        try:
            df = read_jpm_details(jpm)
            sources.append(
                {
                    "来源": "JPM",
                    "文件": jpm.name,
                    "行数": len(df),
                    "列数": len(df.columns),
                    "列名": [str(c).strip() for c in df.columns],
                    "路径": str(jpm),
                }
            )
        except Exception as e:
            sources.append({"来源": "JPM", "文件": jpm.name, "行数": f"错误: {e}", "列数": "-", "路径": str(jpm)})

    # ── BOC ──
    boc = resolve_boc_path(root)
    if boc is not None:
        add_handled(boc)
        try:
            df = drop_boc_balance_column(read_excel_first_sheet(boc, header=0))
            sources.append(
                {
                    "来源": "BOC",
                    "文件": boc.name,
                    "行数": len(df),
                    "列数": len(df.columns),
                    "列名": [str(c).strip() for c in df.columns],
                    "路径": str(boc),
                }
            )
        except Exception as e:
            sources.append({"来源": "BOC", "文件": boc.name, "行数": f"错误: {e}", "列数": "-", "路径": str(boc)})

    # BOC 目录下仅有 PDF：认领以免误报警，流水线仍依赖 Excel
    for d in root.iterdir():
        if not d.is_dir():
            continue
        if "boc" not in d.name.lower() or "流水" not in d.name:
            continue
        for pdf in sorted(d.glob("*.pdf")):
            if str(pdf.resolve()) in handled:
                continue
            add_handled(pdf)
            sources.append(
                {
                    "来源": "BOC",
                    "文件": pdf.name,
                    "行数": "0（不解析 PDF；与导出内容一致的 CSV/Excel 请放入同目录入流水线）",
                    "列数": "-",
                    "路径": str(pdf),
                }
            )

    # ── SCB ──
    scb = resolve_scb_path(root)
    if scb is not None:
        add_handled(scb)
        try:
            df = read_excel_first_sheet(scb, header=0)
            sources.append(
                {
                    "来源": "SCB",
                    "文件": scb.name,
                    "行数": len(df),
                    "列数": len(df.columns),
                    "列名": [str(c).strip() for c in df.columns],
                    "路径": str(scb),
                }
            )
        except Exception as e:
            sources.append({"来源": "SCB", "文件": scb.name, "行数": f"错误: {e}", "列数": "-", "路径": str(scb)})

    # ── BOSH ──
    bosh = resolve_bosh_path(root)
    if bosh is not None:
        add_handled(bosh)
        try:
            df = pd.read_excel(bosh, sheet_name=0, header=5, engine="openpyxl")
            df = normalize_columns(df)
            sources.append(
                {
                    "来源": "BOSH",
                    "文件": bosh.name,
                    "行数": len(df),
                    "列数": len(df.columns),
                    "列名": [str(c).strip() for c in df.columns],
                    "路径": str(bosh),
                }
            )
        except Exception as e:
            sources.append({"来源": "BOSH", "文件": bosh.name, "行数": f"错误: {e}", "列数": "-", "路径": str(bosh)})

    # ── 德意志 DB CSV ──
    for db_dir in resolve_deutsche_db_flow_dirs(root):
        for entity_dir in sorted(db_dir.iterdir()):
            if not entity_dir.is_dir():
                continue
            ent = entity_dir.name.upper()
            for csv_path in sorted(entity_dir.glob("*.csv")):
                add_handled(csv_path)
                try:
                    df = pd.read_csv(csv_path, encoding="utf-8-sig")
                except Exception:
                    try:
                        df = pd.read_csv(csv_path, encoding="utf-8")
                    except Exception as e:
                        sources.append(
                            {
                                "来源": f"DB/{ent}",
                                "文件": csv_path.name,
                                "行数": f"错误: {e}",
                                "列数": "-",
                                "路径": str(csv_path),
                            }
                        )
                        continue
                df.columns = [str(c).strip() for c in df.columns]
                if "Sum of Transaction amount" not in df.columns:
                    sources.append(
                        {
                            "来源": f"DB/{ent}",
                            "文件": csv_path.name,
                            "行数": "跳过(无 Sum of Transaction amount)",
                            "列数": len(df.columns),
                            "路径": str(csv_path),
                        }
                    )
                    continue
                sources.append(
                    {
                        "来源": f"DB/{ent}",
                        "文件": csv_path.name,
                        "行数": len(df),
                        "列数": len(df.columns),
                        "列名": [str(c).strip() for c in df.columns],
                        "路径": str(csv_path),
                    }
                )
            for pdf_path in sorted(entity_dir.glob("*.pdf")):
                add_handled(pdf_path)
                sources.append(
                    {
                        "来源": f"DB/{ent}",
                        "文件": pdf_path.name,
                        "行数": "0（不解析 PDF；与 PDF 同内容的 CSV 放入同主体目录以入流水线）",
                        "列数": "-",
                        "路径": str(pdf_path),
                    }
                )

    # ── DBS ──
    for ddir in resolve_dbs_flow_dirs(root):
        for path in sorted(ddir.glob("*.xls")) + sorted(ddir.glob("*.xlsx")):
            add_handled(path)
            n_tx = _dbs_parse_row_count(path)
            if n_tx < 0:
                sources.append(
                    {
                        "来源": "DBS",
                        "文件": path.name,
                        "行数": "错误: 无法读取或解析",
                        "列数": "-",
                        "路径": str(path),
                    }
                )
            else:
                sources.append(
                    {
                        "来源": "DBS",
                        "文件": path.name,
                        "行数": n_tx,
                        "列数": "-",
                        "路径": str(path),
                    }
                )

    # ── 未纳入解析：候选数据文件不在 handled 中 ──
    warnings: list[dict[str, Any]] = []
    for p in walk_candidate_data_files(root):
        if str(p.resolve()) in handled:
            continue
        rel = _rel(root, p)
        if p.suffix.lower() == ".pdf":
            warnings.append(
                {
                    "相对路径": rel,
                    "原因": "未匹配解析规则（PDF）",
                    "说明": "当前流水线不解析 PDF；请使用对应渠道导出的 CSV/Excel，或放入已支持目录。",
                }
            )
        else:
            warnings.append(
                {
                    "相对路径": rel,
                    "原因": "未匹配任何已知渠道路径/命名规则",
                    "说明": "请将文件放入约定目录（如名称含 CITI流水、BOC流水、DB流水 等），或扩展 own_flow/discovery.py 与 pipeline。",
                }
            )

    return sources, warnings


def _channel_from_inventory_source(来源: str) -> str | None:
    """scan_inventory 的「来源」→ 处理表「渠道」候选。"""
    s = (来源 or "").strip()
    if not s:
        return None
    if s.startswith("PPEU/"):
        sheet = s.split("/", 1)[1]
        return ppeu_sheet_to_channel(sheet)
    if s.startswith("DB/"):
        return "DB"
    if s in ("CITI", "JPM", "BOC", "SCB", "BOSH", "DBS", "RUMG"):
        return s
    return None


def _entity_from_inventory_source(来源: str) -> str | None:
    """仅从 DB 子目录名得到主体代码（如 DB/PPHK → PPHK）。"""
    s = (来源 or "").strip()
    if s.startswith("DB/"):
        ent = s.split("/", 1)[1].strip()
        return ent or None
    return None


# Step5 下拉的表头回退（与 rules / 模版列名一致；扫描列名优先合并）
RULE_HEADER_FALLBACK: list[str] = [
    "Transaction Description",
    "Payment Details",
    "mark（财务）",
    "Description",
    "Name/Address",
]


def rule_ui_options_from_sources(sources: list[dict[str, Any]]) -> tuple[list[str], list[str], list[str]]:
    """由 scan_inventory 结果聚合渠道 / 主体 / 表头（列名）选项。"""
    channels: set[str] = set()
    entities: set[str] = set()
    headers: set[str] = set()
    for src in sources:
        ch = _channel_from_inventory_source(str(src.get("来源", "")))
        if ch:
            channels.add(ch)
        ent = _entity_from_inventory_source(str(src.get("来源", "")))
        if ent:
            entities.add(ent)
        for c in src.get("列名") or []:
            cs = str(c).strip()
            if not cs or cs.lower() == "nan":
                continue
            if cs in _RULE_HEADER_ALLOW:
                headers.add(cs)
    return (
        sorted(channels),
        sorted(entities),
        sorted(headers),
    )


def rule_ui_options_from_root(root: Path | None) -> tuple[list[str], list[str], list[str]]:
    """扫描自有流水根目录，返回 (渠道列表, 主体列表, 表头列名列表)。"""
    if root is None:
        return [], [], []
    root = root.expanduser().resolve()
    if not root.is_dir():
        return [], [], []
    sources, _ = scan_inventory(root)
    return rule_ui_options_from_sources(sources)
