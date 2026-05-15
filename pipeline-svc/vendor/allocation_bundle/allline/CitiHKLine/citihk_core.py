# -*- coding: utf-8 -*-
"""CITIHK PPHK bases: read Excel stream, mapping, transforms (ASCII-only source)."""
from __future__ import annotations

import csv
import logging
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# QuickBI / 银行 CSV 偶发超长单元格（备注、附言等）；标准库默认单字段 ≤128KiB 会报 _csv.Error
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

_HERE = Path(__file__).resolve().parent
_PKG_ROOT = _HERE.parent
# PeCause vendor：``allocation_bundle`` 目录同时包含 ``allline`` 与 ``cost_allocation`` 包。
_COST_PARENT = _PKG_ROOT.parent
if _COST_PARENT.is_dir() and str(_COST_PARENT) not in sys.path:
    sys.path.insert(0, str(_COST_PARENT))
from cost_allocation.bases import norm_channel, norm_entity, norm_month  # noqa: E402

_MAPPING_CSV = "\u8d26\u6237\u5bf9\u5e94\u4e3b\u4f53\u5206\u884cmapping\u8868.csv"
_TEMPLATE_PPHK = "\u57fa\u6570PPHK\u6a21\u7248.xlsx"
_OUT_NAME = "CITIHK_PPHK_\u57fa\u6570.xlsx"

_FILES_ROOT = _PKG_ROOT / "files"
# 占位：真实数据目录为任务 extracted；mapping/模版由 rules/files/allocation 提供（ops 显式传参）
DEFAULT_CITIHK_DIR = _FILES_ROOT / "samples" / "CITIHK"
DEFAULT_MAPPING = _FILES_ROOT / "citihk" / "mapping" / _MAPPING_CSV
DEFAULT_OUT = _FILES_ROOT / "default_outputs" / _OUT_NAME
DEFAULT_TEMPLATE = _FILES_ROOT / "citihk" / "mapping" / _TEMPLATE_PPHK

BU_TO_FINAL = {
    "FlowMore": "\u798f\u8d38",
    "\u4e3b\u7ad9": "\u4e3b\u7ad9",
    "\u56fd\u9645_VN": "APAC-Vietnam",
    "\u56fd\u9645_\u516c\u5171": "APAC-\u516c\u5171",
    "\u56fd\u9645_IN": "APAC-India",
    "\u56fd\u9645_KR": "APAC-South Korea",
    "\u56fd\u9645_US": "\u6b27\u7f8e-SMB",
    "\u56fd\u9645_ID": "APAC-Indonesia",
    "\u56fd\u9645_SEA": "APAC-\u516c\u5171",
    "\u56fd\u9645_TH": "APAC-Thailand",
    "\u56fd\u9645_UK-currentz": "\u6b27\u7f8e-Tiger",
    "currentz": "\u6b27\u7f8e-Panda",
    "\u4e1c\u5357\u4e9a\u673a\u6784\u4e1a\u52a1": "APAC-\u673a\u6784\u4e1a\u52a1",
}

# 映射后的「最终 BU」展示名；若上游导出已在 BU 列填最终口径，须透传否则会整条丢弃。
_FINAL_BU_CANONICAL = frozenset(BU_TO_FINAL.values())


def resolve_final_bu(bu_raw: object) -> str:
    """中间 BU 代码 → 最终 BU；若已是 ``BU_TO_FINAL`` 的值（展示名）则原样保留。"""
    s = _strip(bu_raw)
    if not s:
        return ""
    mapped = BU_TO_FINAL.get(s, "")
    if mapped:
        return mapped
    return s if s in _FINAL_BU_CANONICAL else ""

INBOUND_USECOLS = [
    "\u516c\u53f8\u4e3b\u4f53",
    "\u94f6\u884c/\u901a\u9053\u540d\u79f0",
    "\u4e3b\u4f53\u8d26\u53f7",
    "BU",
    "\u94f6\u884c\u5165\u8d26\u65e5\u671f",
    "\u8d26\u5355\u65e5\u671f",
]
OUTBOUND_USECOLS = [
    "\u4ee3\u53d1\u6e20\u9053\u7684\u516c\u53f8\u4e3b\u4f53",
    "\u4ee3\u53d1\u6e20\u9053\u7684\u5927\u8d26\u53f7",
    "\u94f6\u884c/\u901a\u9053\u540d\u79f0",
    "BU",
    "\u8d26\u5355\u65e5\u671f",
]

CHANNEL_DETAIL = "CITI_HK"
CHANNEL_TEMPLATE = "CITI-HK"
CHANNEL_CODE = "CITI"
TARGET_ENTITY = "PPHK"
TARGET_BRANCH = "CITIHK"
FILTER_BANK = "CITI"
# 源表「银行/通道名称」多为 CITI-HK / CITI_HK，仅等于 "CITI" 会漏数；用 norm_channel + 归一后匹配。
_BANK_CITI_KEYS = frozenset({"CITI", "CITI-HK", "CITIHK"})


def bank_passes_citihk_filter(bank_raw: object) -> bool:
    k = norm_channel(bank_raw).replace("_", "-").strip().upper()
    return k in _BANK_CITI_KEYS


C_MONTH = "\u6708\u4efd"
C_ENTITY = "\u4e3b\u4f53"
C_CH = "\u6e20\u9053\u540d\u79f0"
C_ACCT_MAIN = "\u4e3b\u4f53\u8d26\u53f7"
C_BIG = "\u5927\u8d26\u53f7"
C_IN_CT = "\u5165\u91d1\u7b14\u6570"
C_OUT_CT = "\u51fa\u91d1\u7b14\u6570"
C_IN_VOL = "\u5165\u91d1\u4ea4\u6613\u91cf"
C_OUT_VOL = "\u51fa\u91d1\u4ea4\u6613\u91cf"
C_FINALBU = "\u6700\u7ec8BU"
C_FINALBU_COL = "\u6700\u7ec8bu"
C_SUB = "\u4e3b\u4f53.1"
C_CH_SPLIT = "\u6e20\u9053-\u5206\u884c"
C_CH_CODE = "\u6e20\u9053"
C_IN_SHEET = "\u5165\u91d1\u7b14\u6570"
C_OUT_SHEET = "\u51fa\u91d1\u7b14\u6570"

_RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"


def _strip_pivot_caches_workbook_xml(data: bytes) -> bytes:
    t = data.decode("utf-8")
    t = re.sub(r"<pivotCaches>.*?</pivotCaches>", "", t, count=1, flags=re.DOTALL)
    return t.encode("utf-8")


def _rels_remove_pivot_relationships(data: bytes) -> bytes:
    root = ET.fromstring(data)
    tag_rel = "{%s}Relationship" % _RELS_NS
    drop: list[ET.Element] = []
    for child in root:
        if child.tag != tag_rel:
            continue
        typ = child.get("Type") or ""
        if "pivotCacheDefinition" in typ or "relationships/pivotTable" in typ:
            drop.append(child)
    for c in drop:
        root.remove(c)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _content_types_remove_pivot_parts(data: bytes) -> bytes:
    root = ET.fromstring(data)
    tag_ov = "{%s}Override" % _CT_NS
    drop: list[ET.Element] = []
    for child in root:
        if child.tag != tag_ov:
            continue
        pn = child.get("PartName") or ""
        if "/pivotCache/" in pn or "/pivotTables/" in pn:
            drop.append(child)
    for c in drop:
        root.remove(c)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _xlsx_without_pivot_caches_to_temp(src: Path) -> Path:
    """Copy xlsx to a temp file, dropping pivot cache/table parts so openpyxl loads quickly.

    The template 基数PPHK模版.xlsx ships multi-MB pivotCacheRecords; load_workbook otherwise
    parses them and appears hung. Output is only used for load_workbook + save; original
    ``src`` is not modified.
    """
    skip_prefixes = ("xl/pivotCache/", "xl/pivotTables/")
    fd, tmp = tempfile.mkstemp(suffix="_no_pivot.xlsx")
    os.close(fd)
    tmp_path = Path(tmp)
    try:
        with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(
            tmp_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as zout:
            for item in zin.infolist():
                name = item.filename
                if name.startswith(skip_prefixes):
                    continue
                raw = zin.read(name)
                if name == "xl/workbook.xml":
                    raw = _strip_pivot_caches_workbook_xml(raw)
                elif name == "xl/_rels/workbook.xml.rels":
                    raw = _rels_remove_pivot_relationships(raw)
                elif name == "[Content_Types].xml":
                    raw = _content_types_remove_pivot_parts(raw)
                elif "/worksheets/_rels/" in name and name.endswith(".rels"):
                    raw = _rels_remove_pivot_relationships(raw)
                zout.writestr(item, raw)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return tmp_path


def _strip(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip()


def norm_acct_key(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    if isinstance(v, (int, float)) and v == int(v):
        return str(int(v))
    s = _strip(v)
    if not s or s.lower() == "nan":
        return ""
    if s.endswith(".0") and s[:-2].replace("-", "").isdigit():
        return s[:-2]
    try:
        f = float(s.replace(",", ""))
        if f == int(f):
            return str(int(f))
    except ValueError:
        pass
    return s


def norm_branch_dim(s: object) -> str:
    b = _strip(s).upper()
    if b == "CITIKHK":
        return "CITIHK"
    return b


def load_account_mapping(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    need = {"\u94f6\u884c\u8d26\u53f7", "\u4e3b\u4f531", "\u652f\u884c\u7b80\u79f0"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError("mapping missing %s, have %s" % (miss, list(df.columns)))
    sub = df[list(need)].copy()
    sub["_acct"] = sub["\u94f6\u884c\u8d26\u53f7"].map(norm_acct_key)
    sub = sub[sub["_acct"].ne("")]
    sys_acct = "\u7cfb\u7edf\u8d26\u53f7"
    if sys_acct in df.columns:
        extra = df[[sys_acct, "\u4e3b\u4f531", "\u652f\u884c\u7b80\u79f0"]].copy()
        extra["_acct"] = extra[sys_acct].map(norm_acct_key)
        extra = extra[extra["_acct"].ne("")][["_acct", "\u4e3b\u4f531", "\u652f\u884c\u7b80\u79f0"]]
        sub = pd.concat([sub[["_acct", "\u4e3b\u4f531", "\u652f\u884c\u7b80\u79f0"]], extra], ignore_index=True)
    sub = sub.drop_duplicates(subset=["_acct"], keep="first")
    sub = sub.rename(columns={"\u4e3b\u4f531": "_map_entity", "\u652f\u884c\u7b80\u79f0": "_map_branch"})
    return sub


def row_calendar_month_from_row(d_bank: object, d_bill: object) -> str:
    m = row_calendar_month(d_bank)
    if m:
        return m
    return row_calendar_month(d_bill)


def month_display(ym: str) -> str:
    if len(ym) == 6 and ym.isdigit():
        return "%s-%s" % (ym[:4], ym[4:6])
    return ym


def _report_month_override_from_env() -> str:
    """PECAUSE_CITIHK_REPORT_MONTH：任务期次，覆盖源表入账/账单日历月与答案期次对齐。"""
    import os

    raw = (os.environ.get("PECAUSE_CITIHK_REPORT_MONTH") or "").strip()
    if not raw:
        return ""
    m = norm_month(raw)
    if len(m) == 6 and m.isdigit():
        return m
    return ""


def _effective_taxonomy_month(calendar_mo: str) -> str:
    """先用源列解析出 YYYYMM；CitiHK 流水线若设置 PECAUSE_CITIHK_REPORT_MONTH（任务期次）则替换展示/汇总月份。"""
    if not calendar_mo:
        return ""
    o = _report_month_override_from_env()
    return o if o else calendar_mo


def row_calendar_month(v: object) -> str:
    """Normalize to YYYYMM; handle YYYY-MM-DD from source exports."""
    m = norm_month(v)
    if len(m) == 6 and m.isdigit():
        return m
    s = _strip(v)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        y, mo = s[:4], s[5:7]
        if y.isdigit() and mo.isdigit():
            return y + mo
    return ""


def read_excel_stream(path: Path, usecols: list[str], nrows: Optional[int]) -> pd.DataFrame:
    from openpyxl import load_workbook

    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        it = ws.iter_rows(values_only=True)
        try:
            header_row = next(it)
        except StopIteration:
            return pd.DataFrame(columns=usecols)
        header = [h if h is None else str(h) for h in header_row]
        idx_map = {name: i for i, name in enumerate(header) if name in usecols}
        miss = set(usecols) - set(idx_map.keys())
        if miss:
            raise ValueError("%s missing cols: %s" % (path.name, sorted(miss)))
        rows: list[dict] = []
        taken = 0
        for row in it:
            if nrows is not None and taken >= nrows:
                break
            rec = {
                col: row[idx_map[col]] if idx_map[col] < len(row) else None for col in usecols
            }
            rows.append(rec)
            taken += 1
    finally:
        wb.close()
    return pd.DataFrame(rows)


def _read_one_csv(path: Path, usecols: list[str], nrows: Optional[int]) -> pd.DataFrame:
    return pd.read_csv(
        path,
        usecols=usecols,
        nrows=nrows,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )


def _read_many_csv(
    paths: list[Path],
    usecols: list[str],
    nrows: Optional[int],
    *,
    max_workers: int = 0,
    parallel: str = "thread",
) -> pd.DataFrame:
    eff_workers = max_workers
    if eff_workers <= 0:
        eff_workers = min(8, len(paths)) if len(paths) > 1 else 1
    if len(paths) <= 1 or eff_workers <= 1:
        frames = [_read_one_csv(p, usecols, nrows) for p in paths]
        return pd.concat(frames, ignore_index=True)
    from concurrent.futures import ThreadPoolExecutor

    w = min(eff_workers, len(paths))
    with ThreadPoolExecutor(max_workers=w) as ex:
        frames = list(ex.map(lambda q: _read_one_csv(q, usecols, nrows), paths))
    return pd.concat(frames, ignore_index=True)


def read_many(
    paths: list[Path],
    usecols: list[str],
    nrows: Optional[int],
    *,
    max_workers: int = 0,
    parallel: str = "process",
) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame(columns=usecols)
    sfx = {x.suffix.lower() for x in paths}
    if sfx == {".csv"}:
        return _read_many_csv(
            paths, usecols, nrows, max_workers=max_workers, parallel=parallel
        )
    if ".csv" in sfx:
        raise ValueError(
            "mixed csv and xlsx in paths: %s" % [x.name for x in paths]
        )
    from fast_xlsx import read_many_xlsx

    return read_many_xlsx(
        paths, usecols, nrows, max_workers=max_workers, parallel=parallel
    )


_log = logging.getLogger(__name__)


def _mapping_acct_tuples(mapping: pd.DataFrame) -> dict[str, tuple[str, str]]:
    d: dict[str, tuple[str, str]] = {}
    for _, r in mapping.iterrows():
        a = r["_acct"]
        if not a:
            continue
        sa = str(a)
        if sa not in d:
            d[sa] = (_strip(r["_map_entity"]), _strip(r["_map_branch"]))
    return d


def _eff_csv_workers(max_workers: int, nfiles: int) -> int:
    w = max_workers
    if w <= 0:
        w = min(8, nfiles)
    return max(1, min(w, nfiles))


def _counter_to_inbound_summary(cnt: Counter) -> pd.DataFrame:
    rows = []
    for (mo, fbu, acct), n in sorted(cnt.items(), key=lambda x: (x[0][0], x[0][2], x[0][1])):
        rows.append({"_month": mo, "_final_bu": fbu, "_acct": acct, C_IN_CT: int(n)})
    if not rows:
        return pd.DataFrame(
            columns=["_month", "_final_bu", "_acct", C_IN_CT, C_MONTH, C_ENTITY, C_CH, C_ACCT_MAIN, C_FINALBU]
        )
    agg = pd.DataFrame(rows)
    agg[C_MONTH] = agg["_month"].map(month_display)
    agg[C_ENTITY] = TARGET_ENTITY
    agg[C_CH] = CHANNEL_DETAIL
    agg[C_ACCT_MAIN] = agg["_acct"]
    agg[C_FINALBU] = agg["_final_bu"]
    return agg[
        ["_month", C_MONTH, C_ENTITY, C_CH, C_ACCT_MAIN, C_FINALBU, C_IN_CT]
    ].sort_values([C_MONTH, C_ACCT_MAIN, C_FINALBU])


def _counter_to_outbound_summary(cnt: Counter) -> pd.DataFrame:
    rows = []
    for (mo, fbu, acct), n in sorted(cnt.items(), key=lambda x: (x[0][0], x[0][2], x[0][1])):
        rows.append({"_month": mo, "_final_bu": fbu, "_acct": acct, C_OUT_CT: int(n)})
    if not rows:
        return pd.DataFrame(
            columns=["_month", "_final_bu", "_acct", C_OUT_CT, C_MONTH, C_ENTITY, C_CH, C_ACCT_MAIN, C_FINALBU]
        )
    agg = pd.DataFrame(rows)
    agg[C_MONTH] = agg["_month"].map(month_display)
    agg[C_ENTITY] = TARGET_ENTITY
    agg[C_CH] = CHANNEL_DETAIL
    agg[C_ACCT_MAIN] = agg["_acct"]
    agg[C_FINALBU] = agg["_final_bu"]
    return agg[
        ["_month", C_MONTH, C_ENTITY, C_CH, C_ACCT_MAIN, C_FINALBU, C_OUT_CT]
    ].sort_values([C_MONTH, C_ACCT_MAIN, C_FINALBU])


def _stream_aggregate_inbound_csv(
    path: Path,
    map_d: dict[str, tuple[str, str]],
    *,
    nrows: Optional[int],
    summary_only: bool,
) -> tuple[Counter, list[dict], Optional[Path]]:
    col_ent, col_bank, col_acct, col_bu, col_dbank, col_bill = INBOUND_USECOLS
    cnt: Counter = Counter()
    dropped: list[dict] = []
    line_path: Optional[Path] = None
    line_f: Any = None
    line_w: Any = None
    line_fields = [
        col_ent,
        col_bank,
        col_acct,
        "BU",
        col_dbank,
        col_bill,
        "_month",
        "_final_bu",
        "_acct",
        "_map_entity",
        "_map_branch",
    ]
    if not summary_only:
        line_path = Path(tempfile.NamedTemporaryFile(delete=False, suffix="_inlines.csv").name)
        line_f = line_path.open("w", newline="", encoding="utf-8-sig")
        line_w = csv.DictWriter(line_f, fieldnames=line_fields)
        line_w.writeheader()

    read_i = 0
    n_eb = n_acct = n_map = n_br = n_bu = n_mo = 0
    n_txn = 0
    _log.info("入金 csv 扫描开始: %s", path.name)
    with path.open(newline="", encoding="utf-8-sig") as fp:
        rdr = csv.DictReader(fp)
        fn = rdr.fieldnames or []
        for c in INBOUND_USECOLS:
            if c not in fn:
                raise ValueError("%s missing column %r (have %s)" % (path.name, c, fn))
        for row in rdr:
            if nrows is not None and read_i >= nrows:
                break
            read_i += 1
            ent = row.get(col_ent)
            bank = row.get(col_bank)
            acct_raw = row.get(col_acct)
            bu_raw = row.get("BU")
            d_bank = row.get(col_dbank)
            d_bill = row.get(col_bill)
            mo = _effective_taxonomy_month(row_calendar_month_from_row(d_bank, d_bill))
            er = norm_entity(ent)
            if not (er == TARGET_ENTITY and bank_passes_citihk_filter(bank)):
                continue
            n_eb += 1
            acct = norm_acct_key(acct_raw)
            if not acct:
                continue
            n_acct += 1
            mp = map_d.get(acct)
            if not mp:
                if not summary_only:
                    dropped.append(
                        {
                            col_ent: ent,
                            col_bank: bank,
                            col_acct: acct_raw,
                            "BU": bu_raw,
                            col_dbank: d_bank,
                            col_bill: d_bill,
                            "_month": mo,
                            "_final_bu": "",
                            "_acct": acct,
                            "_map_entity": "",
                            "_map_branch": "",
                            "_reason": "no_mapping_acct",
                        }
                    )
                continue
            m_ent, m_br = mp
            if not _strip(m_ent):
                if not summary_only:
                    dropped.append(
                        {
                            col_ent: ent,
                            col_bank: bank,
                            col_acct: acct_raw,
                            "BU": bu_raw,
                            col_dbank: d_bank,
                            col_bill: d_bill,
                            "_month": mo,
                            "_final_bu": "",
                            "_acct": acct,
                            "_map_entity": m_ent,
                            "_map_branch": m_br,
                            "_reason": "no_mapping_acct",
                        }
                    )
                continue
            n_map += 1
            mbr = norm_branch_dim(m_br)
            # 需求：源数据已筛选公司主体=PPHK，mapping仅用于获取分行维度
            # 不再要求 mapping 的"主体1"也为 PPHK（部分账户如1049708226的mapping主体为PPGT）
            if mbr != TARGET_BRANCH:
                continue
            n_br += 1
            fbu = resolve_final_bu(bu_raw)
            if not fbu:
                if not summary_only:
                    dropped.append(
                        {
                            col_ent: ent,
                            col_bank: bank,
                            col_acct: acct_raw,
                            "BU": bu_raw,
                            col_dbank: d_bank,
                            col_bill: d_bill,
                            "_month": mo,
                            "_final_bu": "",
                            "_acct": acct,
                            "_map_entity": m_ent,
                            "_map_branch": m_br,
                            "_reason": "unmapped_bu",
                        }
                    )
                continue
            n_bu += 1
            if not mo:
                continue
            n_mo += 1
            cnt[(mo, fbu, acct)] += 1
            n_txn += 1
            if line_w is not None:
                line_w.writerow(
                    {
                        col_ent: ent,
                        col_bank: bank,
                        col_acct: acct_raw,
                        "BU": bu_raw,
                        col_dbank: d_bank,
                        col_bill: d_bill,
                        "_month": mo,
                        "_final_bu": fbu,
                        "_acct": acct,
                        "_map_entity": m_ent,
                        "_map_branch": m_br,
                    }
                )

    if line_f is not None:
        line_f.close()
    _log.info(
        "入金 csv 扫描结束: %s 已读=%s 漏斗[PPHK+CITI=%s 有账号=%s 映射命中=%s CITIHK分行=%s BU=%s 有月份=%s] "
        "汇总维度=%s 计入笔数=%s",
        path.name,
        read_i,
        n_eb,
        n_acct,
        n_map,
        n_br,
        n_bu,
        n_mo,
        len(cnt),
        n_txn,
    )
    return cnt, dropped, line_path


def _stream_aggregate_outbound_csv(
    path: Path,
    map_d: dict[str, tuple[str, str]],
    *,
    nrows: Optional[int],
    summary_only: bool,
) -> tuple[Counter, list[dict], Optional[Path]]:
    col_ent, col_acct, col_bank, col_bu, col_bill = OUTBOUND_USECOLS
    cnt: Counter = Counter()
    dropped: list[dict] = []
    line_path: Optional[Path] = None
    line_f: Any = None
    line_w: Any = None
    line_fields = [
        col_ent,
        col_acct,
        col_bank,
        "BU",
        col_bill,
        "_month",
        "_final_bu",
        "_acct",
        "_map_entity",
        "_map_branch",
    ]
    if not summary_only:
        line_path = Path(tempfile.NamedTemporaryFile(delete=False, suffix="_outlines.csv").name)
        line_f = line_path.open("w", newline="", encoding="utf-8-sig")
        line_w = csv.DictWriter(line_f, fieldnames=line_fields)
        line_w.writeheader()

    read_i = 0
    n_eb = n_acct = n_map = n_br = n_bu = n_mo = n_txn = 0
    _log.info("出金 csv 扫描开始: %s", path.name)
    with path.open(newline="", encoding="utf-8-sig") as fp:
        rdr = csv.DictReader(fp)
        fn = rdr.fieldnames or []
        for c in OUTBOUND_USECOLS:
            if c not in fn:
                raise ValueError("%s missing column %r (have %s)" % (path.name, c, fn))
        for row in rdr:
            if nrows is not None and read_i >= nrows:
                break
            read_i += 1
            ent = row.get(col_ent)
            bank = row.get(col_bank)
            acct_raw = row.get(col_acct)
            bu_raw = row.get("BU")
            d_bill = row.get(col_bill)
            mo = _effective_taxonomy_month(row_calendar_month(d_bill))
            er = norm_entity(ent)
            if not (er == TARGET_ENTITY and bank_passes_citihk_filter(bank)):
                continue
            n_eb += 1
            acct = norm_acct_key(acct_raw)
            if not acct:
                continue
            n_acct += 1
            mp = map_d.get(acct)
            if not mp:
                if not summary_only:
                    dropped.append(
                        {
                            col_ent: ent,
                            col_acct: acct_raw,
                            col_bank: bank,
                            "BU": bu_raw,
                            col_bill: d_bill,
                            "_month": mo,
                            "_final_bu": "",
                            "_acct": acct,
                            "_map_entity": "",
                            "_map_branch": "",
                            "_reason": "no_mapping_acct",
                        }
                    )
                continue
            m_ent, m_br = mp
            if not _strip(m_ent):
                if not summary_only:
                    dropped.append(
                        {
                            col_ent: ent,
                            col_acct: acct_raw,
                            col_bank: bank,
                            "BU": bu_raw,
                            col_bill: d_bill,
                            "_month": mo,
                            "_final_bu": "",
                            "_acct": acct,
                            "_map_entity": m_ent,
                            "_map_branch": m_br,
                            "_reason": "no_mapping_acct",
                        }
                    )
                continue
            n_map += 1
            mbr = norm_branch_dim(m_br)
            # 需求：源数据已筛选代发渠道公司主体=PPHK，mapping仅用于获取分行维度
            # 不再要求 mapping 的"主体1"也为 PPHK
            if mbr != TARGET_BRANCH:
                continue
            n_br += 1
            fbu = resolve_final_bu(bu_raw)
            if not fbu:
                if not summary_only:
                    dropped.append(
                        {
                            col_ent: ent,
                            col_acct: acct_raw,
                            col_bank: bank,
                            "BU": bu_raw,
                            col_bill: d_bill,
                            "_month": mo,
                            "_final_bu": "",
                            "_acct": acct,
                            "_map_entity": m_ent,
                            "_map_branch": m_br,
                            "_reason": "unmapped_bu",
                        }
                    )
                continue
            n_bu += 1
            if not mo:
                continue
            n_mo += 1
            cnt[(mo, fbu, acct)] += 1
            n_txn += 1
            if line_w is not None:
                line_w.writerow(
                    {
                        col_ent: ent,
                        col_acct: acct_raw,
                        col_bank: bank,
                        "BU": bu_raw,
                        col_bill: d_bill,
                        "_month": mo,
                        "_final_bu": fbu,
                        "_acct": acct,
                        "_map_entity": m_ent,
                        "_map_branch": m_br,
                    }
                )

    if line_f is not None:
        line_f.close()
    _log.info(
        "出金 csv 扫描结束: %s 已读=%s 漏斗[PPHK+CITI=%s 有账号=%s 映射命中=%s CITIHK分行=%s BU=%s 有月份=%s] "
        "汇总维度=%s 计入笔数=%s",
        path.name,
        read_i,
        n_eb,
        n_acct,
        n_map,
        n_br,
        n_bu,
        n_mo,
        len(cnt),
        n_txn,
    )
    return cnt, dropped, line_path


def _merge_counters(cs: list[Counter]) -> Counter:
    out = Counter()
    for c in cs:
        out.update(c)
    return out


def process_inbound_csv_stream(
    paths: list[Path],
    mapping: pd.DataFrame,
    *,
    nrows: Optional[int],
    max_workers: int,
    summary_only: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    map_d = _mapping_acct_tuples(mapping)
    w = _eff_csv_workers(max_workers, len(paths))
    if len(paths) == 1 or w == 1:
        parts = [_stream_aggregate_inbound_csv(p, map_d, nrows=nrows, summary_only=summary_only) for p in paths]
    else:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=w) as ex:
            parts = list(
                ex.map(
                    lambda q: _stream_aggregate_inbound_csv(q, map_d, nrows=nrows, summary_only=summary_only),
                    paths,
                )
            )
    cnt = _merge_counters([p[0] for p in parts])
    summary = _counter_to_inbound_summary(cnt)
    if summary_only:
        empty_cols = INBOUND_USECOLS + ["_month", "_final_bu", "_acct", "_map_entity", "_map_branch"]
        return summary, pd.DataFrame(columns=empty_cols), pd.DataFrame()
    dropped = pd.DataFrame([r for p in parts for r in p[1]])
    line_frames = []
    for p in parts:
        lp = p[2]
        if lp is not None and lp.is_file() and lp.stat().st_size > 0:
            line_frames.append(pd.read_csv(lp, dtype=str, keep_default_na=False, encoding="utf-8-sig"))
            try:
                lp.unlink()
            except OSError:
                pass
    if not line_frames:
        line_detail = pd.DataFrame(
            columns=[
                INBOUND_USECOLS[0],
                INBOUND_USECOLS[1],
                INBOUND_USECOLS[2],
                "BU",
                INBOUND_USECOLS[4],
                INBOUND_USECOLS[5],
                "_month",
                "_final_bu",
                "_acct",
                "_map_entity",
                "_map_branch",
            ]
        )
    else:
        line_detail = pd.concat(line_frames, ignore_index=True)
    return summary, line_detail, dropped


def process_outbound_csv_stream(
    paths: list[Path],
    mapping: pd.DataFrame,
    *,
    nrows: Optional[int],
    max_workers: int,
    summary_only: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    map_d = _mapping_acct_tuples(mapping)
    w = _eff_csv_workers(max_workers, len(paths))
    if len(paths) == 1 or w == 1:
        parts = [_stream_aggregate_outbound_csv(p, map_d, nrows=nrows, summary_only=summary_only) for p in paths]
    else:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=w) as ex:
            parts = list(
                ex.map(
                    lambda q: _stream_aggregate_outbound_csv(q, map_d, nrows=nrows, summary_only=summary_only),
                    paths,
                )
            )
    cnt = _merge_counters([p[0] for p in parts])
    summary = _counter_to_outbound_summary(cnt)
    if summary_only:
        oc0, oc1, oc2, oc3, oc4 = OUTBOUND_USECOLS
        empty_cols = [oc0, oc1, oc2, "BU", oc4, "_month", "_final_bu", "_acct", "_map_entity", "_map_branch"]
        return summary, pd.DataFrame(columns=empty_cols), pd.DataFrame()
    dropped = pd.DataFrame([r for p in parts for r in p[1]])
    line_frames = []
    for p in parts:
        lp = p[2]
        if lp is not None and lp.is_file() and lp.stat().st_size > 0:
            line_frames.append(pd.read_csv(lp, dtype=str, keep_default_na=False, encoding="utf-8-sig"))
            try:
                lp.unlink()
            except OSError:
                pass
    if not line_frames:
        o0, o1, o2, o3, o4 = OUTBOUND_USECOLS
        line_detail = pd.DataFrame(
            columns=[o0, o1, o2, "BU", o4, "_month", "_final_bu", "_acct", "_map_entity", "_map_branch"]
        )
    else:
        line_detail = pd.concat(line_frames, ignore_index=True)
    return summary, line_detail, dropped


def process_inbound(
    paths: list[Path],
    mapping: pd.DataFrame,
    *,
    nrows: Optional[int],
    max_workers: int = 0,
    parallel: str = "process",
    summary_only: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if paths and all(x.suffix.lower() == ".csv" for x in paths):
        return process_inbound_csv_stream(
            paths,
            mapping,
            nrows=nrows,
            max_workers=max_workers,
            summary_only=summary_only,
        )
    df0 = read_many(
        paths, INBOUND_USECOLS, nrows, max_workers=max_workers, parallel=parallel
    )
    col_ent = "\u516c\u53f8\u4e3b\u4f53"
    col_bank = "\u94f6\u884c/\u901a\u9053\u540d\u79f0"
    col_acct = "\u4e3b\u4f53\u8d26\u53f7"
    col_dbank = "\u94f6\u884c\u5165\u8d26\u65e5\u671f"
    col_bill = "\u8d26\u5355\u65e5\u671f"
    df = df0.copy()
    df["_month"] = [
        _effective_taxonomy_month(row_calendar_month_from_row(a, b))
        for a, b in zip(df[col_dbank], df[col_bill], strict=False)
    ]
    df["_entity_raw"] = df[col_ent].map(norm_entity)
    df = df[
        df["_entity_raw"].eq(TARGET_ENTITY) & df[col_bank].map(bank_passes_citihk_filter)
    ].copy()
    df["_acct"] = df[col_acct].map(norm_acct_key)
    df = df.join(mapping.set_index("_acct"), on="_acct", how="left")
    no_map = pd.DataFrame()
    unmapped_bu = pd.DataFrame()
    if not summary_only:
        no_map = df[df["_map_entity"].isna() | (df["_map_entity"].map(_strip) == "")].copy()
    df = df[df["_map_entity"].notna() & df["_map_entity"].map(_strip).ne("")].copy()
    df["_mbr"] = df["_map_branch"].map(norm_branch_dim)
    # 需求：源数据已筛选公司主体=PPHK，mapping仅用于获取分行维度
    # 不再要求 mapping 的"主体1"也为 PPHK
    df = df[df["_mbr"].eq(TARGET_BRANCH)].copy()
    df["_bu_raw"] = df["BU"].map(_strip)
    df["_final_bu"] = df["_bu_raw"].map(resolve_final_bu)
    if not summary_only:
        unmapped_bu = df[df["_final_bu"].eq("")].copy()
    df = df[df["_final_bu"].ne("")].copy()
    df = df[df["_month"].ne("")].copy()
    dropped = pd.DataFrame()
    if not summary_only:
        dropped = pd.concat(
            [
                no_map.assign(_reason="no_mapping_acct"),
                unmapped_bu.assign(_reason="unmapped_bu"),
            ],
            ignore_index=True,
        )
    keys = ["_month", "_final_bu", "_acct"]
    agg = df.groupby(keys, dropna=False).size().reset_index(name=C_IN_CT)
    agg[C_MONTH] = agg["_month"].map(month_display)
    agg[C_ENTITY] = TARGET_ENTITY
    agg[C_CH] = CHANNEL_DETAIL
    agg[C_ACCT_MAIN] = agg["_acct"]
    agg[C_FINALBU] = agg["_final_bu"]
    summary = agg[
        ["_month", C_MONTH, C_ENTITY, C_CH, C_ACCT_MAIN, C_FINALBU, C_IN_CT]
    ].sort_values([C_MONTH, C_ACCT_MAIN, C_FINALBU])
    if summary_only:
        line_detail = pd.DataFrame(
            columns=[
                col_ent,
                col_bank,
                col_acct,
                "BU",
                col_dbank,
                col_bill,
                "_month",
                "_final_bu",
                "_acct",
                "_map_entity",
                "_map_branch",
            ]
        )
    else:
        line_detail = df[
            [
                col_ent,
                col_bank,
                col_acct,
                "BU",
                col_dbank,
                col_bill,
                "_month",
                "_final_bu",
                "_acct",
                "_map_entity",
                "_map_branch",
            ]
        ].copy()
    return summary, line_detail, dropped


def process_outbound(
    paths: list[Path],
    mapping: pd.DataFrame,
    *,
    nrows: Optional[int],
    max_workers: int = 0,
    parallel: str = "process",
    summary_only: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if paths and all(x.suffix.lower() == ".csv" for x in paths):
        return process_outbound_csv_stream(
            paths,
            mapping,
            nrows=nrows,
            max_workers=max_workers,
            summary_only=summary_only,
        )
    df0 = read_many(
        paths, OUTBOUND_USECOLS, nrows, max_workers=max_workers, parallel=parallel
    )
    col_ent = "\u4ee3\u53d1\u6e20\u9053\u7684\u516c\u53f8\u4e3b\u4f53"
    col_bank = "\u94f6\u884c/\u901a\u9053\u540d\u79f0"
    col_acct = "\u4ee3\u53d1\u6e20\u9053\u7684\u5927\u8d26\u53f7"
    col_bill = "\u8d26\u5355\u65e5\u671f"
    df = df0.copy()
    df["_month"] = df[col_bill].map(
        lambda v: _effective_taxonomy_month(row_calendar_month(v))
    )
    df["_entity_raw"] = df[col_ent].map(norm_entity)
    df = df[
        df["_entity_raw"].eq(TARGET_ENTITY) & df[col_bank].map(bank_passes_citihk_filter)
    ].copy()
    df["_acct"] = df[col_acct].map(norm_acct_key)
    df = df.join(mapping.set_index("_acct"), on="_acct", how="left")
    no_map = pd.DataFrame()
    unmapped_bu = pd.DataFrame()
    if not summary_only:
        no_map = df[df["_map_entity"].isna() | (df["_map_entity"].map(_strip) == "")].copy()
    df = df[df["_map_entity"].notna() & df["_map_entity"].map(_strip).ne("")].copy()
    df["_mbr"] = df["_map_branch"].map(norm_branch_dim)
    # 需求：源数据已筛选代发渠道公司主体=PPHK，mapping仅用于获取分行维度
    # 不再要求 mapping 的"主体1"也为 PPHK
    df = df[df["_mbr"].eq(TARGET_BRANCH)].copy()
    df["_bu_raw"] = df["BU"].map(_strip)
    df["_final_bu"] = df["_bu_raw"].map(resolve_final_bu)
    if not summary_only:
        unmapped_bu = df[df["_final_bu"].eq("")].copy()
    df = df[df["_final_bu"].ne("")].copy()
    df = df[df["_month"].ne("")].copy()
    dropped = pd.DataFrame()
    if not summary_only:
        dropped = pd.concat(
            [
                no_map.assign(_reason="no_mapping_acct"),
                unmapped_bu.assign(_reason="unmapped_bu"),
            ],
            ignore_index=True,
        )
    keys = ["_month", "_final_bu", "_acct"]
    agg = df.groupby(keys, dropna=False).size().reset_index(name=C_OUT_CT)
    agg[C_MONTH] = agg["_month"].map(month_display)
    agg[C_ENTITY] = TARGET_ENTITY
    agg[C_CH] = CHANNEL_DETAIL
    agg[C_ACCT_MAIN] = agg["_acct"]
    agg[C_FINALBU] = agg["_final_bu"]
    summary = agg[
        ["_month", C_MONTH, C_ENTITY, C_CH, C_ACCT_MAIN, C_FINALBU, C_OUT_CT]
    ].sort_values([C_MONTH, C_ACCT_MAIN, C_FINALBU])
    if summary_only:
        line_detail = pd.DataFrame(
            columns=[
                col_ent,
                col_acct,
                col_bank,
                "BU",
                col_bill,
                "_month",
                "_final_bu",
                "_acct",
                "_map_entity",
                "_map_branch",
            ]
        )
    else:
        line_detail = df[
            [
                col_ent,
                col_acct,
                col_bank,
                "BU",
                col_bill,
                "_month",
                "_final_bu",
                "_acct",
                "_map_entity",
                "_map_branch",
            ]
        ].copy()
    return summary, line_detail, dropped


def to_template_inbound(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in summary.iterrows():
        ym = str(r["_month"])
        m_int = int(ym) if ym.isdigit() and len(ym) == 6 else ym
        rows.append(
            {
                C_MONTH: r[C_MONTH],
                C_ENTITY: TARGET_ENTITY,
                C_CH: CHANNEL_TEMPLATE,
                C_BIG: str(r[C_ACCT_MAIN]),
                "\u4e1a\u52a1\u7cfb\u7edf": None,
                "\u5ba2\u6237kyc\u56fd\u5bb6": None,
                C_IN_CT: int(r[C_IN_CT]),
                C_IN_VOL: 0.0,
                C_FINALBU_COL: r[C_FINALBU],
                "month": m_int,
                "BU": r[C_FINALBU],
                C_CH_CODE: CHANNEL_CODE,
                C_CH_SPLIT: CHANNEL_TEMPLATE,
                C_SUB: TARGET_ENTITY,
            }
        )
    return pd.DataFrame(rows)


def to_template_outbound(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in summary.iterrows():
        ym = str(r["_month"])
        m_int = int(ym) if ym.isdigit() and len(ym) == 6 else ym
        rows.append(
            {
                C_MONTH: r[C_MONTH],
                C_ENTITY: TARGET_ENTITY,
                C_CH: CHANNEL_TEMPLATE,
                C_BIG: str(r[C_ACCT_MAIN]),
                "\u4e1a\u52a1\u7cfb\u7edf": None,
                "\u5ba2\u6237kyc\u56fd\u5bb6": None,
                C_OUT_CT: int(r[C_OUT_CT]),
                C_OUT_VOL: 0.0,
                C_FINALBU_COL: r[C_FINALBU],
                "month": m_int,
                "BU": r[C_FINALBU],
                C_CH_CODE: CHANNEL_CODE,
                C_CH_SPLIT: CHANNEL_TEMPLATE,
                C_SUB: TARGET_ENTITY,
            }
        )
    return pd.DataFrame(rows)


def default_inbound_paths(d: Path) -> list[Path]:
    csvs = sorted(d.glob("2-Inbound*.csv"))
    csvs = [p for p in csvs if not p.name.startswith("~$")]
    if len(csvs) == 3:
        return csvs
    paths = sorted(d.glob("2-Inbound*.xlsx"))
    paths = [p for p in paths if not p.name.startswith("~$")]
    if len(paths) != 3:
        raise ValueError(
            "expected 3 Inbound files (2-Inbound*.xlsx or 2-Inbound*.csv), "
            "found xlsx=%s csv=%s"
            % ([p.name for p in paths], [p.name for p in csvs])
        )
    return paths


def resolve_slip_xlsx(d: Path) -> Path | None:
    """资金流 slip 表：历史上为 资金流slip.xlsx，亦有导出为 4资金流slip.xlsx。"""
    for name in ("资金流slip.xlsx", "4资金流slip.xlsx"):
        p = d / name
        if p.is_file():
            return p
    return None


def resolve_slip_csv(d: Path) -> Path | None:
    for name in ("资金流slip.csv", "4资金流slip.csv"):
        p = d / name
        if p.is_file():
            return p
    return None


def default_outbound_paths(d: Path) -> list[Path]:
    ax, ac = d / "4outbound.xlsx", d / "4outbound.csv"
    bx = resolve_slip_xlsx(d)
    bc = resolve_slip_csv(d)
    if ac.is_file() and bc is not None:
        return [ac, bc]
    if not ax.is_file() or bx is None:
        raise FileNotFoundError(
            "need 4outbound.xlsx and (资金流slip.xlsx|4资金流slip.xlsx), "
            "or 4outbound.csv and (资金流slip.csv|4资金流slip.csv)"
        )
    return [ax, bx]


def write_workbook(
    out: Path,
    *,
    template_path: Optional[Path],
    in_summary: pd.DataFrame,
    out_summary: pd.DataFrame,
    in_lines: pd.DataFrame,
    out_lines: pd.DataFrame,
    dropped: pd.DataFrame,
    summary_only: bool = True,
) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmpl_in = to_template_inbound(in_summary)
    tmpl_out = to_template_outbound(out_summary)
    d_in = in_summary.drop(columns=["_month"], errors="ignore").copy()
    d_out = out_summary.drop(columns=["_month"], errors="ignore").copy()

    if template_path and template_path.is_file():
        from openpyxl import load_workbook

        tmp_scrub: Optional[Path] = None
        try:
            tmp_scrub = _xlsx_without_pivot_caches_to_temp(template_path)
            _log.info(
                "已用无透视缓存副本加载模板（避免解析超大 pivotCacheRecords，原文件未改）"
            )
            wb = load_workbook(tmp_scrub)
        finally:
            if tmp_scrub is not None:
                try:
                    tmp_scrub.unlink(missing_ok=True)
                except OSError:
                    pass
        for name, df_t in [(C_IN_SHEET, tmpl_in), (C_OUT_SHEET, tmpl_out)]:
            if name not in wb.sheetnames:
                raise ValueError(
                    "%s: missing sheet %r (need two header rows like full bases template)."
                    % (template_path, name)
                )
            ws = wb[name]
            tmax = ws.max_row
            if tmax > 2:
                ws.delete_rows(3, tmax - 2)
            cols = list(df_t.columns)
            for r_i, row in enumerate(df_t.itertuples(index=False), start=3):
                for c_i in range(len(cols)):
                    ws.cell(row=r_i, column=c_i + 1, value=row[c_i])

        if not summary_only:
            extras = [
                ("\u5165\u91d1\u57fa\u6570_\u660e\u7ec6", d_in),
                ("\u51fa\u91d1\u57fa\u6570_\u660e\u7ec6", d_out),
                ("\u5165\u91d1\u660e\u7ec6_\u884c", in_lines),
                ("\u51fa\u91d1\u660e\u7ec6_\u884c", out_lines),
                ("\u5254\u9664_\u5f85\u6838\u5bf9", dropped),
            ]
            for sname, sdf in extras:
                if sname in wb.sheetnames:
                    wb.remove(wb[sname])
                wsn = wb.create_sheet(sname)
                if sdf.empty:
                    wsn.cell(row=1, column=1, value="(empty)")
                    continue
                for c_i, col in enumerate(sdf.columns, start=1):
                    wsn.cell(row=1, column=c_i, value=str(col))
                for r_i, row in enumerate(sdf.itertuples(index=False), start=2):
                    for c_i, val in enumerate(row, start=1):
                        wsn.cell(row=r_i, column=c_i, value=val)
        wb.save(out)
        wb.close()
    else:
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            tmpl_in.to_excel(writer, sheet_name="in_bases", index=False)
            tmpl_out.to_excel(writer, sheet_name="out_bases", index=False)
            if not summary_only:
                d_in.to_excel(writer, sheet_name="in_detail", index=False)
                d_out.to_excel(writer, sheet_name="out_detail", index=False)
                if not in_lines.empty:
                    in_lines.to_excel(writer, sheet_name="in_lines", index=False)
                if not out_lines.empty:
                    out_lines.to_excel(writer, sheet_name="out_lines", index=False)
                if not dropped.empty:
                    dropped.to_excel(writer, sheet_name="dropped", index=False)
