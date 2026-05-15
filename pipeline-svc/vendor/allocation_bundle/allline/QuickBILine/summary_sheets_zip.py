"""Inject 入金汇总/出金汇总/VA汇总 via OOXML zip patch — avoids openpyxl full parse (pivot caches)."""
from __future__ import annotations

import io
import logging
import re
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
from openpyxl.utils.cell import get_column_letter

from .template_pivot_scrub import sanitize_workbook_xml_for_excel, strip_opc_calc_chain_from_package

_LOG = logging.getLogger("quickbi.line")

MAIN_SS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

REL_WS = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
)
WS_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
)

SRC_IN = "\u5165\u91d1"
SRC_OUT = "\u51fa\u91d1"
SRC_VA = "VA"
SUM_IN = "\u5165\u91d1\u6c47\u603b"
SUM_OUT = "\u51fa\u91d1\u6c47\u603b"
SUM_VA = "VA\u6c47\u603b"

_COL_IN_OUT = ("A", "B", "C", "D", "F", "G", "H", "I", "W")
_COL_VA = ("A", "C", "B", "H", "T")

_HDR_IN = (
    "\u6708\u4efd",
    "\u4e3b\u4f53",
    "\u6e20\u9053\u540d\u79f0",
    "\u5927\u8d26\u53f7",
    "\u4e1a\u52a1\u7cfb\u7edf",
    "\u5ba2\u6237kyc\u56fd\u5bb6",
    "\u5165\u91d1\u7b14\u6570",
    "\u5165\u91d1\u4ea4\u6613\u91cf",
    "\u6700\u7ec8 bu",
)
_HDR_OUT = (
    "\u6708\u4efd",
    "\u4e3b\u4f53",
    "\u6e20\u9053\u540d\u79f0",
    "\u5927\u8d26\u53f7",
    "\u4e1a\u52a1\u7cfb\u7edf",
    "\u5ba2\u6237kyc\u56fd\u5bb6",
    "\u51fa\u91d1\u7b14\u6570",
    "\u51fa\u91d1\u4ea4\u6613\u91cf",
    "\u6700\u7ec8 bu",
)
_HDR_VA = (
    "\u6708\u4efd",
    "\u6e20\u9053\u540d\u79f0",
    "\u4e3b\u4f53",
    "va\u6570",
    "\u6700\u7ec8 bu",
)

_SUMMARY = (SUM_IN, SUM_OUT, SUM_VA)

_XML_ESCAPE = str.maketrans({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&apos;",
})


def _escape_t(s: str) -> str:
    return _sanitize_xml_characters(s).translate(_XML_ESCAPE)


def _sanitize_xml_characters(s: str) -> str:
    """Drop XML 1.0 illegal control chars (Excel may repair/clear sheets that contain them)."""
    out: list[str] = []
    for ch in str(s):
        o = ord(ch)
        if o in (0x9, 0xA, 0xD) or 0x20 <= o <= 0xD7FF:
            out.append(ch)
        elif 0xE000 <= o <= 0xFFFD:
            out.append(ch)
        elif o >= 0x10000 and o <= 0x10FFFF:
            out.append(ch)
    return "".join(out)


def _workbook_sheet_tag_stem(wb_xml: str) -> str:
    """Tag used for workbook child sheets (``sheet`` or ``ns0:sheet``).

    Older regexes assumed ``<sheet`` only; if the file uses a prefix we must match it when removing
    and use the same form when inserting, otherwise duplicates appear and Excel repair wipes them.
    """
    i = wb_xml.find("<sheets")
    if i < 0:
        return "sheet"
    head = wb_xml[i : i + 12000]
    m = re.search(r"<((?:[a-zA-Z][a-zA-Z0-9]*:)?sheet)\b", head)
    return m.group(1) if m else "sheet"


def _ftext(src_sheet: str, col: str, row: int) -> str:
    if src_sheet.isalnum() and src_sheet.isascii():
        return "%s!%s%d" % (src_sheet, col, row)
    return "'%s'!%s%d" % (src_sheet, col, row)


def _build_worksheet_xml_from_df(
    *,
    headers: tuple[str, ...],
    agg_df: pd.DataFrame,
    col_order: list[str],
) -> bytes:
    """Build worksheet XML with actual aggregated data values (not formula refs)."""
    n_hdr = len(headers)
    last_col = get_column_letter(n_hdr)
    nrows = len(agg_df)
    last_row = max(1, nrows + 1)
    chunks: list[str] = []
    chunks.append(
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="%s" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<dimension ref="A1:%s%d"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        "<sheetData>"
        % (MAIN_SS, last_col, last_row)
    )
    # Header row
    h_cells = []
    for i, h in enumerate(headers, start=1):
        cl = get_column_letter(i)
        h_cells.append(
            '<c r="%s1" t="inlineStr"><is><t>%s</t></is></c>' % (cl, _escape_t(h))
        )
    chunks.append('<row r="1">%s</row>' % "".join(h_cells))
    # Data rows
    for i, row in enumerate(agg_df.itertuples(index=False, name=None)):
        r = 2 + i
        cells = []
        for j, col_name in enumerate(col_order, start=1):
            cl = get_column_letter(j)
            val = None
            if col_name in agg_df.columns:
                idx = list(agg_df.columns).index(col_name)
                val = row[idx]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                cells.append('<c r="%s%d"/>' % (cl, r))
            elif isinstance(val, (int, float)) and not isinstance(val, bool):
                cells.append('<c r="%s%d"><v>%s</v></c>' % (cl, r, val))
            else:
                cells.append(
                    '<c r="%s%d" t="inlineStr"><is><t>%s</t></is></c>'
                    % (cl, r, _escape_t(str(val)))
                )
        chunks.append('<row r="%d">%s</row>' % (r, "".join(cells)))
    chunks.append("</sheetData></worksheet>")
    return "".join(chunks).encode("utf-8")


def _build_worksheet_xml(
    *,
    headers: tuple[str, ...],
    src_cols: tuple[str, ...],
    src_sheet: str,
    nrows: int,
) -> bytes:
    n_hdr = len(headers)
    last_col = get_column_letter(n_hdr)
    last_row = max(1, nrows + 1)
    chunks: list[str] = []
    chunks.append(
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="%s" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<dimension ref="A1:%s%d"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        "<sheetData>"
        % (MAIN_SS, last_col, last_row)
    )
    h_cells = []
    for i, h in enumerate(headers, start=1):
        cl = get_column_letter(i)
        h_cells.append(
            '<c r="%s1" t="inlineStr"><is><t>%s</t></is></c>' % (cl, _escape_t(h))
        )
    chunks.append('<row r="1">%s</row>' % "".join(h_cells))
    for r in range(2, nrows + 2):
        cells = []
        for j, letter in enumerate(src_cols, start=1):
            cl = get_column_letter(j)
            cells.append(
                '<c r="%s%d"><f>%s</f></c>' % (cl, r, _ftext(src_sheet, letter, r))
            )
        chunks.append('<row r="%d">%s</row>' % (r, "".join(cells)))
    chunks.append("</sheetData></worksheet>")
    return "".join(chunks).encode("utf-8")


def _zip_path_for_target(target: str) -> str:
    t = target.replace("\\", "/").lstrip("/")
    if t.startswith("xl/"):
        return t
    return "xl/" + t


def _element_tag_stem_from_xml(xml: str, local_name: str) -> str:
    """Return stem of the first occurrence of ``local_name`` matching existing style (e.g. ``ns0:Relationship`` or ``Relationship``)."""
    m = re.search(
        r"<((?:[a-zA-Z][a-zA-Z0-9]*:)?" + re.escape(local_name) + r")\b",
        xml,
    )
    return m.group(1) if m else local_name


def _append_before_closing_tag(xml: str, local_name: str, fragment: str) -> str:
    """Insert *fragment* immediately before the last ``</...localName>`` (allows ``ns0:`` prefix).

    ``workbook.xml.rels`` / ``[Content_Types].xml`` are often rewritten with a default namespace
    prefix (``ns0:``). String replace on ``</Relationships>`` then fails silently so new worksheet
    relationships and overrides are never written — workbook lists rId36+ with no Target → Excel
    repair strips those sheets (empty tabs, no headers).
    """
    pat = re.compile(r"</(?:[a-zA-Z0-9]+:)?" + re.escape(local_name) + r">")
    matches = list(pat.finditer(xml))
    if not matches:
        raise ValueError("no closing tag %r in xml" % local_name)
    m = matches[-1]
    return xml[: m.start()] + fragment + xml[m.start() :]


def _strip_one_workbook_sheet_elements(container: str, nm: str) -> tuple[str, str | None]:
    """Inside ``<sheets>…`` inner XML only: remove ``<sheet ... name=nm .../>``; return (*new*, rId).

    Older pattern ``<sheet[^>]*name="…">`` is wrong when attributes are ordered ``r:id`` / ``sheetId``
    before ``name`` — ``[^>]`` cannot span the whole tag while still matching ``name="",`` so removal
    failed, duplicate summaries were appended, and Excel repair cleared them.
    """
    needle = 'name="%s"' % nm
    j = container.find(needle)
    if j < 0:
        return container, None
    lt = container.rfind("<", 0, j)
    if lt < 0:
        return container, None
    head = container[lt : lt + 48]
    if not re.match(r"^<(?:(?:[a-zA-Z][a-zA-Z0-9]*:)?sheet)\b", head):
        return container, None
    k = container.find("/>", j)
    if k < 0:
        return container, None
    end = k + 2
    seg = container[lt:end]
    rid_m = re.search(r'r:id="(rId\d+)"', seg)
    rid = rid_m.group(1) if rid_m else None
    return container[:lt] + container[end:], rid


def _remove_summary_sheets_from_package(data: dict[str, bytes]) -> None:
    wb_s = data["xl/workbook.xml"].decode("utf-8")
    rel_s = data["xl/_rels/workbook.xml.rels"].decode("utf-8")
    ct_s = data["[Content_Types].xml"].decode("utf-8")

    mo = re.search(r"<(?:[a-zA-Z][a-zA-Z0-9]*:)?sheets\b[^>]*>", wb_s)
    mc = re.search(r"</(?:[a-zA-Z0-9]+:)?sheets>", wb_s)
    if not mo or not mc:
        raise ValueError("workbook.xml missing <sheets>...</sheets>")
    inner_start = mo.end()
    inner_end = mc.start()
    inner = wb_s[inner_start:inner_end]

    removed_rids: list[str] = []
    for nm in _SUMMARY:
        inner, rid = _strip_one_workbook_sheet_elements(inner, nm)
        if rid is not None:
            removed_rids.append(rid)
    wb_s = wb_s[:inner_start] + inner + wb_s[inner_end:]

    for rid in removed_rids:
        m = re.search(
            r"<(?:[a-zA-Z0-9]+:)?Relationship[^>]*Id=\"%s\"[^>]*Target=\"([^\"]+)\""
            % re.escape(rid),
            rel_s,
        )
        tgt = m.group(1) if m else None
        rel_s = re.sub(
            r"<(?:[a-zA-Z0-9]+:)?Relationship[^>]*Id=\"%s\"[^>]*/>\s*" % re.escape(rid),
            "",
            rel_s,
            count=1,
        )
        if tgt:
            zp = _zip_path_for_target(tgt)
            data.pop(zp, None)
            part = "/" + zp
            ct_s = re.sub(
                r"<(?:[a-zA-Z0-9]+:)?Override[^>]*PartName=\"%s\"[^>]*/>\s*"
                % re.escape(part),
                "",
                ct_s,
                count=1,
            )

    data["xl/workbook.xml"] = wb_s.encode("utf-8")
    data["xl/_rels/workbook.xml.rels"] = rel_s.encode("utf-8")
    data["[Content_Types].xml"] = ct_s.encode("utf-8")


def _max_worksheet_num(data: dict[str, bytes]) -> int:
    mx = 0
    for name in data:
        m = re.match(r"xl/worksheets/sheet(\d+)\.xml$", name)
        if m:
            mx = max(mx, int(m.group(1)))
    return mx


def _max_r_id(rels_s: str) -> int:
    mx = 0
    for m in re.finditer(r'Id="rId(\d+)"', rels_s):
        mx = max(mx, int(m.group(1)))
    return mx


def _max_sheet_id(wb_s: str) -> int:
    mx = 0
    for m in re.finditer(r'sheetId="(\d+)"', wb_s):
        mx = max(mx, int(m.group(1)))
    return mx


def append_summary_sheets_zip(
    path: Path,
    *,
    df_in: Optional[pd.DataFrame] = None,
    df_out: Optional[pd.DataFrame] = None,
    df_va: Optional[pd.DataFrame] = None,
    n_in: int = 0,
    n_out: int = 0,
    n_va: int = 0,
) -> None:
    path = Path(path).resolve()
    buf = io.BytesIO()
    with zipfile.ZipFile(path, "r") as zin:
        data = {n: zin.read(n) for n in zin.namelist()}

    strip_opc_calc_chain_from_package(data)

    _remove_summary_sheets_from_package(data)

    wb_s = data["xl/workbook.xml"].decode("utf-8")
    rel_s = data["xl/_rels/workbook.xml.rels"].decode("utf-8")
    ct_s = data["[Content_Types].xml"].decode("utf-8")

    sheet_tag = _workbook_sheet_tag_stem(wb_s)
    wn = _max_worksheet_num(data)
    next_rid = _max_r_id(rel_s) + 1
    next_sid = _max_sheet_id(wb_s) + 1

    # Import aggregation functions
    from .summary_sheets import aggregate_inbound, aggregate_outbound, aggregate_va
    from .quickbi_io import COLS_INBOUND, COLS_OUTBOUND, COLS_VA, FINAL_BU_COL

    triple = (
        (SUM_IN, SRC_IN, _HDR_IN, _COL_IN_OUT, n_in, df_in, "in"),
        (SUM_OUT, SRC_OUT, _HDR_OUT, _COL_IN_OUT, n_out, df_out, "out"),
        (SUM_VA, SRC_VA, _HDR_VA, _COL_VA, n_va, df_va, "va"),
    )

    new_rels: list[str] = []
    new_sheets_xml: list[str] = []
    new_ct_overrides: list[str] = []
    rel_el = _element_tag_stem_from_xml(rel_s, "Relationship")
    ov_el = _element_tag_stem_from_xml(ct_s, "Override")
    for title, src, hdr, cols, n, df, kind in triple:
        wn += 1
        part_rel = "worksheets/sheet%d.xml" % wn
        zp = "xl/" + part_rel
        rid = "rId%d" % next_rid
        next_rid += 1
        sid = next_sid
        next_sid += 1

        # Use aggregated data if DataFrame available
        if df is not None and len(df) > 0:
            if kind == "in":
                agg_df = aggregate_inbound(df)
                col_order = [COLS_INBOUND[0], COLS_INBOUND[1], COLS_INBOUND[2],
                             COLS_INBOUND[3], COLS_INBOUND[5], COLS_INBOUND[6],
                             COLS_INBOUND[7], COLS_INBOUND[8], FINAL_BU_COL]
            elif kind == "out":
                agg_df = aggregate_outbound(df)
                col_order = [COLS_OUTBOUND[0], COLS_OUTBOUND[1], COLS_OUTBOUND[2],
                             COLS_OUTBOUND[3], COLS_OUTBOUND[5], COLS_OUTBOUND[6],
                             COLS_OUTBOUND[7], COLS_OUTBOUND[8], FINAL_BU_COL]
            else:
                agg_df = aggregate_va(df)
                col_order = [COLS_VA[0], COLS_VA[2], COLS_VA[1],
                             COLS_VA[7], FINAL_BU_COL]
            data[zp] = _build_worksheet_xml_from_df(
                headers=hdr, agg_df=agg_df, col_order=col_order,
            )
            _LOG.info("%s: %d rows aggregated -> %d rows", title, len(df), len(agg_df))
        else:
            # Fallback: formula references (legacy behavior)
            data[zp] = _build_worksheet_xml(
                headers=hdr, src_cols=cols, src_sheet=src, nrows=n
            )

        new_rels.append(
            '<%s Id="%s" Type="%s" Target="%s"/>'
            % (rel_el, rid, REL_WS, part_rel)
        )
        new_sheets_xml.append(
            '<%s name="%s" sheetId="%d" r:id="%s"/>'
            % (sheet_tag, title, sid, rid)
        )
        new_ct_overrides.append(
            '<%s PartName="/%s" ContentType="%s"/>'
            % (ov_el, zp, WS_CONTENT_TYPE)
        )

    rel_s = _append_before_closing_tag(rel_s, "Relationships", "".join(new_rels))
    wb_s = _append_before_closing_tag(wb_s, "sheets", "".join(new_sheets_xml))
    ct_s = _append_before_closing_tag(ct_s, "Types", "".join(new_ct_overrides))

    data["xl/workbook.xml"] = sanitize_workbook_xml_for_excel(wb_s.encode("utf-8"))
    data["xl/_rels/workbook.xml.rels"] = rel_s.encode("utf-8")
    data["[Content_Types].xml"] = ct_s.encode("utf-8")

    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name in sorted(data.keys()):
            zout.writestr(name, data[name])

    path.write_bytes(buf.getvalue())
    _LOG.info("summary sheets injected via zip (no full workbook parse)")