"""Rewrite only sheetData for target sheets via zip (avoid openpyxl loading 60MB+ sheet XML)."""
from __future__ import annotations

import io
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
from openpyxl.utils.cell import column_index_from_string, coordinate_from_string, get_column_letter

from .quickbi_io import coerce_numeric_for_excel
from .template_pivot_scrub import (
    content_types_bytes_without_calc_chain,
    rels_bytes_without_calc_chain,
    sanitize_workbook_xml_for_excel,
)

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def _workbook_sheet_parts(z: zipfile.ZipFile) -> dict[str, str]:
    out: dict[str, str] = {}
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    r_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    id_to_target: dict[str, str] = {}
    for rel in rels:
        if rel.tag.split("}")[-1] != "Relationship":
            continue
        i, t = rel.get("Id"), rel.get("Target")
        if i and t:
            id_to_target[i] = t
    for sh in wb.findall(".//{%s}sheet" % MAIN):
        nm, rid = sh.get("name"), sh.get(r_ns)
        if not nm or not rid:
            continue
        t = id_to_target.get(rid)
        if not t:
            continue
        t = t.replace("\\", "/")
        part = t.lstrip("/") if t.startswith("/") else ("xl/" + t if not t.startswith("xl/") else t)
        out[nm] = part
    return out


def _snip_row_xml(raw: bytes, row_num: int) -> bytes:
    """Take one <row r=\"n\" ...>...</row> from sheet xml (rows are near top; do not iterparse+clear)."""
    needle = ('<row r="%d"' % row_num).encode("utf-8")
    start = raw.find(needle)
    if start < 0:
        raise ValueError("missing row %d" % row_num)
    end = raw.find(b"</row>", start)
    if end < 0:
        raise ValueError("unclosed row %d" % row_num)
    return raw[start : end + 6]


def _parse_row_fragment(row_xml: bytes) -> ET.Element:
    wrap = (
        b'<root xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        + row_xml
        + b"</root>"
    )
    root = ET.fromstring(wrap)
    return root[0]


def _cell_has_formula(c: ET.Element) -> bool:
    return any(x.tag.split("}")[-1] == "f" for x in list(c))


def _formula_min_col(row2: ET.Element) -> int:
    m: int | None = None
    for c in row2:
        if c.tag.split("}")[-1] != "c":
            continue
        if not _cell_has_formula(c):
            continue
        ref = c.get("r")
        if not ref:
            continue
        letters, _row = coordinate_from_string(ref)
        ci = column_index_from_string(letters)
        m = ci if m is None else min(m, ci)
    if m is None:
        raise ValueError("row 2 has no formula cells")
    return m


def _bump_formula(s: str, new_row: int) -> str:
    """Shift row-2 cell refs in formula text (supports AA2, $A$2, 入金!J2)."""
    if new_row == 2:
        return s
    out = re.sub(
        r"\$([A-Z]{1,3})\$2(?!\d)",
        lambda m: "$%s$%d" % (m.group(1), new_row),
        s,
    )
    out = re.sub(
        r"\b([A-Z]{1,3})2(?!\d)",
        lambda m: m.group(1) + str(new_row),
        out,
    )
    return out


def _extract_formula_cell_templates(r2b: bytes, row2_el: ET.Element, f_min: int) -> list[str]:
    """Original <c>...</c> strings from row 2 (no ET.tostring ns0: mix)."""
    refs: list[tuple[int, str]] = []
    for c in row2_el:
        if c.tag.split("}")[-1] != "c":
            continue
        if not _cell_has_formula(c):
            continue
        ref = c.get("r")
        if not ref:
            continue
        letters, rn = coordinate_from_string(ref)
        if int(rn) != 2:
            continue
        ci = column_index_from_string(letters)
        if ci >= f_min:
            refs.append((ci, ref))
    refs.sort(key=lambda x: x[0])
    r2s = r2b.decode("utf-8")
    out: list[str] = []
    for _ci, ref in refs:
        m = re.search(r"<c r=\"%s\"[^>]*>[\s\S]*?</c>" % re.escape(ref), r2s)
        if not m:
            raise ValueError("missing cell xml for %s in row 2" % ref)
        out.append(m.group(0))
    return out


def _bump_cloned_cell_xml(cell_xml: str, excel_row: int) -> str:
    u = re.sub(
        r"r=\"([A-Z]{1,3})2\"",
        lambda m: 'r="%s%d"' % (m.group(1), excel_row),
        cell_xml,
        count=1,
    )

    def _bump_f(m: re.Match[str]) -> str:
        open_tag = re.sub(
            r"ref=\"([A-Z]{1,3})2\"",
            lambda m2: 'ref="%s%d"' % (m2.group(1), excel_row),
            m.group(1),
            count=1,
        )
        return open_tag + _bump_formula(m.group(2), excel_row) + m.group(3)

    out = re.sub(r"(<f[^>]*>)([\s\S]*?)(</f>)", _bump_f, u)
    return _strip_formula_cell_cached_result(out)


def _strip_formula_cell_cached_result(cell_xml: str) -> str:
    """Remove cached <v> / result t= so Excel recomputes (stale #N/A from row-2 template)."""
    if "<f" not in cell_xml:
        return cell_xml
    s = re.sub(r"<v[^>]*>[\s\S]*?</v>", "", cell_xml)
    for tok in (' t="e"', ' t="str"', ' t="n"', ' t="b"', ' t="d"'):
        s = s.replace(tok, "")
    return s


def _cell_a1_ref(col_idx: int, row_idx: int) -> str:
    return "%s%d" % (get_column_letter(col_idx), row_idx)


def _escape_xml_text(t: str) -> str:
    return (
        t.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _num_cell_v(v: object) -> str:
    if isinstance(v, float):
        return "%.15g" % v
    return str(v)


def _src_cell_xml(col_idx: int, excel_row: int, val: object, numeric_cols: set[str], col_name: str) -> str:
    ref = _cell_a1_ref(col_idx, excel_row)
    if col_name in numeric_cols:
        v = coerce_numeric_for_excel(val)
        if v is None or v == "":
            return '<c r="%s"/>' % ref
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return '<c r="%s"><v>%s</v></c>' % (ref, _num_cell_v(v))
        # Source exports occasionally put text/date-like values in numeric columns.
        # Writing them as numeric <v> makes the worksheet XML invalid for Excel/openpyxl.
        s = _escape_xml_text(str(v))
        return '<c r="%s" t="inlineStr"><is><t>%s</t></is></c>' % (ref, s)
    if val is None or val == "":
        return '<c r="%s"/>' % ref
    s = _escape_xml_text(str(val))
    return '<c r="%s" t="inlineStr"><is><t>%s</t></is></c>' % (ref, s)


def _row_inner_xml(
    excel_row: int,
    df_row: pd.Series,
    columns: list[str],
    numeric_cols: set[str],
    formula_min_col: int,
    tmpl_cell_xml_strs: list[str],
    row2_spans: str | None,
) -> str:
    parts: list[str] = []
    ra = ['<row r="%d"' % excel_row]
    if row2_spans:
        ra.append(' spans="%s"' % row2_spans)
    ra.append(">")
    parts.append("".join(ra))
    for j, name in enumerate(columns, start=1):
        if j >= formula_min_col:
            break
        parts.append(_src_cell_xml(j, excel_row, df_row.get(name), numeric_cols, name))
    for cell_xml in tmpl_cell_xml_strs:
        parts.append(_bump_cloned_cell_xml(cell_xml, excel_row))
    parts.append("</row>")
    return "".join(parts)


def _new_a1_colrow_ref(before: bytes, last_row: int) -> bytes:
    """Parse dimension like ref=\"A1:W42180\" -> b\"A1:W{last_row}\"."""
    m = re.search(br'<dimension ref="([^"]+)"', before)
    if not m:
        return b"A1:A%d" % last_row
    ref = m.group(1)
    if b":" not in ref:
        return b"A1:A%d" % last_row
    _, br = ref.split(b":", 1)
    mm = re.match(rb"([A-Z]{1,3})(\d+)\s*$", br)
    if not mm:
        return b"A1:A%d" % last_row
    col = mm.group(1)
    return b"A1:%s%d" % (col, last_row)


def _cap_sheetview_rows(blob: bytes, last_row: int) -> bytes:
    """activeCell / sqref pointing past last_row confuse Excel."""
    out = re.sub(
        br'activeCell="([A-Z]{1,3})(\d+)"',
        lambda m: b'activeCell="%s%d"'
        % (m.group(1), min(int(m.group(2)), last_row)),
        blob,
    )
    out = re.sub(
        br'sqref="([A-Z]{1,3})(\d+)"',
        lambda m: b'sqref="%s%d"' % (m.group(1), min(int(m.group(2)), last_row)),
        out,
    )
    return out


def _dim_ref_string_from_raw_and_n(raw: bytes, n_data_rows: int) -> str:
    """Same A1:ColRow as _patch_one_sheet uses (for workbook definedNames)."""
    i = raw.find(b"<sheetData>")
    if i < 0:
        raise ValueError("missing <sheetData>")
    before = raw[:i]
    last_row = max(2, n_data_rows + 1)
    return _new_a1_colrow_ref(before, last_row).decode("ascii")


def _range_abs_from_a1(ref: str) -> str:
    """A1:W16495 -> $A$1:$W$16495 (Excel definedName style)."""
    tl, br = ref.split(":", 1)

    def one(c: str) -> str:
        m = re.match(r"([A-Z]{1,3})(\d+)$", c)
        if not m:
            raise ValueError("bad cell %r" % c)
        return "$%s$%s" % (m.group(1), m.group(2))

    return "%s:%s" % (one(tl), one(br))


def _patch_workbook_filter_names(wb_xml: bytes, sheet_to_a1: dict[str, str]) -> bytes:
    """Update hidden _xlnm._FilterDatabase ranges so they match sheet autoFilter."""
    s = wb_xml.decode("utf-8")
    for sh, a1 in sheet_to_a1.items():
        if not a1:
            continue
        abs_rng = _range_abs_from_a1(a1)
        pat = r"%s!\$A\$1:\$[A-Z]{1,3}\$\d+" % re.escape(sh)
        repl = "%s!%s" % (sh, abs_rng)
        s = re.sub(pat, repl, s)
    return s.encode("utf-8")


def _patch_workbook_calc_full_on_load(wb_xml: bytes) -> bytes:
    """Previously injected ``fullCalcOnLoad`` into ``<calcPr>``.

    Doing so reliably triggers Excel (notably Excel for Mac / cn locale) recovery that removes
    "工作簿的工作表属性" from workbook.xml while leaving the workbook usable.

    Omit the patch — opening the file triggers normal recalculation for updated cells anyway when
    the user enables automatic calculation.

    Keeping this stub so callers can re-enable experimentation later.
    """
    return wb_xml


def _patch_one_sheet(
    raw: bytes,
    df: pd.DataFrame,
    columns: list[str],
    numeric_cols: set[str],
) -> bytes:
    sd_o, sd_c = b"<sheetData>", b"</sheetData>"
    i = raw.find(sd_o)
    if i < 0:
        raise ValueError("missing <sheetData>")
    inner0 = i + len(sd_o)
    j = raw.find(sd_c, inner0)
    if j < 0:
        raise ValueError("missing </sheetData>")
    before = raw[:i]
    after = raw[j + len(sd_c) :]

    r1b = _snip_row_xml(raw, 1)
    r2b = _snip_row_xml(raw, 2)
    row1_xml = r1b.decode("utf-8")
    row2_el = _parse_row_fragment(r2b)
    f_min = _formula_min_col(row2_el)
    tmpl_cell_xml_strs = _extract_formula_cell_templates(r2b, row2_el, f_min)
    row2_spans = row2_el.get("spans")

    n = len(df)
    last_row = max(2, n + 1)
    new_ref = _new_a1_colrow_ref(before, last_row)
    before2 = re.sub(br'<dimension ref="[^"]+"', b'<dimension ref="%s"' % new_ref, before, count=1)
    before2 = _cap_sheetview_rows(before2, last_row)
    after2 = re.sub(br'<autoFilter ref="[^"]+"', b'<autoFilter ref="%s"' % new_ref, after, count=1)

    inner_parts = [row1_xml]
    for idx in range(n):
        excel_row = 2 + idx
        inner_parts.append(
            _row_inner_xml(
                excel_row,
                df.iloc[idx],
                columns,
                numeric_cols,
                f_min,
                tmpl_cell_xml_strs,
                row2_spans,
            )
        )
    new_inner = "".join(inner_parts).encode("utf-8")
    return before2 + b"<sheetData>" + new_inner + b"</sheetData>" + after2


def fill_workbook_via_zip(
    base_xlsx: Path,
    out_xlsx: Path,
    *,
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    df_va: pd.DataFrame,
    sheet_in: str,
    sheet_out: str,
    sheet_va: str,
    cols_in: list[str],
    cols_out: list[str],
    cols_va: list[str],
    numeric_in: set[str],
    numeric_out: set[str],
    numeric_va: set[str],
) -> None:
    base_xlsx = Path(base_xlsx).resolve()
    out_xlsx = Path(out_xlsx).resolve()
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)

    patches: dict[str, tuple[pd.DataFrame, list[str], set[str]]] = {
        sheet_in: (df_in, cols_in, numeric_in),
        sheet_out: (df_out, cols_out, numeric_out),
        sheet_va: (df_va, cols_va, numeric_va),
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(base_xlsx, "r") as zin, zipfile.ZipFile(
        buf, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        parts = _workbook_sheet_parts(zin)
        sheet_to_dim: dict[str, str] = {}
        for sname, (df, _, _) in patches.items():
            part = parts.get(sname)
            if not part:
                raise KeyError("sheet not in workbook: %r" % sname)
            raw0 = zin.read(part)
            sheet_to_dim[sname] = _dim_ref_string_from_raw_and_n(raw0, len(df))
        for info in zin.infolist():
            name = info.filename
            data = zin.read(name)
            hit = None
            for sname, (df, cols, nums) in patches.items():
                if parts.get(sname) == name:
                    hit = (df, cols, nums)
                    break
            if hit is not None:
                df, cols, nums = hit
                data = _patch_one_sheet(data, df, cols, nums)
            elif name == "xl/calcChain.xml":
                continue
            elif name == "xl/_rels/workbook.xml.rels":
                data = rels_bytes_without_calc_chain(data)
            elif name == "[Content_Types].xml":
                data = content_types_bytes_without_calc_chain(data)
            elif name == "xl/workbook.xml":
                data = _patch_workbook_filter_names(data, sheet_to_dim)
                data = _patch_workbook_calc_full_on_load(data)
                data = sanitize_workbook_xml_for_excel(data)
            zout.writestr(info, data)
    out_xlsx.write_bytes(buf.getvalue())
