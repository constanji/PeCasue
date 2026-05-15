"""Strip pivot cache/table parts from xlsx before openpyxl load_workbook (same idea as CitiHKLine)."""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

_RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"

# Bump when scrub logic changes so ensure_scrubbed_xlsx_cached does not reuse stale packages.
_SCRUB_REVISION = "5-excel-workbook-sanitize"


def sanitize_workbook_xml_for_excel(data: bytes) -> bytes:
    """Normalize workbook.xml for Excel/macOS strict validation.

    WPS leaves ``xmlns:dbsheet`` even when unused; some builds treat it as unreadable "workbook props"
    (Chinese repair: 工作簿的工作表属性).

    The ``extLst`` block with ``xcalcf:calcFeatures`` is tightly coupled to how the file was last
    calculated; after we drop pivot parts and zip-rewrite sheets it can still confuse repair.
    Removing the single known calc-features block matches a clean Excel-save pattern.
    """
    s = data.decode("utf-8")
    s = re.sub(
        r'\s+xmlns:dbsheet="http://web\.wps\.cn/et/2021/dbsheet"',
        "",
        s,
        count=1,
    )
    s = re.sub(
        r'<extLst><ext uri="\{B58B0392-4F1F-4190-BB64-5DF3571DCE5F\}"[^>]*>.*?</ext></extLst>',
        "",
        s,
        count=1,
        flags=re.DOTALL,
    )
    return s.encode("utf-8")


def _strip_pivot_caches_workbook_xml(data: bytes) -> bytes:
    """Drop ``<pivotCaches>`` (zip already omits pivot XML), then run ``sanitize_workbook_xml_for_excel``."""
    t = data.decode("utf-8")
    t = re.sub(r"<pivotCaches>.*?</pivotCaches>", "", t, count=1, flags=re.DOTALL)
    return sanitize_workbook_xml_for_excel(t.encode("utf-8"))


def _strip_pivot_selection_from_worksheet(data: bytes) -> bytes:
    """Remove <pivotSelection>…</pivotSelection> after pivot table parts are dropped from the package.

    Otherwise sheetView still references r:id of a removed pivot relationship → Excel repair.
    """
    if b"pivotSelection" not in data:
        return data
    s = data.decode("utf-8")
    s = re.sub(r"<pivotSelection\b[^>]*>.*?</pivotSelection>", "", s, flags=re.DOTALL)
    return s.encode("utf-8")


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


def rels_bytes_without_calc_chain(data: bytes) -> bytes:
    """Remove ``calcChain`` relationship from ``workbook.xml.rels`` (after dropping ``xl/calcChain.xml``)."""
    root = ET.fromstring(data)
    tag_rel = "{%s}Relationship" % _RELS_NS
    drop: list[ET.Element] = []
    for child in root:
        if child.tag != tag_rel:
            continue
        typ = child.get("Type") or ""
        tgt = (child.get("Target") or "").replace("\\", "/")
        if "calcChain" in typ or tgt.rstrip().endswith("calcChain.xml"):
            drop.append(child)
    for c in drop:
        root.remove(c)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def content_types_bytes_without_calc_chain(data: bytes) -> bytes:
    """Remove ``Override`` for ``/xl/calcChain.xml``."""
    root = ET.fromstring(data)
    tag_ov = "{%s}Override" % _CT_NS
    drop: list[ET.Element] = []
    for child in root:
        if child.tag != tag_ov:
            continue
        pn = (child.get("PartName") or "").replace("\\", "/")
        if "/calcChain.xml" in pn:
            drop.append(child)
    for c in drop:
        root.remove(c)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def strip_opc_calc_chain_from_package(data: dict[str, bytes]) -> None:
    """Drop ``xl/calcChain.xml`` and matching OPC rels / content-types (zip path keys)."""
    data.pop("xl/calcChain.xml", None)
    k = "xl/_rels/workbook.xml.rels"
    if k in data:
        data[k] = rels_bytes_without_calc_chain(data[k])
    k2 = "[Content_Types].xml"
    if k2 in data:
        data[k2] = content_types_bytes_without_calc_chain(data[k2])


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


def xlsx_without_pivot_caches_to_temp(src: Path) -> Path:
    """Copy xlsx to a temp file, dropping pivot parts so openpyxl loads faster.

    Large pivotCacheRecords are not needed to read/write cell data. Original ``src`` is not modified.
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
                elif "/worksheets/sheet" in name and name.endswith(".xml"):
                    raw = _strip_pivot_selection_from_worksheet(raw)
                zout.writestr(item, raw)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return tmp_path


def scrub_template_cache_key(template: Path) -> str:
    p = template.resolve()
    st = p.stat()
    ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
    raw = ("%s|%s|%s|%s" % (str(p), st.st_size, ns, _SCRUB_REVISION)).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:40]


def ensure_scrubbed_xlsx_cached(template: Path, cache_dir: Path) -> tuple[Path, bool]:
    """Write or reuse pivot-stripped xlsx. Returns (path, cache_hit)."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = scrub_template_cache_key(template)
    dest = cache_dir / ("%s_scrub.xlsx" % key)
    if dest.is_file():
        return dest, True
    tmp_path = xlsx_without_pivot_caches_to_temp(template)
    try:
        dest.unlink(missing_ok=True)
        tmp_path.replace(dest)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return dest, False
