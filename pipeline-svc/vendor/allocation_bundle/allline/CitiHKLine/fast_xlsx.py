# -*- coding: utf-8 -*-
"""Limited-row XLSX read via sheet XML (faster than openpyxl for huge files)."""
from __future__ import annotations

import zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd
from openpyxl.utils.cell import column_index_from_string, coordinate_from_string

NS_MAIN = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _iterparse_rows(file_obj, events: tuple[str, ...]):
    """Prefer lxml.iterparse (C) if installed."""
    try:
        from lxml.etree import iterparse as lxml_iterparse
    except ImportError:
        return ET.iterparse(file_obj, events=events)
    return lxml_iterparse(file_obj, events=events, recover=True, huge_tree=True)


class _SharedStringsLazy:
    """Parse sharedStrings.xml only up to the highest index actually referenced.

    Wide exports often have a huge shared string table from columns we never read;
    loading the full list dominates runtime. This streams ``<si>`` in order and
    stops after the maximum index needed by cells we visit.
    """

    __slots__ = ("_acc", "_f", "_it")

    def __init__(self, z: zipfile.ZipFile) -> None:
        self._acc: list[str] = []
        self._f = None
        self._it = None
        p = "xl/sharedStrings.xml"
        if p not in z.namelist():
            return
        self._f = z.open(p)
        self._it = _iterparse_rows(self._f, ("end",))

    def get(self, i: int) -> str:
        if self._it is None:
            raise IndexError("no sharedStrings.xml")
        while i >= len(self._acc):
            for _event, el in self._it:
                if el.tag != NS_MAIN + "si":
                    continue
                parts = [t.text or "" for t in el.findall(".//" + NS_MAIN + "t")]
                self._acc.append("".join(parts))
                el.clear()
                break
            else:
                raise IndexError("shared string index %s out of range" % i)
        return self._acc[i]

    def close(self) -> None:
        if self._f is not None:
            self._f.close()
            self._f = None
            self._it = None


def _shared_resolve(shared: Union[list[str], _SharedStringsLazy], idx: int) -> str:
    if isinstance(shared, list):
        return shared[idx]
    return shared.get(idx)


def _cell_value(cell: ET.Element, shared: Union[list[str], _SharedStringsLazy]) -> Any:
    t = cell.get("t")
    is_el = cell.find(NS_MAIN + "is")
    v_el = cell.find(NS_MAIN + "v")
    if t == "inlineStr" or is_el is not None:
        if is_el is None:
            return None
        return "".join((x.text or "") for x in is_el.findall(".//" + NS_MAIN + "t"))
    if v_el is None or v_el.text is None:
        return None
    v = v_el.text
    if t == "s":
        try:
            return _shared_resolve(shared, int(v))
        except (ValueError, IndexError):
            return v
    return v


def _first_sheet_path(z: zipfile.ZipFile) -> str:
    if "xl/worksheets/sheet1.xml" in z.namelist():
        return "xl/worksheets/sheet1.xml"
    names = sorted(x for x in z.namelist() if x.startswith("xl/worksheets/sheet") and x.endswith(".xml"))
    if not names:
        raise ValueError("no worksheets in xlsx")
    return names[0]


def read_xlsx_rows(path: Path, usecols: list[str], max_data_rows: Optional[int]) -> pd.DataFrame:
    usecols_set = frozenset(usecols)
    with zipfile.ZipFile(path, "r") as z:
        shared = _SharedStringsLazy(z)
        try:
            sp = _first_sheet_path(z)
            with z.open(sp) as f:
                idx_map: dict[str, int] = {}
                needed: frozenset[int] | None = None
                out_rows: list[dict] = []
                data_taken = 0
                for _event, el in _iterparse_rows(f, ("end",)):
                    if el.tag != NS_MAIN + "row":
                        continue
                    cells: dict[int, Any] = {}
                    for c in el:
                        if c.tag != NS_MAIN + "c":
                            continue
                        ref = c.get("r")
                        if not ref:
                            continue
                        col_letter, _ = coordinate_from_string(ref)
                        ci = column_index_from_string(col_letter) - 1
                        if needed is not None and ci not in needed:
                            continue
                        cells[ci] = _cell_value(c, shared)
                    el.clear()
                    if not cells:
                        continue
                    if not idx_map:
                        trial: dict[str, int] = {}
                        for ci, val in cells.items():
                            if val is None:
                                continue
                            name = str(val).strip() if not isinstance(val, str) else val.strip()
                            if name in usecols_set:
                                trial[name] = ci
                        if set(usecols) - set(trial.keys()):
                            continue
                        idx_map = trial
                        needed = frozenset(idx_map.values())
                        continue
                    rec = {col: cells.get(idx_map[col]) for col in usecols}
                    out_rows.append(rec)
                    data_taken += 1
                    if max_data_rows is not None and data_taken >= max_data_rows:
                        break
                if not idx_map:
                    raise ValueError(
                        "%s: no header row with all columns %s" % (path.name, sorted(usecols))
                    )
        finally:
            shared.close()
    return pd.DataFrame(out_rows, columns=usecols)


def _read_xlsx_rows_picklable(args: tuple[str, tuple[str, ...], Optional[int]]) -> pd.DataFrame:
    path_str, usecols_t, nrows = args
    return read_xlsx_rows(Path(path_str), list(usecols_t), nrows)


def read_many_xlsx(
    paths: list[Path],
    usecols: list[str],
    nrows: Optional[int],
    *,
    max_workers: int = 0,
    parallel: str = "process",
) -> pd.DataFrame:
    eff_workers = max_workers
    if eff_workers <= 0:
        eff_workers = min(8, len(paths)) if len(paths) > 1 else 1
    if len(paths) <= 1 or eff_workers <= 1:
        frames = [read_xlsx_rows(p, usecols, nrows) for p in paths]
        return pd.concat(frames, ignore_index=True)

    w = min(eff_workers, len(paths))
    usecols_t = tuple(usecols)
    args_list = [(str(p), usecols_t, nrows) for p in paths]

    if parallel == "thread":
        with ThreadPoolExecutor(max_workers=w) as ex:
            frames = list(
                ex.map(
                    lambda a: read_xlsx_rows(Path(a[0]), list(a[1]), a[2]),
                    args_list,
                )
            )
    else:
        try:
            import multiprocessing as mp

            ctx = mp.get_context("spawn")
        except ValueError:
            ctx = __import__("multiprocessing").get_context("fork")

        with ProcessPoolExecutor(max_workers=w, mp_context=ctx) as ex:
            frames = list(ex.map(_read_xlsx_rows_picklable, args_list))

    return pd.concat(frames, ignore_index=True)
