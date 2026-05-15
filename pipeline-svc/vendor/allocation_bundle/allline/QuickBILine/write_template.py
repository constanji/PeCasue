from __future__ import annotations

import logging
import shutil
import threading
import time
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from .quickbi_io import (
    COLS_INBOUND,
    COLS_OUTBOUND,
    COLS_VA,
    coerce_numeric_for_excel,
)
from .template_pivot_scrub import (
    ensure_scrubbed_xlsx_cached,
    xlsx_without_pivot_caches_to_temp,
)
from .summary_sheets import append_summary_sheets_to_workbook, append_summary_sheets_via_load_save
from .xlsx_zip_fill import fill_workbook_via_zip

_LOG = logging.getLogger("quickbi.line")

SHEET_IN = "\u5165\u91d1"
SHEET_OUT = "\u51fa\u91d1"
SHEET_VA = "VA"


def _write_block(
    ws,
    df: pd.DataFrame,
    columns: list[str],
    numeric_cols: set[str],
    *,
    clear_below: str = "bounded",
    clear_extra_rows: int = 5000,
) -> None:
    ncols = len(columns)
    n = len(df)
    max_r = int(ws.max_row or 2)
    if clear_below != "none":
        if clear_below == "all":
            r_clear_end = max_r
        else:
            r_clear_end = min(max_r, n + 1 + max(0, clear_extra_rows))
        for r in range(2, r_clear_end + 1):
            for c in range(1, ncols + 1):
                ws.cell(row=r, column=c).value = None
    for i, row in enumerate(df.itertuples(index=False, name=None)):
        r = 2 + i
        for c, (name, val) in enumerate(zip(columns, row), start=1):
            if name in numeric_cols:
                val = coerce_numeric_for_excel(val)
            ws.cell(row=r, column=c).value = val


def _load_workbook_tracked(path: Path):
    stop = threading.Event()
    t0 = time.perf_counter()

    def _ping():
        while not stop.wait(30.0):
            _LOG.info(
                "openpyxl load_workbook still running... %.0fs elapsed (big xlsx, not frozen)",
                time.perf_counter() - t0,
            )

    th = threading.Thread(target=_ping, daemon=True)
    th.start()
    try:
        return load_workbook(path, data_only=False, keep_links=False)
    finally:
        stop.set()


def fill_shoufukuan_workbook(
    template: Path,
    out: Path,
    *,
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    df_va: pd.DataFrame,
    clear_below: str = "bounded",
    clear_extra_rows: int = 5000,
    scrub_pivot_caches: bool = True,
    scrub_use_cache: bool = True,
    scrub_cache_dir: Path | None = None,
    write_mode: str = "zip",
    include_summary_sheets: bool = True,
) -> None:
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    template = Path(template).resolve()
    out_abs = out.resolve()
    if write_mode not in ("zip", "openpyxl"):
        raise ValueError("write_mode must be zip or openpyxl")
    ephemeral_scrub: Path | None = None
    if write_mode == "zip":
        if out_abs == template:
            raise ValueError(
                "write_mode=zip would overwrite the template; use --output to another path."
            )
        if scrub_pivot_caches:
            # Scrub template FIRST (before zip rewrite) so the base is clean
            t_scrub = time.perf_counter()
            if scrub_use_cache:
                cdir = scrub_cache_dir if scrub_cache_dir is not None else (out_abs.parent / ".quickbi_tpl_scrub")
                load_path, hit = ensure_scrubbed_xlsx_cached(template, cdir)
                mb = load_path.stat().st_size / (1024 * 1024)
                if hit:
                    _LOG.info("Pivot-strip cache hit %s (~%.0f MB), using as base for zip rewrite", load_path.name, mb)
                else:
                    _LOG.info("Pivot-strip cache written in %.1fs -> %s (~%.0f MB), using as base", time.perf_counter() - t_scrub, load_path.name, mb)
            else:
                ephemeral_scrub = xlsx_without_pivot_caches_to_temp(template)
                load_path = ephemeral_scrub
                _LOG.info("Pivot-strip temp xlsx in %.1fs, using as base for zip rewrite", time.perf_counter() - t_scrub)
        else:
            _LOG.info("write_mode=zip: no pivot scrub, patching original template directly")
            load_path = template
    elif scrub_pivot_caches:
        t_scrub = time.perf_counter()
        if scrub_use_cache:
            cdir = scrub_cache_dir if scrub_cache_dir is not None else (out_abs.parent / ".quickbi_tpl_scrub")
            load_path, hit = ensure_scrubbed_xlsx_cached(template, cdir)
            mb = load_path.stat().st_size / (1024 * 1024)
            nxt = (
                "starting zip sheet rewrite (no full openpyxl parse)..."
                if write_mode == "zip"
                else "starting openpyxl load_workbook..."
            )
            if hit:
                _LOG.info(
                    "Pivot-strip cache hit %s (~%.0f MB), %s",
                    load_path.name,
                    mb,
                    nxt,
                )
            else:
                _LOG.info(
                    "Pivot-strip cache written in %.1fs -> %s (~%.0f MB), %s",
                    time.perf_counter() - t_scrub,
                    load_path.name,
                    mb,
                    nxt,
                )
        else:
            ephemeral_scrub = xlsx_without_pivot_caches_to_temp(template)
            load_path = ephemeral_scrub
            _LOG.info(
                "Pivot-strip temp xlsx in %.1fs (~%.0f MB), %s",
                time.perf_counter() - t_scrub,
                load_path.stat().st_size / (1024 * 1024),
                "starting zip sheet rewrite..."
                if write_mode == "zip"
                else "starting openpyxl load_workbook...",
            )
    else:
        if out_abs != template:
            shutil.copy2(template, out)
            load_path = out_abs
        else:
            load_path = template

    if write_mode == "zip":
        if clear_below != "bounded" or clear_extra_rows != 5000:
            _LOG.warning("write_mode=zip ignores clear-below / clear-extra-rows (sheet is rebuilt from row 3)")
        t_zip = time.perf_counter()
        fill_workbook_via_zip(
            load_path,
            out_abs,
            df_in=df_in,
            df_out=df_out,
            df_va=df_va,
            sheet_in=SHEET_IN,
            sheet_out=SHEET_OUT,
            sheet_va=SHEET_VA,
            cols_in=list(COLS_INBOUND),
            cols_out=list(COLS_OUTBOUND),
            cols_va=list(COLS_VA),
            numeric_in={COLS_INBOUND[7], COLS_INBOUND[8]},
            numeric_out={COLS_OUTBOUND[7], COLS_OUTBOUND[8]},
            numeric_va={COLS_VA[7]},
        )
        _LOG.info(
            "zip rewrite wrote %s in %.1fs",
            out_abs.name,
            time.perf_counter() - t_zip,
        )
        if include_summary_sheets:
            t_sum = time.perf_counter()
            append_summary_sheets_via_load_save(
                out_abs,
                df_in=df_in,
                df_out=df_out,
                df_va=df_va,
                n_in=len(df_in),
                n_out=len(df_out),
                n_va=len(df_va),
            )
            _LOG.info(
                "summary sheets (zip inject, no openpyxl load) in %.1fs",
                time.perf_counter() - t_sum,
            )
        if ephemeral_scrub is not None:
            try:
                ephemeral_scrub.unlink(missing_ok=True)
            except OSError:
                pass
        return

    t_load = time.perf_counter()
    wb = _load_workbook_tracked(load_path)
    _LOG.info("openpyxl load_workbook finished in %.1fs", time.perf_counter() - t_load)
    try:
        if SHEET_IN not in wb.sheetnames:
            raise ValueError("template missing sheet %r" % SHEET_IN)
        if SHEET_OUT not in wb.sheetnames:
            raise ValueError("template missing sheet %r" % SHEET_OUT)
        if SHEET_VA not in wb.sheetnames:
            raise ValueError("template missing sheet %r" % SHEET_VA)
        kw = {"clear_below": clear_below, "clear_extra_rows": clear_extra_rows}
        _write_block(
            wb[SHEET_IN],
            df_in,
            COLS_INBOUND,
            {COLS_INBOUND[7], COLS_INBOUND[8]},
            **kw,
        )
        _write_block(
            wb[SHEET_OUT],
            df_out,
            COLS_OUTBOUND,
            {COLS_OUTBOUND[7], COLS_OUTBOUND[8]},
            **kw,
        )
        _write_block(wb[SHEET_VA], df_va, COLS_VA, {COLS_VA[7]}, **kw)
        if include_summary_sheets:
            append_summary_sheets_to_workbook(
                wb,
                df_in=df_in,
                df_out=df_out,
                df_va=df_va,
                n_in=len(df_in),
                n_out=len(df_out),
                n_va=len(df_va),
            )
        t_save = time.perf_counter()
        wb.save(out_abs)
        _LOG.info(
            "Cell updates + wb.save(%s) finished in %.1fs",
            out_abs.name,
            time.perf_counter() - t_save,
        )
    finally:
        wb.close()
        if ephemeral_scrub is not None:
            try:
                ephemeral_scrub.unlink(missing_ok=True)
            except OSError:
                pass
