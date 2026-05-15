#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

_PROJECT = Path(__file__).resolve().parent.parent.parent.parent.parent  # 项目根目录（22222222）

from .channel_mapping import apply_channel_name_mapping
from .filters import (
    filter_inbound_outbound_all_months,
    filter_va_all_months,
    pop_inbound_channel_preserve_mask,
    reclassify_ppus_citi_us_ach_debit_outbound_to_inbound,
)
from .final_bu import enrich_final_bu_from_template
from .paths import (
    DEFAULT_OUTPUT_QUICKBI,
    DEFAULT_QUICKBI_INBOUND,
    DEFAULT_QUICKBI_OUTBOUND,
    DEFAULT_QUICKBI_VA,
    DEFAULT_SHOUFUKUAN_TEMPLATE,
)
from .quickbi_io import read_quickbi_inbound, read_quickbi_outbound, read_quickbi_va
from .quickbi_io import FINAL_BU_COL
from .va_branch import filter_va_branches
from .summary_sheets import write_external_narrow_workbook
from .write_template import fill_shoufukuan_workbook


def _resolved(p: Path) -> Path:
    return p.expanduser().resolve()


def _pick_source(p: Path, prefer_csv: bool) -> Path:
    p = _resolved(p)
    if prefer_csv:
        c = p.with_suffix(".csv")
        if c.is_file():
            return c
    return p


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description="QuickBI 1-2 inbound/outbound/va xlsx -> shoufukuan workbook",
    )
    ap.add_argument(
        "--month",
        required=False,
        help="Legacy option; QuickBI exports are now kept across all source months.",
    )
    ap.add_argument("--in-xlsx", type=Path, default=DEFAULT_QUICKBI_INBOUND)
    ap.add_argument("--out-xlsx", type=Path, default=DEFAULT_QUICKBI_OUTBOUND)
    ap.add_argument("--va-xlsx", type=Path, default=DEFAULT_QUICKBI_VA)
    ap.add_argument("--template", type=Path, default=DEFAULT_SHOUFUKUAN_TEMPLATE)
    ap.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_QUICKBI,
        help="Default: files/default_quickbi_out 下 QuickBI out xlsx（模板副本）",
    )
    ap.add_argument(
        "--external-narrow",
        action="store_true",
        help="Also write <output stem>_对外表.xlsx (cross-workbook links; main file already has 入金汇总/出金汇总/VA汇总)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Only load/filter and log row counts")
    ap.add_argument(
        "--clear-below",
        choices=("bounded", "all", "none"),
        default="bounded",
        help="Clear old A:K (or A:H) cells: bounded=n+clear_extra_rows; all=max_row; none=overwrite only",
    )
    ap.add_argument(
        "--clear-extra-rows",
        type=int,
        default=5000,
        help="With bounded: clear through row n+1+this (default 5000)",
    )
    ap.add_argument(
        "--prefer-csv",
        action="store_true",
        help="If xlsx path with same basename .csv exists, read csv (faster after quickbi_xlsx_to_csv)",
    )
    ap.add_argument(
        "--no-scrub-pivots",
        action="store_true",
        help="Load template as-is (slow if huge pivotCacheRecords); default strips pivot parts like CitiHKLine",
    )
    ap.add_argument(
        "--no-scrub-cache",
        action="store_true",
        help="Do not reuse files/quickbi/.quickbi_tpl_scrub（always rewrite pivot-stripped file to temp）",
    )
    ap.add_argument(
        "--write-mode",
        choices=("zip", "openpyxl"),
        default="zip",
        help="zip=rewrite only 3 sheet XML parts (fast); openpyxl=load full workbook (slow on huge templates)",
    )
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("quickbi.line")

    t0 = time.perf_counter()
    log.info("Reading QuickBI exports...")
    in_src = _pick_source(args.in_xlsx, args.prefer_csv)
    out_src = _pick_source(args.out_xlsx, args.prefer_csv)
    va_src = _pick_source(args.va_xlsx, args.prefer_csv)
    if args.prefer_csv:
        for label, p0, p1 in (
            ("inbound", args.in_xlsx, in_src),
            ("outbound", args.out_xlsx, out_src),
            ("va", args.va_xlsx, va_src),
        ):
            if _resolved(p0) != p1 and p1.suffix.lower() == ".csv":
                log.info("%s: using %s", label, p1.name)
    d_in = read_quickbi_inbound(in_src)
    d_out = read_quickbi_outbound(out_src)
    d_va = read_quickbi_va(va_src)
    log.info("Raw rows in=%s out=%s va=%s", len(d_in), len(d_out), len(d_va))

    d_in = filter_inbound_outbound_all_months(d_in)
    d_out = filter_inbound_outbound_all_months(d_out)
    d_in, d_out = reclassify_ppus_citi_us_ach_debit_outbound_to_inbound(d_in, d_out)
    d_va = filter_va_all_months(d_va)
    d_va = filter_va_branches(d_va)
    tpl = args.template.expanduser().resolve()
    log.info("Applying channel name mapping from template...")
    d_in, _in_preserve = pop_inbound_channel_preserve_mask(d_in)
    d_in = apply_channel_name_mapping(
        d_in,
        template=tpl,
        is_outbound=False,
        mapping_workbook=_PROJECT / "成本分摊基数+输出模板(2).xlsx",
        preserve_channel_mask=_in_preserve,
    )
    d_out = apply_channel_name_mapping(d_out, template=tpl, is_outbound=True, mapping_workbook=_PROJECT / "成本分摊基数+输出模板(2).xlsx")
    log.info("Filling final BU from template mapping...")
    d_in, d_out, d_va = enrich_final_bu_from_template(
        template=tpl,
        df_in=d_in,
        df_out=d_out,
        df_va=d_va,
    )
    log.info(
        "Final BU nonblank in=%s out=%s va=%s",
        int((d_in[FINAL_BU_COL].astype(str).str.strip() != "").sum()),
        int((d_out[FINAL_BU_COL].astype(str).str.strip() != "").sum()),
        int((d_va[FINAL_BU_COL].astype(str).str.strip() != "").sum()),
    )
    log.info(
        "After rules in=%s out=%s va=%s (%.2fs)",
        len(d_in),
        len(d_out),
        len(d_va),
        time.perf_counter() - t0,
    )
    if len(d_in) == 0 and len(d_out) == 0 and len(d_va) == 0:
        log.warning(
            "All three are empty after filters. Output will look like a blank template copy.",
        )
    elif len(d_in) == 0 or len(d_out) == 0 or len(d_va) == 0:
        log.warning(
            "Some sheets empty after filters: in=%s out=%s va=%s",
            len(d_in),
            len(d_out),
            len(d_va),
        )

    if args.dry_run:
        print(
            "dry-run: in=%s out=%s va=%s" % (len(d_in), len(d_out), len(d_va)),
            file=sys.stderr,
        )
        return

    out_path = args.output.expanduser().resolve()

    if args.write_mode == "zip":
        log.info("Writing via zip sheet patch (default fast path), base template %s", tpl.name)
    elif args.no_scrub_pivots:
        log.info("Loading template %s (full xlsx, may be slow)...", tpl.name)
    else:
        log.info(
            "Loading template %s (stripping pivot caches for openpyxl, like CitiHKLine)...",
            tpl.name,
        )
    t1 = time.perf_counter()
    fill_shoufukuan_workbook(
        tpl,
        out_path,
        df_in=d_in,
        df_out=d_out,
        df_va=d_va,
        clear_below=args.clear_below,
        clear_extra_rows=args.clear_extra_rows,
        scrub_pivot_caches=not args.no_scrub_pivots,
        scrub_use_cache=not args.no_scrub_cache,
        write_mode=args.write_mode,
    )
    log.info("Wrote %s in %.2fs", out_path, time.perf_counter() - t1)
    if args.external_narrow:
        narrow_path = out_path.with_name(out_path.stem + "_\u5bf9\u5916\u8868.xlsx")
        t2 = time.perf_counter()
        write_external_narrow_workbook(
            narrow_path,
            main_book_filename=out_path.name,
            n_in=len(d_in),
            n_out=len(d_out),
            n_va=len(d_va),
        )
        log.info(
            "External narrow %s (references [%s] in same folder) in %.2fs",
            narrow_path.name,
            out_path.name,
            time.perf_counter() - t2,
        )
    print(
        "done -> %s | rows in=%s out=%s va=%s%s"
        % (
            out_path,
            len(d_in),
            len(d_out),
            len(d_va),
            "" if not args.external_narrow else " | external-narrow -> %s" % narrow_path,
        )
    )


if __name__ == "__main__":
    main()
