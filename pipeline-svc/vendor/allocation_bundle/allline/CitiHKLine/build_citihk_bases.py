#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI: CITIHK PPHK bases -> default output path（PeCause 由任务 runs 目录指定）。"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from citihk_core import (
    C_IN_CT,
    C_OUT_CT,
    DEFAULT_CITIHK_DIR,
    DEFAULT_MAPPING,
    DEFAULT_OUT,
    DEFAULT_TEMPLATE,
    default_inbound_paths,
    default_outbound_paths,
    load_account_mapping,
    process_inbound,
    process_outbound,
    write_workbook,
)


def main(argv: Optional[list[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="CITIHK PPHK bases")
    ap.add_argument("--citihk-dir", type=Path, default=DEFAULT_CITIHK_DIR)
    ap.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    ap.add_argument("--no-template-copy", action="store_true")
    ap.add_argument("--nrows", type=int, default=None)
    ap.add_argument(
        "--workers",
        type=int,
        default=0,
        help="多文件并行：xlsx 多文件读；csv 流式时每文件一线程。0=自动(最多8)；1=串行",
    )
    ap.add_argument(
        "--parallel",
        choices=["process", "thread"],
        default="thread",
        help="并行方式：thread(默认) 或 process",
    )
    ap.add_argument(
        "--with-details",
        action="store_true",
        help="额外写入基数/行级明细与剔除 sheet（默认只写入金笔数、出金笔数）",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG 级别日志",
    )
    ap.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="仅 WARNING 及以上",
    )
    ap.add_argument(
        "--report-month",
        type=str,
        default=None,
        metavar="YYYYMM",
        help="报表月份：覆盖源表日期列推出的「月份」显示（写入 PECAUSE_CITIHK_REPORT_MONTH）",
    )
    args = ap.parse_args(argv)

    if args.report_month:
        import os

        os.environ["PECAUSE_CITIHK_REPORT_MONTH"] = str(args.report_month).strip()

    if args.quiet and args.verbose:
        ap.error("不能同时使用 --quiet 与 --verbose")
    log_level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("citihk.build")

    t_all = time.perf_counter()
    summary_only = not args.with_details
    citihk = args.citihk_dir.expanduser().resolve()
    mapping_path = args.mapping.expanduser().resolve()
    out_path = args.out.expanduser().resolve()

    log.info(
        "启动 | CITIHK=%s | 输出=%s | summary_only=%s | nrows=%s | workers=%s | parallel=%s",
        citihk,
        out_path,
        summary_only,
        args.nrows,
        args.workers,
        args.parallel,
    )
    log.info(
        "模板=%s (%s)",
        args.template.expanduser().resolve(),
        "跳过" if args.no_template_copy else "复制并填充",
    )

    t0 = time.perf_counter()
    mp = load_account_mapping(mapping_path)
    log.info("已加载 mapping: %s 行，耗时 %.2fs", len(mp), time.perf_counter() - t0)

    in_paths = default_inbound_paths(citihk)
    out_paths = default_outbound_paths(citihk)
    in_kind = "csv" if in_paths and in_paths[0].suffix.lower() == ".csv" else "xlsx"
    log.info(
        "入金源 %s 个 (%s): %s",
        len(in_paths),
        in_kind,
        ", ".join(p.name for p in in_paths),
    )
    log.info(
        "出金源 %s 个 (%s): %s",
        len(out_paths),
        "csv" if out_paths and out_paths[0].suffix.lower() == ".csv" else "xlsx",
        ", ".join(p.name for p in out_paths),
    )
    if in_kind == "xlsx" or (out_paths and out_paths[0].suffix.lower() == ".xlsx"):
        log.warning(
            "当前仍在使用 xlsx 慢路径；这一步会先解析超大 xlsx，再进 pandas。"
            " 若要启用 CSV 流式聚合，请先运行 xlsx_to_csv.py 生成同名 csv。"
        )

    t0 = time.perf_counter()
    log.info("处理入金…")
    in_sum, in_lines, d1 = process_inbound(
        in_paths,
        mp,
        nrows=args.nrows,
        max_workers=args.workers,
        parallel=args.parallel,
        summary_only=summary_only,
    )
    in_ct = (
        int(pd.to_numeric(in_sum[C_IN_CT], errors="coerce").fillna(0).sum())
        if C_IN_CT in in_sum.columns and not in_sum.empty
        else 0
    )
    log.info(
        "入金完成: 汇总行=%s（唯一 月份+最终BU+主体账号），入金笔数合计=%s，耗时 %.2fs",
        len(in_sum),
        in_ct,
        time.perf_counter() - t0,
    )

    t0 = time.perf_counter()
    log.info("处理出金…")
    out_sum, out_lines, d2 = process_outbound(
        out_paths,
        mp,
        nrows=args.nrows,
        max_workers=args.workers,
        parallel=args.parallel,
        summary_only=summary_only,
    )
    out_ct = (
        int(pd.to_numeric(out_sum[C_OUT_CT], errors="coerce").fillna(0).sum())
        if C_OUT_CT in out_sum.columns and not out_sum.empty
        else 0
    )
    log.info(
        "出金完成: 汇总行=%s（唯一 月份+最终BU+主体账号），出金笔数合计=%s，耗时 %.2fs",
        len(out_sum),
        out_ct,
        time.perf_counter() - t0,
    )

    dropped = pd.DataFrame() if summary_only else pd.concat([d1, d2], ignore_index=True)

    tmpl = None if args.no_template_copy else args.template.expanduser().resolve()
    t0 = time.perf_counter()
    log.info("写入工作簿…")
    write_workbook(
        out_path,
        template_path=tmpl,
        in_summary=in_sum,
        out_summary=out_sum,
        in_lines=in_lines,
        out_lines=out_lines,
        dropped=dropped,
        summary_only=summary_only,
    )
    log.info("写入完成，耗时 %.2fs", time.perf_counter() - t0)

    if summary_only:
        msg = "done: in %s out %s -> %s" % (len(in_sum), len(out_sum), out_path)
    else:
        msg = "done: in %s out %s dropped %s -> %s" % (
            len(in_sum),
            len(out_sum),
            len(dropped),
            out_path,
        )
    log.info("全部完成，总耗时 %.2fs | %s", time.perf_counter() - t_all, msg)
    print(msg)


if __name__ == "__main__":
    main()
