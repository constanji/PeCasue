#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Step1: CITIHK xlsx -> same-name csv (usecols only for build / stream)."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Optional

if __package__:
    from .citihk_core import (
        DEFAULT_CITIHK_DIR,
        INBOUND_USECOLS,
        OUTBOUND_USECOLS,
        resolve_slip_xlsx,
    )
else:
    from citihk_core import (
        DEFAULT_CITIHK_DIR,
        INBOUND_USECOLS,
        OUTBOUND_USECOLS,
        resolve_slip_xlsx,
    )


def _inbound_xlsx_paths(d: Path) -> list[Path]:
    paths = sorted(p for p in d.glob("2-Inbound*.xlsx") if not p.name.startswith("~$"))
    if len(paths) != 3:
        raise ValueError(
            "expected 3 xlsx 2-Inbound*.xlsx, found %s" % [p.name for p in paths]
        )
    return paths


def _outbound_xlsx_paths(d: Path) -> tuple[Path, Path]:
    a = d / "4outbound.xlsx"
    b = resolve_slip_xlsx(d)
    if not a.is_file() or b is None:
        raise FileNotFoundError(
            "need 4outbound.xlsx and (资金流slip.xlsx or 4资金流slip.xlsx)"
        )
    return a, b


def _xlsx_to_csv_one(path: Path, usecols: list[str], *, nrows: Optional[int]) -> Path:
    from fast_xlsx import read_xlsx_rows

    out = path.with_suffix(".csv")
    df = read_xlsx_rows(path, list(usecols), nrows)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    return out


Progress = Callable[[str], None] | None


def convert_citihk_dir_to_csv(
    d: str | Path,
    *,
    nrows: Optional[int] = None,
    overwrite: bool = False,
    progress: Progress = None,
) -> list[Path]:
    """Convert CitiHK xlsx exports under ``d`` to paired csv (same basename)."""
    dpath = Path(d).expanduser().resolve()
    converted: list[Path] = []
    for p in _inbound_xlsx_paths(dpath):
        out = p.with_suffix(".csv")
        if out.is_file() and not overwrite:
            if progress:
                progress("入金跳过（CSV 已存在）: %s" % p.name)
            continue
        if progress:
            progress("入金 xlsx -> csv: %s" % p.name)
        converted.append(_xlsx_to_csv_one(p, INBOUND_USECOLS, nrows=nrows))
    ox, oy = _outbound_xlsx_paths(dpath)
    for label, p in (("出金主表", ox), ("资金流 slip", oy)):
        out = p.with_suffix(".csv")
        if out.is_file() and not overwrite:
            if progress:
                progress("%s 跳过（CSV 已存在）: %s" % (label, p.name))
            continue
        if progress:
            progress("%s xlsx -> csv: %s" % (label, p.name))
        converted.append(_xlsx_to_csv_one(p, OUTBOUND_USECOLS, nrows=nrows))
    return converted


def main(argv: Optional[list[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="CITIHK xlsx -> csv (usecols only)")
    ap.add_argument("--citihk-dir", type=Path, default=DEFAULT_CITIHK_DIR)
    ap.add_argument("--nrows", type=int, default=None)
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="覆盖已存在的同名 CSV",
    )
    args = ap.parse_args(argv)

    d = args.citihk_dir.expanduser().resolve()

    def _pr(msg: str) -> None:
        print(msg)

    for out in convert_citihk_dir_to_csv(
        d, nrows=args.nrows, overwrite=args.overwrite, progress=_pr
    ):
        print("written -> %s" % out)


if __name__ == "__main__":
    main()
