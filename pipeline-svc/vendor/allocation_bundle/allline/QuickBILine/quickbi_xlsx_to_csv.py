#!/usr/bin/env python3
"""QuickBI wide xlsx -> utf-8-sig csv with required columns only (streaming read, like CitiHKLine)."""
from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from typing import Any, Callable

from .paths import DEFAULT_QUICKBI_INBOUND, DEFAULT_QUICKBI_OUTBOUND, DEFAULT_QUICKBI_VA
from .quickbi_io import COLS_INBOUND_BASE, COLS_OUTBOUND_BASE, COLS_VA


def _load_read_xlsx_rows() -> Callable[..., Any]:
    root = Path(__file__).resolve().parent.parent / "CitiHKLine" / "fast_xlsx.py"
    if not root.is_file():
        raise FileNotFoundError("need %s (CitiHKLine fast_xlsx)" % root)
    spec = importlib.util.spec_from_file_location("citihk_fast_xlsx", root)
    if spec is None or spec.loader is None:
        raise ImportError("cannot load fast_xlsx from %s" % root)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.read_xlsx_rows


def _one(path: Path, cols: list[str], nrows: int | None, read_xlsx_rows: Callable[..., Any]) -> Path:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    df = read_xlsx_rows(path, list(cols), nrows)
    out = path.with_suffix(".csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="QuickBI xlsx -> csv (required columns only)")
    ap.add_argument("--inbound", type=Path, default=DEFAULT_QUICKBI_INBOUND)
    ap.add_argument("--outbound", type=Path, default=DEFAULT_QUICKBI_OUTBOUND)
    ap.add_argument("--va", type=Path, default=DEFAULT_QUICKBI_VA)
    ap.add_argument("--nrows", type=int, default=None, help="limit rows per file (debug)")
    args = ap.parse_args(argv)

    read_xlsx_rows = _load_read_xlsx_rows()
    pairs = [
        (args.inbound, COLS_INBOUND_BASE),
        (args.outbound, COLS_OUTBOUND_BASE),
        (args.va, COLS_VA),
    ]
    for p, cols in pairs:
        o = _one(p, cols, args.nrows, read_xlsx_rows)
        print("%s -> %s" % (p.name, o))


if __name__ == "__main__":
    main()
