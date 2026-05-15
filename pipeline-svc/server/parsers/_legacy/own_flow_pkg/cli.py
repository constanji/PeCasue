"""自有流水：规则筛选 → 标准列输出。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_APP = Path(__file__).resolve().parent.parent
if str(_APP) not in sys.path:
    sys.path.insert(0, str(_APP))
from .runtime_paths import new_run_id, ownflow_run_dir, project_root

from .pipeline import pipeline_execution_lock, run_pipeline


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="自有流水：全量汇总输出目标列（处理表仅用于命中时填备注/主体）")
    parser.add_argument(
        "input_dir",
        type=Path,
        nargs="?",
        default=None,
        help="2026.02自有流水 目录",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="默认 <midout_dir>/<run_id>/own_bank_statement_matched.xlsx（同账单 run_id 命名）",
    )
    parser.add_argument(
        "--period",
        type=str,
        default="202602",
        help="入账期间 YYYYMM",
    )
    args = parser.parse_args(argv)

    root = project_root()
    default_in = root / "202602" / "2026.02自有流水"
    input_dir = (args.input_dir or default_in).expanduser().resolve()
    out = args.output or (ownflow_run_dir(new_run_id()) / "own_bank_statement_matched.xlsx")
    out = out.expanduser().resolve()

    with pipeline_execution_lock:
        df = run_pipeline(input_dir, args.period)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(out, index=False, engine="openpyxl")

    print(f"输入: {input_dir}")
    print(f"行数: {len(df)}，输出: {out}")


if __name__ == "__main__":
    main()
