"""Streamlit-oriented helpers for CitiHK xlsx->csv and build pipeline."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Callable

from CitiHKLine.xlsx_to_csv import convert_citihk_dir_to_csv

Progress = Callable[[str], None] | None

LINE_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CITIHK_OUTPUT_ROOT = LINE_ROOT / "files" / "default_citihk_runs"
_CITIHK_BUILD = LINE_ROOT / "CitiHKLine" / "build_citihk_bases.py"


def run_citihk_build_subprocess(
    *,
    citihk_dir: Path,
    mapping: Path,
    out: Path,
    template: Path,
    with_details: bool = False,
    workers: int = 0,
    progress: Progress = None,
) -> tuple[int, str]:
    """Run ``build_citihk_bases.py`` and stream merged stdout/stderr lines to ``progress``."""
    cmd = [
        sys.executable,
        "-u",
        str(_CITIHK_BUILD),
        "--citihk-dir",
        str(citihk_dir.expanduser().resolve()),
        "--mapping",
        str(mapping.expanduser().resolve()),
        "--out",
        str(out.expanduser().resolve()),
        "--template",
        str(template.expanduser().resolve()),
        "--workers",
        str(workers),
    ]
    if with_details:
        cmd.append("--with-details")
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        cwd=str(LINE_ROOT / "CitiHKLine"),
    )
    buf: list[str] = []
    if proc.stdout:
        for line in proc.stdout:
            line = line.rstrip()
            buf.append(line)
            if progress:
                progress(line)
    code = proc.wait()
    return code, "\n".join(buf)
