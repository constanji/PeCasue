"""自有流水独立运行时的路径解析。

Pipeline-svc 将 ``own_flow_pkg`` 置于 ``server/parsers/_legacy/``。enrich 所需汇率与 mapping 表统一放在
``PIPELINE_DATA_DIR/rules/files/fx`` 与 ``…/mapping``（见 ``template_enrich``），不再解析「整本模版」xlsx。
可通过环境变量 ``OWN_FLOW_PROJECT_ROOT`` / ``OWN_FLOW_MIDOUT`` 覆盖目录。
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path


def own_flow_app_root() -> Path:
    """Package root；内置模版见 ``files/*.xlsx``（仅数据目录无模版时回退）。

    Patched for pipeline-svc: in the original layout this was the parent of
    the ``own_flow`` package (because ``__file__`` lived at
    ``own_flow/own_flow/runtime_paths.py``). In our vendored layout the file
    lives one level deeper at
    ``server/parsers/_legacy/own_flow_pkg/runtime_paths.py`` and the
    accompanying ``files/`` directory is a sibling of this module — i.e. the
    package root *is* ``parent``, not ``parent.parent``.
    """
    return Path(__file__).resolve().parent


def project_root() -> Path:
    """业务数据根：midout、202602 等相对此目录。"""
    env = os.environ.get("OWN_FLOW_PROJECT_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    app = own_flow_app_root()
    p = app.parent
    if p.name == "script" and (p / "paths.py").exists():
        return p.parent
    if p.name == "allline":
        s = p.parent
        if s.name == "script" and (s / "paths.py").exists():
            return s.parent
    return app


def midout_dir() -> Path:
    """自有流水输出根目录；单次运行文件在 ``ownflow_run_dir(run_id)`` 下。"""
    env = os.environ.get("OWN_FLOW_MIDOUT")
    if env:
        return Path(env).expanduser().resolve()
    app = own_flow_app_root()
    parent = app.parent
    if parent.name == "allline":
        return parent / "midfile" / "ownflow"
    return project_root() / "midout"


def new_run_id() -> str:
    """与 Allline 账单 midfile run_id 一致：UTC，如 20260427T060909Z。"""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ownflow_run_dir(run_id: str) -> Path:
    """本次运行目录：``midout_dir()/<run_id>/``。"""
    return midout_dir() / run_id


def latest_ownflow_matched_excel() -> Path | None:
    """``midout_dir()`` 下最新一份 ``own_bank_statement_matched.xlsx``（含子目录或旧版平铺）。"""
    base = midout_dir()
    if not base.is_dir():
        return None
    candidates: list[Path] = []
    flat = base / "own_bank_statement_matched.xlsx"
    if flat.is_file():
        candidates.append(flat)
    for p in base.glob("*/own_bank_statement_matched.xlsx"):
        if p.is_file():
            candidates.append(p)
    if not candidates:
        return None
    return max(candidates, key=lambda q: q.stat().st_mtime)

