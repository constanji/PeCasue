"""无账号渠道（如 BOC/BOSH）的主体与分行维度。

磁盘优先级：

1. ``OWN_FLOW_FILES_ROOT/mapping/特殊来源主体分行mapping.{xlsx,csv}``（若设置）
2. ``PIPELINE_DATA_DIR/rules/files/mapping/…``（上传规则侧车）

支持 **CSV（utf-8-sig）** 与 **xlsx**。
"""

from __future__ import annotations

import functools
import os
from pathlib import Path

import pandas as pd

from server.core.paths import get_rules_files_dir

_STEM = "特殊来源主体分行mapping"


_DEFAULT_SPECIAL: dict[str, tuple[str, str]] = {
    "boc": ("PPUS", "BOCUS"),
    "bosh": ("PPHK", "BOSH"),
}


def _mapping_roots_ordered() -> list[Path]:
    roots: list[Path] = []
    env_root = os.environ.get("OWN_FLOW_FILES_ROOT", "").strip()
    if env_root:
        roots.append(Path(env_root).expanduser().resolve() / "mapping")
    roots.append(get_rules_files_dir() / "mapping")
    return roots


def default_special_source_path() -> Path:
    """返回首个存在的映射文件路径；否则指向 ``rules/files/mapping/{stem}.csv``（未必已创建）。"""
    for root in _mapping_roots_ordered():
        xlsx = root / f"{_STEM}.xlsx"
        if xlsx.exists():
            return xlsx
        csv_path = root / f"{_STEM}.csv"
        if csv_path.exists():
            return csv_path
    return get_rules_files_dir() / "mapping" / f"{_STEM}.csv"


def _read_special_mapping_table(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(path, encoding="utf-8-sig")
    return pd.read_excel(path, engine="openpyxl")


def invalidate_special_source_mapping_cache() -> None:
    _load_special_source_mapping_cached.cache_clear()


@functools.lru_cache(maxsize=16)
def _load_special_source_mapping_cached(path_resolved: str, mtime_ns: int) -> frozenset[tuple[str, str, str]]:
    """
    按 (路径, 修改时间) 缓存解析结果；mtime 变化时自动失效。
    """
    path = Path(path_resolved)
    default = _DEFAULT_SPECIAL
    try:
        df = _read_special_mapping_table(path)
    except Exception:
        return frozenset((k, v[0], v[1]) for k, v in default.items())
    if df.empty:
        return frozenset((k, v[0], v[1]) for k, v in default.items())
    key_col = "file_group" if "file_group" in df.columns else ("来源" if "来源" in df.columns else None)
    if not key_col or "主体" not in df.columns:
        return frozenset((k, v[0], v[1]) for k, v in default.items())
    br_col = "分行维度" if "分行维度" in df.columns else None
    if not br_col:
        return frozenset((k, v[0], v[1]) for k, v in default.items())
    out = dict(default)
    for _, row in df.iterrows():
        fg = str(row.get(key_col, "") or "").strip().lower()
        if not fg or fg == "nan":
            continue
        ent = str(row.get("主体", "") or "").strip()
        br = str(row.get(br_col, "") or "").strip()
        if ent and br:
            out[fg.lower()] = (ent, br)
    return frozenset((k, v[0], v[1]) for k, v in out.items())


def load_special_source_mapping(path: Path | None = None) -> dict[str, tuple[str, str]]:
    """
    返回 file_group -> (主体, 分行维度)。
    文件不存在时使用内置默认（与业务约定一致）。

    注意：pipeline 每条流水都会查分行维度，此处必须缓存磁盘读取，否则会极慢。
    """
    path = path or default_special_source_path()
    if not path.exists():
        return dict(_DEFAULT_SPECIAL)
    p = path.expanduser().resolve()
    try:
        mt = p.stat().st_mtime_ns
    except OSError:
        return dict(_DEFAULT_SPECIAL)
    frozen = _load_special_source_mapping_cached(str(p), mt)
    out: dict[str, tuple[str, str]] = {}
    for item in frozen:
        fg, ent, br = item
        out[fg] = (ent, br)
    return out
