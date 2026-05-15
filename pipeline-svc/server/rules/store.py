"""Rule store backed by JSON files + sqlite history.

Layout under ``data/rules/``::

    manifest.json
    files/
      account_mapping/
        current.json
        v1.json v2.json ...
      fee_mapping/
      ...
      password_book.enc       (handled by `password_book.py`)
"""
from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any, Optional

from server.core.paths import (
    get_rules_dir,
    get_rules_files_dir,
    get_rules_manifest_path,
)
from server.core.task_db import get_connection
from server.rules.schema import (
    DEFAULT_COLUMNS,
    RuleKind,
    RuleManifest,
    RuleManifestEntry,
    RuleTable,
    _utc_now,
)


def _kind_dir(kind: RuleKind) -> Path:
    return get_rules_files_dir() / kind.value


def _kind_current(kind: RuleKind) -> Path:
    return _kind_dir(kind) / "current.json"


def _kind_version(kind: RuleKind, version: int) -> Path:
    return _kind_dir(kind) / f"v{version}.json"


def load_manifest() -> RuleManifest:
    path = get_rules_manifest_path()
    if not path.exists():
        return RuleManifest()
    try:
        return RuleManifest(**json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return RuleManifest()


def save_manifest(m: RuleManifest) -> None:
    path = get_rules_manifest_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    m.updated_at = _utc_now()
    path.write_text(m.model_dump_json(indent=2), encoding="utf-8")


def load_rule(kind: RuleKind) -> RuleTable:
    path = _kind_current(kind)
    if not path.exists():
        return RuleTable(columns=list(DEFAULT_COLUMNS.get(kind, [])), rows=[])
    try:
        return RuleTable(**json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return RuleTable(columns=list(DEFAULT_COLUMNS.get(kind, [])), rows=[])


def save_rule(
    kind: RuleKind,
    table: RuleTable,
    *,
    author: Optional[str] = None,
    note: Optional[str] = None,
) -> RuleManifestEntry:
    """Bump version, snapshot the new payload, update manifest."""
    manifest = load_manifest()
    entry = manifest.entries.get(kind.value, RuleManifestEntry(kind=kind, version=0))
    new_version = entry.version + 1
    new_entry = RuleManifestEntry(
        kind=kind,
        version=new_version,
        updated_at=_utc_now(),
        updated_by=author,
        rows_count=len(table.rows),
    )

    _kind_dir(kind).mkdir(parents=True, exist_ok=True)
    payload = table.model_dump_json(indent=2)
    _kind_current(kind).write_text(payload, encoding="utf-8")
    snapshot_path = _kind_version(kind, new_version)
    snapshot_path.write_text(payload, encoding="utf-8")

    manifest.entries[kind.value] = new_entry
    save_manifest(manifest)

    with get_connection() as conn:
        conn.execute(
            """INSERT INTO rules_versions (kind, version, snapshot_path, author, note, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (kind.value, new_version, str(snapshot_path), author, note, new_entry.updated_at),
        )

    return new_entry


def _snapshot_table_summary(kind: RuleKind, snapshot_path: str) -> dict[str, Any]:
    """Light read of version JSON for history UI (rows count, fx month, …)."""
    out: dict[str, Any] = {
        "rows_count": None,
        "fx_month_label": None,
        "snapshot_basename": Path(snapshot_path).name,
    }
    p = Path(snapshot_path)
    if not p.is_file():
        return out
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return out
    rows = raw.get("rows")
    if isinstance(rows, list):
        out["rows_count"] = len(rows)
    meta = raw.get("meta")
    if kind == RuleKind.FX and isinstance(meta, dict):
        lab = meta.get("fx_month_label")
        if lab is not None and str(lab).strip():
            out["fx_month_label"] = str(lab).strip()
    return out


def list_versions(kind: RuleKind) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT version, snapshot_path, author, note, created_at
               FROM rules_versions WHERE kind = ?
               ORDER BY version DESC""",
            (kind.value,),
        ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        d.update(_snapshot_table_summary(kind, str(d.get("snapshot_path") or "")))
        out.append(d)
    return out


def get_fx_preferred_yyyymm() -> Optional[str]:
    """从 FX RuleStore meta 读取 fx_month_label，转为 YYYYMM 字符串（如 '202602'）。

    供所有执行路径在多月份 CSV 中选取正确月份使用。
    若未设置则返回 None，调用方应回退到取 CSV 中最新月份。
    """
    try:
        table = load_rule(RuleKind.FX)
        lab = (table.meta or {}).get("fx_month_label") if table.meta else None
        if not lab:
            return None
        s = str(lab).strip()
        m = re.search(r"(20\d{2})年(\d{1,2})月", s)
        if m:
            return f"{m.group(1)}{int(m.group(2)):02d}"
        m2 = re.search(r"(20\d{2})[-/](\d{1,2})", s)
        if m2:
            mo = int(m2.group(2))
            if 1 <= mo <= 12:
                return f"{m2.group(1)}{mo:02d}"
    except Exception:
        pass
    return None


def rollback(
    kind: RuleKind,
    *,
    target_version: int,
    author: Optional[str] = None,
    note: Optional[str] = None,
) -> RuleManifestEntry:
    snapshot = _kind_version(kind, target_version)
    if not snapshot.exists():
        raise FileNotFoundError(f"Version {target_version} not found for {kind.value}")
    raw = snapshot.read_text(encoding="utf-8")
    table = RuleTable(**json.loads(raw))
    return save_rule(
        kind,
        table,
        author=author,
        note=note or f"rollback to v{target_version}",
    )
