"""Channel prescan summaries for UI (aligned with zhangdan folder_match + own_flow discovery).

- ``bill``: folder rows matching ``folder_match`` rules (nested-safe), **without** importing bank parsers.
- ``own_flow``: delegating to ``own_flow_pkg.discovery.scan_inventory``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from server.parsers._legacy.zhangdan.folder_match import first_matching_bank_key


def _first_bank_key(folder_name: str) -> str | None:
    return first_matching_bank_key(folder_name)


def scan_bill_prescan(extracted_root: Path | None) -> dict[str, Any]:
    """Per-folder stats analogous to allline Streamlit 「预扫结果»."""
    if extracted_root is None:
        return {
            "kind": "bill",
            "folders": [],
            "top_level_non_bank_dirs": [],
        }
    root = extracted_root.expanduser().resolve()
    if not root.is_dir():
        return {
            "kind": "bill",
            "folders": [],
            "top_level_non_bank_dirs": [],
        }

    matched_paths: list[tuple[Path, str]] = []

    for folder in sorted(root.rglob("*")):
        if not folder.is_dir():
            continue
        if "__MACOSX" in folder.parts:
            continue
        try:
            folder.relative_to(root)
        except ValueError:
            continue

        bk = _first_bank_key(folder.name)
        if bk is None:
            continue

        nested_same_bank = False
        p = folder.parent
        while p != root:
            try:
                p.relative_to(root)
            except ValueError:
                break
            pb = _first_bank_key(p.name)
            if pb == bk:
                nested_same_bank = True
                break
            p = p.parent

        if nested_same_bank:
            continue

        matched_paths.append((folder, bk))

    seen: set[str] = set()
    folders: list[dict[str, Any]] = []
    for folder, bk in matched_paths:
        key = str(folder.resolve())
        if key in seen:
            continue
        seen.add(key)
        try:
            rel = folder.relative_to(root)
            rel_s = rel.as_posix()
        except ValueError:
            rel_s = folder.name

        file_count = sum(
            1
            for x in folder.rglob("*")
            if x.is_file() and not x.name.startswith(".") and "__MACOSX" not in x.parts
        )
        folders.append(
            {
                "folder_name": folder.name,
                "folder_path": rel_s,
                "bank_key": bk,
                "file_count": file_count,
            }
        )

    folders.sort(key=lambda r: r["folder_path"])

    top_level_non_bank: list[dict[str, str]] = []
    try:
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue
            if child.name == "__MACOSX":
                continue
            if _first_bank_key(child.name):
                continue
            top_level_non_bank.append(
                {
                    "folder_name": child.name,
                    "folder_path": child.relative_to(root).as_posix(),
                    "hint": "该顶层文件夹名未命中「XX账单」别名；流水线会在整树下递归查找子目录（与 generate_summary 一致）",
                }
            )
    except OSError:
        pass

    return {
        "kind": "bill",
        "folders": folders,
        "top_level_non_bank_dirs": top_level_non_bank,
    }


def scan_own_flow_prescan(extracted_root: Path | None) -> dict[str, Any]:
    """Row-wise inventory analogous to allline 「已识别来源」."""
    from server.parsers._legacy.own_flow_pkg.discovery import scan_inventory

    if extracted_root is None:
        return {"kind": "own_flow", "sources": [], "warnings": []}
    root = extracted_root.expanduser().resolve()
    if not root.is_dir():
        return {"kind": "own_flow", "sources": [], "warnings": []}

    sources_raw, warnings_raw = scan_inventory(root)

    sources: list[dict[str, Any]] = []
    for idx, s in enumerate(sources_raw):
        abs_path = str(s.get("路径") or "")
        rel_path = ""
        if abs_path:
            try:
                rel_path = Path(abs_path).resolve().relative_to(root.resolve()).as_posix()
            except ValueError:
                try:
                    rel_path = Path(abs_path).as_posix()
                except Exception:
                    rel_path = abs_path

        row_cnt = s.get("行数")
        col_cnt = s.get("列数")
        sources.append(
            {
                "index": idx,
                "source": s.get("来源"),
                "file": s.get("文件"),
                "row_count": row_cnt if isinstance(row_cnt, (int, float)) else row_cnt,
                "col_count": col_cnt if isinstance(col_cnt, (int, float)) else col_cnt,
                "rel_path": rel_path,
            }
        )

    warnings: list[dict[str, Any]] = []
    for w in warnings_raw:
        warnings.append(
            {
                "rel_path": w.get("相对路径"),
                "reason": w.get("原因"),
                "detail": w.get("说明"),
            }
        )

    return {"kind": "own_flow", "sources": sources, "warnings": warnings}
