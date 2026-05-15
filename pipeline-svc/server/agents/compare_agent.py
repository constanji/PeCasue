"""CompareAgent — narrative wrapper around the compare toolset.

The compare HTTP endpoint runs the deterministic pipeline directly. This agent
gives the Copilot a way to *describe* the result in natural language and
suggest next-step actions (e.g. propose a rule patch when a column drift
pattern is obvious).
"""
from __future__ import annotations

from typing import Any, Dict, List


def explain_compare(meta: Dict[str, Any]) -> str:
    """Render a brief natural-language summary of a compare result."""
    s = meta.get("summary", {}) or {}
    by_col = s.get("by_column") or {}
    parts: List[str] = []
    parts.append(
        f"对比 `{meta.get('left', {}).get('label')}` vs `{meta.get('right', {}).get('label')}` 完成。"
    )
    parts.append(
        f"匹配 {s.get('matched_rows', 0)} 行 / 仅左 {s.get('only_left_rows', 0)} 行 / 仅右 {s.get('only_right_rows', 0)} 行。"
    )
    parts.append(f"共发现 {s.get('diff_cells', 0)} 个差异单元格。")
    if by_col:
        top = sorted(by_col.items(), key=lambda kv: kv[1], reverse=True)[:5]
        parts.append("差异最多的列：" + " · ".join(f"{k}({v})" for k, v in top))
    if s.get("only_left_rows", 0) + s.get("only_right_rows", 0) > 0:
        parts.append(
            "建议：先排查“仅左/仅右”行是否因 mapping 缺失（账户/费项），可在规则页补齐后重跑。"
        )
    return "\n".join(parts)
