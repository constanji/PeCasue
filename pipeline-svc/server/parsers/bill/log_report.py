"""Parse zhangdan ``generate_summary`` stdout into channel blocks.

Structured report consumed by the Pipeline UI ``BillVerifyReport`` component;
logic kept in sync with the legacy Allline Streamlit bill-merge page.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class BillChannelBlock:
    key: str
    body: str
    rows: int | None = None
    tags: set[str] = field(default_factory=set)


def _tag_block(key: str, body: str) -> set[str]:
    t: set[str] = set()
    if re.search(r"成功提取并对齐\s*(\d+)\s*条", body):
        t.add("success")
    if re.search(
        r"未提取到数据|已跳过|没有提取到任何数据|没有提取到任何|未从任何文件夹",
        body,
    ):
        t.add("empty")
    wk = (
        "警告",
        "处理失败",
        "时出错",
        "执行脚本异常",
        "读取中间结果失败",
        "未成功解析",
        "加密",
        "密码",
        "Password",
        "PdfminerException",
    )
    if any(x in body for x in wk) or re.search(r"Exception|Error:|\[.+\] 警告", body):
        t.add("warn")
    if key == "忽略":
        t.add("ignored")
    if key == "OTHER" and body.strip():
        t.add("preamble")
    if not t and key not in ("OTHER",):
        t.add("other")
    return t


def parse_bill_merge_log(text: str) -> list[BillChannelBlock]:
    if not (text or "").strip():
        return []
    text = text.replace("\r\n", "\n")
    text = text.replace("\\n[", "\n[")
    parts = re.split(
        r"(?m)(?=^\[[A-Z0-9_]+\]\s*开始解析|^\[忽略\])",
        text,
    )
    out: list[BillChannelBlock] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^\[([A-Z0-9_]+|忽略)\]\s*", part)
        if m:
            key = m.group(1)
        else:
            key = "OTHER"
        mm = re.search(r"成功提取并对齐\s*(\d+)\s*条", part)
        rows = int(mm.group(1)) if mm else None
        tags = _tag_block(key, part)
        out.append(BillChannelBlock(key=key, body=part, rows=rows, tags=tags))
    return out


def summarize_blocks(blocks: list[BillChannelBlock]) -> dict[str, int]:
    s = e = w = ign = 0
    for b in blocks:
        if "success" in b.tags:
            s += 1
        if "empty" in b.tags:
            e += 1
        if "warn" in b.tags:
            w += 1
        if "ignored" in b.tags or b.key == "忽略":
            ign += 1
    return {
        "success": s,
        "empty": e,
        "warn": w,
        "ignored_folders": ign,
    }


def bill_merge_report_payload(log_text: str) -> dict:
    blocks = parse_bill_merge_log(log_text)
    summary = summarize_blocks(blocks)
    return {
        "summary": summary,
        "blocks": [
            {
                "key": b.key,
                "body": b.body,
                "rows": b.rows,
                "tags": sorted(b.tags),
            }
            for b in blocks
        ],
        "raw_log": log_text,
    }
