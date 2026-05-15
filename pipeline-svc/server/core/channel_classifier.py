"""Heuristic classifier mapping a folder/file path to a PeCause channel id.

Conceptually similar to historical Allline ``SOURCE_CATALOG`` slot scanning but
extended to PeCause's channel set (including ``special_*`` splits).
The classifier is **ordered** — the first rule that matches wins.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

CHANNEL_ID_BILL = "bill"
CHANNEL_ID_OWN_FLOW = "own_flow"
CHANNEL_ID_CUSTOMER = "customer"
CHANNEL_ID_SPECIAL_TRANSFER = "special_transfer"
CHANNEL_ID_SPECIAL_ACH_REFUND = "special_ach_refund"
CHANNEL_ID_SPECIAL_OP_INCOMING = "special_op_incoming"
CHANNEL_ID_SPECIAL_OP_REFUND = "special_op_refund"
CHANNEL_ID_SPECIAL_MERGE = "special_merge"
CHANNEL_ID_FINAL_MERGE = "final_merge"
CHANNEL_ID_CN_JP = "cn_jp"
CHANNEL_ID_ALLOCATION_BASE = "allocation_base"
CHANNEL_ID_SUMMARY = "summary"
CHANNEL_ID_UNKNOWN = "unknown"


@dataclass(frozen=True)
class ChannelDef:
    channel_id: str
    display_name: str
    entry_type: str
    hint: str
    matcher: Callable[[str], bool]


def _has_all(name: str, *needles: str) -> bool:
    return all(n in name for n in needles)


def _has_any(name: str, *needles: str) -> bool:
    return any(n in name for n in needles)


def _ci_has(name: str, *needles: str) -> bool:
    lower = name.lower()
    return any(n.lower() in lower for n in needles)


# 业务常见简写：「2026.03自有」「202603自有」（不要求目录名再含「流水」）
_OWN_FLOW_PERIOD_SHORTHAND = re.compile(
    r"(?:19|20)\d{2}\s*[.\uFF0E／/年\-]\s*\d{1,2}\s*自有"
    r"|(?:19|20)\d{4}(?!\d)\s*自有"
)


def _is_own_flow_name(name: str) -> bool:
    if "客资" in name:
        return False
    if _has_all(name, "自有", "流水"):
        return True
    if "自有" not in name:
        return False
    return bool(_OWN_FLOW_PERIOD_SHORTHAND.search(name))


CHANNEL_CATALOG: tuple[ChannelDef, ...] = (
    ChannelDef(
        CHANNEL_ID_BILL, "账单", "bill", "顶层名含「账单」", lambda n: "账单" in n,
    ),
    ChannelDef(
        CHANNEL_ID_OWN_FLOW,
        "自有流水",
        "own_flow",
        "含「自有+流水」或账期简写如「2026.03自有」",
        _is_own_flow_name,
    ),
    ChannelDef(
        CHANNEL_ID_CUSTOMER, "客资流水", "customer", "含「客资」",
        lambda n: "客资" in n,
    ),
    # special_op_* must precede special_transfer/ach to disambiguate "OP退票" / "OP入账"
    ChannelDef(
        CHANNEL_ID_SPECIAL_OP_INCOMING,
        "OP 入账",
        "special",
        "含「OP表」「op表」或「OP」与「入账」",
        lambda n: ("OP表" in n or "op表" in n.lower())
        or (("OP" in n or "op" in n.lower()) and ("入账" in n)),
    ),
    ChannelDef(
        CHANNEL_ID_SPECIAL_OP_REFUND, "OP 退票", "special",
        "含「OP」与「退」",
        lambda n: ("OP" in n or "op" in n.lower()) and ("退" in n),
    ),
    ChannelDef(
        CHANNEL_ID_SPECIAL_MERGE,
        "特殊来源·合并",
        "special",
        "不参与上传分类，仅合并步骤使用",
        lambda n: False,
    ),
    ChannelDef(
        CHANNEL_ID_FINAL_MERGE,
        "最终合并",
        "special",
        "聚合各渠道已校验产出并运行成本汇总；不参与 zip 目录分类",
        lambda n: False,
    ),
    # 含「内转」的目录优先于 ACH/return，避免「Ach return退款+内转」仅落入 ACH
    ChannelDef(
        CHANNEL_ID_SPECIAL_TRANSFER, "内转", "special",
        "含「内转」或「transfer」(非 OP)",
        lambda n: "内转" in n or (_ci_has(n, "transfer") and not _ci_has(n, "op")),
    ),
    ChannelDef(
        CHANNEL_ID_SPECIAL_ACH_REFUND,
        "ACH 退票",
        "special",
        "含「ACH」或「return」/「退票」（不含「内转」）",
        lambda n: ("内转" not in n)
        and (_ci_has(n, "ach") or _has_any(n, "退票", "return")),
    ),
    ChannelDef(
        CHANNEL_ID_CN_JP, "境内&日本通道", "cn_jp",
        "含「境内」「国内」「日本」「JP」",
        lambda n: _has_any(n, "境内", "国内", "日本") or _ci_has(n, "jp"),
    ),
    ChannelDef(
        CHANNEL_ID_ALLOCATION_BASE, "分摊基数", "allocation_base",
        "含「分摊」或「基数」",
        lambda n: _has_any(n, "分摊", "基数"),
    ),
    ChannelDef(
        CHANNEL_ID_SUMMARY, "汇总", "summary",
        "含「汇总」或「summary」",
        lambda n: "汇总" in n or _ci_has(n, "summary"),
    ),
)

_CHANNEL_BY_ID: dict[str, ChannelDef] = {c.channel_id: c for c in CHANNEL_CATALOG}


def list_channel_defs() -> tuple[ChannelDef, ...]:
    return CHANNEL_CATALOG


def get_channel_def(channel_id: str) -> ChannelDef | None:
    return _CHANNEL_BY_ID.get(channel_id)


def classify_name(name: str) -> str:
    for d in CHANNEL_CATALOG:
        if d.matcher(name):
            return d.channel_id
    return CHANNEL_ID_UNKNOWN


def _visible_children(root: Path) -> list[Path]:
    """Top-level entries excluding dotfiles and macOS zip cruft."""
    skip = {"__MACOSX"}
    return sorted(
        (p for p in root.iterdir() if not p.name.startswith(".") and p.name not in skip),
        key=lambda p: p.name,
    )


def _peel_and_collect_orphans(root: Path, *, max_depth: int = 32) -> tuple[Path, list[Path]]:
    """Enter nested wrappers named ``unknown`` while collecting stray sibling files.

    Typical zip layout::

        说明.txt          ← sibling files used to break ``len(children)==1`` peel
        3月数据/
          2026.03账单/
          …

    Old logic required *only one* top-level entry, so a lone readme beside ``3月数据``
    prevented peeling and the whole folder landed in ``unknown``.
    """
    cur = root.resolve()
    orphans: list[Path] = []
    if not cur.is_dir():
        return cur, orphans
    for _ in range(max_depth):
        visible = _visible_children(cur)
        dirs = [p for p in visible if p.is_dir()]
        files = [p for p in visible if p.is_file()]
        if len(dirs) == 1 and classify_name(dirs[0].name) == CHANNEL_ID_UNKNOWN:
            orphans.extend(files)
            cur = dirs[0].resolve()
            continue
        break
    return cur, orphans


@dataclass
class ClassifiedItem:
    """One physical entry classified into a channel."""

    channel_id: str
    display_name: str
    path: Path                  # absolute path on disk
    rel_path: str               # relative to effective root (after peeling wrappers)
    is_dir: bool
    file_count: int = 0


@dataclass
class ChannelGroup:
    channel_id: str
    display_name: str
    entry_type: str
    items: list[ClassifiedItem] = field(default_factory=list)

    @property
    def total_files(self) -> int:
        return sum(i.file_count for i in self.items)


def _file_count(p: Path) -> int:
    if p.is_file():
        return 1
    if not p.is_dir():
        return 0
    n = 0
    for x in p.rglob("*"):
        if x.is_file():
            n += 1
    return n


def classify_extracted_root(extracted_root: Path) -> dict[str, ChannelGroup]:
    """Walk top-level entries of ``extracted_root`` and group them by channel.

    Top-level files (not dirs) are also classified by name.
    """
    anchor = extracted_root.resolve()
    root, orphans = _peel_and_collect_orphans(extracted_root)
    out: dict[str, ChannelGroup] = {}
    if not root.is_dir():
        return out

    # Pre-seed all known channels so empty groups are visible to the UI.
    for d in CHANNEL_CATALOG:
        out.setdefault(d.channel_id, ChannelGroup(d.channel_id, d.display_name, d.entry_type))
    out.setdefault(
        CHANNEL_ID_UNKNOWN,
        ChannelGroup(CHANNEL_ID_UNKNOWN, "未识别", "unknown"),
    )

    def _append_entry(child: Path, *, rel_path: str | None = None) -> None:
        cid = classify_name(child.name)
        d = _CHANNEL_BY_ID.get(cid)
        display = d.display_name if d else "未识别"
        entry_type = d.entry_type if d else "unknown"
        rp = rel_path if rel_path is not None else str(child.relative_to(root))
        item = ClassifiedItem(
            channel_id=cid,
            display_name=display,
            path=child,
            rel_path=rp,
            is_dir=child.is_dir(),
            file_count=_file_count(child),
        )
        out.setdefault(cid, ChannelGroup(cid, display, entry_type)).items.append(item)

    for child in _visible_children(root):
        _append_entry(child)

    for fp in orphans:
        try:
            rel = str(fp.relative_to(anchor))
        except ValueError:
            rel = fp.name
        _append_entry(fp, rel_path=rel)

    return out


def summarize_classification(groups: dict[str, ChannelGroup]) -> dict[str, dict]:
    """Serializable summary used in API responses."""
    return {
        cid: {
            "channel_id": g.channel_id,
            "display_name": g.display_name,
            "entry_type": g.entry_type,
            "item_count": len(g.items),
            "total_files": g.total_files,
            "items": [
                {
                    "rel_path": it.rel_path,
                    "is_dir": it.is_dir,
                    "file_count": it.file_count,
                }
                for it in g.items
            ],
        }
        for cid, g in groups.items()
    }
