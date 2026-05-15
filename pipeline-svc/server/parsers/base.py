"""Common parser interface.

Each parser receives the absolute paths under ``data/tasks/{tid}/extracted/{ch}/``
and writes outputs to ``data/tasks/{tid}/channels/{ch}/runs/{run_id}/``.

The parser must:
    1. Produce zero or more ``output_files`` (FileEntry) with sha256.
    2. Produce zero or more ``verify_rows`` summarising row-level outcomes
       (pass / warning / pending), surfaced in the channel detail page.
    3. Append warnings (UI shows them as a banner).

It must NOT mutate state.json directly — the orchestrator persists the result.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional

from server.core.paths import is_extracted_rel_path_parse_candidate
from server.core.pipeline_state import FileEntry


@dataclass
class VerifyRow:
    row_id: str
    severity: str   # pass | warning | pending
    summary: str
    rule_ref: Optional[str] = None
    file_ref: Optional[str] = None
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParseContext:
    task_id: str
    channel_id: str
    run_id: str
    extracted_dir: Path     # data/tasks/{tid}/extracted/{ch}/
    output_dir: Path        # data/tasks/{tid}/channels/{ch}/runs/{rid}/
    period: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParseResult:
    output_files: List[FileEntry] = field(default_factory=list)
    verify_rows: List[VerifyRow] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    note: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def make_file_entry(path: Path, *, role: str = "output") -> FileEntry:
    """Helper used by parser implementations to register an output file."""
    stat = path.stat() if path.exists() else None
    return FileEntry(
        name=path.name,
        path=str(path),
        size=stat.st_size if stat else 0,
        sha256=_sha256(path) if stat else None,
        role=role,
    )


class BaseParser:
    """Subclasses set ``channel_id`` / ``display_name`` and implement ``parse``."""

    channel_id: ClassVar[str] = ""
    display_name: ClassVar[str] = ""
    output_filename: ClassVar[str] = "result.csv"

    def parse(self, *, ctx: ParseContext) -> ParseResult:  # pragma: no cover
        raise NotImplementedError

    # ---- shared helpers ----

    @staticmethod
    def list_source_files(extracted_dir: Path) -> list[Path]:
        if not extracted_dir.exists():
            return []
        out: list[Path] = []
        for p in sorted(extracted_dir.rglob("*")):
            if not p.is_file():
                continue
            try:
                rel = p.relative_to(extracted_dir).as_posix()
            except ValueError:
                continue
            if is_extracted_rel_path_parse_candidate(rel):
                out.append(p)
        return out

    def write_manifest(
        self, ctx: ParseContext, *, sources: list[Path], extras: Optional[dict] = None
    ) -> Path:
        manifest = {
            "task_id": ctx.task_id,
            "channel_id": ctx.channel_id,
            "run_id": ctx.run_id,
            "period": ctx.period,
            "parser": self.__class__.__name__,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "input_files": [
                {
                    "name": p.name,
                    "rel_path": str(p.relative_to(ctx.extracted_dir))
                    if p.is_relative_to(ctx.extracted_dir)
                    else str(p),
                    "size": p.stat().st_size,
                }
                for p in sources
            ],
            "extras": extras or {},
        }
        out = ctx.output_dir / "manifest.json"
        out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    def write_placeholder_csv(
        self,
        ctx: ParseContext,
        *,
        sources: list[Path],
        columns: list[str],
        rows: Optional[list[list[Any]]] = None,
    ) -> Path:
        out = ctx.output_dir / self.output_filename
        with open(out, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(columns)
            if rows:
                w.writerows(rows)
            else:
                # default: one row per source file as a smoke-test row
                for p in sources:
                    w.writerow(
                        [p.name]
                        + [""] * (len(columns) - 1)
                    )
        return out
