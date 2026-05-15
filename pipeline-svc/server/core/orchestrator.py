"""Orchestrate single-channel runs.

A *run* is the smallest atomic unit:
    1. Allocate a fresh ``run_id`` and create the run dir.
    2. Append a ``ChannelRunHistory`` entry; mark channel.status = RUNNING.
    3. Invoke the channel parser inside a thread (parsers are sync/CPU-bound).
    4. On success: persist output_files (with hash), verify_summary; flip status
       to ``verified`` / ``verified_with_warning`` based on verify rows.
    5. On failure: write error trace, status = FAILED.
    6. Recompute task-level status (pending/partial/completed/failed).
    7. Append lifecycle events for every transition (Phase 9 observability).

The function intentionally does NOT raise; the caller (FastAPI BackgroundTasks)
should not see exceptions — anything bad lands in the run/channel state.
"""
from __future__ import annotations

import asyncio
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from server.core.paths import (
    get_channel_run_dir,
    get_task_extracted_dir,
    is_extracted_rel_path_parse_candidate,
)
from server.core.pipeline_state import (
    AuditEvent,
    ChannelRunHistory,
    ChannelRunStatus,
    PipelineStep,
    StateManager,
    TaskState,
    TaskStatus,
    utc_now_iso,
)
from server.core.task_logger import task_log
from server.core.task_repo import TaskRepo
from server.parsers import get_parser
from server.parsers.base import ParseContext, ParseResult


def _severity_to_status(verify_rows) -> ChannelRunStatus:
    has_warning = any(r.severity == "warning" for r in verify_rows)
    has_pending = any(r.severity == "pending" for r in verify_rows)
    if has_pending and not any(r.severity == "pass" for r in verify_rows):
        return ChannelRunStatus.PREVIEW_READY
    if has_warning:
        return ChannelRunStatus.VERIFIED_WITH_WARNING
    return ChannelRunStatus.VERIFIED


def _recompute_task_status(state: TaskState) -> None:
    statuses = [ch.status for ch in state.channels.values()]
    if not statuses:
        state.status = TaskStatus.PENDING
        state.current_step = PipelineStep.CREATED
        return
    if any(s == ChannelRunStatus.RUNNING for s in statuses):
        state.status = TaskStatus.RUNNING
        state.current_step = PipelineStep.RUNNING
        return
    if any(s == ChannelRunStatus.FAILED for s in statuses) and not any(
        s in {ChannelRunStatus.VERIFIED, ChannelRunStatus.VERIFIED_WITH_WARNING, ChannelRunStatus.CONFIRMED}
        for s in statuses
    ):
        state.status = TaskStatus.FAILED
        state.current_step = PipelineStep.FAILED
        return
    confirmed = sum(1 for s in statuses if s == ChannelRunStatus.CONFIRMED)
    verified = sum(
        1
        for s in statuses
        if s in {ChannelRunStatus.VERIFIED, ChannelRunStatus.VERIFIED_WITH_WARNING}
    )
    pending = sum(1 for s in statuses if s == ChannelRunStatus.PENDING)
    if pending == 0 and (confirmed + verified) == len(statuses) and confirmed == len(statuses):
        state.status = TaskStatus.COMPLETED
        state.current_step = PipelineStep.COMPLETED
    elif (verified + confirmed) > 0:
        state.status = TaskStatus.PARTIAL
        state.current_step = PipelineStep.RUNNING
    else:
        state.status = TaskStatus.PENDING
        state.current_step = PipelineStep.CREATED


async def run_channel(
    task_id: str,
    channel_id: str,
    *,
    actor: str = "machine",
    run_payload: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Public entry point invoked from `POST /tasks/{tid}/channels/{ch}/run`."""
    state = StateManager.load_state(task_id)
    if state is None:
        return None
    if channel_id not in state.channels:
        return None
    parser_cls = get_parser(channel_id)
    if parser_cls is None:
        return None

    ch = state.channels[channel_id]
    extracted = get_task_extracted_dir(task_id, channel_id)
    if not state.started_at:
        state.started_at = utc_now_iso()

    history = ChannelRunHistory(status=ChannelRunStatus.RUNNING)
    payload = run_payload or {}
    note_val = payload.get("note")
    if isinstance(note_val, str):
        history.note = note_val
    phase_raw = payload.get("allocation_phase")
    if isinstance(phase_raw, str) and phase_raw.strip():
        history.allocation_phase = phase_raw.strip().lower()
    opts = payload.get("allocation_options")
    if isinstance(opts, dict):
        history.run_options = opts

    ch.runs.append(history)
    ch.current_run_id = history.run_id
    ch.status = ChannelRunStatus.RUNNING

    state.audit.append(
        AuditEvent(
            actor=actor,
            action="channel.run.start",
            target=f"{channel_id}/{history.run_id}",
        )
    )
    _recompute_task_status(state)
    StateManager.save_state(state)
    task_log(
        task_id,
        f"── run {history.run_id[:8]} · 开始 · {parser_cls.__name__} ──",
        channel=channel_id,
    )
    task_log(task_id, "通道运行开始（解析）…", channel=channel_id)
    extracted_ct = 0
    for p in extracted.rglob("*"):
        if not p.is_file():
            continue
        try:
            rel = p.relative_to(extracted).as_posix()
        except ValueError:
            continue
        if is_extracted_rel_path_parse_candidate(rel):
            extracted_ct += 1
    task_log(
        task_id,
        f"解析调度 · parser={parser_cls.__name__} · extracted 内有效文件数={extracted_ct}",
        channel=channel_id,
    )
    TaskRepo.append_event(
        task_id,
        "channel.run.start",
        channel_id=channel_id,
        run_id=history.run_id,
        to_status=str(ChannelRunStatus.RUNNING),
    )

    started_perf = time.perf_counter()
    result: Optional[ParseResult] = None

    def _do_parse() -> ParseResult:
        run_dir = get_channel_run_dir(task_id, channel_id, history.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        # Build upstream snapshot for SummaryParser
        upstream_snapshot: Dict[str, Dict[str, Any]] = {}
        if channel_id == "summary":
            for cid, other in state.channels.items():
                if cid == "summary":
                    continue
                upstream_snapshot[cid] = {
                    "ready": other.status
                    in {
                        ChannelRunStatus.VERIFIED,
                        ChannelRunStatus.VERIFIED_WITH_WARNING,
                        ChannelRunStatus.CONFIRMED,
                    },
                    "status": str(other.status),
                }

        ctx = ParseContext(
            task_id=task_id,
            channel_id=channel_id,
            run_id=history.run_id,
            extracted_dir=extracted,
            output_dir=run_dir,
            period=state.period,
            metadata={
                "upstream": upstream_snapshot,
                "allocation_phase": history.allocation_phase,
                "allocation_options": dict(history.run_options or {}),
                "allocation_task_state": dict(state.metadata.get("allocation") or {}),
            },
        )
        parser = parser_cls()
        return parser.parse(ctx=ctx)

    try:
        timeout_sec = int(os.environ.get("PIPELINE_PARSE_TIMEOUT_SEC", "600"))
        result = await asyncio.wait_for(
            asyncio.to_thread(_do_parse),
            timeout=timeout_sec,
        )
        task_log(
            task_id,
            f"解析线程已完成 · 产出校验条目={len(result.verify_rows)} · warnings={len(result.warnings)}",
            channel=channel_id,
        )

        history.output_files = result.output_files
        history.verify_summary = {
            "rows": [
                {
                    "row_id": r.row_id,
                    "severity": r.severity,
                    "summary": r.summary,
                    "rule_ref": r.rule_ref,
                    "file_ref": r.file_ref,
                    "detail": r.detail,
                }
                for r in result.verify_rows
            ],
            "warnings": result.warnings,
            "metrics": result.metrics,
            "note": result.note,
        }
        history.status = _severity_to_status(result.verify_rows)
        history.note = result.note
        history.duration_seconds = round(time.perf_counter() - started_perf, 3)
        history.finished_at = utc_now_iso()
        for w in result.warnings:
            task_log(task_id, f"WARN: {w}", level="WARNING", channel=channel_id)
        task_log(
            task_id,
            f"校验摘要 — {len(result.verify_rows)} 条",
            channel=channel_id,
        )
        for r in result.verify_rows:
            lvl = "WARNING" if r.severity == "warning" else "INFO"
            task_log(task_id, f"  [{r.severity}] {r.summary}", level=lvl, channel=channel_id)
        task_log(
            task_id,
            f"── run {history.run_id[:8]} · 结束 · {history.status} · 产出 {len(history.output_files)} 个文件 ──",
            channel=channel_id,
        )

    except asyncio.TimeoutError:
        history.status = ChannelRunStatus.FAILED
        history.error = (
            f"TimeoutError: 解析超时（>{os.environ.get('PIPELINE_PARSE_TIMEOUT_SEC', '600')}s），"
            "请检查输入文件大小或增加 PIPELINE_PARSE_TIMEOUT_SEC 环境变量后重试。"
        )
        history.duration_seconds = round(time.perf_counter() - started_perf, 3)
        history.finished_at = utc_now_iso()
        task_log(task_id, "Run TIMEOUT: 解析超时", level="ERROR", channel=channel_id)
        task_log(
            task_id,
            f"── run {history.run_id[:8]} · 结束 · TIMEOUT ──",
            channel=channel_id,
        )

    except Exception as exc:  # noqa: BLE001
        history.status = ChannelRunStatus.FAILED
        history.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        history.duration_seconds = round(time.perf_counter() - started_perf, 3)
        history.finished_at = utc_now_iso()
        task_log(task_id, f"Run FAILED: {exc}", level="ERROR", channel=channel_id)
        task_log(
            task_id,
            f"── run {history.run_id[:8]} · 结束 · FAILED ──",
            channel=channel_id,
        )

    # Reload state to avoid overwriting concurrent channel runs (race condition:
    # two channels triggered simultaneously each load the same initial state and
    # the slower one overwrites the faster one's completed run record on final save).
    fresh_state = StateManager.load_state(task_id)
    if fresh_state is not None and channel_id in fresh_state.channels:
        fresh_ch = fresh_state.channels[channel_id]
        # Locate the in-progress run entry in fresh state and update it
        run_entry = next((r for r in fresh_ch.runs if r.run_id == history.run_id), None)
        if run_entry is None:
            # Run was lost due to a concurrent save — re-append it now
            fresh_ch.runs.append(history)
        else:
            run_entry.output_files = history.output_files
            run_entry.verify_summary = history.verify_summary
            run_entry.status = history.status
            run_entry.note = history.note
            run_entry.error = history.error
            run_entry.duration_seconds = history.duration_seconds
            run_entry.finished_at = history.finished_at
        fresh_ch.status = history.status
        fresh_ch.warnings = list(result.warnings if result is not None else ch.warnings)
        fresh_ch.current_run_id = history.run_id
        if channel_id == "allocation_base" and history.status != ChannelRunStatus.FAILED:
            metrics = (result.metrics or {}) if result is not None else {}
            patch = metrics.get("allocation_state_patch")
            if isinstance(patch, dict) and patch:
                alloc_bucket = fresh_state.metadata.setdefault("allocation", {})
                for k, v in patch.items():
                    if isinstance(k, str):
                        alloc_bucket[k] = v
        state = fresh_state
        ch = fresh_ch

    state.audit.append(
        AuditEvent(
            actor=actor,
            action="channel.run.end",
            target=f"{channel_id}/{history.run_id}",
            detail={"status": str(history.status), "duration": history.duration_seconds},
        )
    )
    _recompute_task_status(state)
    StateManager.save_state(state)
    TaskRepo.append_event(
        task_id,
        "channel.run.end",
        channel_id=channel_id,
        run_id=history.run_id,
        to_status=str(history.status),
        payload={
            "duration_seconds": history.duration_seconds,
            "warnings": len(ch.warnings),
            "output_files": len(history.output_files),
        },
    )
    if history.status == ChannelRunStatus.FAILED:
        TaskRepo.append_event(
            task_id,
            "channel.run.failed",
            channel_id=channel_id,
            run_id=history.run_id,
            reason_code=type(history).__name__,
            reason_detail=(history.error or "")[:512],
            payload={"duration_seconds": history.duration_seconds},
        )
    elif history.status == ChannelRunStatus.VERIFIED_WITH_WARNING:
        TaskRepo.append_event(
            task_id,
            "channel.run.warning",
            channel_id=channel_id,
            run_id=history.run_id,
            payload={
                "duration_seconds": history.duration_seconds,
                "warnings": len(ch.warnings),
            },
        )
    return history.run_id
