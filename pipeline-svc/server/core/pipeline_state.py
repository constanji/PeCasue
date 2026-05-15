"""Task / channel / run state model.

Slimmed-down vs `pingpong-master/server/core/pipeline_state.py`:
    - HITL/Intervention fields deferred to Phase 7 (added incrementally then).
    - Channel-centric design (plan §6.2):
        * ChannelState owns a list of ChannelRunHistory (immutable, append-only).
        * Each run records its output files with hash for replace-detection.
        * `is_dirty` propagates when an upstream file is replaced.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from server.core.paths import get_task_state_path, get_task_dir
from server.core.task_repo import TaskRepo


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def compute_duration_seconds(
    started_at: Optional[str], ended_at: Optional[str] = None
) -> Optional[float]:
    started = parse_iso_datetime(started_at)
    if not started:
        return None
    ended = parse_iso_datetime(ended_at) or datetime.now(timezone.utc)
    return max(0.0, (ended - started).total_seconds())


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PARTIAL = "partial"        # some channels done, others not
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    TERMINATED = "terminated"


class PipelineStep(str, Enum):
    """Coarse task-level step. Per-channel finer state lives in ChannelRunStatus."""

    CREATED = "CREATED"
    UPLOADING = "UPLOADING"
    CLASSIFYING = "CLASSIFYING"
    RUNNING = "RUNNING"
    SUMMARY = "SUMMARY"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERVENTION = "INTERVENTION"


class ChannelRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PREVIEW_READY = "preview_ready"
    VERIFIED = "verified"
    VERIFIED_WITH_WARNING = "verified_with_warning"
    EDITED = "edited"
    REPLACED = "replaced"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    SKIPPED = "skipped"


class FileEntry(BaseModel):
    file_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str
    path: str
    size: int = 0
    sha256: Optional[str] = None
    role: str = "output"  # output | intermediate | source
    created_at: str = Field(default_factory=utc_now_iso)


class AgentInteraction(BaseModel):
    interaction_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    kind: str                         # ask | propose_patch | propose_replace | resolve | ...
    summary: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now_iso)
    resolved_at: Optional[str] = None
    resolution: Optional[str] = None  # accepted | rejected | superseded


class ChannelRunHistory(BaseModel):
    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: str = Field(default_factory=utc_now_iso)
    finished_at: Optional[str] = None
    status: ChannelRunStatus = ChannelRunStatus.PENDING
    output_files: List[FileEntry] = Field(default_factory=list)
    verify_summary: Optional[Dict[str, Any]] = None
    agent_interactions: List[AgentInteraction] = Field(default_factory=list)
    error: Optional[str] = None
    duration_seconds: Optional[float] = None
    is_dirty: bool = False
    note: Optional[str] = None
    allocation_phase: Optional[str] = None
    run_options: Dict[str, Any] = Field(default_factory=dict)


class ChannelState(BaseModel):
    channel_id: str
    display_name: str
    entry_type: str = "auto"  # one of: bill, own_flow, customer, special, cn_jp, allocation_base, summary, unknown
    status: ChannelRunStatus = ChannelRunStatus.PENDING
    source_paths: List[str] = Field(default_factory=list)
    current_run_id: Optional[str] = None
    runs: List[ChannelRunHistory] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class AuditEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    actor: str            # user id / "agent" / "machine"
    action: str           # eg "rule.update" / "file.replace" / "channel.run"
    target: Optional[str] = None
    detail: Dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now_iso)


class TaskState(BaseModel):
    task_id: str
    period: Optional[str] = None
    created_by: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    current_step: PipelineStep = PipelineStep.CREATED

    channels: Dict[str, ChannelState] = Field(default_factory=dict)
    audit: List[AuditEvent] = Field(default_factory=list)

    created_at: str = Field(default_factory=utc_now_iso)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None

    error: Optional[str] = None
    logs: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StateManager:
    @staticmethod
    def get_task_dir(task_id: str) -> Path:
        return get_task_dir(task_id)

    @staticmethod
    def get_state_file(task_id: str) -> Path:
        return get_task_state_path(task_id)

    @staticmethod
    def save_state(state: TaskState) -> None:
        # Auto-recompute duration / completed_at on terminal states.
        if state.started_at:
            if (
                state.current_step in {PipelineStep.COMPLETED, PipelineStep.FAILED}
                and not state.completed_at
            ):
                state.completed_at = utc_now_iso()
            state.duration_seconds = compute_duration_seconds(
                state.started_at, state.completed_at
            )

        task_dir = StateManager.get_task_dir(state.task_id)
        task_dir.mkdir(parents=True, exist_ok=True)
        state_file = StateManager.get_state_file(state.task_id)
        temp_file = state_file.with_suffix(f".tmp.{uuid.uuid4().hex}")
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(state.model_dump_json(indent=2))

        TaskRepo.upsert_from_state(state)

        last_error: Optional[OSError] = None
        for _ in range(20):
            try:
                temp_file.replace(state_file)
                return
            except PermissionError as e:
                last_error = e
                time.sleep(0.02)

        try:
            if temp_file.exists():
                temp_file.unlink()
        except OSError:
            pass

        if last_error:
            raise last_error

    @staticmethod
    def load_state(task_id: str) -> Optional[TaskState]:
        state_file = StateManager.get_state_file(task_id)
        if not state_file.exists():
            return None
        last_error: Optional[Exception] = None
        for _ in range(20):
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return TaskState(**data)
            except (json.JSONDecodeError, PermissionError) as e:
                last_error = e
                time.sleep(0.02)
        if last_error:
            raise last_error
        return None
