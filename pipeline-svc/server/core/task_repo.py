"""Read/write helpers around the ``tasks`` and ``task_lifecycle_events`` SQLite tables.

`upsert_from_state` is invoked by `StateManager.save_state` to keep the SQL
mirror in sync with the canonical ``state.json``.
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from server.core.task_db import get_connection
from server.core.paths import get_task_dir

if TYPE_CHECKING:  # avoid circular import at module load
    from server.core.pipeline_state import TaskState


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enum_storage_val(obj: Any) -> str:
    """Persist Enum members as their ``value`` (e.g. ``pending``), not ``TaskStatus.pending``."""
    if obj is None:
        return ""
    v = getattr(obj, "value", None)
    if isinstance(v, str):
        return v
    return str(obj)


def _channel_summary(state: "TaskState") -> str:
    out: Dict[str, Dict[str, Any]] = {}
    for cid, ch in state.channels.items():
        out[cid] = {
            "display_name": ch.display_name,
            "entry_type": ch.entry_type,
            "status": ch.status.value if hasattr(ch.status, "value") else str(ch.status),
            "runs": len(ch.runs),
            "current_run_id": ch.current_run_id,
        }
    return json.dumps(out, ensure_ascii=False)


class TaskRepo:
    # ---------- writes ----------

    @staticmethod
    def upsert_from_state(state: "TaskState") -> None:
        latest_log = state.logs[-1] if state.logs else None
        updated_at = _utc_now_iso()

        with get_connection() as conn:
            existing = conn.execute(
                "SELECT status, current_step FROM tasks WHERE task_id = ?",
                (state.task_id,),
            ).fetchone()

            conn.execute(
                """
                INSERT INTO tasks (
                  task_id, created_by, period, status, current_step,
                  channel_summary, created_at, started_at, completed_at,
                  duration_seconds, latest_log, error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                  created_by=excluded.created_by,
                  period=excluded.period,
                  status=excluded.status,
                  current_step=excluded.current_step,
                  channel_summary=excluded.channel_summary,
                  started_at=excluded.started_at,
                  completed_at=excluded.completed_at,
                  duration_seconds=excluded.duration_seconds,
                  latest_log=excluded.latest_log,
                  error=excluded.error,
                  updated_at=excluded.updated_at
                """,
                (
                    state.task_id,
                    state.created_by,
                    state.period,
                    _enum_storage_val(state.status),
                    _enum_storage_val(state.current_step),
                    _channel_summary(state),
                    state.created_at,
                    state.started_at,
                    state.completed_at,
                    state.duration_seconds,
                    latest_log,
                    state.error,
                    updated_at,
                ),
            )

            if existing is None:
                TaskRepo._append_event(
                    conn,
                    task_id=state.task_id,
                    event_type="created",
                    to_status=_enum_storage_val(state.status),
                    to_step=_enum_storage_val(state.current_step),
                    created_at=updated_at,
                )
            else:
                from_status = existing["status"]
                from_step = existing["current_step"]
                if from_status != _enum_storage_val(state.status):
                    TaskRepo._append_event(
                        conn,
                        task_id=state.task_id,
                        event_type="status_changed",
                        from_status=from_status,
                        to_status=_enum_storage_val(state.status),
                        from_step=from_step,
                        to_step=_enum_storage_val(state.current_step),
                        created_at=updated_at,
                    )
                elif from_step != _enum_storage_val(state.current_step):
                    TaskRepo._append_event(
                        conn,
                        task_id=state.task_id,
                        event_type="step_changed",
                        from_status=from_status,
                        to_status=_enum_storage_val(state.status),
                        from_step=from_step,
                        to_step=_enum_storage_val(state.current_step),
                        created_at=updated_at,
                    )

    @staticmethod
    def _append_event(
        conn,
        *,
        task_id: str,
        event_type: str,
        channel_id: Optional[str] = None,
        run_id: Optional[str] = None,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        from_step: Optional[str] = None,
        to_step: Optional[str] = None,
        reason_code: Optional[str] = None,
        reason_detail: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
    ) -> None:
        conn.execute(
            """INSERT INTO task_lifecycle_events (
                 task_id, channel_id, run_id, event_type,
                 from_status, to_status, from_step, to_step,
                 reason_code, reason_detail, payload_json, created_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task_id,
                channel_id,
                run_id,
                event_type,
                from_status,
                to_status,
                from_step,
                to_step,
                reason_code,
                reason_detail,
                json.dumps(payload, ensure_ascii=False) if payload else None,
                created_at or _utc_now_iso(),
            ),
        )

    @staticmethod
    def append_event(
        task_id: str,
        event_type: str,
        **kwargs: Any,
    ) -> None:
        with get_connection() as conn:
            TaskRepo._append_event(conn, task_id=task_id, event_type=event_type, **kwargs)

    # ---------- reads ----------

    @staticmethod
    def list_tasks() -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT task_id, created_by, period, status, current_step,
                          channel_summary, created_at, started_at, completed_at,
                          duration_seconds, latest_log, error, updated_at
                   FROM tasks
                   ORDER BY created_at DESC"""
            ).fetchall()
        return [TaskRepo._row_to_dict(row) for row in rows]

    @staticmethod
    def get_task(task_id: str) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            row = conn.execute(
                """SELECT task_id, created_by, period, status, current_step,
                          channel_summary, created_at, started_at, completed_at,
                          duration_seconds, latest_log, error, updated_at
                   FROM tasks WHERE task_id = ?""",
                (task_id,),
            ).fetchone()
        return TaskRepo._row_to_dict(row) if row else None

    @staticmethod
    def get_timeline(task_id: str, limit: int = 200) -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                """SELECT id, channel_id, run_id, event_type,
                          from_status, to_status, from_step, to_step,
                          reason_code, reason_detail, payload_json, created_at
                   FROM task_lifecycle_events
                   WHERE task_id = ?
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (task_id, limit),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("payload_json"):
                try:
                    d["payload"] = json.loads(d.pop("payload_json"))
                except json.JSONDecodeError:
                    d["payload"] = None
            else:
                d.pop("payload_json", None)
            out.append(d)
        return out

    @staticmethod
    def delete_task_tree(task_id: str) -> bool:
        with get_connection() as conn:
            cur = conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            removed = cur.rowcount > 0
            conn.execute("DELETE FROM task_lifecycle_events WHERE task_id = ?", (task_id,))
            conn.execute("DELETE FROM compare_runs WHERE task_id = ?", (task_id,))
        td = get_task_dir(task_id)
        if td.exists():
            shutil.rmtree(td, ignore_errors=True)
        return removed

    # ---------- helpers ----------

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        d = dict(row)
        cs_raw = d.pop("channel_summary", None)
        if cs_raw:
            try:
                d["channels"] = json.loads(cs_raw)
            except json.JSONDecodeError:
                d["channels"] = {}
        else:
            d["channels"] = {}
        return d
