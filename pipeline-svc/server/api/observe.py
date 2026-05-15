"""Phase 9 observability — KPI cards, chart datasets, recent event stream.

The implementation reads from ``tasks`` and ``task_lifecycle_events`` (both
populated by the orchestrator and the agent endpoints). All aggregation runs
in SQLite for portability — no extra services required.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from server.core.task_db import get_connection

router = APIRouter()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat()


# ---------- KPI ----------


@router.get("/observe/kpi")
def observe_kpi(window_days: int = Query(default=1, ge=1, le=30)) -> Dict[str, Any]:
    """4 KPI cards: today's tasks, success rate, avg duration, agent intervention rate."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    with get_connection() as conn:
        rows = conn.execute(
            """SELECT status, duration_seconds
               FROM tasks
               WHERE created_at >= ?""",
            (cutoff,),
        ).fetchall()
        total = len(rows)
        completed = sum(1 for r in rows if r["status"] == "completed")
        partial = sum(1 for r in rows if r["status"] == "partial")
        failed = sum(1 for r in rows if r["status"] == "failed")
        durations = [r["duration_seconds"] for r in rows if r["duration_seconds"]]
        avg_duration = round(sum(durations) / len(durations), 1) if durations else 0.0

        agent_events = conn.execute(
            """SELECT COUNT(*) AS c FROM task_lifecycle_events
               WHERE event_type = 'agent.ask' AND created_at >= ?""",
            (cutoff,),
        ).fetchone()["c"]

        run_events = conn.execute(
            """SELECT COUNT(*) AS c FROM task_lifecycle_events
               WHERE event_type = 'channel.run.end' AND created_at >= ?""",
            (cutoff,),
        ).fetchone()["c"]

    success_rate = (completed / total) if total else 0.0
    intervention_rate = (agent_events / run_events) if run_events else 0.0

    return {
        "window_days": window_days,
        "tasks_total": total,
        "tasks_completed": completed,
        "tasks_partial": partial,
        "tasks_failed": failed,
        "success_rate": round(success_rate, 4),
        "avg_duration_seconds": avg_duration,
        "agent_interventions": agent_events,
        "intervention_rate": round(intervention_rate, 4),
        "as_of": _utc_now_iso(),
    }


# ---------- Charts ----------


@router.get("/observe/charts")
def observe_charts(window_days: int = Query(default=7, ge=1, le=90)) -> Dict[str, Any]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    with get_connection() as conn:
        # Chart 1: duration distribution by channel (avg + count)
        duration_rows = conn.execute(
            """SELECT channel_id, payload_json, created_at
               FROM task_lifecycle_events
               WHERE event_type = 'channel.run.end' AND created_at >= ?""",
            (cutoff,),
        ).fetchall()
        per_channel: Dict[str, Dict[str, Any]] = {}
        for r in duration_rows:
            ch = r["channel_id"] or "unknown"
            payload = json.loads(r["payload_json"]) if r["payload_json"] else {}
            d = float(payload.get("duration_seconds") or 0)
            stat = per_channel.setdefault(ch, {"count": 0, "total_dur": 0.0, "max_dur": 0.0})
            stat["count"] += 1
            stat["total_dur"] += d
            stat["max_dur"] = max(stat["max_dur"], d)
        duration_series = sorted(
            [
                {
                    "channel": ch,
                    "count": s["count"],
                    "avg_seconds": round(s["total_dur"] / s["count"], 2) if s["count"] else 0,
                    "max_seconds": round(s["max_dur"], 2),
                }
                for ch, s in per_channel.items()
            ],
            key=lambda x: x["avg_seconds"],
            reverse=True,
        )

        # Chart 2: failure / retry rate by day
        daily_rows = conn.execute(
            """SELECT substr(created_at, 1, 10) AS day, event_type, COUNT(*) AS c
               FROM task_lifecycle_events
               WHERE created_at >= ? AND event_type IN
                 ('channel.run.start','channel.run.end','channel.run.failed','channel.run.warning')
               GROUP BY day, event_type
               ORDER BY day""",
            (cutoff,),
        ).fetchall()
        per_day: Dict[str, Dict[str, int]] = {}
        for r in daily_rows:
            day = r["day"]
            per_day.setdefault(day, {})[r["event_type"]] = r["c"]
        daily_series = []
        for day in sorted(per_day):
            ev = per_day[day]
            starts = ev.get("channel.run.start", 0)
            ends = ev.get("channel.run.end", 0)
            fails = ev.get("channel.run.failed", 0)
            warns = ev.get("channel.run.warning", 0)
            daily_series.append(
                {
                    "day": day,
                    "starts": starts,
                    "ends": ends,
                    "failures": fails,
                    "warnings": warns,
                    "failure_rate": round(fails / max(ends, 1), 4),
                }
            )

        # Chart 3: rule hit / miss (we approximate as agent.ask vs replace events
        # until parser-level rule_hit metrics land in Phase 11)
        rule_event_rows = conn.execute(
            """SELECT event_type, COUNT(*) AS c
               FROM task_lifecycle_events
               WHERE created_at >= ? AND event_type IN
                 ('rule.hit','rule.miss','agent.ask','agent.draft.proposed','audit.file.replaced')
               GROUP BY event_type""",
            (cutoff,),
        ).fetchall()
        rule_hit_series = [{"event": r["event_type"], "count": r["c"]} for r in rule_event_rows]

        # Chart 4: top 10 slow channels (already in duration_series)
        top_slow = duration_series[:10]

        # Chart 5: top 10 error files (from channel.run.failed reason_detail or payload)
        err_rows = conn.execute(
            """SELECT channel_id, reason_detail, COUNT(*) AS c
               FROM task_lifecycle_events
               WHERE event_type = 'channel.run.failed' AND created_at >= ?
               GROUP BY channel_id, reason_detail
               ORDER BY c DESC
               LIMIT 10""",
            (cutoff,),
        ).fetchall()
        top_errors = [
            {
                "channel": r["channel_id"] or "unknown",
                "detail": (r["reason_detail"] or "")[:200],
                "count": r["c"],
            }
            for r in err_rows
        ]

    return {
        "window_days": window_days,
        "duration_by_channel": duration_series,
        "daily_failure_rate": daily_series,
        "rule_events": rule_hit_series,
        "top_slow_channels": top_slow,
        "top_error_files": top_errors,
    }


# ---------- Events ----------


@router.get("/observe/events")
def observe_events(
    limit: int = Query(default=200, ge=1, le=1000),
    task_id: Optional[str] = None,
    channel_id: Optional[str] = None,
    event_type: Optional[str] = None,
) -> Dict[str, Any]:
    sql = (
        "SELECT id, task_id, channel_id, run_id, event_type, "
        "       from_status, to_status, reason_code, reason_detail, "
        "       payload_json, created_at "
        "FROM task_lifecycle_events WHERE 1=1"
    )
    params: List[Any] = []
    if task_id:
        sql += " AND task_id = ?"
        params.append(task_id)
    if channel_id:
        sql += " AND channel_id = ?"
        params.append(channel_id)
    if event_type:
        sql += " AND event_type = ?"
        params.append(event_type)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()

    events: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        try:
            d["payload"] = json.loads(d.pop("payload_json") or "null")
        except Exception:
            d["payload"] = None
        events.append(d)
    return {"events": events, "limit": limit}
