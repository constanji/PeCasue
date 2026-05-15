"""SQLite schema + connection helpers.

Schema highlights (see plan §6.1, §6.2):
    - ``tasks`` — one row per task, latest summary mirror of state.json
    - ``task_lifecycle_events`` — append-only audit (creates / step / status / retry / rule-hit / file-replace)
    - ``compare_runs`` — Phase 8 result registry
    - ``rules_versions`` — Phase 6 version log (semver-ish)
"""
from __future__ import annotations

import sqlite3

from server.core.paths import get_db_path


def get_connection() -> sqlite3.Connection:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
              task_id          TEXT PRIMARY KEY,
              created_by       TEXT,
              period           TEXT,
              status           TEXT NOT NULL,
              current_step     TEXT NOT NULL,
              channel_summary  TEXT,
              created_at       TEXT NOT NULL,
              started_at       TEXT,
              completed_at     TEXT,
              duration_seconds REAL,
              latest_log       TEXT,
              error            TEXT,
              updated_at       TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_tasks_status     ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_period     ON tasks(period);

            CREATE TABLE IF NOT EXISTS task_lifecycle_events (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              task_id       TEXT NOT NULL,
              channel_id    TEXT,
              run_id        TEXT,
              event_type    TEXT NOT NULL,
              from_status   TEXT,
              to_status     TEXT,
              from_step     TEXT,
              to_step       TEXT,
              reason_code   TEXT,
              reason_detail TEXT,
              payload_json  TEXT,
              created_at    TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_lifecycle_task_time
                ON task_lifecycle_events(task_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_lifecycle_event_type
                ON task_lifecycle_events(event_type, created_at DESC);

            CREATE TABLE IF NOT EXISTS compare_runs (
              compare_id    TEXT PRIMARY KEY,
              task_id       TEXT,
              left_ref      TEXT,
              right_ref     TEXT,
              status        TEXT NOT NULL,
              report_path   TEXT,
              summary_json  TEXT,
              created_by    TEXT,
              created_at    TEXT NOT NULL,
              completed_at  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_compare_task ON compare_runs(task_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS rules_versions (
              id           INTEGER PRIMARY KEY AUTOINCREMENT,
              kind         TEXT NOT NULL,
              version      INTEGER NOT NULL,
              snapshot_path TEXT NOT NULL,
              author       TEXT,
              note         TEXT,
              created_at   TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rules_kind_ver ON rules_versions(kind, version DESC);
            """
        )
