"""Per-task and per-channel loguru sinks.

A sink is registered lazily for each task on first log; subsequent logs are
filtered by the bound `task_id`. Channel logs land in a separate file under
``channels/{channel_id}/channel.log`` for the xterm panel.
"""
from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Optional

from loguru import logger

from server.core.paths import (
    get_task_log_path,
    get_channel_log_path,
)

_LOGGER_LOCK = Lock()
_TASK_SINKS: dict[str, int] = {}
_CHANNEL_SINKS: dict[tuple[str, str], int] = {}


def _ensure_task_sink(task_id: str) -> None:
    with _LOGGER_LOCK:
        if task_id in _TASK_SINKS:
            return
        log_path = get_task_log_path(task_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        sink_id = logger.add(
            log_path,
            format=(
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | "
                "{extra[task_id]} | {extra[channel]} | {message}"
            ),
            filter=lambda record, tid=task_id: record["extra"].get("task_id") == tid,
            encoding="utf-8",
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )
        _TASK_SINKS[task_id] = sink_id


def _ensure_channel_sink(task_id: str, channel_id: str) -> None:
    key = (task_id, channel_id)
    with _LOGGER_LOCK:
        if key in _CHANNEL_SINKS:
            return
        log_path = get_channel_log_path(task_id, channel_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        sink_id = logger.add(
            log_path,
            format=(
                "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | {message}"
            ),
            filter=lambda record, tid=task_id, ch=channel_id: (
                record["extra"].get("task_id") == tid
                and record["extra"].get("channel") == ch
            ),
            encoding="utf-8",
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )
        _CHANNEL_SINKS[key] = sink_id


def task_log(
    task_id: str,
    message: str,
    *,
    level: str = "INFO",
    channel: str = "pipeline",
) -> None:
    """Write a message to the task log (and per-channel log if channel != 'pipeline')."""
    _ensure_task_sink(task_id)
    if channel and channel != "pipeline":
        _ensure_channel_sink(task_id, channel)
    logger.bind(task_id=task_id, channel=channel).log(level, message)


def read_task_log_lines(task_id: str, offset: int = 0) -> tuple[list[str], int]:
    log_path = get_task_log_path(task_id)
    if not log_path.exists():
        return [], offset
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(offset)
        lines = f.readlines()
        return lines, f.tell()


def read_channel_log_lines(
    task_id: str, channel_id: str, offset: int = 0
) -> tuple[list[str], int]:
    log_path = get_channel_log_path(task_id, channel_id)
    if not log_path.exists():
        return [], offset
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(offset)
        lines = f.readlines()
        return lines, f.tell()
