"""Task creation, multi-channel upload, and the auto-classifier zip endpoint.

Endpoints (mounted under ``/api/pipeline``):

    POST /tasks                      — create a task (empty)
    POST /tasks/{tid}/upload         — multi-channel form upload (per-channel field)
    POST /tasks/{tid}/upload-zip-auto — single outer archive, auto-classify
    POST /tasks/{tid}/channels/{cid}/upload-zip-replace — replace one channel's extracted tree from an archive
    POST /tasks/{tid}/channels/{cid}/clear-extracted — delete extracted/{cid}/ for one channel
    GET  /tasks/{tid}/classification  — read latest classification

Files land at ``data/tasks/{tid}/raw/...`` (originals) and
``data/tasks/{tid}/extracted/{channel}/...`` (after classification).
"""
from __future__ import annotations

import shutil
import uuid
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, File, HTTPException, Header, Request, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from server.core.channel_classifier import (
    CHANNEL_CATALOG,
    CHANNEL_ID_UNKNOWN,
    ChannelDef,
    classify_extracted_root,
    classify_name,
    get_channel_def,
    list_channel_defs,
    summarize_classification,
)
from server.core.paths import (
    get_task_dir,
    get_task_extracted_dir,
    get_task_raw_dir,
    is_extracted_rel_path_parse_candidate,
)
from server.core.pipeline_state import (
    AuditEvent,
    ChannelState,
    PipelineStep,
    StateManager,
    TaskState,
    TaskStatus,
    utc_now_iso,
)
from server.core.task_logger import task_log
from server.core.task_repo import TaskRepo
from server.core.zip_extractor import (
    extract_archive,
    is_supported_archive,
    repair_rel_path_mojibake,
)

router = APIRouter()


# --------- request / response models ---------


class CreateTaskRequest(BaseModel):
    period: Optional[str] = Field(None, description="期次，例如 202602")
    note: Optional[str] = None


class CreateTaskResponse(BaseModel):
    task_id: str


class UploadResponse(BaseModel):
    task_id: str
    saved_files: list[str]
    channels: dict[str, dict]


# --------- helpers ---------


def _user_from_headers(
    user_id_hdr: Optional[str],
    user_email_hdr: Optional[str],
) -> Optional[str]:
    if user_id_hdr:
        return f"{user_id_hdr}{f' <{user_email_hdr}>' if user_email_hdr else ''}"
    if user_email_hdr:
        return user_email_hdr
    return None


def _get_or_404(task_id: str) -> TaskState:
    state = StateManager.load_state(task_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return state


def _save_upload(target_dir: Path, file: UploadFile) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(file.filename or f"upload-{uuid.uuid4().hex}").name
    out_path = target_dir / safe_name
    if out_path.exists():
        stem = out_path.stem
        suffix = out_path.suffix
        out_path = target_dir / f"{stem}-{uuid.uuid4().hex[:6]}{suffix}"
    with open(out_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return out_path


def _maybe_extract_into(saved: Path, extract_dir: Path) -> list[Path]:
    """If ``saved`` is an archive, extract it and return list of extracted file paths.
    Otherwise return [saved] (file is already in place)."""
    if is_supported_archive(saved.name):
        extract_dir.mkdir(parents=True, exist_ok=True)
        extract_archive(saved, extract_dir)
        return [extract_dir / p for p in extract_archive_listing(extract_dir)]
    return [saved]


def extract_archive_listing(root: Path) -> list[str]:
    out: list[str] = []
    if not root.exists():
        return out
    for p in sorted(root.rglob("*")):
        if p.is_file() and not p.name.startswith("."):
            out.append(str(p.relative_to(root)))
    return out


def _ensure_channel_state(state: TaskState, channel_id: str) -> ChannelState:
    if channel_id in state.channels:
        return state.channels[channel_id]

    matched: Optional[ChannelDef] = None
    for d in CHANNEL_CATALOG:
        if d.channel_id == channel_id:
            matched = d
            break
    display_name = matched.display_name if matched else channel_id
    entry_type = matched.entry_type if matched else "unknown"
    ch = ChannelState(
        channel_id=channel_id,
        display_name=display_name,
        entry_type=entry_type,
    )
    state.channels[channel_id] = ch
    return ch


def _add_audit(
    state: TaskState,
    *,
    actor: str,
    action: str,
    target: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    state.audit.append(
        AuditEvent(actor=actor, action=action, target=target, detail=detail or {})
    )


_EXCEL_SUFFIXES_SPECIAL = frozenset({".xlsx", ".xls", ".xlsm"})
_RX_SPECIAL_ACH = (
    re.compile(r"ach[\s_-]*return", re.IGNORECASE),
    re.compile(r"achreturn", re.IGNORECASE),
    re.compile(r"ach.*退款", re.IGNORECASE),
    re.compile(r"ach.*退票", re.IGNORECASE),
    re.compile(r"退款.*ach", re.IGNORECASE),
    re.compile(r"ach.*退款", re.IGNORECASE),
)
_RX_SPECIAL_TRANSFER = (
    re.compile(r"内转", re.IGNORECASE),
    re.compile(r"transfer", re.IGNORECASE),
    re.compile(r"fundtransfer", re.IGNORECASE),
    re.compile(r"channel[-_ ]?settle", re.IGNORECASE),
)
_RX_SPECIAL_OP_INCOMING = (
    re.compile(r"op\s*表", re.IGNORECASE),
    re.compile(r"op.*入账", re.IGNORECASE),
    re.compile(r"op.*incoming", re.IGNORECASE),
    re.compile(r"福贸入账", re.IGNORECASE),
    re.compile(r"入账.*bu", re.IGNORECASE),
    re.compile(r"入账表.*bu", re.IGNORECASE),
)
_RX_SPECIAL_OP_REFUND = (
    re.compile(r"op.*退票", re.IGNORECASE),
    re.compile(r"主站退票表.*bu", re.IGNORECASE),
    re.compile(r"主站.*退票", re.IGNORECASE),
    re.compile(r"b2b退票表.*bu", re.IGNORECASE),
    re.compile(r"b2b.*退票", re.IGNORECASE),
    re.compile(r"b2b.*bu", re.IGNORECASE),
)


def _rx_hit(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(rx.search(text) for rx in patterns)


def _normalize_special_relpath(rel_path: str) -> str:
    """去除外层包装目录（如「3月数据/特殊渠道/」），保留特殊渠道内相对路径。"""
    s = rel_path.replace("\\", "/").strip()
    parts = [p for p in s.split("/") if p]
    if not parts:
        return s
    for i, p in enumerate(parts):
        if p == "特殊渠道":
            tail = parts[i + 1:]
            return "/".join(tail) if tail else parts[-1]
    return s


def _special_targets_for_relpath(rel_path: str) -> list[str]:
    """按目录+文件名判定 special 子渠道，文件名优先于所在文件夹名。

    规则优先级：
    1. OP 退票 / OP 入账：路径整体匹配
    2. 内转（transfer）：**以文件名为准**，即使位于 "Ach return/" 文件夹内，
       文件名含"内转"的也必须路由到 special_transfer，不因父目录名被屏蔽。
    3. ACH：文件名未命中内转时，才按路径判定 special_ach_refund。
    4. 兜底：文件夹名即渠道标识。
    """
    s = _normalize_special_relpath(rel_path)
    lower = s.lower()
    # 末段文件名，用于覆盖父目录名的影响
    filename = s.rsplit("/", 1)[-1] if "/" in s else s
    targets: list[str] = []

    if _rx_hit(s, _RX_SPECIAL_OP_REFUND):
        targets.append("special_op_refund")
    # OP 入账要排除退票表，避免「入账.*bu」误吃主站退票 BU
    if _rx_hit(s, _RX_SPECIAL_OP_INCOMING) and "退票" not in s:
        targets.append("special_op_incoming")

    # 内转：以文件名末段为准（优先），避免「Ach return/内转.xlsx」因父目录含 ACH 关键字
    # 而被屏蔽，导致 special_transfer 始终为空。
    if _rx_hit(filename, _RX_SPECIAL_TRANSFER) and not _rx_hit(filename, _RX_SPECIAL_ACH):
        if "op表" not in lower and "/op/" not in lower:
            targets.append("special_transfer")
    elif _rx_hit(s, _RX_SPECIAL_TRANSFER) and not _rx_hit(s, _RX_SPECIAL_ACH):
        if "op表" not in lower and "/op/" not in lower:
            targets.append("special_transfer")

    # ACH：仅在文件名本身未命中内转时才按路径判定，防止「内转.xlsx」被父目录误归 ACH
    if not _rx_hit(filename, _RX_SPECIAL_TRANSFER):
        if _rx_hit(s, _RX_SPECIAL_ACH):
            targets.append("special_ach_refund")
        # 兜底：目录名本身即子渠道标识
        if "ach return/" in lower or "/ach return/" in lower:
            targets.append("special_ach_refund")

    # 兜底：op表/内转 文件夹名即子渠道标识
    if "op表/" in lower or "/op表/" in lower:
        if "退票" in s:
            targets.append("special_op_refund")
        else:
            targets.append("special_op_incoming")
    # 精确匹配目录名为「内转」（避免「Ach return退款+内转/」中末段含"内转"触发误判）
    if re.search(r"(?:^|/)内转/", s):
        targets.append("special_transfer")

    return list(dict.fromkeys(targets))


def _rescan_channel_extracted_sources(state: TaskState, task_id: str, channel_id: str) -> None:
    ch = _ensure_channel_state(state, channel_id)
    root = get_task_extracted_dir(task_id, channel_id)
    if not root.exists():
        ch.source_paths = []
    else:
        ch.source_paths = sorted(
            str(p)
            for p in root.rglob("*")
            if p.is_file()
            and is_extracted_rel_path_parse_candidate(p.relative_to(root).as_posix())
        )
    for r in ch.runs:
        r.is_dirty = True


def _clear_channel_extracted(
    *,
    task_id: str,
    state: TaskState,
    channel_id: str,
) -> int:
    """删除 extracted/{channel_id} 并清空 state source_paths，返回删除文件数。"""
    root = get_task_extracted_dir(task_id, channel_id)
    removed = 0
    if root.exists():
        removed = sum(1 for p in root.rglob("*") if p.is_file())
        shutil.rmtree(root, ignore_errors=True)
    ch = _ensure_channel_state(state, channel_id)
    ch.source_paths = []
    for r in ch.runs:
        r.is_dirty = True
    return removed


def _copy_into_channel_with_relpath(
    *,
    task_id: str,
    channel_id: str,
    source_file: Path,
    rel_path: str,
) -> bool:
    dest_root = get_task_extracted_dir(task_id, channel_id)
    rel = _normalize_special_relpath(rel_path)
    dest = dest_root / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest = dest.with_name(f"{dest.stem}-{uuid.uuid4().hex[:6]}{dest.suffix}")
    shutil.copy2(source_file, dest)
    return True


def _collect_special_routes(
    source_root: Path,
) -> tuple[list[tuple[Path, str, list[str]]], list[str]]:
    """从目录中收集 special 路由结果：返回 (命中路由, 未匹配文件列表)。"""
    routes: list[tuple[Path, str, list[str]]] = []
    unmatched: list[str] = []
    for p in sorted(source_root.rglob("*")):
        if not p.is_file() or p.suffix.lower() not in _EXCEL_SUFFIXES_SPECIAL:
            continue
        rel = p.relative_to(source_root).as_posix()
        if not is_extracted_rel_path_parse_candidate(rel):
            continue
        rel_norm = _normalize_special_relpath(rel)
        targets = _special_targets_for_relpath(rel_norm)
        if targets:
            routes.append((p, rel_norm, targets))
        else:
            unmatched.append(rel_norm)
    return routes, unmatched


def _fan_out_special_ach_refund_to_siblings(task_id: str, state: TaskState) -> dict[str, int]:
    """总览「Ach return·内转」zip 只解压到 ``special_ach_refund``；内转/OP parser 只扫各自目录。

    解压后按与 ``Special*Parser`` 相同的文件名规则，将匹配文件**复制**到
    ``special_transfer`` / ``special_op_incoming`` / ``special_op_refund``（保留相对路径），
    以便各子渠道执行与分类统计正确。
    """
    ach_root = get_task_extracted_dir(task_id, "special_ach_refund")
    if not ach_root.is_dir():
        return {}

    sibling_ids = ("special_transfer", "special_op_incoming", "special_op_refund")
    for cid in sibling_ids:
        _clear_channel_extracted(task_id=task_id, state=state, channel_id=cid)

    counts: dict[str, int] = {}
    for src in ach_root.rglob("*"):
        if not src.is_file() or src.suffix.lower() not in _EXCEL_SUFFIXES_SPECIAL:
            continue
        try:
            rel = src.relative_to(ach_root).as_posix()
        except ValueError:
            continue
        if not is_extracted_rel_path_parse_candidate(rel):
            continue

        for cid in _special_targets_for_relpath(rel):
            if cid == "special_ach_refund":
                continue
            _copy_into_channel_with_relpath(
                task_id=task_id,
                channel_id=cid,
                source_file=src,
                rel_path=rel,
            )
            counts[cid] = counts.get(cid, 0) + 1

    for cid in sibling_ids:
        if counts.get(cid, 0) > 0:
            _rescan_channel_extracted_sources(state, task_id, cid)

    if counts:
        parts = [f"{k}←{v}个" for k, v in sorted(counts.items())]
        task_log(
            task_id,
            "special_ach_refund 压缩包内已按文件名同步至 sibling 渠道："
            + ", ".join(parts)
            + "（各渠道可分别执行）",
        )
    return counts


def _fan_out_special_unknown_to_channels(task_id: str, state: TaskState) -> dict[str, int]:
    """外层整包归类后，unknown 下的 special 文件补路由到各 special 子渠道。"""
    unknown_root = get_task_extracted_dir(task_id, CHANNEL_ID_UNKNOWN)
    if not unknown_root.is_dir():
        return {}
    counts: dict[str, int] = {}
    routes, _unmatched = _collect_special_routes(unknown_root)
    for src, rel, targets in routes:
        for cid in targets:
            _copy_into_channel_with_relpath(
                task_id=task_id,
                channel_id=cid,
                source_file=src,
                rel_path=rel,
            )
            counts[cid] = counts.get(cid, 0) + 1
    for cid in ("special_transfer", "special_ach_refund", "special_op_incoming", "special_op_refund"):
        if counts.get(cid, 0) > 0:
            _rescan_channel_extracted_sources(state, task_id, cid)
    if counts:
        parts = [f"{k}←{v}个" for k, v in sorted(counts.items())]
        task_log(
            task_id,
            "upload-zip-auto：unknown 中 special 文件已按名称补路由："
            + ", ".join(parts),
        )
    return counts


# --------- endpoints ---------


@router.get("/channels/catalog")
async def list_channel_catalog() -> dict:
    """Static metadata for the seven canonical channels (used by frontend cards)."""
    return {
        "channels": [
            {
                "channel_id": d.channel_id,
                "display_name": d.display_name,
                "entry_type": d.entry_type,
                "hint": d.hint,
            }
            for d in list_channel_defs()
        ],
    }


@router.post("/tasks", response_model=CreateTaskResponse)
async def create_task(
    payload: CreateTaskRequest,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> CreateTaskResponse:
    task_id = uuid.uuid4().hex
    state = TaskState(
        task_id=task_id,
        period=payload.period,
        created_by=_user_from_headers(x_pecause_user_id, x_pecause_user_email),
        status=TaskStatus.PENDING,
        current_step=PipelineStep.CREATED,
    )
    if payload.note:
        state.metadata["note"] = payload.note

    _add_audit(
        state,
        actor=state.created_by or "anonymous",
        action="task.create",
        target=task_id,
        detail={"period": payload.period},
    )

    StateManager.save_state(state)
    task_log(task_id, f"Task created. period={payload.period}")
    TaskRepo.append_event(
        task_id, "task.created", payload={"period": payload.period}
    )
    return CreateTaskResponse(task_id=task_id)


@router.post("/tasks/{task_id}/upload-zip-auto")
async def upload_zip_auto(
    task_id: str,
    request: Request,
    file: UploadFile = File(..., description="Outer archive (.zip/.7z/.rar)"),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Single outer archive: extract -> auto-classify top-level entries by name."""
    state = _get_or_404(task_id)
    if not is_supported_archive(file.filename or ""):
        raise HTTPException(
            status_code=400, detail="Auto-zip endpoint accepts .zip / .7z / .rar only"
        )

    raw_dir = get_task_raw_dir(task_id)
    extracted_dir = get_task_extracted_dir(task_id)

    saved = _save_upload(raw_dir, file)
    task_log(task_id, f"Saved outer archive: {saved.name}")

    # Stage 1: extract everything to a sandbox under extracted/_outer
    sandbox = extracted_dir / "_outer"
    sandbox.mkdir(parents=True, exist_ok=True)
    try:
        extract_archive(saved, sandbox)
    except Exception as e:
        task_log(task_id, f"Outer archive extract failed: {e}")
        errno = getattr(e, "errno", None)
        if errno == 28 or (
            isinstance(e, OSError)
            and getattr(e, "errno", None) == 28
        ) or "No space left on device" in str(e):
            detail = (
                "解压失败：磁盘空间已满（No space left on device）。"
                "请清理本机磁盘，或将环境变量 PIPELINE_DATA_DIR 指定到有足够剩余空间的目录后再试。"
            )
        else:
            detail = f"解压失败: {e}"
        raise HTTPException(status_code=400, detail=detail) from e

    # Stage 2: classify top-level entries
    groups = classify_extracted_root(sandbox)

    # Stage 3: move top-level entries into channel buckets
    moved: list[str] = []
    for cid, group in groups.items():
        if not group.items:
            continue
        target_channel_dir = get_task_extracted_dir(task_id, cid)
        target_channel_dir.mkdir(parents=True, exist_ok=True)
        ch = _ensure_channel_state(state, cid)
        for it in group.items:
            dest = target_channel_dir / it.path.name
            if dest.exists():
                dest = target_channel_dir / f"{it.path.stem}-{uuid.uuid4().hex[:6]}{it.path.suffix}"
            shutil.move(str(it.path), str(dest))
            ch.source_paths.append(str(dest))
            moved.append(str(dest.relative_to(extracted_dir)))

    special_fan_out = _fan_out_special_unknown_to_channels(task_id, state)

    # Stage 4: clean sandbox (it should be empty if everything was top-level)
    try:
        shutil.rmtree(sandbox, ignore_errors=True)
    except OSError:
        pass

    state.current_step = PipelineStep.CLASSIFYING
    _add_audit(
        state,
        actor=_user_from_headers(x_pecause_user_id, x_pecause_user_email)
        or state.created_by
        or "anonymous",
        action="task.upload_zip_auto",
        target=task_id,
        detail={
            "archive": saved.name,
            "moved_count": len(moved),
            "special_fan_out": special_fan_out or None,
        },
    )
    StateManager.save_state(state)
    task_log(task_id, f"Auto-classified {len(moved)} entries into channels")
    TaskRepo.append_event(
        task_id,
        "files.auto_classified",
        payload={"archive": saved.name, "moved_count": len(moved)},
    )

    return JSONResponse(
        {
            "task_id": task_id,
            "archive": saved.name,
            "moved": moved,
            "channels": summarize_classification(groups),
        }
    )


@router.post("/tasks/{task_id}/channels/{channel_id}/upload-zip-replace")
async def upload_channel_zip_replace(
    task_id: str,
    channel_id: str,
    file: UploadFile = File(..., description="Archive to extract into this channel (.zip/.7z/.rar)"),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Replace ``extracted/{channel_id}/`` entirely after extracting one archive.

    Existing files under that channel are removed. All runs for the channel are marked ``is_dirty``.
    """
    state = _get_or_404(task_id)
    if get_channel_def(channel_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown channel_id: {channel_id}")
    if not is_supported_archive(file.filename or ""):
        raise HTTPException(
            status_code=400, detail="仅支持压缩包：.zip / .7z / .rar"
        )

    raw_dir = get_task_raw_dir(task_id)
    extracted_root = get_task_extracted_dir(task_id)
    saved = _save_upload(raw_dir / channel_id, file)
    task_log(task_id, f"Channel zip replace saved: {channel_id}/{saved.name}")

    staging = extracted_root / f".replace-{channel_id}-{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=True)
    try:
        extract_archive(saved, staging)
    except Exception as e:
        shutil.rmtree(staging, ignore_errors=True)
        task_log(task_id, f"Channel zip extract failed ({channel_id}): {e}")
        errno = getattr(e, "errno", None)
        if errno == 28 or (
            isinstance(e, OSError) and getattr(e, "errno", None) == 28
        ) or "No space left on device" in str(e):
            detail = (
                "解压失败：磁盘空间已满（No space left on device）。"
                "请清理本机磁盘，或将 PIPELINE_DATA_DIR 指向有足够空间的目录后再试。"
            )
        else:
            detail = f"解压失败: {e}"
        raise HTTPException(status_code=400, detail=detail) from e

    extracted_files = [
        p
        for p in staging.rglob("*")
        if p.is_file()
        and is_extracted_rel_path_parse_candidate(p.relative_to(staging).as_posix())
    ]
    if not extracted_files:
        shutil.rmtree(staging, ignore_errors=True)
        raise HTTPException(status_code=400, detail="压缩包解压后没有可用文件")

    if channel_id in ("special_ach_refund", "special_op_incoming"):
        routes, unmatched = _collect_special_routes(staging)
        if not routes:
            shutil.rmtree(staging, ignore_errors=True)
            raise HTTPException(
                status_code=400,
                detail="压缩包内未识别到特殊渠道可用文件（请检查 Ach return/op表/内转/OP 命名）",
            )
        if unmatched:
            sample = ", ".join(unmatched[:8])
            suffix = " …" if len(unmatched) > 8 else ""
            shutil.rmtree(staging, ignore_errors=True)
            raise HTTPException(
                status_code=400,
                detail=(
                    "压缩包包含未识别文件名，请按规范命名后重试。"
                    f" 未识别 {len(unmatched)} 个：{sample}{suffix}"
                ),
            )

        # 单独上传支持双子渠道混合包：先清空所有 special 子渠道，再按命中目标重建。
        for cid in (
            "special_transfer",
            "special_ach_refund",
            "special_op_incoming",
            "special_op_refund",
        ):
            _clear_channel_extracted(task_id=task_id, state=state, channel_id=cid)
        for src, rel, targets in routes:
            for cid in targets:
                _copy_into_channel_with_relpath(
                    task_id=task_id,
                    channel_id=cid,
                    source_file=src,
                    rel_path=rel,
                )
        shutil.rmtree(staging, ignore_errors=True)
        for cid in (
            "special_transfer",
            "special_ach_refund",
            "special_op_incoming",
            "special_op_refund",
        ):
            _rescan_channel_extracted_sources(state, task_id, cid)
        fan_out = {
            cid: len(state.channels.get(cid).source_paths if state.channels.get(cid) else [])
            for cid in ("special_transfer", "special_ach_refund", "special_op_incoming", "special_op_refund")
        }
        state.current_step = PipelineStep.UPLOADING
        actor = (
            _user_from_headers(x_pecause_user_id, x_pecause_user_email)
            or state.created_by
            or "anonymous"
        )
        _add_audit(
            state,
            actor=actor,
            action="task.upload_channel_zip_replace",
            target=f"{task_id}/{channel_id}",
            detail={
                "archive": saved.name,
                "special_reallocated": fan_out,
            },
        )
        StateManager.save_state(state)
        task_log(
            task_id,
            f"Reallocated special channels from archive ({saved.name})",
        )
        TaskRepo.append_event(
            task_id,
            "files.channel_zip_replaced",
            channel_id=channel_id,
            payload={"archive": saved.name, "special_reallocated": fan_out},
        )
        return JSONResponse(
            {
                "task_id": task_id,
                "channel_id": channel_id,
                "archive": saved.name,
                "file_count": fan_out.get(channel_id, 0),
                "special_reallocated": fan_out,
            }
        )

    ch_dest = get_task_extracted_dir(task_id, channel_id)
    old_backup: Path | None = None
    if ch_dest.exists():
        old_backup = extracted_root / f".zip-replace-old-{channel_id}-{uuid.uuid4().hex}"
        try:
            ch_dest.rename(old_backup)
        except OSError:
            shutil.rmtree(ch_dest, ignore_errors=True)
            old_backup = None

    try:
        staging.rename(ch_dest)
    except OSError:
        # shutil.move nests ``staging`` inside ``ch_dest`` when ``ch_dest`` exists as a directory.
        if ch_dest.exists():
            shutil.rmtree(ch_dest, ignore_errors=True)
        shutil.move(str(staging), str(ch_dest))
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)

    if old_backup is not None and old_backup.exists():
        shutil.rmtree(old_backup, ignore_errors=True)

    ch = _ensure_channel_state(state, channel_id)
    ch.source_paths = sorted(
        str(p)
        for p in ch_dest.rglob("*")
        if p.is_file()
        and is_extracted_rel_path_parse_candidate(p.relative_to(ch_dest).as_posix())
    )
    for r in ch.runs:
        r.is_dirty = True

    fan_out: dict[str, int] = {}
    if channel_id == "special_ach_refund":
        fan_out = _fan_out_special_ach_refund_to_siblings(task_id, state)

    state.current_step = PipelineStep.UPLOADING
    actor = (
        _user_from_headers(x_pecause_user_id, x_pecause_user_email)
        or state.created_by
        or "anonymous"
    )
    _add_audit(
        state,
        actor=actor,
        action="task.upload_channel_zip_replace",
        target=f"{task_id}/{channel_id}",
        detail={
            "archive": saved.name,
            "file_count": len(ch.source_paths),
            "special_fan_out": fan_out or None,
        },
    )
    StateManager.save_state(state)
    task_log(
        task_id,
        f"Replaced extracted/{channel_id} from archive ({len(ch.source_paths)} files)",
    )
    TaskRepo.append_event(
        task_id,
        "files.channel_zip_replaced",
        channel_id=channel_id,
        payload={"archive": saved.name, "file_count": len(ch.source_paths)},
    )

    return JSONResponse(
        {
            "task_id": task_id,
            "channel_id": channel_id,
            "archive": saved.name,
            "file_count": len(ch.source_paths),
        }
    )


_CLEARABLE_CHANNEL_IDS = frozenset(d.channel_id for d in list_channel_defs()) | {
    CHANNEL_ID_UNKNOWN
}


@router.post("/tasks/{task_id}/channels/{channel_id}/clear-extracted")
async def clear_single_channel_extracted(
    task_id: str,
    channel_id: str,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """删除 ``extracted/{channel_id}/`` 下所有文件（仅该渠道）。"""
    if channel_id not in _CLEARABLE_CHANNEL_IDS:
        raise HTTPException(status_code=404, detail=f"Unknown or non-clearable channel_id: {channel_id}")
    state = _get_or_404(task_id)
    removed = _clear_channel_extracted(task_id=task_id, state=state, channel_id=channel_id)
    state.current_step = PipelineStep.UPLOADING
    actor = (
        _user_from_headers(x_pecause_user_id, x_pecause_user_email)
        or state.created_by
        or "anonymous"
    )
    _add_audit(
        state,
        actor=actor,
        action="task.clear_channel_extracted",
        target=f"{task_id}/{channel_id}",
        detail={"removed_files": removed},
    )
    StateManager.save_state(state)
    task_log(task_id, f"Cleared extracted/{channel_id}/ ({removed} files)")
    TaskRepo.append_event(
        task_id,
        "files.channel_cleared",
        channel_id=channel_id,
        payload={"removed_files": removed},
    )
    return JSONResponse(
        {
            "task_id": task_id,
            "channel_id": channel_id,
            "removed_files": removed,
        }
    )


@router.post("/tasks/{task_id}/clear-uploads")
async def clear_task_uploads(
    task_id: str,
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """清空该任务所有已上传来源文件（extracted/*），用于重新上传。"""
    state = _get_or_404(task_id)
    extracted_root = get_task_extracted_dir(task_id)
    removed_files = 0
    if extracted_root.exists():
        removed_files = sum(1 for p in extracted_root.rglob("*") if p.is_file())
        shutil.rmtree(extracted_root, ignore_errors=True)
    extracted_root.mkdir(parents=True, exist_ok=True)

    for cid in list(state.channels.keys()):
        ch = _ensure_channel_state(state, cid)
        ch.source_paths = []
        for r in ch.runs:
            r.is_dirty = True

    state.current_step = PipelineStep.UPLOADING
    actor = (
        _user_from_headers(x_pecause_user_id, x_pecause_user_email)
        or state.created_by
        or "anonymous"
    )
    _add_audit(
        state,
        actor=actor,
        action="task.clear_uploads",
        target=task_id,
        detail={"removed_files": removed_files},
    )
    StateManager.save_state(state)
    task_log(task_id, f"Cleared extracted uploads ({removed_files} files removed)")
    TaskRepo.append_event(
        task_id,
        "files.cleared",
        payload={"removed_files": removed_files},
    )
    return JSONResponse(
        {"task_id": task_id, "cleared": True, "removed_files": removed_files}
    )


@router.post("/tasks/{task_id}/upload", response_model=UploadResponse)
async def upload_per_channel(
    task_id: str,
    bill_files: List[UploadFile] = File(default_factory=list),
    own_flow_files: List[UploadFile] = File(default_factory=list),
    customer_files: List[UploadFile] = File(default_factory=list),
    special_files: List[UploadFile] = File(default_factory=list),
    cn_jp_files: List[UploadFile] = File(default_factory=list),
    allocation_files: List[UploadFile] = File(default_factory=list),
    summary_files: List[UploadFile] = File(default_factory=list),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> UploadResponse:
    """Multi-field upload. Each field maps to a channel (or to several special channels)."""
    state = _get_or_404(task_id)

    saved_paths: list[str] = []

    field_to_channel: dict[str, str] = {
        "bill": "bill",
        "own_flow": "own_flow",
        "customer": "customer",
        "cn_jp": "cn_jp",
        "allocation": "allocation_base",
        "summary": "summary",
    }
    channel_buckets: dict[str, list[UploadFile]] = {
        "bill": bill_files,
        "own_flow": own_flow_files,
        "customer": customer_files,
        "cn_jp": cn_jp_files,
        "allocation": allocation_files,
        "summary": summary_files,
    }

    raw_dir = get_task_raw_dir(task_id)

    for field, files in channel_buckets.items():
        if not files:
            continue
        cid = field_to_channel[field]
        ch = _ensure_channel_state(state, cid)
        ch_raw = raw_dir / cid
        ch_extracted = get_task_extracted_dir(task_id, cid)
        _clear_channel_extracted(task_id=task_id, state=state, channel_id=cid)
        for upload in files:
            path = _save_upload(ch_raw, upload)
            saved_paths.append(str(path))
            if is_supported_archive(path.name):
                extract_archive(path, ch_extracted)
                # Resolve concrete files to add to source_paths
                for p in sorted(ch_extracted.rglob("*")):
                    if p.is_file() and not p.name.startswith("."):
                        sp = str(p)
                        if sp not in ch.source_paths:
                            ch.source_paths.append(sp)
            else:
                # Plain file -> mirror into extracted/{cid}/
                ch_extracted.mkdir(parents=True, exist_ok=True)
                dest = ch_extracted / path.name
                if not dest.exists():
                    shutil.copy2(path, dest)
                if str(dest) not in ch.source_paths:
                    ch.source_paths.append(str(dest))

    # special_files supports both:
    # 1) big mixed zip (按文件名路由到子渠道)
    # 2) single special workbook (可同时命中多个子渠道)
    if special_files:
        pending_routes: list[tuple[Path, str, list[str]]] = []
        stage_roots: list[Path] = []
        unmatched_special: list[str] = []
        for upload in special_files:
            path = _save_upload(raw_dir / "special", upload)
            saved_paths.append(str(path))
            if is_supported_archive(path.name):
                stage = get_task_extracted_dir(task_id) / f".special-upload-{uuid.uuid4().hex}"
                stage.mkdir(parents=True, exist_ok=True)
                stage_roots.append(stage)
                extract_archive(path, stage)
                routes, unmatched = _collect_special_routes(stage)
                pending_routes.extend(routes)
                unmatched_special.extend(unmatched)
            else:
                rel = _normalize_special_relpath(path.name)
                targets = _special_targets_for_relpath(rel)
                if targets:
                    pending_routes.append((path, rel, targets))
                else:
                    unmatched_special.append(rel)

        for stage in stage_roots:
            shutil.rmtree(stage, ignore_errors=True)

        if unmatched_special:
            sample = ", ".join(unmatched_special[:8])
            suffix = " …" if len(unmatched_special) > 8 else ""
            raise HTTPException(
                status_code=400,
                detail=(
                    "特殊渠道上传包含未识别文件名，请按规范命名后重试。"
                    f" 未识别 {len(unmatched_special)} 个：{sample}{suffix}"
                ),
            )

        if not pending_routes:
            raise HTTPException(
                status_code=400,
                detail="特殊渠道上传未识别到可路由文件（请检查 Ach return/op表/内转/OP 命名）",
            )

        for cid in (
            "special_transfer",
            "special_ach_refund",
            "special_op_incoming",
            "special_op_refund",
        ):
            _clear_channel_extracted(task_id=task_id, state=state, channel_id=cid)
        for p, rel, targets in pending_routes:
            for cid in targets:
                _copy_into_channel_with_relpath(
                    task_id=task_id,
                    channel_id=cid,
                    source_file=p,
                    rel_path=rel,
                )

        for cid in (
            "special_transfer",
            "special_ach_refund",
            "special_op_incoming",
            "special_op_refund",
        ):
            _rescan_channel_extracted_sources(state, task_id, cid)

    state.current_step = PipelineStep.UPLOADING
    _add_audit(
        state,
        actor=_user_from_headers(x_pecause_user_id, x_pecause_user_email)
        or state.created_by
        or "anonymous",
        action="task.upload_per_channel",
        target=task_id,
        detail={"saved_count": len(saved_paths)},
    )
    StateManager.save_state(state)
    task_log(task_id, f"Per-channel upload saved {len(saved_paths)} file(s)")
    TaskRepo.append_event(
        task_id,
        "files.uploaded_per_channel",
        payload={"saved_count": len(saved_paths)},
    )

    return UploadResponse(
        task_id=task_id,
        saved_files=saved_paths,
        channels={
            cid: {
                "display_name": ch.display_name,
                "entry_type": ch.entry_type,
                "source_count": len(ch.source_paths),
            }
            for cid, ch in state.channels.items()
        },
    )


@router.get("/tasks/{task_id}/classification")
async def get_task_classification(task_id: str) -> dict:
    """Return every catalog channel plus ``unknown``, merging disk scans with task state.

    Previously only ``state.channels`` keys were returned — uploads classified as
    ``unknown`` never appeared under catalog ids, so the UI matrix showed empty cards.
    """
    state = _get_or_404(task_id)
    extracted_root = get_task_extracted_dir(task_id)
    if not extracted_root.exists():
        return {"task_id": task_id, "channels": {}}

    def _scan_channel_files(ch_dir: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not ch_dir.exists():
            return rows
        for p in sorted(ch_dir.rglob("*")):
            if p.is_file() and not p.name.startswith("."):
                rel = str(p.relative_to(ch_dir))
                rows.append(
                    {
                        "rel_path": rel,
                        "display_rel_path": repair_rel_path_mojibake(rel),
                        "size": p.stat().st_size,
                    }
                )
        return rows

    catalog_ids = [d.channel_id for d in list_channel_defs()]
    ordered_ids = catalog_ids + [CHANNEL_ID_UNKNOWN]
    groups: Dict[str, Any] = {}

    for cid in ordered_ids:
        ch_dir = get_task_extracted_dir(task_id, cid)
        files = _scan_channel_files(ch_dir)
        ch_state = state.channels.get(cid)
        meta = get_channel_def(cid)
        if cid == CHANNEL_ID_UNKNOWN:
            display_name = "未识别"
            entry_type = "unknown"
        else:
            display_name = meta.display_name if meta else cid
            entry_type = meta.entry_type if meta else "unknown"

        if ch_state is not None:
            status_val = (
                ch_state.status.value
                if hasattr(ch_state.status, "value")
                else str(ch_state.status)
            )
            runs_count = len(ch_state.runs)
        else:
            status_val = "pending"
            runs_count = 0

        groups[cid] = {
            "display_name": display_name,
            "entry_type": entry_type,
            "status": status_val,
            "runs_count": runs_count,
            "files": files,
        }

    return {"task_id": task_id, "channels": groups}


@router.post("/tasks/{task_id}/channels/final_merge/upload-cost-summary")
async def upload_final_merge_cost_summary(
    task_id: str,
    file: UploadFile = File(...),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """直接上传成本汇总表（成本汇总_*_汇总.xlsx），绕过最终合并流程，直接用于成本出摊输入。"""
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="只接受 .xlsx 文件")
    state = _get_or_404(task_id)

    upload_dir = get_task_dir(task_id) / "channels" / "final_merge" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = Path(file.filename).name
    dest = upload_dir / safe_name
    content = await file.read()
    dest.write_bytes(content)

    now = utc_now_iso()
    actor = _user_from_headers(x_pecause_user_id, x_pecause_user_email) or state.created_by or "anonymous"
    _add_audit(
        state,
        actor=actor,
        action="task.upload_cost_summary",
        target=task_id,
        detail={"name": safe_name, "size": len(content)},
    )
    StateManager.save_state(state)
    task_log(task_id, f"上传成本汇总表：{safe_name}（{len(content)} bytes）")
    return JSONResponse({
        "task_id": task_id,
        "name": safe_name,
        "size": len(content),
        "path": str(dest),
        "uploaded_at": now,
    })


@router.post("/tasks/{task_id}/channels/allocation_base/upload-merge-base")
async def upload_allocation_merge_base(
    task_id: str,
    file: UploadFile = File(...),
    x_pecause_user_id: Optional[str] = Header(default=None),
    x_pecause_user_email: Optional[str] = Header(default=None),
) -> JSONResponse:
    """直接上传分摊基数合并结果表（收付款基数_合并_out.xlsx / 分摊基数表.xlsx），
    绕过 QuickBI / CitiHK 合并流程，直接作为成本出摊的 TEMPLATE_PATH 输入。"""
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="只接受 .xlsx 文件")
    state = _get_or_404(task_id)

    upload_dir = get_task_dir(task_id) / "channels" / "allocation_base" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    safe_name = Path(file.filename).name
    dest = upload_dir / safe_name
    content = await file.read()
    dest.write_bytes(content)

    now = utc_now_iso()
    alloc: Dict[str, Any] = state.metadata.setdefault("allocation", {})
    alloc["merge_output"] = str(dest.resolve())
    alloc["merge_output_is_upload"] = True
    alloc["merge_output_name"] = safe_name
    alloc["merge_output_uploaded_at"] = now

    actor = _user_from_headers(x_pecause_user_id, x_pecause_user_email) or state.created_by or "anonymous"
    _add_audit(
        state,
        actor=actor,
        action="task.upload_allocation_merge_base",
        target=task_id,
        detail={"name": safe_name, "size": len(content)},
    )
    StateManager.save_state(state)
    task_log(task_id, f"上传分摊基数合并表：{safe_name}（{len(content)} bytes）")
    return JSONResponse({
        "task_id": task_id,
        "name": safe_name,
        "size": len(content),
        "path": str(dest),
        "uploaded_at": now,
    })
