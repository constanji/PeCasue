"""Filesystem layout for pipeline-svc.

Data root precedence (first non-empty wins):
    1. ``$PIPELINE_DATA_DIR``
    2. ``$APP_DATA_ROOT`` (compat)
    3. ``<svc_root>/data``

Sub-paths follow the plan in `pecause_流水线模块设计_*.plan.md` §6.1.
"""
from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath

# Matches backups written by ``replace_channel_source_file`` / run output replace:
# ``foo.xlsx`` → ``foo.xlsx.bak.a1b2c3``
_BACKUP_NAME_TAIL_RE = re.compile(r"\.bak\.[0-9a-f]{6}$", re.IGNORECASE)

_DOTENV_LOADED = False
_PACKAGE_ROOT = Path(__file__).resolve().parent.parent  # server/
_SVC_ROOT = _PACKAGE_ROOT.parent  # pipeline-svc/
_REPO_ROOT = _SVC_ROOT.parent  # PeCause/


def _ensure_dotenv() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(_REPO_ROOT / ".env")
        load_dotenv(_SVC_ROOT / ".env")
    except Exception:
        pass
    _DOTENV_LOADED = True


def get_repo_root() -> Path:
    return _REPO_ROOT


def get_svc_root() -> Path:
    return _SVC_ROOT


def get_allocation_bundle_root() -> Path:
    """Vendored ``allocation_bundle``：内含 ``allline``（分摊基数逻辑）与 ``cost_allocation``（CitiHK 依赖）。"""
    return get_svc_root() / "vendor" / "allocation_bundle"


def get_vendored_allline_allocation_root() -> Path:
    """Vendored allline 分摊基数代码根目录（``vendor/allocation_bundle/allline``）。"""
    return get_allocation_bundle_root() / "allline"


def get_data_root() -> Path:
    _ensure_dotenv()
    override = (
        os.environ.get("PIPELINE_DATA_DIR") or os.environ.get("APP_DATA_ROOT") or ""
    ).strip()
    if override:
        return Path(override).expanduser().resolve()
    return (_SVC_ROOT / "data").resolve()


def get_db_path() -> Path:
    return get_data_root() / "tasks.db"


def get_tasks_dir() -> Path:
    return get_data_root() / "tasks"


def get_rules_dir() -> Path:
    return get_data_root() / "rules"


def get_rules_files_dir() -> Path:
    return get_rules_dir() / "files"


def get_rules_manifest_path() -> Path:
    return get_rules_dir() / "manifest.json"


def get_password_book_path() -> Path:
    return get_rules_dir() / "password_book.enc"


def get_secret_key_path() -> Path:
    return get_data_root() / ".secret.key"


def get_task_dir(task_id: str) -> Path:
    return get_tasks_dir() / task_id


def get_task_state_path(task_id: str) -> Path:
    return get_task_dir(task_id) / "state.json"


def get_task_meta_path(task_id: str) -> Path:
    return get_task_dir(task_id) / "meta.json"


def get_task_log_path(task_id: str) -> Path:
    return get_task_dir(task_id) / "task.log"


def get_task_raw_dir(task_id: str) -> Path:
    return get_task_dir(task_id) / "raw"


def get_task_extracted_dir(task_id: str, channel_id: str | None = None) -> Path:
    base = get_task_dir(task_id) / "extracted"
    return base / channel_id if channel_id else base


def get_channel_dir(task_id: str, channel_id: str) -> Path:
    return get_task_dir(task_id) / "channels" / channel_id


def get_channel_run_dir(task_id: str, channel_id: str, run_id: str) -> Path:
    return get_channel_dir(task_id, channel_id) / "runs" / run_id


def resolve_run_artifact_path(run_dir: Path, storage_name: str) -> Path | None:
    """Locate a registered run artifact on disk.

    Bill pipeline writes per-bank tables under ``midfile/`` but historically registered them
    in ``state.json`` with basename-only ``FileEntry.name``. Try ``run_dir / name`` first,
    then ``run_dir / midfile / basename`` when ``name`` contains no path separators.
    """
    if not storage_name:
        return None
    norm = storage_name.replace("\\", "/").strip()
    if not norm or norm.startswith("/"):
        return None
    parts = PurePosixPath(norm).parts
    if ".." in parts:
        return None
    root = run_dir.resolve()
    primary = (run_dir / norm).resolve()
    try:
        primary.relative_to(root)
    except ValueError:
        return None
    if primary.is_file():
        return primary
    if "/" not in norm:
        legacy = (run_dir / "midfile" / norm).resolve()
        try:
            legacy.relative_to(root)
        except ValueError:
            return None
        if legacy.is_file():
            return legacy
    return None


def get_channel_log_path(task_id: str, channel_id: str) -> Path:
    return get_channel_dir(task_id, channel_id) / "channel.log"


def get_compare_dir(task_id: str, compare_id: str) -> Path:
    return get_task_dir(task_id) / "compare" / compare_id


def get_agent_drafts_dir(task_id: str) -> Path:
    return get_task_dir(task_id) / "agent_drafts"


def is_extracted_parse_candidate(filename: str) -> bool:
    """Whether a leaf file under ``extracted/{{channel}}/`` should be parsed or counted.

    Skips dotfiles, Excel lock files (``~$…``), and in-folder replace backups
    (``*.bak.{{6hex}}`` from :func:`server.api.files.replace_channel_source_file`).
    """
    if filename.startswith("."):
        return False
    if filename.startswith("~$"):
        return False
    if _BACKUP_NAME_TAIL_RE.search(filename):
        return False
    return True


def is_extracted_rel_path_parse_candidate(rel_path: str) -> bool:
    """Whether a file under ``extracted/{{channel}}/`` should be listed / parsed / counted.

    Additionally skips macOS zip debris (``__MACOSX``) and AppleDouble ``._*`` leaves.
    """
    norm = rel_path.replace("\\", "/").strip("/")
    if not norm:
        return False
    parts = PurePosixPath(norm).parts
    if "__MACOSX" in parts:
        return False
    leaf = parts[-1]
    if leaf.startswith("._"):
        return False
    return is_extracted_parse_candidate(leaf)


def get_rules_allocation_citihk_mapping_csv_path() -> Path:
    """CitiHK 账户 mapping CSV（与同任务的 RuleStore mapping 共用）。

    优先 ``rules/files/mapping/账户对应主体分行mapping表.csv``；
    若不存在则回退 ``rules/files/allocation/citihk/mapping/…``（旧布局）。
    """
    leaf = "账户对应主体分行mapping表.csv"
    base = get_rules_files_dir()
    primary = base / "mapping" / leaf
    legacy = base / "allocation" / "citihk" / "mapping" / leaf
    if primary.is_file():
        return primary
    if legacy.is_file():
        return legacy
    return primary


def get_rules_allocation_quickbi_template_path() -> Path:
    """收付款成本分摊基数表模版（QuickBI 侧），与 allline ``分摊基数/files/quickbi`` 同名。"""
    return get_rules_files_dir() / "allocation" / "quickbi" / "收付款成本分摊基数表模版.xlsx"


def get_rules_allocation_citihk_pphk_template_path() -> Path:
    """基数 PPHK 模版（CitiHK 侧），与 allline ``分摊基数/files/citihk/mapping`` 同名。"""
    return get_rules_files_dir() / "allocation" / "citihk" / "mapping" / "基数PPHK模版.xlsx"


def get_rules_cost_allocate_workbook_path() -> Path:
    """成本分摊基数+输出模版（``cost_allocate.py`` 主输入：mapping / 入金出金 VA 基数表）。"""
    return get_rules_files_dir() / "allocation" / "成本分摊基数+输出模板.xlsx"


def ensure_data_directories() -> None:
    """Create the data root and common subdirectories on startup."""
    root = get_data_root()
    root.mkdir(parents=True, exist_ok=True)
    get_tasks_dir().mkdir(parents=True, exist_ok=True)
    get_rules_dir().mkdir(parents=True, exist_ok=True)
    get_rules_files_dir().mkdir(parents=True, exist_ok=True)
    for sub in ("mapping", "fx", "rules", "templates"):
        (get_rules_files_dir() / sub).mkdir(parents=True, exist_ok=True)
    # 分摊基数：与 allline「分摊基数/files/quickbi|citihk/mapping」布局对齐（仅存模版 xlsx）
    alloc_quickbi = get_rules_files_dir() / "allocation" / "quickbi"
    alloc_citihk_mapping = get_rules_files_dir() / "allocation" / "citihk" / "mapping"
    alloc_quickbi.mkdir(parents=True, exist_ok=True)
    alloc_citihk_mapping.mkdir(parents=True, exist_ok=True)
