"""Archive extraction with CJK filename repair.

Adapted from `pingpong-master/server/utils/zip_extractor.py`. Supports .zip,
.7z, .rar; the latter two are imported lazily to avoid hard dependency.
"""
from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path, PurePosixPath


# Known Chinese keywords that appear in pipeline filenames; presence of these
# in a candidate decoded name is a strong signal that the decoding is correct.
_KNOWN_KEYWORDS = (
    "入金", "出金", "账单", "流水", "客资", "分摊", "基数", "渠道",
    "自有", "核对", "模板", "模版", "主体", "汇款", "收付款",
    "日本", "通道", "境内", "国内",
    "inbound", "outbound",
)


def _score_zip_name(name: str) -> tuple[int, int, int, int, int, int]:
    """Higher is better. Prefer 「账单」over UTF-8→GBK mojibake (e.g. 璐﹀崟).

    Score tuple: (known_kw, cjk, -vs_compat, -weird, -replacement, len(name)).
    ``known_kw`` counts how many pipeline-relevant keywords appear, giving a strong
    boost to names like "1-2入金ads…" over their garbled equivalents like "1-2復洪مads…".
    """
    cjk = sum(1 for ch in name if "一" <= ch <= "鿿")
    weird = sum(
        1
        for ch in name
        if ch
        in "╔╗╚╝╠╣╦╩╬═│┌┐└┘├┤┬┴┼ÇüéâäàåçêëèïîìÄÅÉæÆôöòûùÿÖÜ¢£¥₧ƒáíóúñÑªº¿⌐¬½¼¡«»"
    )
    replacement = name.count("\ufffd")
    vs_compat = sum(1 for ch in name if "\ufe00" <= ch <= "\ufeff")
    known_kw = sum(1 for kw in _KNOWN_KEYWORDS if kw in name.lower())
    return (known_kw, cjk, -vs_compat, -weird, -replacement, len(name))


def _decode_legacy_zip_name(name: str) -> str:
    if not name:
        return name
    try:
        raw_bytes = name.encode("cp437")
    except UnicodeEncodeError:
        return name

    best = name
    best_score = _score_zip_name(name)
    for encoding in ("gb18030", "gbk", "utf-8"):
        try:
            decoded = raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
        if not decoded:
            continue
        decoded_score = _score_zip_name(decoded)
        if decoded_score > best_score:
            best = decoded
            best_score = decoded_score
    return best


def _gather_zip_filename_candidates(member: zipfile.ZipInfo) -> list[str]:
    """All plausible decoded paths for this central-directory entry (legacy + UTF-8 mislabels)."""
    name = member.filename
    if not name:
        return []
    seen: set[str] = set()
    out: list[str] = []

    def add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    add(name)
    add(_decode_legacy_zip_name(name))
    try:
        add(name.encode("cp437").decode("utf-8"))
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    for enc in ("latin1", "cp437"):
        try:
            raw = name.encode(enc)
        except UnicodeEncodeError:
            continue
        for dec in ("utf-8", "gb18030", "gbk"):
            try:
                add(raw.decode(dec))
            except UnicodeDecodeError:
                continue
    # UTF-8 bytes wrongly shown as GBK codepoints (common macOS / mixed-CN zip)
    try:
        add(name.encode("gbk").decode("utf-8"))
    except (UnicodeDecodeError, UnicodeEncodeError, LookupError):
        pass
    try:
        add(name.encode("gb18030").decode("utf-8"))
    except (UnicodeDecodeError, UnicodeEncodeError, LookupError):
        pass
    return out


def _repair_zip_member_filename(member: zipfile.ZipInfo) -> str:
    """Pick best filename among OEM / UTF-8 / GBK reinterpretations (score by CJK plausibility).

    Note: Even when the ZIP UTF-8 flag is set, some archivers mis-set it; we still score all
    candidates and pick the strongest."""
    candidates = _gather_zip_filename_candidates(member)
    best = member.filename or ""
    best_score = _score_zip_name(best)
    for cand in candidates:
        sc = _score_zip_name(cand)
        if sc > best_score:
            best = cand
            best_score = sc
    return best


def _zip_member_should_skip(filename: str) -> bool:
    parts = PurePosixPath(filename.replace("\\", "/")).parts
    return "__MACOSX" in parts


def _safe_member_path(filename: str) -> Path:
    parts: list[str] = []
    for part in PurePosixPath(filename).parts:
        if part in ("", ".", "/"):
            continue
        if part == "..":
            raise ValueError("Archive member contains unsafe parent path")
        parts.append(part)
    return Path(*parts)


def _extract_zip_with_fixed_names(archive_path: Path, extract_dir: Path) -> None:
    with zipfile.ZipFile(archive_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            filename = _repair_zip_member_filename(member)
            if _zip_member_should_skip(filename):
                continue

            rel_path = _safe_member_path(filename)
            target_path = extract_dir / rel_path

            if member.is_dir() or filename.endswith("/"):
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with zip_ref.open(member, "r") as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)


def extract_archive(archive_path: Path, extract_dir: Path) -> list[str]:
    """Extract a .zip / .7z / .rar archive, returning relative paths."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    extension = archive_path.suffix.lower()

    if extension == ".zip":
        _extract_zip_with_fixed_names(archive_path, extract_dir)
    elif extension == ".7z":
        try:
            import py7zr
        except ImportError as e:  # pragma: no cover
            raise RuntimeError("py7zr module is not installed for .7z support.") from e
        with py7zr.SevenZipFile(archive_path, mode="r") as z:
            z.extractall(path=extract_dir)
    elif extension == ".rar":
        try:
            import rarfile
            with rarfile.RarFile(archive_path) as rf:
                rf.extractall(extract_dir)
        except ImportError as e:  # pragma: no cover
            raise RuntimeError("rarfile module is not installed for .rar support.") from e
        except Exception as e:  # rarfile.RarCannotExec or others
            raise RuntimeError(f"Cannot extract .rar archive: {e}") from e
    else:
        raise ValueError(f"Unsupported archive format: {extension}")

    extracted_files: list[str] = []
    for root, _, files in os.walk(extract_dir):
        for file in files:
            if file.startswith("."):
                continue
            full_path = Path(root) / file
            try:
                rel_path = full_path.relative_to(extract_dir)
            except ValueError:
                continue
            rel_s = rel_path.as_posix()
            if "__MACOSX" in rel_path.parts:
                continue
            if rel_path.name.startswith("._"):
                continue
            extracted_files.append(rel_s)
    return extracted_files


def is_supported_archive(filename: str) -> bool:
    return filename.lower().endswith((".zip", ".7z", ".rar"))


def repair_path_segment_mojibake(segment: str) -> str:
    """修复路径段：常见于 ZIP 内 UTF-8 字节被按 GBK 解码（如 日本通道 → 鏃ユ湰閫氶亾）。"""
    if not segment:
        return segment
    best = segment
    best_sc = _score_zip_name(segment)
    for enc in ("gbk", "gb18030"):
        try:
            cand = segment.encode(enc).decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError, LookupError):
            continue
        sc = _score_zip_name(cand)
        if sc > best_sc:
            best, best_sc = cand, sc
    return best


def repair_rel_path_mojibake(rel_path: str) -> str:
    """仅用于展示/候选文件名评分；不改变磁盘上的真实相对路径。"""
    if not rel_path:
        return rel_path
    return "/".join(
        repair_path_segment_mojibake(p) for p in rel_path.replace("\\", "/").split("/") if p
    )
