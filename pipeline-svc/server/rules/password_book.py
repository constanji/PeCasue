"""Encrypted password book (Fernet).

Storage: ``data/rules/password_book.enc`` (binary, Fernet token of a JSON list).
Key:     ``data/.secret.key`` — auto-generated on first use; in production set
         ``PIPELINE_SECRET`` env var (32 url-safe base64 bytes) and we use it
         directly without writing to disk.

Schema (per row):
    scope:   "bank" | "file_pattern" | "zip_name" | 渠道 ``channel_id``（如 ``bill`` / ``special_transfer``）
    pattern: 匹配串；流水线侧车密码使用固定值 ``__channel_default__`` 表示「该渠道默认密码」
    password: secret
    备注:     optional note
    expires_at: ISO date or null

Non-admin callers receive masked passwords (`••••`) when reading.
"""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from server.core.paths import get_password_book_path, get_secret_key_path


def _load_or_create_key() -> bytes:
    env_key = os.environ.get("PIPELINE_SECRET", "").strip()
    if env_key:
        try:
            return env_key.encode("utf-8")
        except Exception:
            pass
    key_path = get_secret_key_path()
    if key_path.exists():
        return key_path.read_bytes()
    try:
        from cryptography.fernet import Fernet
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "cryptography (Fernet) is required for password_book; pip install cryptography"
        ) from e
    new_key = Fernet.generate_key()
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_bytes(new_key)
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return new_key


def _fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "cryptography is required for password_book; pip install cryptography"
        ) from e
    return Fernet(_load_or_create_key())


def _mask(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return "••••" if value else ""


def load_book(*, mask: bool = True) -> List[Dict[str, Any]]:
    path = get_password_book_path()
    if not path.exists():
        return []
    try:
        f = _fernet()
        decrypted = f.decrypt(path.read_bytes())
        items = json.loads(decrypted.decode("utf-8"))
    except Exception:
        return []
    if not isinstance(items, list):
        return []
    if not mask:
        return items
    masked: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        copy = dict(it)
        if "password" in copy:
            copy["password"] = _mask(copy.get("password"))
        masked.append(copy)
    return masked


def save_book(items: List[Dict[str, Any]]) -> None:
    if not isinstance(items, list):
        raise ValueError("password book must be a list of objects")
    payload = json.dumps(items, ensure_ascii=False).encode("utf-8")
    f = _fernet()
    token = f.encrypt(payload)
    path = get_password_book_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(token)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def upsert_book(
    new_items: List[Dict[str, Any]],
    *,
    preserve_unchanged_passwords: bool = True,
) -> List[Dict[str, Any]]:
    """Save with merge semantics: if a row arrives with masked password and
    a matching (scope, pattern) exists, keep the old password.
    """
    if not preserve_unchanged_passwords:
        save_book(new_items)
        return load_book(mask=True)

    existing = load_book(mask=False)
    existing_index: Dict[tuple, str] = {}
    for it in existing:
        key = (it.get("scope"), it.get("pattern"))
        if isinstance(it.get("password"), str):
            existing_index[key] = it["password"]

    merged: List[Dict[str, Any]] = []
    for it in new_items:
        if not isinstance(it, dict):
            continue
        key = (it.get("scope"), it.get("pattern"))
        copy = dict(it)
        pw = copy.get("password")
        if pw in (None, "", "••••") and key in existing_index:
            copy["password"] = existing_index[key]
        merged.append(copy)
    save_book(merged)
    return load_book(mask=True)


def lookup_password(*, scope: Optional[str], pattern: Optional[str]) -> Optional[str]:
    """按 (scope, pattern) 查密码。

    若在规则页仅配置了「渠道 + 默认密码」，行为为 ``pattern=__channel_default__``（或空）。
    当调用方传入的 *pattern* 未命中时，会对同一 *scope* 回退尝试
    ``__channel_default__`` 与空串，便于账单/文件名等细粒度规则之上再套渠道总密码。
    """
    items = [it for it in load_book(mask=False) if isinstance(it, dict)]

    def _pw(it: Dict[str, Any]) -> Optional[str]:
        pw = it.get("password")
        return str(pw) if pw else None

    if pattern:
        for it in items:
            if scope and it.get("scope") != scope:
                continue
            if it.get("pattern") != pattern:
                continue
            p = _pw(it)
            if p:
                return p
        # 流水线「渠道默认密码」：细粒度 pattern 未命中时再试
        if scope:
            for marker in ("__channel_default__", ""):
                for it in items:
                    if it.get("scope") != scope:
                        continue
                    if it.get("pattern") != marker:
                        continue
                    p = _pw(it)
                    if p:
                        return p

    for it in items:
        if scope and it.get("scope") != scope:
            continue
        if pattern and it.get("pattern") != pattern:
            continue
        p = _pw(it)
        if p:
            return p
    return None
