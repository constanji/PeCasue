"""大模型配置仓储。

职责：
  • 在 SQLite 维护多组 LLM 档案（provider / model_name / api_key / base_url / 等）
  • API Key 用 Fernet 对称加密落库
  • 列表 / 详情接口默认对外仅返回 mask 后的密钥（前 4 + 后 3）
  • 激活档案时自动写入 ``os.environ``，使 ``server/agents/config.py:get_chat_model`` 立即生效
  • 启动迁移：若数据库为空则用 ``.env`` 当前值创建首条激活档案，保持向后兼容
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from server.core.secret_box import SecretBoxError, decrypt, encrypt
from server.core.task_db import get_connection

SUPPORTED_PROVIDERS = ("openai", "anthropic")

_LLM_CONFIGS_TABLE = """\
CREATE TABLE IF NOT EXISTS llm_configs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,
    provider         TEXT NOT NULL DEFAULT 'openai',
    model_name       TEXT NOT NULL DEFAULT 'gpt-4o',
    api_key_encrypted TEXT NOT NULL DEFAULT '',
    base_url         TEXT,
    temperature      REAL,
    max_tokens       INTEGER,
    extra_params     TEXT,
    remark           TEXT,
    is_active        INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
"""


def ensure_llm_configs_table() -> None:
    """Create the llm_configs table if it doesn't exist (called at startup)."""
    with get_connection() as conn:
        conn.executescript(_LLM_CONFIGS_TABLE)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mask_api_key(plain: str) -> str:
    """掩码 API Key：保留前 4 + 后 3，中间用 ``****`` 替代；过短则全部脱敏。"""
    if not plain:
        return ""
    if len(plain) <= 8:
        return "*" * len(plain)
    return f"{plain[:4]}****{plain[-3:]}"


def _row_to_dict(row, *, include_secret: bool = False) -> Dict[str, Any]:
    if row is None:
        return {}
    data = dict(row)
    encrypted = data.pop("api_key_encrypted", "") or ""
    plain = ""
    if encrypted:
        try:
            plain = decrypt(encrypted)
        except SecretBoxError:
            plain = ""
            data["api_key_corrupted"] = True
    data["api_key_masked"] = _mask_api_key(plain)
    data["has_api_key"] = bool(plain)
    if include_secret:
        data["api_key"] = plain
    data["is_active"] = bool(data.get("is_active"))
    return data


def _validate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    name = (payload.get("name") or "").strip()
    if not name:
        raise ValueError("名称不能为空")
    if len(name) > 80:
        raise ValueError("名称过长（≤80）")

    provider = (payload.get("provider") or "").strip().lower()
    if provider not in SUPPORTED_PROVIDERS:
        raise ValueError(
            f"不支持的 provider：{provider or '空'}（仅支持 openai / anthropic）"
        )

    model_name = (payload.get("model_name") or "").strip()
    if not model_name:
        raise ValueError("模型名 model_name 不能为空")

    base_url = (payload.get("base_url") or "").strip() or None
    remark = (payload.get("remark") or "").strip() or None
    extra_params = payload.get("extra_params")
    if extra_params is not None and not isinstance(extra_params, str):
        raise ValueError("extra_params 必须为 JSON 字符串")

    temperature = payload.get("temperature")
    if temperature in (None, ""):
        temperature_val: Optional[float] = None
    else:
        try:
            temperature_val = float(temperature)
        except (TypeError, ValueError) as exc:
            raise ValueError("temperature 必须为数值") from exc
        if not (0.0 <= temperature_val <= 2.0):
            raise ValueError("temperature 范围 0.0 - 2.0")

    max_tokens = payload.get("max_tokens")
    if max_tokens in (None, ""):
        max_tokens_val: Optional[int] = None
    else:
        try:
            max_tokens_val = int(max_tokens)
        except (TypeError, ValueError) as exc:
            raise ValueError("max_tokens 必须为整数") from exc
        if max_tokens_val <= 0 or max_tokens_val > 200_000:
            raise ValueError("max_tokens 范围 1 - 200000")

    return {
        "name": name,
        "provider": provider,
        "model_name": model_name,
        "base_url": base_url,
        "remark": remark,
        "extra_params": extra_params,
        "temperature": temperature_val,
        "max_tokens": max_tokens_val,
    }


def apply_to_env(config: Dict[str, Any]) -> None:
    """把激活配置写入 ``os.environ``。``api_key`` 字段必须是已解密的明文。"""
    if not config:
        return
    os.environ["LLM_PROVIDER"] = str(config.get("provider") or "openai")
    os.environ["LLM_MODEL_NAME"] = str(config.get("model_name") or "")
    os.environ["LLM_API_KEY"] = str(config.get("api_key") or "")
    base_url = config.get("base_url") or ""
    if base_url:
        os.environ["LLM_BASE_URL"] = str(base_url)
    else:
        os.environ.pop("LLM_BASE_URL", None)
    os.environ["PIPELINE_LLM_ENABLED"] = "1"


class LLMConfigRepo:
    @staticmethod
    def list_all() -> List[Dict[str, Any]]:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM llm_configs ORDER BY is_active DESC, name ASC"
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    @staticmethod
    def get(config_id: int, *, include_secret: bool = False) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM llm_configs WHERE id = ?", (config_id,)
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, include_secret=include_secret)

    @staticmethod
    def get_active(*, include_secret: bool = True) -> Optional[Dict[str, Any]]:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM llm_configs WHERE is_active = 1 LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return _row_to_dict(row, include_secret=include_secret)

    @staticmethod
    def create(payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = _validate_payload(payload)
        api_key = (payload.get("api_key") or "").strip()
        if not api_key:
            raise ValueError("API Key 不能为空")
        encrypted = encrypt(api_key)
        now = _now_iso()
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO llm_configs (
                       name, provider, model_name, api_key_encrypted, base_url,
                       temperature, max_tokens, extra_params, remark, is_active,
                       created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)""",
                (
                    normalized["name"],
                    normalized["provider"],
                    normalized["model_name"],
                    encrypted,
                    normalized["base_url"],
                    normalized["temperature"],
                    normalized["max_tokens"],
                    normalized["extra_params"],
                    normalized["remark"],
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM llm_configs WHERE name = ?", (normalized["name"],)
            ).fetchone()
        return _row_to_dict(row)

    @staticmethod
    def update(config_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        existing = LLMConfigRepo.get(config_id)
        if not existing:
            return None
        merged = {**existing, **payload}
        normalized = _validate_payload(merged)

        api_key = payload.get("api_key")
        encrypted_clause = ""
        params: list[Any] = [
            normalized["name"],
            normalized["provider"],
            normalized["model_name"],
            normalized["base_url"],
            normalized["temperature"],
            normalized["max_tokens"],
            normalized["extra_params"],
            normalized["remark"],
            _now_iso(),
        ]
        if api_key:
            encrypted_clause = ", api_key_encrypted = ?"
            params.append(encrypt(str(api_key).strip()))
        params.append(config_id)

        with get_connection() as conn:
            conn.execute(
                f"""UPDATE llm_configs
                       SET name = ?, provider = ?, model_name = ?, base_url = ?,
                           temperature = ?, max_tokens = ?, extra_params = ?,
                           remark = ?, updated_at = ?{encrypted_clause}
                     WHERE id = ?""",
                tuple(params),
            )
        updated = LLMConfigRepo.get(config_id, include_secret=True)
        if updated and updated.get("is_active"):
            apply_to_env(updated)
        return LLMConfigRepo.get(config_id)

    @staticmethod
    def delete(config_id: int) -> bool:
        existing = LLMConfigRepo.get(config_id)
        if not existing:
            return False
        if existing.get("is_active"):
            with get_connection() as conn:
                others = conn.execute(
                    "SELECT id FROM llm_configs WHERE id != ? ORDER BY name ASC LIMIT 1",
                    (config_id,),
                ).fetchone()
            if others is None:
                raise ValueError("不能删除唯一的激活档案；请先新建另一条配置")
            LLMConfigRepo.activate(int(others["id"]))
        with get_connection() as conn:
            cur = conn.execute("DELETE FROM llm_configs WHERE id = ?", (config_id,))
            return cur.rowcount > 0

    @staticmethod
    def activate(config_id: int) -> Optional[Dict[str, Any]]:
        target = LLMConfigRepo.get(config_id, include_secret=True)
        if not target:
            return None
        with get_connection() as conn:
            conn.execute("UPDATE llm_configs SET is_active = 0 WHERE is_active = 1")
            conn.execute("UPDATE llm_configs SET is_active = 1 WHERE id = ?", (config_id,))
        apply_to_env(target)
        return LLMConfigRepo.get(config_id)

    @staticmethod
    def bootstrap_from_env_if_empty() -> None:
        """启动迁移：若数据库为空，用 .env / os.environ 当前值创建首条激活配置。"""
        ensure_llm_configs_table()
        with get_connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM llm_configs").fetchone()
        if row and row["n"] > 0:
            active = LLMConfigRepo.get_active(include_secret=True)
            if active:
                apply_to_env(active)
            return

        api_key = os.environ.get("LLM_API_KEY", "").strip()
        if not api_key or api_key == "mock_key_for_no_env":
            return

        provider = (os.environ.get("LLM_PROVIDER") or "openai").lower()
        if provider not in SUPPORTED_PROVIDERS:
            provider = "openai"
        try:
            LLMConfigRepo.create({
                "name": "default",
                "provider": provider,
                "model_name": os.environ.get("LLM_MODEL_NAME") or "gpt-4o",
                "api_key": api_key,
                "base_url": os.environ.get("LLM_BASE_URL") or None,
                "remark": "由 .env 自动迁移创建",
            })
        except ValueError:
            return
        with get_connection() as conn:
            new_row = conn.execute(
                "SELECT id FROM llm_configs WHERE name = ? LIMIT 1", ("default",)
            ).fetchone()
        if new_row:
            LLMConfigRepo.activate(int(new_row["id"]))


def bootstrap_from_env_if_empty() -> None:
    """:meth:`LLMConfigRepo.bootstrap_from_env_if_empty` 的模块级入口，供 ``main`` 等服务启动时调用。"""
    LLMConfigRepo.bootstrap_from_env_if_empty()