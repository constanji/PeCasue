"""数据源密码安全箱：使用 Fernet 对称加密保护落库的明文密码。

设计要点：
  • 密钥优先从环境变量 ``DATASOURCE_SECRET_KEY`` 读取（base64 编码的 32 字节 key）。
  • 缺失时，自动生成一份并落盘到数据根目录（见 ``server.core.paths.get_secret_key_path``），文件权限 0o600。
  • 任何加解密失败都抛出 ``SecretBoxError``，调用方决定回退策略。
"""

from __future__ import annotations

import os
import stat
import threading
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

from server.core.paths import get_secret_key_path

_lock = threading.Lock()
_cached_fernet: Optional[Fernet] = None


class SecretBoxError(RuntimeError):
    """密码加解密相关错误的统一基类。"""


def _read_or_create_key() -> bytes:
    env_value = os.environ.get("DATASOURCE_SECRET_KEY")
    if env_value:
        return env_value.encode("ascii") if isinstance(env_value, str) else env_value

    secret_file = get_secret_key_path()
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    if secret_file.exists():
        data = secret_file.read_bytes().strip()
        if data:
            return data

    # 首次启动：生成新 key 并写入只读文件
    key = Fernet.generate_key()
    secret_file.write_bytes(key)
    try:
        os.chmod(secret_file, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    return key


def _get_fernet() -> Fernet:
    global _cached_fernet
    if _cached_fernet is not None:
        return _cached_fernet
    with _lock:
        if _cached_fernet is None:
            try:
                _cached_fernet = Fernet(_read_or_create_key())
            except (ValueError, TypeError) as exc:
                raise SecretBoxError(f"密码密钥无效：{exc}") from exc
        return _cached_fernet


def encrypt(plain: str) -> str:
    """将明文密码加密为 base64 字符串，便于直接落库。"""
    if plain is None:
        return ""
    if not isinstance(plain, str):
        plain = str(plain)
    token = _get_fernet().encrypt(plain.encode("utf-8"))
    return token.decode("ascii")


def decrypt(token: str) -> str:
    """将密文还原为明文密码；空字符串原样返回。"""
    if not token:
        return ""
    try:
        plain = _get_fernet().decrypt(token.encode("ascii"))
    except InvalidToken as exc:
        raise SecretBoxError("密码解密失败：密文已损坏或密钥不匹配") from exc
    return plain.decode("utf-8")