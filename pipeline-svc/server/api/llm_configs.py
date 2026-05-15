"""大模型配置 API。

URL 前缀由 main.py 加 ``/api/pipeline`` 得到，故本路由内只写相对路径 ``/llm-configs/...``。

提供：
  • CRUD
  • activate：切换激活档案，并即时写入 ``os.environ``
  • test：发送一次最小聊天请求，探测密钥与 base_url 是否可达
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.core.llm_config_repo import (
    LLMConfigRepo,
    SUPPORTED_PROVIDERS,
)

router = APIRouter()


# ──────────────────────────────  Schemas  ──────────────────────────────


class LLMConfigCreate(BaseModel):
    name: str
    provider: str
    model_name: str
    api_key: str
    base_url: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    extra_params: Optional[str] = None
    remark: Optional[str] = None


class LLMConfigUpdate(BaseModel):
    name: Optional[str] = None
    provider: Optional[str] = None
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    extra_params: Optional[str] = None
    remark: Optional[str] = None


class LLMConfigTestRequest(BaseModel):
    """测试连接：要么传 ``config_id``（用已存档配置），要么完整 payload（新建前预测试）。"""
    config_id: Optional[int] = None
    provider: Optional[str] = None
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


# ──────────────────────────────  CRUD  ──────────────────────────────


@router.get("/llm-configs")
def list_llm_configs() -> Dict[str, Any]:
    return {
        "items": LLMConfigRepo.list_all(),
        "supported_providers": list(SUPPORTED_PROVIDERS),
    }


@router.get("/llm-configs/active")
def get_active_llm_config() -> Dict[str, Any]:
    active = LLMConfigRepo.get_active(include_secret=False)
    if not active:
        return {"active": None}
    return {"active": active}


@router.post("/llm-configs", status_code=201)
def create_llm_config(body: LLMConfigCreate) -> Dict[str, Any]:
    try:
        return LLMConfigRepo.create(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/llm-configs/{config_id}")
def get_llm_config(config_id: int) -> Dict[str, Any]:
    item = LLMConfigRepo.get(config_id)
    if not item:
        raise HTTPException(status_code=404, detail="配置不存在")
    return item


@router.put("/llm-configs/{config_id}")
def update_llm_config(config_id: int, body: LLMConfigUpdate) -> Dict[str, Any]:
    payload = {k: v for k, v in body.model_dump().items() if v is not None}
    try:
        item = LLMConfigRepo.update(config_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not item:
        raise HTTPException(status_code=404, detail="配置不存在")
    return item


@router.delete("/llm-configs/{config_id}", status_code=204)
def delete_llm_config(config_id: int) -> None:
    try:
        ok = LLMConfigRepo.delete(config_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not ok:
        raise HTTPException(status_code=404, detail="配置不存在")
    return None


@router.post("/llm-configs/{config_id}/activate")
def activate_llm_config(config_id: int) -> Dict[str, Any]:
    item = LLMConfigRepo.activate(config_id)
    if not item:
        raise HTTPException(status_code=404, detail="配置不存在")
    return item


# ──────────────────────────────  连接测试  ──────────────────────────────


def _resolve_test_payload(body: LLMConfigTestRequest) -> Dict[str, Any]:
    if body.config_id is not None:
        item = LLMConfigRepo.get(body.config_id, include_secret=True)
        if not item:
            raise HTTPException(status_code=404, detail="配置不存在")
        return {
            "provider": item["provider"],
            "model_name": item["model_name"],
            "api_key": item.get("api_key") or "",
            "base_url": item.get("base_url") or "",
            "temperature": item.get("temperature"),
            "max_tokens": item.get("max_tokens"),
        }
    if not (body.provider and body.model_name and body.api_key):
        raise HTTPException(status_code=400, detail="缺少完整连接信息（provider/model_name/api_key）")
    if body.provider.lower() not in SUPPORTED_PROVIDERS:
        raise HTTPException(status_code=400, detail=f"不支持的 provider：{body.provider}")
    return {
        "provider": body.provider.lower(),
        "model_name": body.model_name,
        "api_key": body.api_key,
        "base_url": body.base_url or "",
        "temperature": body.temperature,
        "max_tokens": body.max_tokens,
    }


def _ping_openai_compatible(payload: Dict[str, Any]) -> Dict[str, Any]:
    base = (payload.get("base_url") or "https://api.openai.com/v1").rstrip("/")
    url = f"{base}/chat/completions"
    headers = {
        "Authorization": f"Bearer {payload['api_key']}",
        "Content-Type": "application/json",
    }
    body = {
        "model": payload["model_name"],
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 8,
        "stream": False,
    }
    if payload.get("temperature") is not None:
        body["temperature"] = float(payload["temperature"])

    started = time.monotonic()
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(url, headers=headers, json=body)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if resp.status_code >= 400:
        return {
            "ok": False,
            "latency_ms": elapsed_ms,
            "message": f"HTTP {resp.status_code}：{resp.text[:300]}",
        }
    data = resp.json()
    sample = ""
    try:
        sample = data["choices"][0]["message"]["content"][:120]
    except Exception:
        sample = str(data)[:120]
    return {"ok": True, "latency_ms": elapsed_ms, "message": "连接成功", "sample": sample}


def _ping_anthropic(payload: Dict[str, Any]) -> Dict[str, Any]:
    base = (payload.get("base_url") or "https://api.anthropic.com").rstrip("/")
    url = f"{base}/v1/messages"
    headers = {
        "x-api-key": payload["api_key"],
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": payload["model_name"],
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 8,
    }

    started = time.monotonic()
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(url, headers=headers, json=body)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    if resp.status_code >= 400:
        return {
            "ok": False,
            "latency_ms": elapsed_ms,
            "message": f"HTTP {resp.status_code}：{resp.text[:300]}",
        }
    data = resp.json()
    sample = ""
    try:
        sample = data["content"][0]["text"][:120]
    except Exception:
        sample = str(data)[:120]
    return {"ok": True, "latency_ms": elapsed_ms, "message": "连接成功", "sample": sample}


@router.post("/llm-configs/test")
def test_llm_config(body: LLMConfigTestRequest) -> Dict[str, Any]:
    payload = _resolve_test_payload(body)
    provider = payload["provider"]
    try:
        if provider == "openai":
            return _ping_openai_compatible(payload)
        if provider == "anthropic":
            return _ping_anthropic(payload)
    except httpx.RequestError as exc:
        return {"ok": False, "latency_ms": 0, "message": f"网络异常：{type(exc).__name__}: {exc}"}
    except Exception as exc:
        return {"ok": False, "latency_ms": 0, "message": f"{type(exc).__name__}: {exc}"}
    raise HTTPException(status_code=400, detail=f"不支持的 provider：{provider}")