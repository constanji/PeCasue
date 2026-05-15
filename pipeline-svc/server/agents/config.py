"""AgentScope chat-model factory for pipeline-svc.

Mirrors the pingpong-master ``server/agents/config.py`` contract so that the
already-migrated ReActAgent paths (PasswordAgent / UnknownChannelStructurer /
MapperAgent / Copilot fallback) can opt into a real LLM by setting:

    PIPELINE_LLM_ENABLED=1
    LLM_PROVIDER=openai            # or anthropic
    LLM_API_KEY=sk-...
    LLM_BASE_URL=                  # optional, e.g. for Azure / proxy
    LLM_MODEL_NAME=gpt-4o

When ``PIPELINE_LLM_ENABLED`` is unset / 0, all agents fall back to their
deterministic heuristics (rule-book lookup, keyword classifier, etc.) and
``get_chat_model()`` is never invoked. The heuristic path remains the source
of truth for unit tests and the e2e smoke script.
"""
from __future__ import annotations

import os
from typing import Any, Tuple


def _llm_config_from_env() -> dict[str, str]:
    return {
        "provider": (os.getenv("LLM_PROVIDER") or "openai").lower(),
        "api_key": (os.getenv("LLM_API_KEY") or "").strip(),
        "base_url": (os.getenv("LLM_BASE_URL") or "").strip(),
        "model_name": (os.getenv("LLM_MODEL_NAME") or "gpt-4o").strip(),
    }


def llm_enabled() -> bool:
    """Single source of truth for the kill-switch checked by every agent."""
    return (os.getenv("PIPELINE_LLM_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def get_chat_model() -> Tuple[Any, Any]:
    """Return ``(model, formatter)`` for AgentScope ReActAgent.

    Raises ``RuntimeError`` if ``PIPELINE_LLM_ENABLED`` is on but no key is
    configured — the agents catch this and fall back to heuristic mode rather
    than crashing the whole service.
    """
    if not llm_enabled():
        raise RuntimeError("PIPELINE_LLM_ENABLED is off; refusing to build LLM client")

    cfg = _llm_config_from_env()
    api_key = cfg["api_key"]
    if not api_key:
        raise RuntimeError(
            "PIPELINE_LLM_ENABLED=1 but LLM_API_KEY is empty; set it in pipeline-svc/.env"
        )

    provider = cfg["provider"]
    base_url = cfg["base_url"]
    model_name = cfg["model_name"]

    if provider == "openai":
        from agentscope.formatter import OpenAIChatFormatter
        from agentscope.model import OpenAIChatModel

        kwargs: dict[str, Any] = {
            "model_name": model_name,
            "api_key": api_key,
            "stream": False,
        }
        if base_url:
            kwargs["client_kwargs"] = {"base_url": base_url}
        return OpenAIChatModel(**kwargs), OpenAIChatFormatter()

    if provider == "anthropic":
        from agentscope.formatter import AnthropicChatFormatter
        from agentscope.model import AnthropicChatModel

        kwargs = {
            "model_name": model_name,
            "api_key": api_key,
            "stream": False,
            "max_tokens": 4096,
        }
        if base_url:
            kwargs["client_kwargs"] = {"base_url": base_url}
        return AnthropicChatModel(**kwargs), AnthropicChatFormatter()

    raise ValueError(
        f"Unsupported LLM_PROVIDER='{provider}'. Use 'openai' or 'anthropic'."
    )
