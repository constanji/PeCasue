"""PasswordAgent — migrated from pingpong-master.

Phase 7 strategy:
    - Keep the public contract (`resolve(...)` -> `PasswordResponse`) identical
      to pingpong's ReActAgent so downstream parsers can call it once they're
      migrated.
    - The default backend is **rule-based**: it consults the encrypted password
      book first, then applies the well-known bank-specific patterns (citi /
      scb / bank-code+digits) that pingpong hard-coded into the LLM prompt.
    - When `PIPELINE_LLM_ENABLED=1` and AgentScope is installed, the original
      ReActAgent path is invoked (best effort, falls back to heuristic).
"""
from __future__ import annotations

import re
from typing import Dict, Optional

from pydantic import BaseModel, Field

from server.agents.config import llm_enabled
from server.rules.password_book import lookup_password


class PasswordResponse(BaseModel):
    password: Optional[str] = Field(default=None)
    confidence: float = 0.0
    reasoning: str = ""


# Bank-specific patterns observed by pingpong-master.
_FIXED_PASSWORDS: Dict[str, str] = {
    "citi": "Pp618618@",
    "citibank": "Pp618618@",
}


def _scb_pattern(filename: str) -> Optional[str]:
    """SCB convention: 'PING' + chars[2:7] of filename (1-indexed positions 2..6)."""
    base = filename.split("/")[-1]
    if len(base) >= 7:
        return "PING" + base[1:6]
    return None


def _heuristic(
    bank_key: str,
    filename: str,
    password_hint: Optional[str],
    known_passwords: Optional[Dict[str, str]],
) -> PasswordResponse:
    bank = (bank_key or "").lower()

    # 0. Encrypted password book (Human-managed) wins.
    pw = lookup_password(scope="bank", pattern=bank) or lookup_password(
        scope="file_pattern", pattern=filename
    )
    if pw:
        return PasswordResponse(
            password=pw, confidence=0.95, reasoning="命中规则页 · 密码簿"
        )

    # 1. Direct hint (`密码是 xxx` / `password: xxx`).
    if password_hint:
        m = re.search(r"(?:密码是|password\s*[:=])\s*([^\s,]+)", password_hint, re.I)
        if m:
            return PasswordResponse(
                password=m.group(1),
                confidence=0.9,
                reasoning="解析自上传时附带的提示",
            )

    # 2. Fixed bank-level passwords.
    if bank in _FIXED_PASSWORDS:
        return PasswordResponse(
            password=_FIXED_PASSWORDS[bank],
            confidence=0.8,
            reasoning=f"内置 {bank} 通用密码模式",
        )

    # 3. SCB filename pattern.
    if bank in ("scb", "standard_chartered"):
        guess = _scb_pattern(filename)
        if guess:
            return PasswordResponse(
                password=guess,
                confidence=0.7,
                reasoning="SCB 文件名拼接规则: PING + filename[1:6]",
            )

    # 4. Reuse a known password from a sibling bank.
    if known_passwords:
        for k, v in known_passwords.items():
            if k and v:
                return PasswordResponse(
                    password=v,
                    confidence=0.4,
                    reasoning=f"复用同期次 {k} 的密码（低置信，建议人工确认）",
                )

    return PasswordResponse(
        password=None,
        confidence=0.0,
        reasoning="无可用线索（密码簿/提示/已知密码全部为空）",
    )


class PasswordAgent:
    """Drop-in replacement for the pingpong ReActAgent."""

    def __init__(self) -> None:
        self._llm_enabled = llm_enabled()
        self._react = None
        if self._llm_enabled:
            try:
                from agentscope.agent import ReActAgent  # type: ignore

                from server.agents.config import get_chat_model

                model, formatter = get_chat_model()
                self._react = ReActAgent(
                    name="PasswordOracle",
                    sys_prompt=(
                        "You are a security analyst specialising in bank statement encryption."
                    ),
                    model=model,
                    formatter=formatter,
                )
            except Exception:
                self._react = None

    async def resolve(
        self,
        bank_key: str,
        filename: str,
        password_hint: Optional[str] = None,
        known_passwords: Optional[Dict[str, str]] = None,
        error_context: Optional[str] = None,
    ) -> PasswordResponse:
        # Always run the heuristic first; if it's confident enough, skip the LLM.
        baseline = _heuristic(bank_key, filename, password_hint, known_passwords)
        if baseline.password and baseline.confidence >= 0.8:
            return baseline
        if self._react is None:
            return baseline
        # Best-effort LLM path; if it errors we keep the baseline.
        try:
            from agentscope.message import Msg  # type: ignore

            clues = [f"BANK KEY: {bank_key}", f"FILENAME: {filename}"]
            if password_hint:
                clues.append(f"USER HINT: {password_hint}")
            if known_passwords:
                clues.append(f"KNOWN PASSWORDS: {known_passwords}")
            if error_context:
                clues.append(f"PREVIOUS ERROR: {error_context}")
            if baseline.password:
                clues.append(f"HEURISTIC GUESS: {baseline.password}")
            response = await self._react(
                Msg("system", "\n".join(clues), "user"),
                structured_model=PasswordResponse,
            )
            meta = getattr(response, "metadata", None)
            if isinstance(meta, dict):
                return PasswordResponse(**meta)
            if isinstance(meta, PasswordResponse):
                return meta
        except Exception:
            pass
        return baseline
