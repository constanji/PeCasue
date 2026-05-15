"""Pipeline Copilot — orchestrates tool calls to answer Human questions.

PHASE 8 implementation:
    - When ``PIPELINE_LLM_ENABLED=1``, uses AgentScope ReActAgent with all
      registered tools for genuine multi-step reasoning: the LLM decides which
      tools to call, in what order, and with what arguments.
    - When LLM is disabled, falls back to an enhanced heuristic planner with
      richer tool coverage (filter, aggregate, compare, lookup, stats).
    - Always returns a ``CopilotReply`` with: thoughts (planning steps),
      tool_calls (tool + args + result digest), answer (markdown text).
"""
from __future__ import annotations

import inspect
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional

EmitFn = Optional[Callable[[Dict[str, Any]], None]]

from server.agents.config import llm_enabled
from server.agents.tools import TOOL_REGISTRY
from server.core.pipeline_state import (
    AgentInteraction,
    StateManager,
)
from server.core.task_logger import task_log
from server.core.task_repo import TaskRepo


@dataclass
class ToolCallRecord:
    name: str
    args: Dict[str, Any]
    result: Any
    elapsed_ms: float = 0.0


@dataclass
class CopilotReply:
    answer: str
    thoughts: List[str] = field(default_factory=list)
    tool_calls: List[ToolCallRecord] = field(default_factory=list)
    drafts: List[Dict[str, Any]] = field(default_factory=list)


def _serialize_tool_record(c: ToolCallRecord) -> Dict[str, Any]:
    return {
        "name": c.name,
        "args": c.args,
        "result": c.result,
        "elapsed_ms": c.elapsed_ms,
    }


def _emit_thought(thoughts: List[str], emit: EmitFn, msg: str) -> None:
    thoughts.append(msg)
    if emit:
        emit({"type": "thought", "step": msg})


def _emit_tool_event(emit: EmitFn, c: ToolCallRecord) -> None:
    if not emit:
        return
    evt: Dict[str, Any] = {"type": "tool_call"}
    evt.update(_serialize_tool_record(c))
    emit(evt)


def _summarise_tool_result(name: str, result: Any) -> str:
    if isinstance(result, dict):
        if "error" in result:
            return f"❌ {result['error']}"
        if "files" in result and isinstance(result["files"], list):
            return f"找到 {len(result['files'])} 个文件"
        if "rows" in result and isinstance(result["rows"], list):
            total = result.get("total_rows", len(result["rows"]))
            matched = result.get("matched", len(result["rows"]))
            if "matched" in result:
                return f"命中 {matched}/{total} 行"
            return f"读取 {len(result['rows'])} 行（含表头 {len(result.get('columns', []))} 列）"
        if "lines" in result and isinstance(result["lines"], list):
            return f"读取 {len(result['lines'])} 行日志"
        if "summary" in result:
            return "已获取校验摘要"
        if "matched" in result:
            return f"命中 {result['matched']} 条规则"
        if "found" in result:
            return "命中" if result["found"] else "未命中"
        if "draft_id" in result:
            return f"草稿已登记 {result['draft_id']}"
        if "diff_count" in result:
            return f"发现 {result['diff_count']} 处差异"
        if "groups" in result and isinstance(result["groups"], dict):
            return f"分组 {len(result['groups'])} 个"
        if "value" in result and result.get("agg_fn"):
            return f"{result['agg_fn']}={result['value']}"
        if "total" in result and "counts" in result:
            c = result["counts"]
            parts = [f"{k}={v}" for k, v in c.items() if v]
            return f"共 {result['total']} 条: {', '.join(parts)}"
    return "ok"


def _text_from_agentscope_reply(resp: Any) -> Optional[str]:
    """Normalize assistant ``Msg``: ``content`` may be ``str`` or a list of blocks."""
    if resp is None:
        return None
    raw = getattr(resp, "content", None)
    if isinstance(raw, str):
        t = raw.strip()
        return t or None
    if isinstance(raw, list):
        parts: List[str] = []
        for block in raw:
            chunk: Optional[str] = None
            if isinstance(block, dict):
                btype = block.get("type")
                if btype == "text":
                    chunk = block.get("text")  # type: ignore[assignment]
                elif btype == "thinking":
                    chunk = block.get("thinking")  # type: ignore[assignment]
            else:
                bt = getattr(block, "type", None)
                if bt == "text":
                    chunk = getattr(block, "text", None)
            if isinstance(chunk, str) and chunk.strip():
                parts.append(chunk.strip())
        out = "\n".join(parts).strip()
        return out or None
    return None


def _filter_args_for_tool(fn: Callable[..., Any], args: Dict[str, Any]) -> Dict[str, Any]:
    """AgentScope 等运行时会向所有工具注入 ``task_id`` 等上下文字段；仅传入目标函数声明的参数。"""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return args
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        return args
    names: set[str] = set()
    for p in sig.parameters.values():
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            names.add(p.name)
    return {k: v for k, v in args.items() if k in names}


def _call_tool(name: str, args: Dict[str, Any]) -> ToolCallRecord:
    fn: Optional[Callable[..., Any]] = TOOL_REGISTRY.get(name)
    if fn is None:
        return ToolCallRecord(name=name, args=args, result={"error": "unknown tool"})
    import time

    t0 = time.perf_counter()
    try:
        if isinstance(args, dict):
            filtered = _filter_args_for_tool(fn, args)
            out = fn(**filtered)
        else:
            out = fn(args)
    except Exception as exc:  # noqa: BLE001
        out = {"error": f"{type(exc).__name__}: {exc}"}
    return ToolCallRecord(
        name=name,
        args=args,
        result=out,
        elapsed_ms=round((time.perf_counter() - t0) * 1000, 1),
    )


def _result_to_tool_response(result: Any) -> Any:
    """AgentScope toolkit requires ToolResponse; keep JSON text for model parsing."""
    from agentscope.message import TextBlock
    from agentscope.tool import ToolResponse

    if isinstance(result, dict):
        text = json.dumps(result, ensure_ascii=False, default=str)
    else:
        text = str(result)
    return ToolResponse(content=[TextBlock(type="text", text=text)])


def _make_react_toolkit(
    recorded_calls: List[ToolCallRecord],
    *,
    emit: EmitFn = None,
) -> Any:
    """Build AgentScope Toolkit with all copilot tools and call recording."""
    from agentscope.tool import Toolkit

    toolkit = Toolkit()

    def _wrap(tool_name: str) -> Callable[..., Any]:
        def wrapper(**kwargs: Any):
            record = _call_tool(tool_name, kwargs)
            recorded_calls.append(record)
            _emit_tool_event(emit, record)
            return _result_to_tool_response(record.result)

        fn = TOOL_REGISTRY[tool_name]
        wrapper.__name__ = tool_name
        wrapper.__doc__ = (fn.__doc__ or "").strip()
        return wrapper

    for name in sorted(TOOL_REGISTRY.keys()):
        toolkit.register_tool_function(
            _wrap(name),
            func_name=name,
            namesake_strategy="raise",
        )
    return toolkit


# ---------- LLM-powered ReAct agent path ----------


def _build_tool_descriptions() -> str:
    """Build a compact tool catalog for the ReAct agent system prompt."""
    lines = []
    for name in sorted(TOOL_REGISTRY.keys()):
        fn = TOOL_REGISTRY[name]
        doc = (fn.__doc__ or "").strip().split("\n")[0]
        lines.append(f"- {name}: {doc}")
    return "\n".join(lines)


_REACT_SYS_PROMPT = """\
你是 PeCause Pipeline Copilot —— 一个专门服务成本分摊流水线的 AI 助手。

## 你的能力
你可以调用以下工具来获取和分析数据：
{tool_descriptions}

## 上下文
- task_id: 当前任务 ID
- channel_id: 当前渠道（如 bill, own_flow, customer, special_transfer 等）
- run_id: 当前运行 ID

## 文件路径规则（非常重要）
- `list_task_files` 返回的 `rel_path` 已经是相对于**任务根目录**的完整路径（如 `extracted/bill/xxx.xlsx`）。
- 读取文件时（`read_csv`/`read_excel`/`filter_table` 等），**直接复制粘贴** `list_task_files` 返回的 `rel_path`，不要手动拼接、修改或截断路径。
- 如果工具返回 "file not found"，说明路径不匹配，请重新调用 `list_task_files` 获取正确路径，而不是猜测路径。

## 回答规则
1. **先调查再回答**：根据问题判断需要调用哪些工具，按逻辑顺序执行。
2. **数据驱动**：回答必须基于工具返回的真实数据，不要编造。
3. **中文回答**：用简洁的中文回答，≤ 300 字。
4. **引用证据**：回答中引用具体文件路径、规则序号、数值等。
5. **校验告警分析**：如果问题是关于某行为什么是 warning/error，先调用 read_verify_summary 获取该行详情，
   再调用 query_rules 或 read_csv/read_excel 读取对应规则和原始数据来解释原因。
6. **数据对比分析**：如果需要对比两个文件，使用 compare_files 工具。
7. **筛选/聚合**：如果需要查看特定条件的数据，使用 filter_table/aggregate_table。
8. **不要猜测**：如果工具无法找到答案，诚实说明。
9. **寒暄**：若用户仅为问好/致谢等简短寒暄，直接用中文简短回应即可，**不必**调用任何工具。
10. **多轮对话**：若消息中带「对话历史」，可延续上文指代；仍须以工具与当前任务数据为准，不要凭记忆编造。

## 工具参数（必须与 Python 签名一致）
ReAct / Function Calling **只能**使用服务端工具函数定义的**确切关键字**，禁止 invented 别名：
- CSV/Excel 路径参数一律为 **rel_path**（不要用 ``path``、``file_path``）。
- **read_csv**：``task_id``、``rel_path``；可选 ``channel_id``、``head``。
- **read_excel**：同上，另可选 ``sheet``。
- **list_task_files**：必须传 **task_id**；可选 ``channel_id``。
- **read_verify_summary**：必须 ``task_id``、``channel_id``；可选 ``run_id``。
- **filter_table**：``task_id``、``rel_path``，关键字 **column**、**op**、**value**（不要用 ``operator``、``column_name``、``search_column`` 等）。
  ``op`` 取值：eq, ne, contains, gt, lt, gte, lte, startswith, endswith。
- **aggregate_table**：``task_id``、``rel_path``、``agg_fn``；可选 ``column``、``group_by``、``channel_id``、``sheet``。
- **compare_files**：``task_id``、**path_a**、**path_b**（不要使用 rel_path/path 混名的键）。
- **query_rules**：必须 **kind**（如 ``own_flow_processing``）；可选 ``filter``、``limit``。读的是服务端 RuleStore，**不要**传 ``task_id``（即使用户在上下文中也不要加）。
- **lookup_password**：可选 ``scope``、``pattern``。**不要**传 ``task_id``。
"""


def _copilot_history_config() -> Dict[str, int]:
    """Human-message windowing + soft-trim sizes (cf. agents-because context pruning)."""
    return {
        "max_messages": int(os.environ.get("PIPELINE_COPILOT_HISTORY_MAX_MESSAGES", "24")),
        "user_max_chars": int(os.environ.get("PIPELINE_COPILOT_HISTORY_USER_MAX_CHARS", "12000")),
        "assistant_max_chars": int(
            os.environ.get("PIPELINE_COPILOT_HISTORY_ASSISTANT_MAX_CHARS", "16000")
        ),
        "soft_head_chars": int(os.environ.get("PIPELINE_COPILOT_HISTORY_SOFT_HEAD_CHARS", "3500")),
        "soft_tail_chars": int(os.environ.get("PIPELINE_COPILOT_HISTORY_SOFT_TAIL_CHARS", "3500")),
    }


def _soft_trim_text(text: str, max_chars: int, head: int, tail: int) -> str:
    """Head+tail retention like agents-because ``softTrimContent`` for long bubbles."""
    s = text.strip()
    if len(s) <= max_chars:
        return s
    h = max(0, head)
    t = max(0, tail)
    if h + t + 40 >= len(s):
        return s
    indicator = (
        f"\n\n… [soft-trim: {len(s)} → {h + t} chars, cf. agents-because] …\n\n"
    )
    return s[:h] + indicator + s[-t:]


def _assistant_turn_for_history(text: str) -> str:
    """Keep the conversational head before evidence footer ``---`` (compact history)."""
    s = text.strip()
    if "\n---\n" in s:
        s = s.split("\n---\n", 1)[0].strip()
    return s


def prune_conversation_history(raw: Optional[List[Dict[str, Any]]]) -> List[Dict[str, str]]:
    """Normalize roles, slice last ``max_messages``, soft-trim long content."""
    if not raw:
        return []
    cfg = _copilot_history_config()
    normalized: List[Dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        role_raw = item.get("role")
        content_raw = item.get("content")
        if not isinstance(role_raw, str) or not isinstance(content_raw, str):
            continue
        role = role_raw.strip().lower()
        if role not in ("user", "assistant"):
            continue
        content = content_raw.strip()
        if not content:
            continue
        if role == "assistant":
            content = _assistant_turn_for_history(content)
            mc = cfg["assistant_max_chars"]
            content = _soft_trim_text(
                content,
                max_chars=mc,
                head=cfg["soft_head_chars"],
                tail=cfg["soft_tail_chars"],
            )
        else:
            mc = cfg["user_max_chars"]
            content = _soft_trim_text(
                content,
                max_chars=mc,
                head=cfg["soft_head_chars"],
                tail=cfg["soft_tail_chars"],
            )
        normalized.append({"role": role, "content": content})
    cap = cfg["max_messages"]
    if cap > 0 and len(normalized) > cap:
        normalized = normalized[-cap:]
    return normalized


def _format_history_markdown(history: List[Dict[str, str]]) -> str:
    parts: List[str] = []
    for i, m in enumerate(history, start=1):
        title = "用户" if m["role"] == "user" else "助手"
        parts.append(f"#### 轮次 {i} · {title}\n{m['content']}")
    return "\n\n".join(parts)


def _compose_react_user_prompt(
    *,
    ctx_parts: List[str],
    question: str,
    conversation_history: Optional[List[Dict[str, Any]]],
) -> str:
    pruned = prune_conversation_history(conversation_history)
    blocks: List[str] = []
    if pruned:
        blocks.append(
            "## 对话历史（服务端节选 + soft-trim，思路参考 agents-because）\n"
            + _format_history_markdown(pruned)
        )
    blocks.append(f"CONTEXT: {', '.join(ctx_parts)}\n\nQUESTION: {question.strip()}")
    return "\n\n".join(blocks)


def _run_react_and_format(
    question: str,
    ctx: Dict[str, Any],
    thoughts: List[str],
    *,
    emit: EmitFn = None,
) -> tuple[Optional[str], List[ToolCallRecord], bool]:
    """Run ReAct; returns (answer_text, tool_calls, crashed).

    ``crashed`` is True only when the agent raised before returning a reply.
    """
    try:
        from agentscope.agent import ReActAgent
        from agentscope.message import Msg
        from server.agents.config import get_chat_model

        model, formatter = get_chat_model()

        recorded_calls: List[ToolCallRecord] = []
        toolkit = _make_react_toolkit(recorded_calls, emit=emit)

        sys_prompt = _REACT_SYS_PROMPT.format(
            tool_descriptions=_build_tool_descriptions()
        )

        ctx_parts = []
        if ctx.get("task_id"):
            ctx_parts.append(f"task_id={ctx['task_id']}")
        if ctx.get("channel_id"):
            ctx_parts.append(f"channel_id={ctx['channel_id']}")
        if ctx.get("run_id"):
            ctx_parts.append(f"run_id={ctx['run_id']}")
        if ctx.get("verify_row_id"):
            ctx_parts.append(f"verify_row_id={ctx['verify_row_id']}")

        hist = ctx.get("conversation_history")
        prompt = _compose_react_user_prompt(
            ctx_parts=ctx_parts,
            question=question,
            conversation_history=hist if isinstance(hist, list) else None,
        )

        agent = ReActAgent(
            name="PipelineCopilot",
            sys_prompt=sys_prompt,
            model=model,
            formatter=formatter,
            toolkit=toolkit,
        )

        resp = await_or_value(agent(Msg("user", prompt, "user")))
        answer_text = _text_from_agentscope_reply(resp)
        return answer_text, recorded_calls, False

    except Exception as exc:
        _emit_thought(thoughts, emit, f"ReAct Agent 异常: {exc}，回退到启发式路径")
        return None, [], True


def await_or_value(coro):
    """Resolve an async coroutine in the current (possibly sync) context."""
    import asyncio
    import inspect

    if inspect.iscoroutine(coro):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(asyncio.run, coro).result()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)
    return coro


# ---------- enhanced heuristic planner (fallback) ----------


_VERIFY_KW = ("校验", "warning", "告警", "异常", "错误", "为什么", "why", "explain", "fail", "error")
_LOG_KW = ("日志", "log", "进度")
_FILE_KW = ("文件", "file", "看一下", "列出")
_RULE_KW = ("规则", "mapping", "处理表", "汇率", "rule")
_COMPARE_KW = ("对比", "比较", "差异", "diff", "compare", "区别")
_FILTER_KW = ("筛选", "过滤", "筛选", "哪些", "哪些行", "找到", "查找")
_STATS_KW = ("统计", "汇总", "总数", "合计", "sum", "count", "平均", "多少")

_SMALLTALK_RE = re.compile(
    r"^(?:你好|您好|hi|hello|hey|哈喽|嗨|在吗|在么|早上好|下午好|晚上好|早安|晚安|"
    r"谢谢|感谢|多谢|拜拜|再见)(?:[!！。.…~\s]*)$",
    re.I,
)


def _is_pure_smalltalk(question: str) -> bool:
    """Short greetings / thanks — skip heavyweight grounding unless user asks explicitly."""
    q = question.strip()
    if not q or len(q) > 48:
        return False
    ql = q.lower()
    needles = (
        *_VERIFY_KW,
        *_LOG_KW,
        *_FILE_KW,
        *_RULE_KW,
        *_COMPARE_KW,
        *_FILTER_KW,
        *_STATS_KW,
    )
    if any(k in ql for k in needles):
        return False
    return bool(_SMALLTALK_RE.match(q))


def _plan(question: str, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a list of tool invocations to execute, in order (heuristic fallback)."""
    q = question.lower()
    plan: List[Dict[str, Any]] = []

    task_id = ctx.get("task_id")
    channel_id = ctx.get("channel_id")
    run_id = ctx.get("run_id")
    verify_row_id = ctx.get("verify_row_id")

    if not task_id:
        return plan

    if not _is_pure_smalltalk(question):
        plan.append(
            {
                "tool": "list_task_files",
                "args": {"task_id": task_id, "channel_id": channel_id},
            }
        )

    # Verify/warning questions
    if any(k in q for k in _VERIFY_KW) or verify_row_id:
        if channel_id:
            plan.append(
                {
                    "tool": "verify_summary_stats",
                    "args": {
                        "task_id": task_id,
                        "channel_id": channel_id,
                        "run_id": run_id,
                    },
                }
            )
            plan.append(
                {
                    "tool": "read_verify_summary",
                    "args": {
                        "task_id": task_id,
                        "channel_id": channel_id,
                        "run_id": run_id,
                    },
                }
            )

    # Log questions
    if any(k in q for k in _LOG_KW):
        plan.append(
            {
                "tool": "read_log",
                "args": {"task_id": task_id, "channel_id": channel_id, "tail": 80},
            }
        )

    # Rule questions
    if any(k in q for k in _RULE_KW):
        kind = None
        if "汇率" in q or "fx" in q:
            kind = "fx"
        elif "处理表" in q or "自有" in q:
            kind = "own_flow_processing"
        elif "费项" in q or "fee" in q:
            kind = "fee_mapping"
        elif "账户" in q or "account" in q:
            kind = "account_mapping"
        elif "分行" in q or "branch" in q:
            kind = "special_branch_mapping"
        elif "模板" in q or "template" in q:
            kind = "result_template"
        if kind:
            plan.append({"tool": "query_rules", "args": {"kind": kind, "limit": 20}})

    # Compare questions — if the question mentions two files or "对比"
    if any(k in q for k in _COMPARE_KW):
        plan.append(
            {
                "tool": "compare_files",
                "args": {
                    "task_id": task_id,
                    "path_a": "",  # Will be filled by LLM or skipped
                    "path_b": "",
                    "channel_id": channel_id,
                },
            }
        )

    # Stats questions
    if any(k in q for k in _STATS_KW) and channel_id:
        plan.append(
            {
                "tool": "verify_summary_stats",
                "args": {
                    "task_id": task_id,
                    "channel_id": channel_id,
                    "run_id": run_id,
                },
            }
        )

    # Remove invalid compare step (empty paths)
    plan = [
        p for p in plan
        if not (p["tool"] == "compare_files" and (not p["args"].get("path_a") or not p["args"].get("path_b")))
    ]

    return plan


def _format_answer(question: str, calls: List[ToolCallRecord], ctx: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"**问题**：{question}")
    if ctx.get("channel_id"):
        lines.append(f"**渠道**：`{ctx.get('channel_id')}`")
    if ctx.get("run_id"):
        lines.append(f"**Run**：`{ctx['run_id'][:8]}`")
    lines.append("")
    lines.append("**调查过程**：")
    if not calls:
        lines.append(
            "- （未调度工具；简短寒暄已跳过文件列举。"
            " 如需分析流水，可直接问「列出文件」「校验告警」或描述具体问题。）"
        )
    for c in calls:
        lines.append(f"- 调用 `{c.name}` → {_summarise_tool_result(c.name, c.result)} ({c.elapsed_ms} ms)")

    # Extract verify summary insights
    for c in reversed(calls):
        if c.name == "read_verify_summary":
            summary = (c.result or {}).get("summary") or {}
            rows = summary.get("rows", [])
            warns = [r for r in rows if r.get("severity") == "warning"]
            if warns:
                lines.append("")
                lines.append("**当前告警行**：")
                for r in warns[:5]:
                    lines.append(
                        f"- `{r.get('row_id')}` · {r.get('summary')}"
                        f" · 规则 `{r.get('rule_ref')}`"
                        f"{' · 文件 `' + (r.get('file_ref') or '') + '`' if r.get('file_ref') else ''}"
                    )
            break

    # Extract verify_summary_stats insights
    for c in calls:
        if c.name == "verify_summary_stats" and isinstance(c.result, dict):
            counts = c.result.get("counts", {})
            by_rule = c.result.get("by_rule", {})
            if counts:
                lines.append("")
                lines.append(f"**校验统计**：共 {c.result.get('total', 0)} 条")
                for sev, cnt in sorted(counts.items()):
                    lines.append(f"  - {sev}: {cnt}")
            if by_rule:
                lines.append("**规则分布**：")
                for rule, cnt in sorted(by_rule.items(), key=lambda x: -x[1])[:5]:
                    lines.append(f"  - `{rule}`: {cnt} 条")

    if not llm_enabled():
        lines.append("")
        lines.append(
            "> 启发式回答（设置 `PIPELINE_LLM_ENABLED=1` 启用 LLM 智能推理）"
        )
    return "\n".join(lines)


def _format_answer_with_evidence(
    question: str,
    llm_answer: Optional[str],
    calls: List[ToolCallRecord],
    ctx: Dict[str, Any],
) -> str:
    """Combine LLM answer with tool call evidence for a complete response."""
    lines: List[str] = []

    if llm_answer and llm_answer.strip():
        lines.append(llm_answer.strip())
        lines.append("")
        lines.append("---")
        lines.append("")

    # Always include the evidence trail
    lines.append(f"**问题**：{question}")
    if ctx.get("channel_id"):
        lines.append(f"**渠道**：`{ctx.get('channel_id')}`")
    if ctx.get("run_id"):
        lines.append(f"**Run**：`{ctx['run_id'][:8]}`")
    lines.append("")
    lines.append("**工具调用**：")
    for c in calls:
        lines.append(f"- `{c.name}` → {_summarise_tool_result(c.name, c.result)} ({c.elapsed_ms} ms)")

    return "\n".join(lines)


# ---------- main entry ----------


def _persist_copilot_log(question: str, ctx: Dict[str, Any], reply: CopilotReply) -> None:
    task_id = ctx.get("task_id")
    if not task_id:
        return
    calls = reply.tool_calls
    state = StateManager.load_state(task_id)
    if state:
        ch_id = ctx.get("channel_id")
        if ch_id and ch_id in state.channels and state.channels[ch_id].runs:
            state.channels[ch_id].runs[-1].agent_interactions.append(
                AgentInteraction(
                    kind="ask",
                    summary=question[:160],
                    payload={
                        "tool_calls": [
                            {"name": c.name, "elapsed_ms": c.elapsed_ms}
                            for c in calls
                        ]
                    },
                )
            )
        StateManager.save_state(state)
    task_log(task_id, f"[copilot] {question[:120]}", channel=ctx.get("channel_id") or "pipeline")
    TaskRepo.append_event(
        task_id,
        "agent.ask",
        channel_id=ctx.get("channel_id"),
        run_id=ctx.get("run_id"),
        payload={"question": question[:200], "tool_count": len(calls)},
    )


def _execute_copilot(
    question: str,
    ctx: Dict[str, Any],
    *,
    emit: EmitFn = None,
) -> CopilotReply:
    thoughts: List[str] = []
    calls: List[ToolCallRecord] = []
    answer: str = ""

    _emit_thought(thoughts, emit, f"解析问题：{question[:80]}")

    task_id = ctx.get("task_id")
    if not task_id:
        _emit_thought(thoughts, emit, "缺少 task_id 上下文，无法调用工具")
        reply = CopilotReply(
            answer="无法回答：缺少任务上下文。请先在总览或渠道详情中选中一个任务后再提问。",
            thoughts=thoughts,
            tool_calls=[],
        )
        if emit:
            emit({"type": "answer", "answer": reply.answer})
        _persist_copilot_log(question, ctx, reply)
        return reply

    if llm_enabled():
        _emit_thought(thoughts, emit, "LLM 已启用 · 启动 ReAct Agent 多步推理")
        llm_answer, calls, react_crashed = _run_react_and_format(
            question, ctx, thoughts, emit=emit
        )
        if calls:
            answer = _format_answer_with_evidence(
                question, llm_answer, calls, ctx
            )
        elif llm_answer:
            answer = llm_answer
            _emit_thought(thoughts, emit, "Agent 直接回答（未使用工具）")
        else:
            if not react_crashed:
                _emit_thought(
                    thoughts,
                    emit,
                    "模型未产出可展示的纯文本且无工具轨迹，改用启发式补全",
                )
            plan = _plan(question, ctx)
            _emit_thought(thoughts, emit, f"启发式规划 {len(plan)} 次工具调用")
            calls = []
            for step in plan:
                _emit_thought(thoughts, emit, f"调度工具 `{step['tool']}`")
                record = _call_tool(step["tool"], step["args"])
                calls.append(record)
                _emit_tool_event(emit, record)
            answer = _format_answer(question, calls, ctx)
    else:
        plan = _plan(question, ctx)
        _emit_thought(thoughts, emit, f"启发式规划 {len(plan)} 次工具调用")
        for step in plan:
            _emit_thought(thoughts, emit, f"调度工具 `{step['tool']}`")
            record = _call_tool(step["tool"], step["args"])
            calls.append(record)
            _emit_tool_event(emit, record)
        answer = _format_answer(question, calls, ctx)

    reply = CopilotReply(answer=answer, thoughts=thoughts, tool_calls=calls)
    if emit:
        emit({"type": "answer", "answer": answer})
    _persist_copilot_log(question, ctx, reply)
    return reply


def ask(question: str, ctx: Dict[str, Any]) -> CopilotReply:
    """Main entry — invoked by ``POST /agent/ask`` (non-streaming)."""
    return _execute_copilot(question, ctx, emit=None)


def _reply_to_wire(reply: CopilotReply) -> Dict[str, Any]:
    return {
        "answer": reply.answer,
        "thoughts": reply.thoughts,
        "tool_calls": [_serialize_tool_record(c) for c in reply.tool_calls],
        "drafts": reply.drafts,
    }


def iter_copilot_ndjson(question: str, ctx: Dict[str, Any]) -> Iterator[str]:
    """Incremental NDJSON chunks (``\\n``-terminated) for SSE-style streaming."""

    import queue
    import threading

    q: queue.Queue[Any] = queue.Queue()
    done_token = object()

    def runner() -> None:
        try:

            def emit(ev: Dict[str, Any]) -> None:
                q.put(json.dumps(ev, ensure_ascii=False, default=str) + "\n")

            reply = _execute_copilot(question, ctx, emit=emit)
            q.put(
                json.dumps(
                    {"type": "done", "reply": _reply_to_wire(reply)},
                    ensure_ascii=False,
                    default=str,
                )
                + "\n"
            )
        except Exception as exc:  # noqa: BLE001 — stream error to client
            q.put(
                json.dumps({"type": "error", "message": str(exc)}, ensure_ascii=False)
                + "\n"
            )
        finally:
            q.put(done_token)

    threading.Thread(target=runner, daemon=True).start()

    while True:
        chunk = q.get()
        if chunk is done_token:
            break
        yield chunk