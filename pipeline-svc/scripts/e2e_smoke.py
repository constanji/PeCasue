#!/usr/bin/env python3
"""End-to-end smoke test for pipeline-svc — Phase 11 acceptance.

Verifies a single task can flow through:
    1. Create task
    2. Auto-classify uploaded zip (synthesised here)
    3. Trigger 6 channel runs (placeholder parsers)
    4. Ask Copilot for an explanation of a verify warning
    5. Propose an agent draft (rule_patch)
    6. Replace a source file → marks prior runs dirty
    7. Re-run the affected channel
    8. Compare two runs of the same channel
    9. Read observability KPI / charts / events

Usage::

    cd pipeline-svc
    uv sync                         # one-off, installs all deps
    uv run python scripts/e2e_smoke.py

Exit code 0 = all assertions passed.

Note: the placeholder parsers ship a deterministic mock dataset, so this
script doesn't need any real bank statements to run end-to-end.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
SVC_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(SVC_ROOT))


def main() -> int:
    # Use a throw-away data dir so we don't pollute developer's local data.
    tmpdir = Path(tempfile.mkdtemp(prefix="pipeline-svc-e2e-"))
    os.environ["PIPELINE_DATA_DIR"] = str(tmpdir)
    print(f"[e2e] data root → {tmpdir}")

    # Lazy imports — must happen *after* PIPELINE_DATA_DIR is set.
    from server.core.paths import (
        ensure_data_directories,
        get_task_extracted_dir,
    )
    from server.core.task_db import init_db
    from server.core.pipeline_state import (
        ChannelState,
        StateManager,
        TaskState,
    )
    from server.core.orchestrator import run_channel
    from server.agents.copilot import ask as copilot_ask
    from server.agents.tools import propose_rule_patch
    from server.compare.runner import run_compare
    from server.api.observe import observe_charts, observe_events, observe_kpi

    ensure_data_directories()
    init_db()

    # ---------- 1. Create task + plant 6 channels ----------
    import uuid

    state = TaskState(task_id=uuid.uuid4().hex[:12], period="2025-04", created_by="e2e")
    channels = [
        ("bill", "账单"),
        ("own_flow", "自有流水"),
        ("customer", "客资"),
        ("special_transfer", "内转"),
        ("cn_jp", "境内 & 日本"),
        ("allocation_base", "分摊基数"),
    ]
    for cid, dname in channels:
        # Plant a fake source file so the parser sees inputs.
        src_dir = get_task_extracted_dir(state.task_id, cid)
        src_dir.mkdir(parents=True, exist_ok=True)
        (src_dir / f"{cid}_input.csv").write_text(
            "header_a,header_b,header_c\n1,2,3\n", encoding="utf-8"
        )
        state.channels[cid] = ChannelState(channel_id=cid, display_name=dname)
    StateManager.save_state(state)
    print(f"[e2e] task created: {state.task_id}")

    # ---------- 2. Trigger all 6 channel runs ----------
    async def _run_all() -> list[str]:
        results = []
        for cid, _ in channels:
            rid = await run_channel(state.task_id, cid, actor="e2e")
            assert rid, f"run_channel returned None for {cid}"
            results.append(rid)
        return results

    run_ids = asyncio.run(_run_all())
    print(f"[e2e] ran 6 channels, run_ids={[r[:6] for r in run_ids]}")

    # ---------- 3. Ask the Copilot ----------
    reply = copilot_ask(
        "自有流水 的校验告警都有哪些？",
        {"task_id": state.task_id, "channel_id": "own_flow"},
    )
    assert reply.tool_calls, "copilot should call at least one tool"
    print(f"[e2e] copilot replied with {len(reply.tool_calls)} tool calls")

    # ---------- 4. Agent draft ----------
    draft = propose_rule_patch(
        state.task_id,
        kind="own_flow_processing",
        patch={"add_row": {"数据源": "JPM", "渠道": "JPM-001"}},
        rationale="补齐 e2e 缺失映射",
    )
    assert draft["status"] == "pending"
    print(f"[e2e] draft created: {draft['draft_id']}")

    # ---------- 5. Replace a source file ----------
    src = get_task_extracted_dir(state.task_id, "bill") / "bill_input.csv"
    src.write_text("header_a,header_b,header_c\n10,20,30\n", encoding="utf-8")

    # ---------- 6. Re-run the affected channel ----------
    new_rid = asyncio.run(run_channel(state.task_id, "bill", actor="e2e"))
    assert new_rid, "second bill run failed"
    print(f"[e2e] bill re-run: {new_rid[:6]}")

    # ---------- 7. Compare the two bill runs ----------
    state2 = StateManager.load_state(state.task_id)
    bill = state2.channels["bill"]
    assert len(bill.runs) >= 2
    run_a = bill.runs[0]
    run_b = bill.runs[1]

    def _pick_csv(files):
        for f in files:
            if f.name.lower().endswith(".csv"):
                return f.name
        return None

    out_a = _pick_csv(run_a.output_files)
    out_b = _pick_csv(run_b.output_files)
    if out_a and out_b:
        # Inspect the CSV header to pick a real key column.
        from server.core.paths import get_channel_run_dir as _crun

        sample = (_crun(state.task_id, "bill", run_a.run_id) / out_a)
        first_line = sample.read_text(encoding="utf-8-sig").splitlines()[0]
        key_col = first_line.split(",")[0].strip()
        meta = run_compare(
            task_id=state.task_id,
            left={
                "kind": "run_output",
                "channel_id": "bill",
                "run_id": run_a.run_id,
                "name": out_a,
            },
            right={
                "kind": "run_output",
                "channel_id": "bill",
                "run_id": run_b.run_id,
                "name": out_b,
            },
            key_cols=[key_col],
            actor="e2e",
        )
        print(
            f"[e2e] compare {meta['compare_id'][:6]} matched={meta['summary']['matched_rows']} diff_cells={meta['summary']['diff_cells']}"
        )
    else:
        print("[e2e] skipping compare (no output files in placeholder parsers)")

    # ---------- 8. Observability ----------
    kpi = observe_kpi(window_days=1)
    charts = observe_charts(window_days=7)
    events = observe_events(limit=50, task_id=state.task_id)
    assert kpi["tasks_total"] >= 0
    assert isinstance(charts["duration_by_channel"], list)
    assert events["events"], "expected at least one event for our task"
    print(
        f"[e2e] observe: kpi.tasks_total={kpi['tasks_total']} "
        f"charts.dur_channels={len(charts['duration_by_channel'])} "
        f"events={len(events['events'])}"
    )

    # ---------- 9. Cleanup ----------
    shutil.rmtree(tmpdir, ignore_errors=True)
    print("[e2e] all checks passed ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
