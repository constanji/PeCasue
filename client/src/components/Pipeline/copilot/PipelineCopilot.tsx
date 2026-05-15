import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { useQueryClient } from '@tanstack/react-query';
import { useRecoilState, useRecoilValue } from 'recoil';
import {
  AlertCircle,
  ChevronDown,
  ChevronUp,
  Loader2,
  MessageSquare,
  Send,
  Sparkles,
  X,
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  pipelineCopilotOpenAtom,
  pipelineCopilotPrefillAtom,
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import {
  PIPELINE_QUERY_KEYS,
  PipelineApi,
  usePipelineAgentDrafts,
  type PipelineCopilotHistoryTurn,
  type PipelineCopilotReply,
  type PipelineCopilotStreamEvent,
  type PipelineCopilotToolCall,
} from '~/data-provider';
import { cn } from '~/utils';

interface CopilotStreamState {
  streaming: boolean;
  thoughts: string[];
  tool_calls: PipelineCopilotToolCall[];
  answer: string;
  error?: string;
}

interface CopilotTurn {
  id: string;
  role: 'user' | 'assistant' | 'system';
  question?: string;
  reply?: PipelineCopilotReply;
  replyStreaming?: CopilotStreamState;
  ts: number;
}

function copilotHistoryFromTurns(turns: CopilotTurn[]): PipelineCopilotHistoryTurn[] {
  const out: PipelineCopilotHistoryTurn[] = [];
  for (const t of turns) {
    if (t.role === 'user' && t.question?.trim()) {
      out.push({ role: 'user', content: t.question.trim() });
    }
    if (t.role === 'assistant' && t.reply?.answer?.trim()) {
      out.push({ role: 'assistant', content: t.reply.answer.trim() });
    }
  }
  return out;
}

const TOOL_LABELS: Record<string, string> = {
  list_task_files: '列任务文件',
  read_text: '读文本片段',
  read_csv: '读 CSV 头',
  read_excel: '读 Excel 头',
  read_verify_summary: '读校验摘要',
  read_log: '读日志',
  query_rules: '查询规则',
  lookup_password: '查密码簿',
  propose_rule_patch: '草稿: 规则补丁',
  propose_replace_file: '草稿: 替换文件',
  mark_row_resolved: '标记已解释',
  filter_table: '筛选数据',
  aggregate_table: '聚合统计',
  compare_files: '文件对比',
  lookup_row: '查找行',
  verify_summary_stats: '校验统计',
};

function toolResultHasError(result: unknown): boolean {
  return (
    !!result &&
    typeof result === 'object' &&
    !Array.isArray(result) &&
    typeof (result as Record<string, unknown>).error === 'string'
  );
}

function summariseResult(result: unknown): string {
  if (!result || typeof result !== 'object') return 'ok';
  const r = result as Record<string, unknown>;
  if (typeof r.error === 'string') return `❌ ${r.error}`;
  if (Array.isArray(r.files)) return `命中 ${r.files.length} 个文件`;
  if (Array.isArray(r.rows)) {
    if (typeof r.total_rows === 'number' && typeof r.matched === 'number') {
      return `命中 ${r.matched}/${r.total_rows} 行`;
    }
    return `读取 ${r.rows.length} 行`;
  }
  if (Array.isArray(r.lines)) return `读取 ${r.lines.length} 行日志`;
  if (typeof r.matched === 'number') return `命中 ${r.matched} 条`;
  if (typeof r.found === 'boolean') return r.found ? '命中' : '未命中';
  if (typeof r.draft_id === 'string') return `草稿 ${r.draft_id}`;
  if (typeof r.diff_count === 'number') return `发现 ${r.diff_count} 处差异`;
  if (r.groups && typeof r.groups === 'object' && !Array.isArray(r.groups)) {
    return `分组 ${Object.keys(r.groups).length} 个`;
  }
  if (r.value != null && typeof r.agg_fn === 'string') {
    return `${r.agg_fn}=${String(r.value)}`;
  }
  if (typeof r.total === 'number' && r.counts && typeof r.counts === 'object') {
    const c = r.counts as Record<string, number>;
    const parts = Object.entries(c)
      .filter(([, v]) => v)
      .map(([k, v]) => `${k}=${v}`)
      .join(', ');
    return `共 ${r.total} 条: ${parts}`;
  }
  return 'ok';
}

function CopilotAnswerMarkdown({ text }: { text: string }) {
  return (
    <div className="prose prose-sm dark:prose-invert prose-p:my-2 prose-li:my-0.5 max-w-none leading-relaxed text-text-primary prose-headings:text-text-primary prose-strong:text-text-primary prose-code:break-words prose-code:text-green-600 dark:prose-code:text-green-400">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{text || ' '}</ReactMarkdown>
    </div>
  );
}

function ToolCallCard({ call }: { call: PipelineCopilotToolCall }) {
  const [open, setOpen] = useState(false);
  const label = TOOL_LABELS[call.name] ?? call.name;
  const failed = toolResultHasError(call.result);
  const StatusIcon = failed ? AlertCircle : Sparkles;
  return (
    <div className="rounded-lg border border-border-light bg-surface-secondary text-xs">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left"
        aria-label={`tool-${call.name}`}
      >
        <span className="flex items-center gap-2">
          <StatusIcon className={cn('h-3.5 w-3.5', failed ? 'text-red-500' : 'text-green-500')} />
          <span className="font-medium text-text-primary">{label}</span>
          <span className="font-mono text-[10px] text-text-secondary">
            {call.name}
          </span>
        </span>
        <span className="flex items-center gap-2 text-text-secondary">
          <span>{summariseResult(call.result)}</span>
          <span>· {call.elapsed_ms} ms</span>
          {open ? (
            <ChevronUp className="h-3 w-3" />
          ) : (
            <ChevronDown className="h-3 w-3" />
          )}
        </span>
      </button>
      {open && (
        <div className="space-y-2 border-t border-border-light px-3 py-2">
          <details>
            <summary className="cursor-pointer text-text-secondary">参数</summary>
            <pre className="mt-1 max-h-40 overflow-auto rounded bg-surface-primary p-2 text-[11px] text-text-primary">
              {JSON.stringify(call.args, null, 2)}
            </pre>
          </details>
          <details>
            <summary className="cursor-pointer text-text-secondary">结果</summary>
            <pre className="mt-1 max-h-60 overflow-auto rounded bg-surface-primary p-2 text-[11px] text-text-primary">
              {JSON.stringify(call.result, null, 2)}
            </pre>
          </details>
        </div>
      )}
    </div>
  );
}

function AssistPanel({
  thoughts,
  tool_calls,
  answerMD,
  thoughtsInitiallyOpen = true,
}: {
  thoughts: string[];
  tool_calls: PipelineCopilotToolCall[];
  answerMD: string;
  thoughtsInitiallyOpen?: boolean;
}) {
  return (
    <div className="flex justify-start">
      <div className="w-[92%] space-y-2">
        {thoughts.length > 0 && (
          <details
            {...(thoughtsInitiallyOpen ? { open: true } : {})}
            className="rounded-lg border border-border-light bg-surface-primary px-3 py-2 text-xs text-text-secondary"
          >
            <summary className="cursor-pointer font-medium text-text-primary">
              思维链 · {thoughts.length} 步
            </summary>
            <ol className="mt-1 list-inside list-decimal space-y-0.5">
              {thoughts.map((text, i) => (
                <li key={`${i}-${text.slice(0, 24)}`}>{text}</li>
              ))}
            </ol>
          </details>
        )}
        {tool_calls.length > 0 && (
          <div className="space-y-1.5">
            {tool_calls.map((c, i) => (
              <ToolCallCard key={`${c.name}-${i}-${c.elapsed_ms}`} call={c} />
            ))}
          </div>
        )}
        {answerMD.trim() ? (
          <div className="rounded-lg border border-border-light bg-surface-secondary p-3 text-sm">
            <CopilotAnswerMarkdown text={answerMD} />
          </div>
        ) : null}
      </div>
    </div>
  );
}

function TurnView({ turn }: { turn: CopilotTurn }) {
  if (turn.role === 'user') {
    return (
      <div className="flex justify-end">
        <div className="max-w-[85%] rounded-lg bg-green-500/15 px-3 py-2 text-sm text-text-primary">
          {turn.question}
        </div>
      </div>
    );
  }
  if (turn.role === 'system') {
    return (
      <div className="text-center text-xs text-text-secondary">
        {turn.question}
      </div>
    );
  }
  if (turn.replyStreaming) {
    const s = turn.replyStreaming;
    return (
      <>
        <AssistPanel
          thoughts={s.thoughts}
          tool_calls={s.tool_calls}
          answerMD={s.answer}
          thoughtsInitiallyOpen
        />
        <div className="flex justify-start">
          <div className="w-[92%] space-y-1">
            {s.streaming ? (
              <div className="flex items-center gap-2 px-3 text-xs text-text-secondary">
                <Loader2 className="h-3.5 w-3.5 animate-spin text-green-500" />
                推理中…
              </div>
            ) : null}
            {s.error ? (
              <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-700 dark:text-red-300">
                ❌ {s.error}
              </div>
            ) : null}
          </div>
        </div>
      </>
    );
  }
  const reply = turn.reply;
  if (!reply || turn.role !== 'assistant') return null;

  const { answer, thoughts, tool_calls } = reply;
  return (
    <AssistPanel
      thoughts={thoughts}
      tool_calls={tool_calls}
      answerMD={answer}
      thoughtsInitiallyOpen={false}
    />
  );
}

function DraftBadge({ count }: { count: number }) {
  if (!count) return null;
  return (
    <span className="ml-1 inline-flex h-4 min-w-[1rem] items-center justify-center rounded-full bg-green-500 px-1 text-[10px] font-medium text-white">
      {count}
    </span>
  );
}

/** Always-visible toggle. Mount this in PipelineContent's tab bar so users can
 *  reopen the drawer regardless of layout / transformed ancestors. */
export function PipelineCopilotToggle() {
  const [open, setOpen] = useRecoilState(pipelineCopilotOpenAtom);
  const taskId = useRecoilValue(pipelineSelectedTaskIdAtom);
  const drafts = usePipelineAgentDrafts(taskId);
  const draftCount =
    drafts.data?.drafts?.filter((d) => d.status === 'pending').length ?? 0;
  return (
    <button
      type="button"
      onClick={() => setOpen(!open)}
      className={cn(
        'inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium transition-colors',
        open
          ? 'border-green-500/40 bg-green-500/10 text-green-400'
          : 'border-border-light text-text-secondary hover:bg-surface-hover hover:text-text-primary',
      )}
      aria-label={open ? '收起 Pipeline Copilot' : '打开 Pipeline Copilot'}
      aria-pressed={open}
    >
      <MessageSquare className="h-3.5 w-3.5" />
      Copilot
      <DraftBadge count={draftCount} />
    </button>
  );
}

export default function PipelineCopilot() {
  const [open, setOpen] = useRecoilState(pipelineCopilotOpenAtom);
  const taskId = useRecoilValue(pipelineSelectedTaskIdAtom);
  const channelId = useRecoilValue(pipelineSelectedChannelIdAtom);
  const runId = useRecoilValue(pipelineSelectedRunIdAtom);
  const [prefill, setPrefill] = useRecoilState(pipelineCopilotPrefillAtom);
  const qc = useQueryClient();

  const [question, setQuestion] = useState('');
  const [turns, setTurns] = useState<CopilotTurn[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  const drafts = usePipelineAgentDrafts(taskId);

  useEffect(() => {
    if (!prefill) return;
    setOpen(true);
    setQuestion(prefill.question);
    setPrefill(null);
  }, [prefill, setOpen, setPrefill]);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [turns, isStreaming]);

  const invalidateAfterCopilot = useCallback(() => {
    if (!taskId) return;
    qc.invalidateQueries(PIPELINE_QUERY_KEYS.agentDrafts(taskId));
    qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
    qc.invalidateQueries(PIPELINE_QUERY_KEYS.timeline(taskId));
    if (channelId) {
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
    }
  }, [qc, taskId, channelId]);

  const submit = () => {
    const q = question.trim();
    if (!q || !taskId || isStreaming) return;
    const userTurn: CopilotTurn = {
      id: `${Date.now()}-u`,
      role: 'user',
      question: q,
      ts: Date.now(),
    };
    const assistantId = `${Date.now()}-a`;
    const assistantPlaceholder: CopilotTurn = {
      id: assistantId,
      role: 'assistant',
      replyStreaming: {
        streaming: true,
        thoughts: [],
        tool_calls: [],
        answer: '',
      },
      ts: Date.now(),
    };
    const priorHistory = copilotHistoryFromTurns(turns);
    setTurns((prev) => [...prev, userTurn, assistantPlaceholder]);
    setQuestion('');
    setIsStreaming(true);

    void PipelineApi.agentAskStream(
      {
        task_id: taskId,
        channel_id: channelId,
        run_id: runId,
        question: q,
        ...(priorHistory.length ? { history: priorHistory } : {}),
      },
      (ev: PipelineCopilotStreamEvent) => {
        setTurns((prev) =>
          prev.map((t) => {
            if (t.id !== assistantId || !t.replyStreaming) return t;
            const rs = t.replyStreaming;
            if (ev.type === 'thought') {
              return {
                ...t,
                replyStreaming: { ...rs, thoughts: [...rs.thoughts, ev.step] },
              };
            }
            if (ev.type === 'tool_call') {
              return {
                ...t,
                replyStreaming: {
                  ...rs,
                  tool_calls: [
                    ...rs.tool_calls,
                    {
                      name: ev.name,
                      args: ev.args,
                      result: ev.result,
                      elapsed_ms: ev.elapsed_ms,
                    },
                  ],
                },
              };
            }
            if (ev.type === 'answer') {
              return { ...t, replyStreaming: { ...rs, answer: ev.answer } };
            }
            if (ev.type === 'done') {
              invalidateAfterCopilot();
              return { ...t, reply: ev.reply, replyStreaming: undefined };
            }
            return t;
          }),
        );
      },
    ).catch((err: Error) => {
      const msg = err?.message?.trim() || '请求失败';
      setTurns((prev) =>
        prev.map((t) =>
          t.id === assistantId && t.replyStreaming
            ? {
                ...t,
                replyStreaming: { ...t.replyStreaming, streaming: false, error: msg },
              }
            : t,
        ),
      );
    }).finally(() => setIsStreaming(false));
  };

  const draftCount = drafts.data?.drafts?.filter((d) => d.status === 'pending').length ?? 0;

  const ctxLabel = useMemo(() => {
    const parts: string[] = [];
    if (taskId) parts.push(`task ${taskId.slice(0, 8)}`);
    if (channelId) parts.push(`channel ${channelId}`);
    if (runId) parts.push(`run ${runId.slice(0, 8)}`);
    return parts.join(' · ') || '未选择任务';
  }, [taskId, channelId, runId]);

  // Drawer is closed → render nothing here; the toggle (in tab bar) reopens it.
  if (!open) return null;

  // Render the drawer via Portal so it always escapes any transformed/overflow
  // ancestor in the PeCause shell. This was the root cause of the original bug
  // where the floating toggle disappeared after closing — its parent layout
  // created a containing block for `position:fixed` and clipped the button.
  if (typeof document === 'undefined') return null;

  const drawer = (
    <div
      className={cn(
        'fixed right-0 top-0 z-[60] flex h-full w-[420px] flex-col',
        'border-l border-border-light bg-surface-primary shadow-2xl',
      )}
      role="dialog"
      aria-label="Pipeline Copilot"
    >
      <div className="flex items-center justify-between border-b border-border-light px-4 py-3">
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-green-500" />
          <div>
            <div className="text-sm font-semibold text-text-primary">
              Pipeline Copilot
              <DraftBadge count={draftCount} />
            </div>
            <div className="font-mono text-[10px] text-text-secondary">
              {ctxLabel}
            </div>
          </div>
        </div>
        <button
          type="button"
          onClick={() => setOpen(false)}
          className="rounded-md p-1 text-text-secondary hover:bg-surface-secondary"
          aria-label="关闭"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div
        ref={scrollRef}
        className="flex-1 space-y-3 overflow-y-auto bg-surface-secondary px-3 py-3"
      >
        {turns.length === 0 ? (
          <div className="rounded-lg border border-dashed border-border-medium p-4 text-xs text-text-secondary">
            <div className="font-medium text-text-primary">使用说明</div>
            <ul className="mt-2 list-inside list-disc space-y-1">
              <li>在<strong>渠道详情 / 校验报告</strong>点 “问 Agent” 可带上下文。</li>
              <li>问题示例：「自有流水 JPM 备注校验为什么是 warning？」</li>
              <li>Agent 会调用工具读文件/规则/日志后给出依据。</li>
              <li>所有「写」操作（替换文件、改规则）都以 <strong>草稿</strong> 形式提交，由 Human 在规则页/文件页确认。</li>
            </ul>
          </div>
        ) : (
          turns.map((t) => <TurnView key={t.id} turn={t} />)
        )}
      </div>

      <div className="border-t border-border-light bg-surface-primary p-3">
        <div className="flex items-end gap-2">
          <textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) submit();
            }}
            rows={2}
            placeholder={
              taskId
                ? '提问（Cmd+Enter 发送）…'
                : '请先在总览选择一个任务'
            }
            disabled={!taskId || isStreaming}
            className={cn(
              'flex-1 resize-none rounded-md border border-border-light bg-surface-primary px-3 py-2',
              'text-sm text-text-primary placeholder:text-text-secondary focus:border-green-500 focus:outline-none',
              'disabled:opacity-60',
            )}
            aria-label="copilot input"
          />
          <button
            type="button"
            onClick={submit}
            disabled={!taskId || !question.trim() || isStreaming}
            className={cn(
              'inline-flex items-center gap-1 rounded-md bg-green-500 px-3 py-2 text-sm text-white',
              'hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-50',
            )}
            aria-label="发送"
          >
            <Send className="h-3.5 w-3.5" />
            发送
          </button>
        </div>
        {drafts.data?.drafts?.length ? (
          <details className="mt-2 rounded-md border border-green-500/30 bg-green-500/5 p-2 text-xs text-text-primary">
            <summary className="cursor-pointer font-medium">
              待 Human 确认的草稿（{drafts.data.drafts.length}）
            </summary>
            <ul className="mt-2 space-y-1.5">
              {drafts.data.drafts.map((d) => (
                <li
                  key={d.draft_id}
                  className="rounded border border-border-light bg-surface-primary p-2"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-mono text-[10px]">{d.draft_id}</span>
                    <span className="text-[10px] text-text-secondary">{d.status}</span>
                  </div>
                  <div className="mt-1 text-text-primary">
                    {d.kind === 'rule_patch'
                      ? `规则补丁 · ${d.rule_kind}`
                      : d.kind === 'replace_file'
                      ? `替换文件 · ${d.channel_id} / ${d.rel_path}`
                      : d.kind}
                  </div>
                  {d.rationale && (
                    <div className="mt-1 text-text-secondary">{d.rationale}</div>
                  )}
                </li>
              ))}
            </ul>
          </details>
        ) : null}
      </div>
    </div>
  );

  return createPortal(drawer, document.body);
}
