import React, { useEffect, useRef, useState } from 'react';
import axios from 'axios';
import { Pause, Play, RotateCcw } from 'lucide-react';
import { cn } from '~/utils';

/** 原生 EventSource 无法携带 Authorization，pipeline 路由需要 Bearer JWT → 用 fetch 读 SSE。 */
function parseSseBlocks(buffer: string): { rest: string; blocks: string[] } {
  const blocks: string[] = [];
  let carry = buffer;
  let idx = carry.indexOf('\n\n');
  while (idx >= 0) {
    const block = carry.slice(0, idx);
    carry = carry.slice(idx + 2);
    if (block.trim()) {
      blocks.push(block);
    }
    idx = carry.indexOf('\n\n');
  }
  return { rest: carry, blocks };
}

function handleSseBlock(
  block: string,
  onLog: (text: string) => void,
) {
  let eventType = 'message';
  const dataLines: string[] = [];
  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) {
      eventType = line.slice(6).trim();
    } else if (line.startsWith('data:')) {
      dataLines.push(line.slice(5).replace(/^\s/, ''));
    }
  }
  if (eventType === 'log' && dataLines.length) {
    try {
      const data = JSON.parse(dataLines.join('\n'));
      onLog(String(data.line ?? ''));
    } catch {
      /* ignore */
    }
  }
}

interface LogLine {
  ts: number;
  text: string;
}

const LEVEL_PALETTE: Record<string, string> = {
  ERROR: 'text-text-primary',
  WARNING: 'text-green-500',
  INFO: 'text-text-secondary',
  DEBUG: 'text-text-secondary opacity-60',
};

function detectLevel(line: string): string | null {
  const m = line.match(/\|\s*(ERROR|WARNING|INFO|DEBUG)\s*\|/);
  return m ? m[1] : null;
}

/** 与 orchestrator 中「── run … · 开始/结束」分隔行对应，便于过滤单次 run */
function isRunStartLine(text: string, runIdPrefix: string): boolean {
  if (!runIdPrefix) return false;
  const p = runIdPrefix.trim();
  if (!p) return false;
  return new RegExp(`── run\\s+${p}\\s*·\\s*开始`).test(text);
}

function isRunEndLine(text: string, runIdPrefix: string): boolean {
  if (!runIdPrefix) return false;
  const p = runIdPrefix.trim();
  if (!p) return false;
  return new RegExp(`── run\\s+${p}\\s*·\\s*结束`).test(text);
}

/** 与 orchestrator 中「── run … · 开始/结束」分隔行对应，便于多批次日志扫读 */
function isRunBatchBoundary(text: string): boolean {
  return text.includes('── run ') && /· (开始|结束)/.test(text);
}

export default function ChannelLogTab({
  taskId,
  channelId,
  /** 任务级 orchestrator 日志（全渠道汇总），默认仍为单渠道流 */
  scope = 'channel',
  /**
   * 仅展示与该 run 前 8 位 ID 匹配的「── run … · 开始」至「── run … · 结束」之间的行
   * （与 orchestrator 日志格式一致）。用于分摊页只看待某次 cost_allocate。
   */
  runIdPrefixFilter,
}: {
  taskId: string;
  channelId?: string;
  scope?: 'channel' | 'task';
  runIdPrefixFilter?: string;
}) {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [paused, setPaused] = useState(false);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const tailRef = useRef<HTMLDivElement | null>(null);
  /** 是否在目标 run 的「开始」与「结束」之间（含两端边界行） */
  const withinRunRef = useRef(false);

  useEffect(() => {
    setLines([]);
    setError(null);
    withinRunRef.current = false;
  }, [taskId, channelId, scope, runIdPrefixFilter]);

  useEffect(() => {
    if (paused) {
      setConnected(false);
      return;
    }
    setError(null);
    const url =
      scope === 'task'
        ? `/api/pipeline/tasks/${encodeURIComponent(taskId)}/logs/stream`
        : `/api/pipeline/tasks/${encodeURIComponent(
            taskId,
          )}/channels/${encodeURIComponent(channelId ?? '')}/logs/stream`;
    if (scope === 'channel' && !channelId) {
      return;
    }
    const prefix = (runIdPrefixFilter ?? '').trim();
    if (scope === 'channel' && runIdPrefixFilter !== undefined && !prefix) {
      return;
    }

    const appendLogLine = (text: string) => {
      if (scope === 'channel' && prefix) {
        if (isRunStartLine(text, prefix)) withinRunRef.current = true;
        if (!withinRunRef.current) return;
      }
      setLines((prev) => {
        const next = [...prev, { ts: Date.now(), text }];
        if (next.length > 5000) next.splice(0, next.length - 5000);
        return next;
      });
      if (scope === 'channel' && prefix && isRunEndLine(text, prefix)) {
        withinRunRef.current = false;
      }
    };

    const ac = new AbortController();
    let cancelled = false;

    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

    void (async function connectLoop() {
      let sseCarry = '';
      while (!cancelled && !ac.signal.aborted) {
        try {
          const authRaw = axios.defaults.headers.common?.Authorization;
          const headers: Record<string, string> = { Accept: 'text/event-stream' };
          if (typeof authRaw === 'string' && authRaw.trim()) {
            headers.Authorization = authRaw;
          }
          const res = await fetch(url, {
            credentials: 'include',
            headers,
            signal: ac.signal,
          });
          if (!res.ok) {
            setConnected(false);
            const body = await res.text().catch(() => '');
            const hint =
              res.status === 401
                ? '未授权（请重新登录）'
                : `日志流 HTTP ${res.status}${body ? ` · ${body.slice(0, 120)}` : ''}`;
            setError(`${hint}，将自动重连`);
            await sleep(2500);
            continue;
          }
          if (!res.body) {
            setConnected(false);
            setError('日志流无响应体，将自动重连');
            await sleep(2500);
            continue;
          }
          setConnected(true);
          setError(null);
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          sseCarry = '';
          while (!cancelled && !ac.signal.aborted) {
            const { done, value } = await reader.read();
            if (done) break;
            sseCarry += decoder.decode(value, { stream: true });
            const { rest, blocks } = parseSseBlocks(sseCarry);
            sseCarry = rest;
            for (const block of blocks) {
              handleSseBlock(block, appendLogLine);
            }
          }
        } catch {
          if (cancelled || ac.signal.aborted) break;
          setConnected(false);
          setError('日志流断开，将自动重连');
          await sleep(2500);
        }
      }
      setConnected(false);
    })();

    return () => {
      cancelled = true;
      ac.abort();
    };
  }, [paused, taskId, channelId, scope, runIdPrefixFilter]);

  useEffect(() => {
    tailRef.current?.scrollTo({ top: tailRef.current.scrollHeight });
  }, [lines]);

  const prefix = (runIdPrefixFilter ?? '').trim();
  const filterActive = scope === 'channel' && runIdPrefixFilter !== undefined && !!prefix;
  const waitingForFilterRun = scope === 'channel' && runIdPrefixFilter !== undefined && !prefix;

  return (
    <div className="flex h-full flex-col p-4">
      <div className="mb-2 flex items-center justify-between">
        <div className="text-xs text-text-secondary">
          {scope === 'task' && (
            <span className="mr-2 text-[11px] text-text-tertiary">任务级日志</span>
          )}
          {filterActive ? (
            <span className="mr-2 text-[11px] text-text-tertiary">
              仅 run <span className="font-mono">{prefix}</span>（单次执行区间）
            </span>
          ) : null}
          {connected ? (
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block h-1.5 w-1.5 rounded-full bg-green-500" />
              已连接
            </span>
          ) : (
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-block h-1.5 w-1.5 rounded-full bg-border-medium" />
              {waitingForFilterRun ? '未连接' : '未连接'}
            </span>
          )}
          <span className="ml-2">{lines.length} 行</span>
          {error && <span className="ml-3 text-text-primary">{error}</span>}
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setPaused((p) => !p)}
            disabled={waitingForFilterRun}
            className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover disabled:opacity-40"
          >
            {paused ? <Play className="h-3 w-3" /> : <Pause className="h-3 w-3" />}
            {paused ? '恢复' : '暂停'}
          </button>
          <button
            type="button"
            onClick={() => {
              setLines([]);
              withinRunRef.current = false;
            }}
            className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
          >
            <RotateCcw className="h-3 w-3" />
            清屏
          </button>
        </div>
      </div>
      <div
        ref={tailRef}
        className="flex-1 overflow-auto rounded-lg border border-border-light bg-surface-secondary p-3 font-mono text-[11px] leading-5"
      >
        {waitingForFilterRun ? (
          <div className="text-text-secondary">
            暂无分摊运行记录。点击「执行分摊」后，此处仅展示该次 cost_allocate 对应的日志区间（不含 QuickBI / CitiHK / 合并 等其它步骤）。
          </div>
        ) : lines.length === 0 ? (
          <div className="text-text-secondary">等待日志…</div>
        ) : (
          lines.map((l, i) => {
            const lvl = detectLevel(l.text);
            const boundary = isRunBatchBoundary(l.text);
            return (
              <div
                key={`${l.ts}-${i}`}
                className={cn(
                  'whitespace-pre-wrap break-all',
                  i > 0 && boundary && 'mt-2 border-t border-border-light/90 pt-2',
                  lvl ? LEVEL_PALETTE[lvl] : 'text-text-primary',
                )}
              >
                {l.text}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
