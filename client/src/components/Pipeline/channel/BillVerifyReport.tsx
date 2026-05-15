import React, { useMemo, useState } from 'react';
import { cn } from '~/utils';

/** Structured bill merge log viewer (PeCause Pipeline verify tab). */

export interface BillMergeReportSummary {
  success: number;
  empty: number;
  warn: number;
  ignored_folders: number;
}

export interface BillMergeReportBlock {
  key: string;
  body: string;
  rows: number | null;
  tags: string[];
}

export interface BillMergeReportPayload {
  summary: BillMergeReportSummary;
  blocks: BillMergeReportBlock[];
  raw_log: string;
}

type BillLogTab = 'ok' | 'empty' | 'warn' | 'all' | 'raw';

function expanderTitle(b: BillMergeReportBlock): string {
  const tagS = b.tags.length ? [...b.tags].sort().join(' · ') : '';
  if (b.rows != null) {
    return tagS
      ? `${b.key} — 对齐 ${b.rows} 行  [${tagS}]`
      : `${b.key} — 对齐 ${b.rows} 行`;
  }
  const first = (b.body.split(/\r?\n/) || [''])[0] ?? '';
  const h = first.length > 100 ? `${first.slice(0, 100)}…` : first;
  return tagS ? `${b.key} — ${h}  [${tagS}]` : `${b.key} — ${h}`;
}

function SummaryMetric({
  label,
  value,
}: {
  label: string;
  value: number;
}) {
  return (
    <div className="rounded-lg border border-border-light bg-surface-primary px-3 py-2.5">
      <div className="text-[11px] text-text-secondary">{label}</div>
      <div className="mt-0.5 text-lg font-semibold tabular-nums text-text-primary">{value}</div>
    </div>
  );
}

function LogBlockCard({
  block,
  defaultOpen,
}: {
  block: BillMergeReportBlock;
  defaultOpen: boolean;
}) {
  return (
    <details
      open={defaultOpen}
      className={cn(
        'rounded-lg border border-border-light bg-surface-secondary/40',
        '[&_summary::-webkit-details-marker]:hidden',
      )}
    >
      <summary className="cursor-pointer select-none px-3 py-2 text-xs font-medium text-text-primary hover:bg-surface-hover/60">
        <span className="inline-flex items-center gap-1.5">
          <span className="text-text-secondary">▸</span>
          {expanderTitle(block)}
        </span>
      </summary>
      <pre className="max-h-72 overflow-auto border-t border-border-light bg-surface-primary/80 px-3 py-2 font-mono text-[11px] leading-relaxed text-text-secondary">
        {block.body.trimEnd()}
      </pre>
    </details>
  );
}

export default function BillVerifyReport({ payload }: { payload: BillMergeReportPayload }) {
  const [tab, setTab] = useState<BillLogTab>('all');

  const { summary, blocks, raw_log: rawLog } = payload;

  const tabBlocks = useMemo(() => {
    const ok = blocks.filter((b) => b.tags.includes('success'));
    const empty = blocks.filter((b) => b.tags.includes('empty'));
    const warn = blocks.filter((b) => b.tags.includes('warn'));
    return { ok, empty, warn, all: blocks };
  }, [blocks]);

  const warnDefaultOpen = tabBlocks.warn.length > 0 && tabBlocks.warn.length <= 3;

  const tabBtn = (id: BillLogTab, label: React.ReactNode, warnAccent?: boolean) => (
    <button
      key={id}
      type="button"
      onClick={() => setTab(id)}
      className={cn(
        'whitespace-nowrap border-b-2 px-2 py-2 text-xs font-medium transition-colors',
        tab === id
          ? warnAccent
            ? 'border-amber-400 text-amber-300'
            : 'border-green-400 text-text-primary'
          : warnAccent
            ? 'border-transparent text-amber-400/90 hover:text-amber-300'
            : 'border-transparent text-text-secondary hover:text-text-primary',
      )}
    >
      {label}
    </button>
  );

  return (
    <div className="overflow-hidden rounded-lg border border-border-light bg-surface-primary">
      <div className="border-b border-border-light bg-surface-secondary/50 px-3 py-2 text-xs font-medium text-text-primary">
        账单合并日志（zhangdan / Allline 同款视图）
      </div>

      <div className="space-y-3 p-3">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          <SummaryMetric label="成功提取(段数)" value={summary.success} />
          <SummaryMetric label="未提取/跳过(段数)" value={summary.empty} />
          <SummaryMetric label="含警告(段数)" value={summary.warn} />
          <SummaryMetric label="忽略文件夹(段数)" value={summary.ignored_folders} />
        </div>
        <p className="text-[11px] leading-relaxed text-text-secondary">
          段数 = 以 <span className="font-mono text-text-primary">[渠道]</span> 或{' '}
          <span className="font-mono text-text-primary">[忽略]</span>{' '}
          划分的块数；同一段可同时计入多个统计（与 Allline Streamlit 一致）。
        </p>

        <div className="flex flex-wrap gap-x-1 gap-y-0 border-b border-border-light">
          {tabBtn('ok', <>成功提取 ({summary.success})</>)}
          {tabBtn('empty', <>未提取/跳过 ({summary.empty})</>)}
          {tabBtn('warn', <>警告/异常 ({summary.warn})</>, true)}
          {tabBtn('all', <>按渠道(全部) ({blocks.length})</>)}
          {tabBtn('raw', '完整原文')}
        </div>

        <div className="max-h-[min(55vh,28rem)] space-y-2 overflow-y-auto pr-1">
          {tab === 'ok' &&
            (tabBlocks.ok.length === 0 ? (
              <div className="rounded-md border border-dashed border-border-medium px-3 py-6 text-center text-xs text-text-secondary">
                无「成功提取并对齐」类段落。
              </div>
            ) : (
              tabBlocks.ok.map((b, i) => (
                <LogBlockCard key={`${b.key}-${i}`} block={b} defaultOpen={false} />
              ))
            ))}

          {tab === 'empty' &&
            (tabBlocks.empty.length === 0 ? (
              <div className="rounded-md border border-dashed border-border-medium px-3 py-6 text-center text-xs text-text-secondary">
                无未提取/跳过/空数据类段落。
              </div>
            ) : (
              tabBlocks.empty.map((b, i) => (
                <LogBlockCard key={`${b.key}-${i}`} block={b} defaultOpen={false} />
              ))
            ))}

          {tab === 'warn' &&
            (tabBlocks.warn.length === 0 ? (
              <div className="rounded-md border border-dashed border-border-medium px-3 py-6 text-center text-xs text-text-secondary">
                无警告/失败/异常类段落。
              </div>
            ) : (
              tabBlocks.warn.map((b, i) => (
                <LogBlockCard key={`${b.key}-${i}`} block={b} defaultOpen={warnDefaultOpen} />
              ))
            ))}

          {tab === 'all' && (
            <>
              <p className="text-[11px] text-text-secondary">
                按运行顺序列出所有分段；标签与上方页签统计一致。
              </p>
              {blocks.map((b, i) => (
                <LogBlockCard key={`${b.key}-all-${i}`} block={b} defaultOpen={false} />
              ))}
            </>
          )}

          {tab === 'raw' && (
            <>
              <p className="text-[11px] text-text-secondary">
                单次合并完整 stdout，便于复制或外部 diff。
              </p>
              <pre className="max-h-[min(50vh,24rem)] overflow-auto rounded-md border border-border-light bg-surface-secondary/60 p-3 font-mono text-[11px] leading-relaxed text-text-secondary">
                {(rawLog || '').trimEnd()}
              </pre>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
