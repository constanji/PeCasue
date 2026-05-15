import React, { useEffect, useMemo, useState } from 'react';
import { MessageSquare } from 'lucide-react';
import { cn } from '~/utils';

/** 仅渲染当前页 DOM，避免一次挂载数千行导致卡顿 */
const RULE_VERIFY_PAGE_SIZE = 125;

/** PeCause API: metrics.own_flow_processing_verify */
export type OwnFlowProcessingVerifyPayload = {
  schema?: string;
  counts: {
    total: number;
    pass: number;
    warn: number;
    pending: number;
    /** 本期汇总无对应来源文件，跳过核对 */
    na?: number;
  };
  pass_rate: number;
  rules: OwnFlowRuleVerifyRow[];
};

export type OwnFlowRuleVerifyRow = {
  规则序号?: number;
  渠道?: string;
  主体?: string;
  文件?: string;
  条件?: string;
  期望备注?: string;
  期望入账科目?: string;
  状态?: string;
  说明?: string;
  命中行数?: number;
  不一致数?: number;
  问题行总数?: number;
  问题行预览?: Record<string, unknown>[];
};

type TabKey = 'pass' | 'warn' | 'pending' | 'na';

const TAB_LABEL: Record<TabKey, string> = {
  pass: '通过',
  warn: '警告',
  pending: '待核算',
  na: '不适用',
};

function statusToTab(s: string | undefined): TabKey {
  if (s === '警告') return 'warn';
  if (s === '待核算') return 'pending';
  if (s === '不适用') return 'na';
  return 'pass';
}

export default function OwnFlowVerifyReport({
  payload,
  onAskAgent,
}: {
  payload: OwnFlowProcessingVerifyPayload;
  onAskAgent?: (rule: OwnFlowRuleVerifyRow) => void;
}) {
  const [tab, setTab] = useState<TabKey>('pass');
  const [page, setPage] = useState(0);

  const { counts, pass_rate, rules } = payload;
  const naCount = counts.na ?? 0;
  const filtered = useMemo(
    () => rules.filter((r) => statusToTab(r.状态) === tab),
    [rules, tab],
  );

  useEffect(() => {
    setPage(0);
  }, [rules, tab]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / RULE_VERIFY_PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  const sliceStart = safePage * RULE_VERIFY_PAGE_SIZE;
  const sliceEnd = Math.min(sliceStart + RULE_VERIFY_PAGE_SIZE, filtered.length);
  const pageRows = useMemo(
    () => filtered.slice(sliceStart, sliceEnd),
    [filtered, sliceStart, sliceEnd],
  );

  useEffect(() => {
    setPage((p) => Math.min(p, Math.max(0, pageCount - 1)));
  }, [pageCount]);

  const pctDisplay = `${(pass_rate * 100).toFixed(1)}%`;

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-lg font-semibold text-text-primary">验证报告</h3>
        <p className="mt-1 text-xs text-text-secondary">
          与 Allline「执行匹配」一致：按处理表每条规则对照汇总结果（备注 / 入账科目）。「不适用」表示本期汇总中未包含该规则对应来源文件，已跳过核对；通过率按排除不适用后的规则数计算。
        </p>
      </div>

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        <SummaryCard label="规则总数" value={counts.total} />
        <SummaryCard label="通过" value={counts.pass} accent="text-green-400" />
        <SummaryCard label="警告" value={counts.warn} accent="text-amber-500" />
        <SummaryCard label="待核算" value={counts.pending} accent="text-blue-400" />
        <SummaryCard label="不适用" value={naCount} accent="text-slate-400" />
      </div>

      <div className="space-y-2">
        <div className="flex items-center justify-between text-xs text-text-secondary">
          <span>通过率</span>
          <span className="font-medium text-text-primary">{pctDisplay}</span>
        </div>
        <div className="h-2 overflow-hidden rounded-full bg-surface-secondary">
          <div
            className="h-full rounded-full bg-blue-500 transition-[width]"
            style={{ width: `${Math.min(100, Math.max(0, pass_rate * 100))}%` }}
          />
        </div>
      </div>

      <div className="flex flex-wrap gap-1 border-b border-border-light pb-0">
        {(['pass', 'warn', 'pending', 'na'] as const).map((k) => {
          const n =
            k === 'pass'
              ? counts.pass
              : k === 'warn'
                ? counts.warn
                : k === 'pending'
                  ? counts.pending
                  : naCount;
          const active = tab === k;
          return (
            <button
              key={k}
              type="button"
              onClick={() => setTab(k)}
              className={cn(
                '-mb-px border-b-2 px-3 py-2 text-sm transition-colors',
                active
                  ? 'border-red-500 font-medium text-text-primary'
                  : 'border-transparent text-text-secondary hover:text-text-primary',
              )}
            >
              {TAB_LABEL[k]}（{n}）
            </button>
          );
        })}
      </div>

      <div className="rounded-lg border border-border-light bg-surface-secondary/60 px-3 py-2 text-[11px] leading-snug text-text-secondary">
        <span className="font-medium text-text-primary">规则明细</span>
        ：全部{' '}
        <span className="tabular-nums text-green-400">{counts.total}</span> 条 · 当前「
        {TAB_LABEL[tab]}」分类{' '}
        <span className="tabular-nums text-text-primary">{filtered.length}</span> 条
        {filtered.length > RULE_VERIFY_PAGE_SIZE ? (
          <>
            {' '}
            · 每页 {RULE_VERIFY_PAGE_SIZE} 条，翻到下一页时再渲染后续行（减轻卡顿）
          </>
        ) : null}
        {filtered.length > 0 ? (
          <>
            {' '}
            · 本页{' '}
            <span className="tabular-nums text-text-primary">
              {sliceStart + 1}–{sliceEnd}
            </span>
          </>
        ) : null}
      </div>

      <div className="overflow-x-auto rounded-lg border border-border-light">
        <table className="w-full text-left text-sm text-text-primary">
          <thead className="bg-surface-secondary text-xs text-text-secondary">
            <tr>
              <th className="px-2 py-2 font-medium">规则序号</th>
              <th className="px-2 py-2 font-medium">渠道</th>
              <th className="px-2 py-2 font-medium">主体</th>
              <th className="px-2 py-2 font-medium">文件</th>
              <th className="min-w-[180px] px-2 py-2 font-medium">条件</th>
              <th className="px-2 py-2 font-medium">期望备注</th>
              <th className="px-2 py-2 font-medium">期望入账科目</th>
              <th className="px-2 py-2 font-medium">状态</th>
              <th className="min-w-[140px] px-2 py-2 font-medium">说明</th>
              {onAskAgent ? (
                <th className="w-24 px-2 py-2 text-right font-medium">操作</th>
              ) : null}
            </tr>
          </thead>
          <tbody className="divide-y divide-border-light">
            {filtered.length === 0 ? (
              <tr>
                <td
                  colSpan={onAskAgent ? 10 : 9}
                  className="px-3 py-8 text-center text-text-secondary"
                >
                  该分类下暂无规则
                </td>
              </tr>
            ) : (
              pageRows.map((r, idx) => (
                <tr
                  key={`${r.规则序号}-${sliceStart + idx}`}
                  className="bg-surface-primary/40 hover:bg-surface-secondary/60"
                >
                  <td className="px-2 py-2 font-mono text-xs text-text-secondary">{r.规则序号 ?? '—'}</td>
                  <td className="px-2 py-2 text-xs text-text-primary">{r.渠道 ?? '—'}</td>
                  <td className="px-2 py-2 text-xs text-text-primary">{r.主体 ?? '—'}</td>
                  <td className="px-2 py-2 text-xs text-text-primary">{r.文件 ?? '—'}</td>
                  <td className="px-2 py-2 font-mono text-[11px] leading-snug text-text-secondary">
                    {r.条件 ?? '—'}
                  </td>
                  <td className="px-2 py-2 text-xs text-text-primary">{r.期望备注 ?? '—'}</td>
                  <td className="px-2 py-2 text-xs text-text-primary">{r.期望入账科目 ?? '—'}</td>
                  <td className="px-2 py-2">
                    <StatusBadge status={r.状态} />
                  </td>
                  <td className="px-2 py-2 text-xs text-text-secondary">{r.说明 ?? '—'}</td>
                  {onAskAgent ? (
                    <td className="px-2 py-2 text-right">
                      <button
                        type="button"
                        onClick={() => onAskAgent(r)}
                        className="inline-flex items-center gap-1 rounded-md border border-green-500/40 bg-green-500/5 px-2 py-1 text-[11px] text-green-400 hover:bg-green-500/15"
                      >
                        <MessageSquare className="h-3 w-3" />
                        Agent
                      </button>
                    </td>
                  ) : null}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {pageCount > 1 ? (
        <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border-light bg-surface-secondary/40 px-3 py-2 text-[11px] text-text-secondary">
          <span>
            第 <span className="font-mono text-text-primary">{safePage + 1}</span> /{' '}
            <span className="font-mono text-text-primary">{pageCount}</span> 页 · 本分类{' '}
            <span className="font-mono text-text-primary">{filtered.length}</span> 条
          </span>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              disabled={safePage <= 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              className="rounded-md border border-border-light px-2 py-1 hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              上一页
            </button>
            <button
              type="button"
              disabled={safePage >= pageCount - 1}
              onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
              className="rounded-md border border-border-light px-2 py-1 hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-40"
            >
              下一页
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SummaryCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: number;
  accent?: string;
}) {
  return (
    <div className="rounded-lg border border-border-light bg-surface-primary px-3 py-3">
      <div className="text-xs text-text-secondary">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${accent ?? 'text-text-primary'}`}>
        {value}
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status?: string }) {
  const s = status ?? '';
  if (s === '通过') {
    return <span className="rounded bg-green-500/15 px-2 py-0.5 text-xs text-green-400">通过</span>;
  }
  if (s === '警告') {
    return <span className="rounded bg-amber-500/15 px-2 py-0.5 text-xs text-amber-400">警告</span>;
  }
  if (s === '待核算') {
    return <span className="rounded bg-blue-500/15 px-2 py-0.5 text-xs text-blue-300">待核算</span>;
  }
  if (s === '不适用') {
    return <span className="rounded bg-slate-500/15 px-2 py-0.5 text-xs text-slate-400">不适用</span>;
  }
  return <span className="text-xs text-text-secondary">{s || '—'}</span>;
}
