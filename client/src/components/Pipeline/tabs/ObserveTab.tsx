import React, { useMemo, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock,
  Sparkles,
} from 'lucide-react';
import {
  usePipelineObserveCharts,
  usePipelineObserveEvents,
  usePipelineObserveKpi,
  type PipelineObserveEvent,
} from '~/data-provider';
import { cn } from '~/utils';

function KpiCard({
  icon,
  label,
  value,
  sub,
  tone = 'default',
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  sub?: string;
  tone?: 'default' | 'good' | 'warn';
}) {
  const cls =
    tone === 'good'
      ? 'border-green-500/30 bg-green-500/5'
      : tone === 'warn'
        ? 'border-green-500/30 bg-green-500/5'
        : 'border-border-light bg-surface-secondary';
  return (
    <div className={cn('flex items-center gap-3 rounded-lg border px-3 py-3', cls)}>
      <div className="flex h-9 w-9 items-center justify-center rounded-md bg-surface-primary text-text-secondary">
        {icon}
      </div>
      <div className="flex-1">
        <div className="text-xs text-text-secondary">{label}</div>
        <div className="text-2xl font-semibold text-text-primary">{value}</div>
        {sub && <div className="text-[11px] text-text-secondary">{sub}</div>}
      </div>
    </div>
  );
}

function HBar({ data, valueKey, labelKey, format = (v: number) => `${v}` }: {
  data: Array<Record<string, unknown>>;
  valueKey: string;
  labelKey: string;
  format?: (v: number) => string;
}) {
  if (data.length === 0) {
    return (
      <div className="rounded-md border border-dashed border-border-medium p-4 text-center text-xs text-text-secondary">
        暂无数据
      </div>
    );
  }
  const values = data.map((d) => Number(d[valueKey] ?? 0));
  const max = Math.max(...values, 1);
  return (
    <div className="space-y-1">
      {data.map((d, i) => {
        const v = Number(d[valueKey] ?? 0);
        const label = String(d[labelKey] ?? '');
        return (
          <div key={`${label}-${i}`} className="flex items-center gap-2 text-xs">
            <div className="w-28 truncate text-right text-text-secondary" title={label}>
              {label}
            </div>
            <div className="relative h-3 flex-1 overflow-hidden rounded bg-surface-secondary">
              <div
                className="h-full rounded bg-green-500"
                style={{ width: `${(v / max) * 100}%` }}
                aria-label={`${label}: ${v}`}
              />
            </div>
            <div className="w-14 text-text-primary">{format(v)}</div>
          </div>
        );
      })}
    </div>
  );
}

function StackedDayBar({
  data,
}: {
  data: Array<{ day: string; ends: number; failures: number; warnings: number }>;
}) {
  if (data.length === 0)
    return (
      <div className="rounded-md border border-dashed border-border-medium p-4 text-center text-xs text-text-secondary">
        暂无数据
      </div>
    );
  const max = Math.max(...data.map((d) => d.ends || 1), 1);
  return (
    <div className="flex h-32 items-end gap-1.5">
      {data.map((d) => {
        const total = d.ends || 1;
        const okH = ((d.ends - d.failures - d.warnings) / max) * 100;
        const warnH = (d.warnings / max) * 100;
        const failH = (d.failures / max) * 100;
        return (
          <div key={d.day} className="flex flex-1 flex-col items-center" title={`${d.day} · ok ${d.ends - d.failures - d.warnings} / warn ${d.warnings} / fail ${d.failures}`}>
            <div className="flex h-full w-full flex-col-reverse overflow-hidden rounded">
              <div className="bg-green-500/80" style={{ height: `${okH}%` }} />
              <div className="bg-green-500/70" style={{ height: `${warnH}%` }} />
              <div className="bg-text-primary/40" style={{ height: `${failH}%` }} />
            </div>
            <div className="mt-1 truncate text-[10px] text-text-secondary">
              {d.day.slice(5)}
            </div>
          </div>
        );
      })}
    </div>
  );
}

const EVENT_BADGE: Record<string, string> = {
  'channel.run.start': 'bg-green-500/10 text-green-500',
  'channel.run.end': 'bg-green-500/10 text-green-400',
  'channel.run.failed': 'bg-text-primary/10 text-text-primary',
  'channel.run.warning': 'bg-green-500/10 text-green-500',
  'agent.ask': 'bg-green-500/10 text-green-500',
  'agent.draft.proposed': 'bg-green-500/10 text-green-500',
  'audit.file.replaced': 'bg-green-500/10 text-green-500',
  'compare.completed': 'bg-green-500/10 text-green-400',
  status_changed: 'bg-surface-secondary text-text-secondary',
  step_changed: 'bg-surface-secondary text-text-secondary',
};

function EventRow({ ev }: { ev: PipelineObserveEvent }) {
  const cls = EVENT_BADGE[ev.event_type] ?? 'bg-surface-secondary text-text-secondary';
  return (
    <li className="flex items-start gap-2 border-b border-border-light px-3 py-2 text-xs last:border-0">
      <span
        className={cn('shrink-0 rounded-full px-1.5 py-0.5 font-mono text-[10px]', cls)}
      >
        {ev.event_type}
      </span>
      <div className="flex-1 min-w-0">
        <div className="flex flex-wrap items-center gap-x-2 text-text-primary">
          <span className="font-mono">{ev.task_id?.slice(0, 8)}</span>
          {ev.channel_id && <span className="font-mono">{ev.channel_id}</span>}
          {ev.run_id && <span className="font-mono text-text-secondary">{ev.run_id.slice(0, 8)}</span>}
          {ev.to_status && (
            <span className="text-text-secondary">→ {ev.to_status}</span>
          )}
        </div>
        {ev.reason_detail && (
          <div className="mt-0.5 truncate text-text-secondary" title={ev.reason_detail}>
            {ev.reason_detail}
          </div>
        )}
      </div>
      <span className="shrink-0 font-mono text-[10px] text-text-secondary">
        {new Date(ev.created_at).toLocaleTimeString()}
      </span>
    </li>
  );
}

export default function ObserveTab() {
  const [windowDays, setWindowDays] = useState<number>(7);
  const [eventFilter, setEventFilter] = useState<string>('');

  const kpi = usePipelineObserveKpi(1);
  const charts = usePipelineObserveCharts(windowDays);
  const events = usePipelineObserveEvents({
    limit: 200,
    event_type: eventFilter || undefined,
  });

  const ruleHits = useMemo(
    () =>
      (charts.data?.rule_events ?? []).map((r) => ({
        label: r.event,
        value: r.count,
      })),
    [charts.data?.rule_events],
  );

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border-light bg-surface-secondary px-4 py-2">
        <div className="flex items-center gap-3">
          <span className="text-xs text-text-secondary">窗口</span>
          <div className="flex gap-1">
            {[1, 7, 30].map((d) => (
              <button
                key={d}
                type="button"
                onClick={() => setWindowDays(d)}
                className={cn(
                  'rounded-md px-2 py-0.5 text-xs',
                  windowDays === d
                    ? 'bg-green-500 text-white'
                    : 'border border-border-light text-text-secondary hover:bg-surface-primary',
                )}
              >
                {d}d
              </button>
            ))}
          </div>
          <div className="ml-auto text-[11px] text-text-secondary">
            最后更新 {kpi.data ? new Date(kpi.data.as_of).toLocaleTimeString() : '—'}
          </div>
        </div>
      </div>

      <div className="flex-1 space-y-4 overflow-auto p-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-4">
          <KpiCard
            icon={<Activity className="h-4 w-4" />}
            label="今日任务数"
            value={`${kpi.data?.tasks_total ?? '—'}`}
            sub={
              kpi.data
                ? `成功 ${kpi.data.tasks_completed} · 部分 ${kpi.data.tasks_partial} · 失败 ${kpi.data.tasks_failed}`
                : undefined
            }
          />
          <KpiCard
            icon={<CheckCircle2 className="h-4 w-4" />}
            label="任务成功率"
            value={`${((kpi.data?.success_rate ?? 0) * 100).toFixed(1)}%`}
            tone={(kpi.data?.success_rate ?? 0) >= 0.8 ? 'good' : 'default'}
          />
          <KpiCard
            icon={<Clock className="h-4 w-4" />}
            label="平均耗时 (s)"
            value={`${kpi.data?.avg_duration_seconds ?? '—'}`}
          />
          <KpiCard
            icon={<Sparkles className="h-4 w-4" />}
            label="Agent 介入率"
            value={`${((kpi.data?.intervention_rate ?? 0) * 100).toFixed(1)}%`}
            sub={kpi.data ? `${kpi.data.agent_interventions} 次提问` : undefined}
            tone={(kpi.data?.intervention_rate ?? 0) > 0 ? 'warn' : 'default'}
          />
        </div>

        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <div className="rounded-lg border border-border-light bg-surface-primary p-3">
            <div className="mb-2 text-xs font-semibold text-text-primary">
              耗时分布（按渠道，平均秒）
            </div>
            <HBar
              data={(charts.data?.duration_by_channel ?? []) as unknown as Array<Record<string, unknown>>}
              valueKey="avg_seconds"
              labelKey="channel"
              format={(v) => `${v.toFixed(1)}s`}
            />
          </div>
          <div className="rounded-lg border border-border-light bg-surface-primary p-3">
            <div className="mb-2 text-xs font-semibold text-text-primary">
              失败 / 告警率（按日，绿=ok 紫=warn 黑=fail）
            </div>
            <StackedDayBar data={charts.data?.daily_failure_rate ?? []} />
          </div>
          <div className="rounded-lg border border-border-light bg-surface-primary p-3">
            <div className="mb-2 text-xs font-semibold text-text-primary">
              规则 / Agent 事件分布
            </div>
            <HBar data={ruleHits as unknown as Array<Record<string, unknown>>} valueKey="value" labelKey="label" />
          </div>
          <div className="rounded-lg border border-border-light bg-surface-primary p-3">
            <div className="mb-2 text-xs font-semibold text-text-primary">
              最易报错的渠道 / 文件 (Top 10)
            </div>
            {(charts.data?.top_error_files ?? []).length === 0 ? (
              <div className="rounded-md border border-dashed border-border-medium p-4 text-center text-xs text-text-secondary">
                <CheckCircle2 className="mx-auto mb-1 h-4 w-4 text-green-500" />
                暂无错误
              </div>
            ) : (
              <ul className="space-y-1">
                {(charts.data?.top_error_files ?? []).map((e, i) => (
                  <li
                    key={`${e.channel}-${i}`}
                    className="rounded border border-border-light bg-surface-secondary px-2 py-1 text-xs"
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono">{e.channel}</span>
                      <span className="text-text-secondary">×{e.count}</span>
                    </div>
                    <div className="mt-0.5 truncate text-text-secondary" title={e.detail}>
                      {e.detail}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        <div className="rounded-lg border border-border-light bg-surface-primary">
          <div className="flex items-center justify-between border-b border-border-light px-3 py-2">
            <div className="flex items-center gap-1 text-xs font-semibold text-text-primary">
              <AlertTriangle className="h-3.5 w-3.5 text-green-500" />
              实时事件流（最近 200）
            </div>
            <div className="flex items-center gap-2">
              <select
                value={eventFilter}
                onChange={(e) => setEventFilter(e.target.value)}
                className="rounded-md border border-border-light bg-surface-primary px-2 py-0.5 text-xs"
                aria-label="事件过滤"
              >
                <option value="">全部事件</option>
                <option value="channel.run.start">run.start</option>
                <option value="channel.run.end">run.end</option>
                <option value="channel.run.failed">run.failed</option>
                <option value="channel.run.warning">run.warning</option>
                <option value="agent.ask">agent.ask</option>
                <option value="agent.draft.proposed">agent.draft</option>
                <option value="audit.file.replaced">file.replaced</option>
                <option value="compare.completed">compare.completed</option>
              </select>
            </div>
          </div>
          <ul className="max-h-96 overflow-auto">
            {(events.data?.events ?? []).map((ev) => (
              <EventRow key={ev.id} ev={ev} />
            ))}
            {(events.data?.events ?? []).length === 0 && (
              <li className="px-3 py-6 text-center text-xs text-text-secondary">
                暂无事件
              </li>
            )}
          </ul>
        </div>
      </div>
    </div>
  );
}
