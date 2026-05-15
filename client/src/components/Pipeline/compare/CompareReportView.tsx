import React from 'react';
import { Download, FileSpreadsheet } from 'lucide-react';
import {
  PipelineApi,
  usePipelineCompareReport,
  type PipelineCompareMeta,
} from '~/data-provider';
import { downloadPipelineArtifactUrl } from '~/lib/office/fetchPipelinePreviewBlob';
import { cn } from '~/utils';

function StatCard({
  label,
  value,
  tone = 'default',
}: {
  label: string;
  value: number | string;
  tone?: 'default' | 'good' | 'warn';
}) {
  const toneCls =
    tone === 'good'
      ? 'border-green-500/30 bg-green-500/5'
      : tone === 'warn'
        ? 'border-green-500/30 bg-green-500/5'
        : 'border-border-light bg-surface-secondary';
  return (
    <div className={cn('rounded-lg border px-3 py-2', toneCls)}>
      <div className="text-xs text-text-secondary">{label}</div>
      <div className="mt-0.5 text-2xl font-semibold text-text-primary">{value}</div>
    </div>
  );
}

function CssBarChart({ data }: { data: { label: string; value: number }[] }) {
  if (data.length === 0) {
    return (
      <div className="rounded-md border border-dashed border-border-medium p-4 text-center text-xs text-text-secondary">
        无差异分布
      </div>
    );
  }
  const max = Math.max(...data.map((d) => d.value), 1);
  return (
    <div className="space-y-1.5">
      {data.map((d) => (
        <div key={d.label} className="flex items-center gap-2 text-xs">
          <div className="w-32 truncate text-right text-text-secondary" title={d.label}>
            {d.label}
          </div>
          <div className="relative h-3 flex-1 overflow-hidden rounded bg-surface-secondary">
            <div
              className="h-full rounded bg-green-500"
              style={{ width: `${(d.value / max) * 100}%` }}
              aria-label={`${d.label}: ${d.value}`}
            />
          </div>
          <div className="w-10 text-text-primary">{d.value}</div>
        </div>
      ))}
    </div>
  );
}

export default function CompareReportView({
  meta,
}: {
  meta: PipelineCompareMeta;
}) {
  const reportQ = usePipelineCompareReport(meta.task_id, meta.compare_id);
  const summary = meta.summary;
  const byCol = Object.entries(summary.by_column || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([label, value]) => ({ label, value: value as number }));

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <div className="text-sm font-semibold text-text-primary">
            对比 #{meta.compare_id}
          </div>
          <div className="mt-0.5 text-xs text-text-secondary">
            {new Date(meta.created_at).toLocaleString()} · {meta.duration_ms} ms
          </div>
        </div>
        <button
          type="button"
          onClick={() => {
            const u = PipelineApi.compareDownloadUrl(meta.task_id, meta.compare_id);
            void downloadPipelineArtifactUrl(
              u,
              `对比报告_${meta.compare_id}.xlsx`,
            ).catch((e) => window.alert((e as Error).message));
          }}
          className="inline-flex items-center gap-1 rounded-md bg-green-500 px-3 py-1.5 text-xs text-white hover:brightness-110"
        >
          <Download className="h-3.5 w-3.5" />
          下载 xlsx 报告
        </button>
      </div>

      <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
        <StatCard
          label="匹配行"
          value={summary.matched_rows}
          tone={summary.matched_rows > 0 ? 'good' : 'default'}
        />
        <StatCard
          label="差异单元格"
          value={summary.diff_cells}
          tone={summary.diff_cells > 0 ? 'warn' : 'good'}
        />
        <StatCard
          label="仅左有"
          value={summary.only_left_rows}
          tone={summary.only_left_rows > 0 ? 'warn' : 'good'}
        />
        <StatCard
          label="仅右有"
          value={summary.only_right_rows}
          tone={summary.only_right_rows > 0 ? 'warn' : 'good'}
        />
      </div>

      <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
        <div className="rounded-lg border border-border-light bg-surface-primary p-3">
          <div className="text-xs font-semibold text-text-primary">差异 Top 列</div>
          <div className="mt-2">
            <CssBarChart data={byCol} />
          </div>
        </div>
        <div className="rounded-lg border border-border-light bg-surface-primary p-3">
          <div className="text-xs font-semibold text-text-primary">列对齐</div>
          <div className="mt-2 space-y-2 text-xs">
            <div>
              <span className="text-text-secondary">公共列：</span>
              <span className="font-mono">
                {meta.alignment.common_columns.join(', ') || '—'}
              </span>
            </div>
            {meta.alignment.left_only.length > 0 && (
              <div>
                <span className="text-text-secondary">仅左：</span>
                <span className="font-mono text-green-500">
                  {meta.alignment.left_only.join(', ')}
                </span>
              </div>
            )}
            {meta.alignment.right_only.length > 0 && (
              <div>
                <span className="text-text-secondary">仅右：</span>
                <span className="font-mono text-green-500">
                  {meta.alignment.right_only.join(', ')}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="rounded-lg border border-border-light bg-surface-primary">
        <div className="flex items-center justify-between border-b border-border-light px-3 py-2 text-xs">
          <div className="flex items-center gap-1 font-semibold text-text-primary">
            <FileSpreadsheet className="h-3.5 w-3.5" />
            差异单元格 (前 500 行)
          </div>
          {reportQ.isLoading && (
            <span className="text-text-secondary">加载中…</span>
          )}
        </div>
        <div className="max-h-96 overflow-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-surface-secondary text-text-secondary">
              <tr>
                <th className="px-2 py-1 text-left">L#</th>
                <th className="px-2 py-1 text-left">R#</th>
                <th className="px-2 py-1 text-left">列</th>
                <th className="px-2 py-1 text-left">左值</th>
                <th className="px-2 py-1 text-left">右值</th>
              </tr>
            </thead>
            <tbody>
              {(reportQ.data?.diff_rows ?? []).map((r, i) => (
                <tr key={i} className="border-t border-border-light">
                  <td className="px-2 py-1 font-mono text-text-secondary">
                    {r.left_index}
                  </td>
                  <td className="px-2 py-1 font-mono text-text-secondary">
                    {r.right_index}
                  </td>
                  <td className="px-2 py-1 font-mono">{r.column}</td>
                  <td className="px-2 py-1 font-mono text-text-primary">
                    {String(r.left_value ?? '')}
                  </td>
                  <td className="px-2 py-1 font-mono text-green-500">
                    {String(r.right_value ?? '')}
                  </td>
                </tr>
              ))}
              {!reportQ.isLoading && (reportQ.data?.diff_rows?.length ?? 0) === 0 && (
                <tr>
                  <td colSpan={5} className="px-2 py-3 text-center text-text-secondary">
                    无差异
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
