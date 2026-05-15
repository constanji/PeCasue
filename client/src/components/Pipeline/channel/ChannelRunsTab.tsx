import React, { useEffect, useState } from 'react';
import { ChevronDown, ChevronRight, Download, Trash2 } from 'lucide-react';
import { PipelineApi, useDeleteChannelRun, type PipelineChannelRun } from '~/data-provider';
import { downloadPipelineArtifactUrl } from '~/lib/office/fetchPipelinePreviewBlob';
import {
  pipelineArtifactDisplayTitle,
  pipelineArtifactRoleLabel,
  pipelineArtifactTechnicalName,
} from '~/components/Pipeline/preview/pipelineArtifactLabels';
import { cn } from '~/utils';

const STATUS_BADGE: Record<string, string> = {
  running: 'bg-green-500/10 text-green-400 border-green-500/30',
  verified: 'bg-green-500/10 text-green-400 border-green-500/30 dark:text-green-400',
  verified_with_warning: 'bg-green-500/10 text-green-400 border-green-500/30',
  preview_ready: 'bg-green-500/10 text-green-400 border-green-500/30',
  edited: 'bg-amber-500/10 text-amber-400 border-amber-500/30',
  confirmed: 'bg-green-500/10 text-green-400 border-green-500/30',
  failed: 'bg-text-primary/10 text-text-primary',
};

export default function ChannelRunsTab({
  taskId,
  channelId,
  runs,
}: {
  taskId: string;
  channelId: string;
  runs: PipelineChannelRun[];
}) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const [pendingDelete, setPendingDelete] = useState<{ runId: string; shortId: string } | null>(
    null,
  );
  const deleteMut = useDeleteChannelRun(taskId, channelId);

  useEffect(() => {
    if (!pendingDelete) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !deleteMut.isLoading) setPendingDelete(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [pendingDelete, deleteMut.isLoading]);

  if (runs.length === 0) {
    return (
      <div className="p-6 text-sm text-text-secondary">
        尚无运行记录。点击右上角“执行”触发一次。
      </div>
    );
  }
  const ordered = [...runs].reverse();
  return (
    <div className="overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-surface-secondary text-xs text-text-secondary">
          <tr>
            <th className="px-3 py-2 text-left" />
            <th className="px-3 py-2 text-left">Run ID</th>
            <th className="px-3 py-2 text-left">开始</th>
            <th className="px-3 py-2 text-left">耗时</th>
            <th className="px-3 py-2 text-left">状态</th>
            <th className="px-3 py-2 text-left">产物</th>
            <th className="px-3 py-2 text-left">校验</th>
            <th className="px-3 py-2 text-left">备注</th>
            <th className="px-3 py-2 text-right">操作</th>
          </tr>
        </thead>
        <tbody>
          {ordered.map((r) => {
            const verifyRows = r.verify_summary?.rows ?? [];
            const warn = verifyRows.filter((row) => row.severity === 'warning').length;
            const pending = verifyRows.filter((row) => row.severity === 'pending').length;
            const pass = verifyRows.filter((row) => row.severity === 'pass').length;
            const expanded = !!open[r.run_id];
            const canDelete = r.status !== 'running';
            return (
              <React.Fragment key={r.run_id}>
                <tr className="border-t border-border-light">
                  <td className="w-8 px-3 py-2">
                    <button
                      type="button"
                      onClick={() =>
                        setOpen((prev) => ({ ...prev, [r.run_id]: !prev[r.run_id] }))
                      }
                      className="text-text-secondary hover:text-text-primary"
                      aria-label="展开产物"
                    >
                      {expanded ? (
                        <ChevronDown className="h-3.5 w-3.5" />
                      ) : (
                        <ChevronRight className="h-3.5 w-3.5" />
                      )}
                    </button>
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-text-primary">
                    {r.run_id.slice(0, 8)}
                    {r.is_dirty && (
                      <span className="ml-1 rounded-sm border border-green-500/40 px-1 text-[10px] text-green-400">
                        dirty
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-xs text-text-secondary">
                    {new Date(r.started_at).toLocaleString()}
                  </td>
                  <td className="px-3 py-2 text-xs text-text-secondary">
                    {r.duration_seconds != null ? `${r.duration_seconds}s` : '—'}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className={cn(
                        'rounded-full border px-2 py-0.5 text-xs font-medium',
                        STATUS_BADGE[r.status] ?? 'border-border-light bg-surface-secondary text-text-secondary',
                      )}
                    >
                      {r.status}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-xs text-text-secondary">
                    {r.output_files.length} 个
                  </td>
                  <td className="px-3 py-2 text-xs">
                    <span className="tabular-nums font-medium text-emerald-400">{pass}</span>
                    <span className="text-text-secondary/70"> / </span>
                    <span className="tabular-nums font-medium text-amber-400">{warn}</span>
                    <span className="text-text-secondary/70"> / </span>
                    <span className="tabular-nums font-medium text-sky-400">{pending}</span>
                    <span className="ml-1 text-text-secondary">(P/W/?)</span>
                  </td>
                  <td className="max-w-[220px] truncate px-3 py-2 text-xs text-text-secondary">
                    {r.error ? r.error.split('\n')[0] : r.note ?? '—'}
                  </td>
                  <td className="px-3 py-2 text-right">
                    <button
                      type="button"
                      disabled={!canDelete || deleteMut.isLoading}
                      title={
                        canDelete
                          ? '删除该次运行记录及磁盘产物（不可恢复）'
                          : '执行中的运行无法删除'
                      }
                      onClick={() =>
                        setPendingDelete({ runId: r.run_id, shortId: r.run_id.slice(0, 8) })
                      }
                      className="inline-flex items-center rounded-md border border-red-500/35 bg-red-500/10 p-1.5 text-red-400 hover:bg-red-500/20 disabled:cursor-not-allowed disabled:opacity-40"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  </td>
                </tr>
                {expanded && (
                  <tr className="border-t border-border-light bg-surface-secondary/40">
                    <td />
                    <td colSpan={8} className="px-3 py-2 text-xs">
                      {r.output_files.length === 0 ? (
                        <div className="text-text-secondary">无产物文件</div>
                      ) : (
                        <div className="space-y-1">
                          {r.output_files.map((f) => {
                            const downloadUrl = PipelineApi.runFileDownloadUrl(
                              taskId,
                              channelId,
                              r.run_id,
                              f.name,
                            );
                            return (
                              <div
                                key={f.file_id}
                                className="flex items-center justify-between gap-2 rounded-md bg-surface-primary px-2 py-1 text-text-primary"
                              >
                                <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                                  <span className="truncate text-xs font-medium text-text-primary">
                                    {pipelineArtifactDisplayTitle(f.name)}
                                  </span>
                                  <span className="truncate font-mono text-[10px] text-text-secondary">
                                    {pipelineArtifactTechnicalName(f.name)} ·{' '}
                                    {pipelineArtifactRoleLabel(f.role)} · {(f.size / 1024).toFixed(1)} KB
                                    {f.sha256 ? ` · sha ${f.sha256.slice(0, 8)}` : ''}
                                    {f.created_at
                                      ? ` · 生成于 ${new Date(f.created_at).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' })}`
                                      : ''}
                                  </span>
                                </div>
                                <button
                                  type="button"
                                  onClick={() => {
                                    void downloadPipelineArtifactUrl(downloadUrl, f.name).catch(
                                      (e) => window.alert((e as Error).message),
                                    );
                                  }}
                                  className="inline-flex items-center gap-1 rounded-md border border-border-light bg-surface-primary px-2 py-0.5 text-text-secondary hover:bg-surface-hover"
                                >
                                  <Download className="h-3 w-3" />
                                  下载
                                </button>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
      {pendingDelete && (
        <div
          className="fixed inset-0 z-[100] flex items-center justify-center bg-black/65 p-4 backdrop-blur-[2px]"
          role="presentation"
          onClick={() => !deleteMut.isLoading && setPendingDelete(null)}
        >
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-run-title"
            className="w-full max-w-sm rounded-xl border border-border-light bg-surface-primary p-4 shadow-2xl ring-1 ring-white/5"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 id="delete-run-title" className="text-sm font-semibold text-text-primary">
              删除运行记录
            </h3>
            <p className="mt-3 text-xs leading-relaxed text-text-secondary">
              将永久删除本次运行{' '}
              <span className="font-mono text-emerald-400">{pendingDelete.shortId}</span>{' '}
              的磁盘产物目录与摘要数据，操作{' '}
              <span className="font-medium text-amber-400">不可恢复</span>。
              <br />
              确定要继续吗？
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                disabled={deleteMut.isLoading}
                onClick={() => setPendingDelete(null)}
                className="rounded-lg border border-border-light bg-surface-secondary px-3 py-1.5 text-xs font-medium text-text-primary hover:bg-surface-hover disabled:opacity-50"
              >
                取消
              </button>
              <button
                type="button"
                disabled={deleteMut.isLoading}
                onClick={() => {
                  deleteMut.mutate(pendingDelete.runId, {
                    onSettled: () => setPendingDelete(null),
                  });
                }}
                className="rounded-lg bg-red-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-red-500 disabled:opacity-50"
              >
                {deleteMut.isLoading ? '删除中…' : '确认删除'}
              </button>
            </div>
          </div>
        </div>
      )}
      {deleteMut.error && (
        <div className="border-t border-border-light px-3 py-2 text-xs text-red-400">
          {(deleteMut.error as Error).message}
        </div>
      )}
    </div>
  );
}
