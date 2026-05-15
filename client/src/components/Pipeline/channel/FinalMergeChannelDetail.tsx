import React, { useEffect, useState } from 'react';
import { ChevronLeft, Download, GitMerge, RotateCcw } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useSetRecoilState } from 'recoil';
import {
  usePipelineFinalMergeInventory,
  usePipelineChannel,
  useTriggerChannelRun,
  type PipelineFinalMergeArtifact,
  type PipelineFinalMergeChannelRow,
  PipelineApi,
  PIPELINE_QUERY_KEYS,
} from '~/data-provider';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import { PIPELINE_FINAL_MERGE_CHANNEL_ID } from '~/components/Pipeline/overviewChannels';
import XlsxPreview from '~/components/Pipeline/preview/XlsxPreview';
import ChannelVerifyTab from '~/components/Pipeline/channel/ChannelVerifyTab';
import ChannelLogTab from '~/components/Pipeline/channel/ChannelLogTab';
import ChannelRunsTab from '~/components/Pipeline/channel/ChannelRunsTab';
import {
  pipelineArtifactDisplayTitle,
  pipelineArtifactRoleLabel,
  pipelineArtifactTechnicalName,
} from '~/components/Pipeline/preview/pipelineArtifactLabels';
import { cn } from '~/utils';

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function formatLocalTime(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  } catch {
    return iso;
  }
}

const SUB_TABS: { id: 'files' | 'runs' | 'verify' | 'logs'; label: string }[] = [
  { id: 'files', label: '文件' },
  { id: 'runs', label: '运行历史' },
  { id: 'verify', label: '校验报告' },
  { id: 'logs', label: '日志' },
];

function isFinalDeliverableRole(role: string | undefined): boolean {
  const raw = role?.trim();
  if (!raw) return true;
  return raw.toLowerCase() === 'output';
}

function partitionArtifacts(artifacts: PipelineFinalMergeArtifact[]): {
  final: PipelineFinalMergeArtifact[];
  intermediate: PipelineFinalMergeArtifact[];
} {
  const final: PipelineFinalMergeArtifact[] = [];
  const intermediate: PipelineFinalMergeArtifact[] = [];
  for (const a of artifacts) {
    if (isFinalDeliverableRole(a.role)) {
      final.push(a);
    } else {
      intermediate.push(a);
    }
  }
  return { final, intermediate };
}

function ArtifactDataTable({ artifacts }: { artifacts: PipelineFinalMergeArtifact[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-border-light">
      <table className="w-full min-w-[48rem] text-left text-xs">
        <thead className="bg-surface-secondary text-[11px] text-text-secondary">
          <tr>
            <th className="px-3 py-2 font-medium">文件名</th>
            <th className="px-3 py-2 font-medium">大小</th>
            <th className="w-24 px-3 py-2 font-medium">行数</th>
            <th className="px-3 py-2 font-medium">文件时间</th>
            <th className="px-3 py-2 font-medium">run id</th>
            <th className="px-3 py-2 font-medium">角色</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border-light bg-surface-primary">
          {artifacts.map((a, i) => (
            <tr key={`${a.run_id}-${a.name}-${i}`} className="text-text-primary">
              <td className="max-w-[min(28rem,50vw)] px-3 py-2 font-mono text-[11px]">
                {a.name}
              </td>
              <td className="whitespace-nowrap px-3 py-2 tabular-nums text-text-secondary">
                {formatBytes(a.size)}
              </td>
              <td className="px-3 py-2 tabular-nums text-text-secondary">
                {a.row_count != null ? String(a.row_count) : '—'}
              </td>
              <td className="whitespace-nowrap px-3 py-2 text-text-secondary">
                {formatLocalTime(a.created_at)}
              </td>
              <td className="px-3 py-2 font-mono text-[11px] text-text-secondary">{a.run_id}</td>
              <td className="px-3 py-2 text-text-tertiary">{a.role || 'output'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ArtifactsTable({
  channelId,
  displayName,
  row,
}: {
  channelId: string;
  displayName: string;
  row: PipelineFinalMergeChannelRow;
}) {
  const run = row.latest_run;
  const artifacts = row.artifacts;
  const { final: finalArts, intermediate: midArts } = partitionArtifacts(artifacts);

  return (
    <section className="mb-8 scroll-mt-4">
      <div className="mb-2 flex flex-wrap items-baseline justify-between gap-2 border-b border-border-light pb-2">
        <div>
          <h3 className="text-sm font-semibold text-text-primary">{displayName}</h3>
          <p className="mt-0.5 font-mono text-[11px] text-text-tertiary">{channelId}</p>
        </div>
        <div className="text-[11px] text-text-secondary">
          渠道状态 <span className="font-mono text-text-primary">{row.channel_status}</span>
          {run ? (
            <>
              {' '}· 最近 run <span className="font-mono text-text-primary">{run.run_id}</span>{' '}
              <span className="text-text-tertiary">({run.status})</span>
              {run.duration_seconds != null && <> · {run.duration_seconds}s</>}
            </>
          ) : (
            ' · 尚未产生运行记录'
          )}
        </div>
      </div>

      {!run || artifacts.length === 0 ? (
        <div className="rounded-lg border border-dashed border-border-medium px-4 py-6 text-center text-xs text-text-secondary">
          {run ? '该渠道最近一次运行未登记产出文件' : '该渠道尚未执行跑批'}
        </div>
      ) : (
        <div className="space-y-4">
          <div>
            <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-text-secondary">
              最终产物
              <span className="ml-2 font-mono text-[10px] font-normal text-text-tertiary">({finalArts.length})</span>
            </h4>
            {finalArts.length === 0 ? (
              <div className="rounded-lg border border-dashed border-border-medium px-3 py-4 text-center text-[11px] text-text-tertiary">
                无（均为中间产物或尚未标记）
              </div>
            ) : (
              <ArtifactDataTable artifacts={finalArts} />
            )}
          </div>

          {midArts.length > 0 ? (
            <details className="rounded-lg border border-border-light bg-surface-secondary/20">
              <summary className="cursor-pointer list-none px-3 py-2.5 text-xs font-medium text-text-secondary marker:content-none [&::-webkit-details-marker]:hidden">
                <span className="inline-flex items-center gap-2">
                  <span className="text-text-primary">中间产物</span>
                  <span className="font-mono text-[10px] text-text-tertiary">({midArts.length})</span>
                </span>
              </summary>
              <div className="border-t border-border-light px-0 pb-3 pt-2">
                <ArtifactDataTable artifacts={midArts} />
              </div>
            </details>
          ) : null}
        </div>
      )}
    </section>
  );
}

export default function FinalMergeChannelDetail({
  taskId,
  onBack,
}: {
  taskId: string;
  onBack: () => void;
}) {
  const inv = usePipelineFinalMergeInventory(taskId);
  // Fetch final_merge channel's own data for runs/verify/logs tabs
  const chData = usePipelineChannel(taskId, 'final_merge', { refetchInterval: 3000 });
  const qc = useQueryClient();
  const setSelectedTaskId = useSetRecoilState(pipelineSelectedTaskIdAtom);
  const setSelectedChannelId = useSetRecoilState(pipelineSelectedChannelIdAtom);
  const setSelectedRunId = useSetRecoilState(pipelineSelectedRunIdAtom);
  const [tab, setTab] = useState<'files' | 'runs' | 'verify' | 'logs'>('files');

  useEffect(() => {
    setSelectedTaskId(taskId);
    setSelectedChannelId(PIPELINE_FINAL_MERGE_CHANNEL_ID);
    setSelectedRunId(null);
  }, [taskId, setSelectedTaskId, setSelectedChannelId, setSelectedRunId]);

  const triggerMut = useTriggerChannelRun(taskId, 'final_merge');
  const fireRun = () => {
    setTab('logs');
    triggerMut.mutate({});
    setTimeout(() => {
      qc.invalidateQueries({ queryKey: ['pipeline', 'channel', taskId] });
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, 'final_merge'));
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, 'final_merge'));
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
    }, 2000);
  };

  const rows = inv.data?.channels ?? [];
  // Get the latest run of final_merge itself for verify tab
  const finalMergeRuns = chData.data?.runs ?? [];
  const latestFinalMergeRun = finalMergeRuns.length > 0 ? finalMergeRuns[finalMergeRuns.length - 1] : undefined;

  useEffect(() => {
    setSelectedRunId(latestFinalMergeRun?.run_id ?? null);
  }, [latestFinalMergeRun?.run_id, setSelectedRunId]);

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden">
      <div className="flex-none border-b border-border-light bg-surface-primary px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={onBack}
              className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
            >
              <ChevronLeft className="h-3.5 w-3.5" />
              返回总览
            </button>
            <div>
              <div className="flex items-center gap-2">
                <GitMerge className="h-5 w-5 text-green-500/90" aria-hidden />
                <h2 className="text-base font-semibold text-text-primary">最终合并</h2>
              </div>
              <p className="mt-0.5 text-xs text-text-secondary">
                汇总各渠道<strong className="text-text-primary/90">最近一次运行</strong>登记的产出文件。任务{' '}
                <span className="font-mono">{taskId.slice(0, 8)}</span>
              </p>
            </div>
          </div>
        </div>
        <div className="mt-3 flex items-center gap-2">
          <button
            type="button"
            disabled={triggerMut.isLoading}
            onClick={fireRun}
            className="inline-flex items-center gap-1.5 rounded-md bg-green-600 px-3.5 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-green-500 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <RotateCcw className="h-3.5 w-3.5" />
            {triggerMut.isLoading ? '提交中…' : '执行最终合并'}
          </button>
          {triggerMut.isError && (
            <span className="text-xs text-red-400">{(triggerMut.error as Error).message}</span>
          )}
          {SUB_TABS.map((t) => (
            <button
              key={t.id}
              type="button"
              onClick={() => setTab(t.id)}
              className={cn(
                'relative px-3 py-1.5 text-xs font-medium transition-colors',
                'border-b-2 border-transparent',
                tab === t.id
                  ? 'border-green-500 text-text-primary'
                  : 'text-text-secondary hover:border-border-medium hover:text-text-primary',
              )}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
          <div
            className={cn(
              'min-h-0 flex-1 px-4 py-4',
              tab === 'logs' ? 'flex flex-col overflow-hidden' : 'overflow-y-auto',
            )}
          >
            {/* 文件 tab: 展示上游各渠道产出汇总 */}
            {tab === 'files' && (
              <>
                {inv.isLoading && (
                  <div className="p-6 text-sm text-text-secondary">加载各渠道产出汇总…</div>
                )}
                {inv.error && (
                  <div className="p-6 text-sm text-text-primary">{(inv.error as Error).message}</div>
                )}
                {!inv.isLoading && !inv.error && rows.length === 0 && (
                  <div className="rounded-lg border border-dashed border-border-medium px-4 py-8 text-center text-sm text-text-secondary">
                    当前任务尚无渠道状态记录
                  </div>
                )}
                {!inv.isLoading &&
                  !inv.error &&
                  rows.map((r) => (
                    <ArtifactsTable
                      key={r.channel_id}
                      channelId={r.channel_id}
                      displayName={r.display_name}
                      row={r}
                    />
                  ))}
              </>
            )}

            {tab === 'runs' && (
              <ChannelRunsTab taskId={taskId} channelId="final_merge" runs={finalMergeRuns} />
            )}

            {tab === 'verify' && (
              <ChannelVerifyTab
                taskId={taskId}
                channelId="final_merge"
                run={latestFinalMergeRun}
              />
            )}

            {tab === 'logs' && (
              <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
                <ChannelLogTab taskId={taskId} channelId="final_merge" scope="channel" />
              </div>
            )}
          </div>
          {latestFinalMergeRun && latestFinalMergeRun.output_files.length > 0 && (
            <div className="flex max-h-[min(52vh,36rem)] shrink-0 flex-col overflow-hidden border-t border-border-light">
              <XlsxPreview
                taskId={taskId}
                channelId={PIPELINE_FINAL_MERGE_CHANNEL_ID}
                runId={latestFinalMergeRun.run_id}
                outputFiles={latestFinalMergeRun.output_files}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}