import React, { useEffect, useState } from 'react';
import { CheckCircle2, ChevronLeft, RotateCcw } from 'lucide-react';
import { useSetRecoilState } from 'recoil';
import {
  useCancelChannelRun,
  useConfirmChannel,
  usePipelineChannel,
  usePipelineClassification,
  useTriggerChannelRun,
  type PipelineChannelRunStatus,
} from '~/data-provider';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import { cn } from '~/utils';
import ChannelFilesTab from './ChannelFilesTab';
import ChannelRunsTab from './ChannelRunsTab';
import ChannelVerifyTab from './ChannelVerifyTab';
import ChannelLogTab from './ChannelLogTab';
import XlsxPreview from '../preview/XlsxPreview';

type SubTab = 'files' | 'runs' | 'verify' | 'logs';

const SUB_TABS: { id: SubTab; label: string }[] = [
  { id: 'files', label: '文件' },
  { id: 'runs', label: '运行历史' },
  { id: 'verify', label: '校验报告' },
  { id: 'logs', label: '日志' },
];

const STATUS_LABEL: Record<string, string> = {
  pending: '待开始',
  running: '执行中',
  preview_ready: '已预览',
  verified: '已校验',
  verified_with_warning: '校验有告警',
  edited: '已修改',
  replaced: '已替换',
  confirmed: '已签发',
  failed: '失败',
  skipped: '已跳过',
};

function StatusPill({ status }: { status: string }) {
  const palette: Partial<Record<PipelineChannelRunStatus, string>> = {
    running: 'bg-green-500/10 text-green-400 border-green-500/30',
    preview_ready: 'bg-green-500/10 text-green-400 border-green-500/30',
    verified: 'bg-green-500/10 text-green-400 border-green-500/30',
    verified_with_warning:
      'bg-green-500/10 text-green-400 border-green-500/30',
    edited: 'bg-amber-500/10 text-amber-400 border-amber-500/30',
    confirmed: 'bg-green-500/10 text-green-400 border-green-500/30',
    failed: 'bg-text-primary/10 text-text-primary border-border-medium',
  };
  const cls = palette[status as PipelineChannelRunStatus] ?? 'bg-surface-secondary text-text-secondary border-border-light';
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs font-medium',
        cls,
      )}
    >
      {STATUS_LABEL[status] ?? status}
    </span>
  );
}

export default function ChannelDetail({
  taskId,
  channelId,
  onBack,
}: {
  taskId: string;
  channelId: string;
  onBack: () => void;
}) {
  const [tab, setTab] = useState<SubTab>('files');
  const channelQuery = usePipelineChannel(taskId, channelId, {
    refetchInterval: 2000,
  });
  const cls = usePipelineClassification(taskId);
  const triggerMut = useTriggerChannelRun(taskId, channelId);
  const cancelMut = useCancelChannelRun(taskId, channelId);
  const confirmMut = useConfirmChannel(taskId, channelId);

  const setSelectedTaskId = useSetRecoilState(pipelineSelectedTaskIdAtom);
  const setSelectedChannelId = useSetRecoilState(pipelineSelectedChannelIdAtom);
  const setSelectedRunId = useSetRecoilState(pipelineSelectedRunIdAtom);

  const ch = channelQuery.data;
  const status = ch?.status ?? 'pending';
  const runs = ch?.runs ?? [];
  const lastRun = runs[runs.length - 1];
  const groupFiles = cls.data?.channels?.[channelId]?.files ?? [];

  const canAuditConfirm =
    runs.length > 0 &&
    status !== 'running' &&
    status !== 'confirmed' &&
    status !== 'failed';

  useEffect(() => {
    setSelectedTaskId(taskId);
    setSelectedChannelId(channelId);
  }, [taskId, channelId, setSelectedTaskId, setSelectedChannelId]);

  useEffect(() => {
    setSelectedRunId(lastRun?.run_id ?? null);
  }, [lastRun?.run_id, setSelectedRunId]);

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border-light bg-surface-primary px-4 py-3">
        <div className="flex items-center justify-between">
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
                <h2 className="text-base font-semibold text-text-primary">
                  {ch?.display_name ?? channelId}
                </h2>
                <StatusPill status={status} />
              </div>
              <div className="mt-0.5 text-xs text-text-secondary">
                <span className="font-mono">{channelId}</span> · 任务{' '}
                <span className="font-mono">{taskId.slice(0, 8)}</span>
                {ch?.entry_type && <> · 类型 {ch.entry_type}</>}
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center justify-end gap-2">
            <button
              type="button"
              disabled={!canAuditConfirm || confirmMut.isLoading}
              onClick={() => confirmMut.mutate()}
              className="inline-flex items-center gap-1 rounded-md border border-green-500/50 bg-green-500/10 px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-green-500/15 disabled:cursor-not-allowed disabled:opacity-45"
              title={
                status === 'confirmed'
                  ? '该渠道已签发确认'
                  : status === 'failed'
                    ? '运行失败，请先重新执行'
                    : runs.length === 0
                      ? '请先执行生成产物后再签发'
                      : '人工审计通过：标记为已签发（同步总览状态与绿点）'
              }
            >
              <CheckCircle2 className="h-3.5 w-3.5" />
              {confirmMut.isLoading ? '提交…' : status === 'confirmed' ? '已签发' : '审计签发'}
            </button>
            {status === 'running' ? (
              <button
                type="button"
                disabled={cancelMut.isLoading}
                onClick={() => cancelMut.mutate()}
                className="inline-flex items-center gap-1 rounded-md bg-red-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-red-500 disabled:opacity-50"
                title="强制中断当前执行中的 run"
              >
                <RotateCcw className="h-3.5 w-3.5" />
                {cancelMut.isLoading ? '中断中…' : '中断执行'}
              </button>
            ) : (
              <button
                type="button"
                disabled={
                  triggerMut.isLoading ||
                  groupFiles.length === 0
                }
                onClick={() => {
                  setTab('logs');
                  triggerMut.mutate(undefined);
                }}
                className="inline-flex items-center gap-1 rounded-md bg-green-500 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50"
                title={
                  groupFiles.length === 0 ? '暂无文件，请先上传' : '触发该渠道的一次新执行'
                }
              >
                <RotateCcw className="h-3.5 w-3.5" />
                {triggerMut.isLoading
                  ? '提交…'
                  : runs.length > 0
                    ? '重新执行'
                    : '执行'}
              </button>
            )}
          </div>
        </div>
        {(triggerMut.error || confirmMut.error || cancelMut.error) && (
          <div className="mt-2 text-xs text-red-400">
            {((triggerMut.error ?? confirmMut.error ?? cancelMut.error) as Error).message}
          </div>
        )}
        <div className="mt-3 flex gap-1">
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
                  : 'text-text-secondary hover:text-text-primary hover:border-border-medium',
              )}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="min-h-0 flex-1 overflow-auto">
        {tab === 'files' && (
          <ChannelFilesTab
            taskId={taskId}
            channelId={channelId}
            files={groupFiles}
          />
        )}
        {tab === 'runs' && (
          <ChannelRunsTab taskId={taskId} channelId={channelId} runs={runs} />
        )}
        {tab === 'verify' && (
          <ChannelVerifyTab
            taskId={taskId}
            channelId={channelId}
            run={lastRun}
          />
        )}
        {tab === 'logs' && (
          <ChannelLogTab taskId={taskId} channelId={channelId} />
        )}
        </div>

        {lastRun && lastRun.output_files.length > 0 && (
          <div className="flex max-h-[min(52vh,36rem)] shrink-0 flex-col overflow-hidden border-t border-border-light">
            <XlsxPreview
              taskId={taskId}
              channelId={channelId}
              runId={lastRun.run_id}
              outputFiles={lastRun.output_files}
            />
          </div>
        )}
      </div>
    </div>
  );
}