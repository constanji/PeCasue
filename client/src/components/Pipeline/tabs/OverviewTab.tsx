import React, { useEffect, useRef, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { useRecoilState } from 'recoil';
import {
  Archive,
  CheckCircle2,
  ChevronLeft,
  FolderUp,
  Trash2,
  PanelLeftOpen,
  Plus,
  Workflow,
} from 'lucide-react';
import {
  PIPELINE_QUERY_KEYS,
  PipelineApi,
  useCreatePipelineTask,
  usePipelineChannel,
  usePipelineClassification,
  usePipelineHealth,
  usePipelineTasks,
  useTriggerChannelRun,
  useUploadChannelZipReplace,
  useClearChannelExtracted,
  useUploadZipAuto,
  type PipelineChannelDef,
  type PipelineClassificationGroup,
  type PipelineTaskSummary,
} from '~/data-provider';
import { pipelineSelectedTaskIdAtom } from '~/store/pipeline';
import { cn } from '~/utils';
import EmptyTabPlaceholder from './EmptyTabPlaceholder';
import {
  PIPELINE_OVERVIEW_MATRIX_CHANNELS,
  PIPELINE_SPECIAL_BUNDLE_IDS,
  PIPELINE_SPECIAL_BUNDLE_LABELS,
  aggregateStatusForChannels,
  normalizePipelineToken,
  pickSpecialBundleDetailChannel,
} from '~/components/Pipeline/overviewChannels';

const TASK_STATUS_ZH: Record<string, string> = {
  pending: '待处理',
  running: '运行中',
  partial: '部分完成',
  completed: '已完成',
  failed: '失败',
  paused: '已暂停',
  terminated: '已终止',
};

const PIPELINE_STEP_ZH: Record<string, string> = {
  created: '已创建',
  uploading: '上传中',
  classifying: '归类中',
  running: '执行中',
  summary: '汇总',
  completed: '已完成',
  failed: '失败',
  intervention: '人工介入',
};

function formatTaskStatusLabel(raw: string): string {
  const key = normalizePipelineToken(raw);
  return TASK_STATUS_ZH[key] ?? raw.replace(/^TaskStatus\./i, '');
}

function formatPipelineStepLabel(raw: string): string {
  const key = normalizePipelineToken(raw);
  return PIPELINE_STEP_ZH[key] ?? raw.replace(/^PipelineStep\./i, '');
}

function StatusDot({ status }: { status: string }) {
  const key = normalizePipelineToken(status);
  const map: Record<string, string> = {
    completed: 'bg-green-500',
    partial: 'bg-green-500/70',
    pending: 'bg-border-medium',
    paused: 'bg-border-heavy',
    terminated: 'bg-border-heavy',
    confirmed: 'bg-green-500 shadow-[0_0_6px_rgba(74,222,128,0.55)]',
    verified: 'bg-amber-400',
    preview_ready: 'bg-amber-400',
    edited: 'bg-amber-400',
    replaced: 'bg-amber-400',
    verified_with_warning: 'bg-amber-400',
    skipped: 'bg-border-heavy',
    running: 'bg-green-400 shadow-[0_0_8px_rgba(74,222,128,0.45)]',
    failed: 'bg-text-secondary/70',
  };
  return (
    <span
      className={cn(
        'inline-block h-2 w-2 shrink-0 rounded-full',
        map[key] ?? 'bg-border-medium',
      )}
    />
  );
}

function OverviewSidecarBadge() {
  const { data, isLoading, error } = usePipelineHealth();
  if (isLoading) {
    return <span className="text-xs text-text-secondary">Sidecar …</span>;
  }
  if (error || !data?.ok) {
    return <span className="text-xs text-amber-400">未连接</span>;
  }
  return (
    <span className="inline-flex shrink-0 items-center gap-1.5 rounded-full border border-green-500/35 bg-green-500/10 px-3 py-1 text-xs font-medium text-green-500">
      <CheckCircle2 className="h-3.5 w-3.5 shrink-0" aria-hidden />
      已连接
    </span>
  );
}

function TaskRow({
  task,
  selected,
  onSelect,
}: {
  task: PipelineTaskSummary;
  selected: boolean;
  onSelect: () => void;
}) {
  const channelCount = Object.keys(task.channels).length;
  const statusLabel = formatTaskStatusLabel(String(task.status));
  const stepLabel = formatPipelineStepLabel(String(task.current_step));

  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'w-full rounded-lg border p-3 text-left transition-colors',
        selected
          ? 'border-green-500/60 bg-green-500/5'
          : 'border-border-light bg-surface-primary hover:border-green-500/40',
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1 text-sm font-medium text-text-primary">
          <StatusDot status={String(task.status)} />
          <span className="font-mono">{task.task_id.slice(0, 8)}</span>
          {task.period && (
            <span className="rounded border border-border-light px-1.5 py-0.5 text-xs font-normal text-text-secondary">
              {task.period}
            </span>
          )}
        </div>
        <span
          className="max-w-[5.5rem] shrink-0 truncate text-right text-xs text-text-secondary sm:max-w-none"
          title={statusLabel}
        >
          {statusLabel}
        </span>
      </div>
      <div className="mt-1.5 flex items-center justify-between gap-2 text-xs text-text-secondary">
        <span className="min-w-0 truncate">
          {channelCount} 渠道 · {stepLabel}
        </span>
        <span className="shrink-0">{new Date(task.created_at).toLocaleDateString()}</span>
      </div>
      {task.latest_log && (
        <div className="mt-2 line-clamp-1 text-xs text-text-secondary">{task.latest_log}</div>
      )}
    </button>
  );
}

function NewTaskInline({
  onCreated,
}: {
  onCreated: (taskId: string) => void;
}) {
  const [period, setPeriod] = useState<string>(
    () => new Date().toISOString().slice(0, 7).replace('-', ''),
  );
  const [open, setOpen] = useState(false);
  const createMut = useCreatePipelineTask();

  if (!open) {
    return (
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="flex w-full items-center gap-2 rounded-lg border border-dashed border-border-medium px-3 py-2 text-sm text-text-secondary hover:border-green-500/60 hover:text-text-primary"
      >
        <Plus className="h-4 w-4" />
        新建任务
      </button>
    );
  }

  return (
    <form
      className="space-y-2 rounded-lg border border-border-light bg-surface-primary p-3"
      onSubmit={async (e) => {
        e.preventDefault();
        try {
          const res = await createMut.mutateAsync({ period: period || null });
          onCreated(res.task_id);
          setOpen(false);
        } catch {
          /* error surfaced via createMut.error below */
        }
      }}
    >
      <label className="block text-xs font-medium text-text-secondary">期次</label>
      <input
        value={period}
        onChange={(e) => setPeriod(e.target.value)}
        placeholder="如 202602"
        className="w-full rounded-md border border-border-light bg-surface-secondary px-2 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
      />
      <div className="flex items-center gap-2 pt-1">
        <button
          type="submit"
          disabled={createMut.isLoading}
          className="rounded-md bg-green-500 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50"
        >
          {createMut.isLoading ? '创建中…' : '创建'}
        </button>
        <button
          type="button"
          onClick={() => setOpen(false)}
          className="rounded-md border border-border-light px-3 py-1.5 text-xs text-text-secondary hover:bg-surface-hover"
        >
          取消
        </button>
      </div>
      {createMut.error && (
        <div className="text-xs text-text-primary">{(createMut.error as Error).message}</div>
      )}
    </form>
  );
}

function CompactZipUpload({ taskId }: { taskId: string }) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const upload = useUploadZipAuto(taskId);
  return (
    <div className="flex max-w-full flex-col items-end gap-1 sm:max-w-md">
      <input
        ref={inputRef}
        type="file"
        accept=".zip,.rar,.7z"
        title="选择外层压缩包"
        aria-label="选择外层压缩包"
        className="hidden"
        onChange={async (e) => {
          const file = e.target.files?.[0];
          if (!file) return;
          try {
            await upload.mutateAsync(file);
          } finally {
            if (inputRef.current) inputRef.current.value = '';
          }
        }}
      />
      <button
        type="button"
        onClick={() => inputRef.current?.click()}
        disabled={upload.isLoading}
        className="inline-flex shrink-0 items-center gap-1.5 rounded-md bg-green-500 px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
      >
        <FolderUp className="h-4 w-4" />
        {upload.isLoading ? '上传中…' : '选择压缩包'}
      </button>
      {upload.data && (
        <span className="text-[11px] text-text-secondary">
          归类完成，移动 {upload.data.moved.length} 项
        </span>
      )}
      {upload.error && (
        <div className="w-full rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1.5 text-left text-[11px] leading-snug text-red-200">
          {(upload.error as Error).message}
        </div>
      )}
    </div>
  );
}

function ChannelCard({
  def,
  fileCount,
  status,
  taskId,
}: {
  def: PipelineChannelDef;
  fileCount: number;
  status: string;
  taskId: string;
}) {
  const zipInputRef = useRef<HTMLInputElement>(null);
  const [, setSearchParams] = useSearchParams();
  const isRunning = normalizePipelineToken(status) === 'running';
  const channelQuery = usePipelineChannel(taskId, def.channel_id, {
    refetchInterval: isRunning ? 2000 : false,
  });
  const triggerMut = useTriggerChannelRun(taskId, def.channel_id);
  const uploadZipReplace = useUploadChannelZipReplace(taskId);
  const clearChannelMut = useClearChannelExtracted(taskId);

  const empty = fileCount === 0;
  const liveStatus = channelQuery.data?.status ?? status;
  const lastRun = channelQuery.data?.runs?.[channelQuery.data.runs.length - 1];

  return (
    <div
      className={cn(
        'flex flex-col rounded-xl border bg-surface-primary p-4 transition-colors',
        empty
          ? 'border-dashed border-border-medium'
          : 'border-border-light hover:border-green-500/50',
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <StatusDot status={liveStatus} />
            <h4 className="truncate text-sm font-semibold text-text-primary">{def.display_name}</h4>
          </div>
          <p className="mt-0.5 line-clamp-2 text-xs text-text-secondary">{def.hint}</p>
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1">
          <div className="flex items-center gap-1">
            <span className="rounded border border-border-light px-1.5 py-0.5 font-mono text-[10px] text-text-secondary">
              {def.channel_id}
            </span>
            <input
              ref={zipInputRef}
              type="file"
              accept=".zip,.rar,.7z"
              className="hidden"
              aria-label="选择压缩包以替换该渠道目录"
              onChange={(e) => {
                const f = e.target.files?.[0];
                e.target.value = '';
                if (f) uploadZipReplace.mutate({ channelId: def.channel_id, file: f });
              }}
            />
            <button
              type="button"
              title="上传压缩包（zip / rar / 7z），解压后将整目录替换为该渠道来源"
              aria-label="上传压缩包替换该渠道"
              disabled={
                uploadZipReplace.isLoading || normalizePipelineToken(liveStatus) === 'running'
              }
              onClick={() => zipInputRef.current?.click()}
              className="inline-flex shrink-0 items-center justify-center rounded-md border border-border-light p-1 text-text-secondary hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Archive className="h-3.5 w-3.5" aria-hidden />
            </button>
          </div>
          <button
            type="button"
            title={`清空 extracted/${def.channel_id}/ 下已解压文件`}
            aria-label={`清空 ${def.display_name} 渠道解压目录`}
            disabled={
              clearChannelMut.isLoading || normalizePipelineToken(liveStatus) === 'running'
            }
            onClick={() => clearChannelMut.mutate(def.channel_id)}
            className="inline-flex shrink-0 items-center justify-center rounded-md border border-white/35 p-1 text-white hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <Trash2 className="h-3.5 w-3.5" aria-hidden />
          </button>
        </div>
      </div>
      <div className="mt-3 flex flex-1 flex-col space-y-1.5">
        {empty ? (
          <div className="rounded-md border border-dashed border-border-medium px-3 py-4 text-center text-xs text-text-secondary">
            暂无文件
          </div>
        ) : (
          <div className="text-xs text-text-secondary">
            <span className="font-medium text-text-primary">{fileCount}</span> 个文件已就绪 · 状态{' '}
            <span className="font-mono">{liveStatus}</span>
          </div>
        )}
        {lastRun && (
          <div className="text-xs text-text-secondary">
            上次：<span className="font-mono">{lastRun.run_id.slice(0, 8)}</span>
            {lastRun.duration_seconds != null && ` · ${lastRun.duration_seconds}s`}
            {lastRun.output_files.length > 0 && ` · ${lastRun.output_files.length} 文件`}
          </div>
        )}
        {uploadZipReplace.data && (
          <div className="text-[11px] text-green-500/90">
            已替换 · {uploadZipReplace.data.file_count} 个文件
          </div>
        )}
        {uploadZipReplace.error && (
          <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] leading-snug text-red-200">
            {(uploadZipReplace.error as Error).message}
          </div>
        )}
        {clearChannelMut.error && (
          <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] leading-snug text-red-200">
            {(clearChannelMut.error as Error).message}
          </div>
        )}
        {triggerMut.error && (
          <div className="text-xs text-text-primary">{(triggerMut.error as Error).message}</div>
        )}
      </div>
      <div className="mt-3 flex items-center justify-between gap-2">
        <button
          type="button"
          disabled={empty || triggerMut.isLoading || normalizePipelineToken(liveStatus) === 'running'}
          onClick={() => triggerMut.mutate(undefined)}
          className="flex-1 rounded-md border border-green-500/60 px-2 py-1 text-xs font-medium text-green-500 hover:bg-green-500/10 disabled:cursor-not-allowed disabled:border-border-light disabled:text-text-secondary"
          title={empty ? '上传文件后可执行' : '在后台执行该渠道一次'}
        >
          {normalizePipelineToken(liveStatus) === 'running'
            ? '执行中…'
            : triggerMut.isLoading
              ? '提交…'
              : '单独执行'}
        </button>
        <button
          type="button"
          onClick={() => {
            const next = new URLSearchParams();
            next.set('taskId', taskId);
            if (def.channel_id === 'allocation_base') {
              next.set('tab', 'final_allocation');
              next.set('alloc', 'merge');
            } else {
              next.set('tab', 'channels');
              next.set('channel', def.channel_id);
            }
            setSearchParams(next);
          }}
          className="flex-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
        >
          查看详情
        </button>
      </div>
    </div>
  );
}

const SPECIAL_ACH_ONLY_ID = 'special_ach_refund' as const;

function AchRefundSpecialBundleCard({
  taskId,
  classification,
}: {
  taskId: string;
  classification: Record<string, PipelineClassificationGroup | undefined>;
}) {
  const achZipInputRef = useRef<HTMLInputElement>(null);
  const [, setSearchParams] = useSearchParams();
  const qc = useQueryClient();
  const uploadAchZip = useUploadChannelZipReplace(taskId);
  const clearChannelMut = useClearChannelExtracted(taskId);
  const backendIds = [...PIPELINE_SPECIAL_BUNDLE_IDS];
  const totalFiles = backendIds.reduce((n, id) => n + (classification[id]?.files?.length ?? 0), 0);
  const aggStatus = aggregateStatusForChannels(backendIds, classification);
  const empty = totalFiles === 0;
  const liveAggRunning = normalizePipelineToken(aggStatus) === 'running';

  const activeSpecialIds = PIPELINE_SPECIAL_BUNDLE_IDS.filter(
    (cid) => (classification[cid]?.files?.length ?? 0) > 0,
  );
  const achOnlyMode =
    activeSpecialIds.length === 1 && activeSpecialIds[0] === SPECIAL_ACH_ONLY_ID;

  const bundleRunMut = useMutation({
    mutationFn: async () => {
      const targets = PIPELINE_SPECIAL_BUNDLE_IDS.filter(
        (cid) => (classification[cid]?.files?.length ?? 0) > 0,
      );
      if (targets.length === 0) throw new Error('暂无文件可执行');
      for (const cid of targets) {
        await PipelineApi.triggerChannelRun(taskId, cid);
      }
    },
    onSuccess: () => {
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
      PIPELINE_SPECIAL_BUNDLE_IDS.forEach((cid) => {
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, cid));
      });
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
    },
  });

  const detailChannel = pickSpecialBundleDetailChannel(classification);

  const subSummaryLine = PIPELINE_SPECIAL_BUNDLE_IDS.filter(
    (cid) => (classification[cid]?.files?.length ?? 0) > 0,
  )
    .map(
      (cid) =>
        `${PIPELINE_SPECIAL_BUNDLE_LABELS[cid]} · ${classification[cid]?.files?.length ?? 0} 文件`,
    )
    .join(' · ');

  return (
    <div
      className={cn(
        'flex flex-col rounded-xl border bg-surface-primary p-4 transition-colors',
        empty ? 'border-dashed border-border-medium' : 'border-border-light hover:border-green-500/50',
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <StatusDot status={aggStatus} />
            <h4 className="truncate text-sm font-semibold text-text-primary">
              Ach return · 退款 · 内转
            </h4>
          </div>
          <p className="mt-0.5 line-clamp-4 text-xs text-text-secondary">
            {achOnlyMode ? (
              <>
                当前仅为 <span className="font-medium text-text-primary/90">单渠道（ACH / return）</span>
                ，压缩包解压目录为{' '}
                <span className="font-mono text-[10px] text-text-tertiary">extracted/special_ach_refund/</span>
                。包内若有<strong className="text-text-primary/90">内转 / OP 入账 / OP 退票</strong>等表，且文件名符合解析规则，会在解压后<strong className="text-text-primary/90">自动复制</strong>到对应{' '}
                <span className="font-mono text-[10px]">special_*</span> 目录，各子渠道可分别执行；仍可用右侧归档按钮追加压缩包。
              </>
            ) : (
              <>「内转、ACH、OP 入账、OP 退票」</>
            )}
          </p>
        </div>
        <div className="flex shrink-0 flex-col items-end gap-1">
          <div className="flex items-center gap-1">
            <span className="rounded border border-border-light px-1.5 py-0.5 font-mono text-[10px] text-text-secondary">
              special_*
            </span>
            <input
              ref={achZipInputRef}
              type="file"
              accept=".zip,.rar,.7z"
              className="hidden"
              aria-label="选择压缩包以写入 special_ach_refund 渠道目录"
              onChange={(e) => {
                const f = e.target.files?.[0];
                e.target.value = '';
                if (f) uploadAchZip.mutate({ channelId: SPECIAL_ACH_ONLY_ID, file: f });
              }}
            />
            <button
              type="button"
              title="上传压缩包（zip / rar / 7z），解压后整目录写入 special_ach_refund"
              aria-label="上传压缩包至 ACH return 渠道"
              disabled={uploadAchZip.isLoading || liveAggRunning}
              onClick={() => achZipInputRef.current?.click()}
              className="inline-flex shrink-0 items-center justify-center rounded-md border border-border-light p-1 text-text-secondary hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Archive className="h-3.5 w-3.5" aria-hidden />
            </button>
          </div>
          <button
            type="button"
            title="清空各 special_* 子渠道 extracted 目录"
            aria-label="清空特殊来源 bundle 下全部解压目录"
            disabled={clearChannelMut.isLoading || liveAggRunning}
            onClick={() => {
              void (async () => {
                for (const cid of PIPELINE_SPECIAL_BUNDLE_IDS) {
                  await clearChannelMut.mutateAsync(cid);
                }
              })();
            }}
            className="inline-flex shrink-0 items-center justify-center rounded-md border border-white/35 p-1 text-white hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <Trash2 className="h-3.5 w-3.5" aria-hidden />
          </button>
        </div>
      </div>
      <div className="mt-3 flex flex-1 flex-col space-y-1.5">
        {empty ? (
          <div className="rounded-md border border-dashed border-border-medium px-3 py-4 text-center text-xs leading-relaxed text-text-secondary">
            暂无文件。可使用顶部「选择压缩包」整包归类，或使用本卡右侧与其他渠道一致的归档按钮，将压缩包解压到{' '}
            <span className="font-mono text-[10px]">special_ach_refund</span>。
          </div>
        ) : (
          <>
            <div className="text-xs text-text-secondary">
              <span className="font-medium text-text-primary">{totalFiles}</span> 个文件（
              <span className="text-text-tertiary">
                {achOnlyMode
                  ? '当前仅 ACH / return 子目录有文件'
                  : '内转 / ACH / OP 入账 / OP 退票'}
              </span>
              ）· 状态 <span className="font-mono">{aggStatus}</span>
            </div>
            {subSummaryLine ? (
              <p className="line-clamp-4 text-[11px] leading-snug text-text-secondary">
                <span className="font-medium text-text-primary/90">按子目录：</span>
                {subSummaryLine}
              </p>
            ) : null}
          </>
        )}
        {bundleRunMut.error != null ? (
          <div className="text-xs text-text-primary">{(bundleRunMut.error as Error).message}</div>
        ) : null}
        {uploadAchZip.data != null ? (
          <div className="text-[11px] text-green-500/90">
            已替换 · {uploadAchZip.data.file_count} 个文件
          </div>
        ) : null}
        {uploadAchZip.error != null ? (
          <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] leading-snug text-red-200">
            {(uploadAchZip.error as Error).message}
          </div>
        ) : null}
        {clearChannelMut.error != null ? (
          <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] leading-snug text-red-200">
            {(clearChannelMut.error as Error).message}
          </div>
        ) : null}
      </div>
      <div className="mt-3 flex items-center justify-between gap-2">
        <button
          type="button"
          disabled={empty || bundleRunMut.isLoading || liveAggRunning}
          onClick={() => bundleRunMut.mutate()}
          className="flex-1 rounded-md border border-green-500/60 px-2 py-1 text-xs font-medium text-green-500 hover:bg-green-500/10 disabled:cursor-not-allowed disabled:border-border-light disabled:text-text-secondary"
          title={
            empty
              ? '上传文件后可执行'
              : '只对「当前有归类文件」的子渠道逐个提交执行；无文件的子渠道自动跳过（不会发请求）'
          }
        >
          {liveAggRunning ? '执行中…' : bundleRunMut.isLoading ? '提交…' : '单独执行'}
        </button>
        <button
          type="button"
          onClick={() => {
            const next = new URLSearchParams();
            next.set('tab', 'channels');
            next.set('taskId', taskId);
            next.set('channel', detailChannel);
            setSearchParams(next);
          }}
          className="flex-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
        >
          查看详情
        </button>
      </div>
    </div>
  );
}

function ChannelMatrix({ taskId }: { taskId: string }) {
  const cls = usePipelineClassification(taskId);

  if (cls.isLoading) {
    return <div className="p-6 text-sm text-text-secondary">加载渠道矩阵…</div>;
  }
  if (cls.error) {
    return <div className="p-6 text-sm text-text-primary">{cls.error.message}</div>;
  }

  const classification = cls.data?.channels ?? {};
  const unknownCount = classification.unknown?.files?.length ?? 0;

  return (
    <div className="flex flex-col gap-4 pb-4">
      {unknownCount > 0 && (
        <div className="mx-4 rounded-lg border border-amber-500/35 bg-amber-500/10 px-3 py-2 text-xs text-text-primary">
          <span className="font-medium">未识别目录 </span>
          <span className="text-text-secondary">
            （extracted/unknown）内有 {unknownCount} 个文件 — 请确认解压后的文件夹命名是否含账单、自有、客资等关键词。
          </span>
        </div>
      )}
      <div className="grid grid-cols-1 gap-3 px-4 sm:grid-cols-2 lg:grid-cols-3">
        {PIPELINE_OVERVIEW_MATRIX_CHANNELS.slice(0, 3).map((row) => (
          <ChannelCard
            key={row.rowId}
            def={{
              channel_id: row.channel_id,
              display_name: row.display_name,
              entry_type: row.channel_id,
              hint: row.hint,
            }}
            fileCount={classification[row.channel_id]?.files?.length ?? 0}
            status={classification[row.channel_id]?.status ?? 'pending'}
            taskId={taskId}
          />
        ))}
        <AchRefundSpecialBundleCard taskId={taskId} classification={classification} />
        {PIPELINE_OVERVIEW_MATRIX_CHANNELS.slice(3).map((row) => (
          <ChannelCard
            key={row.rowId}
            def={{
              channel_id: row.channel_id,
              display_name: row.display_name,
              entry_type: row.channel_id,
              hint: row.hint,
            }}
            fileCount={classification[row.channel_id]?.files?.length ?? 0}
            status={classification[row.channel_id]?.status ?? 'pending'}
            taskId={taskId}
          />
        ))}
      </div>
      <p className="px-4 text-[11px] leading-relaxed text-text-secondary">
        「汇总」流程暂未在此矩阵展示；需要时在「渠道详情」中选择 summary。分摊基数仅通过「最终分摊」进入。
      </p>
    </div>
  );
}

function TaskListPanel({
  selectedTaskId,
  onSelect,
}: {
  selectedTaskId: string | null;
  onSelect: (taskId: string) => void;
}) {
  const { data, isLoading, error } = usePipelineTasks();
  if (isLoading) {
    return <div className="p-3 text-sm text-text-secondary">加载任务列表…</div>;
  }
  if (error) {
    return (
      <div className="p-3 text-sm text-text-secondary">无法加载任务列表：{error.message}</div>
    );
  }
  const tasks = data?.tasks ?? [];
  return (
    <div className="space-y-2 p-3">
      <NewTaskInline onCreated={onSelect} />
      {tasks.length === 0 ? (
        <div className="rounded-md border border-dashed border-border-medium p-3 text-center text-xs text-text-secondary">
          尚无任务
        </div>
      ) : (
        tasks.map((t) => (
          <TaskRow
            key={t.task_id}
            task={t}
            selected={t.task_id === selectedTaskId}
            onSelect={() => onSelect(t.task_id)}
          />
        ))
      )}
    </div>
  );
}

function TaskSidebarBriefRail({
  selectedTaskId,
  onSelect,
  onExpandToDetail,
}: {
  selectedTaskId: string | null;
  onSelect: (taskId: string) => void;
  onExpandToDetail: () => void;
}) {
  const { data, isLoading, error } = usePipelineTasks();

  const tasks = data?.tasks ?? [];

  return (
    <aside className="flex min-h-0 w-[4.75rem] shrink-0 flex-col overflow-hidden rounded-xl border border-border-light bg-gradient-to-b from-surface-secondary/35 to-surface-primary shadow-[inset_0_1px_0_0_rgba(255,255,255,0.04)]">
      <div className="flex flex-none flex-col items-center gap-1 border-b border-border-light px-1 py-2.5">
        <button
          type="button"
          onClick={onExpandToDetail}
          className="flex h-9 w-9 items-center justify-center rounded-lg border border-green-500/35 bg-green-500/10 text-green-500 transition-colors hover:bg-green-500/20 hover:text-green-400"
          title="展开详细列表"
          aria-label="展开详细列表"
        >
          <PanelLeftOpen className="h-4 w-4" aria-hidden />
        </button>
        <span className="px-0.5 text-center text-[9px] leading-[1.15] text-text-secondary">简略视图</span>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden px-1.5 py-2">
        {isLoading && (
          <div className="py-4 text-center text-[10px] text-text-secondary">加载…</div>
        )}
        {!isLoading && error && (
          <div className="rounded-md border border-red-500/30 bg-red-500/10 px-1 py-1.5 text-center text-[9px] leading-snug text-red-300">
            列表失败
          </div>
        )}
        {!isLoading && !error && tasks.length === 0 && (
          <p className="px-0.5 py-2 text-center text-[9px] leading-snug text-text-secondary">暂无任务</p>
        )}
        {!isLoading &&
          !error &&
          tasks.map((t) => {
            const selected = t.task_id === selectedTaskId;
            return (
              <button
                key={t.task_id}
                type="button"
                onClick={() => onSelect(t.task_id)}
                title={`${t.task_id.slice(0, 8)}… · ${formatTaskStatusLabel(String(t.status))}`}
                className={cn(
                  'mb-1.5 flex w-full flex-col items-center gap-1 rounded-lg px-1 py-2 transition-colors',
                  selected
                    ? 'bg-green-500/12 ring-1 ring-green-500/45'
                    : 'hover:bg-surface-hover/80',
                )}
              >
                <StatusDot status={String(t.status)} />
                <span className="font-mono text-[10px] leading-none tracking-tight text-text-primary">
                  {t.task_id.slice(0, 6)}
                </span>
                {t.period && (
                  <span className="max-w-full truncate text-[8px] leading-none text-text-secondary">
                    {t.period}
                  </span>
                )}
              </button>
            );
          })}
      </div>

      <div className="flex-none border-t border-border-light px-1 py-2">
        <button
          type="button"
          onClick={onExpandToDetail}
          className="flex h-9 w-full items-center justify-center rounded-lg border border-dashed border-border-medium text-text-secondary transition-colors hover:border-green-500/45 hover:bg-green-500/10 hover:text-green-500"
          title="新建任务（在详细列表中创建）"
          aria-label="新建任务"
        >
          <Plus className="h-4 w-4" aria-hidden />
        </button>
      </div>
    </aside>
  );
}

const TASK_SIDEBAR_STORAGE_KEY = 'pipeline_overview_task_sidebar_open';

export default function OverviewTab() {
  const [searchParams, setSearchParams] = useSearchParams();
  const urlTaskId = searchParams.get('taskId');
  const [selectedTaskId, setSelectedTaskId] = useRecoilState(pipelineSelectedTaskIdAtom);
  const [taskSidebarOpen, setTaskSidebarOpen] = useState(() => {
    if (typeof window === 'undefined') return true;
    return window.localStorage.getItem(TASK_SIDEBAR_STORAGE_KEY) !== '0';
  });

  useEffect(() => {
    try {
      window.localStorage.setItem(TASK_SIDEBAR_STORAGE_KEY, taskSidebarOpen ? '1' : '0');
    } catch {
      /* ignore */
    }
  }, [taskSidebarOpen]);

  useEffect(() => {
    if (urlTaskId && urlTaskId !== selectedTaskId) {
      setSelectedTaskId(urlTaskId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlTaskId]);

  const handleSelectTask = (tid: string) => {
    setSelectedTaskId(tid);
    const next = new URLSearchParams(searchParams);
    next.set('taskId', tid);
    setSearchParams(next, { replace: false });
  };

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden">
      <header className="flex-none border-b border-border-light bg-surface-secondary/50 px-4 py-2.5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="min-w-0 flex-1">
            <h2 className="text-base font-semibold tracking-tight text-text-primary">流水线</h2>
            <p className="mt-0.5 text-[11px] leading-snug text-text-secondary">
              企业账单与流水智能对账 · Human / Agent / Machine 协作
            </p>
          </div>
          <OverviewSidecarBadge />
        </div>
      </header>

      <div className="flex min-h-0 flex-1 gap-3 overflow-hidden p-4">
        {taskSidebarOpen ? (
          <aside className="flex min-h-0 w-full max-w-sm shrink-0 flex-col overflow-hidden rounded-xl border border-border-light bg-surface-primary lg:h-full lg:w-[min(20rem,34vw)] xl:w-72">
            <div className="flex-none border-b border-border-light px-3 py-3 sm:px-4">
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setTaskSidebarOpen(false)}
                  className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-border-light text-text-secondary hover:bg-surface-hover hover:text-text-primary"
                  title="切换到简略视图"
                  aria-expanded="true"
                  aria-controls="pipeline-task-sidebar"
                >
                  <ChevronLeft className="h-4 w-4" aria-hidden />
                </button>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center justify-between gap-2">
                    <h3 id="pipeline-task-sidebar-title" className="truncate text-sm font-semibold text-text-primary">
                      任务列表
                    </h3>
                    <span className="shrink-0 text-xs text-text-secondary">最近 · 详细</span>
                  </div>
                </div>
              </div>
            </div>
            <div id="pipeline-task-sidebar" className="min-h-0 flex-1 overflow-y-auto">
              <TaskListPanel selectedTaskId={selectedTaskId} onSelect={handleSelectTask} />
            </div>
          </aside>
        ) : (
          <TaskSidebarBriefRail
            selectedTaskId={selectedTaskId}
            onSelect={handleSelectTask}
            onExpandToDetail={() => setTaskSidebarOpen(true)}
          />
        )}

        <section className="flex min-h-0 min-w-0 flex-1 flex-col">
          {selectedTaskId ? (
            <div className="flex min-h-0 flex-1 flex-col rounded-xl border border-border-light bg-surface-primary">
              <div className="flex flex-none flex-wrap items-start justify-between gap-3 border-b border-border-light px-4 py-3">
                <div className="min-w-0">
                  <h3 className="text-sm font-semibold text-text-primary">渠道矩阵</h3>
                  <p className="mt-0.5 text-xs text-text-secondary">
                    当前任务 <span className="font-mono">{selectedTaskId.slice(0, 8)}</span>
                  </p>
                  <p className="mt-1 max-w-xl text-[11px] text-text-secondary">
                    上传单一外层压缩包（.zip / .7z / .rar），服务端解压后按目录关键词归类。特殊来源区卡片上的归档按钮亦可将整包写入{' '}
                    <span className="font-mono text-[10px]">special_ach_refund/</span>。
                  </p>
                </div>
                <div className="flex max-w-full flex-col items-end gap-2 sm:max-w-md">
                  <CompactZipUpload taskId={selectedTaskId} />
                </div>
              </div>
              <div className="min-h-0 flex-1 overflow-y-auto">
                <ChannelMatrix taskId={selectedTaskId} />
              </div>
            </div>
          ) : (
            <EmptyTabPlaceholder
              icon={<Workflow className="h-10 w-10" aria-hidden="true" />}
              title="渠道矩阵"
              description="左侧新建任务或选中一条历史任务后，右侧展示渠道矩阵与整包上传。"
              hints={[
                '矩阵顺序：账单 · 自有 · 客资 · Ach return/内转 · 境内/日本（分摊基数见「最终分摊」；汇总见渠道详情）',
                '解压失败若提示磁盘已满，请清理空间或调整 PIPELINE_DATA_DIR',
              ]}
            />
          )}
        </section>
      </div>
    </div>
  );
}
