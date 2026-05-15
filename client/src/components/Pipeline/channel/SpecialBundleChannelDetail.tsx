import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Archive, CheckCircle2, ChevronLeft, RotateCcw, Trash2 } from 'lucide-react';
import { useSetRecoilState } from 'recoil';
import {
  useClearChannelExtracted,
  useConfirmChannel,
  usePipelineChannel,
  usePipelineClassification,
  useTriggerChannelRun,
  useUploadChannelZipReplace,
  type PipelineChannelRunStatus,
} from '~/data-provider';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import {
  PIPELINE_SPECIAL_BUNDLE_SIDEBAR,
  pickSpecialBundleDetailChannel,
  PIPELINE_SPECIAL_DETAIL_ROUTE_IDS,
  normalizePipelineToken,
} from '~/components/Pipeline/overviewChannels';
import { cn } from '~/utils';
import type { PipelineClassificationFile } from '~/data-provider';
import ChannelFilesTab from './ChannelFilesTab';
import ChannelRunsTab from './ChannelRunsTab';
import ChannelVerifyTab from './ChannelVerifyTab';
import ChannelLogTab from './ChannelLogTab';
import XlsxPreview from '../preview/XlsxPreview';

type SubTab = 'files' | 'runs' | 'verify' | 'logs';
type TopSection = 'transfer_ach' | 'op_incoming' | 'merge';

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
  const cls =
    palette[status as PipelineChannelRunStatus] ??
    'bg-surface-secondary text-text-secondary border-border-light';
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

function topFromChannelId(channelId: string): TopSection {
  if (channelId === 'special_merge') return 'merge';
  if (channelId === 'special_op_incoming') return 'op_incoming';
  return 'transfer_ach';
}

function channelReadyForMerge(ch: { runs?: { status?: string; output_files?: { length: number } }[] } | undefined): boolean {
  const runs = ch?.runs ?? [];
  if (!runs.length) return false;
  const last = runs[runs.length - 1];
  const st = normalizePipelineToken(String(last.status ?? ''));
  if (!['verified', 'verified_with_warning', 'confirmed'].includes(st)) return false;
  return (last.output_files?.length ?? 0) > 0;
}

/**
 * 两处理分区（内转+ACH 联跑、OP入账）+ 合并；内转分区一次执行扫描两目录并按文件名路由。
 */
export default function SpecialBundleChannelDetail({
  taskId,
  channelId,
  onBack,
}: {
  taskId: string;
  channelId: string;
  onBack: () => void;
}) {
  const [searchParams, setSearchParams] = useSearchParams();
  const [tab, setTab] = useState<SubTab>('files');

  const setSelectedTaskId = useSetRecoilState(pipelineSelectedTaskIdAtom);
  const setSelectedChannelId = useSetRecoilState(pipelineSelectedChannelIdAtom);
  const setSelectedRunId = useSetRecoilState(pipelineSelectedRunIdAtom);

  useEffect(() => {
    if (channelId !== 'special_ach_refund') return;
    const next = new URLSearchParams(window.location.search);
    next.set('tab', 'channels');
    next.set('taskId', taskId);
    next.set('channel', 'special_transfer');
    setSearchParams(next, { replace: true });
  }, [channelId, taskId, setSearchParams]);

  const cls = usePipelineClassification(taskId);
  const classification = cls.data?.channels ?? {};

  const chTransferUpstream = usePipelineChannel(taskId, 'special_transfer', { refetchInterval: 3000 });
  const chOpUpstream = usePipelineChannel(taskId, 'special_op_incoming', { refetchInterval: 3000 });
  const chAchUpstream = usePipelineChannel(taskId, 'special_ach_refund', { refetchInterval: 3000 });
  const chMerge = usePipelineChannel(taskId, 'special_merge', { refetchInterval: 3000 });
  const triggerAchMut = useTriggerChannelRun(taskId, 'special_ach_refund');
  const triggerOpRefundMut = useTriggerChannelRun(taskId, 'special_op_refund');

  const coercedChannelId = useMemo(() => {
    if ((PIPELINE_SPECIAL_DETAIL_ROUTE_IDS as readonly string[]).includes(channelId)) {
      return channelId;
    }
    return pickSpecialBundleDetailChannel(classification);
  }, [channelId, classification]);

  const topSection = topFromChannelId(coercedChannelId);

  const effectiveBackendId = useMemo(() => {
    if (topSection === 'merge') return 'special_merge';
    if (topSection === 'op_incoming') return 'special_op_incoming';
    return 'special_transfer';
  }, [topSection]);

  const nTransfer = classification.special_transfer?.files?.length ?? 0;
  const nAch = classification.special_ach_refund?.files?.length ?? 0;
  const nOp = classification.special_op_incoming?.files?.length ?? 0;
  const nOpRefund = classification.special_op_refund?.files?.length ?? 0;
  const nTransferAch = nTransfer + nAch;
  const nOpTotal = nOp + nOpRefund;

  const transferAchDisplayFiles: PipelineClassificationFile[] = useMemo(() => {
    const a = classification.special_transfer?.files ?? [];
    const b = classification.special_ach_refund?.files ?? [];
    return [
      ...a.map((f) => ({ ...f, source_channel_id: 'special_transfer' })),
      ...b.map((f) => ({ ...f, source_channel_id: 'special_ach_refund' })),
    ];
  }, [classification.special_transfer?.files, classification.special_ach_refund?.files]);

  const opDisplayFiles: PipelineClassificationFile[] = useMemo(() => {
    const a = classification.special_op_incoming?.files ?? [];
    const b = classification.special_op_refund?.files ?? [];
    return [
      ...a.map((f) => ({ ...f, source_channel_id: 'special_op_incoming' })),
      ...b.map((f) => ({ ...f, source_channel_id: 'special_op_refund' })),
    ];
  }, [classification.special_op_incoming?.files, classification.special_op_refund?.files]);

  const channelQuery = usePipelineChannel(taskId, effectiveBackendId, {
    refetchInterval: 2000,
  });
  const triggerMut = useTriggerChannelRun(taskId, effectiveBackendId);
  const confirmMut = useConfirmChannel(taskId, effectiveBackendId);
  const uploadZipReplace = useUploadChannelZipReplace(taskId);
  const clearExtractedMut = useClearChannelExtracted(taskId);
  const uploadInputRef = useRef<HTMLInputElement>(null);

  const ch = channelQuery.data;
  const status = ch?.status ?? 'pending';
  const runs = ch?.runs ?? [];
  const lastRun = runs[runs.length - 1];

  // 内转/ACH 分区：合并两个子渠道的最新产出 xlsx（不含 csv 侧车）用于预览
  const achRuns = chAchUpstream.data?.runs ?? [];
  const lastAchRun = achRuns[achRuns.length - 1];
  const transferAchOutputFiles = useMemo(() => {
    const xlsxOnly = (files: typeof lastRun.output_files) =>
      (files ?? []).filter((f) => /\.xlsx$/i.test(f.name));
    return [...xlsxOnly(lastRun?.output_files), ...xlsxOnly(lastAchRun?.output_files)];
  }, [lastRun?.output_files, lastAchRun?.output_files]);
  const groupFiles =
    effectiveBackendId === 'special_merge'
      ? []
      : topSection === 'transfer_ach'
        ? transferAchDisplayFiles
        : topSection === 'op_incoming'
          ? opDisplayFiles
          : (classification[effectiveBackendId]?.files ?? []);

  const mergeIssued =
    normalizePipelineToken(chMerge.data?.status ?? '') === 'confirmed';
  /** 内转+ACH、OP 入账的签发须在「合并」分区产物上先完成审计签发 */
  const auditBlockedUntilMerge =
    effectiveBackendId === 'special_transfer' ||
    effectiveBackendId === 'special_op_incoming';

  const canAuditConfirm =
    runs.length > 0 &&
    status !== 'running' &&
    status !== 'confirmed' &&
    status !== 'failed' &&
    (!auditBlockedUntilMerge || mergeIssued);

  const mergeCanRun = topSection === 'merge';
  const canUploadInSection = !mergeCanRun;
  const uploadTargetChannelId = topSection === 'op_incoming' ? 'special_op_incoming' : 'special_ach_refund';

  const taReady = channelReadyForMerge(chTransferUpstream.data);
  const opReady = channelReadyForMerge(chOpUpstream.data);

  useEffect(() => {
    setSelectedTaskId(taskId);
    setSelectedChannelId(effectiveBackendId);
  }, [taskId, effectiveBackendId, setSelectedTaskId, setSelectedChannelId]);

  useEffect(() => {
    setSelectedRunId(lastRun?.run_id ?? null);
  }, [lastRun?.run_id, setSelectedRunId]);

  const pushChannel = (nextId: string) => {
    setSelectedChannelId(nextId);
    const next = new URLSearchParams(searchParams);
    next.set('tab', 'channels');
    next.set('taskId', taskId);
    next.set('channel', nextId);
    setSearchParams(next, { replace: true });
  };

  const selectTop = (top: TopSection) => {
    setTab('files');
    if (top === 'merge') {
      pushChannel('special_merge');
      return;
    }
    if (top === 'op_incoming') {
      pushChannel('special_op_incoming');
      return;
    }
    pushChannel('special_transfer');
  };

  const fireSectionRun = () => {
    setTab('logs');
    if (mergeCanRun) {
      triggerMut.mutate({ allocation_phase: 'merge' });
    } else if (topSection === 'transfer_ach') {
      // 内转与 ACH return 两个子渠道同时触发
      triggerMut.mutate({});
      triggerAchMut.mutate({});
    } else if (topSection === 'op_incoming') {
      // OP 入账与 OP 退票两个子渠道同时触发
      triggerMut.mutate({});
      triggerOpRefundMut.mutate({});
    } else {
      triggerMut.mutate({});
    }
  };

  const mergeOnlyExecDisabled = triggerMut.isLoading || status === 'running';
  const transferOpExecDisabled =
    triggerMut.isLoading ||
    status === 'running' ||
    (topSection === 'transfer_ach' && nTransferAch === 0) ||
    (topSection === 'op_incoming' && nOpTotal === 0);
  const primaryExecDisabled = mergeCanRun ? mergeOnlyExecDisabled : transferOpExecDisabled;

  const clearSectionExtracted = () => {
    void (async () => {
      if (topSection === 'transfer_ach') {
        await clearExtractedMut.mutateAsync('special_transfer');
        await clearExtractedMut.mutateAsync('special_ach_refund');
      } else if (topSection === 'op_incoming') {
        await clearExtractedMut.mutateAsync('special_op_incoming');
        await clearExtractedMut.mutateAsync('special_op_refund');
      }
    })();
  };

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border-light bg-surface-primary px-4 py-3">
        <div className="flex items-center justify-between gap-2">
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
                  {PIPELINE_SPECIAL_BUNDLE_SIDEBAR.label}
                </h2>
                <StatusPill status={status} />
              </div>
              <div className="mt-0.5 text-xs text-text-secondary">
                <span className="text-text-tertiary">{PIPELINE_SPECIAL_BUNDLE_SIDEBAR.subtitle}</span>
                {' · '}
                渠道 <span className="font-mono">{effectiveBackendId}</span> · 任务{' '}
                <span className="font-mono">{taskId.slice(0, 8)}</span>
                {ch?.entry_type && <> · 类型 {ch.entry_type}</>}
              </div>
            </div>
          </div>
          <div className="flex flex-col items-end gap-1">
            <button
              type="button"
              disabled={!canAuditConfirm || confirmMut.isLoading}
              onClick={() => confirmMut.mutate()}
              className="inline-flex items-center gap-1 rounded-md border border-green-500/50 bg-green-500/10 px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-green-500/15 disabled:cursor-not-allowed disabled:opacity-45"
              title={
                auditBlockedUntilMerge && !mergeIssued
                  ? '请先在上方「合并」分区对合并工作簿完成审计签发后，再回到本分区签发'
                  : '针对当前分区对应的后台渠道签发'
              }
            >
              <CheckCircle2 className="h-3.5 w-3.5" />
              {confirmMut.isLoading ? '提交…' : status === 'confirmed' ? '已签发' : '审计签发'}
            </button>
            {canUploadInSection && (
              <div className="flex items-center gap-1">
                <input
                  ref={uploadInputRef}
                  id="special-section-zip-upload"
                  type="file"
                  accept=".zip,.rar,.7z"
                  className="hidden"
                  aria-label="上传压缩包替换当前特殊分区目录"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    e.target.value = '';
                    if (!f) return;
                    uploadZipReplace.mutate({ channelId: uploadTargetChannelId, file: f });
                  }}
                />
                <button
                  type="button"
                  disabled={uploadZipReplace.isLoading || status === 'running'}
                  onClick={() => uploadInputRef.current?.click()}
                  className="inline-flex items-center justify-center gap-1 rounded-md border border-border-light px-3 py-1.5 text-xs font-medium text-text-secondary hover:bg-surface-hover disabled:opacity-45"
                  title={
                    topSection === 'op_incoming'
                      ? '上传 OP 压缩包（入账/退票命名兼容），按规则写入 OP 子渠道目录'
                      : '上传 Ach return / 内转混合压缩包，按规则写入 special 子渠道目录'
                  }
                >
                  <Archive className="h-3.5 w-3.5" />
                  上传压缩包
                </button>
                <button
                  type="button"
                  disabled={clearExtractedMut.isLoading || status === 'running'}
                  onClick={clearSectionExtracted}
                  className="inline-flex items-center justify-center rounded-md border border-white/35 p-1 text-white hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-45"
                  title={
                    topSection === 'op_incoming'
                      ? '清空 extracted/special_op_incoming/ 与 extracted/special_op_refund/'
                      : '清空 extracted/special_transfer/ 与 extracted/special_ach_refund/'
                  }
                  aria-label={
                    topSection === 'op_incoming'
                      ? '清空 OP 子渠道解压目录'
                      : '清空内转与 ACH 子渠道解压目录'
                  }
                >
                  <Trash2 className="h-3.5 w-3.5" aria-hidden />
                </button>
              </div>
            )}
          </div>
        </div>

        <nav className="mt-3 flex flex-wrap gap-1" aria-label="特殊来源分区">
          <button
            type="button"
            onClick={() => selectTop('transfer_ach')}
            className={cn(
              'rounded-md px-3 py-1.5 text-xs font-medium transition-colors',
              topSection === 'transfer_ach'
                ? 'bg-green-500/15 text-text-primary ring-1 ring-green-500/35'
                : 'text-text-secondary hover:bg-surface-hover hover:text-text-primary',
            )}
          >
            内转 / Ach return
            <span className="ml-1 font-mono text-[10px] text-text-tertiary">({nTransferAch})</span>
          </button>
          <button
            type="button"
            onClick={() => selectTop('op_incoming')}
            className={cn(
              'rounded-md px-3 py-1.5 text-xs font-medium transition-colors',
              topSection === 'op_incoming'
                ? 'bg-green-500/15 text-text-primary ring-1 ring-green-500/35'
                : 'text-text-secondary hover:bg-surface-hover hover:text-text-primary',
            )}
          >
            OP 入账
            <span className="ml-1 font-mono text-[10px] text-text-tertiary">({nOpTotal})</span>
          </button>
          <button
            type="button"
            onClick={() => selectTop('merge')}
            className={cn(
              'rounded-md px-3 py-1.5 text-xs font-medium transition-colors',
              topSection === 'merge'
                ? 'bg-green-500/15 text-text-primary ring-1 ring-green-500/35'
                : 'text-text-secondary hover:bg-surface-hover hover:text-text-primary',
            )}
          >
            合并
          </button>
        </nav>

        {mergeCanRun && (
          <div className="mt-2 rounded-md border border-border-light bg-surface-secondary/30 px-3 py-2 text-[11px] text-text-secondary">
            合并读取各分区最近已校验/已签发产出。
            {(!taReady || !opReady) && (
              <span className="ml-1 text-amber-400">
                内转+ACH 就绪: {taReady ? '是' : '否'} · OP入账 就绪: {opReady ? '是' : '否'}
              </span>
            )}
          </div>
        )}
        {!mergeCanRun && auditBlockedUntilMerge && !mergeIssued && (
          <p className="mt-2 text-[11px] text-text-tertiary">
            在「合并」分区完成审计签发后，本分区状态将自动变为「已确认」。
          </p>
        )}

        <div className="mt-3 flex flex-wrap items-center gap-2">
          <button
            type="button"
            disabled={primaryExecDisabled}
            onClick={fireSectionRun}
            className={cn(
              'inline-flex items-center gap-1 rounded-md px-3 py-1.5 text-xs font-medium text-white disabled:opacity-50',
              'bg-green-600 hover:bg-green-500/95',
            )}
            title={
              mergeCanRun ? '写入多 sheet 合并结果' : '仅执行当前分区对应的后台渠道'
            }
          >
            <RotateCcw className="h-3.5 w-3.5" />
            {mergeCanRun ? '单独执行 · 合并' : topSection === 'transfer_ach' ? '单独执行 · 内转+ACH' : '单独执行 · OP入账'}
          </button>
          {mergeCanRun ? (
            <span className="text-[10px] text-text-tertiary">合并不校验本页目录下是否有上传文件。</span>
          ) : null}
        </div>

        {triggerMut.error != null || confirmMut.error != null ? (
          <div className="mt-2 text-xs text-red-400">
            {((triggerMut.error ?? confirmMut.error) as Error).message}
          </div>
        ) : null}
        {uploadZipReplace.error != null ? (
          <div className="mt-2 text-xs text-red-400">
            {(uploadZipReplace.error as Error).message}
          </div>
        ) : null}
        {clearExtractedMut.error != null ? (
          <div className="mt-2 text-xs text-red-400">
            {(clearExtractedMut.error as Error).message}
          </div>
        ) : null}
        {uploadZipReplace.data != null && canUploadInSection ? (
          <div className="mt-2 text-xs text-green-500/90">
            已替换 {uploadTargetChannelId} · {uploadZipReplace.data.file_count} 个文件
          </div>
        ) : null}

        <div className="mt-3 flex gap-1 border-t border-border-light pt-2">
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
        <div className="min-h-0 flex-1 overflow-auto">
          {tab === 'files' && mergeCanRun ? (
            <div className="p-6 text-sm text-text-secondary">
              <p className="text-text-primary">本分区没有独立上传目录。</p>
              <p className="mt-2">
                请先在「内转 / Ach return」「OP 入账」完成单独执行并得到已校验产出，再点此分区「单独执行 ·
                合并」。
              </p>
              <p className="mt-2 font-mono text-[11px] text-text-tertiary">
                内转+ACH 就绪: {taReady ? '是' : '否'} · OP入账 就绪: {opReady ? '是' : '否'}
              </p>
            </div>
          ) : null}
          {tab === 'files' && !mergeCanRun && topSection === 'transfer_ach' && (
            <ChannelFilesTab
              taskId={taskId}
              channelId="special_transfer"
              files={groupFiles}
              directoryCaption={` data/tasks/${taskId.slice(0, 8)}…/extracted/special_transfer/ 与 …/extracted/special_ach_refund/（一次执行会合并扫描并按文件名路由）`}
            />
          )}
          {tab === 'files' && !mergeCanRun && topSection === 'op_incoming' && (
            <ChannelFilesTab
              taskId={taskId}
              channelId={effectiveBackendId}
              files={groupFiles}
              directoryCaption={` data/tasks/${taskId.slice(0, 8)}…/extracted/special_op_incoming/（含子文件夹；单次执行递归扫描目录下全部 Excel 源文件，与「内转 / Ach return」分区一致）`}
            />
          )}
          {tab === 'runs' && (
            <ChannelRunsTab taskId={taskId} channelId={effectiveBackendId} runs={runs} />
          )}
          {tab === 'verify' && (
            <ChannelVerifyTab taskId={taskId} channelId={effectiveBackendId} run={lastRun} />
          )}
          {tab === 'logs' && <ChannelLogTab taskId={taskId} channelId={effectiveBackendId} />}
        </div>

        {topSection === 'transfer_ach' && transferAchOutputFiles.length > 0 && (
          <div className="flex max-h-[min(52vh,36rem)] shrink-0 flex-col overflow-hidden border-t border-border-light">
            <XlsxPreview
              taskId={taskId}
              channelId={effectiveBackendId}
              runId={lastRun?.run_id ?? ''}
              outputFiles={transferAchOutputFiles}
              primarySectionTitle="内转 · ACH return 产物"
            />
          </div>
        )}
        {topSection !== 'transfer_ach' && lastRun && lastRun.output_files.length > 0 && (
          <div className="flex max-h-[min(52vh,36rem)] shrink-0 flex-col overflow-hidden border-t border-border-light">
            <XlsxPreview
              taskId={taskId}
              channelId={effectiveBackendId}
              runId={lastRun.run_id}
              outputFiles={lastRun.output_files}
            />
          </div>
        )}
      </div>
    </div>
  );
}
