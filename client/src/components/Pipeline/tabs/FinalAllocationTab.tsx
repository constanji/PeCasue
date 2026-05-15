import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { useRecoilState, useSetRecoilState } from 'recoil';
import { OGDialog, OGDialogContent } from '@because/client';
import {
  ChevronRight,
  CircleHelp,
  Download,
  ExternalLink,
  FolderUp,
  Layers,
  PieChart,
  RotateCcw,
  Upload,
} from 'lucide-react';
import {
  PIPELINE_QUERY_KEYS,
  PipelineApi,
  usePipelineChannel,
  usePipelineClassification,
  usePipelineTask,
  useTriggerChannelRun,
  useUploadCostSummary,
  useUploadAllocationMergeBase,
  type PipelineChannelRun,
  type PipelineFileEntry,
} from '~/data-provider';
import { useAuthContext } from '~/hooks/AuthContext';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import { cn } from '~/utils';
import { downloadPipelineArtifactUrl } from '~/lib/office/fetchPipelinePreviewBlob';
import { normalizePipelineToken } from '~/components/Pipeline/overviewChannels';
import EmptyTabPlaceholder from './EmptyTabPlaceholder';
import AllocationChannelDetail from '../channel/AllocationChannelDetail';
import ChannelRunsTab from '../channel/ChannelRunsTab';
import ChannelLogTab from '../channel/ChannelLogTab';
import XlsxPreview, {
  ReplaceFinalOutputButton,
  buildPipelineOfficeIframeSrc,
} from '../preview/XlsxPreview';
import {
  pipelineArtifactDisplayTitle,
  pipelineArtifactRoleLabel,
  pipelineArtifactTechnicalName,
} from '../preview/pipelineArtifactLabels';

const ALLOCATION_BASE_ID = 'allocation_base' as const;
const FINAL_MERGE_CHANNEL_ID = 'final_merge' as const;

type FinalAllocStep = 'merge' | 'allocate';
type AllocateSubTab = 'files' | 'runs' | 'logs';

const SUMMARY_XLSX_RE = /^成本汇总_\d{6}_汇总\.xlsx$/i;

const PREVIEWABLE_EXT = /\.(xlsx|xls|xlsm|csv)$/i;

const ALLOCATE_SUB_TABS: { id: AllocateSubTab; label: string }[] = [
  { id: 'files', label: '文件' },
  { id: 'runs', label: '运行历史' },
  { id: 'logs', label: '日志' },
];

function formatRunTimestamp(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString('zh-CN', { hour12: false });
}

function formatShortZhDateTime(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

function fileKindFromName(name: string): string {
  const m = name.match(/\.([^.]+)$/i);
  return (m?.[1] ?? 'xlsx').toLowerCase();
}

function buildAllocationMergeUploadOfficePreviewSrc(
  taskId: string,
  storageName: string,
  displayTitle: string,
  fileKind: string,
  opts?: { authorization?: string },
): string {
  const path = PipelineApi.allocationMergeBaseUploadDownloadUrl(taskId, storageName);
  const origin = typeof window !== 'undefined' ? window.location.origin : '';
  const fileUrl = `${origin}${path}`;
  const u = new URLSearchParams({
    scope: 'allocation_merge_upload',
    fileUrl,
    fileKind: fileKind.toLowerCase(),
    storageName,
    displayTitle,
    taskId,
    v: String(Date.now()),
  });
  let out = `/office-preview.html?${u.toString()}`;
  const authz = opts?.authorization?.trim();
  if (authz) {
    out += `#authorization=${encodeURIComponent(
      authz.startsWith('Bearer ') ? authz : `Bearer ${authz}`,
    )}`;
  }
  return out;
}

function pickLatestCostSummaryFromFinalMerge(
  runs: PipelineChannelRun[] | undefined,
): { run: PipelineChannelRun; file: PipelineFileEntry } | null {
  if (!runs?.length) return null;
  for (let i = runs.length - 1; i >= 0; i--) {
    const r = runs[i];
    const f = r.output_files?.find((x) => x.role === 'output' && SUMMARY_XLSX_RE.test(x.name));
    if (f) return { run: r, file: f };
  }
  return null;
}

/** 最近一条分摊合并（merge 阶段）产出，用于展示与 final_merge 产物行一致的卡片。 */
function pickLatestMergeRunForInput(
  runs: PipelineChannelRun[] | undefined,
): { run: PipelineChannelRun; file: PipelineFileEntry } | null {
  if (!runs?.length) return null;
  for (let i = runs.length - 1; i >= 0; i--) {
    const r = runs[i];
    if (String(r.allocation_phase ?? '').toLowerCase() !== 'merge') continue;
    const f = r.output_files?.find((x) => (x.role ?? 'output') === 'output');
    if (f) return { run: r, file: f };
  }
  return null;
}

/** 与「产物」分区一致：审计 JSON 归入「中间产物 · 日志/清单」（旧 run 在后端仍为 output 的，在此纠偏） */
function normalizeCostAllocatePreviewFiles(files: PipelineFileEntry[]): PipelineFileEntry[] {
  return files.map((f) =>
    /特殊规则命中\.json$/i.test(f.name) ? { ...f, role: 'auxiliary' } : f,
  );
}

function allocSidebarPhase(statusRaw: string, runsCount: number): string {
  const key = normalizePipelineToken(statusRaw);
  if (key === 'confirmed') return 'confirmed';
  if (key === 'verified_with_warning') return 'warning';
  if (key === 'running') return 'running';
  if (key === 'failed') return 'failed';
  if (['verified', 'preview_ready', 'edited', 'replaced'].includes(key)) {
    return 'pending_confirm';
  }
  if (runsCount === 0) return 'pending_run';
  return 'pending_confirm';
}

function allocPhaseStyle(phase: string): { label: string; dot: string; badge: string } {
  const greyBadge = 'border-border-medium bg-surface-secondary text-text-secondary';
  const greyDot = 'bg-text-secondary/45';
  switch (phase) {
    case 'confirmed':
      return {
        label: '已确认',
        dot: 'bg-green-400 shadow-[0_0_6px_rgba(74,222,128,0.45)]',
        badge: 'border-green-500/40 bg-green-500/10 text-green-400',
      };
    case 'warning':
      return {
        label: '有警告',
        dot: 'bg-amber-400',
        badge: 'border-amber-400/55 bg-amber-400/12 text-amber-300',
      };
    case 'running':
      return { label: '执行中', dot: 'bg-text-secondary/55', badge: greyBadge };
    case 'failed':
      return { label: '失败', dot: greyDot, badge: greyBadge };
    case 'pending_confirm':
      return { label: '待确认', dot: greyDot, badge: greyBadge };
    default:
      return { label: '待运行', dot: greyDot, badge: greyBadge };
  }
}

const iconShellClass =
  'flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-border-light bg-surface-secondary text-text-secondary';

export default function FinalAllocationTab() {
  const { token } = useAuthContext();
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedTaskId, setSelectedTaskId] = useRecoilState(pipelineSelectedTaskIdAtom);
  const setSelectedChannelId = useSetRecoilState(pipelineSelectedChannelIdAtom);
  const setSelectedRunId = useSetRecoilState(pipelineSelectedRunIdAtom);

  const taskIdParam = searchParams.get('taskId');
  const tid = taskIdParam || selectedTaskId;
  const allocRaw = searchParams.get('alloc');
  const step: FinalAllocStep = allocRaw === 'allocate' ? 'allocate' : 'merge';

  useEffect(() => {
    if (taskIdParam && taskIdParam !== selectedTaskId) setSelectedTaskId(taskIdParam);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskIdParam]);

  useEffect(() => {
    if (searchParams.get('tab') !== 'final_allocation') return;
    if (searchParams.get('alloc')) return;
    const next = new URLSearchParams(searchParams);
    next.set('alloc', 'merge');
    setSearchParams(next, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const cls = usePipelineClassification(tid);
  const classification = cls.data?.channels ?? {};
  const ab = classification[ALLOCATION_BASE_ID];
  const abFiles = ab?.files?.length ?? 0;
  const chQ = usePipelineChannel(tid, ALLOCATION_BASE_ID, { refetchInterval: 2000 });
  const finalMergeQ = usePipelineChannel(tid, FINAL_MERGE_CHANNEL_ID, { refetchInterval: 3000 });
  const triggerMut = useTriggerChannelRun(tid, ALLOCATION_BASE_ID);
  const runs = chQ.data?.runs ?? [];
  /** 与 GET /channels/{id} 一致：时间正序（旧 → 新）。listChannelRuns 才是新在前。 */
  const allocateRuns = useMemo(
    () => runs.filter((r) => (r.allocation_phase ?? '') === 'cost_allocate'),
    [runs],
  );
  const mergeRuns = useMemo(
    () => runs.filter((r) => (r.allocation_phase ?? '') === 'merge'),
    [runs],
  );
  /** 分摊合并卡片只读 merge 阶段 run 状态，避免 cost_allocate 警告污染 */
  const mergeCardStatus = useMemo(() => {
    if (mergeRuns.length === 0) return ab?.status ?? 'pending';
    return mergeRuns[mergeRuns.length - 1].status ?? 'pending';
  }, [mergeRuns, ab?.status]);
  const abPhase = allocSidebarPhase(mergeCardStatus, mergeRuns.length);
  const abVis = allocPhaseStyle(abPhase);
  const latestCostAllocateLogPrefix = allocateRuns.at(-1)?.run_id?.slice(0, 8) ?? '';
  /** 从最新往前找，优先展示最近一次有产物落盘的 cost_allocate（避免仅最后一次失败且无文件时空白） */
  const allocateRunForOutputs = useMemo(() => {
    for (let i = allocateRuns.length - 1; i >= 0; i--) {
      const r = allocateRuns[i];
      if ((r.output_files?.length ?? 0) > 0) return r;
    }
    return null;
  }, [allocateRuns]);
  const canRunAllocate = !triggerMut.isLoading && (chQ.data?.status ?? '') !== 'running';
  const [allocateSubTab, setAllocateSubTab] = useState<AllocateSubTab>('files');
  const latestSummary = useMemo(
    () => pickLatestCostSummaryFromFinalMerge(finalMergeQ.data?.runs),
    [finalMergeQ.data?.runs],
  );
  const taskQ = usePipelineTask(tid);
  const allocMeta = useMemo(() => {
    const s = taskQ.data?.state;
    if (!s || typeof s !== 'object') return {};
    const md = (s as { metadata?: unknown }).metadata;
    if (!md || typeof md !== 'object') return {};
    const a = (md as { allocation?: unknown }).allocation;
    return a && typeof a === 'object' ? (a as Record<string, unknown>) : {};
  }, [taskQ.data?.state]);

  const uploadSummaryMut = useUploadCostSummary(tid);
  const uploadMergeBaseMut = useUploadAllocationMergeBase(tid);
  const summaryInputRef = useRef<HTMLInputElement>(null);
  const mergeBaseInputRef = useRef<HTMLInputElement>(null);
  const mergeBaseReplaceInputRef = useRef<HTMLInputElement>(null);
  const [allocMergeOfficeOpen, setAllocMergeOfficeOpen] = useState(false);
  const [allocMergeOfficeSrc, setAllocMergeOfficeSrc] = useState('');

  const latestMergeRunForInput = useMemo(
    () => pickLatestMergeRunForInput(runs),
    [runs],
  );

  useEffect(() => {
    const onMsg = (ev: MessageEvent) => {
      if (ev.origin !== window.location.origin) return;
      const t = (ev.data as { type?: string })?.type;
      if (t === 'office-preview-close') {
        setAllocMergeOfficeOpen(false);
        return;
      }
      if (t === 'office-preview-saved' && tid) {
        queryClient.invalidateQueries(PIPELINE_QUERY_KEYS.task(tid));
        queryClient.invalidateQueries(PIPELINE_QUERY_KEYS.channel(tid, ALLOCATION_BASE_ID));
        queryClient.invalidateQueries(PIPELINE_QUERY_KEYS.classification(tid));
      }
    };
    window.addEventListener('message', onMsg);
    return () => window.removeEventListener('message', onMsg);
  }, [tid, queryClient]);

  if (!tid) {
    return (
      <EmptyTabPlaceholder
        icon={<Layers className="h-10 w-10" aria-hidden="true" />}
        title="最终分摊"
        description="请先在「总览」里选中一个任务，再回到这里处理分摊基数与后续分摊。"
      />
    );
  }

  const setStep = (s: FinalAllocStep) => {
    const next = new URLSearchParams(searchParams);
    next.set('tab', 'final_allocation');
    next.set('taskId', tid);
    next.set('alloc', s);
    setSearchParams(next);
  };

  return (
    <div className="flex h-full min-h-0 overflow-hidden">
      <aside className="flex h-full w-[272px] shrink-0 flex-col overflow-hidden border-r border-border-light bg-surface-secondary/40">
        <div className="flex-none border-b border-border-light px-3 py-3">
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-semibold text-text-primary">最终分摊</h3>
                <span
                  className="inline-flex text-text-secondary hover:text-text-primary"
                  title="分摊合并对应原「分摊基数」渠道；分摊页将用于后续成本汇总与出摊结果。"
                >
                  <CircleHelp className="h-3.5 w-3.5" aria-hidden />
                </span>
              </div>
              <p className="mt-1 text-[11px] text-text-secondary">2 个步骤</p>
            </div>
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto px-2 py-2">
          <div className="space-y-1.5">
            <button
              type="button"
              onClick={() => setStep('merge')}
              className={cn(
                'flex w-full items-center gap-2 rounded-xl border px-2.5 py-2 text-left transition-colors',
                step === 'merge'
                  ? 'border-blue-500/55 bg-blue-500/10 ring-1 ring-blue-500/25'
                  : 'border-border-light bg-surface-primary hover:bg-surface-hover/90',
              )}
            >
              <div className={iconShellClass}>
                <PieChart className="h-4 w-4" aria-hidden />
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-1.5">
                  <span className={cn('inline-block h-2 w-2 shrink-0 rounded-full', abVis.dot)} />
                  <span className="truncate text-sm font-medium text-text-primary">分摊合并</span>
                </div>
                <div className="mt-0.5 text-[11px] text-text-secondary">
                  分摊基数 · 文件{' '}
                  <span className="tabular-nums text-text-primary">{abFiles}</span> 个
                </div>
              </div>
              <div className="flex shrink-0 flex-col items-end gap-1">
                <span
                  className={cn(
                    'rounded-md border px-1.5 py-0.5 text-[10px] font-medium',
                    abVis.badge,
                  )}
                >
                  {abVis.label}
                </span>
                <ChevronRight className="h-4 w-4 text-text-secondary" aria-hidden />
              </div>
            </button>

            <button
              type="button"
              onClick={() => setStep('allocate')}
              className={cn(
                'flex w-full items-center gap-2 rounded-xl border px-2.5 py-2 text-left transition-colors',
                step === 'allocate'
                  ? 'border-blue-500/55 bg-blue-500/10 ring-1 ring-blue-500/25'
                  : 'border-border-light bg-surface-primary hover:bg-surface-hover/90',
              )}
            >
              <div className={iconShellClass}>
                <Layers className="h-4 w-4" aria-hidden />
              </div>
              <div className="min-w-0 flex-1">
                <span className="truncate text-sm font-medium text-text-primary">分摊</span>
                <div className="mt-0.5 text-[11px] text-text-secondary">成本出摊 · cost_allocate</div>
              </div>
              <ChevronRight className="h-4 w-4 shrink-0 text-text-secondary" aria-hidden />
            </button>
          </div>
        </div>
      </aside>

      <div className="min-h-0 min-w-0 flex-1 overflow-hidden">
        {step === 'merge' ? (
          <AllocationChannelDetail
            taskId={tid}
            channelId={ALLOCATION_BASE_ID}
            onBack={() => {
              setSelectedChannelId(null);
              setSelectedRunId(null);
              const next = new URLSearchParams(searchParams);
              next.set('tab', 'overview');
              next.delete('channel');
              next.delete('runId');
              next.delete('alloc');
              setSearchParams(next, { replace: true });
            }}
          />
        ) : (
          <div className="flex h-full flex-col overflow-hidden">
            <div className="flex-none border-b border-border-light bg-surface-primary px-4 py-3">
              <div className="flex items-center justify-between gap-2">
                <div>
                  <h2 className="text-base font-semibold text-text-primary">分摊</h2>
                  <p className="mt-0.5 text-xs text-text-secondary">
                    以 pingpong cost_allocate.py 原逻辑执行；输入为最终合并（final_merge · cost_summary）产出的「成本汇总_*_汇总.xlsx」。
                  </p>
                </div>
                <button
                  type="button"
                  disabled={!canRunAllocate}
                  onClick={() => {
                    setAllocateSubTab('logs');
                    triggerMut.mutate({
                      allocation_phase: 'cost_allocate',
                      allocation_options: { action: 'build' },
                    });
                  }}
                  className="inline-flex items-center gap-1 rounded-md bg-green-600 px-3 py-1.5 text-xs font-medium text-white disabled:opacity-40"
                >
                  <RotateCcw className="h-3.5 w-3.5" />
                  {triggerMut.isLoading ? '提交…' : '执行分摊'}
                </button>
              </div>
              {triggerMut.error ? (
                <div className="mt-2 text-xs text-red-300">{(triggerMut.error as Error).message}</div>
              ) : null}

              <nav className="mt-3 flex gap-1 border-t border-border-light pt-2" aria-label="分摊子页">
                {ALLOCATE_SUB_TABS.map((t) => (
                  <button
                    key={t.id}
                    type="button"
                    onClick={() => setAllocateSubTab(t.id)}
                    className={cn(
                      'relative px-3 py-1.5 text-xs font-medium transition-colors',
                      'border-b-2 border-transparent',
                      allocateSubTab === t.id
                        ? 'border-green-500 text-text-primary'
                        : 'text-text-secondary hover:border-border-medium hover:text-text-primary',
                    )}
                  >
                    {t.label}
                  </button>
                ))}
              </nav>
            </div>

            <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
              {allocateSubTab === 'files' && (
                <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
                  {/* ── 两个输入文件状态卡 ── */}
                  <input ref={summaryInputRef} type="file" accept=".xlsx" className="hidden"
                    aria-label="上传成本汇总表"
                    onChange={(e) => { const f = e.target.files?.[0]; e.target.value = ''; if (f) uploadSummaryMut.mutate(f); }} />
                  <input ref={mergeBaseInputRef} type="file" accept=".xlsx" className="hidden"
                    aria-label="上传分摊基数表"
                    onChange={(e) => { const f = e.target.files?.[0]; e.target.value = ''; if (f) uploadMergeBaseMut.mutate(f); }} />

                  <div className="flex-none space-y-2 border-b border-border-light bg-surface-secondary/25 px-4 py-3">
                    <p className="text-[11px] font-medium text-text-secondary">分摊输入（2 个需求文件）</p>

                    {/* 成本汇总表：优先展示本次上传结果，其次流水线生成 */}
                    <div className="flex items-center gap-3 rounded-lg border border-border-light bg-surface-primary px-3 py-2">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1.5 text-xs">
                          <span className="font-medium text-text-primary">成本汇总表</span>
                          {uploadSummaryMut.data ? (
                            <>
                              <span className="rounded-md border border-blue-500/40 bg-blue-500/10 px-1.5 py-0.5 text-[10px] text-blue-300">直接上传</span>
                              <span className="truncate font-mono text-[11px] text-text-primary">{uploadSummaryMut.data.name}</span>
                              <span className="text-[11px] text-text-tertiary">
                                · {new Date(uploadSummaryMut.data.uploaded_at).toLocaleString('zh-CN', { hour12: false })}
                              </span>
                            </>
                          ) : latestSummary ? (
                            <>
                              <span className="rounded-md border border-green-500/40 bg-green-500/10 px-1.5 py-0.5 text-[10px] text-green-400">流水线生成</span>
                              <span className="truncate font-mono text-[11px] text-text-primary">
                                {latestSummary.file.name}
                              </span>
                              <span className="text-[11px] text-text-tertiary">
                                · {formatRunTimestamp(latestSummary.run.finished_at ?? latestSummary.run.started_at)}
                              </span>
                            </>
                          ) : (
                            <span className="text-[11px] text-text-secondary">流水线未生成</span>
                          )}
                        </div>
                        {uploadSummaryMut.error && (
                          <p className="mt-0.5 text-[11px] text-red-300">{(uploadSummaryMut.error as Error).message}</p>
                        )}
                      </div>
                      <button type="button" disabled={uploadSummaryMut.isLoading}
                        onClick={() => summaryInputRef.current?.click()}
                        className="inline-flex shrink-0 items-center gap-1 rounded-md border border-border-light px-2 py-1 text-[11px] text-text-secondary hover:bg-surface-hover disabled:opacity-40"
                        title="直接上传成本汇总表，绕过最终合并流程">
                        <FolderUp className="h-3.5 w-3.5" />
                        {uploadSummaryMut.isLoading ? '上传中…' : '上传'}
                      </button>
                    </div>

                    {/* 分摊基数表 */}
                    <div className="flex items-center gap-3 rounded-lg border border-border-light bg-surface-primary px-3 py-2">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1.5 text-xs">
                          <span className="font-medium text-text-primary">分摊基数表</span>
                          {allocMeta.merge_output || allocMeta.merge_output_name ? (
                            <>
                              <span className={allocMeta.merge_output_is_upload
                                ? 'rounded-md border border-blue-500/40 bg-blue-500/10 px-1.5 py-0.5 text-[10px] text-blue-300'
                                : 'rounded-md border border-green-500/40 bg-green-500/10 px-1.5 py-0.5 text-[10px] text-green-400'}>
                                {allocMeta.merge_output_is_upload ? '直接上传' : '合并生成'}
                              </span>
                              <span className="truncate font-mono text-[11px] text-text-primary">
                                {String(allocMeta.merge_output_name || allocMeta.merge_output || '').split('/').pop() || '—'}
                              </span>
                              {allocMeta.merge_output_uploaded_at && (
                                <span className="text-[11px] text-text-tertiary">
                                  · {new Date(String(allocMeta.merge_output_uploaded_at)).toLocaleString('zh-CN', { hour12: false })}
                                </span>
                              )}
                            </>
                          ) : uploadMergeBaseMut.data ? (
                            <>
                              <span className="rounded-md border border-blue-500/40 bg-blue-500/10 px-1.5 py-0.5 text-[10px] text-blue-300">直接上传</span>
                              <span className="truncate font-mono text-[11px] text-text-primary">{uploadMergeBaseMut.data.name}</span>
                            </>
                          ) : (
                            <span className="text-[11px] text-text-secondary">流水线未生成</span>
                          )}
                        </div>
                        {uploadMergeBaseMut.error && (
                          <p className="mt-0.5 text-[11px] text-red-300">{(uploadMergeBaseMut.error as Error).message}</p>
                        )}
                      </div>
                      <button type="button" disabled={uploadMergeBaseMut.isLoading}
                        onClick={() => mergeBaseInputRef.current?.click()}
                        className="inline-flex shrink-0 items-center gap-1 rounded-md border border-border-light px-2 py-1 text-[11px] text-text-secondary hover:bg-surface-hover disabled:opacity-40"
                        title="直接上传分摊基数合并表，跳过 QuickBI / CitiHK 流程">
                        <FolderUp className="h-3.5 w-3.5" />
                        {uploadMergeBaseMut.isLoading ? '上传中…' : '上传'}
                      </button>
                    </div>
                  </div>

                  {/* ── 成本汇总预览：直接上传时显示轻量行，流水线生成时显示 XlsxPreview ── */}
                  {/* ── 分摊输入文件列表（成本汇总 + 分摊基数表） ── */}
                  {(() => {
                    const mergeBaseName = uploadMergeBaseMut.data?.name
                      || String(allocMeta.merge_output_name || allocMeta.merge_output || '').split('/').pop()
                      || null;
                    const mergeBaseTime = uploadMergeBaseMut.data?.uploaded_at
                      || (allocMeta.merge_output_uploaded_at ? String(allocMeta.merge_output_uploaded_at) : null);
                    const mergeBaseSize = uploadMergeBaseMut.data?.size ?? null;
                    const mergeBaseIsUpload = !!(
                      uploadMergeBaseMut.data || allocMeta.merge_output_is_upload === true
                    );

                    const summaryFile = uploadSummaryMut.data ?? latestSummary?.file ?? null;
                    const totalInputs = (summaryFile ? 1 : 0) + (mergeBaseName ? 1 : 0);

                    return totalInputs > 0 ? (
                      <div className="flex min-h-0 flex-1 flex-col overflow-auto">
                        {/* 区块标题 */}
                        <div className="flex-none border-b border-border-light bg-surface-secondary/30 px-4 py-2 text-[11px] font-medium text-text-secondary">
                          分摊输入（{totalInputs} 个文件）
                        </div>

                        {/* 成本汇总表 */}
                        {uploadSummaryMut.data ? (
                          <div className="flex items-center gap-3 border-b border-border-light px-4 py-2.5 text-[11px] text-text-secondary">
                            <span className="min-w-0 flex-1 truncate font-medium text-text-primary">{uploadSummaryMut.data.name}</span>
                            <span className="shrink-0 text-text-tertiary">{(uploadSummaryMut.data.size / 1024).toFixed(1)} KB</span>
                            <span className="shrink-0 text-text-tertiary">· 上传于 {new Date(uploadSummaryMut.data.uploaded_at).toLocaleString('zh-CN', { hour12: false })}</span>
                          </div>
                        ) : latestSummary ? (
                          <div className="flex max-h-[min(40vh,32rem)] min-h-0 flex-col overflow-hidden">
                            <XlsxPreview
                              taskId={tid}
                              channelId={FINAL_MERGE_CHANNEL_ID}
                              runId={latestSummary.run.run_id}
                              outputFiles={[latestSummary.file]}
                              artifactDockTitle="分摊输入"
                              primarySectionTitle="成本汇总表（分摊输入）"
                              primaryOutputRowMeta={
                                <>
                                  生成时间：
                                  <span className="tabular-nums text-text-primary">
                                    {formatRunTimestamp(
                                      latestSummary.run.finished_at ?? latestSummary.run.started_at,
                                    )}
                                  </span>
                                  <span className="mx-1 text-text-tertiary">·</span>
                                  最终合并 run{' '}
                                  <span className="font-mono text-text-primary">
                                    {latestSummary.run.run_id.slice(0, 8)}
                                  </span>
                                </>
                              }
                            />
                          </div>
                        ) : null}

                        {/* 分摊基数表（与成本汇总产物行同一卡片样式） */}
                        {mergeBaseName &&
                          (() => {
                            const pair = !mergeBaseIsUpload ? latestMergeRunForInput : null;
                            let pipelineFile = pair?.file ?? null;
                            const pipelineRun = pair?.run ?? null;
                            if (pair && mergeBaseName && pipelineFile && pipelineFile.name !== mergeBaseName) {
                              const alt = pair.run.output_files?.find((x) => x.name === mergeBaseName);
                              if (alt) pipelineFile = alt;
                            }
                            const displayTitle = pipelineArtifactDisplayTitle(mergeBaseName);
                            const tech = pipelineArtifactTechnicalName(mergeBaseName);
                            const kind = fileKindFromName(mergeBaseName);
                            const canOffice = PREVIEWABLE_EXT.test(mergeBaseName);
                            const uploadHref = PipelineApi.allocationMergeBaseUploadDownloadUrl(
                              tid,
                              mergeBaseName,
                            );
                            const pipelineHref =
                              pipelineRun && pipelineFile
                                ? PipelineApi.runFileDownloadUrl(
                                    tid,
                                    ALLOCATION_BASE_ID,
                                    pipelineRun.run_id,
                                    pipelineFile.name,
                                  )
                                : '';
                            const sizeBytes =
                              mergeBaseSize != null ? mergeBaseSize : pipelineFile?.size ?? null;
                            const subLineTimeIso = mergeBaseIsUpload
                              ? mergeBaseTime
                              : pipelineFile?.created_at ??
                                pipelineRun?.finished_at ??
                                pipelineRun?.started_at;

                            return (
                              <div className="min-h-0 border-b border-border-light bg-surface-secondary">
                                <div className="px-4 pb-3 pt-2">
                                  <div className="text-[11px] font-semibold uppercase tracking-wide text-text-secondary">
                                    分摊基数表（分摊输入）
                                    <span className="ml-1.5 font-normal normal-case text-text-secondary">
                                      （1）
                                    </span>
                                  </div>
                                  <div className="mt-2 space-y-2">
                                    <div className="rounded-md border border-border-light bg-surface-primary px-3 py-2 text-text-primary">
                                      <div className="flex flex-wrap items-start justify-between gap-3">
                                        <div className="min-w-0 flex-1">
                                          <div className="truncate text-sm font-medium text-text-primary">
                                            {displayTitle}
                                          </div>
                                          <div className="mt-0.5 truncate font-mono text-[11px] text-text-secondary">
                                            {tech}
                                            {' · '}
                                            <span>
                                              {mergeBaseIsUpload
                                                ? '直接上传'
                                                : pipelineArtifactRoleLabel(pipelineFile?.role ?? 'output')}
                                            </span>
                                            {typeof sizeBytes === 'number' ? (
                                              <>
                                                {' '}
                                                · {(sizeBytes / 1024).toFixed(1)} KB
                                              </>
                                            ) : null}
                                            {subLineTimeIso ? (
                                              <>
                                                {' '}
                                                · {mergeBaseIsUpload ? '上传于' : '生成于'}{' '}
                                                {formatShortZhDateTime(subLineTimeIso)}
                                              </>
                                            ) : null}
                                          </div>
                                        </div>
                                        <div className="flex max-w-full shrink-0 flex-col items-end gap-2">
                                          <div className="max-w-[min(100%,20rem)] text-right text-[11px] leading-snug text-text-secondary">
                                            {mergeBaseIsUpload ? (
                                              <>
                                                上传时间：
                                                <span className="tabular-nums text-text-primary">
                                                  {formatRunTimestamp(mergeBaseTime)}
                                                </span>
                                              </>
                                            ) : pipelineRun ? (
                                              <>
                                                生成时间：
                                                <span className="tabular-nums text-text-primary">
                                                  {formatRunTimestamp(
                                                    pipelineRun.finished_at ?? pipelineRun.started_at,
                                                  )}
                                                </span>
                                                <span className="mx-1 text-text-tertiary">·</span>
                                                分摊合并 run{' '}
                                                <span className="font-mono text-text-primary">
                                                  {pipelineRun.run_id.slice(0, 8)}
                                                </span>
                                              </>
                                            ) : (
                                              <span className="text-amber-300/90">
                                                暂无对应合并 run，请执行分摊合并
                                              </span>
                                            )}
                                          </div>
                                          <div className="flex flex-wrap justify-end gap-2">
                                            {canOffice && (mergeBaseIsUpload || pipelineRun) ? (
                                              <button
                                                type="button"
                                                onClick={() => {
                                                  const authz = token ? `Bearer ${token}` : undefined;
                                                  if (mergeBaseIsUpload) {
                                                    setAllocMergeOfficeSrc(
                                                      buildAllocationMergeUploadOfficePreviewSrc(
                                                        tid,
                                                        mergeBaseName,
                                                        displayTitle,
                                                        kind,
                                                        { authorization: authz },
                                                      ),
                                                    );
                                                  } else if (pipelineRun && pipelineFile) {
                                                    setAllocMergeOfficeSrc(
                                                      buildPipelineOfficeIframeSrc(
                                                        tid,
                                                        ALLOCATION_BASE_ID,
                                                        pipelineRun.run_id,
                                                        pipelineFile.name,
                                                        displayTitle,
                                                        kind,
                                                        { authorization: authz },
                                                      ),
                                                    );
                                                  }
                                                  setAllocMergeOfficeOpen(true);
                                                }}
                                                className="inline-flex items-center gap-1.5 rounded-md border border-border-light bg-surface-secondary px-3 py-1.5 text-xs font-medium text-text-primary hover:bg-surface-hover"
                                              >
                                                <ExternalLink className="h-3.5 w-3.5" />
                                                打开
                                              </button>
                                            ) : null}
                                            {mergeBaseIsUpload || pipelineHref ? (
                                              <button
                                                type="button"
                                                onClick={() => {
                                                  const u = mergeBaseIsUpload ? uploadHref : pipelineHref;
                                                  void downloadPipelineArtifactUrl(u, mergeBaseName).catch(
                                                    (e) => window.alert((e as Error).message),
                                                  );
                                                }}
                                                className="inline-flex items-center gap-1.5 rounded-md border border-green-500/40 bg-green-500/10 px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-green-500/15"
                                              >
                                                <Download className="h-3.5 w-3.5" />
                                                下载
                                              </button>
                                            ) : (
                                              <span className="inline-flex cursor-not-allowed items-center gap-1.5 rounded-md border border-border-light px-3 py-1.5 text-xs font-medium text-text-tertiary opacity-50">
                                                <Download className="h-3.5 w-3.5" />
                                                下载
                                              </span>
                                            )}
                                            {mergeBaseIsUpload ? (
                                              <span className="inline-flex flex-col items-end gap-0.5">
                                                <input
                                                  ref={mergeBaseReplaceInputRef}
                                                  type="file"
                                                  accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                                  className="hidden"
                                                  aria-label={`替换上传的分摊基数表 ${mergeBaseName}`}
                                                  onChange={async (e) => {
                                                    const file = e.target.files?.[0];
                                                    if (!file) return;
                                                    if (file.name !== mergeBaseName) {
                                                      window.alert(
                                                        `须选择与当前文件同名「${mergeBaseName}」，当前为「${file.name}」`,
                                                      );
                                                      e.target.value = '';
                                                      return;
                                                    }
                                                    try {
                                                      await uploadMergeBaseMut.mutateAsync(file);
                                                    } finally {
                                                      e.target.value = '';
                                                    }
                                                  }}
                                                />
                                                <button
                                                  type="button"
                                                  onClick={() => mergeBaseReplaceInputRef.current?.click()}
                                                  disabled={uploadMergeBaseMut.isLoading}
                                                  className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover disabled:opacity-50"
                                                >
                                                  <Upload className="h-3 w-3" />
                                                  {uploadMergeBaseMut.isLoading ? '上传…' : '替换'}
                                                </button>
                                              </span>
                                            ) : pipelineRun && pipelineFile ? (
                                              <ReplaceFinalOutputButton
                                                taskId={tid}
                                                channelId={ALLOCATION_BASE_ID}
                                                runId={pipelineRun.run_id}
                                                outputFileName={pipelineFile.name}
                                              />
                                            ) : null}
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            );
                          })()}
                      </div>
                    ) : (
                      <div className="min-h-0 flex-1 overflow-auto" />
                    );
                  })()}

                  {/* ── 成本分摊输出 ── */}
                  {allocateRunForOutputs ? (
                    <div className="flex max-h-[min(52vh,36rem)] shrink-0 flex-col overflow-hidden border-t border-border-light">
                      <p className="flex-none border-b border-border-light bg-surface-secondary/40 px-4 py-2 text-[11px] leading-snug text-text-secondary">
                        「特殊规则命中」JSON：按 SR_* 记录本期特殊分摊规则命中次数，及「本期从未命中」清单，便于核对规则是否生效。
                      </p>
                      <XlsxPreview
                        taskId={tid}
                        channelId={ALLOCATION_BASE_ID}
                        runId={allocateRunForOutputs.run_id}
                        outputFiles={normalizeCostAllocatePreviewFiles(
                          allocateRunForOutputs.output_files,
                        )}
                        artifactDockTitle="产物"
                        primarySectionTitle="成本分摊输出"
                        primaryOutputRowMeta={
                          <>
                            生成时间：
                            <span className="tabular-nums text-text-primary">
                              {formatRunTimestamp(
                                allocateRunForOutputs.finished_at ??
                                  allocateRunForOutputs.started_at,
                              )}
                            </span>
                            <span className="mx-1 text-text-tertiary">·</span>
                            分摊 run{' '}
                            <span className="font-mono text-text-primary">
                              {allocateRunForOutputs.run_id.slice(0, 8)}
                            </span>
                          </>
                        }
                      />
                    </div>
                  ) : null}
                </div>
              )}

              {allocateSubTab === 'runs' && (
                <div className="min-h-0 flex-1 overflow-auto">
                  <ChannelRunsTab
                    taskId={tid}
                    channelId={ALLOCATION_BASE_ID}
                    runs={allocateRuns}
                  />
                </div>
              )}

              {allocateSubTab === 'logs' && (
                <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
                  <div className="min-h-0 flex-1 overflow-hidden">
                    <ChannelLogTab
                      taskId={tid}
                      channelId={ALLOCATION_BASE_ID}
                      scope="channel"
                      runIdPrefixFilter={latestCostAllocateLogPrefix}
                    />
                  </div>
                  {allocateRunForOutputs ? (
                    <div className="flex max-h-[min(40vh,28rem)] shrink-0 flex-col overflow-hidden border-t border-border-light">
                      <XlsxPreview
                        taskId={tid}
                        channelId={ALLOCATION_BASE_ID}
                        runId={allocateRunForOutputs.run_id}
                        outputFiles={normalizeCostAllocatePreviewFiles(
                          allocateRunForOutputs.output_files,
                        )}
                        artifactDockTitle="产物"
                        primarySectionTitle="成本分摊输出"
                        primaryOutputRowMeta={
                          <>
                            生成时间：
                            <span className="tabular-nums text-text-primary">
                              {formatRunTimestamp(
                                allocateRunForOutputs.finished_at ??
                                  allocateRunForOutputs.started_at,
                              )}
                            </span>
                            <span className="mx-1 text-text-tertiary">·</span>
                            分摊 run{' '}
                            <span className="font-mono text-text-primary">
                              {allocateRunForOutputs.run_id.slice(0, 8)}
                            </span>
                          </>
                        }
                      />
                    </div>
                  ) : null}
                </div>
              )}

              <OGDialog open={allocMergeOfficeOpen} onOpenChange={setAllocMergeOfficeOpen}>
                <OGDialogContent
                  showCloseButton={false}
                  className={cn(
                    'fixed inset-0 left-0 top-0 z-50 h-screen w-screen max-w-none translate-x-0 translate-y-0 rounded-none border-0 bg-black p-0',
                    'overflow-hidden sm:max-w-none',
                  )}
                >
                  {allocMergeOfficeOpen && allocMergeOfficeSrc ? (
                    <iframe
                      key={allocMergeOfficeSrc}
                      title="Office 编辑器"
                      src={allocMergeOfficeSrc}
                      className="h-full w-full border-0 bg-zinc-950"
                    />
                  ) : null}
                </OGDialogContent>
              </OGDialog>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
