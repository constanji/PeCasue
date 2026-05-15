import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Archive, CheckCircle2, ChevronLeft, FolderUp, RotateCcw } from 'lucide-react';
import { useSetRecoilState } from 'recoil';
import {
  useConfirmChannel,
  usePipelineChannel,
  usePipelineClassification,
  usePipelineTask,
  useTriggerChannelRun,
  useUploadAllocationMergeBase,
  useUploadChannelZipReplace,
  type PipelineChannelRun,
  type PipelineChannelRunStatus,
  type PipelineClassificationFile,
} from '~/data-provider';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import { normalizePipelineToken } from '~/components/Pipeline/overviewChannels';
import { cn } from '~/utils';
import ChannelFilesTab from './ChannelFilesTab';
import ChannelRunsTab from './ChannelRunsTab';
import ChannelVerifyTab from './ChannelVerifyTab';
import ChannelLogTab from './ChannelLogTab';
import XlsxPreview from '../preview/XlsxPreview';

type AllocationSection = 'quickbi' | 'citihk' | 'merge';
type SubTab = 'files' | 'runs' | 'verify' | 'logs';

const SECTIONS: { id: AllocationSection; label: string; hint: string }[] = [
  { id: 'quickbi', label: 'QuickBI', hint: '' },
  { id: 'citihk', label: 'CitiHK', hint: '' },
  { id: 'merge', label: '合并', hint: '至少一侧（QuickBI / CitiHK）有有效产出即可合并；缺失侧不会并入，校验中会有提示' },
];

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

function runPhase(r: PipelineChannelRun): string {
  return r.allocation_phase ?? 'inventory';
}

/** 合并完成后若 QuickBI / CitiHK 任一侧又跑过新的一次成功运行，应提示重新合并 */
function mergeNeedsRegeneration(allRuns: PipelineChannelRun[] | undefined): boolean {
  if (!allRuns?.length) return false;
  const mergeRuns = allRuns.filter((r) => runPhase(r) === 'merge' && r.finished_at);
  if (!mergeRuns.length) return false;
  const latestMerge = mergeRuns.reduce((best, r) =>
    (r.finished_at! > best.finished_at! ? r : best),
  );
  const mt = latestMerge.finished_at!;
  const qbNewer = allRuns.some(
    (r) => runPhase(r) === 'quickbi' && r.finished_at && r.finished_at > mt,
  );
  const chNewer = allRuns.some(
    (r) => runPhase(r) === 'citihk' && r.finished_at && r.finished_at > mt,
  );
  return qbNewer || chNewer;
}

function looksQuickBiExport(bn: string, base: string): boolean {
  return (
    bn.includes('finance_channel_inbound') ||
    bn.includes('finance_channel_outbound') ||
    bn.includes('finance_channel_valid_va') ||
    base.includes('入金') ||
    base.includes('出金') ||
    bn.includes('vaads_quickbi') ||
    (bn.includes('quickbi') && /(^|[^a-z])va([^a-z]|$)/i.test(base))
  );
}

function fileMatchesSection(rel: string, section: AllocationSection): boolean {
  const lower = rel.toLowerCase();
  const base = rel.split(/[/\\]+/).filter(Boolean).pop() ?? rel;
  const bn = base.toLowerCase();
  if (section === 'merge') return true;
  if (section === 'quickbi') {
    return looksQuickBiExport(bn, base) || bn.includes('inbound_sm') || bn.includes('outbound_sm');
  }
  const qb = looksQuickBiExport(bn, base);
  return (
    lower.includes('citihk') ||
    base.includes('资金流') ||
    bn.startsWith('2-inbound') ||
    bn.startsWith('4outbound') ||
    (!qb && bn.includes('inbound')) ||
    (!qb && bn.includes('outbound'))
  );
}

function filterClassificationFiles(
  files: PipelineClassificationFile[],
  section: AllocationSection,
): PipelineClassificationFile[] {
  return files.filter((f) => fileMatchesSection(f.rel_path, section));
}

function allocationMetaFromState(state: unknown): Record<string, unknown> {
  if (!state || typeof state !== 'object') return {};
  const md = (state as { metadata?: unknown }).metadata;
  if (!md || typeof md !== 'object') return {};
  const a = (md as { allocation?: unknown }).allocation;
  return a && typeof a === 'object' ? (a as Record<string, unknown>) : {};
}

export default function AllocationChannelDetail({
  taskId,
  channelId,
  onBack,
}: {
  taskId: string;
  channelId: string;
  onBack: () => void;
}) {
  const [section, setSection] = useState<AllocationSection>('quickbi');
  const [tab, setTab] = useState<SubTab>('files');
  const [buMode, setBuMode] = useState<'simulated' | 'excel'>('simulated');
  const [excelDetailName, setExcelDetailName] = useState('');

  const channelQuery = usePipelineChannel(taskId, channelId, {
    refetchInterval: 2000,
  });
  /** 与特殊来源「合并」签发顺序对齐：须先在同一任务下将 special_merge 审计签发 */
  const specialMergeQuery = usePipelineChannel(taskId, 'special_merge', {
    refetchInterval: 3000,
  });
  const taskQuery = usePipelineTask(taskId);
  const cls = usePipelineClassification(taskId);
  const triggerMut = useTriggerChannelRun(taskId, channelId);
  const confirmMut = useConfirmChannel(taskId, channelId);
  const uploadMergeBaseMut = useUploadAllocationMergeBase(taskId);
  const uploadZipReplace = useUploadChannelZipReplace(taskId);
  const mergeBaseInputRef = useRef<HTMLInputElement>(null);
  const zipInputRef = useRef<HTMLInputElement>(null);

  const setSelectedTaskId = useSetRecoilState(pipelineSelectedTaskIdAtom);
  const setSelectedChannelId = useSetRecoilState(pipelineSelectedChannelIdAtom);
  const setSelectedRunId = useSetRecoilState(pipelineSelectedRunIdAtom);

  const ch = channelQuery.data;
  const status = ch?.status ?? 'pending';
  const runs = ch?.runs ?? [];
  const groupFiles = cls.data?.channels?.[channelId]?.files ?? [];

  const allocationMeta = allocationMetaFromState(taskQuery.data?.state);
  const quickbiReady = allocationMeta.quickbi_ready === true;
  const citihkReady = allocationMeta.citihk_ready === true;
  const mergeBlocked =
    triggerMut.isLoading || status === 'running';

  const mergeNeedsRedo = useMemo(() => mergeNeedsRegeneration(runs), [runs]);

  const runsForSection = useMemo(() => {
    return runs.filter((r) => {
      const ph = runPhase(r);
      if (section === 'quickbi') return ph === 'quickbi' || ph === 'inventory';
      if (section === 'citihk') return ph === 'citihk';
      return ph === 'merge';
    });
  }, [runs, section]);

  const lastRun = runsForSection[runsForSection.length - 1];
  const filesForSection = useMemo(
    () => filterClassificationFiles(groupFiles, section),
    [groupFiles, section],
  );

  const mergeIssued =
    normalizePipelineToken(specialMergeQuery.data?.status ?? '') === 'confirmed';

  const canAuditConfirm =
    runs.length > 0 &&
    status !== 'running' &&
    status !== 'confirmed' &&
    status !== 'failed' &&
    mergeIssued;

  useEffect(() => {
    setSelectedTaskId(taskId);
    setSelectedChannelId(channelId);
  }, [taskId, channelId, setSelectedTaskId, setSelectedChannelId]);

  useEffect(() => {
    setSelectedRunId(lastRun?.run_id ?? null);
  }, [lastRun?.run_id, setSelectedRunId]);

  const fireRun = (allocation_phase: AllocationSection, allocation_options: Record<string, unknown>) => {
    setTab('logs');
    triggerMut.mutate({ allocation_phase, allocation_options });
  };

  const activeHint = SECTIONS.find((s) => s.id === section)?.hint ?? '';

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
            {/* 整包上传：解压后写入 allocation_base extracted 目录 */}
            <input
              ref={zipInputRef}
              type="file"
              accept=".zip,.rar,.7z"
              className="hidden"
              aria-label="上传压缩包以写入分摊基数渠道目录"
              onChange={(e) => {
                const f = e.target.files?.[0];
                e.target.value = '';
                if (f) uploadZipReplace.mutate({ channelId, file: f });
              }}
            />
            <button
              type="button"
              title="上传压缩包（zip / rar / 7z），解压后整目录写入 allocation_base"
              disabled={uploadZipReplace.isLoading || status === 'running'}
              onClick={() => zipInputRef.current?.click()}
              className="inline-flex items-center gap-1 rounded-md border border-border-light px-2.5 py-1.5 text-xs text-text-secondary hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Archive className="h-3.5 w-3.5" />
              {uploadZipReplace.isLoading ? '上传中…' : '上传源文件'}
            </button>
            <button
              type="button"
              disabled={!canAuditConfirm || confirmMut.isLoading}
              onClick={() => confirmMut.mutate()}
              className="inline-flex items-center gap-1 rounded-md border border-green-500/50 bg-green-500/10 px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-green-500/15 disabled:cursor-not-allowed disabled:opacity-45"
              title={
                !mergeIssued
                  ? '请先在「Ach return · 内转」渠道详情中的「合并」分区完成合并工作簿的审计签发'
                  : status === 'confirmed'
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
          </div>
        </div>

        {!mergeIssued && (
          <div className="mt-2 rounded-md border border-amber-500/35 bg-amber-500/10 px-3 py-2 text-[11px] leading-snug text-amber-100">
            <span className="font-medium text-amber-50">签发顺序：</span>
            请先在同一任务的「Ach return · 内转」→「合并」中完成合并工作簿的审计签发后，本分摊基数渠道方可签发。
          </div>
        )}

        <nav className="mt-3 flex flex-wrap gap-1" aria-label="分摊基数分区">
          {SECTIONS.map((s) => (
            <button
              key={s.id}
              type="button"
              onClick={() => {
                setSection(s.id);
                setTab('files');
              }}
              className={cn(
                'rounded-md px-3 py-1.5 text-xs font-medium transition-colors',
                section === s.id
                  ? 'bg-green-500/15 text-text-primary ring-1 ring-green-500/35'
                  : 'text-text-secondary hover:bg-surface-hover hover:text-text-primary',
              )}
            >
              {s.label}
            </button>
          ))}
        </nav>

        {(activeHint || section === 'merge') && (
          <div className="mt-2 rounded-md border border-border-light bg-surface-secondary/30 px-3 py-2 text-[11px] leading-snug text-text-secondary">
            {activeHint ? <span>{activeHint}</span> : null}
            {section === 'merge' && (!quickbiReady || !citihkReady) && (
              <div className="mt-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[11px] text-amber-100">
                QuickBI / CitiHK 未全部就绪时仍可点击「执行合并」。缺失侧将跳过并入，汇总表可能仅为单侧数据；请在
                <span className="font-medium text-amber-50">校验报告</span>
                与日志中查看「部分合并」提示。
              </div>
            )}
            {section === 'merge' && mergeNeedsRedo && (
              <div className="mt-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[11px] text-amber-100">
                检测到 QuickBI 或 CitiHK 在「上次合并」之后有新的成功运行。合并结果可能已过时，请再次点击
                <span className="font-medium">执行合并</span> 以生成最新收付款基数表。
              </div>
            )}
            {section === 'merge' && (
              <div
                className={cn(
                  'font-mono text-[10px] text-text-tertiary',
                  activeHint && 'mt-1',
                )}
              >
                QuickBI 就绪: {quickbiReady ? '是' : '否'} · CitiHK 就绪:{' '}
                {citihkReady ? '是' : '否'}
              </div>
            )}
          </div>
        )}

        {section === 'quickbi' && (
          <div className="mt-3 space-y-2 rounded-lg border border-border-light bg-surface-secondary/20 p-3">
            <div className="flex flex-wrap gap-3 text-[11px] text-text-secondary">
              <span className="font-medium text-text-primary">BU 口径：</span>
              <label className="flex cursor-pointer items-center gap-1">
                <input
                  type="radio"
                  name="buMode"
                  checked={buMode === 'simulated'}
                  onChange={() => setBuMode('simulated')}
                />
                模拟公式 BU（一步生成含汇总的 QuickBI 中间表）
              </label>
              <label className="flex cursor-pointer items-center gap-1">
                <input
                  type="radio"
                  name="buMode"
                  checked={buMode === 'excel'}
                  onChange={() => setBuMode('excel')}
                />
                Excel 模板公式 BU（步骤 a → Excel 刷新 → 步骤 c）
              </label>
            </div>
            <div className="flex flex-wrap gap-2">
              {buMode === 'simulated' ? (
                <button
                  type="button"
                  disabled={triggerMut.isLoading || status === 'running' || groupFiles.length === 0}
                  onClick={() => fireRun('quickbi', { action: 'simulated' })}
                  className="inline-flex items-center gap-1 rounded-md bg-green-500 px-2 py-1 text-[11px] font-medium text-white disabled:opacity-40"
                >
                  <RotateCcw className="h-3 w-3" />
                  执行 QuickBI（模拟公式）
                </button>
              ) : (
                <>
                  <button
                    type="button"
                    disabled={triggerMut.isLoading || status === 'running' || groupFiles.length === 0}
                    onClick={() => fireRun('quickbi', { action: 'excel_step_a' })}
                    className="rounded-md bg-green-500/90 px-2 py-1 text-[11px] font-medium text-white disabled:opacity-40"
                  >
                    步骤 a — 生成明细（不含 BU）
                  </button>
                  <div className="flex flex-wrap items-center gap-2">
                    <input
                      value={excelDetailName}
                      onChange={(e) => setExcelDetailName(e.target.value)}
                      placeholder="步骤 c：刷新后的文件名（可选）"
                      className="min-w-[12rem] rounded border border-border-light bg-surface-primary px-2 py-1 text-[11px]"
                    />
                    <button
                      type="button"
                      disabled={triggerMut.isLoading || status === 'running'}
                      onClick={() =>
                        fireRun('quickbi', {
                          action: 'excel_step_c',
                          detail_workbook_name: excelDetailName.trim() || undefined,
                        })
                      }
                      className="rounded-md bg-green-600 px-2 py-1 text-[11px] font-medium text-white disabled:opacity-40"
                    >
                      步骤 c — 生成汇总表（检验最终 BU）
                    </button>
                  </div>
                </>
              )}
            </div>
            {buMode === 'excel' && (
              <p className="text-[10px] leading-relaxed text-text-tertiary">
                模板公式路径：须严格「合并生成明细 → Excel 打开替换缓存 → 生成 QuickBI 汇总中间表」；若最终 BU
                未刷新，步骤 c 将报错（与 allline 一致）。
              </p>
            )}
          </div>
        )}

        {section === 'citihk' && (
          <div className="mt-3 flex flex-wrap items-center gap-2 rounded-lg border border-border-light bg-surface-secondary/20 px-3 py-2">
            <button
              type="button"
              disabled={triggerMut.isLoading || status === 'running' || groupFiles.length === 0}
              onClick={() => fireRun('citihk', { action: 'build' })}
              className="inline-flex items-center gap-1 rounded-md bg-green-500 px-2 py-1 text-[11px] font-medium text-white disabled:opacity-40"
            >
              <RotateCcw className="h-3 w-3" />
              执行 CitiHK 构建
            </button>
          </div>
        )}

        {section === 'merge' && (
          <div className="mt-3 space-y-2 rounded-lg border border-border-light bg-surface-secondary/20 p-3">
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                disabled={mergeBlocked}
                title={
                  mergeBlocked
                    ? '上一段执行尚未结束'
                    : '按任务状态中可用的产出路径合并；仅一侧有文件时生成单侧汇总并在校验中告警'
                }
                onClick={() => fireRun('merge', { output_filename: '收付款基数_合并_out.xlsx' })}
                className="inline-flex items-center gap-1 rounded-md bg-green-600 px-3 py-1.5 text-xs font-medium text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                <RotateCcw className="h-3.5 w-3.5" />
                执行合并
              </button>

              {/* 直接上传分摊基数合并结果表（跳过 QuickBI / CitiHK 流程） */}
              <input
                ref={mergeBaseInputRef}
                type="file"
                accept=".xlsx"
                className="hidden"
                aria-label="上传分摊基数合并结果表"
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  e.target.value = '';
                  if (f) uploadMergeBaseMut.mutate(f);
                }}
              />
              <button
                type="button"
                disabled={uploadMergeBaseMut.isLoading}
                onClick={() => mergeBaseInputRef.current?.click()}
                title="直接上传预先计算好的基数合并结果表（收付款基数_合并_out.xlsx / 分摊基数表.xlsx），用于成本出摊，跳过 QuickBI / CitiHK 流程"
                className="inline-flex items-center gap-1 rounded-md border border-green-500/50 bg-green-500/10 px-3 py-1.5 text-xs font-medium text-green-400 hover:bg-green-500/15 disabled:opacity-40"
              >
                <FolderUp className="h-3.5 w-3.5" />
                {uploadMergeBaseMut.isLoading ? '上传中…' : '上传基数合并表'}
              </button>
            </div>
            {uploadMergeBaseMut.data && (
              <div className="text-[11px] text-green-500/90">
                已上传：{uploadMergeBaseMut.data.name}（{(uploadMergeBaseMut.data.size / 1024).toFixed(1)} KB），将作为成本出摊 TEMPLATE_PATH 使用
              </div>
            )}
            {uploadMergeBaseMut.error && (
              <div className="rounded-md border border-red-500/40 bg-red-500/10 px-2 py-1 text-[11px] leading-snug text-red-200">
                {(uploadMergeBaseMut.error as Error).message}
              </div>
            )}
            {allocationMeta.merge_output_is_upload && allocationMeta.merge_output_name && (
              <div className="text-[11px] text-text-secondary">
                当前基数表：
                <span className="font-medium text-text-primary">
                  {String(allocationMeta.merge_output_name)}
                </span>
                {allocationMeta.merge_output_uploaded_at && (
                  <span className="ml-1 text-text-tertiary">
                    · 上传于 {new Date(String(allocationMeta.merge_output_uploaded_at)).toLocaleString('zh-CN', { hour12: false })}
                  </span>
                )}
              </div>
            )}
          </div>
        )}

        {(triggerMut.error || confirmMut.error || uploadZipReplace.error) && (
          <div className="mt-2 text-xs text-red-400">
            {((triggerMut.error ?? confirmMut.error ?? uploadZipReplace.error) as Error).message}
          </div>
        )}
        {uploadZipReplace.data && (
          <div className="mt-1 text-[11px] text-green-500/90">
            已上传 · {uploadZipReplace.data.file_count} 个文件已写入渠道目录
          </div>
        )}

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
              files={filesForSection}
            />
          )}
          {tab === 'runs' && (
            <ChannelRunsTab taskId={taskId} channelId={channelId} runs={runsForSection} />
          )}
          {tab === 'verify' && (
            <ChannelVerifyTab taskId={taskId} channelId={channelId} run={lastRun} />
          )}
          {tab === 'logs' && <ChannelLogTab taskId={taskId} channelId={channelId} />}
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
