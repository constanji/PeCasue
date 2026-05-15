import React, { useMemo } from 'react';
import {
  ArrowLeftRight,
  ChevronRight,
  CircleHelp,
  FileText,
  GitMerge,
  Globe2,
  PieChart,
  Users,
  Wallet2,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { usePipelineClassification, type PipelineClassificationGroup } from '~/data-provider';
import { cn } from '~/utils';
import {
  PIPELINE_FINAL_MERGE_CHANNEL_ID,
  PIPELINE_OVERVIEW_MAIN_CHANNELS,
  PIPELINE_SPECIAL_BUNDLE_IDS,
  PIPELINE_SPECIAL_BUNDLE_SIDEBAR,
  normalizePipelineToken,
  pickSpecialBundleDetailChannel,
} from '~/components/Pipeline/overviewChannels';

type SidebarPhase =
  | 'pending_run'
  | 'pending_confirm'
  | 'warning'
  | 'confirmed'
  | 'running'
  | 'failed'
  | 'skipped';

/** 业务语义：待运行/待确认为灰，有警告黄，已确认绿；执行中/失败也用中性灰底区分文案 */
function deriveSidebarPhase(statusRaw: string, runsCount: number): SidebarPhase {
  const key = normalizePipelineToken(statusRaw);

  if (key === 'confirmed') return 'confirmed';
  if (key === 'verified_with_warning') return 'warning';
  if (key === 'running') return 'running';
  if (key === 'failed') return 'failed';
  if (key === 'skipped') return 'skipped';

  if (['verified', 'preview_ready', 'edited', 'replaced'].includes(key)) {
    return 'pending_confirm';
  }

  if (runsCount === 0) return 'pending_run';
  return 'pending_confirm';
}

function bundleSidebarPhase(
  classification: Record<string, PipelineClassificationGroup | undefined>,
): SidebarPhase {
  if (
    normalizePipelineToken(classification.special_merge?.status ?? '') === 'confirmed'
  ) {
    return 'confirmed';
  }

  const activeIds = PIPELINE_SPECIAL_BUNDLE_IDS.filter(
    (id) => (classification[id]?.files?.length ?? 0) > 0,
  );

  if (activeIds.length === 0) {
    return 'pending_run';
  }

  const norm = (id: string) =>
    normalizePipelineToken(classification[id]?.status ?? 'pending');

  const norms = activeIds.map(norm);

  if (norms.some((k) => k === 'running')) return 'running';
  if (norms.some((k) => k === 'failed')) return 'failed';
  if (norms.every((k) => k === 'confirmed')) return 'confirmed';
  if (norms.some((k) => k === 'verified_with_warning')) return 'warning';
  if (norms.some((k) => ['verified', 'preview_ready', 'edited', 'replaced'].includes(k))) {
    return 'pending_confirm';
  }

  const everyNoRuns = activeIds.every((id) => (classification[id]?.runs_count ?? 0) === 0);
  if (everyNoRuns) return 'pending_run';

  return 'pending_confirm';
}

function phasePresentation(phase: SidebarPhase): {
  label: string;
  dot: string;
  badge: string;
} {
  const greyBadge =
    'border-border-medium bg-surface-secondary text-text-secondary';
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
      return {
        label: '执行中',
        dot: 'bg-text-secondary/55',
        badge: greyBadge,
      };
    case 'failed':
      return {
        label: '失败',
        dot: greyDot,
        badge: greyBadge,
      };
    case 'skipped':
      return {
        label: '已跳过',
        dot: greyDot,
        badge: greyBadge,
      };
    case 'pending_confirm':
      return {
        label: '待确认',
        dot: greyDot,
        badge: greyBadge,
      };
    case 'pending_run':
    default:
      return {
        label: '待运行',
        dot: greyDot,
        badge: greyBadge,
      };
  }
}

const CHANNEL_ICON: Record<string, LucideIcon> = {
  bill: FileText,
  own_flow: Wallet2,
  customer: Users,
  allocation_base: PieChart,
  cn_jp: Globe2,
};

const iconShellClass =
  'flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-border-light bg-surface-secondary text-text-secondary';

export default function ChannelSidebar({
  taskId,
  selectedChannelId,
  onSelect,
  omitChannelIds = [],
}: {
  taskId: string;
  selectedChannelId: string;
  onSelect: (channelId: string) => void;
  /** 从主线列表中隐藏的渠道（例如分摊基数改由「最终分摊」进入） */
  omitChannelIds?: readonly string[];
}) {
  const cls = usePipelineClassification(taskId);
  const classification = cls.data?.channels ?? {};

  const mainChannelRows = useMemo(
    () =>
      PIPELINE_OVERVIEW_MAIN_CHANNELS.filter((row) => !omitChannelIds.includes(row.channel_id)),
    [omitChannelIds],
  );

  const bundleNavigateTarget = pickSpecialBundleDetailChannel(classification);
  const bundleFiles = PIPELINE_SPECIAL_BUNDLE_IDS.reduce(
    (n, cid) => n + (classification[cid]?.files?.length ?? 0),
    0,
  );
  const bundlePhase = bundleSidebarPhase(classification);
  const bundleVis = phasePresentation(bundlePhase);
  const bundleSelected =
    PIPELINE_SPECIAL_BUNDLE_IDS.some((cid) => cid === selectedChannelId) ||
    selectedChannelId === 'special_merge';

  const finalMergeSelected = selectedChannelId === PIPELINE_FINAL_MERGE_CHANNEL_ID;

  const rows = mainChannelRows.length + 2;

  return (
    <aside className="flex h-full w-[272px] shrink-0 flex-col overflow-hidden border-r border-border-light bg-surface-secondary/40">
      <div className="flex-none border-b border-border-light px-3 py-3">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-semibold text-text-primary">渠道列表</h3>
              <span
                className="inline-flex text-text-secondary hover:text-text-primary"
                title="与「总览」矩阵主线一致：五条主线目录 + Ach return / 内转 合并入口 + 最终合并。"
              >
                <CircleHelp className="h-3.5 w-3.5" aria-hidden />
              </span>
            </div>
            <p className="mt-1 text-[11px] text-text-secondary">共 {rows} 个主线入口</p>
          </div>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-2 py-2">
        {cls.isLoading && (
          <div className="px-2 py-6 text-center text-xs text-text-secondary">加载中…</div>
        )}
        {cls.error && (
          <div className="rounded-lg border border-red-500/35 bg-red-500/10 px-2 py-2 text-[11px] text-red-300">
            {cls.error.message}
          </div>
        )}
        {!cls.isLoading && !cls.error && (
          <div className="space-y-1.5">
            {mainChannelRows.map((row) => {
              const files = classification[row.channel_id]?.files?.length ?? 0;
              const st = classification[row.channel_id]?.status ?? 'pending';
              const runsCount = classification[row.channel_id]?.runs_count ?? 0;
              const phase = deriveSidebarPhase(st, runsCount);
              const vis = phasePresentation(phase);
              const Icon = CHANNEL_ICON[row.channel_id] ?? FileText;
              const sel = selectedChannelId === row.channel_id;
              return (
                <button
                  key={row.channel_id}
                  type="button"
                  onClick={() => onSelect(row.channel_id)}
                  className={cn(
                    'flex w-full items-center gap-2 rounded-xl border px-2.5 py-2 text-left transition-colors',
                    sel
                      ? 'border-blue-500/55 bg-blue-500/10 ring-1 ring-blue-500/25'
                      : 'border-border-light bg-surface-primary hover:bg-surface-hover/90',
                  )}
                >
                  <div className={iconShellClass}>
                    <Icon className="h-4 w-4" aria-hidden />
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-1.5">
                      <span
                        className={cn(
                          'inline-block h-2 w-2 shrink-0 rounded-full',
                          vis.dot,
                        )}
                      />
                      <span className="truncate text-sm font-medium text-text-primary">
                        {row.display_name}
                      </span>
                    </div>
                    <div className="mt-0.5 flex items-center gap-2 text-[11px] text-text-secondary">
                      <span>
                        文件 <span className="tabular-nums text-text-primary">{files}</span> 个
                      </span>
                    </div>
                  </div>
                  <div className="flex shrink-0 flex-col items-end gap-1">
                    <span
                      className={cn(
                        'rounded-md border px-1.5 py-0.5 text-[10px] font-medium',
                        vis.badge,
                      )}
                    >
                      {vis.label}
                    </span>
                    <ChevronRight className="h-4 w-4 text-text-secondary" aria-hidden />
                  </div>
                </button>
              );
            })}

            <button
              type="button"
              onClick={() => onSelect(bundleNavigateTarget)}
              className={cn(
                'flex w-full items-center gap-2 rounded-xl border px-2.5 py-2 text-left transition-colors',
                bundleSelected
                  ? 'border-blue-500/55 bg-blue-500/10 ring-1 ring-blue-500/25'
                  : 'border-border-light bg-surface-primary hover:bg-surface-hover/90',
              )}
            >
              <div className={iconShellClass}>
                <ArrowLeftRight className="h-4 w-4" aria-hidden />
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-1.5">
                  <span
                    className={cn(
                      'inline-block h-2 w-2 shrink-0 rounded-full',
                      bundleVis.dot,
                    )}
                  />
                  <span className="truncate text-sm font-medium text-text-primary">
                    {PIPELINE_SPECIAL_BUNDLE_SIDEBAR.label}
                  </span>
                </div>
                <div className="mt-0.5 flex items-center gap-2 text-[11px] text-text-secondary">
                  <span>
                    文件 <span className="tabular-nums text-text-primary">{bundleFiles}</span> 个
                  </span>
                  <span className="truncate opacity-90">{PIPELINE_SPECIAL_BUNDLE_SIDEBAR.subtitle}</span>
                </div>
              </div>
              <div className="flex shrink-0 flex-col items-end gap-1">
                <span
                  className={cn(
                    'rounded-md border px-1.5 py-0.5 text-[10px] font-medium',
                    bundleVis.badge,
                  )}
                >
                  {bundleVis.label}
                </span>
                <ChevronRight className="h-4 w-4 text-text-secondary" aria-hidden />
              </div>
            </button>

            {/* 第六步：最终合并 */}
            <button
              type="button"
              onClick={() => onSelect(PIPELINE_FINAL_MERGE_CHANNEL_ID)}
              className={cn(
                'flex w-full items-center gap-2 rounded-xl border px-2.5 py-2 text-left transition-colors',
                finalMergeSelected
                  ? 'border-green-500/65 bg-green-500/12 ring-1 ring-green-500/30'
                  : 'border-green-500/25 bg-green-500/5 hover:border-green-500/45 hover:bg-green-500/10',
              )}
              title="查看各渠道最近一次运行产出汇总，执行最终成本汇总"
            >
              <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-green-500/35 bg-green-500/15 text-green-400">
                <GitMerge className="h-4 w-4" aria-hidden />
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-1.5">
                  <span className="truncate text-sm font-medium text-green-400">最终合并</span>
                </div>
                <div className="mt-0.5 text-[11px] text-text-secondary">汇总全渠道产出</div>
              </div>
              <ChevronRight className="h-4 w-4 shrink-0 text-green-400/70" aria-hidden />
            </button>
          </div>
        )}
      </div>
    </aside>
  );
}
