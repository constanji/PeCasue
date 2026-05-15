import type { PipelineClassificationGroup } from '~/data-provider';

/** 总览矩阵中的主线渠道（与总览卡片一致，不含「汇总」）。渠道详情左侧栏同步使用此列表。 */
export const PIPELINE_OVERVIEW_MAIN_CHANNELS = [
  {
    rowId: 'bill',
    channel_id: 'bill',
    display_name: '账单',
    hint: '目录名含「账单」',
  },
  {
    rowId: 'own_flow',
    channel_id: 'own_flow',
    display_name: '自有流水',
    hint: '目录名含「自有」',
  },
  {
    rowId: 'customer',
    channel_id: 'customer',
    display_name: '客资流水',
    hint: '目录名含「客资」',
  },
  {
    rowId: 'allocation_base',
    channel_id: 'allocation_base',
    display_name: '分摊基数',
    hint: '目录名含「分摊」或「基数」',
  },
  {
    rowId: 'cn_jp',
    channel_id: 'cn_jp',
    display_name: '境内 & 日本通道',
    hint: '目录名含 境内 / 国内 / 日本 / JP',
  },
] as const;

export type PipelineOverviewMainChannelRow = (typeof PIPELINE_OVERVIEW_MAIN_CHANNELS)[number];

/** 总览「渠道矩阵」卡片列表（不含「分摊基数」——入口在「最终分摊」） */
export const PIPELINE_OVERVIEW_MATRIX_CHANNELS: PipelineOverviewMainChannelRow[] =
  PIPELINE_OVERVIEW_MAIN_CHANNELS.filter((row) => row.channel_id !== 'allocation_base');

export const PIPELINE_SPECIAL_BUNDLE_IDS = [
  'special_transfer',
  'special_ach_refund',
  'special_op_incoming',
  'special_op_refund',
] as const;

export type PipelineSpecialBundleId = (typeof PIPELINE_SPECIAL_BUNDLE_IDS)[number];

export const PIPELINE_SPECIAL_BUNDLE_LABELS: Record<PipelineSpecialBundleId, string> = {
  special_transfer: '内转 / Ach return',
  special_ach_refund: 'ACH / return',
  special_op_incoming: 'OP 入账',
  special_op_refund: 'OP 退票',
};

/** 总览「Ach return · 内转」合并卡在侧边栏中的一条（跳转至首个有文件的 special_* 详情）。 */
export const PIPELINE_SPECIAL_BUNDLE_SIDEBAR = {
  rowId: 'special_bundle',
  label: 'Ach return · 内转',
  subtitle: '内转+ACH · OP入账 · 合并',
} as const;

export const PIPELINE_SPECIAL_DETAIL_ROUTE_IDS = [
  'special_transfer',
  'special_ach_refund',
  'special_op_incoming',
  'special_merge',
] as const;

/** 虚拟渠道：最终合并汇总页（无独立跑批，仅展示各渠道最新产物） */
export const PIPELINE_FINAL_MERGE_CHANNEL_ID = 'final_merge' as const;

export function isFinalMergeDetailRoute(channelId: string): boolean {
  return channelId === PIPELINE_FINAL_MERGE_CHANNEL_ID;
}

/** 是否为「Ach return · 内转」壳页（两处理分区 + 合并），不含 OP 退票独立扁平页 */
export function isSpecialBundleDetailRoute(channelId: string): boolean {
  return (PIPELINE_SPECIAL_DETAIL_ROUTE_IDS as readonly string[]).includes(channelId);
}

export type PipelineSpecialDetailRouteId = (typeof PIPELINE_SPECIAL_DETAIL_ROUTE_IDS)[number];

export function normalizePipelineToken(raw: string): string {
  const t = (raw || '').trim();
  const ts = /^TaskStatus\.(.+)$/i.exec(t);
  if (ts) return ts[1].toLowerCase();
  const ps = /^PipelineStep\.(.+)$/i.exec(t);
  if (ps) return ps[1].toLowerCase();
  return t.toLowerCase();
}

export function aggregateStatusForChannels(
  channelIds: string[],
  classification: Record<string, PipelineClassificationGroup | undefined>,
): string {
  const mergeNorm = normalizePipelineToken(classification.special_merge?.status ?? '');
  const isSpecialBundleRollup =
    channelIds.length === PIPELINE_SPECIAL_BUNDLE_IDS.length &&
    PIPELINE_SPECIAL_BUNDLE_IDS.every((id) => channelIds.includes(id));

  if (mergeNorm === 'confirmed' && isSpecialBundleRollup) {
    return 'confirmed';
  }

  const norms = channelIds.map((id) =>
    normalizePipelineToken(classification[id]?.status ?? 'pending'),
  );
  if (norms.some((k) => k === 'running')) return 'running';
  if (norms.some((k) => k === 'failed')) return 'failed';
  if (norms.length > 0 && norms.every((k) => k === 'confirmed')) return 'confirmed';
  if (norms.some((k) => k === 'verified_with_warning')) return 'verified_with_warning';
  if (
    norms.some((k) =>
      ['verified', 'preview_ready', 'edited', 'replaced'].includes(k),
    )
  ) {
    return 'verified';
  }
  return 'pending';
}

export function pickSpecialBundleDetailChannel(
  classification: Record<string, PipelineClassificationGroup | undefined>,
): string {
  return (
    PIPELINE_SPECIAL_BUNDLE_IDS.find((cid) => (classification[cid]?.files?.length ?? 0) > 0) ??
    'special_transfer'
  );
}
