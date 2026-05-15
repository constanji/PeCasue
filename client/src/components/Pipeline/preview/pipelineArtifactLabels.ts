/**
 * Human-readable titles for pipeline run artifacts (UI only; paths unchanged).
 * Matching uses basename stem first so localized CSV/XLSX share one label.
 */
const STEM_LABELS: Record<string, string> = {
  bill_summary: '账单合并最终结果',
  own_bank_statement_matched: '自有流水最终结果',
  customer_canonical: '客资规范中间表',
  customer_flow_output: '客资流水输出（规则明细）',
  cn_jp_canonical: '中日渠道规范中间表',
  final_summary: '汇总最终结果',
  summary_long: '汇总明细（长表）',
  allocation_base: '成本分摊基数',
  manifest: '产物清单',
  legacy_stdout: '运行日志',
  special_transfer_canonical: '特殊来源 · 内转规范表',
  special_transfer: '特殊来源 · 内转与 ACH return（合并工作簿）',
  /** 与磁盘文件名 stem 一致（含中文后缀） */
  special_transfer_内转: '特殊来源 · 内转（CSV 明细）',
  special_transfer_ach_return: '特殊来源 · ACH return（CSV 明细）',
  special_ach_refund_canonical: '特殊来源 · ACH return 规范表',
  special_ach_refund: '特殊来源 · ACH return（单一渠道工作簿）',
  special_op_incoming_canonical: '特殊来源 · OP 入账规范表',
  special_op_incoming: '特殊来源 · OP 入账（工作簿与明细）',
  special_op_refund_canonical: '特殊来源 · OP 退款规范表',
  special_op_refund: '特殊来源 · OP 退票（工作簿）',
  内转_ACH_OP合并结果: '特殊来源 · 内转 · ACH · OP 入账（合并最终结果）',
  收付款基数_QuickBI_三表汇总: 'QuickBI 三表汇总（轻量 · 仅汇总页）',
};

/** Backend ``FileEntry.role`` → short Chinese tag for subtitle/tooltips */
const ROLE_LABELS: Record<string, string> = {
  output: '最终产出',
  midfile: '中间表',
  manifest: '清单',
  log: '日志',
};

function stemFromFilename(filename: string): string {
  const base = filename.split(/[/\\]/).pop() ?? filename;
  return base.replace(/\.[^.]+$/, '');
}

/** Filename stem → Chinese title when known; ``*_temp`` midfiles →「前缀 + 临时表」 */
export function pipelineArtifactDisplayTitle(filename: string): string {
  const base = filename.split(/[/\\]/).pop() ?? filename;
  const stem = stemFromFilename(filename);
  const labeled = STEM_LABELS[stem];
  if (labeled) return labeled;
  if (stem.endsWith('_temp')) {
    const prefix = stem.slice(0, -'_temp'.length);
    return `${prefix} 临时表`;
  }
  return base;
}

/** Secondary line for tooltip / subtitle (technical filename). */
export function pipelineArtifactTechnicalName(filename: string): string {
  return filename.split(/[/\\]/).pop() ?? filename;
}

/** Role tag shown next to filename (defaults to 最终产出 when omitted). */
export function pipelineArtifactRoleLabel(role?: string | null): string {
  const key = (role ?? 'output').trim();
  return ROLE_LABELS[key] ?? key;
}
