import React, { useEffect, useRef, useState } from 'react';
import { FolderUp, History, RefreshCw, RotateCcw, CheckCircle2, X } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import {
  PipelineApi,
  PIPELINE_QUERY_KEYS,
  useImportCustomerFlowTemplate,
  useImportOwnFlowTemplate,
  useImportRuleFx,
  useRollbackRule,
  useRule,
  useRuleVersions,
  usePutRule,
  type PipelineRuleExcelImportConfig,
  type PipelineRuleKind,
  type PipelineRuleTable,
} from '~/data-provider';
import RuleTableEditor from './RuleTableEditor';

function parseFxMonthLabelToInput(label: string | null | undefined): string {
  if (!label) return '';
  const zh = /^(20\d{2})年(\d{1,2})月$/.exec(label);
  if (zh) return `${zh[1]}-${zh[2].padStart(2, '0')}`;
  const iso = /^(20\d{2})[-/](\d{1,2})$/.exec(label);
  if (iso) return `${iso[1]}-${iso[2].padStart(2, '0')}`;
  return '';
}

function formatInputMonthToZh(ym: string): string {
  const [y, m] = ym.split('-');
  if (!y || !m) return ym;
  return `${y}年${parseInt(m, 10)}月`;
}

interface RulePaneProps {
  kind: PipelineRuleKind;
  bannerNote?: React.ReactNode;
  passwordColumns?: string[];
  readOnlyColumns?: string[];
  hideColumnTools?: boolean;
  /** Browse-only: hide toolbar, mutation controls, version sidebar */
  viewOnly?: boolean;
  /** Excel 导入：按 variant + scope 仅写入对应规则快照（见后端 Form scope） */
  ruleExcelImport?: PipelineRuleExcelImportConfig;
}

interface RuleExcelUploadBarProps {
  config: PipelineRuleExcelImportConfig;
  fxMonthDraft?: string;
  onFxMonthDraftChange?: (v: string) => void;
  onSaveFxMonthMeta?: () => void | Promise<void>;
  saveFxMonthBusy?: boolean;
  /** 汇率表上传成功后回调（父级可展示「已替换汇率」等） */
  onFxImported?: () => void;
}

function RuleExcelUploadBar({
  config,
  fxMonthDraft = '',
  onFxMonthDraftChange,
  onSaveFxMonthMeta,
  saveFxMonthBusy,
  onFxImported,
}: RuleExcelUploadBarProps) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const fxMut = useImportRuleFx();
  const ownMut = useImportOwnFlowTemplate();
  const custMut = useImportCustomerFlowTemplate();

  const busy =
    config.variant === 'fx'
      ? fxMut.isLoading
      : config.variant === 'own_flow_template'
        ? ownMut.isLoading
        : custMut.isLoading;

  const err =
    config.variant === 'fx'
      ? fxMut.error
      : config.variant === 'own_flow_template'
        ? ownMut.error
        : custMut.error;

  const pick = async (file: File | null | undefined) => {
    if (!file) return;
    try {
      if (config.variant === 'fx') {
        await fxMut.mutateAsync({
          file,
          fxMonthLabel: fxMonthDraft.trim() || undefined,
        });
        onFxImported?.();
      } else if (config.variant === 'own_flow_template') {
        await ownMut.mutateAsync({
          file,
          scope: config.scope,
          fxMonthLabel:
            config.scope === 'fx' ? fxMonthDraft.trim() || undefined : undefined,
        });
      } else {
        await custMut.mutateAsync({ file, scope: config.scope });
      }
    } finally {
      if (inputRef.current) inputRef.current.value = '';
    }
  };

  const okFx = config.variant === 'fx' && fxMut.isSuccess && fxMut.data;
  const okOwn = config.variant === 'own_flow_template' && ownMut.isSuccess && ownMut.data;
  const okCust = config.variant === 'customer_flow_template' && custMut.isSuccess && custMut.data;

  let hint = '';
  let btn = '上传 (.xlsx)';
  let aria = '上传 Excel';
  if (config.variant === 'fx') {
    btn = '上传汇率表 (.xlsx)';
    aria = '上传汇率 Excel';
    hint =
      '请选择所属月份（或沿用当前已保存月份）；解析「货币代码 / 对美元折算率」等列并写入规则与 CSV（data/rules/files/fx/）。';
  } else if (config.variant === 'own_flow_template') {
    btn = '上传模版工作簿 (.xlsx)';
    aria = '上传自有流水模版';
    hint =
      config.scope === 'account_mapping'
        ? '仅解析并覆盖「账户对应主体分行mapping表」→ RuleStore · account_mapping + mapping/*.csv。'
        : config.scope === 'fee_mapping'
          ? '仅解析并覆盖「账单及自有流水费项mapping表」→ fee_mapping。'
          : config.scope === 'fx'
            ? '仅从模版解析汇率表 → fx（若无汇率表则可能跳过）。'
            : config.scope === 'own_flow_processing'
              ? '仅解析「处理表 / 自有流水处理表」→ own_flow_processing + rules/处理表.csv。'
              : '未指定 scope（不应出现）。';
  } else {
    btn = '上传客资模版 (.xlsx)';
    aria = '上传客资流水模版';
    hint =
      config.scope === 'customer_mapping'
        ? '仅写入工作表「客资流水MAPPING」→ customer_mapping。'
        : config.scope === 'customer_fee_mapping'
          ? '仅写入「客资流水费项mapping表」→ customer_fee_mapping。'
          : config.scope === 'customer_branch_mapping'
            ? '仅写入「客资流水分行mapping」→ customer_branch_mapping。'
            : '未指定 scope（不应出现）。';
  }

  return (
    <div className="rounded-xl border border-green-500/25 bg-surface-primary px-3 py-2.5 text-xs shadow-[0_1px_0_0_rgba(74,222,128,0.12)] ring-1 ring-inset ring-green-500/15">
      <div className="flex flex-wrap items-center gap-2">
        <input
          ref={inputRef}
          type="file"
          accept=".xlsx,.xlsm"
          className="hidden"
          aria-label={aria}
          onChange={(e) => pick(e.target.files?.[0])}
        />
        <button
          type="button"
          disabled={busy}
          onClick={() => inputRef.current?.click()}
          className="inline-flex items-center gap-1.5 rounded-md border border-green-500/50 bg-green-500/10 px-3 py-1.5 font-medium text-green-500 hover:bg-green-500/15 disabled:opacity-50"
        >
          <FolderUp className="h-3.5 w-3.5" />
          {btn}
        </button>
        <span className="text-text-secondary">{hint}</span>
      </div>
      {config.variant === 'fx' && onFxMonthDraftChange ? (
        <div className="mt-2 flex flex-wrap items-center gap-2 border-t border-green-500/20 pt-2">
          <span className="shrink-0 text-text-secondary">所属月份</span>
          <input
            type="month"
            value={fxMonthDraft}
            onChange={(e) => onFxMonthDraftChange(e.target.value)}
            className="rounded-md border border-border-medium bg-surface-secondary px-2 py-1 text-text-primary"
            aria-label="汇率所属月份"
          />
          {onSaveFxMonthMeta ? (
            <button
              type="button"
              disabled={saveFxMonthBusy || !fxMonthDraft.trim()}
              onClick={() => void onSaveFxMonthMeta()}
              className="rounded-md border border-green-500/45 bg-green-500/10 px-2 py-1 font-medium text-green-500 hover:bg-green-500/15 disabled:opacity-45"
            >
              保存月份
            </button>
          ) : null}
          <span className="text-[11px] text-text-secondary">
            上传时不选月份则沿用规则内已有「所属月份」。
          </span>
        </div>
      ) : null}
      {busy && <div className="mt-2 text-text-secondary">导入中…</div>}
      {err && <div className="mt-2 text-text-primary">{(err as Error).message}</div>}
      {okFx && (
        <div className="mt-2 text-text-secondary">
          已导入 {okFx.rows} 条汇率 · CSV {okFx.csv_relative}
        </div>
      )}
      {okOwn && (
        <div className="mt-2 space-y-1 text-text-secondary">
          <div className="font-mono text-[10px] text-green-500/90">scope={okOwn.scope ?? '—'}</div>
          <div>
            {okOwn.row_counts.account_mapping != null && (
              <span>账户 {okOwn.row_counts.account_mapping} 行 · </span>
            )}
            {okOwn.row_counts.fee_mapping != null && (
              <span>费项 {okOwn.row_counts.fee_mapping} 行 · </span>
            )}
            {okOwn.row_counts.own_flow_processing != null && (
              <span>处理表 {okOwn.row_counts.own_flow_processing} 行 · </span>
            )}
            {okOwn.row_counts.fx != null && <span>汇率 {okOwn.row_counts.fx} 行</span>}
            {okOwn.fx?.skipped ? (
              <span className="text-amber-400">
                {' '}
                （汇率未写入：{okOwn.fx.reason ?? '跳过'}）
              </span>
            ) : null}
          </div>
        </div>
      )}
      {okCust && (
        <div className="mt-2 space-y-1 text-text-secondary">
          <div className="font-mono text-[10px] text-green-500/90">scope={okCust.scope ?? '—'}</div>
          <div>
            {okCust.row_counts.customer_mapping != null && (
              <span>客资 MAPPING {okCust.row_counts.customer_mapping} 行 · </span>
            )}
            {okCust.row_counts.customer_fee_mapping != null && (
              <span>客资费项 {okCust.row_counts.customer_fee_mapping} 行 · </span>
            )}
            {okCust.row_counts.customer_branch_mapping != null && (
              <span>客资分行 {okCust.row_counts.customer_branch_mapping} 行</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default function RulePane({
  kind,
  bannerNote,
  passwordColumns,
  readOnlyColumns,
  hideColumnTools = false,
  viewOnly = false,
  ruleExcelImport,
}: RulePaneProps) {
  const ruleQ = useRule(kind);
  const versionsQ = useRuleVersions(kind);
  const putMut = usePutRule(kind);
  const rollbackMut = useRollbackRule(kind);
  const qc = useQueryClient();
  const [historyOpen, setHistoryOpen] = useState(false);
  const [refreshBusy, setRefreshBusy] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [fxReplaceNotice, setFxReplaceNotice] = useState<string | null>(null);
  const fxNoticeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const drawerCloseRef = useRef<HTMLButtonElement | null>(null);

  const showFxReplaced = () => {
    if (kind !== 'fx') return;
    setFxReplaceNotice('已替换汇率');
    if (fxNoticeTimerRef.current) clearTimeout(fxNoticeTimerRef.current);
    fxNoticeTimerRef.current = setTimeout(() => {
      setFxReplaceNotice(null);
      fxNoticeTimerRef.current = null;
    }, 6000);
  };

  useEffect(() => {
    setFxReplaceNotice(null);
  }, [kind]);

  useEffect(
    () => () => {
      if (fxNoticeTimerRef.current) clearTimeout(fxNoticeTimerRef.current);
    },
    [],
  );

  const table =
    ruleQ.data?.table ?? ({ columns: [], rows: [] } satisfies PipelineRuleTable);
  const fxMonthLabel =
    kind === 'fx' &&
    table.meta &&
    typeof (table.meta as Record<string, unknown>).fx_month_label === 'string'
      ? String((table.meta as Record<string, unknown>).fx_month_label)
      : null;

  const [fxMonthDraft, setFxMonthDraft] = useState('');
  const fxSyncedKeyRef = useRef<string | null>(null);
  useEffect(() => {
    if (kind !== 'fx') return;
    const key = fxMonthLabel ?? '';
    if (key !== fxSyncedKeyRef.current) {
      fxSyncedKeyRef.current = key || null;
      setFxMonthDraft(parseFxMonthLabelToInput(key));
    }
  }, [kind, fxMonthLabel]);

  useEffect(() => {
    if (!historyOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setHistoryOpen(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [historyOpen]);

  useEffect(() => {
    if (historyOpen) drawerCloseRef.current?.focus();
  }, [historyOpen]);

  const handleSave = async (t: PipelineRuleTable, note?: string) => {
    await putMut.mutateAsync({ table: t, note });
    if (kind === 'fx') showFxReplaced();
  };

  const handleSaveFxMonthMeta = async () => {
    if (kind !== 'fx' || !fxMonthDraft.trim()) return;
    const t = ruleQ.data?.table ?? { columns: [], rows: [] };
    const zh = formatInputMonthToZh(fxMonthDraft.trim());
    const prevMeta =
      t.meta && typeof t.meta === 'object' && !Array.isArray(t.meta)
        ? (t.meta as Record<string, unknown>)
        : {};
    await putMut.mutateAsync({
      table: { ...t, meta: { ...prevMeta, fx_month_label: zh } },
      note: '所属月份',
    });
    showFxReplaced();
  };

  if (ruleQ.isLoading) {
    return <div className="p-4 text-sm text-text-secondary">加载规则…</div>;
  }
  if (ruleQ.error) {
    return <div className="p-4 text-sm text-text-primary">{(ruleQ.error as Error).message}</div>;
  }

  const versions = versionsQ.data?.versions ?? [];

  const handleRollback = async (target: number) => {
    if (!confirm(`确认回滚到 v${target}？当前版本将作为新版本保存为 rollback 副本。`)) return;
    await rollbackMut.mutateAsync({ targetVersion: target });
    if (kind === 'fx') showFxReplaced();
    setHistoryOpen(false);
  };

  const handleRefreshRuleAndFiles = async () => {
    setRefreshBusy(true);
    setRefreshError(null);
    try {
      if (kind !== 'password_book') {
        await PipelineApi.syncRuleSidecars(kind);
      }
      await qc.invalidateQueries(PIPELINE_QUERY_KEYS.rule(kind));
      await qc.invalidateQueries(PIPELINE_QUERY_KEYS.ruleVersions(kind));
      await qc.invalidateQueries(PIPELINE_QUERY_KEYS.rulesManifest);
      await Promise.all([ruleQ.refetch(), versionsQ.refetch()]);
      if (kind === 'fx') showFxReplaced();
    } catch (e) {
      setRefreshError((e as Error).message);
    } finally {
      setRefreshBusy(false);
    }
  };

  return (
    <div className="h-full overflow-auto p-4">
      {ruleExcelImport && (
        <div className="mb-4">
          <RuleExcelUploadBar
            config={ruleExcelImport}
            {...(kind === 'fx' && !viewOnly
              ? {
                  fxMonthDraft,
                  onFxMonthDraftChange: setFxMonthDraft,
                  onSaveFxMonthMeta: handleSaveFxMonthMeta,
                  saveFxMonthBusy: putMut.isLoading,
                  onFxImported: showFxReplaced,
                }
              : {})}
          />
        </div>
      )}
      {kind === 'fx' && fxReplaceNotice ? (
        <div
          className="mb-3 flex items-center justify-between gap-2 rounded-lg border border-green-500/45 bg-green-500/10 px-3 py-2 text-xs text-green-100"
          role="status"
        >
          <span className="flex items-center gap-2 font-medium">
            <CheckCircle2 className="h-4 w-4 shrink-0 text-green-400" aria-hidden />
            {fxReplaceNotice}
          </span>
          <button
            type="button"
            onClick={() => {
              setFxReplaceNotice(null);
              if (fxNoticeTimerRef.current) {
                clearTimeout(fxNoticeTimerRef.current);
                fxNoticeTimerRef.current = null;
              }
            }}
            className="shrink-0 rounded p-0.5 text-green-200/90 hover:bg-green-500/20 hover:text-white"
            aria-label="关闭提示"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      ) : null}
      {!viewOnly && (
        <>
          <div className="mb-3 flex justify-end gap-2">
            <button
              type="button"
              disabled={refreshBusy}
              onClick={() => void handleRefreshRuleAndFiles()}
              title="重新从服务器加载本页规则，并把当前版本同步到 data/rules/files（执行任务读取的 CSV/XLSX）"
              className="inline-flex items-center gap-1.5 rounded-lg border border-border-medium bg-surface-secondary px-3 py-2 text-xs font-medium text-text-secondary shadow-sm transition-colors hover:border-green-500/35 hover:bg-surface-hover hover:text-text-primary disabled:opacity-50"
            >
              <RefreshCw className={refreshBusy ? 'h-4 w-4 shrink-0 animate-spin' : 'h-4 w-4 shrink-0'} aria-hidden />
              刷新
            </button>
            <button
              type="button"
              onClick={() => setHistoryOpen(true)}
              className="inline-flex items-center gap-1.5 rounded-lg border border-border-medium bg-surface-secondary px-3 py-2 text-xs font-medium text-text-secondary shadow-sm transition-colors hover:border-green-500/35 hover:bg-surface-hover hover:text-text-primary"
            >
              <History className="h-4 w-4 shrink-0 text-green-500/90" aria-hidden />
              历史版本
              <span className="rounded-md bg-surface-primary px-1.5 py-0.5 font-mono text-[10px] text-text-secondary">
                {versions.length}
              </span>
            </button>
          </div>
          {refreshError ? (
            <div className="mb-2 rounded-md border border-red-500/35 bg-red-500/10 px-3 py-2 text-[11px] text-red-200">
              刷新失败：{refreshError}
            </div>
          ) : null}
          {historyOpen && (
            <>
              <button
                type="button"
                className="fixed inset-0 z-[60] bg-black/55 backdrop-blur-[1px]"
                aria-label="关闭历史版本面板"
                onClick={() => setHistoryOpen(false)}
              />
              <aside
                className="fixed inset-y-0 right-0 z-[70] flex w-full max-w-md flex-col border-l border-border-light bg-surface-primary shadow-[-12px_0_40px_-8px_rgba(0,0,0,0.45)]"
                role="dialog"
                aria-modal="true"
                aria-labelledby="pipeline-rule-history-title"
              >
                <div className="flex items-center justify-between border-b border-border-light px-4 py-3">
                  <div className="flex items-center gap-2">
                    <History className="h-4 w-4 text-green-500" aria-hidden />
                    <h2 id="pipeline-rule-history-title" className="text-sm font-semibold text-text-primary">
                      历史版本
                    </h2>
                  </div>
                  <button
                    ref={drawerCloseRef}
                    type="button"
                    onClick={() => setHistoryOpen(false)}
                    className="rounded-md border border-border-light p-1.5 text-text-secondary hover:bg-surface-hover hover:text-text-primary"
                    aria-label="关闭"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>
                <div className="min-h-0 flex-1 overflow-y-auto px-1 pb-4">
                  {versionsQ.data?.note ? (
                    <p className="px-3 py-2 text-[11px] text-text-secondary">{versionsQ.data.note}</p>
                  ) : null}
                  {versions.length === 0 ? (
                    <div className="px-4 py-6 text-center text-xs text-text-secondary">尚无历史版本</div>
                  ) : (
                    <ul className="divide-y divide-border-light text-xs">
                      {versions.map((v) => (
                        <li key={v.version} className="space-y-1.5 px-4 py-3">
                          <div className="flex items-center justify-between gap-2">
                            <span className="font-mono text-sm font-medium text-text-primary">v{v.version}</span>
                            <button
                              type="button"
                              onClick={() => handleRollback(v.version)}
                              className="inline-flex shrink-0 items-center gap-1 rounded-md border border-border-light px-2 py-1 text-text-secondary hover:bg-surface-hover"
                            >
                              <RotateCcw className="h-3 w-3" /> 回滚
                            </button>
                          </div>
                          <div className="text-[11px] text-text-secondary">
                            {new Date(v.created_at).toLocaleString()}
                            {v.author ? ` · ${v.author}` : ''}
                          </div>
                          <div className="flex flex-wrap gap-x-3 gap-y-1 text-[11px] leading-snug text-text-tertiary">
                            {v.rows_count != null && v.rows_count >= 0 ? (
                              <span>
                                <span className="text-text-secondary">行数</span> {v.rows_count}
                              </span>
                            ) : null}
                            {kind === 'fx' && v.fx_month_label ? (
                              <span>
                                <span className="text-text-secondary">所属月份</span> {v.fx_month_label}
                              </span>
                            ) : null}
                            {v.snapshot_basename ? (
                              <span className="font-mono text-[10px]" title={v.snapshot_path}>
                                快照 {v.snapshot_basename}
                              </span>
                            ) : null}
                          </div>
                          {v.note ? (
                            <div className="border-t border-border-light/80 pt-1.5 text-[11px] leading-snug text-text-secondary">
                              <span className="font-medium text-text-tertiary">备注：</span>
                              {v.note}
                            </div>
                          ) : null}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
                {rollbackMut.error && (
                  <div className="border-t border-border-light px-4 py-2 text-xs text-text-primary">
                    {(rollbackMut.error as Error).message}
                  </div>
                )}
              </aside>
            </>
          )}
        </>
      )}
      <RuleTableEditor
        table={table}
        passwordColumns={passwordColumns}
        readOnlyColumns={readOnlyColumns}
        hideColumnTools={hideColumnTools}
        viewOnly={viewOnly}
        onSave={handleSave}
        saving={putMut.isLoading}
        bannerNote={bannerNote}
        statsTrailing={
          fxMonthLabel ? (
            <>
              <span className="text-text-secondary">所属月份</span>{' '}
              <span className="font-medium tabular-nums text-text-primary">{fxMonthLabel}</span>
            </>
          ) : undefined
        }
      />
      {putMut.error && (
        <div className="mt-2 text-xs text-text-primary">保存失败：{(putMut.error as Error).message}</div>
      )}
    </div>
  );
}
