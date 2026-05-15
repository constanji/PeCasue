import React, { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useRecoilState } from 'recoil';
import { Loader2, Play, Scale } from 'lucide-react';
import {
  useCreateCompare,
  usePipelineCompares,
  type PipelineCompareMeta,
  type PipelineCompareSource,
} from '~/data-provider';
import { pipelineSelectedTaskIdAtom } from '~/store/pipeline';
import { cn } from '~/utils';
import EmptyTabPlaceholder from './EmptyTabPlaceholder';
import CompareSourcePicker from '../compare/CompareSourcePicker';
import CompareReportView from '../compare/CompareReportView';

export default function CompareTab() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [selectedTaskId, setSelectedTaskId] = useRecoilState(pipelineSelectedTaskIdAtom);
  const taskIdParam = searchParams.get('taskId');
  const taskId = taskIdParam || selectedTaskId;
  useEffect(() => {
    if (taskIdParam && taskIdParam !== selectedTaskId) setSelectedTaskId(taskIdParam);
  }, [taskIdParam, selectedTaskId, setSelectedTaskId]);

  const compareIdParam = searchParams.get('compareId');

  const [left, setLeft] = useState<PipelineCompareSource | null>(null);
  const [right, setRight] = useState<PipelineCompareSource | null>(null);
  const [keyCols, setKeyCols] = useState<string>('');
  const [compareCols, setCompareCols] = useState<string>('');
  const [numericTol, setNumericTol] = useState<string>('0.01');
  const [normalize, setNormalize] = useState<boolean>(true);

  const compares = usePipelineCompares(taskId);
  const createMut = useCreateCompare();

  const activeMeta: PipelineCompareMeta | null = useMemo(() => {
    if (!compareIdParam) return createMut.data ?? null;
    const list = compares.data?.compares ?? [];
    const summary = list.find((c) => c.compare_id === compareIdParam);
    if (!summary) return createMut.data ?? null;
    if (createMut.data?.compare_id === compareIdParam) return createMut.data;
    return null;
  }, [compareIdParam, compares.data, createMut.data]);

  if (!taskId) {
    return (
      <EmptyTabPlaceholder
        icon={<Scale className="h-10 w-10" aria-hidden="true" />}
        title="对比核对"
        description="请先在“总览”里选中一个任务，再回到这里发起对比。"
      />
    );
  }

  const canRun =
    !!left && !!right && keyCols.trim().length > 0 && !createMut.isLoading;

  const submit = async () => {
    if (!canRun || !left || !right) return;
    const meta = await createMut.mutateAsync({
      task_id: taskId,
      left,
      right,
      key_cols: keyCols.split(',').map((s) => s.trim()).filter(Boolean),
      compare_cols: compareCols
        ? compareCols.split(',').map((s) => s.trim()).filter(Boolean)
        : null,
      numeric_tol: Number(numericTol) || 0.01,
      normalize_strings: normalize,
    });
    const next = new URLSearchParams(searchParams);
    next.set('tab', 'compare');
    next.set('taskId', taskId);
    next.set('compareId', meta.compare_id);
    setSearchParams(next, { replace: false });
  };

  return (
    <div className="grid h-full grid-cols-12 gap-3 overflow-hidden p-3">
      {/* Left column — picker + config */}
      <div className="col-span-12 flex flex-col gap-3 overflow-auto md:col-span-5 lg:col-span-4">
        <CompareSourcePicker
          taskId={taskId}
          side="left"
          value={left}
          onChange={setLeft}
        />
        <CompareSourcePicker
          taskId={taskId}
          side="right"
          value={right}
          onChange={setRight}
        />
        <div className="space-y-2 rounded-lg border border-border-light bg-surface-primary p-3">
          <div className="text-xs font-semibold uppercase tracking-wide text-text-secondary">
            对比配置
          </div>
          <label className="block text-xs">
            <span className="text-text-secondary">主键列（逗号分隔）</span>
            <input
              value={keyCols}
              onChange={(e) => setKeyCols(e.target.value)}
              placeholder="如 账户号,日期,流水号"
              className="mt-1 w-full rounded-md border border-border-light bg-surface-primary px-2 py-1 font-mono"
              aria-label="主键列"
            />
          </label>
          <label className="block text-xs">
            <span className="text-text-secondary">对比列（留空 = 所有公共列）</span>
            <input
              value={compareCols}
              onChange={(e) => setCompareCols(e.target.value)}
              placeholder="如 金额,类型,主体"
              className="mt-1 w-full rounded-md border border-border-light bg-surface-primary px-2 py-1 font-mono"
              aria-label="对比列"
            />
          </label>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <label className="block">
              <span className="text-text-secondary">数值容差</span>
              <input
                type="number"
                step="0.001"
                value={numericTol}
                onChange={(e) => setNumericTol(e.target.value)}
                className="mt-1 w-full rounded-md border border-border-light bg-surface-primary px-2 py-1 font-mono"
                aria-label="数值容差"
              />
            </label>
            <label className="flex items-end gap-1">
              <input
                type="checkbox"
                checked={normalize}
                onChange={(e) => setNormalize(e.target.checked)}
                aria-label="字符串归一化"
              />
              <span className="text-text-secondary">字符串归一化</span>
            </label>
          </div>
          <button
            type="button"
            onClick={submit}
            disabled={!canRun}
            className={cn(
              'mt-1 inline-flex w-full items-center justify-center gap-1 rounded-md px-3 py-1.5 text-sm text-white',
              'bg-green-500 hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-50',
            )}
          >
            {createMut.isLoading ? (
              <>
                <Loader2 className="h-3.5 w-3.5 animate-spin" /> 比对中…
              </>
            ) : (
              <>
                <Play className="h-3.5 w-3.5" /> 开始比对
              </>
            )}
          </button>
          {createMut.error && (
            <div className="text-xs text-text-primary">
              ❌ {(createMut.error as Error).message}
            </div>
          )}
        </div>
      </div>

      {/* Middle column — report */}
      <div className="col-span-12 flex flex-col gap-3 overflow-auto md:col-span-7 lg:col-span-6">
        {activeMeta ? (
          <CompareReportView meta={activeMeta} />
        ) : (
          <EmptyTabPlaceholder
            icon={<Scale className="h-10 w-10" aria-hidden="true" />}
            title="尚未发起对比"
            description="在左侧选定 A/B 文件、配置主键列后点击「开始比对」。"
            hints={[
              '左 A/B 文件支持：渠道 run 产物 · 渠道源文件 · 上传外部',
              '主键列必填，决定行匹配；金额列可设容差',
              '产出：xlsx 报告 + JSON 摘要 + 差异 Top 列',
            ]}
          />
        )}
      </div>

      {/* Right column — history */}
      <div className="col-span-12 flex flex-col gap-2 overflow-auto md:col-span-12 lg:col-span-2">
        <div className="rounded-lg border border-border-light bg-surface-primary p-3">
          <div className="text-xs font-semibold uppercase tracking-wide text-text-secondary">
            历史对比
          </div>
          <ul className="mt-2 space-y-1.5">
            {(compares.data?.compares ?? []).map((c) => {
              const active = c.compare_id === compareIdParam;
              return (
                <li key={c.compare_id}>
                  <button
                    type="button"
                    onClick={() => {
                      const next = new URLSearchParams(searchParams);
                      next.set('tab', 'compare');
                      next.set('taskId', taskId);
                      next.set('compareId', c.compare_id);
                      setSearchParams(next);
                    }}
                    className={cn(
                      'w-full rounded-md border px-2 py-1.5 text-left text-xs transition-colors',
                      active
                        ? 'border-green-500 bg-green-500/10'
                        : 'border-border-light hover:bg-surface-secondary',
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono">{c.compare_id.slice(0, 8)}</span>
                      <span className="text-text-secondary">
                        Δ {c.summary?.diff_cells ?? 0}
                      </span>
                    </div>
                    <div className="mt-0.5 truncate text-text-secondary" title={c.left_ref}>
                      L: {c.left_ref}
                    </div>
                    <div className="truncate text-text-secondary" title={c.right_ref}>
                      R: {c.right_ref}
                    </div>
                  </button>
                </li>
              );
            })}
            {(compares.data?.compares ?? []).length === 0 && (
              <li className="text-center text-xs text-text-secondary">
                暂无历史
              </li>
            )}
          </ul>
        </div>
      </div>
    </div>
  );
}
