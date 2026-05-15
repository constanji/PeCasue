import React, { useEffect, useMemo, useState } from 'react';
import { Plus, Save, Trash2, X } from 'lucide-react';
import { useSetRecoilState } from 'recoil';
import { pipelineRulesDirtyAtom } from '~/store/pipeline';
import type { PipelineRuleTable } from '~/data-provider';
import { cn } from '~/utils';

/** 每页行数；超过则分页渲染 tbody，减轻主线程压力 */
const ROW_PAGE_SIZE = 125;

export interface RuleTableEditorProps {
  table: PipelineRuleTable;
  passwordColumns?: string[]; // columns rendered as password input
  readOnlyColumns?: string[];
  /** 上传覆盖类规则：隐藏加列/加行/备注/保存，表格只读。 */
  hideColumnTools?: boolean;
  /** Hide editing toolbar and cells — browse-only (versions API unchanged). */
  viewOnly?: boolean;
  onSave: (next: PipelineRuleTable, note?: string) => Promise<void> | void;
  saving?: boolean;
  bannerNote?: React.ReactNode;
  /** 统计条（总行数行）右侧附加文案，例如汇率所属月份 */
  statsTrailing?: React.ReactNode;
}

function useDraft(initial: PipelineRuleTable) {
  const [columns, setColumns] = useState<string[]>(initial.columns);
  const [rows, setRows] = useState<Record<string, unknown>[]>(initial.rows);
  const [dirty, setDirty] = useState(false);
  useEffect(() => {
    setColumns(initial.columns);
    setRows(initial.rows);
    setDirty(false);
  }, [initial]);
  return { columns, setColumns, rows, setRows, dirty, setDirty };
}

export default function RuleTableEditor({
  table,
  passwordColumns = [],
  readOnlyColumns = [],
  hideColumnTools = false,
  viewOnly = false,
  onSave,
  saving,
  bannerNote,
  statsTrailing,
}: RuleTableEditorProps) {
  const { columns, setColumns, rows, setRows, dirty, setDirty } = useDraft(table);
  const setRulesDirty = useSetRecoilState(pipelineRulesDirtyAtom);
  const [newColName, setNewColName] = useState('');
  const [note, setNote] = useState('');
  const [page, setPage] = useState(0);

  useEffect(() => {
    setPage(0);
  }, [table.rows, table.columns]);

  useEffect(() => {
    const pc = Math.max(1, Math.ceil(rows.length / ROW_PAGE_SIZE));
    setPage((p) => Math.min(p, pc - 1));
  }, [rows.length]);

  const paginated = rows.length > ROW_PAGE_SIZE;
  const pageCount = Math.max(1, Math.ceil(rows.length / ROW_PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  const sliceStart = safePage * ROW_PAGE_SIZE;
  const sliceEnd = paginated ? sliceStart + ROW_PAGE_SIZE : rows.length;
  const visibleRows = paginated ? rows.slice(sliceStart, sliceEnd) : rows;

  useEffect(() => {
    if (viewOnly || hideColumnTools) {
      setRulesDirty(false);
      return () => {
        setRulesDirty(false);
      };
    }
    setRulesDirty(dirty);
    return () => {
      setRulesDirty(false);
    };
  }, [dirty, setRulesDirty, viewOnly, hideColumnTools]);

  // beforeunload guard while there are unsaved edits
  useEffect(() => {
    if (viewOnly || hideColumnTools || !dirty) return;
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = '';
    };
    window.addEventListener('beforeunload', handler);
    return () => window.removeEventListener('beforeunload', handler);
  }, [dirty, viewOnly, hideColumnTools]);

  const passwordSet = useMemo(() => new Set(passwordColumns), [passwordColumns]);
  const readOnlySet = useMemo(() => new Set(readOnlyColumns), [readOnlyColumns]);

  const updateCell = (rowIdx: number, col: string, value: unknown) => {
    setRows((prev) => {
      const next = prev.slice();
      next[rowIdx] = { ...next[rowIdx], [col]: value };
      return next;
    });
    setDirty(true);
  };

  const addRow = () => {
    const blank: Record<string, unknown> = {};
    columns.forEach((c) => (blank[c] = ''));
    setRows((prev) => [...prev, blank]);
    setDirty(true);
  };

  const deleteRow = (idx: number) => {
    setRows((prev) => prev.filter((_, i) => i !== idx));
    setDirty(true);
  };

  const addColumn = () => {
    const name = newColName.trim();
    if (!name || columns.includes(name)) return;
    setColumns((prev) => [...prev, name]);
    setRows((prev) => prev.map((r) => ({ ...r, [name]: '' })));
    setNewColName('');
    setDirty(true);
  };

  const deleteColumn = (col: string) => {
    setColumns((prev) => prev.filter((c) => c !== col));
    setRows((prev) =>
      prev.map((r) => {
        const { [col]: _drop, ...rest } = r;
        return rest;
      }),
    );
    setDirty(true);
  };

  const handleSave = async () => {
    await onSave(
      { columns, rows, note: table.note ?? null, meta: table.meta ?? null },
      note || undefined,
    );
    setNote('');
    setDirty(false);
  };

  return (
    <div className="space-y-3">
      {bannerNote && (
        <div className="rounded-lg border border-border-light bg-surface-secondary p-3 text-xs text-text-secondary">
          {bannerNote}
        </div>
      )}
      {!viewOnly && !hideColumnTools && (
        <div className="flex items-center justify-between gap-3 rounded-xl border border-dashed border-border-medium bg-gradient-to-b from-surface-secondary/95 to-surface-primary/60 px-3 py-3 shadow-[inset_0_1px_0_0_rgba(255,255,255,0.04)] ring-1 ring-border-medium/40">
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="新增列名"
              aria-label="新增列名"
              value={newColName}
              onChange={(e) => setNewColName(e.target.value)}
              className="rounded-md border border-border-light bg-surface-secondary px-2 py-1 text-sm text-text-primary focus:border-green-500 focus:outline-none"
            />
            <button
              type="button"
              onClick={addColumn}
              className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
            >
              <Plus className="h-3 w-3" /> 加列
            </button>
            <button
              type="button"
              onClick={addRow}
              className="inline-flex items-center gap-1 rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
            >
              <Plus className="h-3 w-3" /> 加行
            </button>
          </div>
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="本次修改备注（可选）"
              aria-label="本次修改备注"
              value={note}
              onChange={(e) => setNote(e.target.value)}
              className="w-56 rounded-md border border-border-light bg-surface-secondary px-2 py-1 text-sm text-text-primary focus:border-green-500 focus:outline-none"
            />
            <button
              type="button"
              disabled={!dirty || saving}
              onClick={handleSave}
              className="inline-flex items-center gap-1 rounded-md bg-green-500 px-3 py-1 text-xs font-medium text-white disabled:opacity-50"
            >
              <Save className="h-3 w-3" /> {saving ? '保存中…' : dirty ? '保存版本' : '已保存'}
            </button>
          </div>
        </div>
      )}
      <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border-light bg-surface-secondary/80 px-3 py-2 text-[11px] text-text-secondary">
        <span>
          <span className="font-medium text-text-primary">总行数</span>{' '}
          <span className="tabular-nums text-green-400">{rows.length}</span>
          {paginated && rows.length > 0 ? (
            <>
              {' '}
              · 分页浏览 · 每页{' '}
              <span className="font-mono tabular-nums text-text-primary">{ROW_PAGE_SIZE}</span> 行
            </>
          ) : null}
        </span>
        <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
          {statsTrailing ? <span className="tabular-nums">{statsTrailing}</span> : null}
          {paginated && rows.length > 0 ? (
            <span className="tabular-nums text-text-secondary">
              第{' '}
              <span className="font-mono text-text-primary">{sliceStart + 1}</span>
              –
              <span className="font-mono text-text-primary">{Math.min(sliceEnd, rows.length)}</span>{' '}
              行
            </span>
          ) : null}
        </span>
      </div>
      <div
        className={cn(
          'overflow-auto rounded-lg border border-border-light',
          paginated && rows.length > 0 && 'max-h-[min(70vh,780px)]',
        )}
      >
        <table className="w-full text-sm">
          <thead className="bg-surface-secondary text-xs text-text-secondary">
            <tr>
              <th className="w-8 px-2 py-2 text-left">#</th>
              {columns.map((c) => (
                <th key={c} className="px-2 py-2 text-left">
                  <div className="flex items-center justify-between gap-1">
                    <span className="text-text-primary">{c}</span>
                    {!hideColumnTools && !viewOnly && !readOnlySet.has(c) && (
                      <button
                        type="button"
                        onClick={() => deleteColumn(c)}
                        className="text-text-secondary opacity-50 hover:text-text-primary hover:opacity-100"
                        aria-label={`删除列 ${c}`}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    )}
                  </div>
                </th>
              ))}
              {!viewOnly && !hideColumnTools && (
                <th className="w-10 px-2 py-2 text-right">操作</th>
              )}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length + 1 + (!viewOnly && !hideColumnTools ? 1 : 0)}
                  className="px-3 py-6 text-center text-sm text-text-secondary"
                >
                  {viewOnly
                    ? '暂无数据。'
                    : hideColumnTools
                      ? '暂无规则 — 请使用上方上传导入 Excel。'
                      : '暂无规则行 — 点击「加行」创建。'}
                </td>
              </tr>
            ) : (
              visibleRows.map((row, vIdx) => {
                const rIdx = sliceStart + vIdx;
                return (
                <tr key={rIdx} className="border-t border-border-light">
                  <td className="px-2 py-1 text-xs text-text-secondary">{rIdx + 1}</td>
                  {columns.map((c) => {
                    const v = row[c];
                    const isPassword = passwordSet.has(c);
                    return (
                      <td key={c} className="px-2 py-1">
                        <input
                          type={isPassword ? 'password' : 'text'}
                          readOnly={viewOnly || readOnlySet.has(c) || hideColumnTools}
                          value={v == null ? '' : String(v)}
                          onChange={(e) => updateCell(rIdx, c, e.target.value)}
                          aria-label={c}
                          className={cn(
                            'w-full rounded-sm bg-transparent px-1 py-0.5 text-xs text-text-primary focus:bg-surface-secondary focus:outline-none',
                            (viewOnly || readOnlySet.has(c) || hideColumnTools) &&
                              'cursor-default focus:bg-transparent',
                          )}
                        />
                      </td>
                    );
                  })}
                  {!viewOnly && !hideColumnTools && (
                    <td className="px-2 py-1 text-right">
                      <button
                        type="button"
                        onClick={() => deleteRow(rIdx)}
                        className="text-text-secondary hover:text-text-primary"
                        aria-label="删除行"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  )}
                </tr>
              );
              })
            )}
          </tbody>
        </table>
        {paginated && rows.length > 0 && (
          <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border-light bg-surface-secondary/40 px-3 py-2 text-xs text-text-secondary">
            <span>
              第 <span className="font-mono text-text-primary">{sliceStart + 1}</span>–
              <span className="font-mono text-text-primary">{Math.min(sliceEnd, rows.length)}</span> 行 · 共{' '}
              <span className="font-mono text-text-primary">{rows.length}</span> 行
            </span>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                disabled={safePage <= 0}
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                className="rounded-md border border-border-light px-2 py-1 hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-40"
              >
                上一页
              </button>
              <span className="font-mono">
                {safePage + 1} / {pageCount}
              </span>
              <button
                type="button"
                disabled={safePage >= pageCount - 1}
                onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
                className="rounded-md border border-border-light px-2 py-1 hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-40"
              >
                下一页
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
