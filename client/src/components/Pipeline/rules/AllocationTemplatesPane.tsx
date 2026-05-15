import React, { useRef } from 'react';
import { Loader2, Upload } from 'lucide-react';
import { useAllocationTemplatesStatus, useUploadAllocationTemplate } from '~/data-provider';
import { cn } from '~/utils';

function StatusBadge({ present }: { present: boolean }) {
  return (
    <span
      className={cn(
        'rounded-md border px-2 py-0.5 text-[11px] font-medium',
        present
          ? 'border-green-500/40 bg-green-500/10 text-green-400'
          : 'border-amber-500/45 bg-amber-500/10 text-amber-200',
      )}
    >
      {present ? '已有' : '缺失'}
    </span>
  );
}

export default function AllocationTemplatesPane() {
  const statusQ = useAllocationTemplatesStatus();
  const uploadMut = useUploadAllocationTemplate();
  const quickbiRef = useRef<HTMLInputElement | null>(null);
  const citihkRef = useRef<HTMLInputElement | null>(null);
  const costAllocateRef = useRef<HTMLInputElement | null>(null);

  const pick = async (kind: 'quickbi' | 'citihk' | 'cost_allocate', file: File | undefined) => {
    if (!file) return;
    await uploadMut.mutateAsync({ templateKind: kind, file });
  };

  return (
    <div className="h-full overflow-auto p-4">
      <div className="mx-auto max-w-2xl space-y-4 rounded-xl border border-border-light bg-surface-primary p-4 shadow-sm">
        <div>
          <h2 className="text-sm font-semibold text-text-primary">分摊基数模版</h2>
          <p className="mt-1 text-xs leading-relaxed text-text-secondary">
            文件写入{' '}
            <code className="rounded bg-surface-secondary px-1 py-0.5 font-mono text-[10px]">
              data/rules/files/allocation/
            </code>
            ，供分摊基数流水线与成本分摊步骤读取。
          </p>
        </div>

        {statusQ.isLoading && (
          <div className="flex items-center gap-2 text-xs text-text-secondary">
            <Loader2 className="h-4 w-4 animate-spin text-green-500" aria-hidden />
            加载状态…
          </div>
        )}
        {statusQ.error && (
          <div className="text-xs text-red-300">{(statusQ.error as Error).message}</div>
        )}

        <div className="space-y-3 border-t border-border-light pt-4">
          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border-light bg-surface-secondary/40 px-3 py-3">
            <div className="min-w-0 flex-1">
              <div className="text-xs font-medium text-text-primary">QuickBI · 收付款成本分摊基数表模版</div>
              <div className="mt-0.5 font-mono text-[10px] text-text-secondary">
                allocation/quickbi/收付款成本分摊基数表模版.xlsx
              </div>
            </div>
            <div className="flex shrink-0 flex-wrap items-center gap-2">
              {statusQ.data ? <StatusBadge present={statusQ.data.quickbi_present} /> : null}
              <input
                ref={quickbiRef}
                type="file"
                accept=".xlsx,.xlsm"
                className="hidden"
                onChange={(e) => {
                  void pick('quickbi', e.target.files?.[0]);
                  e.target.value = '';
                }}
              />
              <button
                type="button"
                disabled={uploadMut.isLoading}
                onClick={() => quickbiRef.current?.click()}
                className="inline-flex items-center gap-1 rounded-md border border-green-500/45 bg-green-500/10 px-2 py-1 text-[11px] font-medium text-green-400 hover:bg-green-500/15 disabled:opacity-50"
              >
                <Upload className="h-3 w-3" />
                上传
              </button>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border-light bg-surface-secondary/40 px-3 py-3">
            <div className="min-w-0 flex-1">
              <div className="text-xs font-medium text-text-primary">CitiHK · PPHK 基数模版</div>
              <div className="mt-0.5 font-mono text-[10px] text-text-secondary">
                allocation/citihk/mapping/基数PPHK模版.xlsx
              </div>
            </div>
            <div className="flex shrink-0 flex-wrap items-center gap-2">
              {statusQ.data ? <StatusBadge present={statusQ.data.citihk_present} /> : null}
              <input
                ref={citihkRef}
                type="file"
                accept=".xlsx,.xlsm"
                className="hidden"
                onChange={(e) => {
                  void pick('citihk', e.target.files?.[0]);
                  e.target.value = '';
                }}
              />
              <button
                type="button"
                disabled={uploadMut.isLoading}
                onClick={() => citihkRef.current?.click()}
                className="inline-flex items-center gap-1 rounded-md border border-green-500/45 bg-green-500/10 px-2 py-1 text-[11px] font-medium text-green-400 hover:bg-green-500/15 disabled:opacity-50"
              >
                <Upload className="h-3 w-3" />
                上传
              </button>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border-light bg-surface-secondary/40 px-3 py-3">
            <div className="min-w-0 flex-1">
              <div className="text-xs font-medium text-text-primary">
                成本分摊基数 + 输出模版（cost_allocate）
              </div>
              <div className="mt-0.5 font-mono text-[10px] text-text-secondary">
                allocation/成本分摊基数+输出模板.xlsx
              </div>
            </div>
            <div className="flex shrink-0 flex-wrap items-center gap-2">
              {statusQ.data ? (
                <StatusBadge present={statusQ.data.cost_allocate_workbook_present} />
              ) : null}
              <input
                ref={costAllocateRef}
                type="file"
                accept=".xlsx,.xlsm"
                className="hidden"
                onChange={(e) => {
                  void pick('cost_allocate', e.target.files?.[0]);
                  e.target.value = '';
                }}
              />
              <button
                type="button"
                disabled={uploadMut.isLoading}
                onClick={() => costAllocateRef.current?.click()}
                className="inline-flex items-center gap-1 rounded-md border border-green-500/45 bg-green-500/10 px-2 py-1 text-[11px] font-medium text-green-400 hover:bg-green-500/15 disabled:opacity-50"
              >
                <Upload className="h-3 w-3" />
                上传
              </button>
            </div>
          </div>
        </div>

        {uploadMut.error && (
          <div className="text-xs text-red-300">{(uploadMut.error as Error).message}</div>
        )}
      </div>
    </div>
  );
}
