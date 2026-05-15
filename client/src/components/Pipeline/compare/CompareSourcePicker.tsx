import React, { useMemo, useState } from 'react';
import { ChevronRight, FileUp, Loader2 } from 'lucide-react';
import {
  usePipelineChannel,
  usePipelineChannelCatalog,
  usePipelineClassification,
  useUploadCompareFile,
  type PipelineCompareSource,
} from '~/data-provider';
import {
  pipelineArtifactDisplayTitle,
  pipelineArtifactTechnicalName,
} from '~/components/Pipeline/preview/pipelineArtifactLabels';
import { cn } from '~/utils';

type Mode = 'run_output' | 'source_file' | 'upload';

const MODES: { id: Mode; label: string; hint: string }[] = [
  { id: 'run_output', label: '已产出 (Run)', hint: '从某次执行的产物中选择' },
  { id: 'source_file', label: '原始上传', hint: '从上传的源文件中选择' },
  { id: 'upload', label: '上传外部', hint: '上传一份外部 Excel/CSV 暂存' },
];

export default function CompareSourcePicker({
  taskId,
  side,
  value,
  onChange,
}: {
  taskId: string;
  side: 'left' | 'right';
  value: PipelineCompareSource | null;
  onChange: (next: PipelineCompareSource | null) => void;
}) {
  const [mode, setMode] = useState<Mode>(value?.kind || 'run_output');
  const [channelId, setChannelId] = useState<string | null>(value?.channel_id || null);
  const catalog = usePipelineChannelCatalog();
  const classification = usePipelineClassification(taskId);
  const channelDetail = usePipelineChannel(taskId, channelId);
  const uploadMut = useUploadCompareFile(taskId);

  const channels = catalog.data?.channels ?? [];
  const sourceFiles = channelId
    ? classification.data?.channels?.[channelId]?.files ?? []
    : [];
  const runs = channelDetail.data?.runs ?? [];

  return (
    <div className="space-y-3 rounded-lg border border-border-light bg-surface-primary p-3">
      <div className="flex items-center justify-between">
        <div className="text-xs font-semibold uppercase tracking-wide text-text-secondary">
          {side === 'left' ? '左 · A 文件' : '右 · B 文件'}
        </div>
        <div className="flex gap-1">
          {MODES.map((m) => (
            <button
              key={m.id}
              type="button"
              onClick={() => {
                setMode(m.id);
                onChange(null);
              }}
              className={cn(
                'rounded-md border px-2 py-0.5 text-[11px] transition-colors',
                mode === m.id
                  ? 'border-green-500 bg-green-500/10 text-green-500'
                  : 'border-border-light text-text-secondary hover:bg-surface-secondary',
              )}
              aria-label={m.label}
              title={m.hint}
            >
              {m.label}
            </button>
          ))}
        </div>
      </div>

      {mode !== 'upload' && (
        <select
          value={channelId ?? ''}
          onChange={(e) => {
            setChannelId(e.target.value || null);
            onChange(null);
          }}
          className="w-full rounded-md border border-border-light bg-surface-primary px-2 py-1.5 text-sm text-text-primary"
          aria-label="选择渠道"
        >
          <option value="">— 选择渠道 —</option>
          {channels.map((c) => (
            <option key={c.channel_id} value={c.channel_id}>
              {c.display_name} · {c.channel_id}
            </option>
          ))}
        </select>
      )}

      {mode === 'run_output' && channelId && (
        <div className="space-y-2">
          <div className="text-xs text-text-secondary">选择 run 与产物文件</div>
          {runs.length === 0 ? (
            <div className="rounded-md border border-dashed border-border-medium p-3 text-xs text-text-secondary">
              该渠道还没有 run，请先执行一次。
            </div>
          ) : (
            <div className="max-h-60 space-y-1 overflow-auto">
              {runs
                .slice()
                .reverse()
                .map((r) => (
                  <details key={r.run_id} className="rounded-md border border-border-light">
                    <summary className="flex cursor-pointer items-center justify-between px-2 py-1.5 text-xs text-text-primary">
                      <span className="font-mono text-text-primary">{r.run_id.slice(0, 8)}</span>
                      <span className="text-text-secondary">
                        {r.status} · {r.output_files.length} 个产物
                      </span>
                    </summary>
                    <ul className="border-t border-border-light px-2 py-1">
                      {r.output_files.map((f) => (
                        <li key={f.file_id}>
                          <button
                            type="button"
                            className={cn(
                              'flex w-full items-center justify-between gap-2 px-2 py-1 text-left text-xs text-text-primary hover:bg-surface-secondary',
                              value?.kind === 'run_output' &&
                                value.run_id === r.run_id &&
                                value.name === f.name &&
                                'bg-green-500/10 text-green-500',
                            )}
                            onClick={() =>
                              onChange({
                                kind: 'run_output',
                                channel_id: channelId,
                                run_id: r.run_id,
                                name: f.name,
                              })
                            }
                          >
                            <span className="flex min-w-0 flex-col text-left">
                              <span className="truncate font-medium">
                                {pipelineArtifactDisplayTitle(f.name)}
                              </span>
                              <span className="truncate font-mono text-[10px] text-text-secondary">
                                {pipelineArtifactTechnicalName(f.name)}
                              </span>
                            </span>
                            <ChevronRight className="h-3 w-3" />
                          </button>
                        </li>
                      ))}
                    </ul>
                  </details>
                ))}
            </div>
          )}
        </div>
      )}

      {mode === 'source_file' && channelId && (
        <div className="space-y-2">
          <div className="text-xs text-text-secondary">选择源文件</div>
          {sourceFiles.length === 0 ? (
            <div className="rounded-md border border-dashed border-border-medium p-3 text-xs text-text-secondary">
              该渠道无源文件
            </div>
          ) : (
            <ul className="max-h-60 space-y-0.5 overflow-auto">
              {sourceFiles.map((f) => (
                <li key={f.rel_path}>
                  <button
                    type="button"
                    onClick={() =>
                      onChange({
                        kind: 'source_file',
                        channel_id: channelId,
                        rel_path: f.rel_path,
                      })
                    }
                    className={cn(
                      'flex w-full items-center justify-between gap-2 rounded px-2 py-1 text-left text-xs text-text-primary',
                      value?.kind === 'source_file' &&
                        value.rel_path === f.rel_path
                        ? 'bg-green-500/10 text-green-500'
                        : 'hover:bg-surface-secondary',
                    )}
                  >
                    <span className="truncate font-mono">{f.rel_path}</span>
                    <span className="text-text-secondary">{f.size}B</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {mode === 'upload' && (
        <div className="space-y-2">
          <label
            className={cn(
              'flex cursor-pointer flex-col items-center justify-center gap-1 rounded-md border border-dashed',
              'border-border-medium bg-surface-secondary p-4 text-xs text-text-secondary hover:bg-surface-tertiary',
            )}
          >
            <FileUp className="h-4 w-4" />
            点选或拖入 .xlsx / .csv 暂存
            <input
              type="file"
              accept=".xlsx,.xls,.csv,.tsv"
              className="hidden"
              aria-label={`upload ${side}`}
              onChange={async (e) => {
                const f = e.target.files?.[0];
                if (!f) return;
                const res = await uploadMut.mutateAsync(f);
                onChange({ kind: 'upload', staged_path: res.staged_path, name: res.name });
              }}
            />
          </label>
          {uploadMut.isLoading && (
            <div className="flex items-center gap-1 text-xs text-text-secondary">
              <Loader2 className="h-3 w-3 animate-spin" /> 上传中…
            </div>
          )}
          {value?.kind === 'upload' && (
            <div className="rounded-md bg-green-500/10 px-2 py-1 text-xs text-text-primary">
              已暂存：<span className="font-mono">{value.name ?? value.staged_path}</span>
            </div>
          )}
        </div>
      )}

      {value && value.kind !== 'upload' && (
        <div className="rounded-md bg-green-500/5 px-2 py-1 text-xs text-text-primary">
          已选：
          <span className="ml-1 font-mono">
            {value.channel_id}
            {value.run_id ? ` / ${value.run_id.slice(0, 8)}` : ''}
            {value.name ? ` / ${value.name}` : ''}
            {value.rel_path ? ` / ${value.rel_path}` : ''}
          </span>
        </div>
      )}
    </div>
  );
}
