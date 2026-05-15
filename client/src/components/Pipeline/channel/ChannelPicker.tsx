import React from 'react';
import {
  usePipelineChannelCatalog,
  usePipelineClassification,
} from '~/data-provider';
import { cn } from '~/utils';

export default function ChannelPicker({
  taskId,
  onPick,
}: {
  taskId: string;
  onPick: (channelId: string) => void;
}) {
  const catalog = usePipelineChannelCatalog();
  const cls = usePipelineClassification(taskId);

  if (catalog.isLoading || cls.isLoading) {
    return <div className="text-sm text-text-secondary">加载渠道列表…</div>;
  }
  if (catalog.error) {
    return <div className="text-sm text-text-primary">{catalog.error.message}</div>;
  }
  const defs = catalog.data?.channels ?? [];
  const classification = cls.data?.channels ?? {};

  return (
    <div className="space-y-3">
      <div className="rounded-lg border border-border-light bg-surface-primary p-3 text-sm text-text-secondary">
        当前任务 <span className="font-mono text-text-primary">{taskId.slice(0, 8)}</span>
        ，选择一个渠道查看详情。
      </div>
      <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
        {defs.map((d) => {
          const group = classification[d.channel_id];
          const fc = group?.files?.length ?? 0;
          const status = group?.status ?? 'pending';
          return (
            <button
              key={d.channel_id}
              type="button"
              onClick={() => onPick(d.channel_id)}
              className={cn(
                'rounded-xl border bg-surface-primary p-4 text-left transition-colors hover:border-green-500/50',
                fc === 0 ? 'border-dashed border-border-medium' : 'border-border-light',
              )}
            >
              <div className="flex items-start justify-between">
                <div>
                  <h4 className="text-sm font-semibold text-text-primary">{d.display_name}</h4>
                  <p className="mt-0.5 text-xs text-text-secondary">{d.hint}</p>
                </div>
                <span className="rounded border border-border-light px-1.5 py-0.5 font-mono text-[10px] text-text-secondary">
                  {d.channel_id}
                </span>
              </div>
              <div className="mt-3 text-xs text-text-secondary">
                <span className="font-medium text-text-primary">{fc}</span> 文件 · 状态{' '}
                <span className="font-mono">{status}</span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
