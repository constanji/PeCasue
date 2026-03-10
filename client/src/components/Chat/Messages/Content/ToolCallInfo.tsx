import React, { useMemo } from 'react';
import { useLocalize } from '~/hooks';
import { Tools } from '@because/data-provider';
import { UIResourceRenderer } from '@mcp-ui/client';
import UIResourceCarousel from './UIResourceCarousel';
import type { TAttachment, UIResource } from '@because/data-provider';
import { ChartRenderer, extractChartDataFromToolOutput } from './ChartRenderer';

function OptimizedCodeBlock({ text, maxHeight = 320 }: { text: string; maxHeight?: number }) {
  return (
    <div
      className="rounded-lg bg-surface-tertiary p-2 text-xs text-text-primary"
      style={{
        position: 'relative',
        maxHeight,
        overflow: 'auto',
      }}
    >
      <pre className="m-0 whitespace-pre-wrap break-words" style={{ overflowWrap: 'break-word' }}>
        <code>{text}</code>
      </pre>
    </div>
  );
}

export default function ToolCallInfo({
  input,
  output,
  domain,
  function_name,
  pendingAuth,
  attachments,
}: {
  input: string;
  function_name: string;
  output?: string | null;
  domain?: string;
  pendingAuth?: boolean;
  attachments?: TAttachment[];
}) {
  const localize = useLocalize();
  const formatText = (text: string) => {
    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  };

  const chartData = useMemo(
    () => (typeof output === 'string' ? extractChartDataFromToolOutput(output) : null),
    [output],
  );

  let title =
    domain != null && domain
      ? localize('com_assistants_domain_info', { 0: domain })
      : localize('com_assistants_function_use', { 0: function_name });
  if (pendingAuth === true) {
    title =
      domain != null && domain
        ? localize('com_assistants_action_attempt', { 0: domain })
        : localize('com_assistants_attempt_info');
  }

  const uiResources: UIResource[] =
    attachments
      ?.filter((attachment) => attachment.type === Tools.ui_resources)
      .flatMap((attachment) => {
        const uiResourceData = attachment[Tools.ui_resources] as unknown;
        if (Array.isArray(uiResourceData)) {
          return uiResourceData as UIResource[];
        }
        const container = uiResourceData as { data?: UIResource[] } | undefined;
        if (container?.data && Array.isArray(container.data)) {
          return container.data;
        }
        return [] as UIResource[];
      })
      .filter((resource) => !chartData || typeof resource.chartId !== 'string') ?? [];

  return (
    <div className="w-full p-2">
      <div style={{ opacity: 1 }}>
        <div className="mb-2 text-sm font-medium text-text-primary">{title}</div>
        <div>
          <OptimizedCodeBlock text={formatText(input)} maxHeight={250} />
        </div>
        {chartData && (
          <ChartRenderer
            chartId={`tc-${chartData.chartId}`}
            title={chartData.title}
            data={chartData.data}
            layout={chartData.layout}
          />
        )}
        {output && !chartData && (
          <>
            <div className="my-2 text-sm font-medium text-text-primary">
              {localize('com_ui_result')}
            </div>
            <div>
              <OptimizedCodeBlock text={formatText(output)} maxHeight={250} />
            </div>
          </>
        )}
        {uiResources.length > 0 && (
          <>
            <div className="my-2 text-sm font-medium text-text-primary">
              {localize('com_ui_ui_resources')}
            </div>
            <div>
              {uiResources.length > 1 && <UIResourceCarousel uiResources={uiResources} />}
              {uiResources.length === 1 && (
                <UIResourceRenderer
                  resource={uiResources[0]}
                  onUIAction={async (result) => {
                    console.log('Action:', result);
                  }}
                  htmlProps={{
                    autoResizeIframe: { width: true, height: true },
                  }}
                />
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
