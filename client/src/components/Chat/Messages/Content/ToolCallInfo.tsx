import React from 'react';
import { useLocalize } from '~/hooks';
import { Tools } from '@because/data-provider';
import { UIResourceRenderer } from '@mcp-ui/client';
import UIResourceCarousel from './UIResourceCarousel';
import type { TAttachment, UIResource } from '@because/data-provider';

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
        const uiResourceData = attachment[Tools.ui_resources];
        // 处理不同的数据结构：直接数组或包含data属性的对象
        if (Array.isArray(uiResourceData)) {
          return uiResourceData;
        } else if (uiResourceData && uiResourceData.data && Array.isArray(uiResourceData.data)) {
          return uiResourceData.data;
        }
        return [];
      }) ?? [];

  // 调试日志
  React.useEffect(() => {
    console.log('[ToolCallInfo] ========== Debug Info ==========');
    console.log('[ToolCallInfo] Function:', function_name);
    console.log('[ToolCallInfo] Attachments received:', attachments);
    console.log('[ToolCallInfo] Attachments count:', attachments?.length ?? 0);
    console.log('[ToolCallInfo] UI Resources extracted:', uiResources);
    console.log('[ToolCallInfo] UI Resources count:', uiResources.length);
    if (uiResources.length > 0) {
      console.log('[ToolCallInfo] First UI Resource:', uiResources[0]);
      console.log('[ToolCallInfo] UI Resource type:', uiResources[0].type);
      console.log('[ToolCallInfo] UI Resource has text:', !!uiResources[0].text);
      console.log('[ToolCallInfo] UI Resource text length:', uiResources[0].text?.length ?? 0);
      console.log('[ToolCallInfo] UI Resource uri:', uiResources[0].uri);
    } else {
      console.log('[ToolCallInfo] No UI Resources found');
    }
    console.log('[ToolCallInfo] Output:', output);
    console.log('[ToolCallInfo] =================================');
  }, [attachments, uiResources, function_name, output]);

  return (
    <div className="w-full p-2">
      <div style={{ opacity: 1 }}>
        <div className="mb-2 text-sm font-medium text-text-primary">{title}</div>
        <div>
          <OptimizedCodeBlock text={formatText(input)} maxHeight={250} />
        </div>
        {output && (
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
