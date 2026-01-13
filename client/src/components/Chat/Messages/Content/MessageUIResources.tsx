import { memo, useMemo } from 'react';
import type { TAttachment, UIResource } from '@because/data-provider';
import { Tools } from '@because/data-provider';
import { UIResourceRenderer } from '@mcp-ui/client';

type MessageUIResourcesProps = {
  attachments?: TAttachment[];
  // 排除这些 toolCallIds 的 attachments（因为它们已经在对应的 TEXT part 中渲染了）
  excludedToolCallIds?: Set<string>;
};

/**
 * 在消息末尾统一渲染所有 ui_resources
 * 这个组件用于渲染那些不属于任何特定 TEXT part 的 tool_call_ids 的 ui_resources
 */
const MessageUIResources = memo(({ attachments, excludedToolCallIds }: MessageUIResourcesProps) => {
  const uiResources: UIResource[] = useMemo(() => {
    if (!attachments) return [];

    return attachments
      .filter((attachment) => {
        // 只处理 ui_resources 类型
        if (attachment.type !== Tools.ui_resources) return false;
        
        // 如果 attachment 的 toolCallId 在排除列表中，跳过（因为它已经在 TEXT part 中渲染了）
        if (excludedToolCallIds && attachment.toolCallId && excludedToolCallIds.has(attachment.toolCallId)) {
          return false;
        }
        
        return true;
      })
      .flatMap((attachment) => {
        const uiResourceData = attachment[Tools.ui_resources];
        if (Array.isArray(uiResourceData)) {
          return uiResourceData;
        } else if (uiResourceData && 'data' in uiResourceData && Array.isArray(uiResourceData.data)) {
          return uiResourceData.data;
        }
        return [];
      });
  }, [attachments, excludedToolCallIds]);

  if (uiResources.length === 0) {
    return null;
  }

  return (
    <div className="message-ui-resources-container" style={{ marginTop: '20px' }}>
      {uiResources.map((resource, index) => (
        <UIResourceRenderer
          key={`message-ui-resource-${index}`}
          resource={resource}
          onUIAction={async (result) => {
            console.log('UI Action:', result);
          }}
          htmlProps={{
            autoResizeIframe: { width: true, height: true },
            style: {
              width: '100%',
              minHeight: '500px',
              border: 'none',
              borderRadius: '8px',
            }
          }}
        />
      ))}
    </div>
  );
});

MessageUIResources.displayName = 'MessageUIResources';

export default MessageUIResources;

