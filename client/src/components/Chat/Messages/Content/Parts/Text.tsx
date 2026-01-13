import { memo, useMemo, ReactElement } from 'react';
import { useRecoilValue } from 'recoil';
import MarkdownLite from '~/components/Chat/Messages/Content/MarkdownLite';
import Markdown from '~/components/Chat/Messages/Content/Markdown';
import { useMessageContext } from '~/Providers';
import { cn } from '~/utils';
import store from '~/store';
import type { TAttachment, UIResource } from '@because/data-provider';
import { UIResourceRenderer } from '@mcp-ui/client';

type TextPartProps = {
  text: string;
  showCursor: boolean;
  isCreatedByUser: boolean;
  attachments?: TAttachment[];
};

type ContentType =
  | ReactElement<React.ComponentProps<typeof Markdown>>
  | ReactElement<React.ComponentProps<typeof MarkdownLite>>
  | ReactElement;

const TextPart = memo(({ text, isCreatedByUser, showCursor, attachments }: TextPartProps) => {
  const { isSubmitting = false, isLatestMessage = false } = useMessageContext();
  const enableUserMsgMarkdown = useRecoilValue(store.enableUserMsgMarkdown);
  const showCursorState = useMemo(() => showCursor && isSubmitting, [showCursor, isSubmitting]);

  const content: ContentType = useMemo(() => {
    if (!isCreatedByUser) {
      return <Markdown content={text} isLatestMessage={isLatestMessage} attachments={attachments} />;
    } else if (enableUserMsgMarkdown) {
      return <MarkdownLite content={text} />;
    } else {
      return <>{text}</>;
    }
  }, [isCreatedByUser, enableUserMsgMarkdown, text, isLatestMessage, attachments]);

  // 处理 ui_resources attachments
  // 只渲染明确传递给当前 TEXT part 的 attachments（这些 attachments 已经通过 tool_call_ids 关联）
  const uiResources: UIResource[] = useMemo(() => {
    if (!attachments) return [];
    
    return attachments
      .filter((attachment) => attachment.type === 'ui_resources')
      .flatMap((attachment) => {
        const uiResourceData = attachment.ui_resources;
        if (Array.isArray(uiResourceData)) {
          return uiResourceData;
        } else if (uiResourceData && 'data' in uiResourceData && Array.isArray(uiResourceData.data)) {
          return uiResourceData.data;
        }
        return [];
      });
  }, [attachments]);

  return (
    <div
      className={cn(
        isSubmitting ? 'submitting' : '',
        showCursorState && !!text.length ? 'result-streaming' : '',
        'markdown prose message-content dark:prose-invert light w-full break-words',
        isCreatedByUser && !enableUserMsgMarkdown && 'whitespace-pre-wrap',
        isCreatedByUser ? 'dark:text-gray-20' : 'dark:text-gray-100',
      )}
    >
      {content}

      {/* 渲染 ui_resources */}
      {uiResources.length > 0 && (
        <div className="ui-resources-container" style={{ marginTop: '20px' }}>
          {uiResources.map((resource, index) => (
            <UIResourceRenderer
              key={`ui-resource-${index}`}
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
      )}
    </div>
  );
});

export default TextPart;
