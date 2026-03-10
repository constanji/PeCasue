import { memo, useMemo } from 'react';
import { ContentTypes, Tools } from '@because/data-provider';
import type {
  TMessageContentParts,
  SearchResultData,
  TAttachment,
  Agents,
} from '@because/data-provider';
import { MessageContext, SearchContext } from '~/Providers';
import MemoryArtifacts from './MemoryArtifacts';
import Sources from '~/components/Web/Sources';
import { mapAttachments } from '~/utils/map';
import { EditTextPart } from './Parts';
import Part from './Part';
import MessageUIResources from './MessageUIResources';
import { extractChartDataFromToolOutput } from './ChartRenderer';


type ContentPartsProps = {
  content: Array<TMessageContentParts | undefined> | undefined;
  messageId: string;
  conversationId?: string | null;
  attachments?: TAttachment[];
  searchResults?: { [key: string]: SearchResultData };
  isCreatedByUser: boolean;
  isLast: boolean;
  isSubmitting: boolean;
  isLatestMessage?: boolean;
  edit?: boolean;
  enterEdit?: (cancel?: boolean) => void | null | undefined;
  siblingIdx?: number;
  setSiblingIdx?:
    | ((value: number) => void | React.Dispatch<React.SetStateAction<number>>)
    | null
    | undefined;
};

const ContentParts = memo(
  ({
    content,
    messageId,
    conversationId,
    attachments,
    searchResults,
    isCreatedByUser,
    isLast,
    isSubmitting,
    isLatestMessage,
    edit,
    enterEdit,
    siblingIdx,
    setSiblingIdx,
  }: ContentPartsProps) => {


    const attachmentMap = useMemo(() => mapAttachments(attachments ?? []), [attachments]);

    const effectiveIsSubmitting = isLatestMessage ? isSubmitting : false;

    if (!content) {
      return null;
    }

    // 对于助手消息：如果包含工具调用 / 思维链内容，
    // 只在 chat 中展示「工具调用前的正常文本」和「最后的总结文本」，
    // 中间的思维链叙述 + 工具执行说明只在右侧思维链中展示。
    const filteredContent = useMemo(() => {
      if (!content || isCreatedByUser) {
        return content ?? [];
      }

      const hasToolOrThink = content.some(
        (part) =>
          part &&
          (part.type === ContentTypes.TOOL_CALL ||
            part.type === ContentTypes.THINK ||
            (part as unknown as Agents.ReasoningDeltaUpdate)?.think),
      );

      // 没有工具调用 / 思维链，说明是普通回答，保持原样
      if (!hasToolOrThink) {
        return content;
      }

      // 计算第一个 / 最后一个 TOOL_CALL 或 THINK 的位置
      let firstToolOrThinkIndex = -1;
      let lastToolOrThinkIndex = -1;

      content.forEach((part, idx) => {
        if (!part) return;
        if (
          part.type === ContentTypes.TOOL_CALL ||
          part.type === ContentTypes.THINK ||
          (part as unknown as Agents.ReasoningDeltaUpdate)?.think
        ) {
          if (firstToolOrThinkIndex === -1) {
            firstToolOrThinkIndex = idx;
          }
          lastToolOrThinkIndex = idx;
        }
      });

      // 找到最后一个 TEXT 分片，视为“最终总结回答”
      let lastTextIndex = -1;
      content.forEach((part, idx) => {
        if (!part) return;
        if (part.type === ContentTypes.TEXT) {
          lastTextIndex = idx;
        }
      });

      return content.filter((part, idx) => {
        if (!part) return false;

        // 不在 chat 中展示 THINK（思维链）内容
        if (part.type === ContentTypes.THINK) {
          return false;
        }

        // 只保留：
        // 1）在第一个工具调用 / THINK 之前的 TEXT（模型先说的一段正常话）
        // 2）最后一段 TEXT（工具调用完成后的总结回答）
        if (part.type === ContentTypes.TEXT) {
          if (firstToolOrThinkIndex === -1) {
            // 没有工具调用 / THINK，保留所有文本
            return true;
          }
          const isBeforeFirstTool = idx < firstToolOrThinkIndex;
          const isFinalSummary = idx === lastTextIndex;
          return isBeforeFirstTool || isFinalSummary;
        }

        if (part.type === ContentTypes.TOOL_CALL) {
          const toolCall = part[ContentTypes.TOOL_CALL] as Agents.ToolCall | undefined;
          const toolOutput = toolCall?.output;
          if (typeof toolOutput === 'string' && extractChartDataFromToolOutput(toolOutput) !== null) {
            return true;
          }
          return false;
        }

        // 其他类型的 part 保持不变
        return true;

        // 其他类型（IMAGE_FILE 等）保持不变
        return true;
      });
    }, [content, isCreatedByUser]);
    if (edit === true && enterEdit && setSiblingIdx) {
      return (
        <>
          {content.map((part, idx) => {
            if (!part) {
              return null;
            }
            const isTextPart =
              part?.type === ContentTypes.TEXT ||
              (typeof (part as unknown as Agents.MessageContentText)?.text === 'string' &&
                part?.type !== ContentTypes.THINK);
            const isThinkPart =
              part?.type === ContentTypes.THINK ||
              typeof (part as unknown as Agents.ReasoningDeltaUpdate)?.think === 'string';
            if (!isTextPart && !isThinkPart) {
              return null;
            }

            const isToolCall =
              part.type === ContentTypes.TOOL_CALL || part['tool_call_ids'] != null;
            if (isToolCall) {
              return null;
            }

            return (
              <EditTextPart
                index={idx}
                part={part as Agents.MessageContentText | Agents.ReasoningDeltaUpdate}
                messageId={messageId}
                isSubmitting={isSubmitting}
                enterEdit={enterEdit}
                siblingIdx={siblingIdx ?? null}
                setSiblingIdx={setSiblingIdx}
                key={`edit-${messageId}-${idx}`}
              />
            );
          })}
        </>
      );
    }

    // 收集所有在 TEXT parts 中已经渲染的 tool_call_ids
    // 这些 attachments 不应该在消息末尾再次渲染
    const renderedToolCallIds = useMemo(() => {
      const ids = new Set<string>();
      filteredContent.forEach((part) => {
        if (part && part.type === ContentTypes.TEXT && part.tool_call_ids) {
          part.tool_call_ids.forEach((id) => ids.add(id));
        }
      });
      return ids;
    }, [filteredContent]);

    return (
      <>
        <SearchContext.Provider value={{ searchResults }}>
          <MemoryArtifacts attachments={attachments} />
          <Sources messageId={messageId} conversationId={conversationId || undefined} />
          {filteredContent.map((part, idx) => {
            if (!part) {
              return null;
            }

            const toolCallId =
              (part?.[ContentTypes.TOOL_CALL] as Agents.ToolCall | undefined)?.id ?? '';
            const partAttachments = attachmentMap[toolCallId];

            // 对于文本部分，只使用明确关联的 tool_call_ids 的 attachments
            let textAttachments = partAttachments;
            if (part.type === ContentTypes.TEXT) {
              if (!textAttachments || textAttachments.length === 0) {
                // 检查文本部分是否有关联的工具调用ID
                if (part.tool_call_ids && part.tool_call_ids.length > 0) {
                  // 收集所有 tool_call_ids 对应的 attachments
                  const allTextAttachments: TAttachment[] = [];
                  part.tool_call_ids.forEach((id) => {
                    const atts = attachmentMap[id];
                    if (atts && atts.length > 0) {
                      allTextAttachments.push(...atts);
                    }
                  });
                  if (allTextAttachments.length > 0) {
                    textAttachments = allTextAttachments;
                  }
                }
              }
            }


            return (
              <MessageContext.Provider
                key={`provider-${messageId}-${idx}`}
                value={{
                  messageId,
                  isExpanded: true,
                  conversationId,
                  partIndex: idx,
                  nextType: content[idx + 1]?.type,
                  isSubmitting: effectiveIsSubmitting,
                  isLatestMessage,
                }}
              >
                <Part
                  part={part}
                  attachments={textAttachments || partAttachments}
                  isSubmitting={effectiveIsSubmitting}
                  key={`part-${messageId}-${idx}`}
                  isCreatedByUser={isCreatedByUser}
                  isLast={idx === content.length - 1}
                  showCursor={idx === content.length - 1 && isLast}
                />
              </MessageContext.Provider>
            );
          })}
          {/* 在消息末尾统一渲染所有未关联的 ui_resources */}
          {!isCreatedByUser && (
            <MessageUIResources 
              attachments={attachments} 
              excludedToolCallIds={renderedToolCallIds}
            />
          )}
        </SearchContext.Provider>
      </>
    );
  },
);

export default ContentParts;
