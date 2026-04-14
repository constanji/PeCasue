/**
 * 工具调用提取工具
 * 用于从消息中提取工具调用信息
 */

import { ContentTypes } from '@because/data-provider';

export interface ToolCallInfo {
  name: string;
  args: string | Record<string, unknown>;
  output?: string | null;
  id?: string;
  progress?: number;
  auth?: string;
  expires_at?: number;
}

export interface ToolCallWithThoughtChain {
  thoughtChain: null; // 已移除 dat-server 思维链支持
  toolCall: ToolCallInfo;
}

/**
 * 消息内容项 - 可以是文本或工具调用
 */
export interface MessageContentItem {
  type: 'text' | 'toolCall';
  text?: string; // 文本内容
  toolCall?: ToolCallWithThoughtChain; // 工具调用
  index: number; // 在消息中的原始索引
}

/**
 * 按消息分组的工具调用数据
 * 用于支持多轮对话的工具调用展示
 */
export interface MessageToolCalls {
  messageId: string;
  messageIndex: number; // 消息在对话中的序号（从1开始）
  toolCalls: ToolCallWithThoughtChain[];
  contentItems: MessageContentItem[]; // 按顺序的内容项（文本和工具调用交替）
  isStreaming?: boolean; // 是否正在流式输出
  textContent?: string; // LLM 的文本回复内容（已废弃，使用 contentItems）
}

/**
 * 从消息中提取所有工具调用
 * 返回按时间顺序排列的数组（从旧到新）
 */
export function extractAllToolCalls(messages: any[]): ToolCallWithThoughtChain[] {
  if (!messages || messages.length === 0) {
    return [];
  }

  const result: ToolCallWithThoughtChain[] = [];

  // 从旧到新遍历所有消息，收集所有工具调用
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    if (!message || !message.content || !Array.isArray(message.content)) {
      continue;
    }

    // 遍历消息内容，查找工具调用
    for (const part of message.content) {
      if (!part || part.type !== ContentTypes.TOOL_CALL) {
        continue;
      }

      // 工具调用数据存储在 part[ContentTypes.TOOL_CALL] 中
      const toolCall = part[ContentTypes.TOOL_CALL] || part.tool_call;
      if (!toolCall) {
        continue;
      }

      // 检查工具调用的格式，可能是 function 属性或直接包含 name
      const functionData = toolCall.function || toolCall;
      if (!functionData || !functionData.name) {
        continue;
      }

      const toolName = functionData.name || '';
      const output = functionData.output || toolCall.output;

      // 提取工具调用信息
      const toolCallInfo: ToolCallInfo = {
        name: toolName,
        args: functionData.arguments || toolCall.args || '',
        output: output,
        id: toolCall.id || functionData.id,
        progress: toolCall.progress,
        auth: toolCall.auth,
        expires_at: toolCall.expires_at,
      };

      // 添加工具调用，即使参数还在流式传输中也要显示
      // 这样可以实现实时展示流式传输的参数
      result.push({
        thoughtChain: null,
        toolCall: toolCallInfo,
      });
    }
  }

  return result;
}

/**
 * 从消息中提取工具调用，按消息分组
 * 支持多轮对话的工具调用展示
 */
export function extractToolCallsByMessage(messages: any[]): MessageToolCalls[] {
  if (!messages || messages.length === 0) {
    return [];
  }

  const result: MessageToolCalls[] = [];
  let roundIndex = 0;

  // 遍历所有消息
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    if (!message || !message.content || !Array.isArray(message.content)) {
      continue;
    }

    // 只处理 assistant 消息（非用户消息）
    const isAssistantMessage = message.isCreatedByUser === false;
    if (!isAssistantMessage) {
      continue;
    }

    const messageToolCalls: ToolCallWithThoughtChain[] = [];
    const contentItems: MessageContentItem[] = [];
    let hasToolCall = false;
    let textContent = '';
    let lastToolCallIndex = -1; // 记录最后一个工具调用的索引

    // 遍历消息内容，按照顺序提取文本和工具调用
    for (let partIndex = 0; partIndex < message.content.length; partIndex++) {
      const part = message.content[partIndex];
      if (!part) {
        continue;
      }

      // 提取文本内容（只提取 TEXT 类型，不包括 THINK 类型）
      if (part.type === ContentTypes.TEXT && 'text' in part) {
        const textValue = typeof part.text === 'string' ? part.text : part.text?.value || '';
        if (textValue.trim()) {
          textContent += (textContent ? '\n\n' : '') + textValue.trim();
          // 添加到内容项列表
          contentItems.push({
            type: 'text',
            text: textValue.trim(),
            index: partIndex,
          });
        }
      }

      // 提取工具调用
      if (part.type === ContentTypes.TOOL_CALL) {
        hasToolCall = true;
        lastToolCallIndex = partIndex;
        const toolCall = part[ContentTypes.TOOL_CALL] || part.tool_call;
        if (!toolCall) {
          continue;
        }

        const functionData = toolCall.function || toolCall;
        if (!functionData || !functionData.name) {
          continue;
        }

        const toolName = functionData.name || '';
        const output = functionData.output || toolCall.output;

        const toolCallInfo: ToolCallInfo = {
          name: toolName,
          args: functionData.arguments || toolCall.args || '',
          output: output,
          id: toolCall.id || functionData.id,
          progress: toolCall.progress,
          auth: toolCall.auth,
          expires_at: toolCall.expires_at,
        };

        const toolCallWithChain: ToolCallWithThoughtChain = {
          thoughtChain: null,
          toolCall: toolCallInfo,
        };

        // 添加到工具调用列表
        messageToolCalls.push(toolCallWithChain);

        // 添加到内容项列表
        contentItems.push({
          type: 'toolCall',
          toolCall: toolCallWithChain,
          index: partIndex,
        });
      }
    }

    // 如果该消息包含工具调用或文本内容，添加到结果中
    if (hasToolCall || contentItems.length > 0) {
      roundIndex++;
      
      // 检查是否有任何工具调用还在处理中（output 为空或 progress < 1）
      const isStreaming =
        message.unfinished === true ||
        messageToolCalls.some(
          (tc) =>
            tc.toolCall.output == null ||
            tc.toolCall.output === '' ||
            (tc.toolCall.progress != null && tc.toolCall.progress < 1),
        );

      // 过滤内容项：移除最后一个工具调用之后的所有文本内容
      // 只保留最后一个工具调用之前或同时的内容
      const filteredContentItems = contentItems.filter((item) => {
        if (item.type === 'text') {
          // 如果这个文本项在最后一个工具调用之后，则排除（这是最终结果，不显示在工具调用中）
          if (lastToolCallIndex >= 0 && item.index > lastToolCallIndex) {
            return false;
          }
        }
        return true;
      });

      result.push({
        messageId: message.messageId || `msg-${i}`,
        messageIndex: roundIndex,
        toolCalls: messageToolCalls,
        contentItems: filteredContentItems,
        isStreaming,
        textContent: textContent.trim() || undefined, // 保留用于向后兼容
      });
    }
  }

  return result;
}
