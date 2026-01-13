import { ToolCall } from '@langchain/core/messages/tool';
import {
  ToolMessage,
  isAIMessage,
  isBaseMessage,
} from '@langchain/core/messages';
import {
  END,
  Send,
  Command,
  isCommand,
  isGraphInterrupt,
  MessagesAnnotation,
} from '@langchain/langgraph';
import type {
  RunnableConfig,
  RunnableToolLike,
} from '@langchain/core/runnables';
import type { BaseMessage, AIMessage } from '@langchain/core/messages';
import type { StructuredToolInterface } from '@langchain/core/tools';
import type * as t from '@/types';
import { RunnableCallable } from '@/utils';
import { Constants } from '@/common';

/**
 * Helper to check if a value is a Send object
 */
function isSend(value: unknown): value is Send {
  return value instanceof Send;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class ToolNode<T = any> extends RunnableCallable<T, T> {
  private toolMap: Map<string, StructuredToolInterface | RunnableToolLike>;
  private loadRuntimeTools?: t.ToolRefGenerator;
  handleToolErrors = true;
  trace = false;
  toolCallStepIds?: Map<string, string>;
  errorHandler?: t.ToolNodeConstructorParams['errorHandler'];
  private toolUsageCount: Map<string, number>;
  /** Tool registry for filtering (lazy computation of programmatic maps) */
  private toolRegistry?: t.LCToolRegistry;
  /** Cached programmatic tools (computed once on first PTC call) */
  private programmaticCache?: t.ProgrammaticCache;

  constructor({
    tools,
    toolMap,
    name,
    tags,
    errorHandler,
    toolCallStepIds,
    handleToolErrors,
    loadRuntimeTools,
    toolRegistry,
  }: t.ToolNodeConstructorParams) {
    super({ name, tags, func: (input, config) => this.run(input, config) });
    this.toolMap = toolMap ?? new Map(tools.map((tool) => [tool.name, tool]));
    this.toolCallStepIds = toolCallStepIds;
    this.handleToolErrors = handleToolErrors ?? this.handleToolErrors;
    this.loadRuntimeTools = loadRuntimeTools;
    this.errorHandler = errorHandler;
    this.toolUsageCount = new Map<string, number>();
    this.toolRegistry = toolRegistry;
  }

  /**
   * Returns cached programmatic tools, computing once on first access.
   * Single iteration builds both toolMap and toolDefs simultaneously.
   */
  private getProgrammaticTools(): { toolMap: t.ToolMap; toolDefs: t.LCTool[] } {
    if (this.programmaticCache) return this.programmaticCache;

    const toolMap: t.ToolMap = new Map();
    const toolDefs: t.LCTool[] = [];

    if (this.toolRegistry) {
      for (const [name, toolDef] of this.toolRegistry) {
        if (
          (toolDef.allowed_callers ?? ['direct']).includes('code_execution')
        ) {
          toolDefs.push(toolDef);
          const tool = this.toolMap.get(name);
          if (tool) toolMap.set(name, tool);
        }
      }
    }

    this.programmaticCache = { toolMap, toolDefs };
    return this.programmaticCache;
  }

  /**
   * Returns a snapshot of the current tool usage counts.
   * @returns A ReadonlyMap where keys are tool names and values are their usage counts.
   */
  public getToolUsageCounts(): ReadonlyMap<string, number> {
    return new Map(this.toolUsageCount); // Return a copy
  }

  /**
   * Runs a single tool call with error handling
   */
  protected async runTool(
    call: ToolCall,
    config: RunnableConfig
  ): Promise<BaseMessage | Command> {
    const tool = this.toolMap.get(call.name);
    try {
      if (tool === undefined) {
        throw new Error(`Tool "${call.name}" not found.`);
      }
      const turn = this.toolUsageCount.get(call.name) ?? 0;
      this.toolUsageCount.set(call.name, turn + 1);
      const args = call.args;
      const stepId = this.toolCallStepIds?.get(call.id!);

      // Build invoke params - LangChain extracts non-schema fields to config.toolCall
      let invokeParams: Record<string, unknown> = {
        ...call,
        args,
        type: 'tool_call',
        stepId,
        turn,
      };

      // Inject runtime data for special tools (becomes available at config.toolCall)
      if (call.name === Constants.PROGRAMMATIC_TOOL_CALLING) {
        const { toolMap, toolDefs } = this.getProgrammaticTools();
        invokeParams = {
          ...invokeParams,
          toolMap,
          toolDefs,
        };
      } else if (call.name === Constants.TOOL_SEARCH_REGEX) {
        invokeParams = {
          ...invokeParams,
          toolRegistry: this.toolRegistry,
        };
      }

      const output = await tool.invoke(invokeParams, config);
      if (
        (isBaseMessage(output) && output._getType() === 'tool') ||
        isCommand(output)
      ) {
        return output;
      } else {
        // 检查 output 是否是包含 artifact 的对象
        let content: string;
        let artifact: any = undefined;
        
        if (output && typeof output === 'object' && !Array.isArray(output)) {
          if ('artifact' in output) {
            artifact = output.artifact;
            content = typeof output.content === 'string' ? output.content : JSON.stringify(output.content || output);

            // 如果输出或artifact中还有 _chartData，将其合并到 artifact 中
            if (('_chartData' in output && output._chartData) ||
                (artifact && '_chartData' in artifact && artifact._chartData)) {
              artifact = artifact || {};
              const chartData = output._chartData || artifact._chartData;
              artifact._chartData = chartData;
              console.log('[ToolNode] Merged _chartData into artifact:', {
                chartId: chartData.chartId,
                title: chartData.title,
                hasData: !!chartData.data,
                hasLayout: !!chartData.layout
              });
            }

            // 调试日志：检查 artifact 内容
            if (artifact && typeof artifact === 'object') {
              // 使用 console.log 输出到服务器日志（Node.js 环境）
              console.log('[ToolNode] Artifact detected:', {
                keys: Object.keys(artifact),
                hasUiResources: !!(artifact as any).ui_resources,
                hasMemory: !!(artifact as any).memory,
                hasChartData: !!(artifact as any)._chartData,
                toolName: tool.name,
                toolCallId: call.id,
              });
            }
          } else if ('content' in output) {
            content = typeof output.content === 'string' ? output.content : JSON.stringify(output.content);
          } else {
            content = JSON.stringify(output);
          }
        } else {
          content = typeof output === 'string' ? output : JSON.stringify(output);
        }
        
        const toolMessage = new ToolMessage({
          status: 'success',
          name: tool.name,
          content,
          tool_call_id: call.id!,
          ...(artifact && { artifact }),
        });
        
        // 调试日志：确认 ToolMessage 的 artifact（输出到服务器日志）
        const logger = require('@because/data-schemas').logger;
        if (artifact) {
          logger.info('[ToolNode] ========== Created ToolMessage with artifact ==========', {
            toolName: tool.name,
            toolCallId: call.id,
            hasArtifact: !!(toolMessage as any).artifact,
            artifactKeys: (toolMessage as any).artifact ? Object.keys((toolMessage as any).artifact) : [],
            uiResourcesKeys: (toolMessage as any).artifact?.ui_resources ? Object.keys((toolMessage as any).artifact.ui_resources) : null,
            hasUiResourcesData: !!(toolMessage as any).artifact?.ui_resources?.data,
            uiResourcesDataLength: (toolMessage as any).artifact?.ui_resources?.data?.length || 0,
          });
        } else {
          logger.info('[ToolNode] ========== No artifact in tool output ==========', {
            toolName: tool.name,
            toolCallId: call.id,
            outputType: typeof output,
            hasOutput: !!output,
          });
        }
        
        return toolMessage;
      }
    } catch (_e: unknown) {
      const e = _e as Error;
      if (!this.handleToolErrors) {
        throw e;
      }
      if (isGraphInterrupt(e)) {
        throw e;
      }
      if (this.errorHandler) {
        try {
          await this.errorHandler(
            {
              error: e,
              id: call.id!,
              name: call.name,
              input: call.args,
            },
            config.metadata
          );
        } catch (handlerError) {
          // eslint-disable-next-line no-console
          console.error('Error in errorHandler:', {
            toolName: call.name,
            toolCallId: call.id,
            toolArgs: call.args,
            stepId: this.toolCallStepIds?.get(call.id!),
            turn: this.toolUsageCount.get(call.name),
            originalError: {
              message: e.message,
              stack: e.stack ?? undefined,
            },
            handlerError:
              handlerError instanceof Error
                ? {
                  message: handlerError.message,
                  stack: handlerError.stack ?? undefined,
                }
                : {
                  message: String(handlerError),
                  stack: undefined,
                },
          });
        }
      }
      return new ToolMessage({
        status: 'error',
        content: `Error: ${e.message}\n Please fix your mistakes.`,
        name: call.name,
        tool_call_id: call.id ?? '',
      });
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected async run(input: any, config: RunnableConfig): Promise<T> {
    let outputs: (BaseMessage | Command)[];

    if (this.isSendInput(input)) {
      outputs = [await this.runTool(input.lg_tool_call, config)];
    } else {
      let messages: BaseMessage[];
      if (Array.isArray(input)) {
        messages = input;
      } else if (this.isMessagesState(input)) {
        messages = input.messages;
      } else {
        throw new Error(
          'ToolNode only accepts BaseMessage[] or { messages: BaseMessage[] } as input.'
        );
      }

      const toolMessageIds: Set<string> = new Set(
        messages
          .filter((msg) => msg._getType() === 'tool')
          .map((msg) => (msg as ToolMessage).tool_call_id)
      );

      let aiMessage: AIMessage | undefined;
      for (let i = messages.length - 1; i >= 0; i--) {
        const message = messages[i];
        if (isAIMessage(message)) {
          aiMessage = message;
          break;
        }
      }

      if (aiMessage == null || !isAIMessage(aiMessage)) {
        throw new Error('ToolNode only accepts AIMessages as input.');
      }

      if (this.loadRuntimeTools) {
        const { tools, toolMap } = this.loadRuntimeTools(
          aiMessage.tool_calls ?? []
        );
        this.toolMap =
          toolMap ?? new Map(tools.map((tool) => [tool.name, tool]));
        this.programmaticCache = undefined; // Invalidate cache on toolMap change
      }

      outputs = await Promise.all(
        aiMessage.tool_calls
          ?.filter((call) => {
            /**
             * Filter out:
             * 1. Already processed tool calls (present in toolMessageIds)
             * 2. Server tool calls (e.g., web_search with IDs starting with 'srvtoolu_')
             *    which are executed by the provider's API and don't require invocation
             */
            return (
              (call.id == null || !toolMessageIds.has(call.id)) &&
              !(call.id?.startsWith('srvtoolu_') ?? false)
            );
          })
          .map((call) => this.runTool(call, config)) ?? []
      );
    }

    if (!outputs.some(isCommand)) {
      return (Array.isArray(input) ? outputs : { messages: outputs }) as T;
    }

    const combinedOutputs: (
      | { messages: BaseMessage[] }
      | BaseMessage[]
      | Command
    )[] = [];
    let parentCommand: Command | null = null;

    for (const output of outputs) {
      if (isCommand(output)) {
        if (
          output.graph === Command.PARENT &&
          Array.isArray(output.goto) &&
          output.goto.every((send): send is Send => isSend(send))
        ) {
          if (parentCommand) {
            (parentCommand.goto as Send[]).push(...(output.goto as Send[]));
          } else {
            parentCommand = new Command({
              graph: Command.PARENT,
              goto: output.goto,
            });
          }
        } else {
          combinedOutputs.push(output);
        }
      } else {
        combinedOutputs.push(
          Array.isArray(input) ? [output] : { messages: [output] }
        );
      }
    }

    if (parentCommand) {
      combinedOutputs.push(parentCommand);
    }

    return combinedOutputs as T;
  }

  private isSendInput(input: unknown): input is { lg_tool_call: ToolCall } {
    return (
      typeof input === 'object' && input != null && 'lg_tool_call' in input
    );
  }

  private isMessagesState(
    input: unknown
  ): input is { messages: BaseMessage[] } {
    return (
      typeof input === 'object' &&
      input != null &&
      'messages' in input &&
      Array.isArray((input as { messages: unknown }).messages) &&
      (input as { messages: unknown[] }).messages.every(isBaseMessage)
    );
  }
}

function areToolCallsInvoked(
  message: AIMessage,
  invokedToolIds?: Set<string>
): boolean {
  if (!invokedToolIds || invokedToolIds.size === 0) return false;
  return (
    message.tool_calls?.every(
      (toolCall) => toolCall.id != null && invokedToolIds.has(toolCall.id)
    ) ?? false
  );
}

export function toolsCondition<T extends string>(
  state: BaseMessage[] | typeof MessagesAnnotation.State,
  toolNode: T,
  invokedToolIds?: Set<string>
): T | typeof END {
  const message: AIMessage = Array.isArray(state)
    ? state[state.length - 1]
    : state.messages[state.messages.length - 1];

  if (
    'tool_calls' in message &&
    (message.tool_calls?.length ?? 0) > 0 &&
    !areToolCallsInvoked(message, invokedToolIds)
  ) {
    return toolNode;
  } else {
    return END;
  }
}
