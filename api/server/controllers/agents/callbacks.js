const { nanoid } = require('nanoid');
const { sendEvent } = require('@because/api');
const { logger } = require('@because/data-schemas');
const { Tools, StepTypes, FileContext, ErrorTypes } = require('@because/data-provider');
const {
  EnvVar,
  Providers,
  GraphEvents,
  getMessageId,
  ToolEndHandler,
  handleToolCalls,
  ChatModelStreamHandler,
} = require('@because/agents');
const { processFileCitations } = require('~/server/services/Files/Citations');
const { processCodeOutput } = require('~/server/services/Files/Code/process');
const { loadAuthValues } = require('~/server/services/Tools/credentials');
const { saveBase64Image } = require('~/server/services/Files/process');

class ModelEndHandler {
  /**
   * @param {Array<UsageMetadata>} collectedUsage
   */
  constructor(collectedUsage) {
    if (!Array.isArray(collectedUsage)) {
      throw new Error('collectedUsage must be an array');
    }
    this.collectedUsage = collectedUsage;
  }

  finalize(errorMessage) {
    if (!errorMessage) {
      return;
    }
    throw new Error(errorMessage);
  }

  /**
   * @param {string} event
   * @param {ModelEndData | undefined} data
   * @param {Record<string, unknown> | undefined} metadata
   * @param {StandardGraph} graph
   * @returns {Promise<void>}
   */
  async handle(event, data, metadata, graph) {
    if (!graph || !metadata) {
      console.warn(`Graph or metadata not found in ${event} event`);
      return;
    }

    /** @type {string | undefined} */
    let errorMessage;
    try {
      const agentContext = graph.getAgentContext(metadata);
      const isGoogle = agentContext.provider === Providers.GOOGLE;
      const streamingDisabled = !!agentContext.clientOptions?.disableStreaming;
      if (data?.output?.additional_kwargs?.stop_reason === 'refusal') {
        const info = { ...data.output.additional_kwargs };
        errorMessage = JSON.stringify({
          type: ErrorTypes.REFUSAL,
          info,
        });
        logger.debug(`[ModelEndHandler] Model refused to respond`, {
          ...info,
          userId: metadata.user_id,
          messageId: metadata.run_id,
          conversationId: metadata.thread_id,
        });
      }

      const toolCalls = data?.output?.tool_calls;
      
      // 详细记录LLM返回的tool calls（特别是speckit工具）
      if (toolCalls && Array.isArray(toolCalls) && toolCalls.length > 0) {
        const speckitCalls = toolCalls.filter(tc => tc.function?.name === 'speckit');
        
        if (speckitCalls.length > 0) {
          logger.info('[Agent-LLM交互] ========== LLM返回工具调用请求 ==========');
          const speckitInfo = {
            totalToolCalls: toolCalls.length,
            speckitCallsCount: speckitCalls.length,
            threadId: metadata.thread_id,
            runId: metadata.run_id,
            userId: metadata.user_id,
            timestamp: new Date().toISOString(),
          };
          logger.info(`[Agent-LLM交互] LLM决定调用speckit工具: ${JSON.stringify(speckitInfo, null, 2)}`);
          
          speckitCalls.forEach((tc, index) => {
            const parsedArgs = (() => {
              try {
                return typeof tc.function?.arguments === 'string' 
                  ? JSON.parse(tc.function?.arguments)
                  : tc.function?.arguments;
              } catch {
                return tc.function?.arguments;
              }
            })();
            const toolCallInfo = {
              toolCallId: tc.id,
              functionName: tc.function?.name,
              rawArguments: tc.function?.arguments,
              argumentsType: typeof tc.function?.arguments,
              parsedArguments: parsedArgs,
              // 记录完整的tool call对象，包括LLM生成的所有字段
              fullToolCall: {
                id: tc.id,
                type: tc.type,
                function: {
                  name: tc.function?.name,
                  arguments: tc.function?.arguments,
                },
              },
            };
            logger.info(`[Agent-LLM交互] Speckit工具调用 #${index + 1}: ${JSON.stringify(toolCallInfo, null, 2)}`);
            
            // 特别记录arguments的生成过程
            logger.info(`[Agent-LLM交互] Arguments生成详情 #${index + 1}:`, {
              step: 'LLM原始响应',
              toolCallId: tc.id,
              rawArgumentsString: tc.function?.arguments,
              argumentsLength: typeof tc.function?.arguments === 'string' ? tc.function?.arguments.length : 0,
              isJSON: (() => {
                try {
                  JSON.parse(tc.function?.arguments || '');
                  return true;
                } catch {
                  return false;
                }
              })(),
              parsedResult: parsedArgs,
            });
          });
          
          // 记录LLM的完整响应上下文（帮助理解LLM为什么生成这些arguments）
          logger.info(`[Agent-LLM交互] LLM完整响应上下文: ${JSON.stringify({
            totalToolCalls: toolCalls.length,
            allToolCalls: toolCalls.map(tc => ({
              id: tc.id,
              type: tc.type,
              function: {
                name: tc.function?.name,
                arguments: tc.function?.arguments,
              },
            })),
            modelResponse: {
              content: data?.output?.content,
              stopReason: data?.output?.stop_reason,
            },
          }, null, 2)}`);
          
          logger.info('[Agent-LLM交互] ========== 准备执行工具调用 ==========');
        }
        
        // Debug: Log raw tool calls from ModelScope API response
        logger.debug('[ModelEndHandler] Raw tool calls from API response:', {
          toolCallsCount: toolCalls.length,
          toolCalls: toolCalls.map((tc) => ({
            id: tc.id,
            type: tc.type,
            functionName: tc.function?.name,
            functionArguments: tc.function?.arguments,
            argumentsType: typeof tc.function?.arguments,
            argumentsIsEmpty: tc.function?.arguments === '' || tc.function?.arguments === null || tc.function?.arguments === undefined,
          })),
        });
      }
      
      let hasUnprocessedToolCalls = false;
      if (Array.isArray(toolCalls) && toolCalls.length > 0 && graph?.toolCallStepIds?.has) {
        try {
          hasUnprocessedToolCalls = toolCalls.some(
            (tc) => tc?.id && !graph.toolCallStepIds.has(tc.id),
          );
        } catch {
          hasUnprocessedToolCalls = false;
        }
      }
      if (isGoogle || streamingDisabled || hasUnprocessedToolCalls) {
        await handleToolCalls(toolCalls, metadata, graph);
      }

      const usage = data?.output?.usage_metadata;
      if (!usage) {
        return this.finalize(errorMessage);
      }
      const modelName = metadata?.ls_model_name || agentContext.clientOptions?.model;
      if (modelName) {
        usage.model = modelName;
      }

      this.collectedUsage.push(usage);
      if (!streamingDisabled) {
        return this.finalize(errorMessage);
      }
      if (!data.output.content) {
        return this.finalize(errorMessage);
      }
      const stepKey = graph.getStepKey(metadata);
      const message_id = getMessageId(stepKey, graph) ?? '';
      if (message_id) {
        await graph.dispatchRunStep(stepKey, {
          type: StepTypes.MESSAGE_CREATION,
          message_creation: {
            message_id,
          },
        });
      }
      const stepId = graph.getStepIdByKey(stepKey);
      const content = data.output.content;
      if (typeof content === 'string') {
        await graph.dispatchMessageDelta(stepId, {
          content: [
            {
              type: 'text',
              text: content,
            },
          ],
        });
      } else if (content.every((c) => c.type?.startsWith('text'))) {
        await graph.dispatchMessageDelta(stepId, {
          content,
        });
      }
    } catch (error) {
      logger.error('Error handling model end event:', error);
      return this.finalize(errorMessage);
    }
  }
}

/**
 * @deprecated Agent Chain helper
 * @param {string | undefined} [last_agent_id]
 * @param {string | undefined} [langgraph_node]
 * @returns {boolean}
 */
function checkIfLastAgent(last_agent_id, langgraph_node) {
  if (!last_agent_id || !langgraph_node) {
    return false;
  }
  return langgraph_node?.endsWith(last_agent_id);
}

/**
 * Get default handlers for stream events.
 * @param {Object} options - The options object.
 * @param {ServerResponse} options.res - The options object.
 * @param {ContentAggregator} options.aggregateContent - The options object.
 * @param {ToolEndCallback} options.toolEndCallback - Callback to use when tool ends.
 * @param {Array<UsageMetadata>} options.collectedUsage - The list of collected usage metadata.
 * @returns {Record<string, t.EventHandler>} The default handlers.
 * @throws {Error} If the request is not found.
 */
function getDefaultHandlers({ res, aggregateContent, toolEndCallback, collectedUsage }) {
  if (!res || !aggregateContent) {
    throw new Error(
      `[getDefaultHandlers] Missing required options: res: ${!res}, aggregateContent: ${!aggregateContent}`,
    );
  }
  const handlers = {
    [GraphEvents.CHAT_MODEL_END]: new ModelEndHandler(collectedUsage),
    [GraphEvents.TOOL_END]: new ToolEndHandler(toolEndCallback, logger),
    [GraphEvents.CHAT_MODEL_STREAM]: new ChatModelStreamHandler(),
    [GraphEvents.ON_RUN_STEP]: {
      /**
       * Handle ON_RUN_STEP event.
       * @param {string} event - The event name.
       * @param {StreamEventData} data - The event data.
       * @param {GraphRunnableConfig['configurable']} [metadata] The runnable metadata.
       */
      handle: (event, data, metadata) => {
        if (data?.stepDetails.type === StepTypes.TOOL_CALLS) {
          sendEvent(res, { event, data });
        } else if (checkIfLastAgent(metadata?.last_agent_id, metadata?.langgraph_node)) {
          sendEvent(res, { event, data });
        } else if (!metadata?.hide_sequential_outputs) {
          sendEvent(res, { event, data });
        } else {
          const agentName = metadata?.name ?? 'Agent';
          const isToolCall = data?.stepDetails.type === StepTypes.TOOL_CALLS;
          const action = isToolCall ? 'performing a task...' : 'thinking...';
          sendEvent(res, {
            event: 'on_agent_update',
            data: {
              runId: metadata?.run_id,
              message: `${agentName} is ${action}`,
            },
          });
        }
        aggregateContent({ event, data });
      },
    },
    [GraphEvents.ON_RUN_STEP_DELTA]: {
      /**
       * Handle ON_RUN_STEP_DELTA event.
       * @param {string} event - The event name.
       * @param {StreamEventData} data - The event data.
       * @param {GraphRunnableConfig['configurable']} [metadata] The runnable metadata.
       */
      handle: (event, data, metadata) => {
        // Filter out tool_call_chunks with null args (LangChain sends this as final chunk)
        // This prevents null from overwriting accumulated tool call arguments
        if (data?.delta.type === StepTypes.TOOL_CALLS && data?.delta.tool_calls) {
          const filteredToolCalls = data.delta.tool_calls.filter((tc) => {
            // Keep chunks that have non-null args, or chunks with other meaningful data
            return tc.args !== null || tc.name != null || tc.id != null;
          });
          
          // Only send event if there are valid tool calls after filtering
          if (filteredToolCalls.length > 0) {
            const filteredData = {
              ...data,
              delta: {
                ...data.delta,
                tool_calls: filteredToolCalls,
              },
            };
            sendEvent(res, { event, data: filteredData });
          }
          // Still aggregate the original data for internal processing
          aggregateContent({ event, data });
        } else if (checkIfLastAgent(metadata?.last_agent_id, metadata?.langgraph_node)) {
          sendEvent(res, { event, data });
          aggregateContent({ event, data });
        } else if (!metadata?.hide_sequential_outputs) {
          sendEvent(res, { event, data });
          aggregateContent({ event, data });
        } else {
          aggregateContent({ event, data });
        }
      },
    },
    [GraphEvents.ON_RUN_STEP_COMPLETED]: {
      /**
       * Handle ON_RUN_STEP_COMPLETED event.
       * @param {string} event - The event name.
       * @param {StreamEventData & { result: ToolEndData }} data - The event data.
       * @param {GraphRunnableConfig['configurable']} [metadata] The runnable metadata.
       */
      handle: (event, data, metadata) => {
        if (data?.result != null) {
          sendEvent(res, { event, data });
        } else if (checkIfLastAgent(metadata?.last_agent_id, metadata?.langgraph_node)) {
          sendEvent(res, { event, data });
        } else if (!metadata?.hide_sequential_outputs) {
          sendEvent(res, { event, data });
        }
        aggregateContent({ event, data });
      },
    },
    [GraphEvents.ON_MESSAGE_DELTA]: {
      /**
       * Handle ON_MESSAGE_DELTA event.
       * @param {string} event - The event name.
       * @param {StreamEventData} data - The event data.
       * @param {GraphRunnableConfig['configurable']} [metadata] The runnable metadata.
       */
      handle: (event, data, metadata) => {
        if (checkIfLastAgent(metadata?.last_agent_id, metadata?.langgraph_node)) {
          sendEvent(res, { event, data });
        } else if (!metadata?.hide_sequential_outputs) {
          sendEvent(res, { event, data });
        }
        aggregateContent({ event, data });
      },
    },
    [GraphEvents.ON_REASONING_DELTA]: {
      /**
       * Handle ON_REASONING_DELTA event.
       * @param {string} event - The event name.
       * @param {StreamEventData} data - The event data.
       * @param {GraphRunnableConfig['configurable']} [metadata] The runnable metadata.
       */
      handle: (event, data, metadata) => {
        if (checkIfLastAgent(metadata?.last_agent_id, metadata?.langgraph_node)) {
          sendEvent(res, { event, data });
        } else if (!metadata?.hide_sequential_outputs) {
          sendEvent(res, { event, data });
        }
        aggregateContent({ event, data });
      },
    },
  };

  return handlers;
}

/**
 *
 * @param {Object} params
 * @param {ServerRequest} params.req
 * @param {ServerResponse} params.res
 * @param {Promise<MongoFile | { filename: string; filepath: string; expires: number;} | null>[]} params.artifactPromises
 * @returns {ToolEndCallback} The tool end callback.
 */
function createToolEndCallback({ req, res, artifactPromises }) {
  /**
   * @type {ToolEndCallback}
   */
  return async (data, metadata) => {
    const output = data?.output;
    if (!output) {
      logger.debug('[createToolEndCallback] No output in data');
      return;
    }

    // 调试日志：检查 output 的完整结构
    logger.info('[createToolEndCallback] ========== Tool End Callback Called ==========');
    logger.info('[createToolEndCallback] Output type:', typeof output);
    logger.info('[createToolEndCallback] Output keys:', output && typeof output === 'object' ? Object.keys(output) : []);
    logger.info('[createToolEndCallback] Output is ToolMessage:', output && typeof output === 'object' && 'tool_call_id' in output);
    
    // 详细检查 output 对象
    if (output && typeof output === 'object') {
      logger.info('[createToolEndCallback] Output object details:', {
        hasArtifact: 'artifact' in output,
        hasLc: 'lc' in output,
        hasKwargs: 'kwargs' in output,
        outputKeys: Object.keys(output),
        artifactType: typeof output.artifact,
        toolCallId: output.tool_call_id,
        name: output.name,
      });

      // 检查 LangChain ToolMessage 的结构
      if ('kwargs' in output && output.kwargs) {
        logger.info('[createToolEndCallback] kwargs details:', {
          hasArtifact: 'artifact' in output.kwargs,
          artifactType: typeof output.kwargs.artifact,
          kwargsKeys: Object.keys(output.kwargs),
        });
      }
    }

    // 从 ToolMessage 中获取 tool_call_id
    // ToolMessage 应该有 tool_call_id 属性（LangChain 标准）
    let toolCallId = output.tool_call_id || output.toolCallId;
    
    // 如果仍然没有，尝试从其他位置获取
    if (!toolCallId && typeof output === 'object') {
      const possibleKeys = ['tool_call_id', 'toolCallId', 'id'];
      for (const key of possibleKeys) {
        if (output[key]) {
          toolCallId = output[key];
          logger.debug(`[createToolEndCallback] Found toolCallId in key "${key}":`, toolCallId);
          break;
        }
      }
    }
    
    if (!toolCallId && metadata?.tool_call_id) {
      toolCallId = metadata.tool_call_id;
      logger.debug('[createToolEndCallback] Found toolCallId in metadata:', toolCallId);
    }
    
    if (!toolCallId && data?.input?.tool_call_id) {
      toolCallId = data.input.tool_call_id;
      logger.debug('[createToolEndCallback] Found toolCallId in data.input:', toolCallId);
    }

    logger.info(`[createToolEndCallback] Final toolCallId: ${toolCallId || 'NOT FOUND'}`);

    // 处理 LangChain ToolMessage 的特殊结构
    logger.info('[createToolEndCallback] Checking for artifact...');
    logger.info('[createToolEndCallback] output.artifact exists:', !!output.artifact);
    logger.info('[createToolEndCallback] output.kwargs exists:', !!output.kwargs);
    logger.info('[createToolEndCallback] output.kwargs.artifact exists:', !!(output.kwargs && output.kwargs.artifact));

    let artifact = output.artifact;
    if (!artifact && output.kwargs && output.kwargs.artifact) {
      artifact = output.kwargs.artifact;
      logger.info('[createToolEndCallback] Found artifact in kwargs:', {
        artifactType: typeof artifact,
        artifactKeys: Object.keys(artifact),
        hasUiResources: !!(artifact && artifact.ui_resources),
      });
    }

    // 如果仍然没有artifact，尝试从content中解析（ToolService.js的逻辑）
    if (!artifact && output.content && typeof output.content === 'string') {
      logger.info('[createToolEndCallback] Attempting to parse artifact from content...');
      try {
        const parsed = JSON.parse(output.content);
        if (parsed && typeof parsed === 'object') {
          // 优先处理：如果解析出的对象是完整的工具返回结果（content + artifact）
          if (parsed.artifact) {
            artifact = parsed.artifact;
            logger.info('[createToolEndCallback] Found artifact in tool result:', {
              artifactType: typeof artifact,
              artifactKeys: Object.keys(artifact),
              hasUiResources: !!(artifact && artifact.ui_resources),
              hasChartData: !!(artifact && artifact._chartData),
            });
          }
          // 如果解析出的对象直接就是artifact结构
          else if (parsed.ui_resources || parsed._chartData) {
            artifact = parsed;
            logger.info('[createToolEndCallback] Found direct artifact structure:', {
              hasUiResources: !!parsed.ui_resources,
              hasChartData: !!parsed._chartData,
              artifactKeys: Object.keys(parsed),
            });
          }
        }
      } catch (e) {
        logger.debug('[createToolEndCallback] Content is not valid JSON, trying alternative parsing...');

        // 如果JSON解析失败，可能是因为content包含了额外的文本
        // 尝试查找JSON对象（图表工具可能在JSON后附加了markdown标记）
        const jsonMatch = output.content.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
          try {
            const parsed = JSON.parse(jsonMatch[0]);
            if (parsed && typeof parsed === 'object' && parsed.artifact) {
              artifact = parsed.artifact;
              logger.info('[createToolEndCallback] Found artifact in partial JSON:', {
                artifactKeys: Object.keys(artifact),
                hasChartData: !!(artifact && artifact._chartData),
              });
            }
          } catch (e2) {
            logger.debug('[createToolEndCallback] Partial JSON parsing also failed');
          }
        }
      }
    }

    if (!artifact) {
      logger.warn('[createToolEndCallback] No artifact found in output (checked output.artifact, output.kwargs.artifact, and output.content)');
      logger.warn('[createToolEndCallback] output structure:', {
        hasArtifact: !!output.artifact,
        hasKwargs: !!output.kwargs,
        kwargsKeys: output.kwargs ? Object.keys(output.kwargs) : null,
        kwargsHasArtifact: !!(output.kwargs && output.kwargs.artifact),
        hasContent: !!output.content,
        contentType: typeof output.content,
      });
      return;
    }
    
    logger.info('[createToolEndCallback] Artifact detected!');
    logger.debug('[createToolEndCallback] Artifact keys:', artifact ? Object.keys(artifact) : 'null/undefined');
    logger.debug('[createToolEndCallback] Has ui_resources:', !!(artifact && artifact[Tools.ui_resources]));
    logger.debug('[createToolEndCallback] Has file_search:', !!(artifact && artifact[Tools.file_search]));
    logger.debug('[createToolEndCallback] Has memory:', !!(artifact && artifact[Tools.memory]));
    logger.debug('[createToolEndCallback] Has web_search:', !!(artifact && artifact[Tools.web_search]));
    
    if (artifact && artifact[Tools.ui_resources]) {
      logger.info('[createToolEndCallback] ui_resources artifact found!');
      logger.debug('[createToolEndCallback] ui_resources structure:', JSON.stringify(artifact[Tools.ui_resources], null, 2));
    }

    if (artifact && artifact[Tools.file_search]) {
      artifactPromises.push(
        (async () => {
          const user = req.user;
          const attachment = await processFileCitations({
            user,
            metadata,
            appConfig: req.config,
            toolArtifact: artifact,
            toolCallId: output.tool_call_id,
          });
          if (!attachment) {
            return null;
          }
          if (!res.headersSent) {
            return attachment;
          }
          res.write(`event: attachment\ndata: ${JSON.stringify(attachment)}\n\n`);
          return attachment;
        })().catch((error) => {
          logger.error('Error processing file citations:', error);
          return null;
        }),
      );
    }

    // TODO: a lot of duplicated code in createToolEndCallback
    // we should refactor this to use a helper function in a follow-up PR
    if (artifact && artifact[Tools.ui_resources]) {
      logger.info('[createToolEndCallback] ========== Processing ui_resources artifact ==========');
      logger.info('[createToolEndCallback] ui_resources data:', JSON.stringify(artifact[Tools.ui_resources], null, 2));
      logger.info('[createToolEndCallback] res.headersSent:', res.headersSent);
      logger.info('[createToolEndCallback] toolCallId:', toolCallId);
      logger.info('[createToolEndCallback] metadata.run_id:', metadata.run_id);
      logger.info('[createToolEndCallback] metadata.thread_id:', metadata.thread_id);
      
      const uiResourcesData = artifact[Tools.ui_resources];
      if (!uiResourcesData || !uiResourcesData.data) {
        logger.error('[createToolEndCallback] ui_resources.data is missing!', {
          uiResourcesData,
          hasData: !!uiResourcesData?.data,
        });
      }
      
      artifactPromises.push(
        (async () => {
          try {
          const attachment = {
            type: Tools.ui_resources,
            messageId: metadata.run_id,
            toolCallId: toolCallId || 'unknown',
            conversationId: metadata.thread_id,
              [Tools.ui_resources]: uiResourcesData.data,
              // 如果 artifact 中有 _chartData，也包含在 attachment 中
              ...(artifact && artifact._chartData && { _chartData: artifact._chartData }),
          };

          // 调试：检查_chartData是否存在
          const hasArtifact = !!artifact;
          const artifactKeys = artifact ? Object.keys(artifact) : [];
          const hasChartData = !!(artifact && artifact._chartData);
          const chartDataKeys = artifact && artifact._chartData ? Object.keys(artifact._chartData) : [];
          const attachmentHasChartData = !!attachment._chartData;
          
          logger.info(`[createToolEndCallback] _chartData check: hasArtifact=${hasArtifact}, artifactKeys=[${artifactKeys.join(',')}], hasChartData=${hasChartData}, chartDataKeys=[${chartDataKeys.join(',')}], attachmentHasChartData=${attachmentHasChartData}`);

          // 额外调试：如果有_chartData，输出详细信息
          if (artifact && artifact._chartData) {
            const chartData = artifact._chartData;
            logger.info(`[createToolEndCallback] _chartData details: chartId=${chartData.chartId}, title=${chartData.title}, hasData=${!!chartData.data}, hasLayout=${!!chartData.layout}, dataLength=${chartData.data ? chartData.data.length : 0}`);
          }
          
            logger.info('[createToolEndCallback] Created ui_resources attachment:', JSON.stringify(attachment, null, 2));
          
            // 无论 headers 是否已发送，都尝试立即发送 attachment
            // 如果 headers 已发送，直接写入 SSE 流
            // 如果 headers 未发送，返回 attachment 供后续处理
            if (res.headersSent) {
              logger.info('[createToolEndCallback] Headers already sent, writing attachment event immediately');
              try {
          res.write(`event: attachment\ndata: ${JSON.stringify(attachment)}\n\n`);
                logger.info('[createToolEndCallback] Attachment event written successfully');
              } catch (writeError) {
                logger.error('[createToolEndCallback] Error writing attachment event:', writeError);
                // 即使写入失败，也返回 attachment 供后续处理
                return attachment;
              }
            } else {
              logger.info('[createToolEndCallback] Headers not sent, returning attachment (will be sent via artifactPromises)');
            }
            
          return attachment;
          } catch (error) {
          logger.error('[createToolEndCallback] Error processing ui_resources artifact:', error);
            logger.error('[createToolEndCallback] Error stack:', error.stack);
          return null;
          }
        })(),
      );
      
      logger.info('[createToolEndCallback] ui_resources artifact promise added to artifactPromises');
      logger.info('[createToolEndCallback] artifactPromises length:', artifactPromises.length);
    } else {
      logger.warn('[createToolEndCallback] No ui_resources in artifact!');
      logger.debug('[createToolEndCallback] Available artifact keys:', artifact ? Object.keys(artifact) : 'null/undefined');
    }

    if (artifact && artifact[Tools.web_search]) {
      artifactPromises.push(
        (async () => {
          const attachment = {
            type: Tools.web_search,
            messageId: metadata.run_id,
            toolCallId: output.tool_call_id,
            conversationId: metadata.thread_id,
            [Tools.web_search]: { ...artifact[Tools.web_search] },
          };
          if (!res.headersSent) {
            return attachment;
          }
          res.write(`event: attachment\ndata: ${JSON.stringify(attachment)}\n\n`);
          return attachment;
        })().catch((error) => {
          logger.error('Error processing artifact content:', error);
          return null;
        }),
      );
    }

    if (artifact && artifact.content) {
      /** @type {FormattedContent[]} */
      const content = output.artifact.content;
      for (let i = 0; i < content.length; i++) {
        const part = content[i];
        if (!part) {
          continue;
        }
        if (part.type !== 'image_url') {
          continue;
        }
        const { url } = part.image_url;
        artifactPromises.push(
          (async () => {
            const filename = `${output.name}_${output.tool_call_id}_img_${nanoid()}`;
            const file_id = output.artifact.file_ids?.[i];
            const file = await saveBase64Image(url, {
              req,
              file_id,
              filename,
              endpoint: metadata.provider,
              context: FileContext.image_generation,
            });
            const fileMetadata = Object.assign(file, {
              messageId: metadata.run_id,
              toolCallId: output.tool_call_id,
              conversationId: metadata.thread_id,
            });
            if (!res.headersSent) {
              return fileMetadata;
            }

            if (!fileMetadata) {
              return null;
            }

            res.write(`event: attachment\ndata: ${JSON.stringify(fileMetadata)}\n\n`);
            return fileMetadata;
          })().catch((error) => {
            logger.error('Error processing artifact content:', error);
            return null;
          }),
        );
      }
      return;
    }

    {
      if (output.name !== Tools.execute_code) {
        return;
      }
    }

    if (!output.artifact.files) {
      return;
    }

    for (const file of output.artifact.files) {
      const { id, name } = file;
      artifactPromises.push(
        (async () => {
          const result = await loadAuthValues({
            userId: req.user.id,
            authFields: [EnvVar.CODE_API_KEY],
          });
          const fileMetadata = await processCodeOutput({
            req,
            id,
            name,
            apiKey: result[EnvVar.CODE_API_KEY],
            messageId: metadata.run_id,
            toolCallId: output.tool_call_id,
            conversationId: metadata.thread_id,
            session_id: output.artifact.session_id,
          });
          if (!res.headersSent) {
            return fileMetadata;
          }

          if (!fileMetadata) {
            return null;
          }

          res.write(`event: attachment\ndata: ${JSON.stringify(fileMetadata)}\n\n`);
          return fileMetadata;
        })().catch((error) => {
          logger.error('Error processing code output:', error);
          return null;
        }),
      );
    }
  };
}

module.exports = {
  getDefaultHandlers,
  createToolEndCallback,
};
