import { Run, Providers, Constants } from '@because/agents';
import { providerEndpointMap, KnownEndpoints, type SummarizationConfig } from '@because/data-provider';
import { logger } from '@because/data-schemas';
import type {
  SummarizationConfig as AgentSummarizationConfig,
  MultiAgentGraphConfig,
  ContextPruningConfig,
  OpenAIClientOptions,
  StandardGraphConfig,
  LCToolRegistry,
  AgentInputs,
  GenericTool,
  RunConfig,
  IState,
  LCTool,
} from '@because/agents';
import type { Agent } from '@because/data-provider';
import type { BaseMessage } from '@langchain/core/messages';
import type { IUser } from '@because/data-schemas';
import type * as t from '~/types';
import { resolveHeaders, createSafeUser } from '~/utils/env';

/** Expected shape of JSON tool search results */
interface ToolSearchJsonResult {
  found?: number;
  tools?: Array<{ name: string }>;
}

function parseToolSearchJson(content: string, discoveredTools: Set<string>): boolean {
  try {
    const parsed = JSON.parse(content) as ToolSearchJsonResult;
    if (!parsed.tools || !Array.isArray(parsed.tools)) {
      return false;
    }
    for (const tool of parsed.tools) {
      if (tool.name && typeof tool.name === 'string') {
        discoveredTools.add(tool.name);
      }
    }
    return parsed.tools.length > 0;
  } catch {
    return false;
  }
}

function parseToolSearchLegacy(content: string, discoveredTools: Set<string>): void {
  const toolNameRegex = /^- ([^\s(]+)\s*\(score:/gm;
  let match: RegExpExecArray | null;
  while ((match = toolNameRegex.exec(content)) !== null) {
    const toolName = match[1];
    if (toolName) {
      discoveredTools.add(toolName);
    }
  }
}

export function extractDiscoveredToolsFromHistory(messages: BaseMessage[]): Set<string> {
  const discoveredTools = new Set<string>();

  for (const message of messages) {
    const msgType = message._getType?.() ?? message.constructor?.name ?? '';
    if (msgType !== 'tool') {
      continue;
    }

    const name = (message as { name?: string }).name;
    if (name !== Constants.TOOL_SEARCH) {
      continue;
    }

    const content = message.content;
    if (typeof content !== 'string') {
      continue;
    }

    if (!parseToolSearchJson(content, discoveredTools)) {
      parseToolSearchLegacy(content, discoveredTools);
    }
  }

  return discoveredTools;
}

export function overrideDeferLoadingForDiscoveredTools(
  toolRegistry: LCToolRegistry,
  discoveredTools: Set<string>,
): number {
  let overrideCount = 0;
  for (const toolName of discoveredTools) {
    const toolDef = toolRegistry.get(toolName);
    if (toolDef && toolDef.defer_loading === true) {
      toolDef.defer_loading = false;
      overrideCount++;
    }
  }
  return overrideCount;
}

const customProviders = new Set([
  Providers.XAI,
  Providers.OLLAMA,
  Providers.DEEPSEEK,
  Providers.OPENROUTER,
  Providers.MOONSHOT,
  KnownEndpoints.ollama,
]);

export function getReasoningKey(
  provider: Providers,
  llmConfig: t.RunLLMConfig,
  agentEndpoint?: string | null,
): 'reasoning_content' | 'reasoning' {
  let reasoningKey: 'reasoning_content' | 'reasoning' = 'reasoning_content';
  if (provider === Providers.GOOGLE) {
    reasoningKey = 'reasoning';
  } else if (
    llmConfig.configuration?.baseURL?.includes(KnownEndpoints.openrouter) ||
    (agentEndpoint && agentEndpoint.toLowerCase().includes(KnownEndpoints.openrouter))
  ) {
    reasoningKey = 'reasoning';
  } else if (
    (llmConfig as OpenAIClientOptions).useResponsesApi === true &&
    (provider === Providers.OPENAI || provider === Providers.AZURE)
  ) {
    reasoningKey = 'reasoning';
  }
  return reasoningKey;
}

type RunAgent = Omit<Agent, 'tools'> & {
  tools?: GenericTool[];
  maxContextTokens?: number;
  baseContextTokens?: number;
  useLegacyContent?: boolean;
  toolContextMap?: Record<string, string>;
  toolRegistry?: LCToolRegistry;
  toolDefinitions?: LCTool[];
  hasDeferredTools?: boolean;
  summarization?: SummarizationConfig;
  maxToolResultChars?: number;
};

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function shapeSummarizationConfig(
  config: SummarizationConfig | undefined,
  fallbackProvider: string,
  fallbackModel: string | undefined,
) {
  const provider = config?.provider ?? fallbackProvider;
  const model = config?.model ?? fallbackModel;
  const trigger =
    config?.trigger?.type && config?.trigger?.value
      ? { type: config.trigger.type, value: config.trigger.value }
      : undefined;

  return {
    enabled: config?.enabled !== false && isNonEmptyString(provider) && isNonEmptyString(model),
    config: {
      trigger,
      provider,
      model,
      parameters: config?.parameters,
      prompt: config?.prompt,
      updatePrompt: config?.updatePrompt,
      reserveRatio: config?.reserveRatio,
      maxSummaryTokens: config?.maxSummaryTokens,
    } satisfies AgentSummarizationConfig,
    contextPruning: config?.contextPruning as ContextPruningConfig | undefined,
    reserveRatio: config?.reserveRatio,
  };
}

function computeEffectiveMaxContextTokens(
  reserveRatio: number | undefined,
  baseContextTokens: number | undefined,
  maxContextTokens: number | undefined,
): number | undefined {
  if (reserveRatio == null || reserveRatio <= 0 || reserveRatio >= 1 || baseContextTokens == null) {
    return maxContextTokens;
  }
  const ratioComputed = Math.max(1024, Math.round(baseContextTokens * (1 - reserveRatio)));
  return Math.min(maxContextTokens ?? ratioComputed, ratioComputed);
}

export async function createRun({
  runId,
  signal,
  agents,
  messages,
  requestBody,
  user,
  tokenCounter,
  customHandlers,
  indexTokenCountMap,
  summarizationConfig,
  initialSummary,
  calibrationRatio,
  streaming = true,
  streamUsage = true,
}: {
  agents: RunAgent[];
  signal: AbortSignal;
  runId?: string;
  streaming?: boolean;
  streamUsage?: boolean;
  requestBody?: t.RequestBody;
  user?: IUser;
  messages?: BaseMessage[];
  summarizationConfig?: SummarizationConfig;
  initialSummary?: { text: string; tokenCount: number };
  calibrationRatio?: number;
} & Pick<RunConfig, 'tokenCounter' | 'customHandlers' | 'indexTokenCountMap'>): Promise<
  Run<IState>
> {
  const hasAnyDeferredTools = agents.some((agent) => agent.hasDeferredTools === true);

  const discoveredTools =
    hasAnyDeferredTools && messages?.length
      ? extractDiscoveredToolsFromHistory(messages)
      : new Set<string>();

  const agentInputs: AgentInputs[] = [];
  const buildAgentContext = (agent: RunAgent) => {
    const provider =
      (providerEndpointMap[
        agent.provider as keyof typeof providerEndpointMap
      ] as unknown as Providers) ?? agent.provider;
    const selfModel = agent.model_parameters?.model ?? (agent.model as string | undefined);

    const summarization = shapeSummarizationConfig(
      agent.summarization ?? summarizationConfig,
      provider as string,
      selfModel,
    );

    const llmConfig: t.RunLLMConfig = Object.assign(
      {
        provider,
        streaming,
        streamUsage,
      },
      agent.model_parameters,
    );

    const systemMessage = Object.values(agent.toolContextMap ?? {})
      .join('\n')
      .trim();

    const systemContent = [
      systemMessage,
      agent.instructions ?? '',
      agent.additional_instructions ?? '',
    ]
      .join('\n')
      .trim();

    if (llmConfig?.configuration?.defaultHeaders != null) {
      llmConfig.configuration.defaultHeaders = resolveHeaders({
        headers: llmConfig.configuration.defaultHeaders as Record<string, string>,
        user: createSafeUser(user),
        body: requestBody,
      });
    }

    if (
      customProviders.has(agent.provider) ||
      (agent.provider === Providers.OPENAI && agent.endpoint !== agent.provider)
    ) {
      llmConfig.streamUsage = false;
      llmConfig.usage = true;
    }

    let toolDefinitions = agent.toolDefinitions ?? [];
    if (discoveredTools.size > 0 && agent.toolRegistry) {
      overrideDeferLoadingForDiscoveredTools(agent.toolRegistry, discoveredTools);

      const existingToolNames = new Set(toolDefinitions.map((d) => d.name));
      for (const toolName of discoveredTools) {
        if (existingToolNames.has(toolName)) {
          continue;
        }
        const toolDef = agent.toolRegistry.get(toolName);
        if (toolDef) {
          toolDefinitions = [...toolDefinitions, toolDef];
        }
      }
    }

    const effectiveMaxContextTokens = computeEffectiveMaxContextTokens(
      summarization.reserveRatio,
      agent.baseContextTokens,
      agent.maxContextTokens,
    );

    const reasoningKey = getReasoningKey(provider, llmConfig, agent.endpoint);
    const agentInput: AgentInputs = {
      provider,
      reasoningKey,
      toolDefinitions,
      agentId: agent.id,
      tools: agent.tools,
      clientOptions: llmConfig,
      instructions: systemContent,
      name: agent.name ?? undefined,
      toolRegistry: agent.toolRegistry,
      maxContextTokens: effectiveMaxContextTokens,
      useLegacyContent: agent.useLegacyContent ?? false,
      discoveredTools: discoveredTools.size > 0 ? Array.from(discoveredTools) : undefined,
      summarizationEnabled: summarization.enabled,
      summarizationConfig: summarization.config,
      initialSummary,
      contextPruningConfig: summarization.contextPruning,
      maxToolResultChars: agent.maxToolResultChars,
    };
    agentInputs.push(agentInput);

    const hasSpeckitTool = agent.tools?.some(
      (tool) =>
        (typeof tool === 'string' && tool === 'speckit') ||
        (tool && typeof tool === 'object' && 'name' in tool && tool.name === 'speckit'),
    );

    if (hasSpeckitTool || agentInputs.length === 1) {
      logger.info(`[Agent-Run] Agent Input #${agentInputs.length} agentId=${agent.id}`);
    }
  };

  for (const agent of agents) {
    buildAgentContext(agent);
  }

  const graphConfig: RunConfig['graphConfig'] = {
    signal,
    agents: agentInputs,
    edges: agents[0].edges,
  };

  if (agentInputs.length > 1 || ((graphConfig as MultiAgentGraphConfig).edges?.length ?? 0) > 0) {
    (graphConfig as unknown as MultiAgentGraphConfig).type = 'multi-agent';
  } else {
    (graphConfig as StandardGraphConfig).type = 'standard';
  }

  return Run.create({
    runId,
    graphConfig,
    tokenCounter,
    customHandlers,
    indexTokenCountMap,
    calibrationRatio,
  });
}
