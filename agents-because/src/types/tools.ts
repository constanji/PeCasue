// src/types/tools.ts
import type { StructuredToolInterface } from '@langchain/core/tools';
import type { RunnableToolLike } from '@langchain/core/runnables';
import type { ToolCall } from '@langchain/core/messages/tool';
import type { ToolErrorData } from './stream';
import { EnvVar } from '@/common';

/** Replacement type for `import type { ToolCall } from '@langchain/core/messages/tool'` in order to have stringified args typed */
export type CustomToolCall = {
  name: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  args: string | Record<string, any>;
  id?: string;
  type?: 'tool_call';
  output?: string;
};

export type GenericTool = StructuredToolInterface | RunnableToolLike;

export type ToolMap = Map<string, GenericTool>;
export type ToolRefs = {
  tools: GenericTool[];
  toolMap?: ToolMap;
};

export type ToolRefGenerator = (tool_calls: ToolCall[]) => ToolRefs;

export type ToolEndEvent = {
  /** The Step Id of the Tool Call */
  id: string;
  /** The Completed Tool Call */
  tool_call: ToolCall;
  /** The content index of the tool call */
  index: number;
};

export type CodeEnvFile = {
  id: string;
  name: string;
  session_id: string;
};

export type FileRef = {
  id: string;
  name: string;
  path?: string;
};

export type FileRefs = FileRef[];

/** Mutable code-execution session tracked on the graph for ToolNode injection. */
export type CodeSessionContext = {
  session_id: string;
  files?: CodeEnvFile[];
  lastUpdated?: number;
};

/** Session bucket for code execution / programmatic tools (keyed by tool name constant). */
export type ToolSessionMap = Map<string, CodeSessionContext | Record<string, unknown>>;

export type ToolNodeOptions = {
  name?: string;
  tags?: string[];
  handleToolErrors?: boolean;
  loadRuntimeTools?: ToolRefGenerator;
  toolCallStepIds?: Map<string, string>;
  /** Map of tool call index -> {name, id} for recovering missing info (dashscope issue) */
  toolCallInfoByIndex?: Map<number, { name: string; id: string }>;
  errorHandler?: (
    data: ToolErrorData,
    metadata?: Record<string, unknown>
  ) => Promise<void>;
  /** Tool registry for lazy computation of programmatic tools and tool search */
  toolRegistry?: LCToolRegistry;
  /** Schema-only tool defs in event-driven mode (name → definition); merged with toolRegistry for lookups */
  toolDefinitions?: LCToolRegistry;
  /** Graph-owned session map for code execution continuity */
  sessions?: ToolSessionMap;
  /** When true, dispatch ON_TOOL_EXECUTE instead of invoking tools in-process */
  eventDrivenMode?: boolean;
  agentId?: string;
  /** Tool names that always execute in-process even in event-driven mode */
  directToolNames?: Set<string>;
  maxContextTokens?: number;
  maxToolResultChars?: number;
};

export type ToolNodeConstructorParams = ToolRefs & ToolNodeOptions;

/** Artifact shape on tool results that carry session/files from the code API. */
export type CodeExecutionArtifact = {
  session_id?: string;
  files?: FileRefs;
};

/** Single tool invocation in event-driven (ON_TOOL_EXECUTE) batch. */
export type ToolCallRequest = {
  id: string;
  name: string;
  args: Record<string, unknown>;
  stepId?: string;
  turn: number;
  codeSessionContext?: {
    session_id: string;
    files?: CodeEnvFile[];
  };
};

/** Result from host after executing one tool call in event-driven mode. */
export type ToolExecuteResult = {
  toolCallId: string;
  status: 'success' | 'error';
  content?: unknown;
  errorMessage?: string;
  artifact?: unknown;
};

/** Payload dispatched with ON_TOOL_EXECUTE for the host to run tools and resolve. */
export type ToolExecuteBatchRequest = {
  toolCalls: ToolCallRequest[];
  userId?: string;
  agentId?: string;
  configurable?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  resolve: (results: ToolExecuteResult[]) => void;
  reject: (err: unknown) => void;
};

export type CodeExecutionToolParams =
  | undefined
  | {
      session_id?: string;
      user_id?: string;
      apiKey?: string;
      files?: CodeEnvFile[];
      [EnvVar.CODE_API_KEY]?: string;
    };

export type ExecuteResult = {
  session_id: string;
  stdout: string;
  stderr: string;
  files?: FileRefs;
};

/** JSON Schema type definition for tool parameters */
export type JsonSchemaType = {
  type:
    | 'string'
    | 'number'
    | 'integer'
    | 'float'
    | 'boolean'
    | 'array'
    | 'object';
  enum?: string[];
  items?: JsonSchemaType;
  properties?: Record<string, JsonSchemaType>;
  required?: string[];
  description?: string;
  additionalProperties?: boolean | JsonSchemaType;
};

/**
 * Specifies which contexts can invoke a tool (inspired by Anthropic's allowed_callers)
 * - 'direct': Only callable directly by the LLM (default if omitted)
 * - 'code_execution': Only callable from within programmatic code execution
 */
export type AllowedCaller = 'direct' | 'code_execution';

/** Tool definition with optional deferred loading and caller restrictions */
export type LCTool = {
  name: string;
  description?: string;
  parameters?: JsonSchemaType;
  /** LangChain tool response shape (e.g. content_and_artifact for code tools) */
  responseFormat?: 'content' | 'content_and_artifact' | string;
  /** When true, tool is not loaded into context initially (for tool search) */
  defer_loading?: boolean;
  /**
   * Which contexts can invoke this tool.
   * Default: ['direct'] (only callable directly by LLM)
   * Options: 'direct', 'code_execution'
   */
  allowed_callers?: AllowedCaller[];
};

/** Map of tool names to tool definitions */
export type LCToolRegistry = Map<string, LCTool>;

export type ProgrammaticCache = { toolMap: ToolMap; toolDefs: LCTool[] };

/** Parameters for creating a Tool Search Regex tool */
export type ToolSearchRegexParams = {
  apiKey?: string;
  toolRegistry?: LCToolRegistry;
  onlyDeferred?: boolean;
  baseUrl?: string;
  [key: string]: unknown;
};

/** Simplified tool metadata for search purposes */
export type ToolMetadata = {
  name: string;
  description: string;
  parameters?: JsonSchemaType;
};

/** Individual search result for a matching tool */
export type ToolSearchResult = {
  tool_name: string;
  match_score: number;
  matched_field: string;
  snippet: string;
};

/** Response from the tool search operation */
export type ToolSearchResponse = {
  tool_references: ToolSearchResult[];
  total_tools_searched: number;
  pattern_used: string;
};

/** Artifact returned alongside the formatted search results */
export type ToolSearchArtifact = {
  tool_references: ToolSearchResult[];
  metadata: {
    total_searched: number;
    pattern: string;
    error?: string;
  };
};

// ============================================================================
// Programmatic Tool Calling Types
// ============================================================================

/**
 * Tool call requested by the Code API during programmatic execution
 */
export type PTCToolCall = {
  /** Unique ID like "call_001" */
  id: string;
  /** Tool name */
  name: string;
  /** Input parameters */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  input: Record<string, any>;
};

/**
 * Tool result sent back to the Code API
 */
export type PTCToolResult = {
  /** Matches PTCToolCall.id */
  call_id: string;
  /** Tool execution result (any JSON-serializable value) */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  result: any;
  /** Whether tool execution failed */
  is_error: boolean;
  /** Error details if is_error=true */
  error_message?: string;
};

/**
 * Response from the Code API for programmatic execution
 */
export type ProgrammaticExecutionResponse = {
  status: 'tool_call_required' | 'completed' | 'error' | unknown;
  session_id?: string;

  /** Present when status='tool_call_required' */
  continuation_token?: string;
  tool_calls?: PTCToolCall[];

  /** Present when status='completed' */
  stdout?: string;
  stderr?: string;
  files?: FileRefs;

  /** Present when status='error' */
  error?: string;
};

/**
 * Artifact returned by the PTC tool
 */
export type ProgrammaticExecutionArtifact = {
  session_id?: string;
  files?: FileRefs;
};

/**
 * Initialization parameters for the PTC tool
 */
export type ProgrammaticToolCallingParams = {
  /** Code API key (or use CODE_API_KEY env var) */
  apiKey?: string;
  /** Code API base URL (or use CODE_BASEURL env var) */
  baseUrl?: string;
  /** Safety limit for round-trips (default: 20) */
  maxRoundTrips?: number;
  /** HTTP proxy URL */
  proxy?: string;
  /** Enable debug logging (or set PTC_DEBUG=true env var) */
  debug?: boolean;
  /** Environment variable key for API key */
  [key: string]: unknown;
};
