import axios from 'axios';
import type {
  PipelineAllocationTemplatesStatus,
  PipelineAgentDraftsResponse,
  PipelineAutoZipResponse,
  PipelineChannelCatalog,
  PipelineClearUploadsResponse,
  PipelineClearChannelExtractedResponse,
  PipelineChannelDetail,
  PipelineChannelPrescanResponse,
  PipelineChannelRun,
  PipelineChannelZipReplaceResponse,
  PipelineClassification,
  PipelineCompareCreateRequest,
  PipelineCompareListEntry,
  PipelineCompareMeta,
  PipelineCompareReport,
  PipelineCompareUploadResponse,
  PipelineCopilotAskRequest,
  PipelineCopilotReply,
  PipelineCopilotStreamEvent,
  PipelineCopilotToolCall,
  PipelineCreateTaskRequest,
  PipelineHealth,
  PipelineLLMConfig,
  PipelineLLMConfigCreateRequest,
  PipelineLLMConfigsResponse,
  PipelineLLMConfigTestRequest,
  PipelineLLMConfigTestResult,
  PipelineLLMConfigUpdateRequest,
  PipelineObserveCharts,
  PipelineObserveEventsResponse,
  PipelineObserveKpi,
  PipelineOwnFlowTemplateImportResponse,
  PipelineCustomerFlowTemplateImportResponse,
  PipelinePreviewResponse,
  PipelineFinalMergeInventory,
  PipelineRuleFxImportResponse,
  PipelineRuleKind,
  PipelineRuleManifest,
  PipelineRuleTable,
  PipelineRuleVersion,
  PipelineRunResponse,
  PipelineTaskSummary,
  PipelineUploadResponse,
  PipelineAllocationMergeBaseUploadResponse,
  PipelineCostSummaryUploadResponse,
} from './types';

const API_PREFIX = '/api/pipeline';

function getAuthHeaders(initHeaders?: HeadersInit): Record<string, string> {
  const merged: Record<string, string> = {
    ...((initHeaders || {}) as Record<string, string>),
  };
  const auth = axios.defaults.headers.common?.Authorization;
  if (typeof auth === 'string' && auth.trim() && !merged.Authorization) {
    merged.Authorization = auth;
  }
  return merged;
}

function coerceStreamEvent(raw: unknown): PipelineCopilotStreamEvent | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const o = raw as Record<string, unknown>;
  const t = o.type;
  if (typeof t !== 'string') return null;
  switch (t) {
    case 'thought': {
      if (typeof o.step === 'string') return { type: 'thought', step: o.step };
      break;
    }
    case 'tool_call': {
      if (
        typeof o.name === 'string' &&
        o.args !== null &&
        typeof o.args === 'object' &&
        !Array.isArray(o.args)
      ) {
        return {
          type: 'tool_call',
          name: o.name,
          args: o.args as Record<string, unknown>,
          result: 'result' in o ? o.result : null,
          elapsed_ms: typeof o.elapsed_ms === 'number' ? o.elapsed_ms : Number(o.elapsed_ms ?? 0),
        };
      }
      break;
    }
    case 'answer': {
      if (typeof o.answer === 'string') return { type: 'answer', answer: o.answer };
      break;
    }
    case 'error': {
      if (typeof o.message === 'string') return { type: 'error', message: o.message };
      break;
    }
    case 'done': {
      const rep = o.reply;
      if (rep && typeof rep === 'object' && !Array.isArray(rep)) {
        const r = rep as Record<string, unknown>;
        if (
          typeof r.answer === 'string' &&
          Array.isArray(r.thoughts) &&
          Array.isArray(r.tool_calls)
        ) {
          return {
            type: 'done',
            reply: {
              answer: r.answer,
              thoughts: r.thoughts.filter((x): x is string => typeof x === 'string'),
              tool_calls: (r.tool_calls as unknown[]).map((tc): PipelineCopilotToolCall => {
                const row = tc as Record<string, unknown>;
                return {
                  name: String(row.name ?? ''),
                  args:
                    row.args !== null && typeof row.args === 'object' && !Array.isArray(row.args)
                      ? (row.args as Record<string, unknown>)
                      : {},
                  result: row.result ?? null,
                  elapsed_ms:
                    typeof row.elapsed_ms === 'number'
                      ? row.elapsed_ms
                      : Number(row.elapsed_ms ?? 0),
                };
              }),
              drafts: Array.isArray(r.drafts) ? r.drafts : undefined,
            },
          };
        }
      }
      break;
    }
    default:
      break;
  }
  return null;
}

async function request<T>(input: string, init?: RequestInit): Promise<T> {
  const isFormData =
    typeof FormData !== 'undefined' && init?.body instanceof FormData;
  const headers: Record<string, string> = isFormData
    ? getAuthHeaders(init?.headers)
    : { 'Content-Type': 'application/json', ...getAuthHeaders(init?.headers) };
  const res = await fetch(`${API_PREFIX}${input}`, {
    credentials: 'include',
    ...init,
    headers,
  });
  if (!res.ok) {
    let detail: unknown = null;
    try {
      detail = await res.json();
    } catch {
      /* ignore */
    }
    let msg = `Pipeline request failed: ${res.status}`;
    if (detail && typeof detail === 'object') {
      const d = detail as { message?: unknown; detail?: unknown };
      if (typeof d.message === 'string' && d.message.trim()) msg = d.message;
      else if (typeof d.detail === 'string' && d.detail.trim()) msg = d.detail;
      else if (Array.isArray(d.detail))
        msg = d.detail.map((x) => (typeof x === 'object' && x && 'msg' in x ? String((x as { msg?: unknown }).msg) : JSON.stringify(x))).join('; ');
    }
    throw new Error(msg);
  }
  return (await res.json()) as T;
}

export const PipelineApi = {
  health(): Promise<PipelineHealth> {
    return request<PipelineHealth>('/health');
  },

  listTasks(): Promise<{ tasks: PipelineTaskSummary[] }> {
    return request<{ tasks: PipelineTaskSummary[] }>('/tasks');
  },

  getTask(taskId: string): Promise<{ summary: PipelineTaskSummary; state: unknown | null }> {
    return request<{ summary: PipelineTaskSummary; state: unknown | null }>(
      `/tasks/${encodeURIComponent(taskId)}`,
    );
  },

  getTimeline(taskId: string): Promise<{ task_id: string; events: unknown[] }> {
    return request<{ task_id: string; events: unknown[] }>(
      `/tasks/${encodeURIComponent(taskId)}/timeline`,
    );
  },

  deleteTask(taskId: string): Promise<{ deleted: boolean; task_id: string }> {
    return request<{ deleted: boolean; task_id: string }>(
      `/tasks/${encodeURIComponent(taskId)}`,
      { method: 'DELETE' },
    );
  },

  getFinalMergeInventory(taskId: string): Promise<PipelineFinalMergeInventory> {
    return request<PipelineFinalMergeInventory>(
      `/tasks/${encodeURIComponent(taskId)}/final-merge-inventory`,
    );
  },

  channelCatalog(): Promise<PipelineChannelCatalog> {
    return request<PipelineChannelCatalog>('/channels/catalog');
  },

  createTask(payload: PipelineCreateTaskRequest): Promise<{ task_id: string }> {
    return request<{ task_id: string }>('/tasks', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  },

  uploadZipAuto(taskId: string, file: File): Promise<PipelineAutoZipResponse> {
    const fd = new FormData();
    fd.append('file', file, file.name);
    return request<PipelineAutoZipResponse>(
      `/tasks/${encodeURIComponent(taskId)}/upload-zip-auto`,
      { method: 'POST', body: fd },
    );
  },

  clearTaskUploads(taskId: string): Promise<PipelineClearUploadsResponse> {
    return request<PipelineClearUploadsResponse>(
      `/tasks/${encodeURIComponent(taskId)}/clear-uploads`,
      { method: 'POST' },
    );
  },

  uploadCostSummary(
    taskId: string,
    file: File,
  ): Promise<PipelineCostSummaryUploadResponse> {
    const fd = new FormData();
    fd.append('file', file, file.name);
    return request<PipelineCostSummaryUploadResponse>(
      `/tasks/${encodeURIComponent(taskId)}/channels/final_merge/upload-cost-summary`,
      { method: 'POST', body: fd },
    );
  },

  uploadAllocationMergeBase(
    taskId: string,
    file: File,
  ): Promise<PipelineAllocationMergeBaseUploadResponse> {
    const fd = new FormData();
    fd.append('file', file, file.name);
    return request<PipelineAllocationMergeBaseUploadResponse>(
      `/tasks/${encodeURIComponent(taskId)}/channels/allocation_base/upload-merge-base`,
      { method: 'POST', body: fd },
    );
  },

  clearChannelExtracted(
    taskId: string,
    channelId: string,
  ): Promise<PipelineClearChannelExtractedResponse> {
    return request<PipelineClearChannelExtractedResponse>(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/clear-extracted`,
      { method: 'POST' },
    );
  },

  uploadChannelZipReplace(
    taskId: string,
    channelId: string,
    file: File,
  ): Promise<PipelineChannelZipReplaceResponse> {
    const fd = new FormData();
    fd.append('file', file, file.name);
    return request<PipelineChannelZipReplaceResponse>(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/upload-zip-replace`,
      { method: 'POST', body: fd },
    );
  },

  uploadPerChannel(
    taskId: string,
    files: Partial<{
      bill_files: File[];
      own_flow_files: File[];
      customer_files: File[];
      special_files: File[];
      cn_jp_files: File[];
      allocation_files: File[];
      summary_files: File[];
    }>,
  ): Promise<PipelineUploadResponse> {
    const fd = new FormData();
    for (const [field, list] of Object.entries(files)) {
      if (!list || list.length === 0) continue;
      list.forEach((f) => fd.append(field, f, f.name));
    }
    return request<PipelineUploadResponse>(
      `/tasks/${encodeURIComponent(taskId)}/upload`,
      { method: 'POST', body: fd },
    );
  },

  getClassification(taskId: string): Promise<PipelineClassification> {
    return request<PipelineClassification>(
      `/tasks/${encodeURIComponent(taskId)}/classification`,
    );
  },

  getChannelPrescan(
    taskId: string,
    channelId: string,
  ): Promise<PipelineChannelPrescanResponse> {
    return request<PipelineChannelPrescanResponse>(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/prescan`,
    );
  },

  getChannel(taskId: string, channelId: string): Promise<PipelineChannelDetail> {
    return request<PipelineChannelDetail>(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}`,
    );
  },

  listChannelRuns(
    taskId: string,
    channelId: string,
  ): Promise<{ task_id: string; channel_id: string; runs: PipelineChannelRun[] }> {
    return request(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/runs`,
    );
  },

  getChannelRun(
    taskId: string,
    channelId: string,
    runId: string,
  ): Promise<PipelineChannelRun> {
    return request<PipelineChannelRun>(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/runs/${encodeURIComponent(runId)}`,
    );
  },

  deleteChannelRun(
    taskId: string,
    channelId: string,
    runId: string,
  ): Promise<{ deleted: boolean; task_id: string; channel_id: string; run_id: string }> {
    return request(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/runs/${encodeURIComponent(runId)}`,
      { method: 'DELETE' },
    );
  },

  triggerChannelRun(
    taskId: string,
    channelId: string,
    payload?: {
      note?: string | null;
      allocation_phase?: string;
      allocation_options?: Record<string, unknown>;
    },
  ): Promise<PipelineRunResponse> {
    return request<PipelineRunResponse>(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/run`,
      {
        method: 'POST',
        body: JSON.stringify(payload ?? {}),
      },
    );
  },

  cancelChannelRun(
    taskId: string,
    channelId: string,
  ): Promise<{ task_id: string; channel_id: string; cancelled: boolean }> {
    return request(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/cancel`,
      { method: 'POST' },
    );
  },

  confirmChannel(
    taskId: string,
    channelId: string,
  ): Promise<{ task_id: string; channel_id: string; status: string; already?: boolean }> {
    return request(`/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(channelId)}/confirm`, {
      method: 'POST',
    });
  },

  runFileDownloadUrl(
    taskId: string,
    channelId: string,
    runId: string,
    name: string,
  ): string {
    return `${API_PREFIX}/tasks/${encodeURIComponent(
      taskId,
    )}/channels/${encodeURIComponent(channelId)}/runs/${encodeURIComponent(
      runId,
    )}/files/${encodeURIComponent(name)}`;
  },

  allocationMergeBaseUploadDownloadUrl(taskId: string, name: string): string {
    return `${API_PREFIX}/tasks/${encodeURIComponent(
      taskId,
    )}/channels/allocation_base/uploads/${encodeURIComponent(name)}`;
  },

  sourceFileDownloadUrl(taskId: string, channelId: string, relPath: string): string {
    const u = new URLSearchParams({ rel_path: relPath });
    return `${API_PREFIX}/tasks/${encodeURIComponent(
      taskId,
    )}/channels/${encodeURIComponent(channelId)}/files/download?${u.toString()}`;
  },

  // ---------- rules ----------

  getRulesManifest(): Promise<PipelineRuleManifest> {
    return request<PipelineRuleManifest>('/rules/manifest');
  },

  getRule(
    kind: PipelineRuleKind,
    init?: { headers?: Record<string, string> },
  ): Promise<{
    kind: PipelineRuleKind;
    table: PipelineRuleTable;
    is_password_book?: boolean;
    masked?: boolean;
  }> {
    return request(`/rules/${encodeURIComponent(kind)}`, {
      headers: init?.headers,
    });
  },

  putRule(
    kind: PipelineRuleKind,
    table: PipelineRuleTable,
    note?: string,
  ): Promise<{
    kind: PipelineRuleKind;
    entry?: { kind: PipelineRuleKind; version: number; rows_count: number };
    rows_count?: number;
    is_password_book?: boolean;
  }> {
    return request(`/rules/${encodeURIComponent(kind)}`, {
      method: 'PUT',
      body: JSON.stringify({ table, note: note ?? null }),
    });
  },

  getRuleVersions(
    kind: PipelineRuleKind,
  ): Promise<{ kind: PipelineRuleKind; versions: PipelineRuleVersion[]; note?: string }> {
    return request(`/rules/${encodeURIComponent(kind)}/versions`);
  },

  /** 将当前规则 JSON 同步到 data/rules/files 侧车 CSV/XLSX（解决执行仍读旧文件） */
  syncRuleSidecars(kind: PipelineRuleKind): Promise<{
    kind: PipelineRuleKind;
    sidecars: { csv_relative: string; xlsx_relative: string; rows: number } | null;
  }> {
    return request(`/rules/${encodeURIComponent(kind)}/sync-sidecars`, { method: 'POST' });
  },

  rollbackRule(
    kind: PipelineRuleKind,
    targetVersion: number,
    note?: string,
  ): Promise<{ kind: PipelineRuleKind; entry: { version: number } }> {
    return request(`/rules/${encodeURIComponent(kind)}/rollback`, {
      method: 'POST',
      body: JSON.stringify({ target_version: targetVersion, note: note ?? null }),
    });
  },

  getAllocationTemplatesStatus(): Promise<PipelineAllocationTemplatesStatus> {
    return request<PipelineAllocationTemplatesStatus>('/rules/allocation-templates/status');
  },

  uploadAllocationTemplate(
    templateKind: 'quickbi' | 'citihk' | 'cost_allocate',
    file: File,
  ): Promise<{ ok: boolean; template_kind: string }> {
    const fd = new FormData();
    fd.append('template_kind', templateKind);
    fd.append('file', file);
    return request<{ ok: boolean; template_kind: string }>('/rules/allocation-templates/upload', {
      method: 'POST',
      body: fd,
    });
  },

  importRuleFx(
    file: File,
    note?: string,
    fxMonthLabel?: string,
  ): Promise<PipelineRuleFxImportResponse> {
    const fd = new FormData();
    fd.append('file', file);
    if (note) fd.append('note', note);
    if (fxMonthLabel) fd.append('fx_month_label', fxMonthLabel);
    return request<PipelineRuleFxImportResponse>('/rules/import/fx', {
      method: 'POST',
      body: fd,
    });
  },

  importOwnFlowTemplate(
    file: File,
    note?: string,
    scope?: string,
    fxMonthLabel?: string,
  ): Promise<PipelineOwnFlowTemplateImportResponse> {
    const fd = new FormData();
    fd.append('file', file);
    if (note) fd.append('note', note);
    if (scope) fd.append('scope', scope);
    if (fxMonthLabel) fd.append('fx_month_label', fxMonthLabel);
    return request<PipelineOwnFlowTemplateImportResponse>('/rules/import/own_flow_template', {
      method: 'POST',
      body: fd,
    });
  },

  importCustomerFlowTemplate(
    file: File,
    note?: string,
    scope?: string,
  ): Promise<PipelineCustomerFlowTemplateImportResponse> {
    const fd = new FormData();
    fd.append('file', file);
    if (note) fd.append('note', note);
    if (scope) fd.append('scope', scope);
    return request<PipelineCustomerFlowTemplateImportResponse>(
      '/rules/import/customer_flow_template',
      {
        method: 'POST',
        body: fd,
      },
    );
  },

  // ---------- observe ----------

  observeKpi(windowDays = 1): Promise<PipelineObserveKpi> {
    return request<PipelineObserveKpi>(`/observe/kpi?window_days=${windowDays}`);
  },

  observeCharts(windowDays = 7): Promise<PipelineObserveCharts> {
    return request<PipelineObserveCharts>(`/observe/charts?window_days=${windowDays}`);
  },

  observeEvents(params: {
    limit?: number;
    task_id?: string;
    channel_id?: string;
    event_type?: string;
  } = {}): Promise<PipelineObserveEventsResponse> {
    const u = new URLSearchParams();
    if (params.limit) u.set('limit', String(params.limit));
    if (params.task_id) u.set('task_id', params.task_id);
    if (params.channel_id) u.set('channel_id', params.channel_id);
    if (params.event_type) u.set('event_type', params.event_type);
    const qs = u.toString();
    return request<PipelineObserveEventsResponse>(
      `/observe/events${qs ? `?${qs}` : ''}`,
    );
  },

  // ---------- preview ----------

  previewRunFile(
    taskId: string,
    channelId: string,
    runId: string,
    filename: string,
  ): Promise<PipelinePreviewResponse> {
    const u = new URLSearchParams({
      task_id: taskId,
      channel_id: channelId,
      run_id: runId,
      filename,
    });
    return request<PipelinePreviewResponse>(`/preview/run-file?${u.toString()}`);
  },

  // ---------- compare ----------

  createCompare(payload: PipelineCompareCreateRequest): Promise<PipelineCompareMeta> {
    return request<PipelineCompareMeta>('/compare', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  },

  listCompares(taskId?: string): Promise<{ compares: PipelineCompareListEntry[] }> {
    const qs = taskId ? `?task_id=${encodeURIComponent(taskId)}` : '';
    return request(`/compare${qs}`);
  },

  getCompareMeta(taskId: string, compareId: string): Promise<PipelineCompareMeta> {
    return request<PipelineCompareMeta>(
      `/compare/${encodeURIComponent(compareId)}?task_id=${encodeURIComponent(taskId)}`,
    );
  },

  getCompareReport(taskId: string, compareId: string): Promise<PipelineCompareReport> {
    return request<PipelineCompareReport>(
      `/compare/${encodeURIComponent(compareId)}/report?task_id=${encodeURIComponent(taskId)}`,
    );
  },

  compareDownloadUrl(taskId: string, compareId: string): string {
    return `${API_PREFIX}/compare/${encodeURIComponent(
      compareId,
    )}/download?task_id=${encodeURIComponent(taskId)}`;
  },

  uploadCompareFile(taskId: string, file: File): Promise<PipelineCompareUploadResponse> {
    const fd = new FormData();
    fd.append('task_id', taskId);
    fd.append('file', file, file.name);
    return request<PipelineCompareUploadResponse>('/compare/upload', {
      method: 'POST',
      body: fd,
    });
  },

  agentAsk(payload: PipelineCopilotAskRequest): Promise<PipelineCopilotReply> {
    return request<PipelineCopilotReply>('/agent/ask', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  },

  async agentAskStream(
    payload: PipelineCopilotAskRequest,
    onEvent: (e: PipelineCopilotStreamEvent) => void,
    options?: { signal?: AbortSignal },
  ): Promise<PipelineCopilotReply> {
    const res = await fetch(`${API_PREFIX}/agent/ask/stream`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
        ...getAuthHeaders(),
      },
      body: JSON.stringify({
        task_id: payload.task_id,
        channel_id: payload.channel_id,
        verify_row_id: payload.verify_row_id,
        run_id: payload.run_id,
        question: payload.question,
        ...(payload.history && payload.history.length > 0
          ? { history: payload.history }
          : {}),
      }),
      signal: options?.signal,
    });
    if (!res.ok) {
      let detail: unknown = null;
      try {
        detail = await res.json();
      } catch {
        /* ignore */
      }
      let msg = `Pipeline stream failed: ${res.status}`;
      if (detail && typeof detail === 'object') {
        const d = detail as { message?: unknown; detail?: unknown };
        if (typeof d.message === 'string' && d.message.trim()) msg = d.message;
        else if (typeof d.detail === 'string' && d.detail.trim()) msg = d.detail;
      }
      throw new Error(msg);
    }
    const reader = res.body?.getReader();
    if (!reader) {
      throw new Error('Pipeline stream: missing response body');
    }
    const decoder = new TextDecoder();
    let buf = '';
    let finalReply: PipelineCopilotReply | null = null;

    const handleLine = (line: string) => {
      if (!line) return;
      try {
        const raw = JSON.parse(line) as unknown;
        const ev = coerceStreamEvent(raw);
        if (!ev) return;
        onEvent(ev);
        if (ev.type === 'done') finalReply = ev.reply;
        if (ev.type === 'error') throw new Error(ev.message);
      } catch (e) {
        if (e instanceof SyntaxError) return;
        throw e;
      }
    };

    while (true) {
      const { done, value } = await reader.read();
      buf += decoder.decode(value ?? undefined, { stream: !done });
      let nl: number;
      while ((nl = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        handleLine(line);
      }
      if (done) break;
    }
    handleLine(buf.trim());
    if (!finalReply) throw new Error('Pipeline stream ended without done event');
    return finalReply;
  },

  agentDrafts(taskId: string): Promise<PipelineAgentDraftsResponse> {
    return request<PipelineAgentDraftsResponse>(
      `/agent/drafts/${encodeURIComponent(taskId)}`,
    );
  },

  replaceSourceFile(
    taskId: string,
    channelId: string,
    relPath: string,
    file: File,
  ): Promise<{
    task_id: string;
    channel_id: string;
    rel_path: string;
    old_sha256: string;
    new_sha256: string;
    backup_path: string;
    is_changed: boolean;
    advisory: string;
  }> {
    const fd = new FormData();
    fd.append('rel_path', relPath);
    fd.append('file', file, file.name);
    return request(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(
        channelId,
      )}/files/replace`,
      { method: 'POST', body: fd },
    );
  },

  replaceRunOutputFile(
    taskId: string,
    channelId: string,
    runId: string,
    outputFileName: string,
    file: File,
  ): Promise<{
    task_id: string;
    channel_id: string;
    run_id: string;
    name: string;
    old_sha256: string | null;
    new_sha256: string;
    backup_path: string;
    size: number;
    run_status: string;
  }> {
    const fd = new FormData();
    fd.append('name', outputFileName);
    fd.append('file', file, file.name);
    return request(
      `/tasks/${encodeURIComponent(taskId)}/channels/${encodeURIComponent(
        channelId,
      )}/runs/${encodeURIComponent(runId)}/files/replace`,
      { method: 'POST', body: fd },
    );
  },

  // ---------- LLM Configs ----------

  listLLMConfigs(): Promise<PipelineLLMConfigsResponse> {
    return request('/llm-configs');
  },

  getActiveLLMConfig(): Promise<{ active: PipelineLLMConfig | null }> {
    return request('/llm-configs/active');
  },

  createLLMConfig(payload: PipelineLLMConfigCreateRequest): Promise<PipelineLLMConfig> {
    return request('/llm-configs', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  },

  updateLLMConfig(
    configId: number,
    payload: PipelineLLMConfigUpdateRequest,
  ): Promise<PipelineLLMConfig> {
    return request(`/llm-configs/${configId}`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
  },

  deleteLLMConfig(configId: number): Promise<void> {
    return request(`/llm-configs/${configId}`, { method: 'DELETE' });
  },

  activateLLMConfig(configId: number): Promise<PipelineLLMConfig> {
    return request(`/llm-configs/${configId}/activate`, { method: 'POST' });
  },

  testLLMConfig(payload: PipelineLLMConfigTestRequest): Promise<PipelineLLMConfigTestResult> {
    return request('/llm-configs/test', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  },
};
