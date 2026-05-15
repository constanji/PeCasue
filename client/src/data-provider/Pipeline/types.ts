export interface PipelineFinalMergeArtifact {
  name: string;
  size: number;
  created_at: string;
  role: string;
  sha256: string | null;
  row_count: number | null;
  run_id: string;
}

export interface PipelineFinalMergeLatestRun {
  run_id: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  duration_seconds: number | null;
}

export interface PipelineFinalMergeChannelRow {
  channel_id: string;
  display_name: string;
  entry_type: string;
  channel_status: string;
  latest_run: PipelineFinalMergeLatestRun | null;
  artifacts: PipelineFinalMergeArtifact[];
}

export interface PipelineFinalMergeInventory {
  task_id: string;
  channels: PipelineFinalMergeChannelRow[];
}

export type PipelineTaskStatus =
  | 'pending'
  | 'running'
  | 'partial'
  | 'completed'
  | 'failed'
  | 'paused'
  | 'terminated';

export type PipelineStep =
  | 'CREATED'
  | 'UPLOADING'
  | 'CLASSIFYING'
  | 'RUNNING'
  | 'SUMMARY'
  | 'COMPLETED'
  | 'FAILED'
  | 'INTERVENTION';

export interface PipelineChannelSummary {
  display_name: string;
  entry_type: string;
  status: string;
  runs: number;
  current_run_id: string | null;
}

export interface PipelineTaskSummary {
  task_id: string;
  created_by: string | null;
  period: string | null;
  status: PipelineTaskStatus;
  current_step: PipelineStep | string;
  channels: Record<string, PipelineChannelSummary>;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  duration_seconds: number | null;
  latest_log: string | null;
  error: string | null;
  updated_at: string;
}

export interface PipelineHealth {
  ok: boolean;
  service: string;
  version: string;
  data_root: string;
}

export interface PipelineChannelDef {
  channel_id: string;
  display_name: string;
  entry_type: string;
  hint: string;
}

export interface PipelineChannelCatalog {
  channels: PipelineChannelDef[];
}

export interface PipelineClassificationFile {
  rel_path: string;
  /** 修正 ZIP/系统编码误读后的展示路径；下载仍用 ``rel_path``。 */
  display_rel_path?: string;
  size: number;
  /** UI-only：同源文件来自哪一个 extracted 渠道目录（用于双目录并列展示时的下载/替换）。 */
  source_channel_id?: string;
}

export interface PipelineClassificationGroup {
  display_name: string;
  entry_type: string;
  status: string;
  /** Task-state run history length; used by UI to distinguish 待运行 vs 待确认 */
  runs_count?: number;
  files: PipelineClassificationFile[];
}

export interface PipelineClassification {
  task_id: string;
  channels: Record<string, PipelineClassificationGroup>;
}

/** GET …/channels/bill/prescan — folders matched to zhangdan BANK_SCRIPTS (nested-aware). */
export interface PipelineBillPrescanFolderRow {
  folder_name: string;
  folder_path: string;
  bank_key: string;
  file_count: number;
}

export interface PipelineBillPrescanDirHint {
  folder_name: string;
  folder_path: string;
  hint: string;
}

export interface PipelineBillPrescanResponse {
  task_id: string;
  channel_id: string;
  kind: 'bill';
  folders: PipelineBillPrescanFolderRow[];
  top_level_non_bank_dirs: PipelineBillPrescanDirHint[];
}

/** GET …/channels/own_flow/prescan — mirrors discovery.scan_inventory. */
export interface PipelineOwnFlowPrescanSourceRow {
  index: number;
  source: string | null | undefined;
  file: string | null | undefined;
  row_count: number | string | null | undefined;
  col_count: number | string | null | undefined;
  rel_path: string;
}

export interface PipelineOwnFlowPrescanWarningRow {
  rel_path?: string | null;
  reason?: string | null;
  detail?: string | null;
}

export interface PipelineOwnFlowPrescanResponse {
  task_id: string;
  channel_id: string;
  kind: 'own_flow';
  sources: PipelineOwnFlowPrescanSourceRow[];
  warnings: PipelineOwnFlowPrescanWarningRow[];
}

export interface PipelineChannelPrescanUnsupportedResponse {
  task_id: string;
  channel_id: string;
  kind: 'unsupported';
  message: string;
}

export type PipelineChannelPrescanResponse =
  | PipelineBillPrescanResponse
  | PipelineOwnFlowPrescanResponse
  | PipelineChannelPrescanUnsupportedResponse;

export interface PipelineCreateTaskRequest {
  period?: string | null;
  note?: string | null;
}

export interface PipelineUploadResponse {
  task_id: string;
  saved_files: string[];
  channels: Record<
    string,
    { display_name: string; entry_type: string; source_count: number }
  >;
}

export interface PipelineAutoZipResponse {
  task_id: string;
  archive: string;
  moved: string[];
  channels: Record<
    string,
    {
      channel_id: string;
      display_name: string;
      entry_type: string;
      item_count: number;
      total_files: number;
      items: { rel_path: string; is_dir: boolean; file_count: number }[];
    }
  >;
}

export interface PipelineClearUploadsResponse {
  task_id: string;
  cleared: boolean;
  removed_files: number;
}

export interface PipelineClearChannelExtractedResponse {
  task_id: string;
  channel_id: string;
  removed_files: number;
}

/** POST …/channels/{cid}/upload-zip-replace */
export interface PipelineChannelZipReplaceResponse {
  task_id: string;
  channel_id: string;
  archive: string;
  file_count: number;
}

export type PipelineChannelRunStatus =
  | 'pending'
  | 'running'
  | 'preview_ready'
  | 'verified'
  | 'verified_with_warning'
  | 'edited'
  | 'replaced'
  | 'confirmed'
  | 'failed'
  | 'skipped';

export interface PipelineFileEntry {
  file_id: string;
  name: string;
  path: string;
  size: number;
  sha256: string | null;
  role: string;
  created_at: string;
}

export interface PipelineVerifyRow {
  row_id: string;
  severity: 'pass' | 'warning' | 'pending';
  summary: string;
  rule_ref: string | null;
  file_ref: string | null;
  detail: Record<string, unknown>;
}

export interface PipelineVerifySummary {
  rows: PipelineVerifyRow[];
  warnings: string[];
  metrics: Record<string, unknown>;
  note: string | null;
}

export interface PipelineChannelRun {
  run_id: string;
  started_at: string;
  finished_at: string | null;
  status: PipelineChannelRunStatus;
  output_files: PipelineFileEntry[];
  verify_summary: PipelineVerifySummary | null;
  agent_interactions: unknown[];
  error: string | null;
  duration_seconds: number | null;
  is_dirty: boolean;
  note: string | null;
  /** 分摊基数渠道：quickbi | citihk | merge | cost_allocate；缺省表示库存扫描等 */
  allocation_phase?: string | null;
  run_options?: Record<string, unknown>;
}

export interface PipelineChannelDetail {
  channel_id: string;
  display_name: string;
  entry_type: string;
  status: PipelineChannelRunStatus;
  source_paths: string[];
  current_run_id: string | null;
  runs: PipelineChannelRun[];
  warnings: string[];
}

export interface PipelineRunResponse {
  task_id: string;
  channel_id: string;
  run_id: string | null;
  accepted: boolean;
}

export type PipelineRuleKind =
  | 'account_mapping'
  | 'fee_mapping'
  | 'fx'
  | 'own_flow_processing'
  | 'special_branch_mapping'
  | 'result_template'
  | 'password_book'
  | 'customer_mapping'
  | 'customer_fee_mapping'
  | 'customer_branch_mapping';

/** 规则页 Excel 导入栏配置（按当前子 Tab 限定 scope，避免误覆盖其它规则）。 */
export type PipelineRuleExcelImportConfig =
  | { variant: 'fx' }
  | { variant: 'own_flow_template'; scope?: string }
  | { variant: 'customer_flow_template'; scope?: string };

export interface PipelineRuleTable {
  columns: string[];
  rows: Record<string, unknown>[];
  note?: string | null;
  /** 后端扩展（如汇率页 `fx_month_label`） */
  meta?: Record<string, unknown> | null;
}

export interface PipelineRuleManifestEntry {
  kind: PipelineRuleKind;
  version: number;
  updated_at: string;
  updated_by: string | null;
  rows_count: number;
}

export interface PipelineRuleManifest {
  entries: Record<string, PipelineRuleManifestEntry>;
  created_at: string;
  updated_at: string;
}

/** GET /rules/allocation-templates/status — 分摊基数模版磁盘是否存在（非 RuleStore）。 */
export interface PipelineAllocationTemplatesStatus {
  quickbi_present: boolean;
  citihk_present: boolean;
  /** ``data/rules/files/allocation/成本分摊基数+输出模板.xlsx``（cost_allocate  workbook） */
  cost_allocate_workbook_present: boolean;
}

export interface PipelineRuleVersion {
  version: number;
  snapshot_path: string;
  author: string | null;
  note: string | null;
  created_at: string;
  /** 自快照 JSON 解析；用于历史列表展示 */
  rows_count?: number | null;
  /** 汇率规则：meta.fx_month_label */
  fx_month_label?: string | null;
  snapshot_basename?: string;
}

/** POST /rules/import/fx */
export interface PipelineRuleFxImportResponse {
  kind: string;
  rows: number;
  csv_relative: string;
  entry: {
    kind: string;
    version: number;
    rows_count: number;
    updated_at?: string;
    updated_by?: string | null;
  };
}

/** POST /rules/import/own_flow_template */
export interface PipelineOwnFlowTemplateImportResponse {
  imported: Partial<
    Record<
      string,
      | {
          kind: string;
          version: number;
          rows_count: number;
          updated_at?: string;
          updated_by?: string | null;
        }
      | null
    >
  >;
  row_counts: Record<string, number>;
  csv_roots_relative?: string;
  fx?: { rows?: number; skipped?: boolean; reason?: string };
  /** 后端回传的导入范围（all 或具体 kind） */
  scope?: string;
}

/** POST /rules/import/customer_flow_template */
export interface PipelineCustomerFlowTemplateImportResponse {
  imported: Partial<
    Record<
      string,
      | {
          kind: string;
          version: number;
          rows_count: number;
          updated_at?: string;
          updated_by?: string | null;
        }
      | null
    >
  >;
  row_counts: Record<string, number>;
  csv_roots_relative?: string;
  scope?: string;
}

export interface PipelineCopilotToolCall {
  name: string;
  args: Record<string, unknown>;
  result: unknown;
  elapsed_ms: number;
}

export interface PipelineCopilotReply {
  answer: string;
  thoughts: string[];
  tool_calls: PipelineCopilotToolCall[];
  drafts?: unknown[];
}

export type PipelineCopilotStreamEvent =
  | { type: 'thought'; step: string }
  | ({ type: 'tool_call' } & PipelineCopilotToolCall)
  | { type: 'answer'; answer: string }
  | { type: 'error'; message: string }
  | { type: 'done'; reply: PipelineCopilotReply };

export interface PipelineCopilotHistoryTurn {
  role: 'user' | 'assistant';
  content: string;
}

export interface PipelineCopilotAskRequest {
  task_id: string;
  channel_id?: string | null;
  run_id?: string | null;
  verify_row_id?: string | null;
  question: string;
  /** Completed turns before this request (same Copilot drawer). */
  history?: PipelineCopilotHistoryTurn[];
}

export interface PipelineAgentDraft {
  draft_id: string;
  kind: 'rule_patch' | 'replace_file' | string;
  rule_kind?: string;
  patch?: Record<string, unknown>;
  channel_id?: string;
  rel_path?: string;
  new_content_uri?: string;
  rationale?: string | null;
  status: 'pending' | 'accepted' | 'rejected';
  created_at: string;
}

export interface PipelineAgentDraftsResponse {
  task_id: string;
  drafts: PipelineAgentDraft[];
}

// ---------- Compare ----------

export type PipelineCompareSourceKind = 'run_output' | 'source_file' | 'upload';

export interface PipelineCompareSource {
  kind: PipelineCompareSourceKind;
  channel_id?: string | null;
  run_id?: string | null;
  name?: string | null;
  rel_path?: string | null;
  staged_path?: string | null;
}

export interface PipelineCompareCreateRequest {
  task_id: string;
  left: PipelineCompareSource;
  right: PipelineCompareSource;
  key_cols: string[];
  compare_cols?: string[] | null;
  column_mapping?: Record<string, string> | null;
  numeric_tol?: number;
  normalize_strings?: boolean;
  note?: string | null;
}

export interface PipelineCompareSummary {
  matched_rows: number;
  only_left_rows: number;
  only_right_rows: number;
  diff_cells: number;
  by_column: Record<string, number>;
}

export interface PipelineCompareSideMeta {
  label: string;
  kind: PipelineCompareSourceKind;
  rows: number;
}

export interface PipelineCompareMeta {
  compare_id: string;
  task_id: string;
  left: PipelineCompareSideMeta;
  right: PipelineCompareSideMeta;
  key_cols: string[];
  compare_cols: string[] | null;
  column_mapping: Record<string, string>;
  tolerances: { numeric: number; normalize_strings: boolean };
  alignment: {
    common_columns: string[];
    left_only: string[];
    right_only: string[];
    rename_applied: Record<string, string>;
  };
  summary: PipelineCompareSummary;
  note: string | null;
  created_by: string | null;
  created_at: string;
  duration_ms: number;
}

export interface PipelineCompareReport {
  summary: PipelineCompareSummary;
  alignment: PipelineCompareMeta['alignment'];
  diff_rows: Array<{
    left_index: number;
    right_index: number;
    column: string;
    left_value: unknown;
    right_value: unknown;
  }>;
  diff_total: number;
  only_left_preview: Array<Record<string, unknown>>;
  only_right_preview: Array<Record<string, unknown>>;
}

export interface PipelineCompareListEntry {
  compare_id: string;
  task_id: string;
  left_ref: string;
  right_ref: string;
  status: string;
  report_path: string;
  summary: PipelineCompareSummary;
  created_by: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface PipelineCompareUploadResponse {
  task_id: string;
  staged_path: string;
  name: string;
  size: number;
}

// ---------- Observe ----------

export interface PipelineObserveKpi {
  window_days: number;
  tasks_total: number;
  tasks_completed: number;
  tasks_partial: number;
  tasks_failed: number;
  success_rate: number;
  avg_duration_seconds: number;
  agent_interventions: number;
  intervention_rate: number;
  as_of: string;
}

export interface PipelineObserveCharts {
  window_days: number;
  duration_by_channel: Array<{
    channel: string;
    count: number;
    avg_seconds: number;
    max_seconds: number;
  }>;
  daily_failure_rate: Array<{
    day: string;
    starts: number;
    ends: number;
    failures: number;
    warnings: number;
    failure_rate: number;
  }>;
  rule_events: Array<{ event: string; count: number }>;
  top_slow_channels: Array<{
    channel: string;
    count: number;
    avg_seconds: number;
    max_seconds: number;
  }>;
  top_error_files: Array<{ channel: string; detail: string; count: number }>;
}

export interface PipelineObserveEvent {
  id: number;
  task_id: string;
  channel_id: string | null;
  run_id: string | null;
  event_type: string;
  from_status: string | null;
  to_status: string | null;
  reason_code: string | null;
  reason_detail: string | null;
  payload: unknown;
  created_at: string;
}

export interface PipelineObserveEventsResponse {
  events: PipelineObserveEvent[];
  limit: number;
}

// ---------- Preview ----------

export interface PipelinePreviewSheet {
  name: string;
  headers: string[];
  rows: Array<Array<string | number | boolean>>;
  total_rows: number;
  total_cols: number;
  truncated_rows: boolean;
  truncated_cols: boolean;
}

export interface PipelinePreviewResponse {
  task_id: string;
  channel_id: string;
  run_id: string;
  filename: string;
  /** xlsx | csv | unsupported */
  kind: 'xlsx' | 'csv' | 'unsupported';
  sheets: PipelinePreviewSheet[];
  error: string | null;
}

// ---------- LLM Configs ----------

export interface PipelineLLMConfig {
  id: number;
  name: string;
  provider: string;
  model_name: string;
  api_key_masked: string;
  has_api_key: boolean;
  base_url: string | null;
  temperature: number | null;
  max_tokens: number | null;
  extra_params: string | null;
  remark: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface PipelineLLMConfigsResponse {
  items: PipelineLLMConfig[];
  supported_providers: string[];
}

export interface PipelineLLMConfigCreateRequest {
  name: string;
  provider: string;
  model_name: string;
  api_key: string;
  base_url?: string;
  temperature?: number;
  max_tokens?: number;
  extra_params?: string;
  remark?: string;
}

export interface PipelineLLMConfigUpdateRequest {
  name?: string;
  provider?: string;
  model_name?: string;
  api_key?: string;
  base_url?: string;
  temperature?: number;
  max_tokens?: number;
  extra_params?: string;
  remark?: string;
}

export interface PipelineLLMConfigTestRequest {
  config_id?: number;
  provider?: string;
  model_name?: string;
  api_key?: string;
  base_url?: string;
  temperature?: number;
  max_tokens?: number;
}

export interface PipelineLLMConfigTestResult {
  ok: boolean;
  latency_ms: number;
  message: string;
  sample?: string;
}

export interface PipelineAllocationMergeBaseUploadResponse {
  task_id: string;
  name: string;
  size: number;
  path: string;
  uploaded_at: string;
}

export interface PipelineCostSummaryUploadResponse {
  task_id: string;
  name: string;
  size: number;
  path: string;
  uploaded_at: string;
}
