import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { PipelineApi } from './client';
import type {
  PipelineAllocationTemplatesStatus,
  PipelineAgentDraftsResponse,
  PipelineAutoZipResponse,
  PipelineChannelCatalog,
  PipelineClearUploadsResponse,
  PipelineAllocationMergeBaseUploadResponse,
  PipelineCostSummaryUploadResponse,
  PipelineClearChannelExtractedResponse,
  PipelineChannelZipReplaceResponse,
  PipelineChannelDetail,
  PipelineChannelRun,
  PipelineChannelPrescanResponse,
  PipelineClassification,
  PipelineCompareCreateRequest,
  PipelineCompareListEntry,
  PipelineCompareMeta,
  PipelineCompareReport,
  PipelineCompareUploadResponse,
  PipelineCopilotAskRequest,
  PipelineCopilotReply,
  PipelineCreateTaskRequest,
  PipelineHealth,
  PipelineObserveCharts,
  PipelineObserveEventsResponse,
  PipelineObserveKpi,
  PipelineOwnFlowTemplateImportResponse,
  PipelineCustomerFlowTemplateImportResponse,
  PipelineFinalMergeInventory,
  PipelinePreviewResponse,
  PipelineRuleFxImportResponse,
  PipelineRuleKind,
  PipelineRuleManifest,
  PipelineRuleTable,
  PipelineRuleVersion,
  PipelineRunResponse,
  PipelineTaskSummary,
  PipelineUploadResponse,
} from './types';

export const PIPELINE_QUERY_KEYS = {
  health: ['pipeline', 'health'] as const,
  tasks: ['pipeline', 'tasks'] as const,
  task: (id: string) => ['pipeline', 'task', id] as const,
  timeline: (id: string) => ['pipeline', 'timeline', id] as const,
  channelCatalog: ['pipeline', 'channelCatalog'] as const,
  classification: (id: string) => ['pipeline', 'classification', id] as const,
  channel: (taskId: string, channelId: string) =>
    ['pipeline', 'channel', taskId, channelId] as const,
  channelRuns: (taskId: string, channelId: string) =>
    ['pipeline', 'channelRuns', taskId, channelId] as const,
  channelRun: (taskId: string, channelId: string, runId: string) =>
    ['pipeline', 'channelRun', taskId, channelId, runId] as const,
  rulesManifest: ['pipeline', 'rules', 'manifest'] as const,
  rule: (kind: string) => ['pipeline', 'rule', kind] as const,
  ruleVersions: (kind: string) => ['pipeline', 'rule', kind, 'versions'] as const,
  allocationTemplates: ['pipeline', 'rules', 'allocationTemplates'] as const,
  agentDrafts: (taskId: string) => ['pipeline', 'agent', 'drafts', taskId] as const,
  compares: (taskId: string | null | undefined) =>
    ['pipeline', 'compares', taskId ?? 'all'] as const,
  compare: (taskId: string, compareId: string) =>
    ['pipeline', 'compare', taskId, compareId] as const,
  compareReport: (taskId: string, compareId: string) =>
    ['pipeline', 'compare', taskId, compareId, 'report'] as const,
  observeKpi: (windowDays: number) => ['pipeline', 'observe', 'kpi', windowDays] as const,
  observeCharts: (windowDays: number) =>
    ['pipeline', 'observe', 'charts', windowDays] as const,
  observeEvents: (params: Record<string, unknown>) =>
    ['pipeline', 'observe', 'events', params] as const,
  preview: (taskId: string, channelId: string, runId: string, filename: string) =>
    ['pipeline', 'preview', taskId, channelId, runId, filename] as const,
  channelPrescan: (taskId: string, channelId: string) =>
    ['pipeline', 'channelPrescan', taskId, channelId] as const,
  finalMergeInventory: (id: string) => ['pipeline', 'finalMergeInventory', id] as const,
};

export function usePipelineHealth() {
  return useQuery<PipelineHealth, Error>(
    PIPELINE_QUERY_KEYS.health,
    () => PipelineApi.health(),
    {
      staleTime: 30_000,
      retry: 1,
    },
  );
}

export function usePipelineTasks() {
  return useQuery<{ tasks: PipelineTaskSummary[] }, Error>(
    PIPELINE_QUERY_KEYS.tasks,
    () => PipelineApi.listTasks(),
    {
      staleTime: 5 * 60_000,
      refetchOnWindowFocus: false,
    },
  );
}

export function usePipelineTask(taskId: string | null | undefined) {
  return useQuery(
    taskId ? PIPELINE_QUERY_KEYS.task(taskId) : ['pipeline', 'task', 'noop'],
    () => (taskId ? PipelineApi.getTask(taskId) : Promise.reject(new Error('no task'))),
    {
      enabled: !!taskId,
      staleTime: 30_000,
    },
  );
}

export function usePipelineFinalMergeInventory(taskId: string | null | undefined) {
  return useQuery<PipelineFinalMergeInventory, Error>(
    taskId
      ? PIPELINE_QUERY_KEYS.finalMergeInventory(taskId)
      : ['pipeline', 'finalMergeInventory', 'noop'],
    () =>
      taskId
        ? PipelineApi.getFinalMergeInventory(taskId)
        : Promise.reject(new Error('no task')),
    { enabled: !!taskId, staleTime: 5_000 },
  );
}

export function usePipelineChannelCatalog() {
  return useQuery<PipelineChannelCatalog, Error>(
    PIPELINE_QUERY_KEYS.channelCatalog,
    () => PipelineApi.channelCatalog(),
    { staleTime: 60 * 60_000 },
  );
}

export function useChannelPrescan(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  const enabled =
    !!taskId &&
    !!channelId &&
    (channelId === 'bill' || channelId === 'own_flow');
  return useQuery<PipelineChannelPrescanResponse, Error>(
    taskId && channelId
      ? PIPELINE_QUERY_KEYS.channelPrescan(taskId, channelId)
      : ['pipeline', 'channelPrescan', 'noop'],
    () =>
      taskId && channelId
        ? PipelineApi.getChannelPrescan(taskId, channelId)
        : Promise.reject(new Error('no task/channel')),
    {
      enabled,
      staleTime: 15_000,
      retry: 1,
    },
  );
}

export function usePipelineClassification(taskId: string | null | undefined) {
  return useQuery<PipelineClassification, Error>(
    taskId
      ? PIPELINE_QUERY_KEYS.classification(taskId)
      : ['pipeline', 'classification', 'noop'],
    () =>
      taskId
        ? PipelineApi.getClassification(taskId)
        : Promise.reject(new Error('no task')),
    { enabled: !!taskId, staleTime: 5_000 },
  );
}

export function useCreatePipelineTask() {
  const qc = useQueryClient();
  return useMutation<{ task_id: string }, Error, PipelineCreateTaskRequest>(
    (payload) => PipelineApi.createTask(payload),
    {
      onSuccess: () => {
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
      },
    },
  );
}

export function useUploadZipAuto(taskId: string | null | undefined) {
  const qc = useQueryClient();
  return useMutation<PipelineAutoZipResponse, Error, File>(
    (file) => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.uploadZipAuto(taskId, file);
    },
    {
      onSuccess: () => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries({ queryKey: ['pipeline', 'channel', taskId] });
        qc.invalidateQueries({ queryKey: ['pipeline', 'channelPrescan', taskId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

export function useClearTaskUploads(taskId: string | null | undefined) {
  const qc = useQueryClient();
  return useMutation<PipelineClearUploadsResponse, Error, void>(
    () => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.clearTaskUploads(taskId);
    },
    {
      onSuccess: () => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries({ queryKey: ['pipeline', 'channel', taskId] });
        qc.invalidateQueries({ queryKey: ['pipeline', 'channelPrescan', taskId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

export function useClearChannelExtracted(taskId: string | null | undefined) {
  const qc = useQueryClient();
  return useMutation<PipelineClearChannelExtractedResponse, Error, string>(
    (channelId) => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.clearChannelExtracted(taskId, channelId);
    },
    {
      onSuccess: (_, channelId) => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries({ queryKey: ['pipeline', 'channel', taskId] });
        qc.invalidateQueries({ queryKey: ['pipeline', 'channelPrescan', taskId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

export function useUploadCostSummary(taskId: string | null | undefined) {
  const qc = useQueryClient();
  return useMutation<PipelineCostSummaryUploadResponse, Error, File>(
    (file) => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.uploadCostSummary(taskId, file);
    },
    {
      onSuccess: () => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, 'final_merge'));
      },
    },
  );
}

export function useUploadAllocationMergeBase(taskId: string | null | undefined) {
  const qc = useQueryClient();
  return useMutation<PipelineAllocationMergeBaseUploadResponse, Error, File>(
    (file) => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.uploadAllocationMergeBase(taskId, file);
    },
    {
      onSuccess: () => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, 'allocation_base'));
      },
    },
  );
}

export function useUploadChannelZipReplace(taskId: string | null | undefined) {
  const qc = useQueryClient();
  type Vars = { channelId: string; file: File };
  return useMutation<PipelineChannelZipReplaceResponse, Error, Vars>(
    ({ channelId, file }) => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.uploadChannelZipReplace(taskId, channelId, file);
    },
    {
      onSuccess: (_, variables) => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, variables.channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, variables.channelId));
        qc.invalidateQueries({ queryKey: ['pipeline', 'channel', taskId] });
        qc.invalidateQueries({ queryKey: ['pipeline', 'channelPrescan', taskId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

export function useUploadPerChannel(taskId: string | null | undefined) {
  const qc = useQueryClient();
  type Vars = Parameters<typeof PipelineApi.uploadPerChannel>[1];
  return useMutation<PipelineUploadResponse, Error, Vars>(
    (files) => {
      if (!taskId) return Promise.reject(new Error('no task'));
      return PipelineApi.uploadPerChannel(taskId, files);
    },
    {
      onSuccess: () => {
        if (!taskId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries({ queryKey: ['pipeline', 'channelPrescan', taskId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

export function usePipelineChannel(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
  options?: { refetchInterval?: number | false },
) {
  return useQuery<PipelineChannelDetail, Error>(
    taskId && channelId
      ? PIPELINE_QUERY_KEYS.channel(taskId, channelId)
      : ['pipeline', 'channel', 'noop'],
    () =>
      taskId && channelId
        ? PipelineApi.getChannel(taskId, channelId)
        : Promise.reject(new Error('no task/channel')),
    {
      enabled: !!taskId && !!channelId,
      staleTime: 2_000,
      refetchInterval: options?.refetchInterval ?? false,
    },
  );
}

export function usePipelineChannelRuns(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  return useQuery<
    { task_id: string; channel_id: string; runs: PipelineChannelRun[] },
    Error
  >(
    taskId && channelId
      ? PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId)
      : ['pipeline', 'channelRuns', 'noop'],
    () =>
      taskId && channelId
        ? PipelineApi.listChannelRuns(taskId, channelId)
        : Promise.reject(new Error('no task/channel')),
    {
      enabled: !!taskId && !!channelId,
      staleTime: 2_000,
    },
  );
}

export function useDeleteChannelRun(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  const qc = useQueryClient();
  return useMutation<
    { deleted: boolean; task_id: string; channel_id: string; run_id: string },
    Error,
    string
  >(
    (runId) => {
      if (!taskId || !channelId) return Promise.reject(new Error('no task/channel'));
      return PipelineApi.deleteChannelRun(taskId, channelId, runId);
    },
    {
      onSuccess: () => {
        if (!taskId || !channelId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.timeline(taskId));
      },
    },
  );
}

// ---------- Rules ----------

export function useRulesManifest() {
  return useQuery<PipelineRuleManifest, Error>(
    PIPELINE_QUERY_KEYS.rulesManifest,
    () => PipelineApi.getRulesManifest(),
    { staleTime: 30_000 },
  );
}

export function useRule(
  kind: PipelineRuleKind | null | undefined,
  options?: { userRole?: string | null },
) {
  const roleKey = options?.userRole ?? '__norole__';
  return useQuery(
    kind ? [...PIPELINE_QUERY_KEYS.rule(kind), roleKey] : ['pipeline', 'rule', 'noop'],
    () =>
      kind
        ? PipelineApi.getRule(kind, {
            headers: options?.userRole
              ? { 'X-PeCause-User-Role': options.userRole }
              : undefined,
          })
        : Promise.reject(new Error('no kind')),
    { enabled: !!kind, staleTime: 10_000 },
  );
}

export function useRuleVersions(kind: PipelineRuleKind | null | undefined) {
  return useQuery(
    kind ? PIPELINE_QUERY_KEYS.ruleVersions(kind) : ['pipeline', 'rule-versions', 'noop'],
    () =>
      kind
        ? PipelineApi.getRuleVersions(kind)
        : Promise.reject(new Error('no kind')),
    { enabled: !!kind, staleTime: 10_000 },
  );
}

export function usePutRule(kind: PipelineRuleKind | null | undefined) {
  const qc = useQueryClient();
  return useMutation<
    Awaited<ReturnType<typeof PipelineApi.putRule>>,
    Error,
    { table: PipelineRuleTable; note?: string }
  >(
    ({ table, note }) => {
      if (!kind) return Promise.reject(new Error('no kind'));
      return PipelineApi.putRule(kind, table, note);
    },
    {
      onSuccess: () => {
        if (!kind) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.rule(kind));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.ruleVersions(kind));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.rulesManifest);
      },
    },
  );
}

export function useRollbackRule(kind: PipelineRuleKind | null | undefined) {
  const qc = useQueryClient();
  return useMutation<
    Awaited<ReturnType<typeof PipelineApi.rollbackRule>>,
    Error,
    { targetVersion: number; note?: string }
  >(
    ({ targetVersion, note }) => {
      if (!kind) return Promise.reject(new Error('no kind'));
      return PipelineApi.rollbackRule(kind, targetVersion, note);
    },
    {
      onSuccess: () => {
        if (!kind) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.rule(kind));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.ruleVersions(kind));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.rulesManifest);
      },
    },
  );
}

export function useAllocationTemplatesStatus() {
  return useQuery<PipelineAllocationTemplatesStatus, Error>(
    PIPELINE_QUERY_KEYS.allocationTemplates,
    () => PipelineApi.getAllocationTemplatesStatus(),
    { staleTime: 30_000 },
  );
}

export function useUploadAllocationTemplate() {
  const qc = useQueryClient();
  return useMutation<
    Awaited<ReturnType<typeof PipelineApi.uploadAllocationTemplate>>,
    Error,
    { templateKind: 'quickbi' | 'citihk' | 'cost_allocate'; file: File }
  >(({ templateKind, file }) => PipelineApi.uploadAllocationTemplate(templateKind, file), {
    onSuccess: () => {
      qc.invalidateQueries(PIPELINE_QUERY_KEYS.allocationTemplates);
    },
  });
}

const OWN_FLOW_IMPORT_KINDS: PipelineRuleKind[] = [
  'account_mapping',
  'fee_mapping',
  'fx',
  'own_flow_processing',
];

const CUSTOMER_IMPORT_KINDS: PipelineRuleKind[] = [
  'customer_mapping',
  'customer_fee_mapping',
  'customer_branch_mapping',
];

function invalidateImportedKinds(
  qc: ReturnType<typeof useQueryClient>,
  kinds: PipelineRuleKind[],
) {
  qc.invalidateQueries(PIPELINE_QUERY_KEYS.rulesManifest);
  kinds.forEach((k) => {
    qc.invalidateQueries(PIPELINE_QUERY_KEYS.rule(k));
    qc.invalidateQueries(PIPELINE_QUERY_KEYS.ruleVersions(k));
  });
}

export function useImportRuleFx() {
  const qc = useQueryClient();
  return useMutation<
    PipelineRuleFxImportResponse,
    Error,
    { file: File; note?: string; fxMonthLabel?: string }
  >(({ file, note, fxMonthLabel }) => PipelineApi.importRuleFx(file, note, fxMonthLabel),
    {
      onSuccess: () => {
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.rulesManifest);
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.rule('fx'));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.ruleVersions('fx'));
      },
    },
  );
}

export function useImportOwnFlowTemplate() {
  const qc = useQueryClient();
  return useMutation<
    PipelineOwnFlowTemplateImportResponse,
    Error,
    { file: File; note?: string; scope?: string; fxMonthLabel?: string }
  >(({ file, note, scope, fxMonthLabel }) =>
    PipelineApi.importOwnFlowTemplate(file, note, scope, fxMonthLabel), {
    onSuccess: (data) => {
      const keys = Object.keys(data.imported ?? {}).filter((k) =>
        OWN_FLOW_IMPORT_KINDS.includes(k as PipelineRuleKind),
      ) as PipelineRuleKind[];
      invalidateImportedKinds(qc, keys.length ? keys : OWN_FLOW_IMPORT_KINDS);
    },
  });
}

export function useImportCustomerFlowTemplate() {
  const qc = useQueryClient();
  return useMutation<
    PipelineCustomerFlowTemplateImportResponse,
    Error,
    { file: File; note?: string; scope?: string }
  >(({ file, note, scope }) => PipelineApi.importCustomerFlowTemplate(file, note, scope), {
    onSuccess: (data) => {
      const keys = Object.keys(data.imported ?? {}).filter((k) =>
        CUSTOMER_IMPORT_KINDS.includes(k as PipelineRuleKind),
      ) as PipelineRuleKind[];
      invalidateImportedKinds(qc, keys.length ? keys : CUSTOMER_IMPORT_KINDS);
    },
  });
}

export function useReplaceSourceFile(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  const qc = useQueryClient();
  return useMutation<
    Awaited<ReturnType<typeof PipelineApi.replaceSourceFile>>,
    Error,
    { relPath: string; file: File }
  >(
    ({ relPath, file }) => {
      if (!taskId || !channelId) return Promise.reject(new Error('no task/channel'));
      return PipelineApi.replaceSourceFile(taskId, channelId, relPath, file);
    },
    {
      onSuccess: () => {
        if (!taskId || !channelId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries({ queryKey: ['pipeline', 'channelPrescan', taskId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
      },
    },
  );
}

export function useReplaceRunOutputFile(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
  runId: string | null | undefined,
) {
  const qc = useQueryClient();
  return useMutation<
    Awaited<ReturnType<typeof PipelineApi.replaceRunOutputFile>>,
    Error,
    { outputFileName: string; file: File }
  >(
    ({ outputFileName, file }) => {
      if (!taskId || !channelId || !runId) {
        return Promise.reject(new Error('no task/channel/run'));
      }
      return PipelineApi.replaceRunOutputFile(taskId, channelId, runId, outputFileName, file);
    },
    {
      onSuccess: () => {
        if (!taskId || !channelId || !runId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRun(taskId, channelId, runId));
        qc.invalidateQueries({ queryKey: ['pipeline', 'preview', taskId, channelId, runId] });
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.timeline(taskId));
      },
    },
  );
}

// ---------- Observe ----------

export function usePipelineObserveKpi(windowDays = 1) {
  return useQuery<PipelineObserveKpi, Error>(
    PIPELINE_QUERY_KEYS.observeKpi(windowDays),
    () => PipelineApi.observeKpi(windowDays),
    { staleTime: 30_000, refetchInterval: 30_000 },
  );
}

export function usePipelineObserveCharts(windowDays = 7) {
  return useQuery<PipelineObserveCharts, Error>(
    PIPELINE_QUERY_KEYS.observeCharts(windowDays),
    () => PipelineApi.observeCharts(windowDays),
    { staleTime: 60_000 },
  );
}

export function usePipelineObserveEvents(params: {
  limit?: number;
  task_id?: string;
  channel_id?: string;
  event_type?: string;
} = {}) {
  return useQuery<PipelineObserveEventsResponse, Error>(
    PIPELINE_QUERY_KEYS.observeEvents(params),
    () => PipelineApi.observeEvents(params),
    { staleTime: 5_000, refetchInterval: 10_000 },
  );
}

// ---------- Preview ----------

export function usePipelinePreview(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
  runId: string | null | undefined,
  filename: string | null | undefined,
) {
  const enabled = !!taskId && !!channelId && !!runId && !!filename;
  return useQuery<PipelinePreviewResponse, Error>(
    PIPELINE_QUERY_KEYS.preview(
      taskId || '',
      channelId || '',
      runId || '',
      filename || '',
    ),
    () =>
      PipelineApi.previewRunFile(
        taskId as string,
        channelId as string,
        runId as string,
        filename as string,
      ),
    {
      enabled,
      staleTime: 30_000,
      // Treat structured "unsupported / error" responses as success — only
      // network-level failures should retry.
      retry: 1,
    },
  );
}

// ---------- Compare ----------

export function usePipelineCompares(taskId: string | null | undefined) {
  return useQuery<{ compares: PipelineCompareListEntry[] }, Error>(
    PIPELINE_QUERY_KEYS.compares(taskId),
    () => PipelineApi.listCompares(taskId || undefined),
    { staleTime: 30_000 },
  );
}

export function usePipelineCompareReport(
  taskId: string | null | undefined,
  compareId: string | null | undefined,
) {
  return useQuery<PipelineCompareReport, Error>(
    taskId && compareId
      ? PIPELINE_QUERY_KEYS.compareReport(taskId, compareId)
      : ['pipeline', 'compare', 'noop'],
    () =>
      taskId && compareId
        ? PipelineApi.getCompareReport(taskId, compareId)
        : Promise.reject(new Error('no compare')),
    { enabled: !!taskId && !!compareId, staleTime: 60_000 },
  );
}

export function useCreateCompare() {
  const qc = useQueryClient();
  return useMutation<PipelineCompareMeta, Error, PipelineCompareCreateRequest>(
    (payload) => PipelineApi.createCompare(payload),
    {
      onSuccess: (_data, vars) => {
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.compares(vars.task_id));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.compares(null));
      },
    },
  );
}

export function useUploadCompareFile(taskId: string | null | undefined) {
  return useMutation<PipelineCompareUploadResponse, Error, File>((file) => {
    if (!taskId) return Promise.reject(new Error('no task'));
    return PipelineApi.uploadCompareFile(taskId, file);
  });
}

// ---------- Agent / Copilot ----------

export function usePipelineCopilotAsk() {
  const qc = useQueryClient();
  return useMutation<PipelineCopilotReply, Error, PipelineCopilotAskRequest>(
    (payload) => PipelineApi.agentAsk(payload),
    {
      onSuccess: (_data, vars) => {
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.agentDrafts(vars.task_id));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(vars.task_id));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.timeline(vars.task_id));
        if (vars.channel_id) {
          qc.invalidateQueries(
            PIPELINE_QUERY_KEYS.channel(vars.task_id, vars.channel_id),
          );
        }
      },
    },
  );
}

export function usePipelineAgentDrafts(taskId: string | null | undefined) {
  return useQuery<PipelineAgentDraftsResponse, Error>(
    taskId
      ? PIPELINE_QUERY_KEYS.agentDrafts(taskId)
      : ['pipeline', 'agent', 'drafts', 'noop'],
    () =>
      taskId
        ? PipelineApi.agentDrafts(taskId)
        : Promise.reject(new Error('no task')),
    {
      enabled: !!taskId,
      staleTime: 5_000,
    },
  );
}

export type PipelineTriggerChannelRunPayload = {
  note?: string | null;
  allocation_phase?: string;
  allocation_options?: Record<string, unknown>;
};

export function useTriggerChannelRun(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  const qc = useQueryClient();
  return useMutation<PipelineRunResponse, Error, PipelineTriggerChannelRunPayload | void>(
    (payload) => {
      if (!taskId || !channelId) return Promise.reject(new Error('no task/channel'));
      return PipelineApi.triggerChannelRun(taskId, channelId, payload ?? {});
    },
    {
      onSuccess: () => {
        if (!taskId || !channelId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

export function useCancelChannelRun(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  const qc = useQueryClient();
  return useMutation<{ task_id: string; channel_id: string; cancelled: boolean }, Error, void>(
    () => {
      if (!taskId || !channelId) return Promise.reject(new Error('no task/channel'));
      return PipelineApi.cancelChannelRun(taskId, channelId);
    },
    {
      onSuccess: () => {
        if (!taskId || !channelId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
      },
    },
  );
}

export function useConfirmChannel(
  taskId: string | null | undefined,
  channelId: string | null | undefined,
) {
  const qc = useQueryClient();
  return useMutation<
    { task_id: string; channel_id: string; status: string; already?: boolean },
    Error,
    void
  >(
    () => {
      if (!taskId || !channelId) return Promise.reject(new Error('no task/channel'));
      return PipelineApi.confirmChannel(taskId, channelId);
    },
    {
      onSuccess: () => {
        if (!taskId || !channelId) return;
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channel(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.channelRuns(taskId, channelId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.classification(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.task(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.tasks);
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.timeline(taskId));
        qc.invalidateQueries(PIPELINE_QUERY_KEYS.finalMergeInventory(taskId));
      },
    },
  );
}

// ---------- LLM Configs ----------

export function useLLMConfigs() {
  return useQuery(
    ['pipeline', 'llm-configs'],
    () => PipelineApi.listLLMConfigs(),
    { staleTime: 30_000 },
  );
}

export function useCreateLLMConfig() {
  const qc = useQueryClient();
  return useMutation(
    (payload: import('./types').PipelineLLMConfigCreateRequest) =>
      PipelineApi.createLLMConfig(payload),
    {
      onSuccess: () => {
        qc.invalidateQueries(['pipeline', 'llm-configs']);
      },
    },
  );
}

export function useUpdateLLMConfig() {
  const qc = useQueryClient();
  return useMutation(
    ({ configId, payload }: { configId: number; payload: import('./types').PipelineLLMConfigUpdateRequest }) =>
      PipelineApi.updateLLMConfig(configId, payload),
    {
      onSuccess: () => {
        qc.invalidateQueries(['pipeline', 'llm-configs']);
      },
    },
  );
}

export function useDeleteLLMConfig() {
  const qc = useQueryClient();
  return useMutation(
    (configId: number) => PipelineApi.deleteLLMConfig(configId),
    {
      onSuccess: () => {
        qc.invalidateQueries(['pipeline', 'llm-configs']);
      },
    },
  );
}

export function useActivateLLMConfig() {
  const qc = useQueryClient();
  return useMutation(
    (configId: number) => PipelineApi.activateLLMConfig(configId),
    {
      onSuccess: () => {
        qc.invalidateQueries(['pipeline', 'llm-configs']);
      },
    },
  );
}

export function useTestLLMConfig() {
  return useMutation(
    (payload: import('./types').PipelineLLMConfigTestRequest) =>
      PipelineApi.testLLMConfig(payload),
  );
}
