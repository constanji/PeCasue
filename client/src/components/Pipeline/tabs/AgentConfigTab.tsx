import React, { useState } from 'react';
import {
  CheckCircle2,
  Loader2,
  Plus,
  RefreshCcw,
  Save,
  TestTube2,
  Trash2,
  XCircle,
  Zap,
} from 'lucide-react';
import {
  useLLMConfigs,
  useCreateLLMConfig,
  useUpdateLLMConfig,
  useDeleteLLMConfig,
  useActivateLLMConfig,
  useTestLLMConfig,
} from '~/data-provider';
import type {
  PipelineLLMConfig,
  PipelineLLMConfigCreateRequest,
  PipelineLLMConfigTestResult,
} from '~/data-provider';
import { cn } from '~/utils';

const PROVIDERS = ['openai', 'anthropic'] as const;

const EMPTY_FORM: PipelineLLMConfigCreateRequest = {
  name: '',
  provider: 'openai',
  model_name: 'gpt-4o',
  api_key: '',
  base_url: '',
  temperature: undefined,
  max_tokens: undefined,
  remark: '',
};

function ConfigCard({
  config,
  onEdit,
  onDelete,
  onActivate,
  onTest,
}: {
  config: PipelineLLMConfig;
  onEdit: () => void;
  onDelete: () => void;
  onActivate: () => void;
  onTest: () => void;
}) {
  return (
    <div
      className={cn(
        'rounded-lg border p-4 transition-colors',
        config.is_active
          ? 'border-green-500/50 bg-green-500/5'
          : 'border-border-light bg-surface-primary',
      )}
    >
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-text-primary">{config.name}</span>
            {config.is_active && (
              <span className="inline-flex items-center gap-1 rounded-full bg-green-500/20 px-2 py-0.5 text-[10px] font-medium text-green-400">
                <Zap className="h-2.5 w-2.5" /> 激活中
              </span>
            )}
          </div>
          <div className="mt-1 flex items-center gap-3 text-xs text-text-secondary">
            <span className="font-mono">{config.provider}</span>
            <span>·</span>
            <span className="font-mono">{config.model_name}</span>
            <span>·</span>
            <span>Key: {config.has_api_key ? config.api_key_masked : '未设置'}</span>
          </div>
          {config.base_url && (
            <div className="mt-1 font-mono text-[11px] text-text-tertiary">
              Base URL: {config.base_url}
            </div>
          )}
          {config.remark && (
            <div className="mt-1 text-xs text-text-tertiary">{config.remark}</div>
          )}
          <div className="mt-1 text-[10px] text-text-tertiary">
            创建于 {new Date(config.created_at).toLocaleString()}
          </div>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            type="button"
            onClick={onTest}
            className="rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
            title="测试连接"
          >
            <TestTube2 className="h-3.5 w-3.5" />
          </button>
          {!config.is_active && (
            <button
              type="button"
              onClick={onActivate}
              className="rounded-md border border-green-500/40 bg-green-500/5 px-2 py-1 text-xs text-green-400 hover:bg-green-500/15"
              title="激活此配置"
            >
              激活
            </button>
          )}
          <button
            type="button"
            onClick={onEdit}
            className="rounded-md border border-border-light px-2 py-1 text-xs text-text-secondary hover:bg-surface-hover"
          >
            编辑
          </button>
          {!config.is_active && (
            <button
              type="button"
              onClick={onDelete}
              className="rounded-md border border-red-500/30 px-2 py-1 text-xs text-red-400 hover:bg-red-500/10"
              title="删除"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

function ConfigForm({
  initial,
  supportedProviders,
  onCancel,
  onSave,
  onTest,
}: {
  initial?: PipelineLLMConfig | null;
  supportedProviders: string[];
  onCancel: () => void;
  onSave: (data: PipelineLLMConfigCreateRequest) => void;
  onTest: (data: PipelineLLMConfigCreateRequest) => void;
}) {
  const [form, setForm] = useState<PipelineLLMConfigCreateRequest>({
    name: initial?.name ?? '',
    provider: initial?.provider ?? 'openai',
    model_name: initial?.model_name ?? 'gpt-4o',
    api_key: '',
    base_url: initial?.base_url ?? '',
    temperature: initial?.temperature ?? undefined,
    max_tokens: initial?.max_tokens ?? undefined,
    remark: initial?.remark ?? '',
  });

  const update = (patch: Partial<PipelineLLMConfigCreateRequest>) =>
    setForm((f) => ({ ...f, ...patch }));

  return (
    <div className="space-y-3 rounded-lg border border-green-500/30 bg-surface-primary p-4">
      <div className="grid grid-cols-2 gap-3">
        <label className="block text-xs">
          <span className="mb-1 block text-text-secondary">名称 *</span>
          <input
            value={form.name}
            onChange={(e) => update({ name: e.target.value })}
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
            placeholder="如 production-gpt4o"
          />
        </label>
        <label className="block text-xs">
          <span className="mb-1 block text-text-secondary">Provider *</span>
          <select
            value={form.provider}
            onChange={(e) => update({ provider: e.target.value })}
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
          >
            {supportedProviders.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </label>
        <label className="block text-xs">
          <span className="mb-1 block text-text-secondary">模型名 *</span>
          <input
            value={form.model_name}
            onChange={(e) => update({ model_name: e.target.value })}
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
            placeholder="gpt-4o"
          />
        </label>
        <label className="block text-xs">
          <span className="mb-1 block text-text-secondary">API Key *</span>
          <input
            type="password"
            value={form.api_key}
            onChange={(e) => update({ api_key: e.target.value })}
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
            placeholder={initial ? '留空则不修改' : 'sk-...'}
          />
        </label>
        <label className="block text-xs col-span-2">
          <span className="mb-1 block text-text-secondary">Base URL（可选）</span>
          <input
            value={form.base_url ?? ''}
            onChange={(e) => update({ base_url: e.target.value || undefined })}
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
            placeholder="https://api.openai.com/v1（默认）"
          />
        </label>
        <label className="block text-xs">
          <span className="mb-1 block text-text-secondary">Temperature</span>
          <input
            type="number"
            step="0.1"
            min="0"
            max="2"
            value={form.temperature ?? ''}
            onChange={(e) =>
              update({ temperature: e.target.value ? Number(e.target.value) : undefined })
            }
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
          />
        </label>
        <label className="block text-xs">
          <span className="mb-1 block text-text-secondary">Max Tokens</span>
          <input
            type="number"
            min="1"
            max="200000"
            value={form.max_tokens ?? ''}
            onChange={(e) =>
              update({ max_tokens: e.target.value ? Number(e.target.value) : undefined })
            }
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
          />
        </label>
        <label className="block text-xs col-span-2">
          <span className="mb-1 block text-text-secondary">备注</span>
          <input
            value={form.remark ?? ''}
            onChange={(e) => update({ remark: e.target.value || undefined })}
            className="w-full rounded-md border border-border-light bg-surface-secondary px-2.5 py-1.5 text-sm text-text-primary focus:border-green-500 focus:outline-none"
          />
        </label>
      </div>
      <div className="flex items-center gap-2 pt-1">
        <button
          type="button"
          onClick={() => onSave(form)}
          disabled={!form.name || !form.model_name || (!initial && !form.api_key)}
          className="inline-flex items-center gap-1 rounded-md bg-green-500 px-3 py-1.5 text-xs font-medium text-white hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-50"
        >
          <Save className="h-3.5 w-3.5" />
          {initial ? '更新' : '创建'}
        </button>
        <button
          type="button"
          onClick={() => onTest(form)}
          disabled={!form.model_name}
          className="inline-flex items-center gap-1 rounded-md border border-border-light px-3 py-1.5 text-xs text-text-secondary hover:bg-surface-hover disabled:cursor-not-allowed disabled:opacity-50"
        >
          <TestTube2 className="h-3.5 w-3.5" />
          测试连接
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="rounded-md px-3 py-1.5 text-xs text-text-secondary hover:bg-surface-hover"
        >
          取消
        </button>
      </div>
    </div>
  );
}

export default function AgentConfigTab() {
  const configs = useLLMConfigs();
  const createMut = useCreateLLMConfig();
  const updateMut = useUpdateLLMConfig();
  const deleteMut = useDeleteLLMConfig();
  const activateMut = useActivateLLMConfig();
  const testMut = useTestLLMConfig();

  const [mode, setMode] = useState<'list' | 'create' | 'edit'>('list');
  const [editId, setEditId] = useState<number | null>(null);
  const [testResult, setTestResult] = useState<PipelineLLMConfigTestResult | null>(null);

  const items = configs.data?.items ?? [];
  const supportedProviders = configs.data?.supported_providers ?? [...PROVIDERS];
  const editing = editId !== null ? items.find((i) => i.id === editId) : null;

  const handleSave = (data: PipelineLLMConfigCreateRequest) => {
    if (mode === 'edit' && editId !== null) {
      const payload: Record<string, unknown> = { ...data };
      if (!payload.api_key) delete payload.api_key;
      updateMut.mutate(
        { configId: editId, payload },
        { onSuccess: () => { setMode('list'); setEditId(null); } },
      );
    } else {
      createMut.mutate(data, {
        onSuccess: () => setMode('list'),
      });
    }
  };

  const handleTest = (data: PipelineLLMConfigCreateRequest) => {
    setTestResult(null);
    if (mode === 'edit' && editId !== null) {
      testMut.mutate({ config_id: editId }, { onSuccess: setTestResult });
    } else if (data.api_key && data.model_name) {
      testMut.mutate(
        { provider: data.provider, model_name: data.model_name, api_key: data.api_key, base_url: data.base_url },
        { onSuccess: setTestResult },
      );
    }
  };

  const handleDelete = (id: number) => {
    if (window.confirm('确认删除此 LLM 配置？')) {
      deleteMut.mutate(id);
    }
  };

  return (
    <div className="space-y-4 p-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold text-text-primary">大模型 Agent 配置</h3>
          <p className="mt-0.5 text-xs text-text-secondary">
            管理 Pipeline Copilot 使用的 LLM 连接。激活的配置将用于"问 Agent"的 ReAct 推理。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => configs.refetch()}
            className="rounded-md border border-border-light p-1.5 text-text-secondary hover:bg-surface-hover"
            title="刷新"
          >
            <RefreshCcw className="h-3.5 w-3.5" />
          </button>
          {mode === 'list' && (
            <button
              type="button"
              onClick={() => { setMode('create'); setEditId(null); setTestResult(null); }}
              className="inline-flex items-center gap-1 rounded-md bg-green-500 px-3 py-1.5 text-xs font-medium text-white hover:brightness-110"
            >
              <Plus className="h-3.5 w-3.5" />
              新建配置
            </button>
          )}
        </div>
      </div>

      {/* Status hint */}
      {items.length === 0 && mode === 'list' && (
        <div className="rounded-lg border border-dashed border-border-medium p-6 text-center text-xs text-text-secondary">
          尚无 LLM 配置。点击「新建配置」添加一个，或在 <code>pipeline-svc/.env</code> 中设置环境变量（首次启动会自动迁移）。
        </div>
      )}

      {/* Test result */}
      {testResult && (
        <div
          className={cn(
            'rounded-lg border p-3 text-xs',
            testResult.ok
              ? 'border-green-500/40 bg-green-500/5 text-green-300'
              : 'border-red-500/40 bg-red-500/5 text-red-300',
          )}
        >
          <div className="flex items-center gap-2">
            {testResult.ok ? <CheckCircle2 className="h-4 w-4" /> : <XCircle className="h-4 w-4" />}
            <span className="font-medium">{testResult.message}</span>
            {testResult.latency_ms > 0 && <span>· {testResult.latency_ms} ms</span>}
          </div>
          {testResult.sample && (
            <pre className="mt-2 max-h-20 overflow-auto rounded bg-surface-primary p-2 text-[11px] text-text-primary">
              {testResult.sample}
            </pre>
          )}
        </div>
      )}

      {/* Form */}
      {(mode === 'create' || (mode === 'edit' && editing)) && (
        <ConfigForm
          initial={editing}
          supportedProviders={supportedProviders}
          onCancel={() => { setMode('list'); setEditId(null); setTestResult(null); }}
          onSave={handleSave}
          onTest={handleTest}
        />
      )}

      {/* Loading */}
      {configs.isLoading && (
        <div className="flex items-center gap-2 text-xs text-text-secondary">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          加载中…
        </div>
      )}

      {/* Config list */}
      {mode === 'list' && (
        <div className="space-y-2">
          {items.map((cfg) => (
            <ConfigCard
              key={cfg.id}
              config={cfg}
              onEdit={() => { setMode('edit'); setEditId(cfg.id); setTestResult(null); }}
              onDelete={() => handleDelete(cfg.id)}
              onActivate={() => activateMut.mutate(cfg.id)}
              onTest={() => {
                setTestResult(null);
                testMut.mutate({ config_id: cfg.id }, { onSuccess: setTestResult });
              }}
            />
          ))}
        </div>
      )}

      {/* Mutation loading */}
      {(createMut.isLoading || updateMut.isLoading) && (
        <div className="flex items-center gap-2 text-xs text-text-secondary">
          <Loader2 className="h-3.5 w-3.5 animate-spin text-green-500" />
          保存中…
        </div>
      )}
      {testMut.isLoading && (
        <div className="flex items-center gap-2 text-xs text-text-secondary">
          <Loader2 className="h-3.5 w-3.5 animate-spin text-green-500" />
          正在测试连接…
        </div>
      )}
    </div>
  );
}