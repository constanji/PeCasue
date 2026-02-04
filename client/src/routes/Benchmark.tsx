import React, { useState, useEffect, useMemo, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { ChevronLeft } from 'lucide-react';
import { useGetEndpointsQuery } from '~/data-provider';
import { useGetModelsQuery } from '@because/data-provider/react-query';
import { useAuthContext } from '~/hooks/AuthContext';
import { SystemRoles } from '@because/data-provider';
import useAuthRedirect from './useAuthRedirect';

const BENCHMARK_CONFIG_KEY = 'benchmark_config';
const BENCHMARK_CURRENT_TASK_KEY = 'benchmark_current_task_id';

const defaultEvaluationMetrics = [
  { id: 'EX', name: '执行准确率 (Execution Accuracy, EX)', description: '衡量预测SQL与标准答案执行结果匹配程度', checked: true },
  { id: 'R-VES', name: '基于奖励的效率分数 (R-VES)', description: '基于执行时间比率的奖励机制', checked: false },
  { id: 'Soft F1', name: '结果中的表结构相似度 (Soft F1-Score)', description: '预测SQL与标准答案表结构相似度', checked: false },
];

const databases = [
  { id: 'debit_card_specializing', name: '借记卡专业化', description: '借记卡在加油站场景的专业化应用' },
  { id: 'financial', name: '银行金融系统', description: '银行核心业务系统，包含客户、账户、交易等' },
  { id: 'student_club', name: '学生俱乐部', description: '学生俱乐部管理系统' },
  { id: 'thrombosis_prediction', name: '血栓预测', description: '医疗健康领域的血栓预测数据库' },
  { id: 'european_football_2', name: '欧洲足球', description: '欧洲足球联赛数据' },
  { id: 'formula_1', name: '一级方程式赛车', description: 'F1赛车相关数据' },
  { id: 'superhero', name: '超级英雄', description: '超级英雄相关数据' },
  { id: 'codebase_community', name: '代码库社区', description: '代码库社区数据' },
  { id: 'card_games', name: '卡牌游戏', description: '卡牌游戏相关数据' },
  { id: 'toxicology', name: '毒理学', description: '毒理学研究数据' },
  { id: 'california_schools', name: '加州学校', description: '加州学校数据' },
];

export default function Benchmark() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { token, user, isAuthenticated } = useAuthContext();
  useAuthRedirect();

  const handleNewTest = () => {
    setTaskId(null);
    setTaskStatus(null);
    setStatusLogs([]);
    try {
      localStorage.removeItem(BENCHMARK_CURRENT_TASK_KEY);
    } catch {
      // ignore
    }
    setSearchParams({}, { replace: true });
  };

  const [selectedDatabaseType, setSelectedDatabaseType] = useState<'SQLite' | 'MySQL' | 'PostgreSQL'>('SQLite');
  const [selectedDatabase, setSelectedDatabase] = useState<string>('');
  const [selectedEndpoint, setSelectedEndpoint] = useState<string>('');
  const [selectedModel, setSelectedModel] = useState<string>('');
  const [useKnowledge, setUseKnowledge] = useState<boolean>(false);
  const [qaType, setQAType] = useState<'desc' | 'sql' | 'full' | null>(null);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<any>(null);
  const [statusLogs, setStatusLogs] = useState<
    Array<{ time: string; message: string; type: 'info' | 'success' | 'warning' | 'error' }>
  >([]);
  const [evaluationMetrics, setEvaluationMetrics] = useState(defaultEvaluationMetrics);
  const logContainerRef = useRef<HTMLDivElement>(null);

  const { data: endpointsConfig } = useGetEndpointsQuery({ enabled: isAuthenticated });
  const { data: modelsConfig } = useGetModelsQuery();

  const availableEndpoints = useMemo(() => {
    if (!endpointsConfig) return [];
    return Object.keys(endpointsConfig).map((key) => ({
      id: key,
      name: endpointsConfig[key]?.modelDisplayLabel || key,
      type: endpointsConfig[key]?.type || 'custom',
    }));
  }, [endpointsConfig]);

  const availableModels = useMemo(() => {
    if (!selectedEndpoint || !modelsConfig) return [];
    return modelsConfig[selectedEndpoint] || [];
  }, [selectedEndpoint, modelsConfig]);

  useEffect(() => {
    try {
      const raw = localStorage.getItem(BENCHMARK_CONFIG_KEY);
      if (raw) {
        const saved = JSON.parse(raw) as Record<string, unknown>;
        if (saved.selectedDatabaseType) setSelectedDatabaseType(saved.selectedDatabaseType as 'SQLite' | 'MySQL' | 'PostgreSQL');
        if (saved.selectedDatabase != null) setSelectedDatabase(String(saved.selectedDatabase));
        if (saved.selectedEndpoint != null) setSelectedEndpoint(String(saved.selectedEndpoint));
        if (saved.selectedModel != null) setSelectedModel(String(saved.selectedModel));
        if (typeof saved.useKnowledge === 'boolean') setUseKnowledge(saved.useKnowledge);
        if (saved.qaType != null) setQAType(saved.qaType as 'desc' | 'sql' | 'full' | null);
        if (Array.isArray(saved.evaluationMetrics)) setEvaluationMetrics(saved.evaluationMetrics as typeof defaultEvaluationMetrics);
      }
    } catch {
      // ignore invalid stored config
    }
  }, []);

  useEffect(() => {
    const payload = {
      selectedDatabaseType,
      selectedDatabase,
      selectedEndpoint,
      selectedModel,
      useKnowledge,
      qaType,
      evaluationMetrics,
    };
    try {
      localStorage.setItem(BENCHMARK_CONFIG_KEY, JSON.stringify(payload));
    } catch {
      // ignore quota or parse errors
    }
  }, [selectedDatabaseType, selectedDatabase, selectedEndpoint, selectedModel, useKnowledge, qaType, evaluationMetrics]);

  const databaseTypes = [
    { id: 'SQLite', name: 'SQLite' },
    { id: 'MySQL', name: 'MySQL' },
    { id: 'PostgreSQL', name: 'PostgreSQL' },
  ];

  // 仅在有 ?new=1 时重置任务状态；返回页面时不重置
  useEffect(() => {
    if (searchParams.get('new') === '1') {
      setTaskId(null);
      setTaskStatus(null);
      setStatusLogs([]);
      try {
        localStorage.removeItem(BENCHMARK_CURRENT_TASK_KEY);
      } catch {
        // ignore
      }
      setSearchParams({}, { replace: true });
      return;
    }
    // 从结果页返回时恢复当前任务，避免刷新导致状态丢失
    try {
      const saved = localStorage.getItem(BENCHMARK_CURRENT_TASK_KEY);
      if (saved && saved.trim()) setTaskId(saved.trim());
    } catch {
      // ignore
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const authHeaders = useMemo(() => {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (token) headers['Authorization'] = `Bearer ${token}`;
    return headers;
  }, [token]);

  const handleStartBenchmark = async () => {
    if (!selectedDatabase || !selectedDatabaseType || !selectedEndpoint || !selectedModel) {
      alert('请先选择数据库类型、数据库、端点和模型');
      return;
    }

    try {
      const response = await fetch('/api/benchmark/run', {
        method: 'POST',
        headers: authHeaders,
        credentials: 'include',
        body: JSON.stringify({
          datasetId: `${selectedDatabase}_${selectedDatabaseType.toLowerCase()}`,
          sqlDialect: selectedDatabaseType,
          databaseName: selectedDatabase,
          endpointName: selectedEndpoint,
          model: selectedModel,
          toolsConfig: { useKnowledge, qaType },
          evaluationMetrics: evaluationMetrics.filter((m) => m.checked).map((m) => m.id),
        }),
      });
      const data = await response.json();
      if (data.taskId) {
        setTaskId(data.taskId);
        try {
          localStorage.setItem(BENCHMARK_CURRENT_TASK_KEY, data.taskId);
        } catch {
          // ignore
        }
        setStatusLogs([{ time: new Date().toLocaleTimeString(), message: '✅ 测试任务已创建，开始初始化...', type: 'success' }]);
      } else {
        alert(data.error || '启动测试失败');
      }
    } catch (error: any) {
      console.error('Error starting benchmark:', error);
      alert('启动测试失败: ' + (error?.message || String(error)));
    }
  };

  useEffect(() => {
    if (!taskId) return;
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`/api/benchmark/task/${taskId}`, { headers: authHeaders, credentials: 'include' });
        if (!response.ok) {
          if (response.status === 404) {
            try {
              const resultResponse = await fetch(`/api/benchmark/result/${taskId}`, { headers: authHeaders, credentials: 'include' });
              if (resultResponse.ok) {
                const resultData = await resultResponse.json();
                setTaskStatus({ ...resultData, status: 'completed', progress: 100 });
                setStatusLogs((prev) => [...prev, { time: new Date().toLocaleTimeString(), message: '✅ 从历史记录中恢复任务状态', type: 'success' }]);
                clearInterval(interval);
                return;
              }
            } catch {
              // ignore
            }
          }
          return;
        }
        const data = await response.json();
        setTaskStatus(data);
        // 与后端 [BenchmarkService] 日志格式保持一致
        if (Array.isArray(data.statusLogs) && data.statusLogs.length > 0) {
          setStatusLogs(
            data.statusLogs.map((message: string) => ({
              time: '',
              message,
              type: 'info' as const,
            })),
          );
        }
        if (data.status === 'completed' || data.status === 'failed') clearInterval(interval);
      } catch (error) {
        console.error('Error polling task status:', error);
      }
    }, 2000);
    return () => clearInterval(interval);
  }, [taskId, authHeaders]);

  useEffect(() => {
    if (logContainerRef.current) logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
  }, [statusLogs]);

  const getCurrentStageDescription = () => {
    if (!taskStatus) return '';
    const { status, progress, completed, total } = taskStatus;
    if (status === 'pending') return '等待开始...';
    if (status === 'running') {
      if (progress <= 5) return '正在加载数据集';
      if (progress <= 60) return `正在生成 SQL (${completed}/${total})`;
      if (progress < 100) return '正在运行评估';
    }
    if (status === 'completed') return '测试已完成';
    if (status === 'failed') return '测试失败';
    return '处理中...';
  };

  if (!isAuthenticated) return null;

  const isAdmin = user?.role === SystemRoles.ADMIN;
  if (!isAdmin) {
    navigate('/c/new', { replace: true });
    return null;
  }

  return (
    <div className="flex h-full w-full flex-col overflow-hidden bg-background">
      <div className="mb-4 px-4 pt-4">
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-text-primary">基准测试</h1>
            <p className="mt-1 text-sm text-text-secondary">配置数据集、模型端点和评估指标，开始 SQL 基准测试</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              className="btn btn-neutral border-token-border-light relative flex items-center gap-2 rounded-lg px-3 py-2"
              onClick={() => navigate('/c/new')}
              aria-label="返回"
            >
              <ChevronLeft className="h-4 w-4" />
              <span>返回</span>
            </button>
            <button
              type="button"
              className="btn btn-primary rounded-lg px-3 py-2 text-sm font-medium"
              onClick={handleNewTest}
            >
              开启新测试
            </button>
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto px-4 pb-4">
        <div className="mx-auto max-w-6xl">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-6">
              <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
                <h2 className="mb-4 text-lg font-semibold text-text-primary">1. 选择模型</h2>
                <div className="space-y-3">
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">端点</label>
                    <select
                      aria-label="选择端点"
                      value={selectedEndpoint}
                      onChange={(e) => {
                        setSelectedEndpoint(e.target.value);
                        setSelectedModel('');
                      }}
                      className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:border-primary focus:outline-none"
                    >
                      <option value="">-- 选择端点 --</option>
                      {availableEndpoints.map((ep) => (
                        <option key={ep.id} value={ep.id}>
                          {ep.name}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">模型</label>
                    {selectedEndpoint ? (
                      <select
                        aria-label="选择模型"
                        value={selectedModel}
                        onChange={(e) => setSelectedModel(e.target.value)}
                        className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:border-primary focus:outline-none"
                      >
                        <option value="">-- 选择模型 --</option>
                        {availableModels.map((model) => (
                          <option key={model} value={model}>
                            {model}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <div className="rounded-md border border-border-light bg-surface-tertiary px-3 py-2 text-sm text-text-secondary">请先选择端点</div>
                    )}
                  </div>
                </div>
              </div>

              <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
                <h2 className="mb-4 text-lg font-semibold text-text-primary">2. 选择数据库</h2>
                <div className="space-y-3">
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">数据库类型</label>
                    <select
                      aria-label="数据库类型"
                      value={selectedDatabaseType}
                      onChange={(e) => {
                        setSelectedDatabaseType(e.target.value as 'SQLite' | 'MySQL' | 'PostgreSQL');
                        setSelectedDatabase('');
                      }}
                      className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:border-primary focus:outline-none"
                    >
                      {databaseTypes.map((t) => (
                        <option key={t.id} value={t.id}>{t.name}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="mb-2 block text-sm font-medium text-text-primary">数据集</label>
                    <select
                      aria-label="选择数据集"
                      value={selectedDatabase}
                      onChange={(e) => setSelectedDatabase(e.target.value)}
                      className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:border-primary focus:outline-none"
                    >
                      <option value="">-- 请选择数据集 --</option>
                      {databases.map((db) => (
                        <option key={db.id} value={db.id}>{db.name} ({db.id})</option>
                      ))}
                    </select>
                  </div>
                  {selectedDatabase && (() => {
                    const d = databases.find((x) => x.id === selectedDatabase);
                    if (!d) return null;
                    return (
                      <div className="rounded-md border border-border-light bg-surface-tertiary px-3 py-2">
                        <div className="text-sm text-text-primary">
                          <span className="font-medium">已选择:</span> {d.name}
                        </div>
                        <div className="mt-1 text-xs text-text-secondary">
                          数据库类型: <span className="font-medium">{selectedDatabaseType}</span>
                        </div>
                        {d.description && (
                          <div className="mt-1 text-xs text-text-secondary">{d.description}</div>
                        )}
                      </div>
                    );
                  })()}
                </div>
              </div>

              <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
                <h2 className="mb-4 text-lg font-semibold text-text-primary">3. 工具配置</h2>
                <label className="flex cursor-pointer items-center rounded-md border border-border-light bg-surface-primary px-3 py-2 hover:bg-surface-hover">
                  <input
                    type="checkbox"
                    checked={useKnowledge}
                    onChange={(e) => setUseKnowledge(e.target.checked)}
                    className="mr-3 h-4 w-4 rounded border-border-light text-primary focus:ring-primary"
                  />
                  <div className="flex-1">
                    <span className="text-sm font-medium text-text-primary">启用知识库</span>
                    <div className="mt-1 text-xs text-text-secondary">允许模型读取知识库文档以提升SQL生成准确性</div>
                  </div>
                </label>
                {useKnowledge && (
                  <div className="mt-3 space-y-2">
                    {(['desc', 'sql', 'full'] as const).map((t) => (
                      <label key={t} className="flex cursor-pointer items-center">
                        <input type="radio" name="qaType" checked={qaType === t} onChange={() => setQAType(t)} className="mr-2 h-4 w-4 text-primary focus:ring-primary" />
                        <span className="text-sm text-text-primary">
                          {t === 'desc' ? '问题和Description' : t === 'sql' ? '问题和SQL示例' : '问题+Description+SQL示例'}
                        </span>
                      </label>
                    ))}
                  </div>
                )}
              </div>

              <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
                <h2 className="mb-4 text-lg font-semibold text-text-primary">4. 选择评估指标</h2>
                <div className="space-y-3">
                  {evaluationMetrics.map((metric) => (
                    <label key={metric.id} className="flex cursor-pointer items-center rounded-md border border-border-light bg-surface-primary px-3 py-2 hover:bg-surface-hover">
                      <input
                        type="checkbox"
                        checked={metric.checked}
                        onChange={(e) =>
                          setEvaluationMetrics((prev) =>
                            prev.map((m) => (m.id === metric.id ? { ...m, checked: e.target.checked } : m)),
                          )
                        }
                        className="mr-3 h-4 w-4 rounded border-border-light text-primary focus:ring-primary"
                      />
                      <span className="text-sm text-text-primary">{metric.name}</span>
                    </label>
                  ))}
                </div>
              </div>

              <button
                onClick={handleStartBenchmark}
                disabled={!selectedDatabase || !selectedDatabaseType || !selectedEndpoint || !selectedModel}
                className="btn btn-primary relative flex w-full items-center justify-center gap-2 rounded-lg px-4 py-3 text-sm font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                开始测试
              </button>
            </div>

            <div className="space-y-6">
              {taskId && (
                <div className="rounded-lg border border-border-light bg-surface-secondary p-4 shadow-sm">
                  <h2 className="mb-4 text-lg font-semibold text-text-primary">测试进度</h2>
                  {taskStatus ? (
                    <div className="space-y-4">
                      <div className="rounded-md border border-border-light bg-surface-primary p-3">
                        <div className="flex items-center justify-between text-sm">
                          <span className="text-text-secondary">状态:</span>
                          <span className={`font-medium ${taskStatus.status === 'completed' ? 'text-green-600' : taskStatus.status === 'failed' ? 'text-red-600' : 'text-text-primary'}`}>
                            {taskStatus.status === 'completed' ? '已完成' : taskStatus.status === 'failed' ? '失败' : taskStatus.status === 'running' ? '运行中' : taskStatus.status}
                          </span>
                        </div>
                        <div className="mt-2 flex items-center justify-between text-sm">
                          <span className="text-text-secondary">进度:</span>
                          <span className="font-medium text-text-primary">{taskStatus.completed || 0} / {taskStatus.total || 0}</span>
                        </div>
                      </div>
                      <div>
                        <div className="mb-2 flex justify-between text-xs text-text-secondary">
                          <span>{getCurrentStageDescription()}</span>
                          <span>{Math.round(taskStatus.progress || 0)}%</span>
                        </div>
                        <div className="h-2 w-full overflow-hidden rounded-full bg-surface-tertiary">
                          <div className="h-full bg-primary transition-all duration-300" style={{ width: `${taskStatus.progress || 0}%` }} />
                        </div>
                      </div>
                      {statusLogs.length > 0 && (
                        <div className="rounded-md border border-border-light bg-surface-primary p-3">
                          <div className="mb-2 flex items-center justify-between">
                            <div className="text-xs font-medium text-text-secondary">📝 测试日志</div>
                            <button type="button" onClick={() => setStatusLogs([])} className="text-xs text-text-tertiary hover:text-text-secondary">清空</button>
                          </div>
                          <div ref={logContainerRef} className="max-h-48 space-y-1 overflow-y-auto text-xs font-mono" style={{ scrollBehavior: 'smooth' }}>
                            {statusLogs.map((log, index) => (
                              <div
                                key={index}
                                className={`flex items-start gap-2 py-0.5 ${log.type === 'success' ? 'text-green-600' : log.type === 'error' ? 'text-red-600' : log.type === 'warning' ? 'text-yellow-600' : 'text-text-secondary'}`}
                              >
                                {log.time ? (
                                  <span className="shrink-0 text-text-tertiary font-normal">[{log.time}]</span>
                                ) : null}
                                <span className="flex-1 font-mono text-xs">{log.message}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                      {taskStatus.status === 'completed' && (
                        <div className="space-y-2">
                          <div className="flex flex-wrap gap-2">
                            <button
                              type="button"
                              onClick={() => navigate(`/result/${taskId}`)}
                              className="flex flex-1 items-center justify-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2 text-sm text-green-600 hover:bg-green-500/10"
                            >
                              <span>📊 查看详细结果</span>
                              <span>→</span>
                            </button>
                            <button
                              type="button"
                              onClick={handleNewTest}
                              className="btn btn-primary rounded-md px-3 py-2 text-sm"
                            >
                              开启新测试
                            </button>
                          </div>
                          {taskStatus.results && (
                            <div className="rounded-md border border-border-light bg-surface-tertiary p-3">
                              <div className="text-xs font-medium text-text-secondary mb-1">评估结果摘要</div>
                              {Object.keys(taskStatus.results).map((metric) => {
                                const result = taskStatus.results[metric];
                                if (result.error) return <div key={metric} className="text-xs text-red-600">{metric}: 评估失败</div>;
                                return <div key={metric} className="text-xs text-text-primary">{metric}: {result.accuracy !== undefined ? `${result.accuracy.toFixed(2)}%` : '已完成'}</div>;
                              })}
                            </div>
                          )}
                        </div>
                      )}
                      {taskStatus.error && (
                        <div className="rounded-md border border-red-500/20 bg-red-500/5 p-3">
                          <div className="text-sm font-medium text-red-600">错误信息</div>
                          <div className="mt-1 text-xs text-red-500">{taskStatus.error}</div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="flex items-center justify-center py-8">
                      <div className="text-sm text-text-secondary">加载中...</div>
                    </div>
                  )}
                </div>
              )}
              {!taskId && (
                <div className="flex min-h-[400px] items-center justify-center rounded-lg border border-border-light bg-surface-secondary">
                  <div className="text-center">
                    <div className="mb-2 text-4xl">📊</div>
                    <div className="text-sm font-medium text-text-primary">等待开始测试</div>
                    <div className="mt-1 text-xs text-text-secondary">配置完成后点击「开始测试」按钮</div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
