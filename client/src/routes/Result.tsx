import React, { useEffect, useMemo, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useAuthContext } from '~/hooks/AuthContext';
import { SystemRoles } from '@because/data-provider';
import useAuthRedirect from './useAuthRedirect';

export default function Result() {
  const { taskId } = useParams<{ taskId: string }>();
  const navigate = useNavigate();
  const { token } = useAuthContext();
  useAuthRedirect();

  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedMetrics, setExpandedMetrics] = useState<Set<string>>(new Set());
  const [sqlComparison, setSqlComparison] = useState<any>(null);
  const [loadingComparison, setLoadingComparison] = useState(false);

  const authHeaders = useMemo(() => {
    const headers: Record<string, string> = {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    return headers;
  }, [token]);

  useEffect(() => {
    if (!taskId) {
      setError('任务ID不存在');
      setLoading(false);
      return;
    }
    const fetchResult = async () => {
      try {
        const response = await fetch(`/api/benchmark/result/${taskId}`, { headers: authHeaders, credentials: 'include' });
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        const data = await response.json();
        setResult(data);
      } catch (err: any) {
        setError(err.message || '获取结果失败');
      } finally {
        setLoading(false);
      }
    };
    fetchResult();
  }, [taskId, authHeaders]);

  const fetchSQLComparison = async () => {
    if (!taskId || loadingComparison) return;
    setLoadingComparison(true);
    try {
      const response = await fetch(`/api/benchmark/sql-comparison/${taskId}`, { headers: authHeaders, credentials: 'include' });
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      const data = await response.json();
      setSqlComparison(data);
    } catch (err) {
      console.error('Failed to fetch SQL comparison:', err);
    } finally {
      setLoadingComparison(false);
    }
  };

  const toggleSQLComparison = (metric: string) => {
    const newExpanded = new Set(expandedMetrics);
    if (newExpanded.has(metric)) {
      newExpanded.delete(metric);
    } else {
      newExpanded.add(metric);
      if (!sqlComparison) fetchSQLComparison();
    }
    setExpandedMetrics(newExpanded);
  };

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-center">
          <div className="mb-4 text-4xl">⏳</div>
          <div className="text-lg text-text-primary">加载结果中...</div>
        </div>
      </div>
    );
  }

  if (error || !result) {
    return (
      <div className="flex h-full flex-col overflow-hidden bg-background">
        <div className="flex flex-1 items-center justify-center p-6">
          <div className="text-center">
            <div className="mb-4 text-4xl">❌</div>
            <div className="text-lg text-text-primary">{error || '结果不存在'}</div>
            <div className="mt-4 flex flex-wrap justify-center gap-3">
              <button
                type="button"
                onClick={() => navigate('/benchmark')}
                className="rounded-lg border border-border-light bg-surface-secondary px-4 py-2 text-sm hover:bg-surface-hover"
              >
                返回测试页面
              </button>
              <button
                type="button"
                onClick={() => navigate('/benchmark?new=1')}
                className="btn btn-primary rounded-lg px-4 py-2 text-sm"
              >
                开启新测试
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  const { results, config, completedAt, totalDuration } = result;

  const formatDuration = (ms: number | null) => {
    if (!ms) return '未知';
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    if (hours > 0) return `${hours}小时${minutes % 60}分钟${seconds % 60}秒`;
    if (minutes > 0) return `${minutes}分钟${seconds % 60}秒`;
    return `${seconds}秒`;
  };

  return (
    <div className="flex h-full flex-col overflow-hidden bg-background">
      <div className="flex-1 overflow-y-auto p-6">
        <div className="mx-auto max-w-6xl">
          <div className="mb-6 flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={() => navigate('/benchmark')}
              className="text-sm text-text-secondary hover:text-text-primary"
            >
              ← 返回测试页面
            </button>
            <button
              type="button"
              onClick={() => navigate('/benchmark?new=1')}
              className="btn btn-primary rounded-lg px-4 py-2 text-sm font-medium"
            >
              开启新测试
            </button>
          </div>
          <h1 className="text-3xl font-bold text-text-primary">测试结果详情</h1>
          <p className="mt-2 text-sm text-text-secondary">
            任务ID: {taskId} | 完成时间: {new Date(completedAt).toLocaleString('zh-CN')} | 总耗时: {formatDuration(totalDuration)}
          </p>
          <div className="mb-6 rounded-lg border border-border-light bg-surface-secondary p-4">
          <h2 className="mb-3 text-lg font-semibold text-text-primary">测试配置</h2>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div><span className="text-text-secondary">数据库:</span><span className="ml-2 font-medium text-text-primary">{config.databaseName}</span></div>
            <div><span className="text-text-secondary">SQL方言:</span><span className="ml-2 font-medium text-text-primary">{config.sqlDialect}</span></div>
            <div><span className="text-text-secondary">端点:</span><span className="ml-2 font-medium text-text-primary">{config.endpointConfig?.name}</span></div>
            <div><span className="text-text-secondary">模型:</span><span className="ml-2 font-medium text-text-primary">{config.modelConfig?.model}</span></div>
          </div>
          </div>

          <div className="space-y-4">
          <h2 className="text-lg font-semibold text-text-primary">评估结果</h2>
          {results && Object.keys(results).length > 0 ? (
            Object.entries(results).map(([metric, res]: [string, any]) => (
              <div key={metric} className="rounded-lg border border-border-light bg-surface-secondary p-4">
                <div className="mb-3 flex items-center justify-between">
                  <h3 className="text-base font-semibold text-text-primary">
                    {metric === 'EX' ? '执行准确率 (EX)' : metric === 'R-VES' ? '基于奖励的效率分数 (R-VES)' : metric === 'Soft F1' ? '生成SQL表结构相似度 (Soft F1-Score)' : metric}
                  </h3>
                  {res.error ? (
                    <span className="rounded-full bg-red-500/20 px-3 py-1 text-xs text-red-600">失败</span>
                  ) : (
                    <span className="rounded-full bg-green-500/20 px-3 py-1 text-xs text-green-600">成功</span>
                  )}
                </div>
                {res.error ? (
                  <div className="rounded-md border border-red-500/20 bg-red-500/5 p-3">
                    <div className="text-sm font-medium text-red-600">错误信息</div>
                    <div className="mt-1 text-xs text-red-500 font-mono whitespace-pre-wrap break-words">{res.error}</div>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {res.accuracy !== undefined && (
                      <div className="flex items-center justify-between rounded-md bg-surface-primary p-3">
                        <span className="text-sm text-text-secondary">总体准确率</span>
                        <span className="text-2xl font-bold text-primary">{res.accuracy.toFixed(2)}%</span>
                      </div>
                    )}
                    <div className="grid grid-cols-3 gap-3">
                      {res.simple !== undefined && (
                        <div className="rounded-md bg-surface-primary p-3 text-center">
                          <div className="text-xs text-text-secondary">简单</div>
                          <div className="mt-1 text-lg font-semibold text-text-primary">{res.simple.toFixed(2)}%</div>
                        </div>
                      )}
                      {res.moderate !== undefined && (
                        <div className="rounded-md bg-surface-primary p-3 text-center">
                          <div className="text-xs text-text-secondary">中等</div>
                          <div className="mt-1 text-lg font-semibold text-text-primary">{res.moderate.toFixed(2)}%</div>
                        </div>
                      )}
                      {res.challenging !== undefined && (
                        <div className="rounded-md bg-surface-primary p-3 text-center">
                          <div className="text-xs text-text-secondary">困难</div>
                          <div className="mt-1 text-lg font-semibold text-text-primary">{res.challenging.toFixed(2)}%</div>
                        </div>
                      )}
                    </div>
                    {res.total !== undefined && <div className="text-xs text-text-secondary text-center">总计: {res.total} 个问题</div>}
                    <div className="mt-3 border-t border-border-light pt-3">
                      <button
                        type="button"
                        onClick={() => toggleSQLComparison(metric)}
                        className="flex w-full items-center justify-between rounded-md bg-surface-primary px-3 py-2 text-sm text-text-primary hover:bg-surface-primary/80 transition-colors"
                      >
                        <span className="flex items-center gap-2">{expandedMetrics.has(metric) ? '▼' : '▶'} 查看SQL对比</span>
                        {loadingComparison && expandedMetrics.has(metric) && <span className="text-xs text-text-secondary">加载中...</span>}
                      </button>
                      {expandedMetrics.has(metric) && sqlComparison && (
                        <div className="mt-3 max-h-96 space-y-3 overflow-y-auto rounded-md border border-border-light bg-surface-primary p-3">
                          {sqlComparison.comparison?.length > 0 ? (
                            sqlComparison.comparison.map((item: any) => (
                              <div key={item.index} className="rounded-md border border-border-light bg-background p-3">
                                <div className="mb-2 text-xs font-semibold text-text-secondary">问题 #{item.index + 1} | 数据库: {item.db_id}</div>
                                <div className="mb-2">
                                  <div className="mb-1 text-xs font-medium text-green-600">生成的SQL:</div>
                                  <div className="rounded bg-green-500/10 p-2 text-xs font-mono text-text-primary whitespace-pre-wrap break-words">{item.predictedSQL || '(空)'}</div>
                                </div>
                                <div>
                                  <div className="mb-1 text-xs font-medium text-blue-600">标准答案SQL:</div>
                                  <div className="rounded bg-blue-500/10 p-2 text-xs font-mono text-text-primary whitespace-pre-wrap break-words">{item.groundTruthSQL || '(空)'}</div>
                                </div>
                              </div>
                            ))
                          ) : (
                            <div className="text-center text-sm text-text-secondary">暂无SQL对比数据</div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))
          ) : (
            <div className="rounded-lg border border-border-light bg-surface-secondary p-8 text-center">
              <div className="text-text-secondary">暂无评估结果</div>
            </div>
          )}
          </div>
        </div>
      </div>
    </div>
  );
}
