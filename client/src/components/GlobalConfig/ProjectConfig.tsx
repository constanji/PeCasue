import React, { useState } from 'react';
import { useToastContext } from '@because/client';
import { useListDataSourcesQuery } from '~/data-provider/DataSources';
import type { DataSource } from '@because/data-provider';
import { Server, Database } from 'lucide-react';
import { cn } from '~/utils';

export default function ProjectConfig() {
  const { showToast } = useToastContext();
  const [selectedDataSourceId, setSelectedDataSourceId] = useState<string | null>(null);

  // 获取数据源列表
  const { data: dataSourcesResponse, isLoading } = useListDataSourcesQuery();
  const dataSources = dataSourcesResponse?.data || [];
  const selectedDataSource = selectedDataSourceId 
    ? dataSources.find((ds: DataSource) => ds._id === selectedDataSourceId)
    : null;

  const handleDataSourceChange = (dataSourceId: string) => {
    setSelectedDataSourceId(dataSourceId);
    showToast({
      message: '数据源已选择',
      status: 'success',
    });
  };

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="mb-4">
        <div>
          <h3 className="text-lg font-semibold text-text-primary">项目配置</h3>
          <p className="mt-1 text-sm text-text-secondary">
            配置项目相关设置，包括数据源选择等
          </p>
        </div>
      </div>

      {/* 数据源选择 */}
      <div className="mb-4 rounded-lg border border-border-light bg-surface-secondary p-4">
        <div className="mb-2 flex items-center gap-2">
          <Server className="h-4 w-4 text-text-secondary" />
          <label className="text-sm font-medium text-text-primary">选择数据源：</label>
        </div>
        {isLoading ? (
          <div className="py-4 text-center text-sm text-text-secondary">加载中...</div>
        ) : (
          <div className="space-y-2">
            <select
              value={selectedDataSourceId || ''}
              onChange={(e) => handleDataSourceChange(e.target.value)}
              className="w-full rounded border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:outline-none focus:ring-2 focus:ring-primary"
              aria-label="选择数据源"
              title="选择数据源"
            >
              <option value="">-- 请选择数据源 --</option>
              {dataSources.map((ds: DataSource) => (
                <option key={ds._id} value={ds._id}>
                  {ds.name} ({ds.type} - {ds.database})
                </option>
              ))}
            </select>
            {selectedDataSource && (
              <div className="mt-3 rounded-lg border border-border-light bg-surface-primary p-3">
                <div className="flex items-start gap-3">
                  <Database className="h-5 w-5 text-text-secondary mt-0.5" />
                  <div className="flex-1">
                    <div className="font-medium text-text-primary">{selectedDataSource.name}</div>
                    <div className="mt-1 flex flex-wrap gap-2 text-xs text-text-secondary">
                      <span className="rounded bg-surface-secondary px-2 py-1">
                        类型: {selectedDataSource.type}
                      </span>
                      <span className="rounded bg-surface-secondary px-2 py-1">
                        数据库: {selectedDataSource.database}
                      </span>
                      <span className="rounded bg-surface-secondary px-2 py-1">
                        {selectedDataSource.host}:{selectedDataSource.port}
                      </span>
                      <span className={cn(
                        "rounded px-2 py-1",
                        selectedDataSource.status === 'active' 
                          ? "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200"
                          : "bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200"
                      )}>
                        状态: {selectedDataSource.status === 'active' ? '已启用' : '未启用'}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* 其他配置区域（预留） */}
      <div className="flex-1 rounded-lg border border-border-light bg-surface-secondary p-4">
        <div className="text-sm text-text-secondary">
          工具启用状态/功能配置
        </div>
      </div>
    </div>
  );
}

