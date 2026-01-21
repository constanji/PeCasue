import React, { useState, useEffect } from 'react';
import { OGDialog, OGDialogContent, OGDialogHeader, OGDialogTitle, useToastContext, Spinner } from '@because/client';
import { Database, RefreshCw, X } from 'lucide-react';
import { cn } from '~/utils';
import { dataService } from '@because/data-provider';
import type { DataSource } from '@because/data-provider';
import { Button } from '@because/client';

interface DatabaseSchemaDialogProps {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  dataSource: DataSource | null;
}

interface DatabaseSchema {
  success: boolean;
  database: string;
  schema: Record<string, {
    columns: Array<{
      column_name: string;
      data_type: string;
      is_nullable: boolean | string;
      column_key: string;
      column_comment: string;
      column_default: any;
    }>;
    indexes: Array<{
      index_name: string;
      column_name: string;
      non_unique: number;
    }>;
  }>;
}

export default function DatabaseSchemaDialog({ isOpen, onOpenChange, dataSource }: DatabaseSchemaDialogProps) {
  const { showToast } = useToastContext();
  const [schema, setSchema] = useState<DatabaseSchema | null>(null);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);

  useEffect(() => {
    if (isOpen && dataSource) {
      loadSchema();
    } else {
      // 关闭对话框时重置状态
      setSchema(null);
      setSelectedTable(null);
    }
  }, [isOpen, dataSource]);

  const loadSchema = async () => {
    if (!dataSource?._id) return;
    
    try {
      setLoadingSchema(true);
      const response = await dataService.getDataSourceSchema({ id: dataSource._id });
      if (response.success && response.data) {
        setSchema(response.data);
      } else {
        showToast({
          message: response.error || '获取数据库结构失败',
          status: 'error',
        });
      }
    } catch (error) {
      showToast({
        message: `获取数据库结构失败: ${error instanceof Error ? error.message : '未知错误'}`,
        status: 'error',
      });
    } finally {
      setLoadingSchema(false);
    }
  };

  const tables = schema ? Object.keys(schema.schema) : [];

  if (!dataSource) {
    return null;
  }

  return (
    <OGDialog open={isOpen} onOpenChange={onOpenChange}>
      <OGDialogContent showCloseButton={false} className="flex max-h-[90vh] w-11/12 max-w-4xl flex-col">
        <OGDialogHeader>
          <div className="flex items-center justify-between">
            <div>
              <OGDialogTitle className="text-xl font-semibold text-text-primary">
                数据库表结构
              </OGDialogTitle>
              <p className="mt-1 text-sm text-text-secondary">
                {dataSource.name} ({dataSource.type.toUpperCase()}) · {dataSource.database}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Button
                onClick={loadSchema}
                disabled={loadingSchema}
                variant="ghost"
                className="btn btn-neutral border-token-border-light relative flex items-center gap-2 rounded-lg px-3 py-2"
              >
                {loadingSchema ? (
                  <Spinner className="h-4 w-4" />
                ) : (
                  <RefreshCw className="h-4 w-4" />
                )}
                刷新
              </Button>
              <Button
                onClick={() => onOpenChange(false)}
                variant="ghost"
                className="btn btn-neutral border-token-border-light relative flex items-center gap-2 rounded-lg px-3 py-2"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </OGDialogHeader>

        <div className="flex-1 overflow-auto mt-4">
          {loadingSchema ? (
            <div className="flex h-full items-center justify-center py-12">
              <div className="text-center">
                <Spinner className="h-8 w-8 mx-auto mb-4 text-text-primary" />
                <p className="text-text-secondary">正在加载数据库结构...</p>
              </div>
            </div>
          ) : !schema ? (
            <div className="flex h-full items-center justify-center py-12">
              <div className="text-center">
                <Database className="h-12 w-12 mx-auto mb-4 text-text-tertiary" />
                <p className="text-text-secondary mb-4">尚未加载数据库结构</p>
                <Button
                  onClick={loadSchema}
                  disabled={loadingSchema}
                  className="btn btn-primary relative flex items-center gap-2 rounded-lg px-4 py-2"
                >
                  <Database className="h-4 w-4" />
                  加载数据库结构
                </Button>
              </div>
            </div>
          ) : (
            <div className="space-y-2">
              <div className="mb-4 flex items-center justify-between">
                <div className="text-sm text-text-secondary">
                  共 {tables.length} 张表
                </div>
              </div>
              {tables.length === 0 ? (
                <div className="text-center py-12">
                  <p className="text-text-secondary">该数据库中没有表</p>
                </div>
              ) : (
                tables.map((tableName) => {
                  const tableInfo = schema.schema[tableName];
                  const isSelected = selectedTable === tableName;
                  return (
                    <div
                      key={tableName}
                      className={cn(
                        'rounded-lg border p-4 cursor-pointer transition-colors',
                        isSelected
                          ? 'border-green-500 bg-green-50 dark:bg-green-900/20'
                          : 'border-border-light bg-surface-secondary hover:bg-surface-hover',
                      )}
                      onClick={() => setSelectedTable(isSelected ? null : tableName)}
                    >
                      <div className="flex items-center justify-between mb-2">
                        <h4 className="font-medium text-text-primary">{tableName}</h4>
                        <div className="text-xs text-text-secondary">
                          {tableInfo.columns.length} 列
                        </div>
                      </div>
                      {isSelected && (
                        <div className="mt-3 space-y-2">
                          <div className="text-sm font-medium text-text-secondary mb-2">列信息:</div>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                            {tableInfo.columns.map((col) => (
                              <div
                                key={col.column_name}
                                className="text-xs p-2 rounded bg-surface-primary border border-border-light"
                              >
                                <div className="flex items-center gap-2 flex-wrap">
                                  <span className="font-medium text-text-primary">
                                    {col.column_name}
                                  </span>
                                  <span className="text-text-secondary">({col.data_type})</span>
                                  {col.column_key === 'PRI' && (
                                    <span className="px-1 py-0.5 text-xs bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200 rounded">
                                      主键
                                    </span>
                                  )}
                                  {col.column_key === 'UNI' && (
                                    <span className="px-1 py-0.5 text-xs bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 rounded">
                                      唯一
                                    </span>
                                  )}
                                </div>
                                {col.column_comment && (
                                  <div className="text-text-secondary mt-1">
                                    {col.column_comment}
                                  </div>
                                )}
                                {col.column_default !== null && col.column_default !== undefined && (
                                  <div className="text-text-tertiary mt-1 text-xs">
                                    默认值: {String(col.column_default)}
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                          {tableInfo.indexes && tableInfo.indexes.length > 0 && (
                            <div className="mt-3">
                              <div className="text-sm font-medium text-text-secondary mb-2">索引:</div>
                              <div className="flex flex-wrap gap-2">
                                {Object.entries(
                                  tableInfo.indexes.reduce((acc: Record<string, string[]>, idx) => {
                                    if (!acc[idx.index_name]) {
                                      acc[idx.index_name] = [];
                                    }
                                    acc[idx.index_name].push(idx.column_name);
                                    return acc;
                                  }, {})
                                ).map(([indexName, columns]) => (
                                  <div
                                    key={indexName}
                                    className="text-xs px-2 py-1 rounded bg-surface-primary border border-border-light"
                                  >
                                    <span className="font-medium text-text-primary">{indexName}:</span>{' '}
                                    <span className="text-text-secondary">{columns.join(', ')}</span>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })
              )}
            </div>
          )}
        </div>
      </OGDialogContent>
    </OGDialog>
  );
}

