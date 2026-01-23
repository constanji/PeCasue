import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Button, useToastContext } from '@because/client';
import { useQueryClient } from '@tanstack/react-query';
import { QueryKeys } from '@because/data-provider';
import { useGetStartupConfig } from '~/data-provider';
import { useListDataSourcesQuery } from '~/data-provider/DataSources';
import { useLocalize, useAuthContext } from '~/hooks';
import { Plus, Trash2 } from 'lucide-react';
import {
  BulbOutlined,
  InfoCircleOutlined,
  RocketOutlined,
  SmileOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import type { TStartupConfig } from '@because/data-provider';
import type { DataSource } from '@because/data-provider';
import { cn } from '~/utils';

interface PromptsManagementProps {
  startupConfig?: TStartupConfig;
}

interface PromptItem {
  key: string;
  icon: string;
  label: string;
  description: string;
  prompt: string;
}

interface PromptsConfig {
  title?: string;
  items: PromptItem[];
}

interface AgentPromptsConfig {
  global?: PromptsConfig;
  dataSources?: Record<string, PromptsConfig>;
}

const iconOptions = [
  { value: 'bulb', label: '灯泡', icon: <BulbOutlined /> },
  { value: 'info', label: '信息', icon: <InfoCircleOutlined /> },
  { value: 'rocket', label: '火箭', icon: <RocketOutlined /> },
  { value: 'smile', label: '笑脸', icon: <SmileOutlined /> },
  { value: 'warning', label: '警告', icon: <WarningOutlined /> },
];

// 预览组件：显示提示项的实际效果
function PromptItemPreview({ item }: { item: PromptItem }) {
  const getIconComponent = useCallback((iconType: string) => {
    const iconProps = { className: 'text-base' };
    switch (iconType) {
      case 'bulb':
        return <BulbOutlined {...iconProps} style={{ color: '#FFD700' }} />;
      case 'info':
        return <InfoCircleOutlined {...iconProps} style={{ color: '#1890FF' }} />;
      case 'rocket':
        return <RocketOutlined {...iconProps} style={{ color: '#722ED1' }} />;
      case 'smile':
        return <SmileOutlined {...iconProps} style={{ color: '#52C41A' }} />;
      case 'warning':
        return <WarningOutlined {...iconProps} style={{ color: '#FF4D4F' }} />;
      default:
        return <BulbOutlined {...iconProps} style={{ color: '#FFD700' }} />;
    }
  }, []);

  return (
    <div
      className={cn(
        'group relative flex w-52 cursor-default flex-col gap-2 rounded-xl',
        'border border-border-light bg-surface-tertiary px-3 pb-4 pt-3',
        'text-start align-top text-[15px]',
        'shadow-sm transition-all duration-300 ease-in-out',
        'hover:border-border-medium hover:bg-surface-hover hover:shadow-lg'
      )}
    >
      {/* 图标和标签 */}
      <div className="flex items-center gap-2">
        <span className="flex-shrink-0">
          {getIconComponent(item.icon)}
        </span>
        <span className="break-word line-clamp-2 overflow-hidden text-balance text-sm font-semibold text-text-primary">
          {item.label || '未设置标签'}
        </span>
      </div>
      
      {/* 描述 */}
      {item.description && (
        <p className="break-word line-clamp-2 overflow-hidden text-balance break-all text-xs text-text-secondary">
          {item.description}
        </p>
      )}
    </div>
  );
}

export default function PromptsManagement({ startupConfig: propStartupConfig }: PromptsManagementProps) {
  const localize = useLocalize();
  const { showToast } = useToastContext();
  const { token } = useAuthContext();
  const queryClient = useQueryClient();
  const { data: startupConfigFromQuery, refetch } = useGetStartupConfig();
  const { data: dataSourcesResponse } = useListDataSourcesQuery();
  const startupConfig = propStartupConfig || startupConfigFromQuery;

  const [isSaving, setIsSaving] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [dataSource, setDataSource] = useState<'global' | string>('global');
  const [config, setConfig] = useState<AgentPromptsConfig>({
    global: {
      title: '✨ 提示集',
      items: [],
    },
    dataSources: {},
  });

  // 获取数据源列表
  const dataSourcesList = useMemo(() => {
    if (!dataSourcesResponse?.data) {
      return [];
    }
    return dataSourcesResponse.data
      .filter((ds: DataSource) => ds.status === 'active')
      .map((ds: DataSource) => ({
        id: ds._id,
        name: ds.name || '未命名数据源',
      }));
  }, [dataSourcesResponse]);

  // 从 startupConfig 加载配置
  useEffect(() => {
    if (startupConfig?.agentPrompts) {
      const loadedConfig: AgentPromptsConfig = {
        global: startupConfig.agentPrompts.global || {
          title: '✨ 提示集',
          items: [],
        },
        dataSources: startupConfig.agentPrompts.dataSources || {},
      };
      setConfig(loadedConfig);
    }
    setIsLoading(false);
  }, [startupConfig]);

  // 获取当前数据源的配置
  const currentConfig = useMemo(() => {
    if (dataSource === 'global') {
      return config.global || { title: '✨ 提示集', items: [] };
    }
    return config.dataSources?.[dataSource] || { title: '✨ 提示集', items: [] };
  }, [config, dataSource]);

  // 更新当前配置
  const updateCurrentConfig = useCallback(
    (updater: (prev: PromptsConfig) => PromptsConfig) => {
      setConfig((prev) => {
        const newConfig = { ...prev };
        if (dataSource === 'global') {
          newConfig.global = updater(prev.global || { title: '✨ 提示集', items: [] });
        } else {
          newConfig.dataSources = { ...(prev.dataSources || {}) };
          newConfig.dataSources[dataSource] = updater(
            prev.dataSources?.[dataSource] || { title: '✨ 提示集', items: [] },
          );
        }
        return newConfig;
      });
    },
    [dataSource],
  );

  // 添加提示项
  const handleAddItem = useCallback(() => {
    updateCurrentConfig((prev) => ({
      ...prev,
      items: [
        ...prev.items,
        {
          key: `prompt-${Date.now()}`,
          icon: 'bulb',
          label: '',
          description: '',
          prompt: '',
        },
      ],
    }));
  }, [updateCurrentConfig]);

  // 删除提示项
  const handleDeleteItem = useCallback(
    (key: string) => {
      updateCurrentConfig((prev) => ({
        ...prev,
        items: prev.items.filter((item) => item.key !== key),
      }));
    },
    [updateCurrentConfig],
  );

  // 更新提示项
  const handleUpdateItem = useCallback(
    (key: string, field: keyof PromptItem, value: any) => {
      updateCurrentConfig((prev) => ({
        ...prev,
        items: prev.items.map((item) => (item.key === key ? { ...item, [field]: value } : item)),
      }));
    },
    [updateCurrentConfig],
  );

  // 更新标题
  const handleUpdateTitle = useCallback(
    (title: string) => {
      updateCurrentConfig((prev) => ({ ...prev, title }));
    },
    [updateCurrentConfig],
  );

  // 保存配置
  const handleSave = async () => {
    setIsSaving(true);
    try {
      const baseEl = document.querySelector('base');
      const baseHref = baseEl?.getAttribute('href') || '/';
      const apiBase = baseHref.endsWith('/') ? baseHref.slice(0, -1) : baseHref;

      const headers: HeadersInit = {
        'Content-Type': 'application/json',
      };

      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }

      // 调试：打印要保存的配置
      console.log('[PromptsManagement] Saving config:', JSON.stringify(config, null, 2));
      console.log('[PromptsManagement] Current config items:', currentConfig.items);

      const response = await fetch(`${apiBase}/api/config/agent-prompts`, {
        method: 'POST',
        headers,
        credentials: 'include',
        body: JSON.stringify({ agentPrompts: config }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || '保存失败');
      }

      // 清除缓存并刷新配置
      queryClient.invalidateQueries([QueryKeys.startupConfig]);
      await refetch();
      showToast({ status: 'success', message: '提示集配置保存成功' });
    } catch (error: any) {
      console.error('保存提示集配置失败:', error);
      showToast({ status: 'error', message: error.message || '保存失败' });
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex h-32 items-center justify-center text-text-secondary">
        <p className="text-sm">加载中...</p>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-text-primary">提示集管理</h2>
          <p className="mt-1 text-sm text-text-secondary">
            配置初始对话界面中显示的提示集
          </p>
        </div>
        <Button
          type="button"
          onClick={handleSave}
          disabled={isSaving}
          className="btn btn-primary relative flex items-center gap-2 rounded-lg px-3 py-2"
        >
          {isSaving ? '保存中...' : '保存配置'}
        </Button>
      </div>

      <div className="flex-1 overflow-auto">
        <div className="space-y-6">
          {/* 数据源选择 */}
          <div className="rounded-lg border border-border-light bg-surface-secondary p-4">
            <label className="mb-2 block text-sm font-medium text-text-primary">
              数据源
            </label>
            <p className="mb-3 text-xs text-text-secondary">
              选择要配置的提示集数据源，全局提示集将应用于所有智能体
            </p>
            <select
              value={dataSource}
              onChange={(e) => setDataSource(e.target.value)}
              className="w-full max-w-xs rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:border-primary focus:outline-none"
            >
              <option value="global">全局提示集</option>
              {dataSourcesList.map((ds) => (
                <option key={ds.id} value={ds.id}>
                  {ds.name}
                </option>
              ))}
            </select>
          </div>

          {/* 整体预览 */}
          {currentConfig.items.length > 0 && (
            <div className="rounded-lg border border-border-light bg-surface-secondary p-4">
              <div className="mb-3">
                <h3 className="text-sm font-semibold text-text-primary">整体预览</h3>
                <p className="mt-1 text-xs text-text-secondary">
                  预览所有提示项在聊天界面中的显示效果
                </p>
              </div>
              <div className="flex min-h-[150px] flex-wrap items-start justify-center gap-3 rounded-md border border-border-light bg-surface-tertiary p-4">
                {currentConfig.items.length === 0 ? (
                  <p className="py-8 text-sm text-text-secondary">暂无提示项</p>
                ) : (
                  currentConfig.items.map((item) => (
                    <PromptItemPreview key={item.key} item={item} />
                  ))
                )}
              </div>
            </div>
          )}

          {/* 提示项列表 */}
          <div className="rounded-lg border border-border-light bg-surface-secondary p-4">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h3 className="text-sm font-semibold text-text-primary">提示项</h3>
                <p className="mt-1 text-xs text-text-secondary">
                  配置提示集的各个提示项，用户点击后将自动发送对应的提示内容
                </p>
              </div>
              <button
                type="button"
                onClick={handleAddItem}
                className="btn btn-neutral flex items-center gap-2 rounded-lg px-3 py-2 text-sm"
              >
                <Plus className="h-4 w-4" />
                添加提示项
              </button>
            </div>

            {currentConfig.items.length === 0 ? (
              <div className="rounded-md border border-border-light bg-surface-primary p-6 text-center">
                <p className="text-sm text-text-secondary">
                  暂无提示项，点击"添加提示项"开始配置
                </p>
              </div>
            ) : (
              <div className="space-y-6">
                {currentConfig.items.map((item, index) => (
                  <div
                    key={item.key}
                    className="rounded-md border border-border-light bg-surface-primary p-4"
                  >
                    <div className="mb-4 flex items-center justify-between">
                      <h4 className="text-sm font-medium text-text-primary">
                        提示项 {index + 1}
                      </h4>
                      <button
                        type="button"
                        onClick={() => {
                          if (
                            window.confirm('确定要删除这个提示项吗？')
                          ) {
                            handleDeleteItem(item.key);
                          }
                        }}
                        className="flex items-center gap-1 rounded px-2 py-1 text-sm text-red-500 hover:bg-surface-tertiary"
                      >
                        <Trash2 className="h-4 w-4" />
                        删除
                      </button>
                    </div>

                    <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
                      {/* 左侧：配置表单 */}
                      <div className="space-y-3">
                        {/* 图标类型 */}
                        <div>
                          <label className="mb-1 block text-xs font-medium text-text-primary">
                            图标类型
                          </label>
                          <select
                            value={item.icon}
                            onChange={(e) =>
                              handleUpdateItem(item.key, 'icon', e.target.value)
                            }
                            className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary focus:border-primary focus:outline-none"
                          >
                            {iconOptions.map((option) => (
                              <option key={option.value} value={option.value}>
                                {option.label}
                              </option>
                            ))}
                          </select>
                        </div>

                        {/* 标签 */}
                        <div>
                          <label className="mb-1 block text-xs font-medium text-text-primary">
                            标签
                          </label>
                          <input
                            type="text"
                            value={item.label}
                            onChange={(e) =>
                              handleUpdateItem(item.key, 'label', e.target.value)
                            }
                            placeholder="输入提示项标签"
                            className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary placeholder:text-text-tertiary focus:border-primary focus:outline-none"
                          />
                        </div>

                        {/* 描述 */}
                        <div>
                          <label className="mb-1 block text-xs font-medium text-text-primary">
                            描述
                          </label>
                          <textarea
                            value={item.description}
                            onChange={(e) =>
                              handleUpdateItem(
                                item.key,
                                'description',
                                e.target.value,
                              )
                            }
                            placeholder="输入提示项描述"
                            rows={2}
                            className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary placeholder:text-text-tertiary focus:border-primary focus:outline-none"
                          />
                        </div>

                        {/* 提示内容 */}
                        <div>
                          <label className="mb-1 block text-xs font-medium text-text-primary">
                            提示内容
                          </label>
                          <p className="mb-1 text-xs text-text-secondary">
                            点击后发送的提示内容（留空则使用标签）
                          </p>
                          <textarea
                            value={item.prompt}
                            onChange={(e) =>
                              handleUpdateItem(item.key, 'prompt', e.target.value)
                            }
                            placeholder="输入点击后发送的提示内容（留空则使用标签）"
                            rows={3}
                            className="w-full rounded-md border border-border-light bg-surface-primary px-3 py-2 text-sm text-text-primary placeholder:text-text-tertiary focus:border-primary focus:outline-none"
                          />
                        </div>
                      </div>

                      {/* 右侧：实时预览 */}
                      <div className="flex flex-col">
                        <label className="mb-2 block text-xs font-medium text-text-primary">
                          预览效果
                        </label>
                        <p className="mb-3 text-xs text-text-secondary">
                          实时预览提示项在聊天界面中的显示效果
                        </p>
                        <div className="flex min-h-[120px] items-start justify-center rounded-md border border-border-light bg-surface-tertiary p-4">
                          <PromptItemPreview item={item} />
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

