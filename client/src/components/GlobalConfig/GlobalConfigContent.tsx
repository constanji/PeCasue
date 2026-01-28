import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import type { TStartupConfig } from '@because/data-provider';
import { useGetStartupConfig } from '~/data-provider';
import { cn } from '~/utils';
import AgentsManagement from './AgentsManagement';
import MCPManagement from './MCPManagement';
import EndpointsConfig from './EndpointsConfig';
import UsersManagement from './UsersManagement';
import FeaturesManagement from './FeaturesManagement';
import MarketplaceContent from './MarketplaceContent';
import AvailableToolsManagement from './AvailableToolsManagement';
import PromptsManagement from './PromptsManagement';

interface GlobalConfigContentProps {
  startupConfig?: TStartupConfig;
}

type TabType =
  | 'modelSpecs'
  | 'agents'
  | 'marketplace'
  | 'mcp'
  | 'availableTools'
  | 'users'
  | 'features'
  | 'prompts';

const isValidTab = (tab: string | null): tab is TabType => {
  return (
    tab === 'modelSpecs' ||
    tab === 'agents' ||
    tab === 'marketplace' ||
    tab === 'mcp' ||
    tab === 'availableTools' ||
    tab === 'users' ||
    tab === 'features' ||
    tab === 'prompts'
  );
};

export default function GlobalConfigContent({ startupConfig: propStartupConfig }: GlobalConfigContentProps) {
  const { data: startupConfigFromQuery } = useGetStartupConfig();
  const startupConfig = propStartupConfig || startupConfigFromQuery;
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const initialTab: TabType = isValidTab(tabParam) ? tabParam : 'modelSpecs';
  const [activeTab, setActiveTab] = useState<TabType>(initialTab);

  // 当 URL 参数变化时，更新活动标签页
  useEffect(() => {
    if (isValidTab(tabParam)) {
      setActiveTab(tabParam);
    }
  }, [tabParam]);

  // 处理标签页切换，同时更新 URL 参数
  const handleTabChange = (tab: TabType) => {
    setActiveTab(tab);
    setSearchParams({ tab });
  };

  const tabs: { id: TabType; label: string; description: string }[] = [
    {
      id: 'modelSpecs',
      label: '端点配置',
      description: '管理自定义端点配置',
    },
    {
      id: 'agents',
      label: '智能体管理',
      description: '管理所有智能体，设置是否展示给用户',
    },
    {
      id: 'marketplace',
      label: '智能体市场',
      description: '浏览和发现智能体',
    },
    {
      id: 'availableTools',
      label: '可用工具',
      description: '查看当前已注册的工具列表',
    },
    {
      id: 'mcp',
      label: 'MCP管理',
      description: '查看和管理MCP服务器的连接状态',
    },
    {
      id: 'users',
      label: '用户管理',
      description: '查看和管理所有注册用户',
    },
    {
      id: 'features',
      label: '初始界面配置',
      description: '管理初始界面的欢迎语和模型',
    },
    {
      id: 'prompts',
      label: '提示集管理',
      description: '管理初始对话界面中显示的提示集',
    },
  ];

  return (
    <div className="flex h-full flex-col overflow-hidden">
      {/* 标签页导航 */}
      <div className="border-b border-border-light bg-surface-secondary">
        <div className="flex gap-1 px-4 pt-2">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => handleTabChange(tab.id)}
              className={cn(
                'relative px-4 py-2 text-sm font-medium transition-colors',
                'border-b-2 border-transparent',
                activeTab === tab.id
                  ? 'border-primary text-text-primary'
                  : 'text-text-secondary hover:text-text-primary hover:border-border-subtle',
              )}
              aria-label={tab.label}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* 标签页内容 */}
      <div className="flex-1 overflow-hidden">
        {activeTab === 'modelSpecs' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <EndpointsConfig startupConfig={startupConfig} />
          </div>
        )}
        {activeTab === 'agents' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <AgentsManagement />
          </div>
        )}
        {activeTab === 'marketplace' && (
          <div className="h-full overflow-hidden">
            <MarketplaceContent />
          </div>
        )}
        {activeTab === 'availableTools' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <AvailableToolsManagement />
          </div>
        )}
        {activeTab === 'mcp' && (
          <div className="h-full overflow-hidden px-4 py-4" key="mcp-management">
            <MCPManagement startupConfig={startupConfig} />
          </div>
        )}
        {activeTab === 'users' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <UsersManagement />
          </div>
        )}
        {activeTab === 'features' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <FeaturesManagement startupConfig={startupConfig} />
          </div>
        )}
        {activeTab === 'prompts' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <PromptsManagement startupConfig={startupConfig} />
          </div>
        )}
      </div>
    </div>
  );
}

