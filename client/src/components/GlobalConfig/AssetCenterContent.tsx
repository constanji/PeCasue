import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import type { TStartupConfig } from '@because/data-provider';
import { useGetStartupConfig } from '~/data-provider';
import { cn } from '~/utils';
import KnowledgeBaseManagement from './KnowledgeBaseManagement';
import DataSourceManagement from './DataSourceManagement';
import ProjectConfig from './ProjectConfig';
import PromptsManagement from './PromptsManagement';

type TabType = 'dataSources' | 'knowledgeBase' | 'projectConfig' | 'prompts';

const isValidTab = (tab: string | null): tab is TabType => {
  return tab === 'dataSources' || tab === 'knowledgeBase' || tab === 'projectConfig' || tab === 'prompts';
};

export default function AssetCenterContent() {
  const { data: startupConfig } = useGetStartupConfig();
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const initialTab: TabType = isValidTab(tabParam) ? tabParam : 'projectConfig';
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
      id: 'projectConfig',
      label: '项目配置',
      description: '配置项目相关设置，包括数据源选择等',
    },
    {
      id: 'dataSources',
      label: '数据源管理',
      description: '管理数据库连接配置，支持 MySQL 和 PostgreSQL',
    },
    {
      id: 'knowledgeBase',
      label: '知识库管理',
      description: '管理向量数据库中的语义模型、QA对、同义词和业务知识',
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
        {activeTab === 'projectConfig' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <ProjectConfig />
          </div>
        )}
        {activeTab === 'dataSources' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <DataSourceManagement />
          </div>
        )}
        {activeTab === 'knowledgeBase' && (
          <div className="h-full overflow-hidden px-4 py-4">
            <KnowledgeBaseManagement />
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

