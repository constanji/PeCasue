import React, { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { cn } from '~/utils';
import OverviewTab from './tabs/OverviewTab';
import ChannelsTab from './tabs/ChannelsTab';
import RulesTab from './tabs/RulesTab';
import CompareTab from './tabs/CompareTab';
import ObserveTab from './tabs/ObserveTab';
import AgentConfigTab from './tabs/AgentConfigTab';
import FinalAllocationTab from './tabs/FinalAllocationTab';
import PipelineCopilot, { PipelineCopilotToggle } from './copilot/PipelineCopilot';
import { usePipelineUrlSync } from './usePipelineUrlSync';

export type PipelineTabId =
  | 'overview'
  | 'channels'
  | 'final_allocation'
  | 'rules'
  | 'compare'
  | 'observe'
  | 'agent';

const isValidTab = (tab: string | null): tab is PipelineTabId => {
  return (
    tab === 'overview' ||
    tab === 'channels' ||
    tab === 'final_allocation' ||
    tab === 'rules' ||
    tab === 'compare' ||
    tab === 'observe' ||
    tab === 'agent'
  );
};

interface PipelineTabDef {
  id: PipelineTabId;
  label: string;
  description: string;
}

const TABS: PipelineTabDef[] = [
  { id: 'overview', label: '总览', description: '任务列表与渠道矩阵' },
  { id: 'channels', label: '渠道详情', description: '单渠道执行历史、文件、校验、日志' },
  {
    id: 'final_allocation',
    label: '最终分摊',
    description: '分摊基数合并与后续成本分摊',
  },
  { id: 'rules', label: '规则配置', description: '账户/费项/汇率/处理表/密码簿等静态规则' },
  { id: 'compare', label: '对比核对', description: '已产出文件 vs 上传文件的差异报告' },
  { id: 'observe', label: '观测运维', description: '耗时分布、失败率、规则命中率与事件流' },
  { id: 'agent', label: 'Agent 配置', description: 'LLM 模型连接管理与 Pipeline Copilot 设置' },
];

export default function PipelineContent() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const initialTab: PipelineTabId = isValidTab(tabParam) ? tabParam : 'overview';
  const [activeTab, setActiveTab] = useState<PipelineTabId>(initialTab);
  usePipelineUrlSync();

  useEffect(() => {
    if (isValidTab(tabParam) && tabParam !== activeTab) {
      setActiveTab(tabParam);
    }
  }, [tabParam]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleTabChange = (tab: PipelineTabId) => {
    setActiveTab(tab);
    const next = new URLSearchParams(searchParams);
    next.set('tab', tab);
    setSearchParams(next, { replace: false });
  };

  const activeDef = useMemo(() => TABS.find((t) => t.id === activeTab), [activeTab]);

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border-light bg-surface-secondary">
        <div className="flex items-end justify-between px-4 pt-2">
          <div className="flex gap-1">
            {TABS.map((tab) => (
              <button
                key={tab.id}
                type="button"
                onClick={() => handleTabChange(tab.id)}
                className={cn(
                  'relative px-4 py-2 text-sm font-medium transition-colors',
                  'border-b-2 border-transparent',
                  activeTab === tab.id
                    ? 'border-green-500 text-text-primary'
                    : 'text-text-secondary hover:text-text-primary hover:border-border-medium',
                )}
                aria-label={tab.label}
                aria-selected={activeTab === tab.id}
                role="tab"
              >
                {tab.label}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-3 pb-2">
            {activeDef && (
              <div className="hidden text-xs text-text-secondary md:block">
                {activeDef.description}
              </div>
            )}
            <PipelineCopilotToggle />
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-hidden">
        {activeTab === 'overview' && <OverviewTab />}
        {activeTab === 'channels' && <ChannelsTab />}
        {activeTab === 'final_allocation' && <FinalAllocationTab />}
        {activeTab === 'rules' && <RulesTab />}
        {activeTab === 'compare' && <CompareTab />}
        {activeTab === 'observe' && <ObserveTab />}
        {activeTab === 'agent' && <AgentConfigTab />}
      </div>

      <PipelineCopilot />
    </div>
  );
}
