import { useMemo, useCallback } from 'react';
import {
  BulbOutlined,
  InfoCircleOutlined,
  RocketOutlined,
  SmileOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { useChatContext, useAgentsMapContext } from '~/Providers';
import { EModelEndpoint, LocalStorageKeys } from '@because/data-provider';
import { useGetStartupConfig, useGetEndpointsQuery } from '~/data-provider';
import { getIconEndpoint, getEntity, cn } from '~/utils';
import { useSubmitMessage } from '~/hooks';
import { useToastContext } from '@because/client';
import useLocalStorage from '~/hooks/useLocalStorage';

interface PromptItem {
  key: string;
  icon: string;
  label: string;
  description: string;
  prompt: string;
}

function AgentPromptsContent() {
  const { conversation } = useChatContext();
  const agentsMap = useAgentsMapContext();
  const { data: startupConfig } = useGetStartupConfig();
  const { data: endpointsConfig } = useGetEndpointsQuery();
  const { submitMessage } = useSubmitMessage();
  const { showToast } = useToastContext();

  const endpointType = useMemo(() => {
    let ep = conversation?.endpoint ?? '';
    if (
      [
        EModelEndpoint.chatGPTBrowser,
        EModelEndpoint.azureOpenAI,
        EModelEndpoint.gptPlugins,
      ].includes(ep as EModelEndpoint)
    ) {
      ep = EModelEndpoint.openAI;
    }
    return getIconEndpoint({
      endpointsConfig,
      iconURL: conversation?.iconURL,
      endpoint: ep,
    });
  }, [conversation?.endpoint, conversation?.iconURL, endpointsConfig]);

  const { entity, isAgent } = getEntity({
    endpoint: endpointType,
    agentsMap,
    assistantMap: {},
    agent_id: conversation?.agent_id,
    assistant_id: conversation?.assistant_id,
  });

  // 从左侧业务列表获取当前选中的数据源（与 AgentsList 同步）
  const [selectedDataSourceId] = useLocalStorage<string | null>(
    LocalStorageKeys.LAST_DATA_SOURCE_ID,
    null,
  );

  // 获取提示集配置
  const promptsConfig = useMemo(() => {
    if (!startupConfig?.agentPrompts) {
      return null;
    }

    // 检查全局提示集是否有有效的 items
    const globalConfig = startupConfig.agentPrompts.global;
    const hasValidGlobalItems = globalConfig?.items && Array.isArray(globalConfig.items) && globalConfig.items.length > 0;

    // 优先使用左侧业务列表选中的数据源，其次使用对话关联的数据源
    const dataSourceId = selectedDataSourceId ?? (conversation as any)?.data_source_id;
    const hasExplicitDataSourceSelection = !!selectedDataSourceId;

    if (dataSourceId && startupConfig.agentPrompts.dataSources?.[dataSourceId]) {
      const dataSourceConfig = startupConfig.agentPrompts.dataSources[dataSourceId];
      const hasValidDataSourceItems = dataSourceConfig?.items && Array.isArray(dataSourceConfig.items) && dataSourceConfig.items.length > 0;
      if (hasValidDataSourceItems) {
        return dataSourceConfig;
      }
      // 数据源未配置提示集，回退到全局
      if (hasValidGlobalItems) {
        return globalConfig;
      }
      // 全局也为空，不显示任何提示集
      return null;
    }

    if (hasExplicitDataSourceSelection) {
      // 用户已选择数据源，但该数据源在配置中不存在，回退到全局
      if (hasValidGlobalItems) {
        return globalConfig;
      }
      return null;
    }

    // 未选择数据源时，使用原有回退逻辑
    if (hasValidGlobalItems) {
      return globalConfig;
    }

    if (startupConfig.agentPrompts.dataSources) {
      const dataSourceEntries = Object.entries(startupConfig.agentPrompts.dataSources);
      for (const [dsId, dsConfig] of dataSourceEntries) {
        const config = dsConfig as any;
        const hasValidItems = config?.items && Array.isArray(config.items) && config.items.length > 0;
        if (hasValidItems) {
          return config;
        }
      }
    }

    if (isAgent && entity?.id && startupConfig.agentPrompts.agents?.[entity.id]) {
      const agentConfig = startupConfig.agentPrompts.agents[entity.id];
      const hasValidAgentItems = agentConfig?.items && Array.isArray(agentConfig.items) && agentConfig.items.length > 0;
      if (hasValidAgentItems) {
        return agentConfig;
      }
    }

    return null;
  }, [startupConfig, conversation, isAgent, entity, selectedDataSourceId]);

  // 转换配置为提示项数组
  const items: PromptItem[] = useMemo(() => {
    if (!promptsConfig?.items || !Array.isArray(promptsConfig.items)) {
      return [];
    }

    return promptsConfig.items.map((item: any) => ({
      key: item.key || `prompt-${Math.random()}`,
      icon: item.icon || 'bulb',
      label: item.label || '',
      description: item.description || '',
      prompt: item.prompt || item.label || '',
    }));
  }, [promptsConfig]);

  // 根据图标类型返回对应的图标组件
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

  // 处理点击事件
  const handleItemClick = useCallback((item: PromptItem) => {
    const promptText = item.prompt || item.label;
    if (promptText) {
      submitMessage({ text: promptText });
      showToast({ status: 'success', message: `已发送提示: ${item.label}` });
    }
  }, [submitMessage, showToast]);

  // 处理键盘事件
  const handleKeyDown = useCallback((event: React.KeyboardEvent, item: PromptItem) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      handleItemClick(item);
    }
  }, [handleItemClick]);

  // 如果没有配置或没有项目，不显示
  if (!promptsConfig || !items || items.length === 0) {
    return null;
  }

  return (
    <div className="mb-6 w-full max-w-3xl px-4 xl:max-w-4xl">
      {/* 提示项网格 */}
      <div className="flex flex-wrap justify-center gap-3">
        {items.map((item) => (
          <button
            key={item.key}
            onClick={() => handleItemClick(item)}
            onKeyDown={(e) => handleKeyDown(e, item)}
            className={cn(
              'group relative flex w-52 cursor-pointer flex-col gap-2 rounded-xl',
              'border border-border-light bg-surface-tertiary px-3 pb-4 pt-3',
              'text-start align-top text-[15px]',
              'shadow-sm transition-all duration-300 ease-in-out',
              'hover:border-border-medium hover:bg-surface-hover hover:shadow-lg',
              'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-border-xheavy'
            )}
            aria-label={`${item.label}: ${item.description}`}
            type="button"
          >
            {/* 图标和标签 */}
            <div className="flex items-center gap-2">
              <span className="flex-shrink-0">
                {getIconComponent(item.icon)}
              </span>
              <span className="break-word line-clamp-2 overflow-hidden text-balance text-sm font-semibold text-text-primary">
                {item.label}
              </span>
            </div>
            
            {/* 描述 */}
            {item.description && (
              <p className="break-word line-clamp-2 overflow-hidden text-balance break-all text-xs text-text-secondary">
                {item.description}
              </p>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}

export default function AgentPrompts() {
  return <AgentPromptsContent />;
}

