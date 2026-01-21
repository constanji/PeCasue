import React, { useMemo, useCallback, useState } from 'react';
import { useNavigate, useLocation, useSearchParams } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Bot, Database, CheckCircle2, Info } from 'lucide-react';
import { EModelEndpoint, Constants, SystemRoles, QueryKeys, LocalStorageKeys } from '@because/data-provider';
import { useListAgentsQuery } from '~/data-provider';
import { useListDataSourcesQuery } from '~/data-provider/DataSources';
import { useLocalize, useAgentDefaultPermissionLevel, useNewConvo, useAuthContext } from '~/hooks';
import { useToastContext } from '@because/client';
import { clearMessagesCache } from '~/utils';
import { cn } from '~/utils';
import { getAgentAvatarUrl } from '~/utils/agents';
import type { Agent } from '@because/data-provider';
import type { DataSource } from '@because/data-provider';
import store from '~/store';
import useLocalStorage from '~/hooks/useLocalStorage';
import DatabaseSchemaDialog from './DatabaseSchemaDialog';

interface AgentsListProps {
  toggleNav?: () => void;
}


export default function AgentsList({ toggleNav }: AgentsListProps) {
  const localize = useLocalize();
  const navigate = useNavigate();
  const location = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const permissionLevel = useAgentDefaultPermissionLevel();
  const { newConversation } = useNewConvo();
  const { user } = useAuthContext();
  const { conversation } = store.useCreateConversationAtom(0);
  
  // 数据源列表
  const { data: dataSourcesResponse } = useListDataSourcesQuery();
  const allDataSources = dataSourcesResponse?.data || [];
  const enabledDataSources = useMemo(
    () => allDataSources.filter((ds: DataSource) => {
      const isPublic = (ds as any).isPublic;
      return ds.status === 'active' && Boolean(isPublic) === true;
    }),
    [allDataSources],
  );
  
  // 选中的数据源ID（保存到localStorage）
  const [selectedDataSourceId, setSelectedDataSourceId] = useLocalStorage<string | null>(
    LocalStorageKeys.LAST_DATA_SOURCE_ID,
    null,
  );
  
  // 当前选中的数据源
  const currentDataSourceId = selectedDataSourceId;
  
  // 数据库结构对话框状态
  const [schemaDialogOpen, setSchemaDialogOpen] = useState(false);
  const [viewingDataSource, setViewingDataSource] = useState<DataSource | null>(null);
  
  // 只获取公开的智能体（管理员选择展示的）
  const { data: agentsResponse } = useListAgentsQuery(
    { requiredPermission: permissionLevel },
    {
      select: (res) => ({
        ...res,
        // 只显示公开的智能体（isPublic: true）
        data: res.data.filter((agent) => agent.isPublic === true),
      }),
    },
  );

  const agents = useMemo(() => agentsResponse?.data ?? [], [agentsResponse]);

  const handleAgentClick = useCallback(
    (agent: Agent) => {
      // 清除当前对话的消息缓存，避免影响历史对话
      clearMessagesCache(queryClient, conversation?.conversationId);
      queryClient.invalidateQueries([QueryKeys.messages]);
      
      // 创建新对话并设置智能体，同时设置模型名称
      // 注意：这里只创建新对话，不会影响历史对话的状态
      newConversation({
        preset: {
          endpoint: EModelEndpoint.agents,
          agent_id: agent.id,
          model: agent.model || '', // 设置 agent 的 model
          conversationId: Constants.NEW_CONVO as string,
        },
        keepLatestMessage: false,
      });
      
      // 导航到新对话，使用 replace: false 确保不会影响浏览器历史
      navigate(`/c/new?agent_id=${agent.id}`, {
        replace: false,
        state: {
          agentId: agent.id,
          agentName: agent.name,
        },
      });
      
      if (toggleNav) {
        toggleNav();
      }
    },
    [navigate, toggleNav, newConversation, queryClient, conversation],
  );


  return (
    <>
      <div className="mb-4 border-t border-border-light pt-4">
        <div className="mb-2 px-2">
          <h2 className="text-sm font-semibold text-text-primary">智能体</h2>
        </div>
        <div className="rounded-lg border border-border-light bg-surface-secondary p-2">
          {agents.length === 0 ? (
            <div className="py-2 text-center text-xs text-text-tertiary">
              暂无可用智能体
            </div>
          ) : (
            <div className="space-y-1">
              {agents.map((agent, index) => {
                // 检查当前 URL 参数或 state 中的 agent_id
                const urlParams = new URLSearchParams(location.search);
                const urlAgentId = urlParams.get('agent_id');
                const isActive =
                  urlAgentId === agent.id ||
                  (location.pathname.includes(`/c/`) && location.state?.agentId === agent.id);

                return (
                  <AgentListItem
                    key={agent.id}
                    agent={agent}
                    isActive={isActive}
                    onClick={() => handleAgentClick(agent)}
                  />
                );
              })}
            </div>
          )}
        </div>
      </div>
      
      {/* 数据源选择器 - 直接展示已启用的数据源 */}
      <div className="mb-4 border-t border-border-light pt-4">
        <div className="mb-2 px-2">
          <h2 className="text-sm font-semibold text-text-primary">业务列表</h2>
        </div>
        <div className="rounded-lg border border-border-light bg-surface-secondary p-2">
          {enabledDataSources.length === 0 ? (
            <div className="py-2 text-center text-xs text-text-tertiary">
              暂无已配置的数据源
            </div>
          ) : (
            <div className="space-y-1">
              {enabledDataSources.map((dataSource: DataSource) => {
                const isSelected = currentDataSourceId === dataSource._id;
                return (
                  <div
                    key={dataSource._id}
                    className="flex items-center gap-1 group"
                  >
                    <button
                      onClick={() => {
                        // 直接选择数据源（保存到localStorage）
                        setSelectedDataSourceId(dataSource._id);
                      }}
                      className={cn(
                        'flex flex-1 items-center gap-2 rounded-lg px-2 py-1.5 text-left transition-colors',
                        isSelected
                          ? 'text-text-primary'
                          : 'text-text-secondary hover:bg-surface-hover',
                      )}
                      aria-label={dataSource.name ? `选择数据源: ${dataSource.name}` : '选择数据源'}
                    >
                      <Database className="h-4 w-4 flex-shrink-0" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="truncate text-sm font-medium">{dataSource.name}</span>
                          {isSelected && (
                            <CheckCircle2 className="h-3 w-3 flex-shrink-0 text-green-600" />
                          )}
                        </div>
                        <div className="text-xs text-text-tertiary">
                          {dataSource.type === 'mysql' ? 'MySQL' : 'PostgreSQL'} · {dataSource.database}
                        </div>
                      </div>
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setViewingDataSource(dataSource);
                        setSchemaDialogOpen(true);
                      }}
                      className={cn(
                        'flex-shrink-0 p-1.5 rounded-lg transition-all',
                        'opacity-60 group-hover:opacity-100',
                        'hover:bg-surface-hover text-text-secondary hover:text-text-primary',
                        'focus:opacity-100 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-1',
                      )}
                      aria-label={`查看 ${dataSource.name} 的数据库结构`}
                      title="查看数据库结构"
                    >
                      <Info className="h-4 w-4" />
                    </button>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
      
      {/* 数据库结构对话框 */}
      <DatabaseSchemaDialog
        isOpen={schemaDialogOpen}
        onOpenChange={setSchemaDialogOpen}
        dataSource={viewingDataSource}
      />
    </>
  );
}

interface AgentListItemProps {
  agent: Agent;
  isActive: boolean;
  onClick: () => void;
}

function AgentListItem({ agent, isActive, onClick }: AgentListItemProps) {
  const avatarUrl = getAgentAvatarUrl(agent);

  return (
    <button
      onClick={onClick}
      className={cn(
        'flex w-full items-center gap-3 rounded-lg px-2 py-1.5 text-left transition-colors',
        'hover:bg-surface-hover',
        isActive && 'bg-surface-active text-text-primary',
        !isActive && 'text-text-secondary',
      )}
      aria-label={agent.name}
    >
      <div className="flex-shrink-0">
        {avatarUrl ? (
          <img src={avatarUrl} alt={agent.name || '智能体'} className="h-5 w-5 rounded-full object-cover" />
        ) : (
          <Bot className="h-5 w-5 text-text-primary" />
        )}
      </div>
      <span className="flex-1 truncate text-sm font-medium">{agent.name}</span>
    </button>
  );
}

