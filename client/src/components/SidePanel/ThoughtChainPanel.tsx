import { useEffect, memo, useMemo, useState } from 'react';
import { ConfigProvider, Typography, Flex } from 'antd';
import type { ThoughtChainItemType } from '@ant-design/x';
import { ThoughtChain, CodeHighlighter } from '@ant-design/x';
import { CheckCircleTwoTone, LoadingOutlined, CloseCircleTwoTone, CodeOutlined } from '@ant-design/icons';
import { actionDelimiter, actionDomainSeparator, Constants } from '@because/data-provider';
import { useChatContext } from '~/Providers';
import type { MessageToolCalls, MessageContentItem } from '~/utils/parseDatServerResponse';
import { mapAttachments } from '~/utils/map';
import { useLocalize } from '~/hooks';
import MarkdownLite from '~/components/Chat/Messages/Content/MarkdownLite';

const { Text } = Typography;

// 扩展 ThoughtChainItemType 以支持 children
type ExtendedThoughtChainItemType = ThoughtChainItemType & {
  children?: React.ReactNode;
};

interface ThoughtChainPanelProps {
  toolCallsByMessage: MessageToolCalls[];
  shouldRender: boolean;
  onRenderChange: (shouldRender: boolean) => void;
}


/**
 * SQL 执行结果组件
 */
function SqlExecuteResult({ content }: { content: string }) {
  const parsed = useMemo(() => {
    try {
      let toParse = content.trim();
      if (toParse.startsWith('"') && toParse.endsWith('"')) {
        toParse = JSON.parse(toParse);
      }
      if (typeof toParse === 'string') {
        return JSON.parse(toParse);
      }
      return toParse;
    } catch {
      return null;
    }
  }, [content]);

  if (Array.isArray(parsed) && parsed.length > 0) {
    const keys = Object.keys(parsed[0]);
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse border border-border-light text-sm">
          <thead>
            <tr className="bg-surface-secondary">
              {keys.map((key) => (
                <th
                  key={key}
                  className="border border-border-light px-2 py-1 text-left font-medium text-text-primary"
                >
                  {key}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {parsed.slice(0, 20).map((row: any, idx: number) => (
              <tr key={idx} className="hover:bg-surface-tertiary">
                {keys.map((key) => (
                  <td key={key} className="border border-border-light px-2 py-1 text-text-primary">
                    {typeof row[key] === 'number'
                      ? row[key].toLocaleString('zh-CN', { maximumFractionDigits: 2 })
                      : String(row[key] ?? '')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {parsed.length > 20 && (
          <div className="mt-1 text-xs text-text-secondary">
            显示前 20 条，共 {parsed.length} 条记录
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="max-h-40 overflow-auto rounded bg-surface-tertiary p-2 text-sm text-text-primary">
      <pre className="whitespace-pre-wrap break-words text-text-primary">{content}</pre>
    </div>
  );
}

/**
 * 代码块组件 - 用于展示工具调用的参数和输出
 * 修复溢出问题，确保内容在容器内正确显示
 */
function OptimizedCodeBlock({ text, maxHeight = 200 }: { text: string; maxHeight?: number }) {
  const formatText = (str: string) => {
    try {
      return JSON.stringify(JSON.parse(str), null, 2);
    } catch {
      return str;
    }
  };

  return (
    <div
      className="mt-1 w-full overflow-hidden rounded-md bg-surface-tertiary"
      style={{ maxWidth: '100%' }}
    >
      <div
        className="overflow-auto p-2 text-xs text-text-primary"
        style={{ maxHeight, maxWidth: '100%' }}
      >
        <pre
          className="m-0 whitespace-pre-wrap"
          style={{
            wordBreak: 'break-all',
            overflowWrap: 'break-word',
            maxWidth: '100%',
          }}
        >
          <code>{formatText(text)}</code>
        </pre>
      </div>
    </div>
  );
}

/**
 * 工具调用详情内容组件 - 用于在可折叠区域内展示
 */
function ToolCallDetailContent({
  args,
  output,
  domain,
  function_name,
  localize,
}: {
  args: string;
  output?: string | null;
  domain: string | null;
  function_name: string;
  // 使用宽松类型避免与 useLocalize 的类型签名不兼容
  localize: any;
}) {
  const hasOutput = output != null && output.length > 0;

  return (
    <div className="w-full space-y-3 overflow-hidden" style={{ maxWidth: '100%' }}>
      {/* 参数 */}
      {args && (
        <div className="w-full overflow-hidden" style={{ maxWidth: '100%' }}>
          <Text type="secondary" className="mb-1 block text-xs">
            {domain
              ? localize('com_assistants_domain_info', { 0: domain })
              : localize('com_assistants_function_use', { 0: function_name })}
          </Text>
          <OptimizedCodeBlock text={args} />
        </div>
      )}

      {/* 输出结果 */}
      {hasOutput && (
        <div className="w-full overflow-hidden" style={{ maxWidth: '100%' }}>
          <Text type="secondary" className="mb-1 block text-xs">
            {localize('com_ui_result')}
          </Text>
          <OptimizedCodeBlock text={output!} />
        </div>
      )}
    </div>
  );
}

/**
 * 单个工具调用项 - 使用 ant-design-x ThoughtChain 组件
 * 支持折叠功能，保持实时数据更新能力
 */
function SidePanelToolCallItem({
  toolCall,
  isSubmitting,
  itemKey,
}: {
  toolCall: {
    name: string;
    args: string | Record<string, unknown>;
    output?: string | null;
    progress?: number;
    id?: string;
    auth?: string;
    expires_at?: number;
  };
  attachments?: any[];
  isSubmitting: boolean;
  itemKey: string;
}) {
  const localize = useLocalize();

  // 解析工具名称和域名 - 与原生 ToolCall 逻辑一致
  const { function_name, domain, isMCPToolCall } = useMemo(() => {
    const name = toolCall.name;
    if (typeof name !== 'string') {
      return { function_name: '', domain: null, isMCPToolCall: false };
    }
    if (name.includes(Constants.mcp_delimiter)) {
      const [func, server] = name.split(Constants.mcp_delimiter);
      return {
        function_name: func || '',
        domain: server && (server.replaceAll(actionDomainSeparator, '.') || null),
        isMCPToolCall: true,
      };
    }
    const [func, _domain] = name.includes(actionDelimiter)
      ? name.split(actionDelimiter)
      : [name, ''];
    return {
      function_name: func || '',
      domain: _domain && (_domain.replaceAll(actionDomainSeparator, '.') || null),
      isMCPToolCall: false,
    };
  }, [toolCall.name]);

  // BeCauseSkills 子工具名称映射
  const becauseSkillsCommandMap: Record<string, string> = {
    'database-schema': '获取数据库Schema',
    'intent-classification': '意图识别',
    'rag-retrieval': 'RAG检索',
    'sql-validation': 'SQL语句验证',
    'sql-executor': 'SQL执行',
    'result-analysis': '归因调查',
    'chart-generation': '可视化图表生成',
    'reranker': '结果重排序',
  };

  // 解析工具参数，提取 command
  const parsedArgs = useMemo(() => {
    if (typeof toolCall.args === 'string') {
      try {
        return JSON.parse(toolCall.args);
      } catch {
        return null;
      }
    }
    return toolCall.args;
  }, [toolCall.args]);

  // 获取子工具名称（如果是 because_skills）
  const subToolName = useMemo(() => {
    if (function_name === 'because_skills') {
      // 方法1: 从解析后的参数中获取（优先）
      if (parsedArgs && typeof parsedArgs === 'object' && parsedArgs !== null) {
        if ('command' in parsedArgs) {
          const command = parsedArgs.command as string;
          if (command && typeof command === 'string' && command.length > 0) {
            return becauseSkillsCommandMap[command] || command;
          }
        }
      }

      // 方法2: 直接从对象格式的 args 中提取
      if (typeof toolCall.args === 'object' && toolCall.args !== null && !Array.isArray(toolCall.args)) {
        if ('command' in toolCall.args) {
          const command = (toolCall.args as any).command;
          if (command && typeof command === 'string' && command.length > 0) {
            return becauseSkillsCommandMap[command] || command;
          }
        }
      }

      // 方法3: 从原始字符串中提取 command（支持流式传输中的不完整JSON）
      if (typeof toolCall.args === 'string' && toolCall.args.length > 0) {
        // 方法1: 尝试匹配双引号格式 "command":"value"
        let commandMatch = toolCall.args.match(/"command"\s*:\s*"([^"]+)"/);
        if (commandMatch && commandMatch[1]) {
          const command = commandMatch[1];
          if (command && command.length > 0) {
            return becauseSkillsCommandMap[command] || command;
          }
        }

        // 方法2: 尝试匹配单引号格式 'command':'value' (不标准但可能遇到)
        commandMatch = toolCall.args.match(/'command'\s*:\s*'([^']+)'/);
        if (commandMatch && commandMatch[1]) {
          const command = commandMatch[1];
          if (command && command.length > 0) {
            return becauseSkillsCommandMap[command] || command;
          }
        }

        // 方法3: 尝试匹配不带引号的格式 "command":value (value可能是字符串或标识符)
        commandMatch = toolCall.args.match(/"command"\s*:\s*([^,}\s]+)/);
        if (commandMatch && commandMatch[1]) {
          let command = commandMatch[1].replace(/^["']|["']$/g, ''); // 移除首尾引号
          if (command && command.length > 0) {
            return becauseSkillsCommandMap[command] || command;
          }
        }

        // 方法4: 尝试部分匹配（流式传输中可能只有部分内容）
        // 查找 "command" 后面的内容，即使JSON不完整
        const partialMatch = toolCall.args.match(/"command"\s*:\s*"([^"]*)/);
        if (partialMatch && partialMatch[1] && partialMatch[1].length > 0) {
          const command = partialMatch[1];
          // 只返回有效的命令（在映射表中或至少3个字符）
          if (command.length >= 3) {
            return becauseSkillsCommandMap[command] || command;
          }
        }
      }
    }
    return null;
  }, [function_name, parsedArgs, toolCall.args]);

  // 格式化参数
  const args = useMemo(() => {
    if (typeof toolCall.args === 'string') {
      return toolCall.args;
    }
    try {
      return JSON.stringify(toolCall.args, null, 2);
    } catch {
      return '';
    }
  }, [toolCall.args]);

  // 状态计算
  const hasOutput = toolCall.output != null && toolCall.output.length > 0;
  const error =
    typeof toolCall.output === 'string' &&
    toolCall.output.toLowerCase().includes('error processing tool');
  const isLoading = !hasOutput && isSubmitting;
  const cancelled = !isSubmitting && !hasOutput && !error;

  // 获取状态 - ThoughtChain 支持 'success' | 'error' | 'loading' 等
  const getStatus = (): 'success' | 'error' | 'loading' => {
    if (error) return 'error';
    if (cancelled) return 'error';
    if (hasOutput) return 'success';
    return 'loading';
  };

  // 获取图标 - 使用 TwoTone 图标组件，支持 twoToneColor 属性设置颜色
  const getIcon = () => {
    if (isLoading) return <LoadingOutlined spin />;
    if (error || cancelled) {
      return <CloseCircleTwoTone twoToneColor="#ef4444" />;
    }
    if (hasOutput) {
      return <CheckCircleTwoTone twoToneColor="#10b981" />;
    }
    return <CodeOutlined />;
  };

  // 获取标题文本
  const getTitle = () => {
    // 如果是 because_skills，优先使用子工具名称
    // 即使在运行时（isLoading），也要尝试提取工具名称
    let displayName = subToolName;
    
    // 如果 subToolName 为空，且是 because_skills，尝试实时提取
    if (!displayName && function_name === 'because_skills') {
      // 方法1: 从解析后的对象中提取（优先）
      if (parsedArgs && typeof parsedArgs === 'object' && parsedArgs !== null) {
        if ('command' in parsedArgs) {
          const command = parsedArgs.command as string;
          if (command && typeof command === 'string' && command.length > 0) {
            displayName = becauseSkillsCommandMap[command] || command;
          }
        }
      }
      
      // 方法2: 从原始字符串中提取（流式传输中可能只有部分内容）
      if (!displayName && typeof toolCall.args === 'string' && toolCall.args.length > 0) {
        // 尝试多种格式匹配
        let commandMatch = toolCall.args.match(/"command"\s*:\s*"([^"]+)"/);
        if (!commandMatch) {
          commandMatch = toolCall.args.match(/"command"\s*:\s*"([^"]*)/); // 部分匹配
        }
        if (!commandMatch) {
          commandMatch = toolCall.args.match(/'command'\s*:\s*'([^']+)'/); // 单引号
        }
        if (!commandMatch) {
          commandMatch = toolCall.args.match(/"command"\s*:\s*([^,}\s]+)/); // 无引号
        }
        
        if (commandMatch && commandMatch[1]) {
          let command = commandMatch[1].replace(/^["']|["']$/g, ''); // 移除首尾引号
          if (command && command.length >= 3) { // 至少3个字符才认为是有效命令
            displayName = becauseSkillsCommandMap[command] || command;
          }
        }
      }
      
      // 方法3: 从对象格式的 args 中直接提取
      if (!displayName && typeof toolCall.args === 'object' && toolCall.args !== null) {
        if ('command' in toolCall.args) {
          const command = (toolCall.args as any).command;
          if (command && typeof command === 'string' && command.length > 0) {
            displayName = becauseSkillsCommandMap[command] || command;
          }
        }
      }
    }
    
    // 如果还是没有提取到，使用 function_name（但避免显示 because_skills）
    if (!displayName) {
      displayName = function_name === 'because_skills' ? null : function_name;
    }
    
    if (isLoading) {
      return displayName
        ? localize('com_assistants_running_var', { 0: displayName })
        : localize('com_assistants_running_action');
    }
    if (cancelled) {
      return localize('com_ui_cancelled');
    }
    if (isMCPToolCall) {
      return localize('com_assistants_completed_function', { 0: displayName });
    }
    if (domain && domain.length !== Constants.ENCODED_DOMAIN_LENGTH) {
      return localize('com_assistants_completed_action', { 0: domain });
    }
    return localize('com_assistants_completed_function', { 0: displayName });
  };

  // 是否有详情内容
  const hasDetails = args || hasOutput;

  // 构建 ThoughtChain 项目 - 显式指定类型避免类型错误
  const status = getStatus();
  // 如果没有 domain，就不要在 description 再重复 function_name，避免出现
  //「运行 because」下一行又单独显示「because」的重复效果
  const description =
    domain && domain.length !== Constants.ENCODED_DOMAIN_LENGTH ? domain : '';

  const toolCallItems: ExtendedThoughtChainItemType[] = [
    {
      key: itemKey,
      title: getTitle(),
      description,
      icon: getIcon(),
      status,
      collapsible: !!hasDetails,
      content: hasDetails ? (
        <ToolCallDetailContent
          args={args}
          output={toolCall.output}
          domain={domain}
          function_name={function_name}
          localize={localize}
        />
      ) : undefined,
    },
  ];

  return (
    <div className="w-full overflow-hidden" style={{ maxWidth: '100%' }}>
      <ThoughtChain items={toolCallItems} />
    </div>
  );
}

/**
 * ThoughtChainPanel 组件 - 使用 ant-design-x ThoughtChain 组件展示思维链
 * 直接引用原生 ToolCall 组件实现实时展示
 */
const ThoughtChainPanel = memo(function ThoughtChainPanel({
  toolCallsByMessage,
  shouldRender,
  onRenderChange,
}: ThoughtChainPanelProps) {
  const { getMessages, isSubmitting } = useChatContext();
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);

  // 获取所有消息的附件
  const attachmentsMap = useMemo(() => {
    const messages = getMessages();
    if (!messages || messages.length === 0) {
      return {};
    }

    const allAttachments: any[] = [];
    messages.forEach((message: any) => {
      if (message.attachments && Array.isArray(message.attachments)) {
        allAttachments.push(...message.attachments);
      }
    });

    return mapAttachments(allAttachments);
  }, [getMessages]);

  // 通知父组件是否有数据需要渲染
  useEffect(() => {
    if (toolCallsByMessage.length > 0) {
      onRenderChange(true);
    }
  }, [toolCallsByMessage, onRenderChange]);

  // 自动展开最新的轮次
  useEffect(() => {
    if (toolCallsByMessage.length > 0) {
      const latestKey = `round-${toolCallsByMessage.length - 1}`;
      setExpandedKeys((prev) => {
        if (!prev.includes(latestKey)) {
          return [...prev, latestKey];
        }
        return prev;
      });
    }
  }, [toolCallsByMessage.length]);

  if (!shouldRender) {
    return null;
  }

  if (toolCallsByMessage.length === 0) {
    return (
      <div className="flex h-full items-center justify-center">
        <div className="text-center text-text-secondary">
          <p className="text-sm">暂无思维链数据</p>
        </div>
      </div>
    );
  }

  // 构建 ThoughtChain 项目 - 按对话轮次分组
  // 只展示「包含工具调用」的轮次；纯文本轮次不进入思维链
  const chainItems: ExtendedThoughtChainItemType[] = toolCallsByMessage
    .filter((messageData) => messageData.toolCalls && messageData.toolCalls.length > 0)
    .map((messageData, roundIdx) => {
      const roundKey = `round-${roundIdx}`;
      const toolCount = messageData.toolCalls.length;
      const isStreaming = messageData.isStreaming;

      const hasAnyLoading = messageData.toolCalls.some(
        (tc) => tc.toolCall.output == null || tc.toolCall.output.length === 0,
      );

      // 构建描述文本
      const descriptionParts: string[] = [];
      if (toolCount > 0) {
        descriptionParts.push(`${toolCount} 个工具调用`);
      }
      const textCount = messageData.contentItems?.filter((item) => item.type === 'text').length || 0;
      if (textCount > 0) {
        descriptionParts.push(`${textCount} 段思考`);
      }
      const description = descriptionParts.join('，') || '无内容';

      // 按照 contentItems 的顺序渲染内容
      const renderContentItems = () => {
        if (!messageData.contentItems || messageData.contentItems.length === 0) {
          return null;
        }

        return (
          <Flex gap="small" vertical style={{ width: '100%' }}>
            {messageData.contentItems.map((item, itemIdx) => {
              if (item.type === 'text') {
                // 渲染文本内容（说明类文案：字号与轮次标题一致，但颜色更浅）
                return (
                  <Text
                    key={`${roundKey}-text-${itemIdx}`}
                    type="secondary"
                    className="text-sm leading-5 text-text-tertiary"
                    style={{ width: '100%', wordBreak: 'break-word' }}
                  >
                    <div className="prose prose-sm max-w-none text-text-tertiary [&_*]:!text-text-tertiary [&_p]:mb-1.5 [&_p]:last:mb-0">
                      <MarkdownLite content={item.text || ''} />
                    </div>
                  </Text>
                );
              } else if (item.type === 'toolCall' && item.toolCall) {
                // 渲染工具调用
                const tcKey = `${roundKey}-tc-${itemIdx}`;
                const tcAttachments = item.toolCall.toolCall.id
                  ? attachmentsMap[item.toolCall.toolCall.id]
                  : undefined;

                return (
                  <SidePanelToolCallItem
                    key={tcKey}
                    itemKey={tcKey}
                    toolCall={item.toolCall.toolCall}
                    attachments={tcAttachments}
                    isSubmitting={isSubmitting}
                  />
                );
              }
              return null;
            })}
          </Flex>
        );
      };

      return {
        key: roundKey,
        title: `第 ${messageData.messageIndex} 轮对话`,
        description,
        status: isStreaming || hasAnyLoading ? 'loading' : 'success',
        collapsible: true,
        content: renderContentItems(),
      };
    },
  );

  return (
    <ConfigProvider
      theme={{
        token: {
          colorBgContainer: 'var(--bg-surface-secondary)',
          colorText: 'var(--text-primary)',
          colorBorder: 'var(--border-light)',
          colorTextDescription: 'var(--text-secondary)',
        },
        components: {
          // ThoughtChain 是 @ant-design/x 的组件，这里通过 any 绕过 antd 类型检查
          ...( {
          ThoughtChain: {
            titleColor: 'var(--text-primary)',
            descriptionColor: 'var(--text-secondary)',
            itemBg: 'transparent',
            itemHoverBg: 'var(--surface-hover)',
              // 状态颜色配置
              successColor: '#10b981', // 绿色
              errorColor: '#ef4444', // 红色
              loadingColor: 'var(--text-secondary)', // 加载中颜色
          },
          } as any),
        },
      } as any}
    >
      <div className="flex h-full flex-col overflow-hidden">
        {/* 标题 */}
        <div className="flex flex-shrink-0 items-center justify-between border-b border-border-light bg-background px-4 py-3">
          <div className="text-base font-semibold text-text-primary">思维链</div>
          <div className="text-xs text-text-secondary">共 {toolCallsByMessage.length} 轮</div>
        </div>

        {/* 思维链内容 */}
        <div 
          className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden px-3 py-2 thought-chain-container text-sm"
          style={{ 
            // 思维链整体基准文字颜色用三级文字色，更浅一些
            color: 'var(--text-tertiary)',
          } as React.CSSProperties}
        >
          <style dangerouslySetInnerHTML={{ __html: `
            /* 基础文字颜色：使用次级文字色，降低对比度 */
            .thought-chain-container,
            .thought-chain-container *,
            .thought-chain-container *::before,
            .thought-chain-container *::after {
              color: var(--text-tertiary) !important;
            }
            /* 标题元素使用主文字色，保证层级感 */
            .thought-chain-container [class*="title"],
            .thought-chain-container [class*="Title"],
            .thought-chain-container [class*="title"] *,
            .thought-chain-container [class*="Title"] *,
            .thought-chain-container button span:first-child,
            .thought-chain-container div[role="button"] span:first-child,
            .thought-chain-container button > span:first-of-type,
            .thought-chain-container div[role="button"] > span:first-of-type {
              color: var(--text-primary) !important;
            }
            /* 描述、副文本维持为次级文字色 */
            .thought-chain-container [class*="description"],
            .thought-chain-container [class*="Description"],
            .thought-chain-container [class*="description"] *,
            .thought-chain-container [class*="Description"] *,
            .thought-chain-container button span:last-child,
            .thought-chain-container div[role="button"] span:last-child,
            .thought-chain-container button > span:last-of-type,
            .thought-chain-container div[role="button"] > span:last-of-type {
              color: var(--text-tertiary) !important;
            }
            /* 覆盖内联颜色为三级文字色，进一步降低对比 */
            .thought-chain-container [style*="color"] {
              color: var(--text-tertiary) !important;
            }
            .thought-chain-container [style*="color"] [class*="description"],
            .thought-chain-container [style*="color"] [class*="Description"] {
              color: var(--text-tertiary) !important;
            }
            /* 工具调用图标颜色 - 根据状态设置 */
            .thought-chain-container [class*="anticon"][style*="color: rgb(239, 68, 68)"],
            .thought-chain-container [class*="anticon"][style*="color:#ef4444"],
            .thought-chain-container [class*="anticon"][style*="color: #ef4444"] {
              color: #ef4444 !important;
            }
            .thought-chain-container [class*="anticon"][style*="color: rgb(16, 185, 129)"],
            .thought-chain-container [class*="anticon"][style*="color:#10b981"],
            .thought-chain-container [class*="anticon"][style*="color: #10b981"] {
              color: #10b981 !important;
            }
            /* 通过 data-tool-call-status 属性设置图标颜色 - 使用更高优先级 */
            .thought-chain-container [data-tool-call-status="error"] [class*="anticon"],
            .thought-chain-container [data-tool-call-status="error"] [class*="anticon"] svg,
            .thought-chain-container [data-tool-call-status="error"] [class*="anticon"] path,
            .thought-chain-container [data-tool-call-status="error"] svg,
            .thought-chain-container [data-tool-call-status="error"] svg path,
            .thought-chain-container [data-tool-call-status="error"] path {
              color: #ef4444 !important;
              fill: #ef4444 !important;
              stroke: #ef4444 !important;
            }
            .thought-chain-container [data-tool-call-status="success"] [class*="anticon"],
            .thought-chain-container [data-tool-call-status="success"] [class*="anticon"] svg,
            .thought-chain-container [data-tool-call-status="success"] [class*="anticon"] path,
            .thought-chain-container [data-tool-call-status="success"] svg,
            .thought-chain-container [data-tool-call-status="success"] svg path,
            .thought-chain-container [data-tool-call-status="success"] path {
              color: #10b981 !important;
              fill: #10b981 !important;
              stroke: #10b981 !important;
            }
            /* 直接针对 Ant Design 图标类名和 SVG 元素 */
            .thought-chain-container .tool-call-icon-error [class*="anticon"],
            .thought-chain-container .tool-call-icon-error [class*="anticon"] svg,
            .thought-chain-container .tool-call-icon-error [class*="anticon"] svg path,
            .thought-chain-container .tool-call-icon-error svg,
            .thought-chain-container .tool-call-icon-error svg path,
            .thought-chain-container .tool-call-icon-error path {
              color: #ef4444 !important;
              fill: #ef4444 !important;
              stroke: #ef4444 !important;
            }
            .thought-chain-container .tool-call-icon-success [class*="anticon"],
            .thought-chain-container .tool-call-icon-success [class*="anticon"] svg,
            .thought-chain-container .tool-call-icon-success [class*="anticon"] svg path,
            .thought-chain-container .tool-call-icon-success svg,
            .thought-chain-container .tool-call-icon-success svg path,
            .thought-chain-container .tool-call-icon-success path {
              color: #10b981 !important;
              fill: #10b981 !important;
              stroke: #10b981 !important;
            }
            /* 针对 Ant Design 图标的具体类名 */
            .thought-chain-container .anticon-close-circle svg,
            .thought-chain-container .anticon-close-circle svg path {
              fill: #ef4444 !important;
              color: #ef4444 !important;
            }
            .thought-chain-container .anticon-check-circle svg,
            .thought-chain-container .anticon-check-circle svg path {
              fill: #10b981 !important;
              color: #10b981 !important;
            }
            /* TwoTone 图标颜色支持 */
            .thought-chain-container .anticon-check-circle-two-tone svg path[fill*="#"],
            .thought-chain-container .anticon-check-circle-two-tone svg path[fill*="rgb"] {
              fill: #10b981 !important;
            }
            .thought-chain-container .anticon-close-circle-two-tone svg path[fill*="#"],
            .thought-chain-container .anticon-close-circle-two-tone svg path[fill*="rgb"] {
              fill: #ef4444 !important;
            }
          `}} />
          <ThoughtChain items={chainItems} expandedKeys={expandedKeys} onExpand={setExpandedKeys} />
        </div>
      </div>
    </ConfigProvider>
  );
});

ThoughtChainPanel.displayName = 'ThoughtChainPanel';

export default ThoughtChainPanel;

