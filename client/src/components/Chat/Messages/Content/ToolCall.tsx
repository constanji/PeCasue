import { useMemo, useState, useEffect, useRef, useLayoutEffect } from 'react';
import { Button } from '@because/client';
import { TriangleAlert } from 'lucide-react';
import { actionDelimiter, actionDomainSeparator, Constants, Tools } from '@because/data-provider';
import type { TAttachment } from '@because/data-provider';
import { useLocalize, useProgress } from '~/hooks';
import { AttachmentGroup } from './Parts';
import ToolCallInfo from './ToolCallInfo';
import ProgressText from './ProgressText';
import { logger, cn } from '~/utils';
import { extractChartDataFromToolOutput } from './ChartRenderer';

export default function ToolCall({
  initialProgress = 0.1,
  isLast = false,
  isSubmitting,
  name,
  args: _args = '',
  output,
  attachments,
  auth,
}: {
  initialProgress: number;
  isLast?: boolean;
  isSubmitting: boolean;
  name: string;
  args: string | Record<string, unknown>;
  output?: string | null;
  attachments?: TAttachment[];
  auth?: string;
  expires_at?: number;
}) {
  const localize = useLocalize();
  const [showInfo, setShowInfo] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);
  const [contentHeight, setContentHeight] = useState<number | undefined>(0);
  const [isAnimating, setIsAnimating] = useState(false);
  const prevShowInfoRef = useRef<boolean>(showInfo);

  const becauseSkillsCommandMap: Record<string, string> = {
    'database-schema': '获取数据库Schema',
    'intent-classification': '意图识别',
    'rag-retrieval': 'RAG检索',
    'sql-validation': 'SQL语句验证',
    'sql-executor': 'SQL执行',
    'result-analysis': '归因调查',
    'chart-generation': '可视化图表生成',
    'reranker': '结果重排序',
    'fluctuation-attribution': '波动归因分析',
  };

  const { function_name, domain, isMCPToolCall } = useMemo(() => {
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
  }, [name]);

  const displayName = useMemo(() => {
    if (function_name !== 'because_skills' && function_name !== 'because_skills_2') {
      return function_name;
    }
    const extractCommand = (source: string | Record<string, unknown>) => {
      if (typeof source === 'object' && source !== null && 'command' in source) {
        const cmd = source.command;
        if (typeof cmd === 'string' && cmd.length > 0) {
          return becauseSkillsCommandMap[cmd] || cmd;
        }
      }
      if (typeof source === 'string' && source.length > 0) {
        try {
          const parsed = JSON.parse(source);
          if (parsed?.command) {
            return becauseSkillsCommandMap[parsed.command] || parsed.command;
          }
        } catch { /* noop */ }
        const m = source.match(/"command"\s*:\s*"([^"]+)"/);
        if (m?.[1]) {
          return becauseSkillsCommandMap[m[1]] || m[1];
        }
        const partial = source.match(/"command"\s*:\s*"([^"]*)/);
        if (partial?.[1] && partial[1].length >= 3) {
          return becauseSkillsCommandMap[partial[1]] || partial[1];
        }
      }
      return null;
    };
    return extractCommand(_args) || function_name;
  }, [function_name, _args]);

  const error =
    typeof output === 'string' && output.toLowerCase().includes('error processing tool');

  const args = useMemo(() => {
    if (typeof _args === 'string') {
      return _args;
    }
    try {
      return JSON.stringify(_args, null, 2);
    } catch (e) {
      logger.error(
        'client/src/components/Chat/Messages/Content/ToolCall.tsx - Failed to stringify args',
        e,
      );
      return '';
    }
  }, [_args]) as string | undefined;

  const hasInfo = useMemo(
    () => {
      const hasArgs = (args?.length ?? 0) > 0;
      const hasOutput = (output?.length ?? 0) > 0;
      const hasUiResources = attachments?.some(
        (att) => {
          if (att.type === Tools.ui_resources) {
            const uiResourceData = att[Tools.ui_resources] as unknown;
            if (Array.isArray(uiResourceData)) {
              return uiResourceData.length > 0;
            }
            const container = uiResourceData as { data?: unknown[] } | undefined;
            if (container?.data && Array.isArray(container.data)) {
              return container.data.length > 0;
            }
          }
          return false;
        },
      ) ?? false;
      return hasArgs || hasOutput || hasUiResources;
    },
    [args, output, attachments],
  );

  const hasChartOutput = useMemo(
    () => (typeof output === 'string' ? extractChartDataFromToolOutput(output) !== null : false),
    [output],
  );

  const authDomain = useMemo(() => {
    const authURL = auth ?? '';
    if (!authURL) {
      return '';
    }
    try {
      const url = new URL(authURL);
      return url.hostname;
    } catch (e) {
      logger.error(
        'client/src/components/Chat/Messages/Content/ToolCall.tsx - Failed to parse auth URL',
        e,
      );
      return '';
    }
  }, [auth]);

  const progress = useProgress(initialProgress);
  const cancelled = (!isSubmitting && progress < 1) || error === true;

  const getFinishedText = () => {
    if (cancelled) {
      return localize('com_ui_cancelled');
    }
    if (isMCPToolCall === true) {
      return localize('com_assistants_completed_function', { 0: displayName });
    }
    if (domain != null && domain && domain.length !== Constants.ENCODED_DOMAIN_LENGTH) {
      return localize('com_assistants_completed_action', { 0: domain });
    }
    return localize('com_assistants_completed_function', { 0: displayName });
  };

  useLayoutEffect(() => {
    if (showInfo !== prevShowInfoRef.current) {
      prevShowInfoRef.current = showInfo;
      setIsAnimating(true);

      if (showInfo && contentRef.current) {
        requestAnimationFrame(() => {
          if (contentRef.current) {
            const height = contentRef.current.scrollHeight;
            setContentHeight(height + 4);
          }
        });
      } else {
        setContentHeight(0);
      }

      const timer = setTimeout(() => {
        setIsAnimating(false);
      }, 400);

      return () => clearTimeout(timer);
    }
  }, [showInfo]);

  useEffect(() => {
    if (!contentRef.current) {
      return;
    }
    const resizeObserver = new ResizeObserver((entries) => {
      if (showInfo && !isAnimating) {
        for (const entry of entries) {
          if (entry.target === contentRef.current) {
            setContentHeight(entry.contentRect.height + 4);
          }
        }
      }
    });
    resizeObserver.observe(contentRef.current);
    return () => {
      resizeObserver.disconnect();
    };
  }, [showInfo, isAnimating]);

  useEffect(() => {
    if (hasChartOutput && hasInfo) {
      setShowInfo(true);
    }
  }, [hasChartOutput, hasInfo]);

  if (!isLast && (!function_name || function_name.length === 0) && !output) {
    return null;
  }

  return (
    <>
      <div className="relative my-2.5 flex h-5 shrink-0 items-center gap-2.5">
        <ProgressText
          progress={progress}
          onClick={() => setShowInfo((prev) => !prev)}
          inProgressText={
            displayName
              ? localize('com_assistants_running_var', { 0: displayName })
              : localize('com_assistants_running_action')
          }
          authText={
            !cancelled && authDomain.length > 0 ? localize('com_ui_requires_auth') : undefined
          }
          finishedText={getFinishedText()}
          hasInput={hasInfo}
          isExpanded={showInfo}
          error={cancelled}
        />
      </div>
      <div
        className="relative"
        style={{
          height: showInfo ? contentHeight : 0,
          overflow: 'hidden',
          transition:
            'height 0.4s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.4s cubic-bezier(0.16, 1, 0.3, 1)',
          opacity: showInfo ? 1 : 0,
          transformOrigin: 'top',
          willChange: 'height, opacity',
          perspective: '1000px',
          backfaceVisibility: 'hidden',
          WebkitFontSmoothing: 'subpixel-antialiased',
        }}
      >
        <div
          className={cn(
            'overflow-hidden rounded-xl border border-border-light bg-surface-secondary shadow-md',
            showInfo && 'shadow-lg',
          )}
          style={{
            transform: showInfo ? 'translateY(0) scale(1)' : 'translateY(-8px) scale(0.98)',
            opacity: showInfo ? 1 : 0,
            transition:
              'transform 0.4s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.4s cubic-bezier(0.16, 1, 0.3, 1)',
          }}
        >
          <div ref={contentRef}>
            {showInfo && hasInfo && (
              <ToolCallInfo
                key="tool-call-info"
                input={args ?? ''}
                output={output}
                domain={authDomain || (domain ?? '')}
                function_name={function_name}
                pendingAuth={authDomain.length > 0 && !cancelled && progress < 1}
                attachments={attachments}
              />
            )}
          </div>
        </div>
      </div>
      {auth != null && auth && progress < 1 && !cancelled && (
        <div className="flex w-full flex-col gap-2.5">
          <div className="mb-1 mt-2">
            <Button
              className="font-mediu inline-flex items-center justify-center rounded-xl px-4 py-2 text-sm"
              variant="default"
              rel="noopener noreferrer"
              onClick={() => window.open(auth, '_blank', 'noopener,noreferrer')}
            >
              {localize('com_ui_sign_in_to_domain', { 0: authDomain })}
            </Button>
          </div>
          <p className="flex items-center text-xs text-text-warning">
            <TriangleAlert className="mr-1.5 inline-block h-4 w-4" />
            {localize('com_assistants_allow_sites_you_trust')}
          </p>
        </div>
      )}
      {attachments && attachments.length > 0 && <AttachmentGroup attachments={attachments} />}
    </>
  );
}
