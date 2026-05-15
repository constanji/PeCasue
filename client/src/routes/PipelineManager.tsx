import { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronLeft, Workflow } from 'lucide-react';
import { Spinner } from '@because/client';
import { useGetStartupConfig, useGetEndpointsQuery } from '~/data-provider';
import { useAppStartup, useLocalize, usePipelineAccess } from '~/hooks';
import { ChatContext } from '~/Providers';
import useChatHelpers from '~/hooks/Chat/useChatHelpers';
import useAuthRedirect from './useAuthRedirect';
import store from '~/store';
import { PipelineContent } from '~/components/Pipeline';

export default function PipelineManager() {
  const { data: startupConfig } = useGetStartupConfig();
  const { isAuthenticated, user } = useAuthRedirect();
  const navigate = useNavigate();
  const localize = useLocalize();
  const endpointsQuery = useGetEndpointsQuery({ enabled: isAuthenticated });
  const { hasAccess } = usePipelineAccess();

  useAppStartup({ startupConfig, user });

  const index = 0;
  const { conversation } = store.useCreateConversationAtom(index);
  const chatHelpers = useChatHelpers(index, 'new');

  useEffect(() => {
    if (isAuthenticated && !hasAccess) {
      navigate('/c/new', { replace: true });
    }
  }, [isAuthenticated, hasAccess, navigate]);

  if (endpointsQuery.isLoading) {
    return (
      <div className="flex h-screen items-center justify-center" aria-live="polite" role="status">
        <Spinner className="text-text-primary" />
      </div>
    );
  }

  if (!isAuthenticated || !hasAccess) {
    return null;
  }

  return (
    <ChatContext.Provider value={chatHelpers}>
      <div className="flex h-full w-full flex-col overflow-hidden bg-background">
        <div className="flex h-full w-full flex-row">
          <div className="flex h-full w-full flex-col overflow-hidden">
            <div className="flex h-full w-full flex-col">
              <div className="mb-4 px-4 pt-4">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div
                      className="flex h-10 w-10 items-center justify-center rounded-xl border border-border-light bg-green-500/10 text-green-500"
                      aria-hidden="true"
                    >
                      <Workflow className="h-5 w-5" />
                    </div>
                    <div>
                      <h1 className="text-2xl font-semibold text-text-primary">流水线</h1>
                      <p className="mt-1 text-sm text-text-secondary">
                        企业账单与流水智能对账 · Human / Agent / Machine 协作
                      </p>
                    </div>
                  </div>
                  <button
                    type="button"
                    className="btn btn-neutral border-token-border-light relative flex items-center gap-2 rounded-lg px-3 py-2"
                    onClick={() => navigate('/c/new')}
                    aria-label={localize('com_ui_back')}
                  >
                    <ChevronLeft className="h-4 w-4" />
                    <span>{localize('com_ui_back')}</span>
                  </button>
                </div>
              </div>
              <div className="flex-1 min-h-0 overflow-hidden">
                <PipelineContent />
              </div>
            </div>
          </div>
        </div>
      </div>
    </ChatContext.Provider>
  );
}
