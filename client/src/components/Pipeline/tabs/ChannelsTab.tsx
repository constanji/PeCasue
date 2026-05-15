import React, { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useRecoilState, useSetRecoilState } from 'recoil';
import { GitBranch } from 'lucide-react';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';
import EmptyTabPlaceholder from './EmptyTabPlaceholder';
import ChannelDetail from '../channel/ChannelDetail';
import SpecialBundleChannelDetail from '../channel/SpecialBundleChannelDetail';
import ChannelSidebar from '../channel/ChannelSidebar';
import FinalMergeChannelDetail from '../channel/FinalMergeChannelDetail';
import {
  PIPELINE_OVERVIEW_MAIN_CHANNELS,
  isFinalMergeDetailRoute,
  isSpecialBundleDetailRoute,
} from '~/components/Pipeline/overviewChannels';

const DEFAULT_MAIN_CHANNEL_ID = PIPELINE_OVERVIEW_MAIN_CHANNELS[0].channel_id;

export default function ChannelsTab() {
  const [searchParams, setSearchParams] = useSearchParams();
  const taskId = searchParams.get('taskId');
  const channelId = searchParams.get('channel');
  const [selectedTaskId, setSelectedTaskId] = useRecoilState(pipelineSelectedTaskIdAtom);
  const [selectedChannelId, setSelectedChannelId] = useRecoilState(
    pipelineSelectedChannelIdAtom,
  );
  const setSelectedRunId = useSetRecoilState(pipelineSelectedRunIdAtom);

  useEffect(() => {
    if (taskId && taskId !== selectedTaskId) setSelectedTaskId(taskId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId]);
  useEffect(() => {
    if (channelId && channelId !== selectedChannelId) setSelectedChannelId(channelId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [channelId]);

  const tid = taskId || selectedTaskId;

  const tab = searchParams.get('tab');

  /** 分摊基数改在「最终分摊」承接：旧链接 /channel=allocation_base 自动跳转 */
  useEffect(() => {
    if (tab !== 'channels' || !tid) return;
    const c = channelId || selectedChannelId;
    if (c !== 'allocation_base') return;
    const next = new URLSearchParams(searchParams);
    next.set('tab', 'final_allocation');
    next.set('alloc', 'merge');
    next.set('taskId', tid);
    next.delete('channel');
    next.delete('runId');
    setSearchParams(next, { replace: true });
    setSelectedChannelId(null);
    setSelectedRunId(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, tid, channelId, selectedChannelId]);

  useEffect(() => {
    if (!tid || channelId) return;
    /** 仅在「渠道详情」页补全默认 channel；返回总览后 tab=overview 且无时，不得把 URL 抢回 channels */
    if (tab !== 'channels') return;
    const next = new URLSearchParams(searchParams);
    next.set('tab', 'channels');
    next.set('taskId', tid);
    next.set('channel', DEFAULT_MAIN_CHANNEL_ID);
    setSearchParams(next, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tid, channelId, tab]);

  if (!tid) {
    return (
      <EmptyTabPlaceholder
        icon={<GitBranch className="h-10 w-10" aria-hidden="true" />}
        title="渠道详情"
        description="请先在“总览”里选中一个任务，再回到这里查看渠道执行明细。"
      />
    );
  }

  const cid = channelId || selectedChannelId || DEFAULT_MAIN_CHANNEL_ID;

  if (cid === 'allocation_base') {
    return (
      <div className="flex h-full items-center justify-center p-6 text-sm text-text-secondary">
        正在跳转「最终分摊」…
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 overflow-hidden">
      <ChannelSidebar
        taskId={tid}
        selectedChannelId={cid}
        omitChannelIds={['allocation_base']}
        onSelect={(picked) => {
          const next = new URLSearchParams(searchParams);
          next.set('tab', 'channels');
          next.set('taskId', tid);
          next.set('channel', picked);
          setSearchParams(next);
          setSelectedChannelId(picked);
        }}
      />
      <div className="min-h-0 min-w-0 flex-1 overflow-hidden">
        {isFinalMergeDetailRoute(cid) ? (
          <FinalMergeChannelDetail
            taskId={tid}
            onBack={() => {
              setSelectedChannelId(null);
              setSelectedRunId(null);
              const next = new URLSearchParams(searchParams);
              next.set('tab', 'overview');
              next.delete('channel');
              next.delete('runId');
              setSearchParams(next, { replace: true });
            }}
          />
        ) : isSpecialBundleDetailRoute(cid) ? (
          <SpecialBundleChannelDetail
            taskId={tid}
            channelId={cid}
            onBack={() => {
              setSelectedChannelId(null);
              setSelectedRunId(null);
              const next = new URLSearchParams(searchParams);
              next.set('tab', 'overview');
              next.delete('channel');
              next.delete('runId');
              setSearchParams(next, { replace: true });
            }}
          />
        ) : (
          <ChannelDetail
            taskId={tid}
            channelId={cid}
            onBack={() => {
              setSelectedChannelId(null);
              setSelectedRunId(null);
              const next = new URLSearchParams(searchParams);
              next.set('tab', 'overview');
              next.delete('channel');
              next.delete('runId');
              setSearchParams(next, { replace: true });
            }}
          />
        )}
      </div>
    </div>
  );
}
