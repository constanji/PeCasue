import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useRecoilState } from 'recoil';
import {
  pipelineSelectedChannelIdAtom,
  pipelineSelectedRunIdAtom,
  pipelineSelectedTaskIdAtom,
} from '~/store/pipeline';

/**
 * Bi-directional sync between URL search params and the pipeline Recoil atoms.
 *
 * URL is treated as the source of truth so a hard refresh restores the user's
 * exact context. Recoil mirrors the values so cross-tab handlers / components
 * far from the router can subscribe without re-parsing search params on every
 * render.
 *
 * Tracked params:
 *   - taskId   ↔ pipelineSelectedTaskIdAtom
 *   - channel  ↔ pipelineSelectedChannelIdAtom
 *   - runId    ↔ pipelineSelectedRunIdAtom
 *
 * The other params (`tab`, `compareId`, `rule`) are owned by the individual
 * tab components because they're tab-local UI state.
 */
export function usePipelineUrlSync() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [taskId, setTaskId] = useRecoilState(pipelineSelectedTaskIdAtom);
  const [channelId, setChannelId] = useRecoilState(pipelineSelectedChannelIdAtom);
  const [runId, setRunId] = useRecoilState(pipelineSelectedRunIdAtom);

  // URL → Recoil
  useEffect(() => {
    const t = searchParams.get('taskId');
    const c = searchParams.get('channel');
    const r = searchParams.get('runId');
    if (t !== taskId) setTaskId(t || null);
    if (c !== channelId) setChannelId(c || null);
    if (r !== runId) setRunId(r || null);
    // intentionally omit deps: only react to URL changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  // Recoil → URL (mirror the atoms back so navigating from non-tab components,
  // e.g. selecting a task in OverviewTab, lands in the URL).
  useEffect(() => {
    const next = new URLSearchParams(searchParams);
    let changed = false;
    if (taskId && next.get('taskId') !== taskId) {
      next.set('taskId', taskId);
      changed = true;
    } else if (!taskId && next.has('taskId')) {
      next.delete('taskId');
      changed = true;
    }
    if (channelId && next.get('channel') !== channelId) {
      next.set('channel', channelId);
      changed = true;
    } else if (!channelId && next.has('channel')) {
      next.delete('channel');
      changed = true;
    }
    if (runId && next.get('runId') !== runId) {
      next.set('runId', runId);
      changed = true;
    } else if (!runId && next.has('runId')) {
      next.delete('runId');
      changed = true;
    }
    if (changed) setSearchParams(next, { replace: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId, channelId, runId]);
}
