import { atom } from 'recoil';

/** Recoil atoms for the pipeline module. URL stays the source of truth for ?taskId=
 *  &channel=&runId=&compareId=, but we mirror them here so cross-tab handlers
 *  can read without re-parsing search params on every render.
 */

export const pipelineSelectedTaskIdAtom = atom<string | null>({
  key: 'pipeline.selectedTaskId',
  default: null,
});

export const pipelineSelectedChannelIdAtom = atom<string | null>({
  key: 'pipeline.selectedChannelId',
  default: null,
});

export const pipelineSelectedRunIdAtom = atom<string | null>({
  key: 'pipeline.selectedRunId',
  default: null,
});

export const pipelineCopilotOpenAtom = atom<boolean>({
  key: 'pipeline.copilotOpen',
  default: false,
});

/** Phase 6: regulation page draft cache (used by beforeunload guard). */
export const pipelineRulesDirtyAtom = atom<boolean>({
  key: 'pipeline.rulesDirty',
  default: false,
});

/** Phase 7: Copilot pre-fill — stamped when user clicks "问 Agent" on a verify row. */
export interface PipelineCopilotPrefill {
  question: string;
  channel_id?: string | null;
  run_id?: string | null;
  verify_row_id?: string | null;
  stamp: number;
}

export const pipelineCopilotPrefillAtom = atom<PipelineCopilotPrefill | null>({
  key: 'pipeline.copilotPrefill',
  default: null,
});
