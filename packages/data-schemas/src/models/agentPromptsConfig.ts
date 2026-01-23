import agentPromptsConfigSchema from '~/schema/agentPromptsConfig';
import type { IAgentPromptsConfig } from '~/types';

/**
 * Creates or returns the AgentPromptsConfig model using the provided mongoose instance and schema
 */
export function createAgentPromptsConfigModel(mongoose: typeof import('mongoose')) {
  return (
    mongoose.models.AgentPromptsConfig ||
    mongoose.model<IAgentPromptsConfig>('AgentPromptsConfig', agentPromptsConfigSchema)
  );
}

