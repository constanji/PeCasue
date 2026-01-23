import type { Document, Types } from 'mongoose';

export interface PromptItem {
  key: string;
  icon: string;
  label: string;
  description: string;
  prompt: string;
}

export interface PromptsConfig {
  title?: string;
  items: PromptItem[];
}

export type AgentPromptsConfigData = {
  configId?: string;
  global?: PromptsConfig;
  dataSources?: Record<string, PromptsConfig>;
};

export type IAgentPromptsConfig = AgentPromptsConfigData &
  Document & {
    _id: Types.ObjectId;
    createdAt?: Date;
    updatedAt?: Date;
  };

