import { Schema } from 'mongoose';
import type { IAgentPromptsConfig } from '~/types';

const promptItemSchema = new Schema(
  {
    key: { type: String, required: true },
    icon: { type: String, default: 'bulb' },
    label: { type: String, default: '' },
    description: { type: String, default: '' },
    prompt: { type: String, default: '' },
  },
  { _id: false },
);

const promptsConfigSchema = new Schema(
  {
    title: { type: String },
    items: { type: [promptItemSchema], default: [] },
  },
  { _id: false },
);

const agentPromptsConfigSchema = new Schema<IAgentPromptsConfig>(
  {
    configId: {
      type: String,
      default: 'default',
      unique: true,
      index: true,
    },
    global: { type: promptsConfigSchema },
    dataSources: { type: Schema.Types.Mixed, default: {} },
  },
  {
    timestamps: true,
  },
);

export default agentPromptsConfigSchema;

