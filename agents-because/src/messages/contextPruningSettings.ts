import type { ContextPruningConfig } from '@/types/graph';

/** Resolved numeric/string settings for {@link applyContextPruning}. */
export interface ContextPruningSettings {
  enabled: boolean;
  keepLastAssistants: number;
  softTrimRatio: number;
  hardClearRatio: number;
  minPrunableToolChars: number;
  softTrim: {
    maxChars: number;
    headChars: number;
    tailChars: number;
  };
  hardClear: {
    enabled: boolean;
    placeholder: string;
  };
}

const DEFAULTS: ContextPruningSettings = {
  enabled: true,
  keepLastAssistants: 2,
  softTrimRatio: 0.85,
  hardClearRatio: 0.95,
  minPrunableToolChars: 500,
  softTrim: {
    maxChars: 8000,
    headChars: 2000,
    tailChars: 2000,
  },
  hardClear: {
    enabled: true,
    placeholder: '[tool output cleared for context]',
  },
};

/**
 * Merges partial YAML / graph config with defaults for position-based tool pruning.
 */
export function resolveContextPruningSettings(
  config?: ContextPruningConfig
): ContextPruningSettings {
  if (!config) {
    return { ...DEFAULTS };
  }

  const soft = config.softTrim ?? {};
  const hard = config.hardClear ?? {};

  return {
    enabled: config.enabled ?? DEFAULTS.enabled,
    keepLastAssistants: config.keepLastAssistants ?? DEFAULTS.keepLastAssistants,
    softTrimRatio: config.softTrimRatio ?? DEFAULTS.softTrimRatio,
    hardClearRatio: config.hardClearRatio ?? DEFAULTS.hardClearRatio,
    minPrunableToolChars: config.minPrunableToolChars ?? DEFAULTS.minPrunableToolChars,
    softTrim: {
      maxChars: soft.maxChars ?? DEFAULTS.softTrim.maxChars,
      headChars: soft.headChars ?? DEFAULTS.softTrim.headChars,
      tailChars: soft.tailChars ?? DEFAULTS.softTrim.tailChars,
    },
    hardClear: {
      enabled: hard.enabled ?? DEFAULTS.hardClear.enabled,
      placeholder: hard.placeholder ?? DEFAULTS.hardClear.placeholder,
    },
  };
}
