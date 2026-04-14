import { zodToJsonSchema } from 'zod-to-json-schema';
import type { ZodType } from 'zod';

function isZodSchema(schema: unknown): schema is ZodType {
  return (
    typeof schema === 'object' &&
    schema !== null &&
    '_def' in schema &&
    typeof (schema as { parse?: unknown }).parse === 'function'
  );
}

function isJsonSchemaLike(value: Record<string, unknown>): boolean {
  return (
    value.type === 'object' ||
    (Array.isArray(value.type) && value.type.includes('object')) ||
    (typeof value.properties === 'object' && value.properties !== null) ||
    typeof value.$schema === 'string'
  );
}

/**
 * Build an OpenAI-style tool JSON blob for token counting (aligned with
 * inline `toolDefinitions` shape in AgentContext).
 */
export function toJsonSchema(
  schema: unknown,
  toolName: string,
  description: string
): {
  type: 'function';
  function: {
    name: string;
    description: string;
    parameters: Record<string, unknown>;
  };
} {
  let parameters: Record<string, unknown> = { type: 'object', properties: {} };

  if (schema != null && typeof schema === 'object') {
    const s = schema as Record<string, unknown>;

    if (s.type === 'function' && s.function && typeof s.function === 'object') {
      return s as {
        type: 'function';
        function: {
          name: string;
          description: string;
          parameters: Record<string, unknown>;
        };
      };
    }

    if (isZodSchema(schema)) {
      try {
        parameters = zodToJsonSchema(schema) as Record<string, unknown>;
      } catch {
        parameters = { type: 'object', properties: {} };
      }
    } else if (isJsonSchemaLike(s)) {
      parameters = s;
    }
  }

  return {
    type: 'function',
    function: {
      name: toolName,
      description,
      parameters,
    },
  };
}
