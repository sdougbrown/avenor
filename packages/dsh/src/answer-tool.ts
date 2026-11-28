import type { ContentBlock } from './types.js'

export const ANSWER_TOOL_NAME = 'avenor_answer_permission'

export interface AnswerRequest {
  readonly runId: string
  readonly optionId: string
  readonly requestId?: string
}

export interface AnswerDeps {
  answer(request: AnswerRequest): Promise<{ ok: boolean }>
}

/**
 * A tool definition as `ctx.tools.register` consumes it.
 *
 * Normally authored with `defineTool` from `@deepseek-ai/dsh-tools`, which
 * compiles the schema DSL and wraps `execute` with argument validation. A
 * bundle cannot import that module — DSH resolves shipped packages for *loader
 * rows*, not for imports from a plugin's own JS — so this declares the same
 * contract directly, in plain JSON Schema, and validates its own arguments.
 */
export interface ToolDefinitionLike {
  readonly name: string
  readonly description: string
  readonly parameters: {
    readonly type: 'object'
    readonly properties: Record<string, unknown>
    readonly required?: readonly string[]
  }
  readonly output: {
    readonly schema: unknown
    render(args: unknown, value: unknown): ContentBlock[]
  }
  execute(args: unknown): Promise<unknown>
}

function parseAnswerRequest(args: unknown): AnswerRequest {
  if (typeof args !== 'object' || args === null) throw new Error('args must be an object')
  const record = args as Record<string, unknown>
  const runId = record.runId
  const optionId = record.optionId
  const requestId = record.requestId
  if (typeof runId !== 'string' || runId === '') throw new Error('runId must be a non-empty string')
  if (typeof optionId !== 'string' || optionId === '') {
    throw new Error('optionId must be a non-empty string')
  }
  if (requestId !== undefined && typeof requestId !== 'string') {
    throw new Error('requestId must be a string when present')
  }
  return {
    runId,
    optionId,
    ...(requestId === undefined ? {} : { requestId }),
  }
}

/**
 * The tool the orchestrator calls to answer a blocked child. The option ids
 * come from the request Avenor reported (`allow_once`, `allow_always`, `deny`
 * today), never hardcoded here.
 */
export function answerToolDefinition(deps: AnswerDeps): ToolDefinitionLike {
  return {
    name: ANSWER_TOOL_NAME,
    description:
      'Answer a permission request from an Avenor run you delegated. Pass the run id and ' +
      'one of the option ids from the request; the run continues or stops accordingly.',
    parameters: {
      type: 'object',
      properties: {
        runId: {
          type: 'string',
          description: 'Run id reported in the permission notice.',
        },
        optionId: {
          type: 'string',
          description: 'One option id from the notice, e.g. allow_once, allow_always, or deny.',
        },
        requestId: {
          type: 'string',
          description: 'Request id from the notice; omit to answer the currently pending one.',
        },
      },
      required: ['runId', 'optionId'],
    },
    output: {
      schema: {
        type: 'object',
        properties: { ok: { type: 'boolean' } },
        required: ['ok'],
      },
      render: (_args, value) => {
        const ok = (value as { ok?: unknown } | null)?.ok === true
        return [
          {
            type: 'text',
            text: ok ? 'The permission answer was accepted.' : 'The permission answer was not accepted.',
          },
        ]
      },
    },
    execute: async (args) => deps.answer(parseAnswerRequest(args)),
  }
}
