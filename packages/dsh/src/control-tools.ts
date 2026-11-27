import { killTool, resultTool, statusTool } from '@dougbots/avenor-core'
import type { ToolDefinitionLike } from './answer-tool.js'

export const STATUS_TOOL_NAME = 'avenor_status'
export const RESULT_TOOL_NAME = 'avenor_result'
export const KILL_TOOL_NAME = 'avenor_kill'

export interface StatusRequest {
  readonly runId?: string
  readonly view?: 'lifecycle' | 'full'
}

export interface ResultRequest {
  readonly runId: string
  readonly wait?: boolean
  readonly timeout?: string
}

export interface KillRequest {
  readonly runId: string
}

/** The control calls these tools make; injectable so tests need no broker. */
export interface ControlTools {
  status(request: StatusRequest): Promise<unknown>
  result(request: ResultRequest): Promise<unknown>
  kill(request: KillRequest): Promise<unknown>
}

const defaultControlTools: ControlTools = {
  status: (request) => statusTool(request),
  result: (request) => resultTool(request),
  kill: (request) => killTool(request),
}

// A raw JSON Schema the registry accepts: no authoring-only types.
const outputSchema = { type: 'object', properties: {}, additionalProperties: true }

function renderJson(_args: unknown, value: unknown) {
  const record = (value ?? {}) as { text?: unknown }
  const text =
    typeof record.text === 'string' && record.text !== '' ? record.text : JSON.stringify(value)
  return [{ type: 'text' as const, text }]
}

/**
 * One control tool per verb. These act on the session's supervisor, not on the
 * bound conversation: they read and stop any run it knows, including one this
 * session never spawned.
 */
export function controlToolDefinitions(control: ControlTools = defaultControlTools): ToolDefinitionLike[] {
  return [
    {
      name: STATUS_TOOL_NAME,
      description:
        'Report Avenor run status: one run, or every run on the supervisor. The result carries ' +
        'pending_permission when a run is blocked, so it is also how a stalled delegation is found ' +
        'after its notice was missed.',
      parameters: {
        type: 'object',
        properties: {
          runId: { type: 'string', description: 'Run id, label, or runtime id. Omit to list every run.' },
          view: {
            type: 'string',
            enum: ['lifecycle', 'full'],
            description: 'lifecycle omits final output and diagnostics; full is the default.',
          },
        },
        required: [],
      },
      output: { schema: outputSchema, render: renderJson },
      execute: async (args) => control.status(parseStatus(args)),
    },
    {
      name: RESULT_TOOL_NAME,
      description:
        'Read a finished Avenor run’s final output. A delegation from this session already delivers ' +
        'its result when the child settles, so this is for a run you did not spawn, one whose notice ' +
        'was compacted away, or one read after a restart. It does not wait unless wait is true.',
      parameters: {
        type: 'object',
        properties: {
          runId: { type: 'string', description: 'Run id, label, or runtime id.' },
          wait: {
            type: 'boolean',
            description: 'Block until the run finishes (default false: report what is ready now).',
          },
          timeout: {
            type: 'string',
            description: 'Bound on the wait, such as 90s or 5m. Only used when wait is true.',
          },
        },
        required: ['runId'],
      },
      output: { schema: outputSchema, render: renderJson },
      execute: async (args) => control.result(parseResult(args)),
    },
    {
      name: KILL_TOOL_NAME,
      description:
        'Cancel an Avenor run. The delegation has no other stop path: a child that hangs, or one ' +
        'blocked on a permission nobody will answer, keeps its slot until this is called.',
      parameters: {
        type: 'object',
        properties: {
          runId: { type: 'string', description: 'Run id, label, or runtime id.' },
        },
        required: ['runId'],
      },
      output: { schema: outputSchema, render: renderJson },
      execute: async (args) => control.kill(parseKill(args)),
    },
  ]
}

function parseStatus(args: unknown): StatusRequest {
  const record = objectOf(args)
  const runId = optionalString(record, 'runId')
  const view = record.view
  if (view !== undefined && view !== 'lifecycle' && view !== 'full') {
    throw new Error('view must be lifecycle or full')
  }
  return { ...(runId === undefined ? {} : { runId }), ...(view === undefined ? {} : { view }) }
}

function parseResult(args: unknown): ResultRequest {
  const record = objectOf(args)
  const runId = requiredString(record, 'runId')
  const wait = record.wait
  if (wait !== undefined && typeof wait !== 'boolean') throw new Error('wait must be a boolean')
  const timeout = optionalString(record, 'timeout')
  return {
    runId,
    wait: wait === true,
    ...(timeout === undefined ? {} : { timeout }),
  }
}

function parseKill(args: unknown): KillRequest {
  return { runId: requiredString(objectOf(args), 'runId') }
}

function objectOf(args: unknown): Record<string, unknown> {
  if (typeof args !== 'object' || args === null) throw new Error('args must be an object')
  return args as Record<string, unknown>
}

function optionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key]
  if (value === undefined) return undefined
  if (typeof value !== 'string' || value === '') throw new Error(`${key} must be a non-empty string`)
  return value
}

function requiredString(record: Record<string, unknown>, key: string): string {
  const value = optionalString(record, key)
  if (value === undefined) throw new Error(`${key} is required`)
  return value
}
