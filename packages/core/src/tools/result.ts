import { normalizeRunEvent } from '../run-events.js'
import { readNdjsonRecords } from './events.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { inspectTool } from './inspect.js'
import { findExternalRun } from './run-registry.js'
import { statusTool, type StatusResult } from './status.js'
import { validateTimeout } from './validate.js'

const DEFAULT_POLL_INTERVAL_MS = 1_000
const TERMINAL_STATUSES = new Set(['done', 'failed', 'timeout', 'killed'])

export interface ResultResult {
  run_id: string
  label: string
  status: string
  ready: boolean
  runtime_id?: string
  session_id?: string
  stop_reason?: string
  pending_permission?: StatusResult['pending_permission']
  output?: string
  // Present only when an older/unavailable control server prevented lossless
  // retrieval; callers can resume via avenor_result or the durable event path.
  output_truncated?: boolean
  output_event_path?: string
  timed_out?: boolean
}

interface ResultToolDeps {
  status: typeof statusTool
  // Only the final-output fallbacks the inspector can supply are read here.
  inspect: (
    args: Parameters<typeof inspectTool>[0],
  ) => Promise<{ final_output?: string; snapshot: { final_output?: string; assistant_text?: string } }>
  /** The control result verb for a runtime; older supervisors may not implement it. */
  result(supervisorId: string | undefined, runtimeId: string): Promise<Record<string, unknown>>
  /** The run's registered sentinel path, or undefined when the run is not registered. */
  sentinelPath(supervisorId: string | undefined, runId: string): Promise<string | undefined>
  sleep(ms: number, signal?: AbortSignal): Promise<void>
  now(): number
  pollIntervalMs: number
}

function abortError(): Error {
  const error = new Error('result wait cancelled')
  error.name = 'AbortError'
  return error
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(abortError())

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = () => {
      clearTimeout(timer)
      reject(abortError())
    }
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

async function outputFromEvents(filePath: string): Promise<string | undefined> {
  // A missing or unreadable log rejects and the caller's catch leaves the
  // fallback output marked possibly truncated.
  let output: string | undefined
  for await (const record of readNdjsonRecords(filePath)) {
    try {
      const event = normalizeRunEvent(record)
      if (event?.event === 'session.end' && event.final_output !== undefined) {
        output = event.final_output
      }
    } catch {
      // Ignore incomplete or malformed events.
    }
  }
  return output
}

async function realResult(supervisorId: string | undefined, runtimeId: string): Promise<Record<string, unknown>> {
  const { client, isSingleton } = await realGetSupervisorClient(supervisorId)
  try {
    return await client.result(runtimeId)
  } finally {
    if (!isSingleton) client.close()
  }
}

async function realSentinelPath(supervisorId: string | undefined, runId: string): Promise<string | undefined> {
  const { sup, supervisorId: resolvedId } = await realGetSupervisorClient(supervisorId)
  const local = sup ? sup.getRunByReference(runId) : undefined
  return local?.sentinelPath ?? findExternalRun(resolvedId, runId)?.sentinelPath
}

const defaultDeps: ResultToolDeps = {
  status: statusTool,
  inspect: inspectTool,
  result: realResult,
  sentinelPath: realSentinelPath,
  sleep,
  now: Date.now,
  pollIntervalMs: DEFAULT_POLL_INTERVAL_MS,
}

function toResult(status: StatusResult, timedOut = false): ResultResult {
  const ready = TERMINAL_STATUSES.has(status.status)
  return {
    run_id: status.run_id,
    label: status.label,
    status: status.status,
    ready,
    runtime_id: status.runtime_id,
    session_id: status.session_id,
    stop_reason: status.stop_reason,
    pending_permission: status.pending_permission,
    output: ready ? status.final_output : undefined,
    output_truncated: ready && status.final_output_truncated ? true : undefined,
    output_event_path: ready && status.final_output_truncated ? status.event_path : undefined,
    timed_out: timedOut || undefined,
  }
}

export interface ResultToolArgs {
  runId: string
  supervisorId?: string
  wait?: boolean
  timeout?: string
  signal?: AbortSignal
}

async function executeResultTool(args: ResultToolArgs, deps: ResultToolDeps): Promise<ResultResult> {
  const wait = args.wait ?? true
  const timeoutMs = args.timeout === undefined ? undefined : validateTimeout(args.timeout) * 1_000
  const deadline = timeoutMs === undefined ? undefined : deps.now() + timeoutMs
  const pollIntervalMs = Math.max(1, deps.pollIntervalMs)

  while (true) {
    if (args.signal?.aborted) throw abortError()

    const raw = await deps.status({
      runId: args.runId,
      supervisorId: args.supervisorId,
      view: 'full',
    })
    const status = Array.isArray(raw) ? raw[0] : raw
    if (!status) throw new Error(`run not found: ${args.runId}`)

    if (TERMINAL_STATUSES.has(status.status)) {
      // The runtime can already belong to another turn. Only the per-run
      // event log identifies this turn's complete terminal answer.
      let exactOutputRetrieved = false
      try {
        let output = status.event_path ? await outputFromEvents(status.event_path).catch(() => undefined) : undefined
        if (output !== undefined) {
          status.final_output = output
          status.final_output_truncated = false
          exactOutputRetrieved = true
        }
      } catch {
        // Older supervisors may not implement result yet. Any presentation
        // fallback below is explicitly marked as possibly truncated.
      }
      if (!exactOutputRetrieved && status.runtime_output_available !== false && status.runtime_id) {
        // The runtime can already own another turn. The control result verb
        // answers for the runtime's current turn, so its output belongs to
        // this turn only when it identifies this turn's sentinel. A run
        // with no registered turn has no turn identity to check against.
        try {
          const result = await deps.result(args.supervisorId, status.runtime_id)
          const output = typeof result.final_output === 'string' ? result.final_output : undefined
          const sentinel = await deps.sentinelPath(args.supervisorId, status.run_id)
          const matchesTurn = sentinel === undefined
            ? true
            : sentinel !== '' && typeof result.sentinel_file === 'string' && result.sentinel_file === sentinel
          if (output !== undefined && matchesTurn) {
            status.final_output = output
            status.final_output_truncated = false
            exactOutputRetrieved = true
          }
        } catch {
          // The result verb is unavailable or refused: any presentation
          // fallback below is explicitly marked as possibly truncated.
        }
      }
      if (!exactOutputRetrieved && status.final_output === undefined && status.runtime_output_available !== false) {
        try {
          const inspected = await deps.inspect({
            runId: args.runId,
            supervisorId: args.supervisorId,
          })
          status.final_output = inspected.final_output ?? inspected.snapshot.final_output ?? inspected.snapshot.assistant_text
        } catch {
          // The terminal status is still useful when detailed run history is unavailable.
        }
      }
      // Older supervisors did not expose final_output_truncated. Since status
      // and inspection are bounded presentation surfaces, a fallback output
      // must remain explicitly uncertain rather than being claimed complete.
      if (!exactOutputRetrieved && status.final_output !== undefined) {
        status.final_output_truncated = true
      }
      return toResult(status)
    }

    if (status.status === 'waiting' || !wait) {
      return toResult(status)
    }

    if (deadline !== undefined && deps.now() >= deadline) {
      return toResult(status, true)
    }

    const delay = deadline === undefined
      ? pollIntervalMs
      : Math.min(pollIntervalMs, Math.max(1, deadline - deps.now()))
    await deps.sleep(delay, args.signal)
  }
}

export async function resultTool(args: ResultToolArgs): Promise<ResultResult> {
  return executeResultTool(args, defaultDeps)
}

export function createResultTool(deps: ResultToolDeps): (args: ResultToolArgs) => Promise<ResultResult> {
  return args => executeResultTool(args, deps)
}
