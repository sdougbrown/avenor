import { inspectTool } from './inspect.js'
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
  timed_out?: boolean
}

interface ResultToolDeps {
  status: typeof statusTool
  inspect: typeof inspectTool
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

const defaultDeps: ResultToolDeps = {
  status: statusTool,
  inspect: inspectTool,
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
      if (!status.final_output) {
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
