/**
 * Public telemetry types and adapters for companion Pi extensions.
 *
 * The transport is Pi's shared `pi.events` bus. This module owns no event
 * handlers or singleton state, so consumers can use the surface even when
 * package dependencies are resolved from different module instances.
 *
 * Payloads select status fields and omit transcripts, final output, directories,
 * socket paths, raw status objects, and tool arguments.
 */

import { createHash } from 'node:crypto'
import type { EventBus } from '@earendil-works/pi-coding-agent'
import type { RunStatusEntry } from './types.js'
import { sanitizeText } from './watch.js'

/** Emitted when the official extension completes a polling cycle. */
export const CHANNEL_POLL_COMPLETED = 'avenor:poll:completed' as const

/** Emitted when the extension observes a terminal run result. */
export const CHANNEL_RUN_TERMINAL = 'avenor:run:terminal' as const

/** Emitted when a best-effort polling operation fails. */
export const CHANNEL_POLL_ERROR = 'avenor:poll:error' as const

export type PollErrorSource = 'singleton-list' | 'run-status' | 'spawn-status'

export interface PollErrorPayload {
  /** Polling operation that failed. */
  readonly source: PollErrorSource
  /** Run associated with the failure, when the operation was run-specific. */
  readonly runId?: string
  /** Human-readable operation description. */
  readonly message: string
  /** Bounded, sanitized error text suitable for debug consumers. */
  readonly error: string
  /** Number of polling errors observed by this extension instance. */
  readonly count: number
  /** Wall-clock timestamp in epoch milliseconds. */
  readonly timestamp: number
}

export interface PollErrorPayloadInput {
  readonly source: PollErrorSource
  readonly runId?: string
  readonly message: string
  readonly error: unknown
  readonly count: number
  readonly timestamp: number
}

export type AvenorRunStatus = Readonly<Omit<
  RunStatusEntry,
  'supervisorId' | 'pid' | 'permissionDescription'
> & {
  supervisorKey: string
}>

export interface PollCompletedPayload {
  /** Status for each direct run tracked by the official extension. */
  readonly entries: readonly AvenorRunStatus[]
  /** Wall-clock timestamp in epoch milliseconds. */
  readonly timestamp: number
  /** Monotonic counter for this official-extension instance. */
  readonly generation: number
}

export interface RunTerminalPayload {
  readonly runId: string
  /** Stable opaque scope for the explicit or singleton supervisor. */
  readonly supervisorKey: string
  readonly runtimeId?: string
  readonly label: string
  readonly agent: string
  readonly status: string
  /** Number of active descendants when the run was removed, if available. */
  readonly nestedCount?: number
  readonly backend?: string
}

export interface RunTerminalPayloadInput {
  readonly runId: string
  /** Local supervisor socket, converted to an opaque supervisorKey. */
  readonly supervisorId?: string
  readonly runtimeId?: string
  readonly label: string
  readonly agent: string
  readonly status: string
  readonly nestedCount?: number
  readonly backend?: string
}

export interface AvenorEventMap {
  [CHANNEL_POLL_COMPLETED]: PollCompletedPayload
  [CHANNEL_RUN_TERMINAL]: RunTerminalPayload
  [CHANNEL_POLL_ERROR]: PollErrorPayload
}

export type AvenorEventChannel = keyof AvenorEventMap
export type AvenorEventBus = Pick<EventBus, 'emit' | 'on'>

/** Maximum sanitized error text exposed to companion extensions. */
const POLL_ERROR_TEXT_CHARS = 600

function sanitizeAndBoundText(value: unknown): string {
  const text = sanitizeText(String(value)).trim()
  if (!text) return 'unknown error'
  return text.length <= POLL_ERROR_TEXT_CHARS
    ? text
    : `${text.slice(0, POLL_ERROR_TEXT_CHARS - 1)}…`
}

function createSupervisorKey(supervisorId?: string): string {
  if (!supervisorId) return 'singleton'
  return `supervisor:${createHash('sha256').update(supervisorId).digest('hex').slice(0, 16)}`
}

/** Emit an Avenor event with a type-checked payload. */
export function emitAvenorEvent<K extends AvenorEventChannel>(
  bus: Pick<EventBus, 'emit'>,
  event: K,
  data: AvenorEventMap[K],
): void {
  bus.emit(event, data)
}

/** Subscribe to an Avenor event and return Pi's unsubscribe function. */
export function onAvenorEvent<K extends AvenorEventChannel>(
  bus: Pick<EventBus, 'on'>,
  event: K,
  handler: (data: AvenorEventMap[K]) => void,
): () => void {
  // Pi's bus accepts unknown payloads, so this cast preserves the generic handler type.
  return bus.on(event, handler as (data: unknown) => void)
}

/**
 * Copy and freeze the selected fields exposed to companion consumers.
 * Keep this allowlist explicit so future private RunStatusEntry fields are not
 * included in the event accidentally.
 */
export function createPollCompletedPayload(
  entries: readonly RunStatusEntry[],
  timestamp: number,
  generation: number,
): PollCompletedPayload {
  const publicEntries: readonly AvenorRunStatus[] = entries.map(entry => Object.freeze({
    runId: entry.runId,
    supervisorKey: createSupervisorKey(entry.supervisorId),
    ...(entry.runtimeId !== undefined && { runtimeId: entry.runtimeId }),
    label: entry.label,
    status: entry.status,
    ...(entry.phase !== undefined && { phase: entry.phase }),
    ...(entry.phaseLabel !== undefined && { phaseLabel: entry.phaseLabel }),
    agent: entry.agent,
    ...(entry.pendingPermission !== undefined && { pendingPermission: entry.pendingPermission }),
    ...(entry.backend !== undefined && { backend: entry.backend }),
    ...(entry.nestedCount !== undefined && { nestedCount: entry.nestedCount }),
  }))

  return Object.freeze({
    entries: Object.freeze(publicEntries),
    timestamp,
    generation,
  })
}

/** Copy and freeze a terminal payload before publishing it. */
export function createRunTerminalPayload(
  payload: RunTerminalPayloadInput,
): RunTerminalPayload {
  return Object.freeze({
    runId: payload.runId,
    supervisorKey: createSupervisorKey(payload.supervisorId),
    ...(payload.runtimeId !== undefined && { runtimeId: payload.runtimeId }),
    label: payload.label,
    agent: payload.agent,
    status: payload.status,
    ...(payload.nestedCount !== undefined && { nestedCount: payload.nestedCount }),
    ...(payload.backend !== undefined && { backend: payload.backend }),
  })
}

/** Copy, bound, sanitize, and freeze a polling error before publishing it. */
export function createPollErrorPayload(
  payload: PollErrorPayloadInput,
): PollErrorPayload {
  return Object.freeze({
    source: payload.source,
    ...(payload.runId !== undefined && { runId: payload.runId }),
    message: sanitizeAndBoundText(payload.message),
    error: sanitizeAndBoundText(payload.error),
    count: payload.count,
    timestamp: payload.timestamp,
  })
}
