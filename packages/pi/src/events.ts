/**
 * Public telemetry types and adapters for companion Pi extensions.
 *
 * The transport is Pi's shared `pi.events` bus. This module owns no event
 * handlers or singleton state, so consumers can use the surface even when
 * package dependencies are resolved from different module instances.
 *
 * Payloads are limited status views. They do not contain transcripts, final
 * output, directories, raw status objects, or tool arguments.
 */

import type { EventBus } from '@earendil-works/pi-coding-agent'
import type { RunStatusEntry } from './types.js'

/** Event channel for a completed official-extension polling cycle. */
export const CHANNEL_POLL_COMPLETED = 'avenor:poll:completed' as const

/** Event channel for a terminal run result observed by the extension. */
export const CHANNEL_RUN_TERMINAL = 'avenor:run:terminal' as const

export type AvenorRunStatus = Readonly<RunStatusEntry>

export interface PollCompletedPayload {
  /** Direct run statuses known to the official extension. */
  readonly entries: readonly AvenorRunStatus[]
  /** Wall-clock timestamp in epoch milliseconds. */
  readonly timestamp: number
  /** Monotonic counter for this official-extension instance. */
  readonly generation: number
}

export interface RunTerminalPayload {
  readonly runId: string
  /** Supervisor socket that scopes the run/runtime identity, when explicit. */
  readonly supervisorId?: string
  /** Runtime identity, scoped by supervisorId (or the singleton supervisor). */
  readonly runtimeId?: string
  readonly label: string
  readonly agent: string
  readonly status: string
  /** Active descendant count at removal time, when it was available. */
  readonly nestedCount?: number
  readonly backend?: string
}

export interface AvenorEventMap {
  [CHANNEL_POLL_COMPLETED]: PollCompletedPayload
  [CHANNEL_RUN_TERMINAL]: RunTerminalPayload
}

export type AvenorEventChannel = keyof AvenorEventMap
export type AvenorEventBus = Pick<EventBus, 'emit' | 'on'>

/** Emit a typed Avenor event through Pi's shared event bus. */
export function emitAvenorEvent<K extends AvenorEventChannel>(
  bus: Pick<EventBus, 'emit'>,
  event: K,
  data: AvenorEventMap[K],
): void {
  bus.emit(event, data)
}

/** Subscribe to a typed Avenor event and receive Pi's unsubscribe function. */
export function onAvenorEvent<K extends AvenorEventChannel>(
  bus: Pick<EventBus, 'on'>,
  event: K,
  handler: (data: AvenorEventMap[K]) => void,
): () => void {
  return bus.on(event, handler as (data: unknown) => void)
}

/**
 * Copy and freeze the bounded fields intended for companion consumers.
 * `RunStatusEntry` currently contains only display-safe status metadata, but
 * selecting fields explicitly keeps future private fields out of the event.
 */
export function createPollCompletedPayload(
  entries: readonly RunStatusEntry[],
  timestamp: number,
  generation: number,
): PollCompletedPayload {
  const publicEntries: readonly AvenorRunStatus[] = entries.map(entry => Object.freeze({
    runId: entry.runId,
    ...(entry.supervisorId !== undefined && { supervisorId: entry.supervisorId }),
    ...(entry.runtimeId !== undefined && { runtimeId: entry.runtimeId }),
    label: entry.label,
    status: entry.status,
    ...(entry.phase !== undefined && { phase: entry.phase }),
    ...(entry.phaseLabel !== undefined && { phaseLabel: entry.phaseLabel }),
    agent: entry.agent,
    ...(entry.pendingPermission !== undefined && { pendingPermission: entry.pendingPermission }),
    ...(entry.permissionDescription !== undefined && { permissionDescription: entry.permissionDescription }),
    ...(entry.pid !== undefined && { pid: entry.pid }),
    ...(entry.backend !== undefined && { backend: entry.backend }),
    ...(entry.nestedCount !== undefined && { nestedCount: entry.nestedCount }),
  }))

  return Object.freeze({
    entries: Object.freeze(publicEntries),
    timestamp,
    generation,
  })
}

/** Copy and freeze a terminal transition before publishing it. */
export function createRunTerminalPayload(
  payload: RunTerminalPayload,
): RunTerminalPayload {
  return Object.freeze({
    runId: payload.runId,
    ...(payload.supervisorId !== undefined && { supervisorId: payload.supervisorId }),
    ...(payload.runtimeId !== undefined && { runtimeId: payload.runtimeId }),
    label: payload.label,
    agent: payload.agent,
    status: payload.status,
    ...(payload.nestedCount !== undefined && { nestedCount: payload.nestedCount }),
    ...(payload.backend !== undefined && { backend: payload.backend }),
  })
}
