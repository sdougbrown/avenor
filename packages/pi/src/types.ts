import type { StatusResult } from '@dougbots/avenor-core'

export interface TrackedRun {
  runId: string
  agent?: string
  label: string
  supervisorId?: string
  runtimeId?: string
  startTime: number
  lastStatus?: StatusResult
  /** true when delivery is owned by an in-flight spawn wait (wait=true) or
   * avenor_result call. A spawn-time blocking wait handles permissions via
   * onUpdate; a result wait suppresses the automatic completion notice while
   * it may still deliver the output itself. Clearing it hands delivery back
   * to the polling tick. */
  blocking?: boolean
  /** Active descendants seen by the last poll, carried into terminal telemetry. */
  nestedCount?: number
  /** true while an idle completion send is in flight; terminal processing for
   * this run is skipped until the send resolves. */
  idleSendInFlight?: boolean
  /** When the in-flight idle send started, used to re-arm a send that was
   * accepted by the void API but never confirmed through message_end. */
  idleSentAt?: number
  /** Idle sends attempted for the current notice. Bounds the re-arm loop so an
   * unconfirmable send stops instead of retrying forever. */
  idleSendAttempts?: number
  /** true if we've injected a permission notification via sendUserMessage */
  permissionNotified?: boolean
  /** true once avenor_result has successfully returned this run's final result to
   * the caller. Set only on a successful (non-interrupted) result; the run is
   * dropped from tracking and its pending notice is discarded immediately. */
  consumed?: boolean
  /** true once CHANNEL_RUN_TERMINAL has been emitted for this run. */
  terminalEventEmitted?: boolean
  /** One terminal completion waiting for a safe model-facing delivery. Run
   * identity lives on this tracked run. */
  pendingNotice?: CompletionNotice
}

/** One terminal completion waiting for a safe model-facing delivery. */
export interface CompletionNotice {
  status: TerminalStatus
  text: string
}

/**
 * Find a live status for a tracked run from the unqualified live-status list.
 *
 * `statusTool({})` only describes the current singleton supervisor, so it is
 * authoritative solely for runs that live in the singleton namespace. Runs on
 * another supervisor must be polled through their own supervisor; matching
 * them against this list could correlate a run with a colliding id (for
 * example a shared `rt_1`) owned by a different supervisor.
 *
 * @param singletonScope true when the tracked run belongs to the singleton
 *   supervisor (no explicit socket, or a socket equal to the current instance).
 */
export function findLiveStatusForTrackedRun(
  run: Pick<TrackedRun, 'runId' | 'runtimeId' | 'supervisorId' | 'lastStatus'>,
  liveStatuses: Iterable<StatusResult>,
  singletonScope: boolean,
): StatusResult | undefined {
  if (!singletonScope) return undefined
  const knownRunIds = new Set([run.runId, run.lastStatus?.run_id].filter((id): id is string => !!id))
  let runtimeMatch: StatusResult | undefined
  for (const status of liveStatuses) {
    if (knownRunIds.has(status.run_id)) return status
    // Runtime IDs are scoped to a supervisor. The unqualified live-status list and
    // tracked runs without an explicit supervisor both refer to the singleton.
    if (!run.supervisorId && run.runtimeId === status.runtime_id) {
      runtimeMatch = status
    }
  }
  return runtimeMatch
}

export interface RunStatusEntry {
  runId: string
  /** Supervisor socket provided at spawn time for namespace scoping. */
  supervisorId?: string
  /** Runtime ID scoped to the explicit or singleton supervisor. */
  runtimeId?: string
  label: string
  status: string
  phase?: string
  phaseLabel?: string
  agent: string
  pendingPermission?: boolean
  permissionDescription?: string
  /** OS PID of the spawned pi process (pi backend only), for discovering child supervisors. */
  pid?: number
  /** Backend type, used to determine whether to look for child supervisors. */
  backend?: string
  /** Count of active (non-terminal) descendants, if discovered. */
  nestedCount?: number
}

export type TerminalStatus = 'done' | 'failed' | 'timeout' | 'killed'

export const TERMINAL_STATUSES: ReadonlySet<string> = new Set<TerminalStatus>(['done', 'failed', 'timeout', 'killed'])

export const STATUS_EMOJI: Record<string, string> = {
  running: '🟢',
  idle: '🟡',
  done: '✅',
  failed: '❌',
  timeout: '⏱️',
  killed: '💀',
  permission: '🔒',
}

export function statusEmoji(status: string): string {
  return STATUS_EMOJI[status] ?? '⚪'
}

export function formatRunLine(entry: RunStatusEntry): string {
  const emoji = statusEmoji(entry.status)
  const perm = entry.pendingPermission ? ` — blocked: ${entry.permissionDescription ?? 'permission'}` : ''
  const phase = entry.phaseLabel ? ` (${entry.phaseLabel})` : ''
  return `${emoji} ${entry.label} (${entry.agent}) — ${entry.status}${phase}${perm}`
}
