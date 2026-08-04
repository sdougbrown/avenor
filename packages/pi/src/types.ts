import type { StatusResult } from '@dougbots/avenor-core'

export interface TrackedRun {
  runId: string
  agent?: string
  label: string
  supervisorId?: string
  runtimeId?: string
  startTime: number
  lastStatus?: StatusResult
  /** true if spawned with wait=true (blocking mode handles permissions via onUpdate) */
  blocking?: boolean
  /** true if we've injected a permission notification via sendUserMessage */
  permissionNotified?: boolean
  /** true if the run reached terminal status and a completion message is pending.
   * The message is deferred by one tick cycle so avenor_result can consume
   * the result first without a duplicate steering message. */
  completionPending?: boolean
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

/**
 * CompletionAction represents the decision for a tracked run that has
 * reached terminal status during a polling tick.
 *
 * - `skip` — the run is blocking (avenor_result is waiting); do nothing.
 * - `defer` — first tick seeing terminal status; set completionPending and
 *   wait one cycle so avenor_result can consume the result first.
 * - `send` — second tick seeing terminal status; deliver the completion
 *   steering message and remove the run from tracking.
 */
export type CompletionAction = 'skip' | 'defer' | 'send'

/**
 * decideCompletion determines whether to skip, defer, or send the completion
 * steering message for a tracked run that has reached terminal status.
 *
 * The deferral prevents duplication when avenor_result consumes the result
 * in the same turn: if the agent calls avenor_result between the defer and
 * the next tick, the run is deleted from tracking and the send never fires.
 */
export function decideCompletion(run: Pick<TrackedRun, 'blocking' | 'completionPending'>): CompletionAction {
  if (run.blocking) return 'skip'
  if (!run.completionPending) return 'defer'
  return 'send'
}
