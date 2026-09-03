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
  /** true once avenor_result has successfully returned this run's final result to
   * the caller. Set only on a successful (non-interrupted) result; suppresses the
   * automatic completion notification without deleting the run. */
  consumed?: boolean
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
 * - `skip` — the result is spoken for: avenor_result is actively waiting
 *   (`blocking`) or has already returned the result successfully
 *   (`consumed`); do not notify.
 * - `send` — the caller has not obtained the result; deliver the completion
 *   steering message and remove the run from tracking.
 */
export type CompletionAction = 'skip' | 'send'

/**
 * decideCompletion determines whether to skip or send the completion
 * steering message for a tracked run that has reached terminal status.
 *
 * The message is delivered on the same terminal tick — never deferred to a
 * later cycle — so a run that is the last live one still gets its completion.
 * `blocking` suppresses it while avenor_result is awaiting (it will deliver
 * the result itself and set `consumed`), and `consumed` suppresses it once
 * avenor_result has already returned the result successfully.
 */
export function decideCompletion(run: Pick<TrackedRun, 'blocking' | 'consumed'>): CompletionAction {
  if (run.blocking) return 'skip'
  if (run.consumed) return 'skip'
  return 'send'
}
