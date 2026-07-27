import type { StatusResult } from '@dougbots/avenor-core'

export interface TrackedRun {
  runId: string
  agent: string
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

export function findLiveStatusForTrackedRun(
  run: Pick<TrackedRun, 'runId' | 'runtimeId' | 'supervisorId' | 'lastStatus'>,
  liveStatuses: Iterable<StatusResult>,
): StatusResult | undefined {
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
  label: string
  status: string
  phase?: string
  phaseLabel?: string
  agent: string
  pendingPermission?: boolean
  permissionDescription?: string
}

export type TerminalStatus = 'done' | 'failed' | 'timeout' | 'killed'

export const TERMINAL_STATUSES = new Set<TerminalStatus>(['done', 'failed', 'timeout', 'killed'])

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
