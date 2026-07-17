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
