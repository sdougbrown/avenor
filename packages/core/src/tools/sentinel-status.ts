/**
 * Sentinel statuses that settle the provider session itself, so the recorded
 * session id is a loadable transcript. TIMEOUT and KILLED are supervisor-imposed
 * terminations (time budget, cancellation) where an automatic continuation would
 * surprise. Matches the Go `readSentinelSession`.
 */
export const RESUMABLE_SENTINEL_STATUSES = new Set(['DONE', 'FAILED', 'BLOCKED'])

/**
 * The terminal sentinel statuses the provider's state fallback answers from,
 * matching the Go `terminalStatusFromRunInfo` set: a run that settled before
 * a supervisor restart keeps its outcome discoverable by run id.
 */
export const TERMINAL_SENTINEL_STATUSES = new Set(['DONE', 'FAILED', 'BLOCKED', 'TIMEOUT', 'KILLED'])

/**
 * The result word a sentinel status settles on. Both the status surface and the
 * message provider's state fallback answer from this one mapping, so the same
 * sentinel file cannot classify differently between them. BLOCKED reads as
 * failed: it names why the run stopped, not a result of its own.
 */
export function sentinelStatusToResult(sentinelStatus: string): string {
  switch (sentinelStatus) {
    case 'DONE':
      return 'done'
    case 'FAILED':
      return 'failed'
    case 'TIMEOUT':
      return 'timeout'
    case 'KILLED':
      return 'killed'
    case 'BLOCKED':
      return 'failed'
    default:
      return 'running'
  }
}

/**
 * Whether a terminal sentinel leaves a session worth resuming. Mirrors the only
 * hit Go's `runFactsFromStatus` reaches for a registry entry: a non-empty
 * session id. The sentinel word never decides this.
 */
export function resumableFromSession(sessionId: unknown): boolean {
  return typeof sessionId === 'string' && sessionId !== ''
}
