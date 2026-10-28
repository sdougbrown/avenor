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
