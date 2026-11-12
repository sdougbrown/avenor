/**
 * The run lifecycle vocabulary. Mirrors Go's `internal/runstate`; both are
 * pinned to `schemas/runstate.conformance.json`.
 */

/** A consumer-facing runtime status. */
export type RuntimeStatus =
  | 'idle'
  | 'running'
  | 'ended'
  | 'done'
  | 'failed'
  | 'timeout'
  | 'killed'
  | 'waiting'
  | 'blocked'
  | 'unknown'

/** A runtime phase as the supervisor reports it; the empty string is no phase. */
export type Phase = string

export type TerminalStatus = 'done' | 'failed' | 'timeout' | 'killed'

export interface Translation {
  status: RuntimeStatus
  phase: Phase
  turnComplete: boolean
  /** False only for the `unknown` fallback of an unrecognized raw status. */
  recognized: boolean
}

const TERMINAL_STATUSES: ReadonlySet<string> = new Set<TerminalStatus>(['done', 'failed', 'timeout', 'killed'])

/** Whether a consumer-facing status is terminal. */
export function isTerminalStatus(status: string | undefined): status is TerminalStatus {
  return status !== undefined && TERMINAL_STATUSES.has(status)
}

/** Whether a phase value is one of the terminal consumer-facing statuses. */
export function isTerminalPhase(phase: Phase): phase is TerminalStatus {
  return isTerminalStatus(phase)
}

/**
 * Normalize a raw supervisor status and phase. Case-sensitive, like Go. An idle
 * runtime is complete only when its phase is terminal. A running runtime stays
 * running even when terminal phase metadata arrives before active-attempt
 * cleanup finishes. An unrecognized status maps to `unknown`.
 */
export function translate(status: string, phase: Phase): Translation {
  let translatedStatus: RuntimeStatus
  let translatedPhase = phase
  let recognized = true

  switch (status) {
    case 'running':
      translatedStatus = 'running'
      if (isTerminalPhase(phase)) translatedPhase = ''
      break
    case 'idle':
      translatedStatus = isTerminalPhase(phase) ? phase : 'running'
      break
    case 'ended':
      translatedStatus = 'done'
      break
    case 'done':
    case 'failed':
    case 'timeout':
    case 'killed':
    case 'waiting':
      translatedStatus = status
      break
    case 'blocked':
      translatedStatus = 'failed'
      break
    default:
      translatedStatus = 'unknown'
      recognized = false
  }

  return {
    status: translatedStatus,
    phase: translatedPhase,
    turnComplete: isTerminalStatus(translatedStatus),
    recognized,
  }
}

/**
 * The consumer-facing status a completion sentinel's status word represents.
 * An unlisted word passes through lowercased.
 */
export function sentinelStatus(word: string): string {
  switch (word) {
    case 'DONE':
      return 'done'
    case 'FAILED':
    case 'BLOCKED':
      return 'failed'
    case 'TIMEOUT':
      return 'timeout'
    case 'KILLED':
      return 'killed'
    default:
      return word.toLowerCase()
  }
}

/**
 * Whether a sentinel status word names an outcome a reader may answer from after
 * the run's runtime is gone.
 */
export function isTerminalSentinel(word: string): boolean {
  return isTerminalStatus(sentinelStatus(word.toUpperCase()))
}

/**
 * Whether a sentinel status word names a status a resume may continue. DONE,
 * FAILED and BLOCKED settle the provider session itself; TIMEOUT and KILLED are
 * supervisor-imposed terminations where an automatic continuation would
 * surprise.
 */
export function isResumableSentinel(word: string): boolean {
  const status = sentinelStatus(word.toUpperCase())
  return status === 'done' || status === 'failed'
}
