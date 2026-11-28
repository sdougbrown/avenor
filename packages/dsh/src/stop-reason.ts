import type { SubagentResult, SubagentStopReason } from './types.js'

/** What an Avenor run reports once it reaches a terminal state. */
export interface AvenorTerminal {
  readonly runId: string
  /** Avenor lifecycle status: `done`, `killed`, `failed`, `timeout`, `unknown`, … */
  readonly status: string
  /** Avenor's own reason, when the control server recorded one. */
  readonly stopReason?: string
  readonly ready: boolean
  readonly output?: string
  readonly timedOut?: boolean
}

const COMPLETED = 'completed'
const ABORTED = 'aborted'
const ERROR = 'error'

/**
 * Avenor terminal statuses that mean "the work finished, in a shape a
 * subagent result can represent". Everything else is a diagnostic, because the
 * DSH vocabulary has no slot for it.
 */
const STATUS_TO_STOP_REASON: Record<string, SubagentStopReason> = {
  done: COMPLETED,
  killed: ABORTED,
}

export function stopReasonFor(terminal: AvenorTerminal): SubagentStopReason {
  if (!terminal.ready) return ERROR
  return STATUS_TO_STOP_REASON[terminal.status] ?? ERROR
}

function outputOf(terminal: AvenorTerminal): readonly { type: 'text'; text: string }[] {
  return [{ type: 'text', text: terminal.output ?? '' }]
}

/** The non-`completed` detail a consumer shows beside the output. */
export function diagnosticFor(terminal: AvenorTerminal): string | undefined {
  if (terminal.ready && terminal.status === 'done') return undefined
  if (!terminal.ready) {
    return terminal.timedOut
      ? `avenor run ${terminal.runId} did not settle before the wait expired`
      : `avenor run ${terminal.runId} is not settled (status ${terminal.status})`
  }
  const reason = terminal.stopReason ? ` (${terminal.stopReason})` : ''
  return `avenor run ${terminal.runId} ended ${terminal.status}${reason}`
}

/**
 * Project one Avenor terminal state onto a DSH subagent result. This never
 * rejects: a child-level failure is a resolved result with a non-`completed`
 * stop reason, which is what the seam requires.
 */
export function toSubagentResult(terminal: AvenorTerminal): SubagentResult {
  const stopReason = stopReasonFor(terminal)
  const diagnostic = diagnosticFor(terminal)
  return {
    output: outputOf(terminal),
    stopReason,
    ...(diagnostic === undefined ? {} : { diagnostic }),
  }
}
