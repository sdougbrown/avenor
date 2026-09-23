import * as fs from 'node:fs'
import * as path from 'node:path'

import type { ControlClient, Decision, GateDefinition, WorkflowDetail } from './types.js'

export interface WaitForDecisionOptions {
  gateTimeoutMs: number
  pollMs: number
  /** Monotonic-ish clock; injectable for tests. */
  now?: () => number
  sleep?: (ms: number) => Promise<void>
  log?: (message: string) => void
}

export interface WaitResult {
  decision: Decision | null
  decisionFile: string | null
  reason: string
}

/**
 * Poll for the transport's decision file, re-checking workflow state.
 *
 * reason is 'answered', 'timeout' (gate-timeout expired while still parked;
 * the caller re-asks), 'unparked' (the activation resolved another way), or
 * 'terminal' (the workflow reached a terminal state while waiting). The file
 * is left in place for the caller to archive only after the decision is
 * recorded kernel-side: a failed record must not consume the human's answer.
 */
export async function waitForDecision(
  ctl: ControlClient,
  decisionsDir: string,
  workflowId: string,
  activationId: string,
  gate: GateDefinition,
  opts: WaitForDecisionOptions,
): Promise<WaitResult> {
  const now = opts.now ?? (() => performance.now())
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)))
  const log = opts.log ?? (() => {})
  const decisionFile = path.join(decisionsDir, `decision-${activationId}-${gate.id}.json`)
  const deadline = now() + opts.gateTimeoutMs
  while (now() < deadline) {
    if (fs.existsSync(decisionFile)) {
      try {
        const decision = JSON.parse(fs.readFileSync(decisionFile, 'utf8')) as Decision
        return { decision, decisionFile, reason: 'answered' }
      } catch (exc) {
        // Malformed file: move it out of the way so the transport can write a
        // fresh one, and surface the parse error on the next ask instead of
        // looping on the same broken file.
        const rejected = moveAside(decisionsDir, activationId, gate.id, decisionFile)
        return {
          decision: null,
          decisionFile: null,
          reason: `invalid decision file (${(exc as Error).message}); rewritten to ${path.basename(rejected)}`,
        }
      }
    }
    await sleep(opts.pollMs)
    // Re-check state on every tick: another path may resolve the gate or the
    // workflow while this decision is pending.
    const detail = (await ctl.call('workflow.inspect', {
      workflow_id: workflowId,
    })) as WorkflowDetail
    const act = (detail.activations ?? []).find((a) => a.activation_id === activationId)
    if (!act || act.status !== 'awaiting_gate') {
      log(`bridge: ${activationId} no longer parked; dropping wait on ${gate.id}`)
      return { decision: null, decisionFile: null, reason: 'unparked' }
    }
    const status = detail.instance?.status
    if (status === 'completed' || status === 'failed' || status === 'canceled') {
      return { decision: null, decisionFile: null, reason: 'terminal' }
    }
  }
  return { decision: null, decisionFile: null, reason: 'timeout' }
}

/** Park a decision file under rejected/ so it is never re-read. */
export function moveAside(
  decisionsDir: string,
  activationId: string,
  gateId: string,
  from: string,
): string {
  const rejectedDir = path.join(decisionsDir, 'rejected')
  fs.mkdirSync(rejectedDir, { recursive: true })
  const rejected = path.join(rejectedDir, `${activationId}-${gateId}-${Math.floor(Date.now() / 1000)}.json`)
  fs.renameSync(from, rejected)
  return rejected
}
