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

const SAFE_FILE_COMPONENT = /^[A-Za-z0-9._-]+$/

/** Gate/activation ids are interpolated into decision file names; anything
 * outside this set could be a path traversal (e.g. `a/../..`). */
export function assertSafeId(value: string, what: string): void {
  if (!SAFE_FILE_COMPONENT.test(value)) {
    throw new Error(`unsafe ${what} for decision file naming: ${JSON.stringify(value)}`)
  }
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
  assertSafeId(activationId, 'activation_id')
  assertSafeId(gate.id, 'gate id')
  const decisionFile = path.join(decisionsDir, `decision-${activationId}-${gate.id}.json`)
  const deadline = now() + opts.gateTimeoutMs
  // Content of the last unreadable decision file; a move to rejected/ happens
  // only once the same content fails two consecutive polls (a single failure
  // may be a file the transport is still writing in place).
  let malformedRaw: string | null = null
  while (now() < deadline) {
    let raw: string | null = null
    if (fs.existsSync(decisionFile)) {
      try {
        raw = fs.readFileSync(decisionFile, 'utf8')
      } catch (exc) {
        // A read failure (EACCES/EBUSY/...) must not park a possibly-valid
        // decision under rejected/; propagate it to the caller's backoff
        // path. ENOENT is only the existsSync→readFileSync race: the next
        // tick retries.
        if ((exc as NodeJS.ErrnoException).code !== 'ENOENT') throw exc
      }
    }
    if (raw === null) {
      malformedRaw = null
    } else {
      let parsed: unknown = undefined
      let parseFailed = false
      try {
        parsed = JSON.parse(raw)
      } catch (exc) {
        if (raw === malformedRaw) {
          // Same content failed two consecutive polls: not a partial write.
          // Move it out of the way so the transport can write a fresh one,
          // and surface the parse error on the next ask instead of looping
          // on the same broken file.
          const rejected = moveAside(decisionsDir, activationId, gate.id, decisionFile)
          return {
            decision: null,
            decisionFile: null,
            reason: `invalid decision file (${(exc as Error).message}); rewritten to ${path.basename(rejected)}`,
          }
        }
        malformedRaw = raw
        parseFailed = true
      }
      if (!parseFailed) {
        // The file must hold an object; a JSON null/primitive/array would
        // otherwise fall through the caller's null-decision path and be
        // re-asked forever without ever being parked.
        if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
          const rejected = moveAside(decisionsDir, activationId, gate.id, decisionFile)
          return {
            decision: null,
            decisionFile: null,
            reason: `invalid decision file (expected a JSON object); rewritten to ${path.basename(rejected)}`,
          }
        }
        return { decision: parsed as Decision, decisionFile, reason: 'answered' }
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
  assertSafeId(activationId, 'activation_id')
  assertSafeId(gateId, 'gate id')
  const rejectedDir = path.join(decisionsDir, 'rejected')
  fs.mkdirSync(rejectedDir, { recursive: true })
  const rejected = path.join(rejectedDir, `${activationId}-${gateId}-${Math.floor(Date.now() / 1000)}.json`)
  fs.renameSync(from, rejected)
  return rejected
}
