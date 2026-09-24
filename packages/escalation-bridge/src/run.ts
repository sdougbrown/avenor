import * as fs from 'node:fs'
import * as path from 'node:path'

import { dial } from '@dougbots/avenor-core'

import { buildGateCommand } from './command.js'
import { latestOutputs, loadHumanGates, pendingHumanGates } from './gates.js'
import type { ControlClient, Decision, GateCommand, WorkflowDetail } from './types.js'
import { assertSafeId, moveAside, waitForDecision } from './wait.js'

export const BACKOFF_MS = 5000

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))
const defaultLog = (message: string, error?: Error) => {
  console.log(message)
  if (error) console.error(error.stack ?? error.message)
}

/** POST a JSON question payload to the transport webhook.
 *
 * Unlike the Python reference (urllib follows redirects), 3xx responses are
 * refused outright: a 307/308 redirect preserves method and body, so the
 * question payload would be forwarded to the target. A transport behind an
 * HTTP→HTTPS redirect must be addressed at its final URL here. */
export async function askWebhook(url: string, payload: unknown): Promise<void> {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
    redirect: 'error',
    signal: AbortSignal.timeout(10_000),
  })
  await resp.arrayBuffer()
  if (!resp.ok) throw new Error(`webhook responded ${resp.status}`)
}

export interface BridgeOptions {
  /** Avenor stable control socket path. */
  socketPath: string
  workflowId: string
  /** Transport endpoint the question payload is POSTed to. */
  webhookUrl: string
  /** Directory the transport writes decision files into. */
  decisionDir: string
  /** Template JSON the workflow was instantiated from. */
  templatePath: string
  /** workflow.wait timeout per tick (default 30s). */
  waitMs?: number
  /** Max wait per asked gate before re-asking (default 15m). */
  gateTimeoutMs?: number
  /** Decision-file poll interval (default 2s). */
  pollMs?: number
  connect?: (socketPath: string, callTimeoutMs: number) => Promise<ControlClient>
  ask?: (url: string, payload: unknown) => Promise<void>
  sleep?: (ms: number) => Promise<void>
  now?: () => number
  log?: (message: string, error?: Error) => void
}

/**
 * Watch a durable workflow for parked `awaiting_gate` activations, carry each
 * pending human gate to the transport, and record the human's answer through
 * the ordinary gate command path. Returns the process exit code: 0 once the
 * workflow reaches a terminal state.
 */
export async function runBridge(options: BridgeOptions): Promise<number> {
  const waitMs = options.waitMs ?? 30_000
  const gateTimeoutMs = options.gateTimeoutMs ?? 15 * 60_000
  // The Python reference floors the poll at 1.0 *seconds*; a lower floor
  // would tight-loop the file check + inspect RPC for the workflow's life.
  const pollMs = Math.max(options.pollMs ?? 2_000, 1_000)
  const connect =
    options.connect ?? ((socketPath, callTimeoutMs) => dial(socketPath, { callTimeoutMs }))
  const ask = options.ask ?? askWebhook
  const sleep = options.sleep ?? defaultSleep
  const log = options.log ?? defaultLog

  fs.mkdirSync(options.decisionDir, { recursive: true, mode: 0o700 })
  // The decision dir must stay local-user scoped: on a group- or
  // other-writable pre-existing dir, any local user could drop a
  // decision-*.json and forge an attributed decision (the actor is
  // self-asserted). Refuse to start on one.
  if ((fs.statSync(options.decisionDir).mode & 0o077) !== 0) {
    throw new Error(`decision dir ${options.decisionDir} must not be group- or other-writable`)
  }
  const templateGates = loadHumanGates(options.templatePath)
  // Gate ids are operator-supplied and interpolated into decision file
  // names; reject traversal-prone ids up front so runBridge fails clean
  // instead of backing off forever.
  for (const gates of templateGates.values()) {
    for (const gate of gates) assertSafeId(gate.id, 'gate id')
  }

  let ctl: ControlClient | null = null
  const decisionErrors = new Map<string, string>()
  log(`bridge: watching ${options.workflowId} on ${options.socketPath}`)
  for (;;) {
    try {
      if (ctl === null || ctl.isClosed()) {
        // The wait tick is the longest call; give every call room for it.
        ctl = await connect(options.socketPath, waitMs + 10_000)
      }
      const result = ((await ctl.call('workflow.wait', {
        workflow_id: options.workflowId,
        timeout_ms: waitMs,
      })) ?? {}) as { terminal?: boolean; instance?: { status?: string; terminal_outcome?: string } }
      if (result.terminal) {
        log(
          `bridge: workflow terminal (${result.instance?.status}, outcome ${result.instance?.terminal_outcome ?? ''})`,
        )
        return 0
      }

      const detail = (await ctl.call('workflow.inspect', {
        workflow_id: options.workflowId,
      })) as WorkflowDetail
      for (const [act, gate] of pendingHumanGates(detail, templateGates)) {
        const question: Record<string, unknown> = {
          workflow_id: options.workflowId,
          node_id: act.node_id,
          activation_id: act.activation_id,
          gate_id: gate.id,
          gate_name: gate.name ?? gate.id,
          subject_type: gate.subject_type ?? null,
          allowed_outcomes: gate.allowed_outcomes ?? [],
          selected_outcome: act.selected_outcome ?? null,
          outputs: latestOutputs(detail),
          decision_file: `decision-${act.activation_id}-${gate.id}.json`,
          // The human must be able to decide from the message alone.
          note: 'Include what is being authorized, its exact scope, and what denial means.',
        }
        const errorKey = `${act.activation_id}\u0000${gate.id}`
        const previousError = decisionErrors.get(errorKey)
        if (previousError !== undefined) {
          question.previous_decision_error = previousError
          decisionErrors.delete(errorKey)
        }
        log(`bridge: asking ${gate.id} on ${act.node_id} (${act.activation_id})`)
        await ask(options.webhookUrl, question)

        const wait = await waitForDecision(ctl, options.decisionDir, options.workflowId, act.activation_id, gate, {
          gateTimeoutMs,
          pollMs,
          now: options.now,
          sleep: options.sleep,
          log,
        })
        if (wait.decision === null) {
          if (wait.reason === 'terminal') {
            // Workflow ended while waiting; stop asking anyone — the next
            // workflow.wait observes the terminal state.
            break
          } else if (wait.reason.startsWith('invalid decision file')) {
            // Malformed file: surface the parse error on the next ask instead
            // of re-asking silently.
            decisionErrors.set(errorKey, wait.reason)
          }
          continue // timeout or unparked; the next tick re-asks
        }
        const decision = wait.decision
        let command: GateCommand
        try {
          command = buildGateCommand(act.node_id, act.activation_id, gate, decision)
        } catch (exc) {
          // The human's answer is invalid (bad operation, missing actor/reason,
          // subject type mismatch). Park the file under rejected/ so the same
          // answer is not re-read forever, and carry the reason on the next ask.
          moveAside(options.decisionDir, act.activation_id, gate.id, wait.decisionFile!)
          decisionErrors.set(errorKey, (exc as Error).message)
          log(`bridge: rejected decision on ${gate.id}: ${(exc as Error).message}`)
          continue
        }
        await ctl.call('workflow.command', { workflow_id: options.workflowId, command })
        // Archive only once the kernel accepted the decision; a failed record
        // leaves the file in place so the next tick retries it (the kernel's
        // gate idempotency key makes that a safe no-op).
        const recorded = path.join(
          options.decisionDir,
          'recorded',
          `${act.activation_id}-${gate.id}-${command.response_hash}.json`,
        )
        assertSafeId(act.activation_id, 'activation_id')
        fs.mkdirSync(path.dirname(recorded), { recursive: true })
        fs.renameSync(wait.decisionFile!, recorded)
        log(
          `bridge: recorded ${decision.decision} by ${decision.actor} on ${gate.id} (${act.activation_id})`,
        )
      }
    } catch (exc) {
      // Transient RPC/webhook failures must not kill the watcher. Release the
      // broken client's socket fd before reconnecting on the next tick.
      const err = exc as Error
      log(`bridge: ${err.name}: ${err.message}; backing off ${BACKOFF_MS / 1000}s`, err)
      ctl?.close()
      ctl = null
      await sleep(BACKOFF_MS)
    }
  }
}

export type { Decision, GateCommand, WorkflowDetail }
