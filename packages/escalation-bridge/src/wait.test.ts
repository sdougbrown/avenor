import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { beforeEach, describe, expect, test } from 'bun:test'

import { waitForDecision } from './wait.js'
import type { ControlClient, GateDefinition, WorkflowDetail } from './types.js'

function parkedActivation(actId = 'act_1', nodeId = 'merge-auth') {
  return {
    activation_id: actId,
    node_id: nodeId,
    status: 'awaiting_gate',
    selected_outcome: 'authorized',
  }
}

function mergeAuthGate(): GateDefinition {
  return {
    id: 'merge-authorization',
    name: 'Merge authorization',
    type: 'human',
    required: true,
    subject_type: 'pull_request',
    allowed_outcomes: ['authorized'],
  }
}

/** Minimal stand-in for the control client returning canned inspect results. */
class FakeControl implements ControlClient {
  results: unknown[]
  calls: string[] = []
  onCall?: (method: string) => void

  constructor(results: unknown[]) {
    this.results = results
  }

  async call(method: string): Promise<unknown> {
    this.calls.push(method)
    this.onCall?.(method)
    return this.results.shift()
  }

  isClosed(): boolean {
    return false
  }

  close(): void {}
}

describe('waitForDecision', () => {
  let decisions: string
  const gate = mergeAuthGate()
  const opts = { gateTimeoutMs: 5000, pollMs: 1 }

  beforeEach(() => {
    decisions = fs.mkdtempSync(path.join(os.tmpdir(), 'escalation-bridge-'))
  })

  const writeDecision = (payload: string | object): string => {
    const file = path.join(decisions, 'decision-act_1-merge-authorization.json')
    fs.writeFileSync(file, typeof payload === 'string' ? payload : JSON.stringify(payload))
    return file
  }

  test('answered', async () => {
    writeDecision({ decision: 'satisfy', actor: 'a', reason: 'r' })
    const ctl = new FakeControl([])
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.reason).toBe('answered')
    expect(result.decision!.decision).toBe('satisfy')
    expect(fs.existsSync(result.decisionFile!)).toBe(true)
  })

  test('answered after file arrives on a later poll', async () => {
    // The real-world sequence: the transport writes the decision file only
    // after the bridge asks, so the file must be re-checked each poll tick.
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    const ctl = new FakeControl([detail])
    ctl.onCall = () => {
      writeDecision({ decision: 'satisfy', actor: 'a', reason: 'r' })
    }
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.reason).toBe('answered')
    expect(result.decision!.decision).toBe('satisfy')
  })

  test('terminal reason', async () => {
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'completed' },
    }
    const ctl = new FakeControl([detail])
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.reason).toBe('terminal')
  })

  test('unparked reason', async () => {
    const detail: WorkflowDetail = {
      activations: [{ ...parkedActivation(), status: 'satisfied' }],
      gates: null,
      instance: { status: 'active' },
    }
    const ctl = new FakeControl([detail])
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.reason).toBe('unparked')
  })

  test('timeout reason', async () => {
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    const ctl = new FakeControl([detail])
    // Script the clock (ms) but fall back to a value past the deadline for
    // any extra call, so a loop change fails the assertion instead of
    // throwing inside the fake clock.
    const scripted = [0, 0, 6000]
    const now = () => (scripted.length > 1 ? scripted.shift()! : scripted[0])
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, {
      ...opts,
      now,
      sleep: async () => {},
    })
    expect(result.reason).toBe('timeout')
  })

  test('malformed file moves aside and reports', async () => {
    writeDecision('{not json')
    const ctl = new FakeControl([])
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.decision).toBeNull()
    expect(result.reason).toContain('invalid decision file')
    const rejected = fs.readdirSync(path.join(decisions, 'rejected'))
    expect(rejected).toHaveLength(1)
  })
})
