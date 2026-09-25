import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { beforeEach, describe, expect, test } from 'bun:test'

import { moveAside, subjectUnchanged, waitForDecision } from './wait.js'
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
    // Pin the iteration count: an extra loop tick would draw an inspect
    // result the fake cannot supply.
    expect(ctl.calls).toEqual(['workflow.inspect'])
  })

  test('malformed file moves aside once stable across two polls', async () => {
    writeDecision('{not json')
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    // Two parked ticks: the first poll sees the malformed content, the
    // second poll sees the same content and moves it aside.
    const ctl = new FakeControl([detail, detail])
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.decision).toBeNull()
    expect(result.reason).toContain('invalid decision file')
    const rejected = fs.readdirSync(path.join(decisions, 'rejected'))
    expect(rejected).toHaveLength(1)
  })

  test('two parks of the same gate in one second produce distinct files', () => {
    // Parked names use a millisecond timestamp plus a per-process counter;
    // whole-second names would let a second park overwrite the first.
    const file = writeDecision('{not json')
    const first = moveAside(decisions, 'act_1', 'merge-authorization', file)
    fs.writeFileSync(file, '{still not json')
    const second = moveAside(decisions, 'act_1', 'merge-authorization', file)
    expect(first).not.toBe(second)
    const rejected = fs.readdirSync(path.join(decisions, 'rejected')).sort()
    expect(rejected).toHaveLength(2)
    expect(fs.readFileSync(path.join(decisions, 'rejected', rejected[0]), 'utf8')).toBe('{not json')
    expect(fs.readFileSync(path.join(decisions, 'rejected', rejected[1]), 'utf8')).toBe(
      '{still not json',
    )
  })

  test('a file still being written is not moved aside until stable', async () => {
    writeDecision('{not json yet')
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    const ctl = new FakeControl([detail])
    ctl.onCall = () => {
      // The transport finishes the write between polls; the completed file
      // must be answered, not parked under rejected/.
      writeDecision({ decision: 'satisfy', actor: 'a', reason: 'r' })
    }
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.reason).toBe('answered')
    expect(fs.existsSync(path.join(decisions, 'rejected'))).toBe(false)
  })

  test('non-object JSON files are invalid, not answers', async () => {
    const file = path.join(decisions, 'decision-act_1-merge-authorization.json')
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    for (const raw of ['null', '[1, 2]', '"answer"', '42', 'true']) {
      fs.writeFileSync(file, raw)
      const ctl = new FakeControl([detail])
      const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
      // This path moves the file aside on the first poll.
      expect(result.decision, raw).toBeNull()
      expect(result.reason, raw).toContain('invalid decision file')
      expect(fs.existsSync(file), raw).toBe(false)
    }
  })

  test('a read failure propagates instead of parking the file', async () => {
    // A directory where the decision file should be: existsSync is true but
    // readFileSync throws a non-ENOENT fs error (EISDIR/EACCES).
    fs.mkdirSync(path.join(decisions, 'decision-act_1-merge-authorization.json'))
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    const ctl = new FakeControl([detail])
    await expect(waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)).rejects.toThrow()
    expect(fs.existsSync(path.join(decisions, 'rejected'))).toBe(false)
  })

  test('an ENOENT between polls resets the malformed hold', async () => {
    const file = path.join(decisions, 'decision-act_1-merge-authorization.json')
    const payload = '{not json yet'
    fs.writeFileSync(file, payload)
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: null,
      instance: { status: 'active' },
    }
    // Four parked ticks: read 1 malformed (hold), read 2 ENOENT (reset),
    // read 3 same content (fresh first failure, hold), read 4 same content
    // (second consecutive failure) -> moved aside. Without the reset, read 3
    // would have moved the file aside. The file's disappearance between polls
    // is driven by the inspect callback (call 1 removes it, call 2 rewrites
    // the same content); fs.readFileSync cannot be monkey-patched in Bun.
    const ctl = new FakeControl([detail, detail, detail, detail])
    let inspects = 0
    ctl.onCall = () => {
      inspects += 1
      if (inspects === 1) fs.unlinkSync(file)
      if (inspects === 2) fs.writeFileSync(file, payload)
    }
    const result = await waitForDecision(ctl, decisions, 'wf_1', 'act_1', gate, opts)
    expect(result.decision).toBeNull()
    expect(result.reason).toContain('invalid decision file')
    const rejected = fs.readdirSync(path.join(decisions, 'rejected'))
    expect(rejected).toHaveLength(1)
    expect(fs.readFileSync(path.join(decisions, 'rejected', rejected[0]), 'utf8')).toBe(payload)
  })

  test('rejects unsafe ids before touching paths', async () => {
    const evil = { ...mergeAuthGate(), id: '../../evil' }
    const ctl = new FakeControl([])
    await expect(waitForDecision(ctl, decisions, 'wf_1', 'act_1', evil, opts)).rejects.toThrow(
      /unsafe gate id/,
    )
  })
})

describe('subjectUnchanged', () => {
  // The pre-submit re-inspect of a bound-gate decision.
  const PINNED = {
    type: 'pull_request',
    repository: 'org/repo',
    pull_request: 123,
    revision: 'abc123',
  }

  function inspectWith(act: Record<string, unknown>): WorkflowDetail {
    return { activations: [act as never], gates: null, instance: { status: 'active' } }
  }

  function parkedWithPin(subject: Record<string, unknown>): Record<string, unknown> {
    return {
      ...parkedActivation(),
      resolved_gates: { 'merge-authorization': { subject } },
    }
  }

  test('ok when still parked with the same pinned subject', async () => {
    const ctl = new FakeControl([inspectWith(parkedWithPin({ ...PINNED }))])
    const result = await subjectUnchanged(ctl, 'wf_1', 'act_1', 'merge-authorization', PINNED)
    expect(result.ok).toBe(true)
    expect(result.reason).toBe('ok')
  })

  test('unparked when the activation resolved', async () => {
    const ctl = new FakeControl([
      inspectWith({ ...parkedWithPin({ ...PINNED }), status: 'satisfied' }),
    ])
    const result = await subjectUnchanged(ctl, 'wf_1', 'act_1', 'merge-authorization', PINNED)
    expect(result.ok).toBe(false)
    expect(result.reason).toBe('unparked')
  })

  test('subject changed when the pinned subject differs', async () => {
    const ctl = new FakeControl([
      inspectWith(parkedWithPin({ ...PINNED, revision: 'deadbeef' })),
    ])
    const result = await subjectUnchanged(ctl, 'wf_1', 'act_1', 'merge-authorization', PINNED)
    expect(result.ok).toBe(false)
    expect(result.reason).toBe('subject changed')
  })

  test('unparked when the activation is gone', async () => {
    const ctl = new FakeControl([{ activations: null, gates: null }])
    const result = await subjectUnchanged(ctl, 'wf_1', 'act_1', 'merge-authorization', PINNED)
    expect(result.ok).toBe(false)
    expect(result.reason).toBe('unparked')
  })
})
