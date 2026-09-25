import { describe, expect, test } from 'bun:test'

import { buildGateCommand, decisionResponseHash, stableStringify } from './command.js'
import type { Decision, GateDefinition } from './types.js'

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

const decision: Decision = {
  decision: 'satisfy',
  actor: 'austin',
  reason: 'Diff reviewed; approved',
  evidence: 'https://slack.com/archives/C123/p1650',
  subject: {
    type: 'pull_request',
    repository: 'org/repo',
    pull_request: 123,
    revision: 'abc123',
  },
}

describe('stableStringify', () => {
  // Expected strings and hashes generated with Python's
  // json.dumps(value, sort_keys=True) so the TS and Python bridges derive
  // identical response hashes for the same decision.
  test('matches python json.dumps(sort_keys=True)', () => {
    const tricky = {
      s: 'quote " back\\slash \n newline \t tab é ünïcode 😀 slash/kept',
      n: 123,
      f: 1.5,
      b: true,
      z: null,
      list: [3, 1, { y: 2, x: 1 }],
      nested: { b: { a: [1, 'two'] }, a: 'v' },
    }
    expect(stableStringify(tricky)).toBe(
      '{"b": true, "f": 1.5, "list": [3, 1, {"x": 1, "y": 2}], "n": 123, ' +
        '"nested": {"a": "v", "b": {"a": [1, "two"]}}, ' +
        '"s": "quote \\" back\\\\slash \\n newline \\t tab \\u00e9 \\u00fcn\\u00efcode \\ud83d\\ude00 slash/kept", ' +
        '"z": null}',
    )
  })

  test('sorts object keys by code point, not UTF-16 code unit', () => {
    // UTF-16 order would place 😀 (D83D DE00) before \uffff (FFFF); Python's
    // sort_keys orders by code point: a < \uffff < 😀.
    const astral = { '\ud83d\ude00': 'astral-smiley', '\uffff': 'bmp-max', a: 'ascii' }
    expect(stableStringify(astral)).toBe(
      '{"a": "ascii", "\\uffff": "bmp-max", "\\ud83d\\ude00": "astral-smiley"}',
    )
    expect(
      decisionResponseHash(astral as unknown as Decision),
    ).toBe('d7a6dc6f1aa79212982977d5114d5c8e')
  })

  test('escapes \r, \b, and \f like python', () => {
    const ctrl = { s: 'cr\r bs\b ff\f' }
    expect(stableStringify(ctrl)).toBe('{"s": "cr\\r bs\\b ff\\f"}')
    expect(decisionResponseHash(ctrl as unknown as Decision)).toBe(
      '25bc9793241f60dbc71025ae11ae5e01',
    )
  })
})

describe('decisionResponseHash', () => {
  test('matches python-generated hashes', () => {
    expect(decisionResponseHash(decision)).toBe('da537894c0390034cbe7ab5db09bf2e9')
    expect(decisionResponseHash({ decision: 'satisfy', actor: 'a', reason: 'r' })).toBe(
      'a5568cba2a39448ec7248a00ecf651a6',
    )
  })
})

describe('buildGateCommand', () => {
  test('valid satisfy', () => {
    const command = buildGateCommand('merge-auth', 'act_1', mergeAuthGate(), decision)
    expect(command.operation).toBe('satisfy')
    expect(command.actor).toBe('austin')
    expect(command.node_id).toBe('merge-auth')
    expect(command.activation_id).toBe('act_1')
    expect(command.gate_id).toBe('merge-authorization')
    expect(command.subject?.revision).toBe('abc123')
    expect(command.source).toBe('escalation-bridge')
    // Kernel requires at least one evidence id; the transport pointer
    // travels in the reason so it survives in the gate history.
    expect(command.evidence_ids).toHaveLength(1)
    expect(command.evidence_ids[0].startsWith('ev_bridge_')).toBe(true)
    expect(command.reason).toContain('slack.com/archives')
  })

  test('response hash is stable per decision', () => {
    const gate = mergeAuthGate()
    const first = buildGateCommand('n', 'act_1', gate, decision)
    const second = buildGateCommand('n', 'act_1', gate, { ...decision })
    expect(first.response_hash).toBe(second.response_hash)
    // The hash doubles as the evidence id and archive filename, so it must
    // be a fixed width and must distinguish decisions.
    expect(first.response_hash).toHaveLength(32)
    const third = buildGateCommand('n', 'act_1', gate, { ...decision, reason: 'declined after review' })
    expect(third.response_hash).not.toBe(first.response_hash)
    expect(third.response_hash).toBe('0a6cb8478c16d02dfd71219c1d6b4980')
  })

  test('rejects invalid operation value', () => {
    expect(() => buildGateCommand('n', 'act_1', mergeAuthGate(), { ...decision, decision: 'approve' })).toThrow(
      Error,
    )
  })

  test('rejects missing actor or reason', () => {
    for (const field of ['actor', 'reason'] as const) {
      const broken = { ...decision }
      delete broken[field]
      expect(() => buildGateCommand('n', 'act_1', mergeAuthGate(), broken)).toThrow(Error)
    }
  })

  test('subject required when gate declares subject_type', () => {
    const broken = { ...decision }
    delete broken.subject
    expect(() => buildGateCommand('n', 'act_1', mergeAuthGate(), broken)).toThrow(Error)
  })

  test('subject type must match declaration', () => {
    const broken: Decision = {
      ...decision,
      subject: { ...decision.subject!, type: 'commit' },
    }
    expect(() => buildGateCommand('n', 'act_1', mergeAuthGate(), broken)).toThrow(Error)
  })

  test('subject type enforced only when declared', () => {
    const gate = mergeAuthGate()
    delete gate.subject_type
    const command = buildGateCommand('n', 'act_1', gate, {
      decision: 'satisfy',
      actor: 'a',
      reason: 'r',
    })
    expect(command.subject).toBeUndefined()
  })

  test('outcome passthrough', () => {
    const command = buildGateCommand('n', 'act_1', mergeAuthGate(), { ...decision, outcome: 'authorized' })
    expect(command.outcome).toBe('authorized')
  })

  test('bound gate submits the pinned subject, not the transport\'s', () => {
    // The transport supplies a different subject; a bound gate must submit
    // exactly the pinned one, ignoring the transport's.
    const transportSubject = {
      type: 'pull_request',
      repository: 'wrong/repo',
      pull_request: 999,
      revision: 'deadbeef',
    }
    const pinned = {
      type: 'pull_request',
      repository: 'org/repo',
      pull_request: 123,
      revision: 'abc123',
    }
    const command = buildGateCommand('wf_1', 'n', mergeAuthGate(), { ...decision, subject: transportSubject }, pinned)
    expect(command.subject).toEqual(pinned)
    expect(command.subject!.revision).toBe('abc123')
    expect(command.subject!.pull_request).toBe(123)
  })

  test('bound gate requires no transport subject', () => {
    // A bound gate submits the pinned subject even when the transport
    // supplies none at all, and skips the subject_type validation that
    // would otherwise reject the missing subject.
    const { subject: _omitted, ...broken } = decision
    const pinned = {
      type: 'pull_request',
      repository: 'org/repo',
      pull_request: 123,
      revision: 'abc123',
    }
    const command = buildGateCommand('wf_1', 'n', mergeAuthGate(), broken, pinned)
    expect(command.subject).toEqual(pinned)
  })

  test('pinned substitution does not change the response hash', () => {
    // Both bridges hash the transport's decision payload as delivered; the
    // kernel-side subject substitution happens after hashing.
    const command = buildGateCommand('n', 'act_1', mergeAuthGate(), decision, {
      type: 'pull_request',
      repository: 'org/repo',
      pull_request: 123,
      revision: 'abc123',
    })
    expect(command.response_hash).toBe(decisionResponseHash(decision))
  })
})
