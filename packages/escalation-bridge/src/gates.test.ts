import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { describe, expect, test } from 'bun:test'

import { latestOutputs, loadHumanGates, pendingHumanGates, pinnedSubject } from './gates.js'
import type { Activation, GateDefinition, WorkflowDetail } from './types.js'

function parkedActivation(actId = 'act_1', nodeId = 'merge-auth'): Activation {
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

describe('pendingHumanGates', () => {
  const gates = new Map([['merge-auth', [mergeAuthGate()]]])

  test('yields parked required human gate', () => {
    const detail: WorkflowDetail = { activations: [parkedActivation()], gates: null }
    const found = [...pendingHumanGates(detail, gates)]
    expect(found).toHaveLength(1)
    const [act, gate] = found[0]
    expect(act.activation_id).toBe('act_1')
    expect(gate.id).toBe('merge-authorization')
  })

  test('skips unparked activations', () => {
    const act = parkedActivation()
    act.status = 'satisfied'
    const detail: WorkflowDetail = { activations: [act], gates: null }
    expect([...pendingHumanGates(detail, gates)]).toEqual([])
  })

  test.each(['passed', 'waived'])('skips settled gate instances (%s)', (settledStatus) => {
    const detail: WorkflowDetail = {
      activations: [parkedActivation()],
      gates: [
        {
          activation_id: 'act_1',
          gate_id: 'merge-authorization',
          status: settledStatus,
        },
      ],
    }
    expect([...pendingHumanGates(detail, gates)]).toEqual([])
  })

  test('skips unrequired gates', () => {
    const gate = mergeAuthGate()
    gate.required = false
    const detail: WorkflowDetail = { activations: [parkedActivation()], gates: null }
    expect([...pendingHumanGates(detail, new Map([['merge-auth', [gate]]]))]).toEqual([])
  })

  test('tolerates null lists', () => {
    expect([...pendingHumanGates({}, gates)]).toEqual([])
  })
})

describe('latestOutputs', () => {
  test('decodes and keeps highest revision', () => {
    const detail: WorkflowDetail = {
      outputs: [
        { definition_id: 'head_sha', revision: 1, value: '"old"' },
        { definition_id: 'head_sha', revision: 2, value: '"abc123"' },
        { definition_id: 'pull_number', revision: 1, value: '123' },
      ],
    }
    expect(latestOutputs(detail)).toEqual({ head_sha: 'abc123', pull_number: 123 })
  })

  test('tolerates null and raw values', () => {
    const detail: WorkflowDetail = {
      outputs: [
        { definition_id: 'x', revision: 1, value: null },
        { definition_id: 'y', revision: 1, value: 'not json' },
      ],
    }
    expect(latestOutputs(detail)).toEqual({ x: null, y: 'not json' })
  })

  test('keeps the later entry when revisions tie', () => {
    // Python's `rev >= best` keeps the later list entry on a tie; the TS
    // loop must too, so both bridges ask with the same payload.
    const detail: WorkflowDetail = {
      outputs: [
        { definition_id: 'head_sha', revision: 2, value: '"first"' },
        { definition_id: 'head_sha', revision: 2, value: '"second"' },
      ],
    }
    expect(latestOutputs(detail)).toEqual({ head_sha: 'second' })
  })
})

describe('loadHumanGates', () => {
  test('maps only human gates by node', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'escalation-bridge-'))
    const templatePath = path.join(dir, 't.json')
    fs.writeFileSync(
      templatePath,
      JSON.stringify({
        nodes: [
          { id: 'a', gates: [{ id: 'g1', type: 'human' }] },
          { id: 'b', gates: [{ id: 'g2', type: 'external' }] },
          { id: 'c' },
        ],
      }),
    )
    const gates = loadHumanGates(templatePath)
    expect([...gates.keys()]).toEqual(['a'])
    expect(gates.get('a')![0].id).toBe('g1')
  })

  test('throws on a node that declares human gates without an id', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'escalation-bridge-'))
    const templatePath = path.join(dir, 't.json')
    fs.writeFileSync(
      templatePath,
      JSON.stringify({
        nodes: [
          { id: 'a', gates: [{ id: 'g1', type: 'human' }] },
          { gates: [{ id: 'typoed-node-gate', type: 'human' }] },
          { gates: [{ type: 'human' }] },
        ],
      }),
    )
    // The gates would be silently unreachable (no activation can match an
    // id-less node), parking the workflow forever; Python fails the same
    // template with a KeyError on node["id"].
    expect(() => loadHumanGates(templatePath)).toThrow(/without an id/)
    expect(() => loadHumanGates(templatePath)).toThrow(/typoed-node-gate/)
    // The message falls back for a gate that has no id of its own (its own
    // template: the first offending node throws and stops the walk).
    const noGateIdPath = path.join(dir, 't2.json')
    fs.writeFileSync(noGateIdPath, JSON.stringify({ nodes: [{ gates: [{ type: 'human' }] }] }))
    expect(() => loadHumanGates(noGateIdPath)).toThrow(/no gate id/)
  })
})

describe('pinnedSubject', () => {
  const PINNED = {
    type: 'pull_request',
    repository: 'org/repo',
    pull_request: 123,
    revision: 'abc123',
  }

  test('returns the pinned subject when resolved', () => {
    const act: Activation = {
      activation_id: 'act_1',
      node_id: 'merge-auth',
      status: 'awaiting_gate',
      resolved_gates: { g1: { subject: { ...PINNED } } },
    }
    expect(pinnedSubject(act, 'g1')).toEqual(PINNED)
  })

  test('returns null when the pin is unresolved', () => {
    // An unresolved pin has no subject key (omitempty) and lists the
    // unresolved fields instead.
    const act: Activation = {
      activation_id: 'act_1',
      node_id: 'merge-auth',
      status: 'awaiting_gate',
      resolved_gates: { g1: { unresolved: ['subject.repository'] } },
    }
    expect(pinnedSubject(act, 'g1')).toBeNull()
  })

  test('returns null when the gate has no pin', () => {
    const act: Activation = {
      activation_id: 'act_1',
      node_id: 'merge-auth',
      status: 'awaiting_gate',
      resolved_gates: { other: { subject: { type: 'pull_request' } } },
    }
    expect(pinnedSubject(act, 'g1')).toBeNull()
  })

  test('returns null when there are no resolved gates', () => {
    const act: Activation = {
      activation_id: 'act_1',
      node_id: 'merge-auth',
      status: 'awaiting_gate',
    }
    expect(pinnedSubject(act, 'g1')).toBeNull()
  })
})
