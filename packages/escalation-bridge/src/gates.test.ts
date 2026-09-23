import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { describe, expect, test } from 'bun:test'

import { latestOutputs, loadHumanGates, pendingHumanGates } from './gates.js'
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
      outputs: [{ definition_id: 'x', revision: 1, value: null }],
    }
    expect(latestOutputs(detail)).toEqual({ x: null })
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
})
