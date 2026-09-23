import * as fs from 'node:fs'

import type { Activation, GateDefinition, OutputEntry, WorkflowDetail } from './types.js'

/** Map node_id -> [gate definition] for human gates declared in the template. */
export function loadHumanGates(templatePath: string): Map<string, GateDefinition[]> {
  const template = JSON.parse(fs.readFileSync(templatePath, 'utf8')) as {
    nodes?: Array<{ id?: string; gates?: GateDefinition[] }>
  }
  const gates = new Map<string, GateDefinition[]>()
  for (const node of template.nodes ?? []) {
    const human = (node.gates ?? []).filter((g) => g.type === 'human')
    if (human.length > 0 && node.id) gates.set(node.id, human)
  }
  return gates
}

/**
 * Yield [activation, gate definition] for each parked human gate.
 *
 * Parking lives on activations (status awaiting_gate); a gate instance is
 * recorded only when a decision lands, so a passed/waived instance means the
 * gate no longer blocks this activation.
 */
export function* pendingHumanGates(
  detail: WorkflowDetail,
  templateGates: Map<string, GateDefinition[]>,
): Generator<[Activation, GateDefinition]> {
  const settled = new Set(
    (detail.gates ?? [])
      .filter((g) => g.status === 'passed' || g.status === 'waived')
      .map((g) => `${g.activation_id}\u0000${g.gate_id}`),
  )
  for (const act of detail.activations ?? []) {
    if (act.status !== 'awaiting_gate') continue
    for (const gate of templateGates.get(act.node_id) ?? []) {
      if (!gate.required) continue
      if (settled.has(`${act.activation_id}\u0000${gate.id}`)) continue
      yield [act, gate]
    }
  }
}

/** Map definition_id -> decoded value, keeping the highest revision per output. */
export function latestOutputs(detail: WorkflowDetail): Record<string, unknown> {
  const latest = new Map<string, { rev: number; value: unknown }>()
  for (const out of (detail.outputs ?? []) as OutputEntry[]) {
    const oid = out.definition_id
    if (oid === null || oid === undefined) continue
    const rev = out.revision ?? 0
    const prev = latest.get(oid)
    // Keep the entry only when its revision is >= the best seen so far
    // (0 for a first sighting), matching the Python reference.
    if ((prev?.rev ?? 0) > rev) continue
    let value: unknown
    try {
      value = JSON.parse(out.value || 'null')
    } catch {
      value = out.value
    }
    latest.set(oid, { rev, value })
  }
  return Object.fromEntries([...latest].map(([oid, entry]) => [oid, entry.value]))
}
