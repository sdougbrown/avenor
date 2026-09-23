import { createHash } from 'node:crypto'

import type { Decision, GateCommand, GateDefinition } from './types.js'

// String escaping matching Python's json.dumps(ensure_ascii=True): escapes
// anything outside printable ASCII, including astral chars as surrogate
// pairs, so both bridge implementations hash identical bytes.
function escapeString(s: string): string {
  let out = '"'
  for (const ch of s) {
    const code = ch.codePointAt(0)!
    if (ch === '"') out += '\\"'
    else if (ch === '\\') out += '\\\\'
    else if (ch === '\n') out += '\\n'
    else if (ch === '\r') out += '\\r'
    else if (ch === '\t') out += '\\t'
    else if (ch === '\b') out += '\\b'
    else if (ch === '\f') out += '\\f'
    else if (code >= 0x20 && code <= 0x7e) out += ch
    else if (code > 0xffff) {
      const high = Math.floor((code - 0x10000) / 0x400) + 0xd800
      const low = ((code - 0x10000) % 0x400) + 0xdc00
      out += '\\u' + high.toString(16).padStart(4, '0') + '\\u' + low.toString(16).padStart(4, '0')
    } else {
      out += '\\u' + code.toString(16).padStart(4, '0')
    }
  }
  return out + '"'
}

// Code-point comparison matching Python's str sort (used by
// json.dumps(sort_keys=True)); JS default '<' compares UTF-16 code units,
// which orders astral keys before some BMP keys.
function compareCodePoints(a: string, b: string): number {
  const ai = a[Symbol.iterator]()
  const bi = b[Symbol.iterator]()
  for (;;) {
    const ra = ai.next()
    const rb = bi.next()
    if (ra.done && rb.done) return 0
    if (ra.done) return -1
    if (rb.done) return 1
    const ca = ra.value.codePointAt(0)!
    const cb = rb.value.codePointAt(0)!
    if (ca !== cb) return ca < cb ? -1 : 1
  }
}

/**
 * Serialize like Python's json.dumps(value, sort_keys=True) — sorted object
 * keys, ', '/': ' separators, ensure_ascii escaping — so the TypeScript and
 * Python bridges derive identical response hashes for the same decision.
 * Parity holds for the string/integer/bool/null decision payloads the
 * protocol calls for; floats can diverge (Python repr vs JSON.stringify
 * formatting), so float-valued decision fields have no cross-bridge hash
 * guarantee.
 */
export function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  const t = typeof value
  if (t === 'boolean') return value ? 'true' : 'false'
  if (t === 'number') return JSON.stringify(value)
  if (t === 'string') return escapeString(value as string)
  if (Array.isArray(value)) return '[' + value.map(stableStringify).join(', ') + ']'
  if (t === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
      compareCodePoints(a, b),
    )
    return (
      '{' + entries.map(([k, v]) => escapeString(k) + ': ' + stableStringify(v)).join(', ') + '}'
    )
  }
  throw new TypeError(`cannot serialize ${t} in decision payload`)
}

export function decisionResponseHash(decision: Decision): string {
  return createHash('sha256').update(stableStringify(decision)).digest('hex').slice(0, 32)
}

/**
 * Build the workflow.command gate payload from the transport's decision.
 *
 * The kernel requires actor, reason, and at least one evidence id for
 * satisfy/reject/waive, plus a subject with a non-empty type when the gate
 * declares a subject_type. Evidence ids on a bridge-recorded decision are
 * opaque audit references: the transport's durable pointer is appended to
 * the reason so it survives in the append-only gate history.
 */
export function buildGateCommand(
  nodeId: string,
  activationId: string,
  gate: GateDefinition,
  decision: Decision,
): GateCommand {
  if (!gate.id) throw new Error('gate definition is missing an id')
  const op = decision.decision
  if (op !== 'satisfy' && op !== 'reject') {
    throw new Error(`decision must be 'satisfy' or 'reject', got ${JSON.stringify(op)}`)
  }
  const actor = decision.actor
  let reason = decision.reason
  if (!actor || !reason) throw new Error("decision requires 'actor' and 'reason'")

  const subjectType = gate.subject_type
  const subject = decision.subject
  if (subjectType) {
    if (!subject || subject.type !== subjectType) {
      throw new Error(
        `gate declares subject_type ${JSON.stringify(subjectType)}; decision.subject ` +
          'must carry a matching non-empty type',
      )
    }
  }

  const evidencePointer = decision.evidence
  if (evidencePointer) reason = `${reason}\nevidence: ${evidencePointer}`
  // Hashed over the original decision, before the evidence pointer is
  // appended to the reason — same as the Python bridge.
  const responseHash = decisionResponseHash(decision)
  const evidenceIds = ['ev_bridge_' + responseHash]

  const command: GateCommand = {
    op: 'gate',
    node_id: nodeId,
    activation_id: activationId,
    gate_id: gate.id,
    operation: op,
    actor,
    reason,
    source: 'escalation-bridge',
    response_hash: responseHash,
    evidence_ids: evidenceIds,
  }
  if (subject) command.subject = subject
  if (decision.outcome) command.outcome = decision.outcome
  return command
}
