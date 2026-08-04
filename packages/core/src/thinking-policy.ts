/**
 * Portable thinking-policy contract.
 *
 * THINKING_LEVELS is the single TypeScript source for the canonical tuple and
 * is exported for hosts (MCP, OpenCode, Pi) to derive their enums from. It is
 * verified against schemas/thinking_policy.umpire.json and the Go tuple by
 * introspection and conformance tests so all languages agree.
 *
 * The backend policy (which canonical values each backend supports, on start
 * vs. explicit resume) is modelled directly in the Umpire schema's eitherOf
 * branches with fairWhen rules and conditions (backend, resume). Both Go and
 * TypeScript derive the policy from the schema, eliminating hand-written
 * policy tables that could drift apart.
 */

import schema from '../../../schemas/thinking_policy.umpire.json'

export const THINKING_LEVELS = [
  'off',
  'minimal',
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
] as const

export type ThinkingLevel = (typeof THINKING_LEVELS)[number]

export function isThinkingLevel(value: string): boolean {
  return (THINKING_LEVELS as readonly string[]).includes(value)
}

/** Accepts an empty value (backend default) or a canonical thinking level. */
export function validateThinking(value: unknown): void {
  if (value === undefined || value === '') return
  if (typeof value !== 'string' || !isThinkingLevel(value)) {
    throw new Error(
      `invalid thinking value ${JSON.stringify(value)} (allowed: ${THINKING_LEVELS.join(', ')})`,
    )
  }
}

export type ThinkingOutcome =
  | 'ok'
  | 'unsupportedCapability'
  | 'unsupportedValue'
  | 'startOnly'

// ---------------------------------------------------------------------------
// Schema-derived backend policy
// ---------------------------------------------------------------------------
//
// The Umpire schema's eitherOf group "thinkingPolicy" has named branches, each
// with a fairWhen rule that encodes a backend-specific policy as a condition
// expression. We parse these branches at module load time to derive:
//
//   - supportedBackends: the set of backends that appear in condIn expressions
//   - isFair(backend, value, resume): evaluates the branch expressions
//
// This makes the schema the single source of truth for backend policies.

interface EitherOfBranch {
  when: Expr
  reason: string
}

type Expr =
  | { op: 'absent'; field: string }
  | { op: 'in'; field: string; values: string[] }
  | { op: 'condIn'; condition: string; values: string[] }
  | { op: 'condEq'; condition: string; value: string | boolean }
  | { op: 'and'; exprs: Expr[] }
  | { op: 'or'; exprs: Expr[] }

const thinkingSchema = schema as {
  conditions: Record<string, { type: string }>
  fields: Record<string, unknown>
  rules: Array<{
    type: string
    field?: string
    check?: { op: string; value?: string[] }
    branches?: Record<string, Array<{ type: string; field: string; when: Expr; reason: string }>>
  }>
}

// Extract canonical values from the check rule
const schemaCanonical =
  thinkingSchema.rules.find((r) => r.check?.op === 'in')?.check?.value ?? []

// Extract eitherOf fairWhen branches
const policyBranches: EitherOfBranch[] = (() => {
  const eitherOf = thinkingSchema.rules.find((r) => r.type === 'eitherOf')
  if (!eitherOf?.branches) return []
  const branches: EitherOfBranch[] = []
  for (const branchRules of Object.values(eitherOf.branches)) {
    for (const rule of branchRules) {
      if (rule.type === 'fairWhen') {
        branches.push({ when: rule.when, reason: rule.reason })
      }
    }
  }
  return branches
})()

// Derive the set of backends that have thinking support from the schema
const supportedBackends: ReadonlySet<string> = (() => {
  const backends = new Set<string>()
  for (const branch of policyBranches) {
    collectBackends(branch.when, backends)
  }
  return backends
})()

function collectBackends(expr: Expr, backends: Set<string>): void {
  if (expr.op === 'condIn' && expr.condition === 'backend') {
    for (const v of expr.values) backends.add(v)
    return
  }
  if (expr.op === 'and' || expr.op === 'or') {
    for (const sub of expr.exprs) collectBackends(sub, backends)
  }
}

// Evaluate a fairWhen branch expression for a given (value, backend, resume)
function evalExpr(expr: Expr, value: string, backend: string, resume: boolean): boolean {
  switch (expr.op) {
    case 'absent':
      return value === ''
    case 'in':
      return (expr.values as readonly string[]).includes(value)
    case 'condIn':
      return expr.condition === 'backend' && expr.values.includes(backend)
    case 'condEq':
      if (expr.condition === 'backend') return backend === expr.value
      if (expr.condition === 'resume') return resume === expr.value
      return false
    case 'and':
      return expr.exprs.every((e) => evalExpr(e, value, backend, resume))
    case 'or':
      return expr.exprs.some((e) => evalExpr(e, value, backend, resume))
    default:
      return false
  }
}

/** Evaluates the schema-derived backend policy for a (backend, value, resume) combination. */
export function evaluateThinkingPolicy(
  backend: string,
  value: string,
  resume = false,
): ThinkingOutcome {
  if (value === '') return 'ok'
  // Check if any branch makes this fair
  const fair = policyBranches.some((b) => evalExpr(b.when, value, backend, resume))
  if (fair) return 'ok'
  if (!supportedBackends.has(backend)) return 'unsupportedCapability'
  // Known backend but value rejected; check if it would pass on start
  if (resume) {
    const startFair = policyBranches.some((b) => evalExpr(b.when, value, backend, false))
    if (startFair) return 'startOnly'
  }
  return 'unsupportedValue'
}

/** Returns the canonical values supported by backend on a fresh start. */
function startValues(backend: string): readonly string[] {
  if (!supportedBackends.has(backend)) return []
  return THINKING_LEVELS.filter((v) =>
    policyBranches.some((b) => evalExpr(b.when, v, backend, false)),
  )
}

/** Returns the canonical values supported by backend on an explicit resume. */
function resumeValues(backend: string): readonly string[] {
  if (!supportedBackends.has(backend)) return []
  return THINKING_LEVELS.filter((v) =>
    policyBranches.some((b) => evalExpr(b.when, v, backend, true)),
  )
}

/** Accepts a value iff it is valid under the static backend policy (start). */
export function validateThinkingForBackend(backend: string, value: unknown): void {
  validateThinking(value)
  const v = value === undefined ? '' : String(value)
  if (v === '') return
  const outcome = evaluateThinkingPolicy(backend, v, false)
  if (outcome === 'ok') return
  if (outcome === 'unsupportedValue') {
    const allowed = startValues(backend).join(', ')
    throw new Error(
      `backend ${JSON.stringify(backend)} does not support thinking value ${JSON.stringify(v)} (allowed: ${allowed})`,
    )
  }
  throw new Error(
    `backend ${JSON.stringify(backend)} does not support parameter "thinking"`,
  )
}

/** Accepts a value iff it is valid on an explicit resume for the backend. */
export function validateThinkingForBackendResume(backend: string, value: unknown): void {
  validateThinking(value)
  const v = value === undefined ? '' : String(value)
  if (v === '') return
  const outcome = evaluateThinkingPolicy(backend, v, true)
  if (outcome === 'ok') return
  if (outcome === 'startOnly') {
    throw new Error(
      `backend ${JSON.stringify(backend)} supports parameter "thinking" only when starting a session`,
    )
  }
  if (outcome === 'unsupportedValue') {
    const allowed = resumeValues(backend).join(', ')
    throw new Error(
      `backend ${JSON.stringify(backend)} does not support thinking value ${JSON.stringify(v)} (allowed: ${allowed})`,
    )
  }
  throw new Error(
    `backend ${JSON.stringify(backend)} does not support parameter "thinking"`,
  )
}