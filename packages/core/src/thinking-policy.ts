/**
 * Portable thinking-policy contract.
 *
 * THINKING_LEVELS is the single TypeScript source for the canonical tuple and
 * is exported for hosts (MCP, OpenCode, Pi) to derive their enums from. It is
 * verified against schemas/thinking_policy.umpire.json and the Go tuple by
 * introspection and conformance tests so all languages agree.
 */

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

/** Canonical values a backend supports on a fresh start and on an explicit resume. */
interface ThinkingPolicy {
  readonly start: readonly string[]
  readonly resume: readonly string[]
}

// Mirrors internal/thinkingpolicy.Policies: start support and explicit-resume
// support are represented separately. Verified against the shared conformance
// fixture and the Go policy table.
export const THINKING_POLICIES: Readonly<Record<string, ThinkingPolicy>> = {
  'codex-app-server': { start: THINKING_LEVELS, resume: THINKING_LEVELS },
  pi: { start: THINKING_LEVELS, resume: THINKING_LEVELS },
  claude: { start: ['low', 'medium', 'high', 'xhigh', 'max'] }, // start-only on resume
  'claude-channel': { start: ['low', 'medium', 'high', 'xhigh', 'max'] }, // start-only on resume
  'opencode-acp': {},
  'opencode-http': {},
  'gemini-acp': {},
  'cursor-acp': {},
  agy: {},
  pony: {},
}

/** Applies the static backend policy for a (backend, value, resume) combination. */
export function evaluateThinkingPolicy(
  backend: string,
  value: string,
  resume = false,
): ThinkingOutcome {
  if (value === '') return 'ok'
  const policy = THINKING_POLICIES[backend]
  if (!policy) return 'unsupportedCapability'
  const start = policy.start ?? []
  const resumed = policy.resume ?? []
  const set = resume ? resumed : start
  if (set.length === 0) {
    if (resume && start.length > 0) return 'startOnly'
    return 'unsupportedCapability'
  }
  return set.includes(value) ? 'ok' : 'unsupportedValue'
}

/** Accepts a value iff it is valid under the static backend policy (start). */
export function validateThinkingForBackend(backend: string, value: unknown): void {
  validateThinking(value)
  const v = value === undefined ? '' : String(value)
  if (v === '') return
  const outcome = evaluateThinkingPolicy(backend, v, false)
  if (outcome === 'ok') return
  if (outcome === 'unsupportedValue') {
    const allowed = (THINKING_POLICIES[backend]?.start ?? []).join(', ')
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
    const allowed = (THINKING_POLICIES[backend]?.resume ?? []).join(', ')
    throw new Error(
      `backend ${JSON.stringify(backend)} does not support thinking value ${JSON.stringify(v)} (allowed: ${allowed})`,
    )
  }
  throw new Error(
    `backend ${JSON.stringify(backend)} does not support parameter "thinking"`,
  )
}
