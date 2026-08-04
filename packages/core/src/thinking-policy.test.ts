import { describe, expect, it } from 'bun:test'
import schema from '../../../schemas/thinking_policy.umpire.json'
import conformance from '../../../schemas/thinking_policy.conformance.json'
import {
  THINKING_LEVELS,
  evaluateThinkingPolicy,
  isThinkingLevel,
  validateThinking,
  validateThinkingForBackend,
  validateThinkingForBackendResume,
} from './thinking-policy.js'

const thinkingSchema = schema as {
  rules: Array<{ type: string; check?: { op: string; value?: string[] } }>
}
const conformanceData = conformance as {
  canonicalCases: Array<{ name: string; value: string; valid: boolean }>
  backendCases: Array<{
    name: string
    backend: string
    value: string
    resume: boolean
    valid: boolean
  }>
}

const schemaCanonical =
  thinkingSchema.rules.find((r) => r.check?.op === 'in')?.check?.value ?? []

describe('thinking policy conformance', () => {
  it('THINKING_LEVELS matches the canonical tuple embedded in the Umpire schema', () => {
    expect([...THINKING_LEVELS]).toEqual(schemaCanonical)
    for (const level of THINKING_LEVELS) {
      expect(isThinkingLevel(level)).toBe(true)
    }
    expect(isThinkingLevel('HIGH')).toBe(false)
  })

  for (const c of conformanceData.canonicalCases) {
    it(`accepts/rejects canonical case: ${c.name}`, () => {
      if (c.valid) {
        expect(() => validateThinking(c.value)).not.toThrow()
      } else {
        expect(() => validateThinking(c.value)).toThrow()
      }
    })
  }

  for (const c of conformanceData.backendCases) {
    it(`matches backend policy case: ${c.name}`, () => {
      const outcome = evaluateThinkingPolicy(c.backend, c.value, c.resume)
      expect(outcome === 'ok').toBe(c.valid)
    })
  }
})

describe('thinking policy helpers', () => {
  it('rejects unknown canonical values with a descriptive error', () => {
    expect(() => validateThinking('HIGH')).toThrow(
      'off, minimal, low, medium, high, xhigh, max',
    )
  })

  it('distinguishes unsupported capability, unsupported value, and start-only', () => {
    expect(() => validateThinkingForBackend('agy', 'low')).toThrow(
      'does not support parameter "thinking"',
    )
    expect(() => validateThinkingForBackend('claude', 'off')).toThrow(
      'allowed: low, medium, high, xhigh, max',
    )
    expect(() => validateThinkingForBackend('claude', 'low')).not.toThrow()
    expect(() => validateThinkingForBackendResume('claude', 'low')).toThrow(
      'only when starting a session',
    )
  })

  it('all 10 backends are exercised by the conformance fixture', () => {
    const allBackends = [
      'codex-app-server',
      'pi',
      'claude',
      'claude-channel',
      'opencode-acp',
      'opencode-http',
      'gemini-acp',
      'cursor-acp',
      'agy',
      'pony',
    ]
    const fixtureBackends = new Set(
      conformanceData.backendCases.map((c) => c.backend),
    )
    for (const backend of allBackends) {
      expect(fixtureBackends.has(backend)).toBe(true)
    }
  })
})
