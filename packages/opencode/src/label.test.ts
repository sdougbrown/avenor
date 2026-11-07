import { describe, expect, it, mock } from 'bun:test'

// The label pipeline imports '@dougbots/avenor-core', which bun cannot resolve
// to source on its own, so the shared core module is registered from core's
// source before label.ts loads — the same seam plugin.test.ts uses.
import * as coreModule from '../../core/src/index.js'
import { MAX_SESSION_NAME_RUNES, stripDisallowedSessionNameRunes } from '../../core/src/message.js'
import fixture from '../../../schemas/session-name-rule.json'

mock.module('@dougbots/avenor-core', () => ({ ...coreModule }))

const { labelUsable, sanitizeSessionLabel } = await import('./label.js')

type NameRuleFixture = {
  limit: number
  reservedReferences: string[]
  cases: Array<{ name: string; input: string; valid: boolean; disallowedRunes: boolean }>
  sanitizeCases: Array<{ name: string; input: string; label: string }>
}

const rule = fixture as NameRuleFixture

/**
 * The strip path removes exactly the disallowed runes and nothing else: a
 * name with none passes through unchanged. Checked separately from the rule
 * below, which stands on its own via labelUsable.
 */
const stripChanged = (name: string): boolean => stripDisallowedSessionNameRunes(name) !== name

describe('session name rule conformance', () => {
  it('the fixture limit matches the shared name limit', () => {
    expect(rule.limit).toBe(MAX_SESSION_NAME_RUNES)
  })

  it('the fixture reserved references match the shared reserved set', () => {
    expect([...rule.reservedReferences].sort()).toEqual(['self', 'supervisor'])
    for (const ref of rule.reservedReferences) {
      expect(labelUsable(ref)).toBe(false)
    }
  })

  for (const c of rule.cases) {
    it(`accepts/refuses case: ${c.name}`, () => {
      expect(labelUsable(c.input)).toBe(c.valid)
      expect(stripChanged(c.input)).toBe(c.disallowedRunes)
    })
  }

  for (const c of rule.sanitizeCases) {
    it(`sanitizes case: ${c.name}`, () => {
      expect(sanitizeSessionLabel(c.input)).toBe(c.label)
      // Every label the derivation produces must itself pass the name rule.
      expect(labelUsable(c.label)).toBe(true)
    })
  }
})
