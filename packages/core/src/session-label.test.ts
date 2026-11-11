import { describe, expect, it } from 'bun:test'

import fixture from '../../../schemas/session-name-rule.json'
import { MAX_SESSION_NAME_RUNES, stripDisallowedSessionNameRunes } from './message.js'
import { hostLabel, labelUsable, pickSessionLabel, sanitizeSessionLabel, sessionLabel, takenLabelsFromListing } from './session-label.js'

type NameRuleFixture = {
  limit: number
  reservedReferences: string[]
  cases: Array<{ name: string; input: string; valid: boolean; disallowedRunes: boolean }>
  sanitizeCases: Array<{ name: string; input: string; label: string }>
  deriveCases: Array<{ name: string; toplevel: string; branch: string; label: string }>
  hostCases: Array<{ name: string; host: string; work: string; label: string }>
  pickCases: Array<{ name: string; base: string; taken: string[]; label: string }>
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

  for (const c of rule.deriveCases) {
    it(`derives case: ${c.name}`, async () => {
      const git = async (_dir: string, args: string[]) => (args[0] === 'rev-parse' ? c.toplevel : c.branch)
      expect(await sessionLabel('/work', git)).toBe(c.label)
    })
  }

  for (const c of rule.hostCases) {
    it(`prefixes host case: ${c.name}`, () => {
      expect(hostLabel(c.host, c.work)).toBe(c.label)
    })
  }

  for (const c of rule.pickCases) {
    it(`picks case: ${c.name}`, () => {
      expect(pickSessionLabel(c.base, new Set(c.taken))).toBe(c.label)
    })
  }
})

describe('takenLabelsFromListing', () => {
  const peers = [
    { run_id: 'a', label: 'one' },
    { run_id: 'b', label: 'two' },
    { run_id: 'c' },
    { run_id: 'd', label: '' },
  ]

  it('collects the labels the listed peers hold', () => {
    expect(takenLabelsFromListing({ peers })).toEqual(new Set(['one', 'two']))
  })

  it('leaves out the excluded run', () => {
    expect(takenLabelsFromListing({ peers }, 'a')).toEqual(new Set(['two']))
  })

  it('is no information when the listing is marked incomplete', () => {
    expect(takenLabelsFromListing({ peers, detail: 'one host did not answer' })).toBeUndefined()
  })
})
