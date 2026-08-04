import { describe, expect, it } from 'bun:test'
import { validateSpawnSelection } from './spawn-selection.js'
import conformance from '../../../schemas/spawn_selection.conformance.json'

const cases = (conformance as { cases: Array<{
  name: string
  input: Record<string, unknown>
  rosterConfigured: boolean
  valid: boolean
  errorContains: string
}> }).cases

describe('validateSpawnSelection conformance', () => {
  it('drives the shared portable fixture', () => {
    expect(cases.length).toBeGreaterThan(0)
  })

  for (const c of cases) {
    it(`matches fixture case: ${c.name}`, () => {
      if (c.valid) {
        expect(() => validateSpawnSelection(c.input, c.rosterConfigured)).not.toThrow()
      } else {
        expect(() => validateSpawnSelection(c.input, c.rosterConfigured)).toThrow(c.errorContains)
      }
    })
  }
})
