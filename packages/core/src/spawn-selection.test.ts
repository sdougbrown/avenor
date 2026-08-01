import { describe, expect, it } from 'bun:test'
import { validateSpawnSelection } from './spawn-selection.js'

describe('validateSpawnSelection', () => {
  const accepted: Array<[string, unknown, boolean?]> = [
    ['direct neither supplied', {}, false],
    ['direct backend only', { backend: 'agy' }, false],
    ['direct agent only', { agent: 'reviewer' }, false],
    ['direct model only', { model: 'provider/model' }, false],
    ['direct valid identity', { agent: 'reviewer', model: 'provider/model', backend: 'opencode-acp' }, false],
    ['roster pair', { roster_file: '/repo/roster.json', roster_entry: 'planner' }, false],
    ['configured roster context', { roster_entry: 'planner' }, true],
  ]

  for (const [name, input, rosterConfigured] of accepted) {
    it(`accepts ${name}`, () => {
      expect(() => validateSpawnSelection(input, rosterConfigured)).not.toThrow()
    })
  }

  const rejected: Array<[string, unknown, boolean?]> = [
    ['missing roster entry', { roster_file: '/repo/roster.json' }, false],
    ['missing roster file', { roster_entry: 'planner' }, false],
    ['mixed agent', { roster_file: '/repo/roster.json', roster_entry: 'planner', agent: 'reviewer' }, false],
    ['mixed model', { roster_file: '/repo/roster.json', roster_entry: 'planner', model: 'provider/model' }, false],
    ['mixed backend', { roster_file: '/repo/roster.json', roster_entry: 'planner', backend: 'agy' }, false],
    ['deferred thinking field', { thinking: 'high' }, false],
    ['deferred system field', { system: 'deferred' }, false],
    ['misspelled roster field', { rosterFile: '/repo/roster.json', roster_entry: 'planner' }, false],
  ]

  for (const [name, input, rosterConfigured] of rejected) {
    it(`rejects ${name}`, () => {
      expect(() => validateSpawnSelection(input, rosterConfigured)).toThrow()
    })
  }
})
