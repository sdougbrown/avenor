import { describe, expect, it } from 'bun:test'

import {
  spawnFieldDescription,
  spawnFieldNames,
  spawnFieldProperties,
  spawnSurfaceFields,
} from './spawn-schema'

const SURFACES = ['go', 'mcp', 'pi', 'opencode'] as const

describe('spawn input schema', () => {
  it('describes every field it declares', () => {
    expect(spawnFieldNames.length).toBeGreaterThan(10)
    for (const name of spawnFieldNames) {
      expect(spawnFieldDescription(name).length).toBeGreaterThan(20)
    }
  })

  it('names at least one known surface for every field', () => {
    for (const name of spawnFieldNames) {
      const surfaces = spawnFieldProperties[name]?.['x-avenor-surfaces'] ?? []
      expect(surfaces.length).toBeGreaterThan(0)
      for (const surface of surfaces) {
        expect(SURFACES).toContain(surface as (typeof SURFACES)[number])
      }
    }
  })

  it('gives each surface the field set its annotations name', () => {
    expect(spawnSurfaceFields('go')).toEqual([
      'agent', 'auto_approve', 'backend', 'idempotency_key', 'label', 'launch_wrapper',
      'messaging', 'model', 'persist_sessions', 'prompt', 'prompt_file', 'repo_dir',
      'roster_entry', 'roster_file', 'server_url', 'supervisor_id', 'thinking', 'timeout',
    ])
    expect(spawnSurfaceFields('mcp')).toEqual([
      'agent', 'backend', 'label', 'model', 'prompt', 'prompt_file', 'repo_dir',
      'roster_entry', 'roster_file', 'server_url', 'supervisor_id', 'thinking', 'timeout',
    ])
    expect(spawnSurfaceFields('pi')).toEqual([
      'agent', 'backend', 'dir', 'label', 'messaging', 'model', 'persist_sessions',
      'prompt', 'prompt_file', 'roster_entry', 'roster_file', 'server_url',
      'supervisor_id', 'thinking', 'timeout', 'wait',
    ])
    expect(spawnSurfaceFields('opencode')).toEqual([
      'agent', 'backend', 'dir', 'label', 'model', 'prompt', 'prompt_file',
      'roster_entry', 'roster_file', 'server_url', 'supervisor_id', 'thinking',
      'timeout', 'wait',
    ])
  })

  it('refuses a field it does not declare', () => {
    expect(() => spawnFieldDescription('system')).toThrow('has no description')
  })
})
