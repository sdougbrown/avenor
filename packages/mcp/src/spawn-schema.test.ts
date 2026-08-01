import { describe, expect, it } from 'bun:test'
import { validateSpawnSelection } from '@dougbots/avenor-core'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema'

const spawnSchema = z.object(spawnInputShape)

const expectedFields = [
  'agent',
  'repo_dir',
  'prompt',
  'prompt_file',
  'label',
  'timeout',
  'model',
  'thinking',
  'backend',
  'roster_file',
  'roster_entry',
  'server_url',
  'supervisor_id',
]

describe('MCP spawn schema', () => {
  it('keeps the flat field contract and makes selectors optional', () => {
    expect(Object.keys(spawnInputShape)).toEqual(expectedFields)
    expect(spawnSchema.safeParse({ repo_dir: '/tmp/repo' }).success).toBe(true)
    expect(spawnSchema.safeParse({ repo_dir: '/tmp/repo', agent: 'reviewer' }).success).toBe(true)
    expect(spawnSchema.safeParse({ repo_dir: '/tmp/repo', model: 'provider/model' }).success).toBe(true)
    expect(spawnSchema.safeParse({ repo_dir: '/tmp/repo', backend: 'agy' }).success).toBe(true)
    expect(spawnSchema.safeParse({ repo_dir: '/tmp/repo', roster_file: '/repo/roster.json' }).success).toBe(true)
    expect(spawnSchema.safeParse({ repo_dir: '/tmp/repo', roster_entry: 'planner' }).success).toBe(true)
    expect(spawnSchema.safeParse({
      repo_dir: '/tmp/repo',
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
    }).success).toBe(true)
    expect(expectedFields).not.toContain('mode')
  })

  it('matches the shared strict selector validation for mixed fields', () => {
    const rejected: Array<[Record<string, string>, string]> = [
      [
        { roster_file: '/repo/roster.json' },
        'invalid spawn selector: roster_file requires roster_entry',
      ],
      [
        { roster_entry: 'planner' },
        'invalid spawn selector: roster_entry requires roster_file unless a roster context is configured',
      ],
      [
        { roster_file: '/repo/roster.json', roster_entry: 'planner', agent: 'reviewer' },
        'invalid spawn selector: direct identity fields are disabled in roster mode',
      ],
      [
        { roster_file: '/repo/roster.json', roster_entry: 'planner', model: 'provider/model' },
        'invalid spawn selector: direct identity fields are disabled in roster mode',
      ],
      [
        { roster_file: '/repo/roster.json', roster_entry: 'planner', backend: 'agy' },
        'invalid spawn selector: direct identity fields are disabled in roster mode',
      ],
    ]

    for (const [input, message] of rejected) {
      expect(() => validateSpawnSelection(input)).toThrow(message)
    }
  })

  it('retains direct allow-neither and backend-only behavior', () => {
    expect(() => validateSpawnSelection({})).not.toThrow()
    expect(() => validateSpawnSelection({ backend: 'agy' })).not.toThrow()
  })
})
