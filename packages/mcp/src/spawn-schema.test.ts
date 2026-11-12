import { describe, expect, it } from 'bun:test'
import {
  THINKING_LEVELS,
  spawnFieldDescription,
  spawnFieldEnum,
  spawnSurfaceFields,
  validateSpawnSelection,
} from '@dougbots/avenor-core'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema'

const spawnSchema = z.object(spawnInputShape)

// The published field set comes from schemas/spawn.input.json, not from a list
// remembered here: a field this surface gains or loses shows up as a failure.
const expectedFields = spawnSurfaceFields('mcp')

describe('MCP spawn schema', () => {
  it('refuses a backend the schema does not declare', () => {
    for (const backend of spawnFieldEnum('backend')) {
      expect(spawnSchema.safeParse({ dir: '/tmp/repo', backend }).success).toBe(true)
    }
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', backend: 'opencode-htt' }).success).toBe(false)
  })

  it('takes every description from the shared spawn schema', () => {
    for (const [name, schema] of Object.entries(spawnInputShape)) {
      expect(schema.description ?? '').toBe(spawnFieldDescription(name))
    }
  })

  it('keeps the flat field contract and makes selectors optional', () => {
    expect(Object.keys(spawnInputShape).sort()).toEqual(expectedFields)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', agent: 'reviewer' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', model: 'provider/model' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', backend: 'agy' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', roster_file: '/repo/roster.json' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', roster_entry: 'planner' }).success).toBe(true)
    expect(spawnSchema.safeParse({
      dir: '/tmp/repo',
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
    }).success).toBe(true)

    // Exercise the remaining optional flat fields
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', prompt: 'hello' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', prompt_file: '/tmp/prompt.md' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', label: 'test-run' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', timeout: '5m' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', server_url: 'http://localhost:3000' }).success).toBe(true)
    expect(spawnSchema.safeParse({ dir: '/tmp/repo', supervisor_id: 'svc-1' }).success).toBe(true)

    // All optional flat fields together
    expect(spawnSchema.safeParse({
      dir: '/tmp/repo',
      prompt: 'hello',
      prompt_file: '/tmp/prompt.md',
      label: 'test-run',
      timeout: '5m',
      server_url: 'http://localhost:3000',
      supervisor_id: 'svc-1',
    }).success).toBe(true)

    // dir is required
    expect(spawnSchema.safeParse({}).success).toBe(false)
    expect(spawnSchema.safeParse({ agent: 'reviewer' }).success).toBe(false)

    expect(expectedFields).not.toContain('mode')

    // Host schema must publish exactly the shared thinking tuple: the enum a
    // client reads is the values it may send, so extra or missing levels change it.
    const publishedThinking: unknown = z.toJSONSchema(spawnInputShape.thinking, { target: 'draft-7', io: 'input' })
    expect(publishedThinking).toMatchObject({ type: 'string', enum: [...THINKING_LEVELS] })
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
        'invalid spawn selector: agent is a direct identity field, disabled in roster mode (roster_entry is set); drop agent to use the roster entry, or drop roster_file and roster_entry to use direct identity',
      ],
      [
        { roster_file: '/repo/roster.json', roster_entry: 'planner', model: 'provider/model' },
        'invalid spawn selector: model is a direct identity field, disabled in roster mode (roster_entry is set); drop model to use the roster entry, or drop roster_file and roster_entry to use direct identity',
      ],
      [
        { roster_file: '/repo/roster.json', roster_entry: 'planner', backend: 'agy' },
        'invalid spawn selector: backend is a direct identity field, disabled in roster mode (roster_entry is set); drop backend to use the roster entry, or drop roster_file and roster_entry to use direct identity',
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
