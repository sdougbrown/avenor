import { describe, expect, it } from 'bun:test'
import {
  THINKING_LEVELS,
  spawnFieldDescription,
  spawnFieldProperties,
  spawnSurfaceFields,
} from '@dougbots/avenor-core'

import { AvenorPlugin } from './plugin'

// The spawn tool's arguments are built where the plugin registers them, so the
// test asks the plugin for the tool it publishes rather than importing a
// parallel copy.
function makeCtx() {
  return {
    client: { session: {} } as any,
    project: {},
    directory: '/tmp/test',
    worktree: '/tmp/test',
    serverUrl: new URL('http://localhost:4096'),
    $: {} as any,
    experimental_workspace: { register: () => {} },
  }
}

async function spawnArgs() {
  const hooks = await AvenorPlugin(makeCtx() as any)
  const spawn = hooks.tool?.avenor_spawn as any
  return spawn.args as Record<string, { description?: string }>
}

describe('opencode spawn arguments', () => {
  it('publishes exactly the fields the schema assigns to the opencode surface', async () => {
    expect(Object.keys(await spawnArgs()).sort()).toEqual(spawnSurfaceFields('opencode'))
  })

  it('accepts and refuses the value types the schema declares', async () => {
    for (const [name, schema] of Object.entries(await spawnArgs())) {
      const declared = spawnFieldProperties[name]?.type
      const parsed = schema as unknown as { safeParse: (value: unknown) => { success: boolean } }
      if (declared === 'string') {
        // thinking is the one string field the surfaces narrow to an enum; the
        // schema keeps it a plain string because the backend validates the level.
        const sample = name === 'thinking' ? THINKING_LEVELS[0] : 'value'
        expect(parsed.safeParse(sample).success).toBe(true)
        expect(parsed.safeParse(true).success).toBe(false)
      } else if (declared === 'boolean') {
        expect(parsed.safeParse(true).success).toBe(true)
        expect(parsed.safeParse('value').success).toBe(false)
      } else {
        throw new Error(`spawn field "${name}" declares type "${declared}", which this test does not cover`)
      }
    }
  })

  it('takes every description from the shared spawn schema', async () => {
    for (const [name, schema] of Object.entries(await spawnArgs())) {
      expect(schema.description ?? '').toBe(spawnFieldDescription(name))
    }
  })
})
