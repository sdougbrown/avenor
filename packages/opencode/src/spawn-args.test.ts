import { describe, expect, it } from 'bun:test'
import { spawnFieldDescription, spawnSurfaceFields } from '@dougbots/avenor-core'

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

  it('takes every description from the shared spawn schema', async () => {
    for (const [name, schema] of Object.entries(await spawnArgs())) {
      expect(schema.description ?? '').toBe(spawnFieldDescription(name))
    }
  })
})
