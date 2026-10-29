import { describe, expect, test } from 'bun:test'

import plugin, { setupAvenor } from './index.js'

function makeV2Ctx() {
  const added: Array<Record<string, any>> = []
  const permissionHooks: Array<{ name: string; callback: (event: any) => Promise<void> }> = []
  const sessionHooks: Record<string, (event: any) => Promise<void> | void> = {}
  const subscribeOptions: Array<Record<string, unknown>> = []
  const ctx = {
    location: { directory: '/tmp/project' },
    tool: {
      transform: async (callback: (editor: { add: (tool: Record<string, any>) => void }) => Promise<void>) => {
        await callback({ add: (tool) => added.push(tool) })
      },
    },
    permission: {
      hook: async (name: string, callback: (event: any) => Promise<void>) => {
        permissionHooks.push({ name, callback })
      },
    },
    session: {
      hook: async (name: string, callback: (event: any) => Promise<void> | void) => {
        sessionHooks[name] = callback
      },
    },
    event: {
      subscribe: (options?: Record<string, unknown>) => {
        subscribeOptions.push(options ?? {})
        return (async function* () {})()
      },
    },
  }
  return { ctx, added, permissionHooks, sessionHooks, subscribeOptions }
}

describe('dual entrypoint', () => {
  test('default export carries the V2 definition and the V1 server hook', () => {
    expect(typeof plugin.id).toBe('string')
    expect(plugin.id.length).toBeGreaterThan(0)
    expect(typeof plugin.setup).toBe('function')
    expect(typeof (plugin as Record<string, unknown>).server).toBe('function')
  })
})

describe('setupAvenor', () => {
  test('registers every tool with a JSON Schema input and the domain hooks', async () => {
    const { ctx, added, permissionHooks, sessionHooks, subscribeOptions } = makeV2Ctx()
    const cleanup = await setupAvenor(ctx as never)
    expect(typeof cleanup).toBe('function')

    expect(added.length).toBeGreaterThan(10)
    const spawn = added.find((tool) => tool.name === 'avenor_spawn')
    expect(spawn).toBeDefined()
    expect(spawn!.input.type).toBe('object')
    expect(spawn!.input.properties.agent).toBeDefined()
    expect(spawn!.input.properties.wait.default).toBe(true)
    expect(spawn!.execute).toBeInstanceOf(Function)

    expect(permissionHooks.map((hook) => hook.name)).toEqual(['evaluate'])
    expect(Object.keys(sessionHooks)).toEqual(['prompt'])
    expect(subscribeOptions.length).toBe(1)
  })

  test('prompt hook reformats channel blocks before admission', async () => {
    const { ctx, sessionHooks } = makeV2Ctx()
    await setupAvenor(ctx as never)
    const prompt = { text: '<channel source="agent-reviewer" from_run_id="abc123">status?</channel>' }
    await sessionHooks['prompt']({ prompt })
    expect(prompt.text).toContain('📨')
    expect(prompt.text).not.toContain('<channel ')
  })
})
