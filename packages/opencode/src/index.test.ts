import { describe, expect, test } from 'bun:test'

import plugin, { setupAvenor, toV2ToolResult } from './index.js'

function makeV2Ctx(options: { contextThrowsFirst?: boolean } = {}) {
  const added: Array<Record<string, any>> = []
  const permissionHooks: Array<{ name: string; callback: (event: any) => Promise<void> }> = []
  const sessionHooks: Record<string, (event: any) => Promise<void> | void> = {}
  const subscribeOptions: Array<Record<string, unknown>> = []
  const contextCalls: Array<Record<string, unknown>> = []
  const events: Array<Record<string, unknown>> = []
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
      context: async (input: Record<string, unknown>) => {
        contextCalls.push(input)
        if (options.contextThrowsFirst && contextCalls.length === 1) throw new Error('boom')
        return [{
          id: 'm1',
          type: 'assistant',
          content: [{ type: 'text', text: 'hello' }],
          time: { created: 1, completed: 2 },
        }]
      },
    },
    event: {
      subscribe: (opts?: Record<string, unknown>) => {
        subscribeOptions.push(opts ?? {})
        return (async function* () {
          for (const event of events) yield event
        })()
      },
    },
  }
  return { ctx, added, permissionHooks, sessionHooks, subscribeOptions, contextCalls, events }
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

  test('step.ended events reach the session context lookup; one failure does not stop the loop', async () => {
    const { ctx, contextCalls, events } = makeV2Ctx({ contextThrowsFirst: true })
    events.push(
      { type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } },
      { type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm2' } },
      { type: 'session.idle', data: { sessionID: 's1' } },
    )
    await setupAvenor(ctx as never)
    await new Promise(resolve => setTimeout(resolve, 20))

    expect(contextCalls.length).toBe(2)
    expect(contextCalls[0]).toEqual({ sessionID: 's1' })
  })

  test('cleanup aborts the event subscription', async () => {
    const { ctx, subscribeOptions } = makeV2Ctx()
    const cleanup = await setupAvenor(ctx as never)
    const signal = subscribeOptions[0]!.signal as AbortSignal
    expect(signal.aborted).toBe(false)
    await cleanup()
    expect(signal.aborted).toBe(true)
  })
})

describe('toV2ToolResult', () => {
  test('maps a string result onto text content', () => {
    expect(toV2ToolResult('done')).toEqual({ content: 'done', metadata: undefined })
  })

  test('folds the V1 title into metadata and keeps result metadata', () => {
    const result = toV2ToolResult({ title: 'run — done', output: 'final text', metadata: { run_id: 'r1' } })
    expect(result.metadata).toEqual({ run_id: 'r1', title: 'run — done' })
    expect(result.content).toEqual([{ type: 'text', text: 'final text' }])
  })

  test('maps V1 attachments onto V2 file content', () => {
    const result = toV2ToolResult({
      output: 'ok',
      attachments: [{ type: 'file', mime: 'image/png', url: 'data:image/png;base64,AAA', filename: 'pic.png' }],
    })
    expect(result.content).toEqual([
      { type: 'text', text: 'ok' },
      { type: 'file', uri: 'data:image/png;base64,AAA', mime: 'image/png', name: 'pic.png' },
    ])
  })
})
