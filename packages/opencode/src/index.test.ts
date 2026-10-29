import { describe, expect, test } from 'bun:test'
import { tool } from '@opencode-ai/plugin'

import plugin, { createV2EventHandler, evaluatePermissionAsk, permissionAskFromEvent, setupAvenor, stepEndedInfo, toV2ToolResult, v2Host, v2ToolEntry } from './index.js'

function makeV2Ctx() {
  const added: Array<Record<string, any>> = []
  const permissionHooks: Array<{ name: string; callback: (event: any) => Promise<void> }> = []
  const sessionHooks: Record<string, (event: any) => Promise<void> | void> = {}
  const subscribeOptions: Array<Record<string, unknown>> = []
  const contextCalls: Array<Record<string, unknown>> = []
  const getCalls: Array<Record<string, unknown>> = []
  const promptCalls: Array<Record<string, unknown>> = []
  const events: Array<Record<string, unknown>> = []
  let drainedResolve: () => void = () => {}
  const drained = new Promise<void>(resolve => { drainedResolve = resolve })
  const ctx = {
    location: { directory: '/plugin/location' },
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
      get: async (input: Record<string, unknown>) => {
        getCalls.push(input)
        return { location: { directory: `/session/${input.sessionID}/dir` } }
      },
      context: async (input: Record<string, unknown>) => {
        contextCalls.push(input)
        return [{
          id: 'm1',
          type: 'assistant',
          content: [{ type: 'text', text: 'hello' }],
          time: { created: 1, completed: 2 },
        }]
      },
      prompt: async (input: Record<string, unknown>) => {
        promptCalls.push(input)
      },
    },
    event: {
      subscribe: (opts?: Record<string, unknown>) => {
        subscribeOptions.push(opts ?? {})
        return (async function* () {
          try {
            for (const event of events) yield event
          } finally {
            drainedResolve()
          }
        })()
      },
    },
  }
  return { ctx, added, permissionHooks, sessionHooks, subscribeOptions, contextCalls, getCalls, promptCalls, events, drained }
}

const assistantMessage = (id: string) => ({
  id,
  type: 'assistant',
  content: [{ type: 'text', text: `text of ${id}` }],
  time: { created: 1, completed: 2 },
})

describe('dual entrypoint', () => {
  test('default export carries the V2 definition and a working V1 server hook', async () => {
    expect(plugin.id).toBe('avenor')
    expect(typeof plugin.setup).toBe('function')
    const server = (plugin as unknown as { server: (ctx: unknown) => Promise<Record<string, unknown>> }).server
    const hooks = await server({})
    expect(typeof hooks.event).toBe('function')
    expect(typeof hooks['permission.ask']).toBe('function')
    expect(typeof hooks['chat.message']).toBe('function')
    expect(typeof hooks['experimental.text.complete']).toBe('function')
    expect(typeof hooks.tool).toBe('object')
    expect(Object.keys(hooks.tool as Record<string, unknown>).length).toBeGreaterThan(10)
  })
})

describe('setupAvenor', () => {
  test('registers every tool with a JSON Schema input and the domain hooks', async () => {
    const { ctx, added, permissionHooks, sessionHooks, subscribeOptions } = makeV2Ctx()
    const cleanup = await setupAvenor(ctx as never)
    expect(typeof cleanup).toBe('function')

    expect(added.length).toBeGreaterThan(10)
    const spawn = added.find((tool) => tool.name === 'avenor_spawn')!
    expect(spawn.input.type).toBe('object')
    expect(spawn.input.properties.agent).toBeDefined()
    expect(spawn.input.properties.wait.default).toBe(true)
    expect(spawn.execute).toBeInstanceOf(Function)

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

  test('step.ended for an unbound session does not read the session context', async () => {
    const { ctx, contextCalls, events, drained } = makeV2Ctx()
    events.push(
      { type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } },
      { type: 'session.idle', data: { sessionID: 's1' } },
    )
    await setupAvenor(ctx as never)
    await drained
    expect(contextCalls).toEqual([])
  })

  test('an event stream that throws does not break setup or cleanup', async () => {
    const ctx = {
      location: { directory: '/plugin/location' },
      tool: { transform: async (cb: (e: { add: (t: unknown) => void }) => Promise<void>) => cb({ add: () => {} }) },
      permission: { hook: async () => {} },
      session: { hook: async () => {} },
      event: {
        subscribe: () => {
          // The pump is fire-and-forget; a failing stream must not produce an
          // unhandled rejection or block cleanup.
          return { [Symbol.asyncIterator]() { return { next: () => Promise.reject(new Error('stream gone')) } } }
        },
      },
    }
    const cleanup = await setupAvenor(ctx as never)
    await new Promise(resolve => setTimeout(resolve, 10))
    await expect(Promise.resolve(cleanup())).resolves.toBeUndefined()
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

describe('createV2EventHandler', () => {
  test('a throwing handler does not stop later events', async () => {
    const handled: string[] = []
    const onEvent = createV2EventHandler({
      onSessionIdle: () => { handled.push('idle'); throw new Error('idle boom') },
      onStepEnded: () => { handled.push('step'); throw new Error('step boom') },
    })
    await onEvent({ type: 'session.idle', data: { sessionID: 's1' } })
    await onEvent({ type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } })
    await onEvent({ type: 'session.idle', data: { sessionID: 's1' } })
    expect(handled).toEqual(['idle', 'step', 'idle'])
  })

  test('routes idle and step.ended onto the core handlers', async () => {
    const calls: string[] = []
    const onEvent = createV2EventHandler({
      onSessionIdle: async (sessionID) => { calls.push(`idle:${sessionID}`) },
      onStepEnded: async (sessionID, messageID) => { calls.push(`step:${sessionID}:${messageID}`) },
    })
    await onEvent({ type: 'session.idle', data: { sessionID: 's1' } })
    await onEvent({ type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } })
    await onEvent({ type: 'other.event' })
    expect(calls).toEqual(['idle:s1', 'step:s1:m1'])
  })
})

describe('v2ToolEntry', () => {
  const makeCtx = () => {
    const progressUpdates: Array<Record<string, unknown>> = []
    const signal = new AbortController().signal
    const ctx = {
      location: { directory: '/plugin/location' },
      session: {
        get: async ({ sessionID }: { sessionID: string }) => ({ location: { directory: `/session/${sessionID}/dir` } }),
      },
    }
    const toolContext = { sessionID: 's1', agent: 'build', messageID: 'msg1', signal, progress: async (u: Record<string, unknown>) => { progressUpdates.push(u) } }
    return { ctx, toolContext, progressUpdates, signal }
  }

  const definition = {
    description: 'test tool',
    args: { filePath: tool.schema.string().describe('a path') },
    execute: async (input: Record<string, unknown>, context: Record<string, unknown>) => {
      ;(definition as any).seenContext = context
      return { title: 'done', output: `ran ${String(input.filePath)}`, metadata: { run_id: 'r1' } }
    },
  }

  test('builds the JSON Schema input and maps the tool context and result', async () => {
    const { ctx, toolContext, progressUpdates, signal } = makeCtx()
    const entry = v2ToolEntry('test_tool', definition, ctx as never)
    expect(entry.name).toBe('test_tool')
    expect(entry.description).toBe('test tool')
    const schema = entry.input as { type: string; properties: Record<string, unknown> }
    expect(schema.type).toBe('object')
    expect(schema.properties.filePath).toBeDefined()

    const result = await entry.execute({ filePath: 'x' }, toolContext)
    const seen = (definition as any).seenContext
    expect(seen.sessionID).toBe('s1')
    expect(seen.agent).toBe('build')
    expect(seen.messageID).toBe('msg1')
    expect(seen.abort).toBe(signal)
    expect(seen.directory).toBe('/session/s1/dir')
    expect(seen.worktree).toBe('/session/s1/dir')

    await seen.metadata({ title: 'starting' })
    expect(progressUpdates).toEqual([{ title: 'starting' }])

    expect(result).toEqual({
      content: [{ type: 'text', text: 'ran x' }],
      metadata: { run_id: 'r1', title: 'done' },
    })
  })

  test('falls back to the plugin location when the session lookup fails', async () => {
    const ctx = {
      location: { directory: '/plugin/location' },
      session: { get: async () => { throw new Error('gone') } },
    }
    const entry = v2ToolEntry('test_tool', definition, ctx as never)
    await entry.execute({}, { sessionID: 's1', agent: 'a', messageID: 'm', signal: new AbortController().signal, progress: async () => {} })
    expect((definition as any).seenContext.directory).toBe('/plugin/location')
  })

  test('applies JSON Schema defaults before execute', async () => {
    const { ctx } = makeV2Ctx()
    const def = {
      description: 'waited',
      args: { wait: tool.schema.boolean().default(true) },
      execute: async (input: Record<string, unknown>) => ({ output: `wait=${String(input.wait)}` }),
    }
    const entry = v2ToolEntry('waited', def, ctx as never, new Map())
    const result = await entry.execute({}, { sessionID: 's1', agent: 'a', messageID: 'm', signal: new AbortController().signal, progress: async () => {} })
    expect(result).toEqual({ content: [{ type: 'text', text: 'wait=true' }], metadata: {} })
  })

  test('one shared directory cache serves every tool of the instance', async () => {
    const { ctx, getCalls } = makeV2Ctx()
    const directories = new Map<string, string>()
    const first = v2ToolEntry('tool_a', definition, ctx as never, directories)
    const second = v2ToolEntry('tool_b', definition, ctx as never, directories)
    const toolContext = { sessionID: 's1', agent: 'a', messageID: 'm', signal: new AbortController().signal, progress: async () => {} }
    await first.execute({}, toolContext)
    await second.execute({}, toolContext)
    expect(getCalls).toEqual([{ sessionID: 's1' }])
    expect((definition as any).seenContext.directory).toBe('/session/s1/dir')
  })
})

describe('v2Host', () => {
  test('promptAsync maps onto session.prompt with steer delivery and swallows failures', async () => {
    const promptCalls: Array<Record<string, unknown>> = []
    const ctx = {
      location: { directory: '/x' },
      session: {
        prompt: async (input: Record<string, unknown>) => {
          promptCalls.push(input)
          if (promptCalls.length === 1) throw new Error('busy')
        },
        context: async () => [],
      },
    }
    const host = v2Host(ctx as never)
    await host.promptAsync('s1', 'run finished')
    expect(promptCalls).toEqual([{ sessionID: 's1', text: 'run finished', delivery: 'steer' }])
  })

  test('getMessage finds the message by id in the session context', async () => {
    const ctx = {
      location: { directory: '/x' },
      session: {
        prompt: async () => {},
        context: async () => [{ id: 'm1' }, { id: 'm2', type: 'assistant', content: [{ type: 'text', text: 'hi' }] }],
      },
    }
    const host = v2Host(ctx as never)
    expect(await host.getMessage('s1', 'm2')).toEqual({ id: 'm2', type: 'assistant', content: [{ type: 'text', text: 'hi' }] } as never)
    expect(await host.getMessage('s1', 'missing')).toBeUndefined()
  })
})

describe('permissionAskFromEvent', () => {
  test('maps the V2 evaluate event onto the V1 permission-ask shape without the tool-call id', () => {
    expect(permissionAskFromEvent({
      sessionID: 's1',
      action: 'shell',
      resources: ['git push *'],
      source: { type: 'tool', id: 'call_1' },
    })).toEqual({ sessionID: 's1', type: 'shell', pattern: ['git push *'], id: undefined })
  })

  test('leaves the id unset when the source is not a tool call', () => {
    const ask = permissionAskFromEvent({ sessionID: 's1', action: 'edit', resources: [], source: undefined })
    expect(ask.id).toBeUndefined()
    expect(ask.pattern).toEqual([])
  })

  test('the evaluate hook forwards only ask decisions', async () => {
    const routed: Array<Record<string, unknown>> = []
    const onAsk = evaluatePermissionAsk({ onPermissionAsk: async (ask) => { routed.push(ask) } })
    await onAsk({ sessionID: 's1', action: 'shell', resources: [], effect: 'ask', source: { type: 'tool', id: 'c1' } })
    await onAsk({ sessionID: 's1', action: 'edit', resources: [], effect: 'allow' })
    await onAsk({ sessionID: 's1', action: 'edit', resources: [], effect: 'deny' })
    expect(routed).toEqual([
      { sessionID: 's1', type: 'shell', pattern: [], id: undefined },
    ])
  })
})

describe('stepEndedInfo', () => {
  test('projects an assistant message onto the V1 info shape with its text', () => {
    const settled = stepEndedInfo(
      assistantMessage('m1'),
      's1',
      'm1',
    )!
    expect(settled.info).toEqual({
      role: 'assistant',
      sessionID: 's1',
      id: 'm1',
      summary: false,
      time: { created: 1, completed: 2 },
      finish: undefined,
    })
    expect(settled.settledText).toEqual(['text of m1'])
  })

  test('maps a finish reason and an error type onto the info shape', () => {
    const settled = stepEndedInfo({
      content: [{ type: 'text', text: 'hi' }],
      time: { created: 1, completed: 2 },
      finish: 'tool-calls',
      error: { type: 'aborted' },
    } as never, 's1', 'm1')!
    expect(settled.info.finish).toBe('tool-calls')
    expect(settled.info.error).toEqual({ name: 'MessageAbortedError' })
    const capped = stepEndedInfo({
      content: [], time: { created: 1, completed: 2 }, error: { type: 'output_length_exceeded' },
    } as never, 's1', 'm1')!
    expect(capped.info.error).toEqual({ name: 'MessageOutputLengthError' })
    const other = stepEndedInfo({
      content: [], time: { created: 1, completed: 2 }, error: { type: 'rate_limit' },
    } as never, 's1', 'm1')!
    expect(other.info.error).toEqual({ name: 'rate_limit' })
  })

  test('is undefined when the message has not completed', () => {
    expect(stepEndedInfo({ content: [], time: { created: 1 } } as never, 's1', 'm1')).toBeUndefined()
    expect(stepEndedInfo(undefined, 's1', 'm1')).toBeUndefined()
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

  test('emits no title key when the result has none', () => {
    const result = toV2ToolResult({ output: 'ok' })
    expect(result.metadata).toEqual({})
    expect('title' in (result.metadata ?? {})).toBe(false)
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
