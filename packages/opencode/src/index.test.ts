import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test'
import * as nodeChildProcess from 'node:child_process'
import { tool } from '@opencode-ai/plugin'

import plugin, { createV2EventHandler, evaluatePermissionAsk, permissionAskFromEvent, setupAvenor, stepEndedInfo, toV2ToolResult, v2Host, v2ToolEntry } from './index.js'
import { sanitizeSessionLabel, sessionLabel } from './label.js'

// The git reader's contract — resolve '' on any error or timeout, never
// reject — is what makes the once-per-session attempt mark safe, so the real
// reader is exercised through a mocked execFile: a callback invoked with an
// error must land as an empty answer, not a rejection.
const execFileMock = mock((_file: string, _args: string[], _options: unknown, callback: (error: Error | null, stdout: string) => void) => {
  callback(new Error('git missing'), '')
})
mock.module('node:child_process', () => ({ ...nodeChildProcess, execFile: execFileMock }))

function makeV2Ctx() {
  const added: Array<Record<string, any>> = []
  const permissionHooks: Array<{ name: string; callback: (event: any) => Promise<void> }> = []
  const sessionHooks: Record<string, (event: any) => Promise<void> | void> = {}
  const subscribeOptions: Array<Record<string, unknown>> = []
  const disposed: string[] = []
  const contextCalls: Array<Record<string, unknown>> = []
  const getCalls: Array<Record<string, unknown>> = []
  const promptCalls: Array<Record<string, unknown>> = []
  const events: Array<Record<string, unknown>> = []
  let drainedResolve: () => void = () => {}
  const drained = new Promise<void>(resolve => { drainedResolve = resolve })
  const registration = (name: string) => ({ dispose: async () => { disposed.push(name) } })
  const ctx = {
    location: { directory: '/plugin/location' },
    tool: {
      transform: async (callback: (editor: { add: (tool: Record<string, any>) => void }) => Promise<void>) => {
        await callback({ add: (tool) => added.push(tool) })
        return registration('tool')
      },
    },
    permission: {
      hook: async (name: string, callback: (event: any) => Promise<void>) => {
        permissionHooks.push({ name, callback })
        return registration('permission')
      },
    },
    session: {
      hook: async (name: string, callback: (event: any) => Promise<void> | void) => {
        sessionHooks[name] = callback
        return registration(`session.${name}`)
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
  return { ctx, added, permissionHooks, sessionHooks, subscribeOptions, contextCalls, getCalls, promptCalls, events, drained, disposed }
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
    // A V1-shaped context: the host wraps ctx.client lazily, so the hooks
    // object must build without touching it.
    const v1Ctx = {
      client: {
        session: {
          promptAsync: async () => undefined,
          message: async () => ({ data: undefined }),
        },
      },
    }
    const hooks = await (plugin as unknown as { server: (ctx: unknown) => Promise<Record<string, unknown>> }).server(v1Ctx)
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

    expect(added.map((tool) => tool.name).sort()).toEqual([
      'avenor_answer_permission', 'avenor_events', 'avenor_inspect', 'avenor_kill',
      'avenor_message', 'avenor_peers', 'avenor_result', 'avenor_shutdown',
      'avenor_spawn', 'avenor_status', 'avenor_workflow',
    ])
    const spawn = added.find((tool) => tool.name === 'avenor_spawn')!
    expect(spawn.input.type).toBe('object')
    expect(spawn.input.properties.agent).toBeDefined()
    expect(spawn.input.properties.wait.default).toBe(true)
    expect(spawn.execute).toBeInstanceOf(Function)

    expect(permissionHooks.map((hook) => hook.name)).toEqual(['evaluate'])
    expect(Object.keys(sessionHooks)).toEqual(['prompt'])
    expect(subscribeOptions.length).toBe(1)
  })

  test('cleanup disposes the registrations and aborts the subscription', async () => {
    const { ctx, subscribeOptions, disposed } = makeV2Ctx()
    const cleanup = await setupAvenor(ctx as never)
    expect(disposed).toEqual([])
    await cleanup()
    expect(disposed).toEqual(['tool', 'permission', 'session.prompt'])
    const signal = subscribeOptions[0]!.signal as AbortSignal
    expect(signal.aborted).toBe(true)
  })

  test('prompt hook reformats channel blocks before admission', async () => {
    const { ctx, sessionHooks } = makeV2Ctx()
    await setupAvenor(ctx as never)
    const prompt = { text: '<channel source="agent-reviewer" from_run_id="abc123">status?</channel>' }
    await sessionHooks['prompt']({ prompt })
    expect(prompt.text).toContain('📨')
    expect(prompt.text).not.toContain('<channel ')
  })

  test('a step.ended for an unbound session publishes nothing', async () => {
    const { ctx, contextCalls, promptCalls, events, drained } = makeV2Ctx()
    events.push(
      { type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } },
      { type: 'session.idle', data: { sessionID: 's1' } },
    )
    await setupAvenor(ctx as never)
    await drained
    // The settled message is read once, but with no bound conversation the
    // turn publishes nowhere, and an idle alone never contacts the supervisor.
    expect(contextCalls).toEqual([{ sessionID: 's1' }])
    expect(promptCalls).toEqual([])
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
})

describe('createV2EventHandler', () => {
  test('a throwing handler does not stop later events', async () => {
    const handled: string[] = []
    const onEvent = createV2EventHandler({
      onSessionAppeared: async () => {},
      onSessionRenamed: async () => {},
      onMessageUpdated: async () => {},
      onSessionIdle: () => { handled.push('idle'); throw new Error('idle boom') },
      onStepEnded: () => { handled.push('step'); throw new Error('step boom') },
    })
    await onEvent({ type: 'session.idle', data: { sessionID: 's1' } })
    await onEvent({ type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } })
    await onEvent({ type: 'session.idle', data: { sessionID: 's1' } })
    expect(handled).toEqual(['idle', 'step', 'idle'])
  })

  test('routes idle and step.ended onto the core handlers', async () => {    const calls: string[] = []
    const onEvent = createV2EventHandler({
      onSessionAppeared: async () => {},
      onSessionRenamed: async () => {},
      onMessageUpdated: async () => {},
      onSessionIdle: async (sessionID) => { calls.push(`idle:${sessionID}`) },
      onStepEnded: async (sessionID, messageID) => { calls.push(`step:${sessionID}:${messageID}`) },
    })
    await onEvent({ type: 'session.idle', data: { sessionID: 's1' } })
    await onEvent({ type: 'session.step.ended', data: { sessionID: 's1', assistantMessageID: 'm1' } })
    await onEvent({ type: 'other.event' })
    expect(calls).toEqual(['idle:s1', 'step:s1:m1'])
  })

  test('drops incomplete events without calling the core handlers', async () => {
    const calls: string[] = []
    const onEvent = createV2EventHandler({
      onSessionAppeared: async (sessionID) => { calls.push(`appeared:${sessionID}`) },
      onSessionRenamed: async () => {},
      onMessageUpdated: async () => {},
      onSessionIdle: async (sessionID) => { calls.push(`idle:${sessionID}`) },
      onStepEnded: async (sessionID, messageID) => { calls.push(`step:${sessionID}:${messageID}`) },
    })
    await onEvent({ type: 'session.idle', data: {} })
    await onEvent({ type: 'session.step.ended', data: { sessionID: 's1' } })
    await onEvent({ type: 'session.step.ended', data: { assistantMessageID: 'm1' } })
    await onEvent({ type: 'session.idle' })
    await onEvent({ type: 'session.created', data: {} })
    await onEvent({ type: 'session.execution.succeeded', data: {} })
    expect(calls).toEqual([])
  })

  test('routes rename, update and message events from the V2 data field', async () => {
    const calls: string[] = []
    const onEvent = createV2EventHandler({
      onSessionAppeared: async () => {},
      onSessionRenamed: async (sessionID, title) => { calls.push(`renamed:${sessionID}:${title}`) },
      onMessageUpdated: async (info) => { calls.push(`message:${String(info.id)}`) },
      onSessionIdle: async () => {},
      onStepEnded: async () => {},
    })
    await onEvent({ type: 'session.renamed', data: { sessionID: 's1', title: 'My Title' } })
    await onEvent({ type: 'session.updated', data: { info: { id: 's2', title: 'Other Title' } } })
    await onEvent({ type: 'message.updated', data: { info: { id: 'm1', role: 'assistant' } } })
    // The V2 domain carries the same fields under `data`, not `properties`, so
    // a wrong extraction field leaves every one of these unrouted.
    expect(calls).toEqual(['renamed:s1:My Title', 'renamed:s2:Other Title', 'message:m1'])
  })

  test('routes a created session with its directory and a turn start without one', async () => {
    const calls: Array<[string, string, string | undefined]> = []
    const onEvent = createV2EventHandler({
      onSessionAppeared: async (sessionID, directory) => { calls.push(['appeared', sessionID, directory]) },
      onSessionRenamed: async () => {},
      onMessageUpdated: async () => {},
      onSessionIdle: async () => {},
      onStepEnded: async () => {},
    })
    await onEvent({ type: 'session.created', data: { sessionID: 's1', location: { directory: '/repo/dir' } } })
    await onEvent({ type: 'session.execution.started', data: { sessionID: 's2' } })
    // The appearance handling is fire-and-forget, so the handlers run after
    // the pump has moved on; a macrotask tick drains their microtasks.
    await new Promise(resolve => setTimeout(resolve, 0))
    // The first turn is the reconcile for a plugin that loaded after the
    // session appeared, so it binds with no directory to derive a name from.
    expect(calls).toEqual([['appeared', 's1', '/repo/dir'], ['appeared', 's2', undefined]])
  })

  test('routes a successful execution onto the idle handler that restarts a feed', async () => {
    const calls: string[] = []
    const onEvent = createV2EventHandler({
      onSessionAppeared: async () => {},
      onSessionRenamed: async () => {},
      onMessageUpdated: async () => {},
      onSessionIdle: async (sessionID) => { calls.push(`idle:${sessionID}`) },
      onStepEnded: async () => {},
    })
    await onEvent({ type: 'session.execution.succeeded', data: { sessionID: 's1' } })
    // Unknown events reach no handler at all.
    await onEvent({ type: 'project.updated', data: { id: 'p1' } })
    await onEvent({ type: 'session.usage.updated', data: { sessionID: 's1' } })
    expect(calls).toEqual(['idle:s1'])
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

  test('retries the directory read after a failed lookup instead of caching the fallback', async () => {
    let calls = 0
    const ctx = {
      location: { directory: '/plugin/location' },
      session: { get: async () => {
        calls++
        if (calls === 1) throw new Error('gone')
        return { location: { directory: '/session/s1/dir' } }
      } },
    }
    const entry = v2ToolEntry('test_tool', definition, ctx as never, new Map())
    const toolContext = { sessionID: 's1', agent: 'a', messageID: 'm', signal: new AbortController().signal, progress: async () => {} }
    await entry.execute({}, toolContext)
    await entry.execute({}, toolContext)
    expect(calls).toBe(2)
    expect((definition as any).seenContext.directory).toBe('/session/s1/dir')
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
  test('promptAsync maps onto session.prompt with steer delivery and surfaces failures', async () => {
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
    await expect(host.promptAsync('s1', 'run finished')).rejects.toThrow('busy')
    await host.promptAsync('s1', 'run finished')
    expect(promptCalls).toEqual([
      { sessionID: 's1', text: 'run finished', delivery: 'steer' },
      { sessionID: 's1', text: 'run finished', delivery: 'steer' },
    ])
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

  test('getSessionTitle reports the title the session carries, and nothing when it cannot be read', async () => {
    const ctx = {
      location: { directory: '/x' },
      session: {
        prompt: async () => {},
        context: async () => [],
        get: async ({ sessionID }: { sessionID: string }) => {
          if (sessionID === 'broken') throw new Error('session gone')
          return sessionID === 'untitled' ? { title: 7 } : { title: 'my title' }
        },
      },
    }
    const host = v2Host(ctx as never)
    const readTitle = host.getSessionTitle
    if (!readTitle) throw new Error('the V2 host must expose a session title reader')
    expect(await readTitle('s1')).toBe('my title')
    expect(await readTitle('untitled')).toBeUndefined()
    // A session the adapter cannot read has no name to publish, so the caller
    // falls back instead of failing the notify.
    expect(await readTitle('broken')).toBeUndefined()
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

  test('forwards the summary flag so the publish gate can skip compaction summaries', () => {
    const plain = stepEndedInfo(assistantMessage('m1'), 's1', 'm1')!
    expect(plain.info.summary).toBe(false)
    const summary = stepEndedInfo({ ...assistantMessage('m1'), summary: true } as never, 's1', 'm1')!
    expect(summary.info.summary).toBe(true)
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

describe('sessionLabel', () => {
  const git = (answers: Record<string, string>) => async (_dir: string, args: string[]) => answers[args.join(' ')] ?? ''

  test('derives the repo base name and carries a non-default branch', async () => {
    const label = await sessionLabel('/repo', git({ 'rev-parse --show-toplevel': '/repo', 'branch --show-current': 'feat/561' }))
    expect(label).toBe('repo@feat-561')
  })

  test('omits the conventional default branch names', async () => {
    for (const branch of ['main', 'master']) {
      const label = await sessionLabel('/repo', git({ 'rev-parse --show-toplevel': '/repo', 'branch --show-current': branch }))
      expect(label).toBe('repo')
    }
  })

  test('falls back to the directory base name when git fails', async () => {
    const label = await sessionLabel('/home/me/avenor', git({}))
    expect(label).toBe('avenor')
  })

  test('yields nothing without a directory', async () => {
    expect(await sessionLabel(undefined, git({}))).toBe('')
  })

  test('falls back to the directory base name when the repo top level is a reserved reference', async () => {
    // A repo named `self` with an ordinary working directory yields the
    // directory's own base name, not nothing.
    const label = await sessionLabel('/repos/self/work', git({ 'rev-parse --show-toplevel': '/repos/self', 'branch --show-current': 'main' }))
    expect(label).toBe('work')
  })

  test('yields nothing for a reserved reference rather than a name the broker refuses', async () => {
    // Both the derived repo name and the basename fallback hit the exclusion.
    expect(await sessionLabel('/home/me/self', git({ 'rev-parse --show-toplevel': '/home/me/self', 'branch --show-current': 'main' }))).toBe('')
    expect(await sessionLabel('/home/me/self', git({}))).toBe('')
  })

  test('sanitizes separators and collapsing whitespace', async () => {
    expect(sanitizeSessionLabel('my repo\\dir name')).toBe('my-repo-dir-name')
    expect(sanitizeSessionLabel('--dashes--in--a--row--')).toBe('dashes-in-a-row')
    expect(sanitizeSessionLabel('a\u0000b')).toBe('ab')
  })

  test('truncates to 64 runes without splitting a surrogate pair', async () => {
    // 63 BMP runes plus one supplementary rune: 64 runes but 65 code units, so
    // a code-unit slice would cut the pair in two.
    const label = sanitizeSessionLabel('a'.repeat(63) + '𝄞' + '-tail')
    expect(label).toBe('a'.repeat(63) + '𝄞')
  })
})

describe('defaultGitReader', () => {
  beforeEach(() => {
    execFileMock.mockImplementation((_file: string, _args: string[], _options: unknown, callback: (error: Error | null, stdout: string) => void) => {
      callback(new Error('git missing'), '')
    })
  })

  afterEach(() => {
    execFileMock.mockClear()
  })

  test('resolves an empty label when git errors, never rejecting', async () => {
    // An error falls through to the directory base name; nothing here may
    // reject, or the once-per-session attempt would leave a bound conversation
    // unnamed forever.
    await expect(sessionLabel('/repo/avenor')).resolves.toBe('avenor')
  })

  test('resolves an empty label when git is killed by its timeout', async () => {
    execFileMock.mockImplementation((_file: string, _args: string[], options: unknown, callback: (error: Error | null, stdout: string) => void) => {
      // The reader bounds each git call at five seconds.
      expect((options as { timeout?: number }).timeout).toBe(5000)
      callback(Object.assign(new Error('command failed'), { killed: true, signal: 'SIGTERM' }), '')
    })
    await expect(sessionLabel('/repo/avenor')).resolves.toBe('avenor')
  })
})
