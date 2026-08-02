import { afterEach, beforeEach, describe, expect, it, jest, mock } from 'bun:test'

import { observeRun } from '../../core/src/run-observer.js'
import { createRunSnapshot } from '../../core/src/run-reducer.js'
import { extractEventText } from '../../core/src/run-events.js'

const spawnToolMock = mock(async () => ({
  run_id: 'run-1',
  runtime_id: 'rt-1',
  label: 'test run',
  supervisor_id: '/tmp/avenor.sock',
}))
const statusToolMock = mock(async () => ({
  run_id: 'run-1',
  runtime_id: 'rt-1',
  session_id: 'child-session',
  status: 'running',
  latest_seq: 0,
}))
const eventsToolMock = mock(async () => ({ events: [] }))
const answerPermissionToolMock = mock(async () => ({ ok: true }))
const followUpToolMock = mock(async () => ({ run_id: 'run-2', label: 'test follow-up' }))
const resultToolMock = mock(async () => ({ run_id: 'run-1', label: 'test run', status: 'done', ready: true, output: 'final answer' }))
const shutdownToolMock = mock(async () => ({ ok: true }))
const inspectToolMock = mock(async () => {
  const snapshot = createRunSnapshot()
  snapshot.identity = { runtime_id: 'rt-1', session_id: 'child-session', run_id: 'run-1', run_label: 'test run' }
  snapshot.phase = 'done'
  snapshot.latest_seq = 5
  snapshot.ended = true
  snapshot.final_output = 'final answer'
  snapshot.transcript = [{ kind: 'assistant', event: 'agent.message_chunk', source_event: 'agent.message_chunk', text: 'final answer' }]
  return {
    run_id: 'run-1',
    label: 'test run',
    status: {
      run_id: 'run-1',
      label: 'test run',
      status: 'done',
      runtime_id: 'rt-1',
      session_id: 'child-session',
      latest_seq: 5,
      final_output: 'final answer',
    },
    snapshot,
    transcript: snapshot.transcript,
    tools: [],
    live_tools: [],
    permissions: [],
    final_output: 'final answer',
  }
})

function makeEventClient(events: unknown[], options: { hangAfterExhausted?: boolean; errorAfterExhausted?: Error } = {}) {
  let index = 0
  let closed = false
  let pendingResolve: ((result: IteratorResult<any>) => void) | null = null
  const calls: Array<Record<string, unknown> | undefined> = []

  const returnMock = mock(async () => {
    closed = true
    pendingResolve?.({ value: undefined, done: true })
    pendingResolve = null
    return { value: undefined, done: true as const }
  })

  const iterator = {
    async next() {
      if (closed) {
        return { value: undefined, done: true as const }
      }
      if (index < events.length) {
        return { value: events[index++], done: false as const }
      }
      if (options.errorAfterExhausted) {
        throw options.errorAfterExhausted
      }
      if (!options.hangAfterExhausted) {
        closed = true
        return { value: undefined, done: true as const }
      }
      return new Promise<IteratorResult<any>>((resolve) => {
        pendingResolve = resolve
      })
    },
    return: returnMock,
    [Symbol.asyncIterator]() {
      return this
    },
  }

  const eventsMock = mock((streamOptions?: Record<string, unknown>) => {
    calls.push(streamOptions)
    return iterator
  })
  const closeMock = mock(() => {
    closed = true
    pendingResolve?.({ value: undefined, done: true })
    pendingResolve = null
  })

  return {
    client: { events: eventsMock, close: closeMock },
    calls,
    eventsMock,
    closeMock,
    returnMock,
  }
}

const dialQueue: Array<ReturnType<typeof makeEventClient>> = []
let dialOverride: (() => Promise<any>) | undefined
const dialMock = mock(async () => {
  if (dialOverride) return dialOverride()
  const next = dialQueue.shift()
  if (!next) {
    throw new Error('unexpected dial')
  }
  return next.client as any
})

const currentSupervisorMock = mock(() => ({ supervisorId: '/tmp/avenor.sock' }))

mock.module('@dougbots/avenor-core', () => ({
  spawnTool: spawnToolMock,
  statusTool: statusToolMock,
  eventsTool: eventsToolMock,
  answerPermissionTool: answerPermissionToolMock,
  followUpTool: followUpToolMock,
  resultTool: resultToolMock,
  shutdownTool: shutdownToolMock,
  inspectTool: inspectToolMock,
  createRunSnapshot,
  extractEventText,
  observeRun,
  dial: dialMock,
  Supervisor: {
    currentInstance: currentSupervisorMock,
  },
}))

const { AvenorPlugin, buildWaitingText, terminalStatus } = await import('./plugin.js')
const originalFetch = globalThis.fetch

function makeClient(promptAsync = mock(async () => {})) {
  return {
    session: {
      promptAsync,
    },
  }
}

function makeCtx(overrides: Record<string, unknown> = {}) {
  return {
    client: makeClient(),
    project: {},
    directory: '/tmp/test',
    worktree: '/tmp/test',
    serverUrl: new URL('http://localhost:4096'),
    $: {} as any,
    experimental_workspace: { register: () => {} },
    ...overrides,
  }
}

function makeSnapshot(overrides: Record<string, any> = {}) {
  const snapshot = createRunSnapshot()
  return {
    ...snapshot,
    ...overrides,
    identity: {
      ...snapshot.identity,
      ...(overrides.identity ?? {}),
    },
  }
}

async function flush(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 0))
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 8; index++) {
    await Promise.resolve()
  }
}

describe('AvenorPlugin', () => {
  beforeEach(() => {
    spawnToolMock.mockClear()
    statusToolMock.mockClear()
    eventsToolMock.mockClear()
    answerPermissionToolMock.mockClear()
    followUpToolMock.mockClear()
    resultToolMock.mockClear()
    shutdownToolMock.mockClear()
    inspectToolMock.mockClear()
    dialMock.mockClear()
    currentSupervisorMock.mockClear()
    dialQueue.length = 0
    dialOverride = undefined

    spawnToolMock.mockImplementation(async () => ({
      run_id: 'run-1',
      runtime_id: 'rt-1',
      label: 'test run',
      supervisor_id: '/tmp/avenor.sock',
    }))
    statusToolMock.mockImplementation(async () => ({
      run_id: 'run-1',
      runtime_id: 'rt-1',
      session_id: 'child-session',
      status: 'running',
      latest_seq: 0,
    }))
    resultToolMock.mockImplementation(async () => ({
      run_id: 'run-1',
      label: 'test run',
      status: 'done',
      ready: true,
      output: 'final answer',
    }))
    inspectToolMock.mockImplementation(async () => ({
      run_id: 'run-1',
      label: 'test run',
      status: {
        run_id: 'run-1',
        label: 'test run',
        status: 'done',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        latest_seq: 5,
        final_output: 'final answer',
      },
      snapshot: makeSnapshot({
        identity: { runtime_id: 'rt-1', session_id: 'child-session', run_id: 'run-1', run_label: 'test run' },
        phase: 'done',
        latest_seq: 5,
        ended: true,
        final_output: 'final answer',
        transcript: [{ kind: 'assistant', event: 'agent.message_chunk', source_event: 'agent.message_chunk', text: 'final answer' }],
      }),
      transcript: [{ kind: 'assistant', event: 'agent.message_chunk', source_event: 'agent.message_chunk', text: 'final answer' }],
      tools: [],
      live_tools: [],
      permissions: [],
      final_output: 'final answer',
    }))
  })

  afterEach(() => {
    dialQueue.length = 0
    jest.useRealTimers()
    globalThis.fetch = originalFetch
  })

  it('derives terminal status from fallback, phase, stop reason, and defaults', () => {
    expect(terminalStatus(makeSnapshot({ phase: 'done' }))).toBe('done')
    expect(terminalStatus(makeSnapshot({ phase: 'running', stop_reason: 'timeout' }))).toBe('timeout')
    expect(terminalStatus(makeSnapshot(), { run_id: 'run-1', label: 'run', status: 'failed' })).toBe('failed')
    expect(terminalStatus(makeSnapshot())).toBe('done')
  })

  it('formats permission and question waiting paths without redundant guidance', () => {
    const run = {
      runId: 'run-1',
      runtimeId: 'rt-1',
      agent: 'horse',
      orchestratorSessionId: 'parent',
      label: 'worker',
      monitoring: false,
    }
    const permissionText = buildWaitingText(run, makeSnapshot({
      pending_permission: { request_id: 'req-1', question: 'Allow access?', options: [] },
    }))
    expect(permissionText).toContain('Permission request: Allow access?')
    expect(permissionText).toContain('avenor_answer_permission')
    expect(permissionText).not.toContain('avenor_follow_up')

    const questionText = buildWaitingText(run, makeSnapshot({ phase_label: 'Which file?' }))
    expect(questionText).toContain('Question: Which file?')
    expect(questionText).toContain('avenor_follow_up')
  })

  it('registers existing hooks plus avenor_inspect and formats channel messages', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)

    expect(typeof hooks.event).toBe('function')
    expect(typeof hooks['permission.ask']).toBe('function')
    expect(typeof hooks['chat.message']).toBe('function')
    expect(typeof hooks['experimental.text.complete']).toBe('function')
    expect(Object.keys(hooks.tool ?? {})).toEqual(expect.arrayContaining([
      'avenor_spawn',
      'avenor_status',
      'avenor_result',
      'avenor_answer_permission',
      'avenor_follow_up',
      'avenor_events',
      'avenor_inspect',
      'avenor_shutdown',
    ]))

    const message = {
      parts: [{ type: 'text', text: '<channel source="agent-reviewer" from_run_id="abc12345" from_role="reviewer">Looks good</channel>' }],
    }
    await hooks['chat.message']?.(message as any, {} as any)
    expect(message.parts[0]?.text).toContain('📨 reviewer (abc12345)')

    const output = { text: '<channel source="agent-reviewer" from_run_id="abc12345" from_role="reviewer">Looks good</channel>' }
    await hooks['experimental.text.complete']?.({ sessionID: 'session-1', messageID: 'm', partID: 'p' }, output)
    expect(output.text).toContain('📨 reviewer (abc12345)')
  })

  it('passes lifecycle and result retrieval arguments through to core tools', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    const context = { sessionID: 'orchestrator-session', directory: '/tmp/test', abort: new AbortController().signal }

    await (hooks.tool?.avenor_status as any).execute(
      { run_id: 'run-1', view: 'lifecycle', supervisor_id: '/tmp/avenor.sock' },
      context,
    )
    expect(statusToolMock).toHaveBeenCalledWith({
      runId: 'run-1',
      supervisorId: '/tmp/avenor.sock',
      view: 'lifecycle',
    })

    const output = await (hooks.tool?.avenor_result as any).execute(
      { run_id: 'run-1', timeout: '5m', supervisor_id: '/tmp/avenor.sock' },
      context,
    )
    expect(output).toEqual(expect.objectContaining({
      title: 'test run — done',
      output: expect.stringContaining('Final output:\nfinal answer'),
      metadata: expect.objectContaining({ output: 'final answer' }),
    }))
    expect(resultToolMock).toHaveBeenCalledWith({
      runId: 'run-1',
      supervisorId: '/tmp/avenor.sock',
      wait: undefined,
      timeout: '5m',
      signal: context.abort,
    })
  })

  it('reports observer setup failures without rejecting or losing the tracked run', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    const result = await (hooks.tool?.avenor_spawn as any).execute(
      { agent: 'jockey', thinking: 'medium', wait: true },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({ thinking: 'medium' })
    expect(result.title).toBe('test run — monitoring error')
    expect(result.output).toContain('unexpected dial')
    expect(result.output).toContain('may still be active')
    expect(result.metadata).toEqual({ run_id: 'run-1' })
  })

  it('reports observer stream failures after setup and closes the client', async () => {
    const stream = makeEventClient([], { errorAfterExhausted: new Error('stream failed') })
    dialQueue.push(stream)
    const hooks = await AvenorPlugin(makeCtx() as any)

    const result = await (hooks.tool?.avenor_spawn as any).execute(
      { agent: 'jockey', wait: true },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    expect(result.title).toBe('test run — monitoring error')
    expect(result.output).toContain('stream failed')
    expect(stream.closeMock).toHaveBeenCalledTimes(1)
  })

  it('returns an aborted outcome when blocking observation is cancelled', async () => {
    const stream = makeEventClient([], { hangAfterExhausted: true })
    dialQueue.push(stream)
    const controller = new AbortController()
    const hooks = await AvenorPlugin(makeCtx() as any)

    const resultPromise = (hooks.tool?.avenor_spawn as any).execute(
      { agent: 'jockey', wait: true },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: controller.signal,
        metadata: () => {},
      },
    )
    await flush()
    controller.abort()
    const result = await resultPromise

    expect(result.title).toBe('test run — aborted')
    expect(result.output).toContain('may still be active')
    expect(stream.returnMock).toHaveBeenCalledTimes(1)
    expect(stream.closeMock).toHaveBeenCalledTimes(1)
  })

  it('honors an abort that arrives while the observer is being created', async () => {
    const stream = makeEventClient([], { hangAfterExhausted: true })
    let releaseDial: ((client: any) => void) | undefined
    dialOverride = () => new Promise((resolve) => {
      releaseDial = resolve
    })
    const controller = new AbortController()
    const hooks = await AvenorPlugin(makeCtx() as any)

    const resultPromise = (hooks.tool?.avenor_spawn as any).execute(
      { agent: 'jockey', wait: true },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: controller.signal,
        metadata: () => {},
      },
    )
    await flushMicrotasks()
    controller.abort()
    releaseDial?.(stream.client)
    const result = await resultPromise

    expect(result.title).toBe('test run — aborted')
    expect(stream.returnMock).toHaveBeenCalledTimes(1)
    expect(stream.closeMock).toHaveBeenCalledTimes(1)
  })

  it('uses the shared observer for blocking metadata and final output without raw event polling', async () => {
    const stream = makeEventClient([
      { event: 'session.start', runtime_id: 'rt-1', session_id: 'child-session', seq: 1 },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'Hel' },
      { event: 'avenor.message.delta', runtime_id: 'rt-1', seq: 3, text: 'lo' },
      { event: 'avenor.tool.start', runtime_id: 'rt-1', seq: 4, id: 'tool-1', title: 'bash', input: 'echo hi' },
      { event: 'session.end', runtime_id: 'rt-1', seq: 5, final_output: 'Hello world', stop_reason: 'end_turn' },
    ])
    dialQueue.push(stream)

    inspectToolMock.mockImplementationOnce(async () => ({
      run_id: 'run-1',
      label: 'test run',
      status: {
        run_id: 'run-1',
        label: 'test run',
        status: 'done',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        latest_seq: 5,
        final_output: 'Hello world',
      },
      snapshot: makeSnapshot({
        identity: { runtime_id: 'rt-1', session_id: 'child-session', run_id: 'run-1', run_label: 'test run' },
        phase: 'done',
        latest_seq: 5,
        ended: true,
        final_output: 'Hello world',
        transcript: [{ kind: 'assistant', event: 'agent.message_chunk', source_event: 'agent.message_chunk', text: 'Hello world' }],
      }),
      transcript: [{ kind: 'assistant', event: 'agent.message_chunk', source_event: 'agent.message_chunk', text: 'Hello world' }],
      tools: [],
      live_tools: [],
      permissions: [],
      final_output: 'Hello world',
    }))

    const metadata = mock(() => {})
    const hooks = await AvenorPlugin(makeCtx() as any)
    const result = await (hooks.tool?.avenor_spawn as any).execute(
      { agent: 'jockey', wait: true },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata,
      },
    )

    expect(stream.calls[0]).toEqual({ runtime_id: 'rt-1', replay: true })
    expect(metadata).toHaveBeenCalledWith(expect.objectContaining({
      metadata: expect.objectContaining({
        latest_seq: 4,
        live_tool: expect.stringContaining('bash'),
        transcript_preview: expect.stringContaining('assistant: Hello'),
      }),
    }))
    expect(result.output).toContain('Final output:')
    expect(result.output).toContain('Hello world')
    expect(result.metadata).toEqual(expect.objectContaining({
      run_id: 'run-1',
      session_id: 'child-session',
      final_output: 'Hello world',
      latest_seq: 5,
    }))
    expect(eventsToolMock).not.toHaveBeenCalled()
    expect(stream.returnMock).toHaveBeenCalledTimes(1)
    expect(stream.closeMock).toHaveBeenCalledTimes(1)
  })

  it('stops on waiting, restarts after idle with after_seq, and prompts on completion', async () => {
    const waitingStream = makeEventClient([
      { event: 'session.start', runtime_id: 'rt-1', session_id: 'child-session', seq: 1 },
      {
        event: 'permission.request',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        seq: 2,
        request_id: 'perm-1',
        description: 'read /outside/project',
        options: [],
      },
    ])
    const completionStream = makeEventClient([
      { event: 'permission.response', runtime_id: 'rt-1', session_id: 'child-session', seq: 3, request_id: 'perm-1', kind: 'allow_once' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 4, delta: 'done' },
      { event: 'session.end', runtime_id: 'rt-1', seq: 5, final_output: 'final answer', stop_reason: 'end_turn' },
    ])
    dialQueue.push(waitingStream, completionStream)

    const promptAsync = mock(async () => {})
    const hooks = await AvenorPlugin(makeCtx({ client: makeClient(promptAsync) }) as any)
    const spawn = hooks.tool?.avenor_spawn as any

    await spawn.execute(
      { agent: 'jockey', wait: false },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    await flush()
    expect(promptAsync).toHaveBeenCalledTimes(1)
    expect(promptAsync.mock.calls[0]?.[0]).toEqual({
      path: { id: 'orchestrator-session' },
      body: {
        parts: [{
          type: 'text',
          text: expect.stringContaining('Permission request: read /outside/project'),
        }],
      },
    })

    await hooks.event?.({
      event: { type: 'session.idle', properties: { sessionID: 'orchestrator-session' } } as any,
    })
    await flush()

    expect(completionStream.calls[0]).toEqual({ runtime_id: 'rt-1', replay: true, after_seq: 2 })
    expect(promptAsync).toHaveBeenCalledTimes(2)
    expect(promptAsync.mock.calls[1]?.[0]).toEqual({
      path: { id: 'orchestrator-session' },
      body: {
        parts: [{
          type: 'text',
          text: expect.stringContaining('Final output:'),
        }],
      },
    })
    expect(waitingStream.returnMock).toHaveBeenCalledTimes(1)
    expect(waitingStream.closeMock).toHaveBeenCalledTimes(1)
    expect(completionStream.returnMock).toHaveBeenCalledTimes(1)
    expect(completionStream.closeMock).toHaveBeenCalledTimes(1)
  })

  it('routes permission.ask via session mapping, closes the observer, and resumes after idle', async () => {
    statusToolMock
      .mockImplementationOnce(async () => ({
        run_id: 'run-1',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        status: 'running',
        latest_seq: 6,
      }))
      .mockImplementationOnce(async () => ({
        run_id: 'run-1',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        status: 'waiting',
        latest_seq: 7,
        pending_permission: {
          request_id: 'perm-1',
          description: 'read /outside/project',
          options: [],
        },
      }))

    const hangingStream = makeEventClient([], { hangAfterExhausted: true })
    const completionStream = makeEventClient([
      { event: 'session.end', runtime_id: 'rt-1', session_id: 'child-session', seq: 8, final_output: 'routed final', stop_reason: 'end_turn' },
    ])
    dialQueue.push(hangingStream, completionStream)

    inspectToolMock.mockImplementationOnce(async () => ({
      run_id: 'run-1',
      label: 'test run',
      status: {
        run_id: 'run-1',
        label: 'test run',
        status: 'done',
        runtime_id: 'rt-1',
        session_id: 'child-session',
        latest_seq: 8,
        final_output: 'routed final',
      },
      snapshot: makeSnapshot({
        identity: { runtime_id: 'rt-1', session_id: 'child-session', run_id: 'run-1', run_label: 'test run' },
        phase: 'done',
        latest_seq: 8,
        ended: true,
        final_output: 'routed final',
      }),
      transcript: [],
      tools: [],
      live_tools: [],
      permissions: [],
      final_output: 'routed final',
    }))

    const promptAsync = mock(async () => {})
    const hooks = await AvenorPlugin(makeCtx({ client: makeClient(promptAsync) }) as any)
    const spawn = hooks.tool?.avenor_spawn as any

    await spawn.execute(
      { agent: 'jockey', wait: false },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    await hooks['permission.ask']?.(
      {
        id: 'perm-1',
        sessionID: 'child-session',
        type: 'fs.read',
        title: 'Read file',
        pattern: '/outside/project',
        messageID: 'msg-1',
        metadata: {},
        time: { created: Date.now() },
      },
      { status: 'ask' as const },
    )
    await flush()

    expect(promptAsync).toHaveBeenCalledTimes(1)
    expect(promptAsync.mock.calls[0]?.[0]).toEqual({
      path: { id: 'orchestrator-session' },
      body: {
        parts: [{
          type: 'text',
          text: expect.stringContaining('request_id: "perm-1"'),
        }],
      },
    })
    expect(hangingStream.returnMock).toHaveBeenCalledTimes(1)
    expect(hangingStream.closeMock).toHaveBeenCalledTimes(1)

    await hooks.event?.({
      event: { type: 'session.idle', properties: { sessionID: 'orchestrator-session' } } as any,
    })
    await flush()

    expect(completionStream.calls[0]).toEqual({ runtime_id: 'rt-1', replay: true, after_seq: 7 })
    expect(promptAsync).toHaveBeenCalledTimes(2)
    expect(promptAsync.mock.calls[1]?.[0]).toEqual({
      path: { id: 'orchestrator-session' },
      body: {
        parts: [{
          type: 'text',
          text: expect.stringContaining('routed final'),
        }],
      },
    })
  })

  it('clears reverse session mappings when a completed run is unregistered', async () => {
    const firstStream = makeEventClient([
      { event: 'session.end', runtime_id: 'rt-1', session_id: 'child-session', seq: 1, stop_reason: 'end_turn' },
    ])
    dialQueue.push(firstStream)
    const promptAsync = mock(async () => {})
    const hooks = await AvenorPlugin(makeCtx({ client: makeClient(promptAsync) }) as any)
    const spawn = hooks.tool?.avenor_spawn as any

    await spawn.execute(
      { agent: 'jockey', wait: true },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    spawnToolMock.mockResolvedValueOnce({
      run_id: 'run-2',
      runtime_id: 'rt-2',
      label: 'second run',
      supervisor_id: '/tmp/avenor.sock',
    })
    statusToolMock.mockResolvedValue({
      run_id: 'run-2',
      runtime_id: 'rt-2',
      session_id: 'child-session',
      status: 'running',
      latest_seq: 0,
    })
    const secondStream = makeEventClient([], { hangAfterExhausted: true })
    dialQueue.push(secondStream)
    await spawn.execute(
      { agent: 'jockey', wait: false },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    promptAsync.mockClear()
    await hooks['permission.ask']?.(
      {
        id: 'perm-2',
        sessionID: 'child-session',
        type: 'fs.read',
        title: 'Read file',
        pattern: '/tmp/file',
        messageID: 'msg-2',
        metadata: {},
        time: { created: Date.now() },
      },
      { status: 'ask' as const },
    )

    expect(promptAsync).toHaveBeenCalledTimes(1)
    expect(promptAsync.mock.calls[0]?.[0].body.parts[0].text).toContain('run_id: "run-2"')
    expect(secondStream.returnMock).toHaveBeenCalledTimes(1)
  })

  it('polls broker messages and retries after a transient fetch error', async () => {
    jest.useFakeTimers()
    const fetchMock = mock()
      .mockRejectedValueOnce(new Error('temporary broker failure'))
      .mockResolvedValueOnce({
        ok: true,
        json: async () => [{
          type: 'agent_message',
          from_run_id: 'child-run',
          payload: JSON.stringify({ from_run_id: 'child-run', role: 'reviewer', message: 'review complete' }),
        }],
      })
    globalThis.fetch = fetchMock as any
    spawnToolMock.mockResolvedValueOnce({
      run_id: 'run-1',
      runtime_id: 'rt-1',
      label: 'test run',
      supervisor_id: '/tmp/avenor.sock',
      broker_url: 'http://broker.test',
      parent_token: 'parent-token',
    } as any)
    const stream = makeEventClient([], { hangAfterExhausted: true })
    dialQueue.push(stream)
    const promptAsync = mock(async () => { throw new Error('stop polling after delivery') })
    const hooks = await AvenorPlugin(makeCtx({ client: makeClient(promptAsync) }) as any)

    await (hooks.tool?.avenor_spawn as any).execute(
      { agent: 'jockey', wait: false },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )
    await hooks.event?.({
      event: { type: 'session.idle', properties: { sessionID: 'orchestrator-session' } } as any,
    })

    jest.advanceTimersByTime(3_000)
    await flushMicrotasks()
    jest.advanceTimersByTime(3_000)
    await flushMicrotasks()

    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[1]?.[0]).toBe('http://broker.test/poll-control')
    expect(JSON.parse(fetchMock.mock.calls[1]?.[1].body)).toEqual({
      run_id: expect.any(String),
      token: 'parent-token',
    })
    expect(promptAsync).toHaveBeenCalledWith(expect.objectContaining({
      path: { id: 'orchestrator-session' },
      body: { parts: [{ type: 'text', text: expect.stringContaining('review complete') }] },
    }))

    await (hooks.tool?.avenor_shutdown as any).execute({}, {})
  })

  it('closes observers on shutdown and exposes avenor_inspect output', async () => {
    const hangingStream = makeEventClient([], { hangAfterExhausted: true })
    dialQueue.push(hangingStream)

    const hooks = await AvenorPlugin(makeCtx() as any)
    const spawn = hooks.tool?.avenor_spawn as any
    await spawn.execute(
      { agent: 'jockey', wait: false },
      {
        sessionID: 'orchestrator-session',
        directory: '/tmp/test',
        abort: new AbortController().signal,
        metadata: () => {},
      },
    )

    const shutdown = hooks.tool?.avenor_shutdown as any
    await shutdown.execute({ supervisor_id: '/tmp/avenor.sock' }, {})
    await flush()

    expect(hangingStream.returnMock).toHaveBeenCalledTimes(1)
    expect(hangingStream.closeMock).toHaveBeenCalledTimes(1)
    expect(shutdownToolMock).toHaveBeenCalledWith({ supervisorId: '/tmp/avenor.sock', force: undefined })

    const inspect = hooks.tool?.avenor_inspect as any
    const output = await inspect.execute({
      run_id: 'run-42',
      limit: 10,
      after_seq: 3,
      supervisor_id: '/tmp/avenor.sock',
    }, {})

    expect(inspectToolMock).toHaveBeenCalledWith({
      runId: 'run-42',
      limit: 10,
      after_seq: 3,
      supervisorId: '/tmp/avenor.sock',
    })
    expect(output).toEqual(expect.objectContaining({
      title: 'test run — done',
      output: expect.stringContaining('Inspect: test run — done'),
      metadata: expect.objectContaining({
        run_id: 'run-1',
        final_output: expect.any(String),
      }),
    }))
  })
})
