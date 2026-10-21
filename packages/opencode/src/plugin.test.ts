import { afterEach, beforeEach, describe, expect, it, jest, mock } from 'bun:test'

import { observeRun } from '../../core/src/run-observer.js'
import { createRunSnapshot } from '../../core/src/run-reducer.js'
import { extractEventText } from '../../core/src/run-events.js'
import { validateSpawnSelection } from '../../core/src/spawn-selection.js'
import { THINKING_LEVELS } from '../../core/src/thinking-policy.js'
import { ToolError, renderPeerList } from '../../core/src/message.js'
import { boundFinalOutput, extractTurnText, normalizeTurnStopReason, renderLast } from '../../core/src/messaging-text.js'
import { messageTool as realMessageTool } from '../../core/src/tools/message.js'
import { peersTool as realPeersTool } from '../../core/src/tools/peers.js'
import type { MessageProvider } from '../../core/src/tools/message.js'
import conformance from '../../../schemas/message_route.conformance.json'
import resultFixture from '../../../schemas/message_route.result.json'
import { peersDetails, resultOutput } from '../../core/src/message.js'

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
const workflowCancelToolMock = mock(async () => ({ cancelled: true }))
let providerFactory: () => MessageProvider = () => makeProvider()
const getSupervisorClientMock = mock(async () => ({
  client: { messagePreflight: async () => ({ generation: 'gen-1' }) },
  supervisorId: '/tmp/avenor.sock',
  isSingleton: true,
  sup: null,
}))

function makeProvider(overrides: Partial<MessageProvider> = {}): MessageProvider {
  return {
    caller: () => ({ kind: 'owner', runId: 'supervisor' }),
    surface: () => 'opencode',
    role: () => 'host',
    resolve: async (ref: string) => ({ runId: ref, label: ref, match: 'unique' }),
    state: async () => ({ live: false, resumable: true, status: 'done' }),
    tell: async () => ({ outcome: 'sent' }),
    ask: async () => ({ outcome: 'sent' }),
    reply: async () => ({ outcome: 'replied' }),
    withdraw: async () => ({ outcome: 'withdrawn' }),
    resume: async () => ({
      outcome: 'resumed',
      runId: 'run-2',
      runtimeId: 'rt-2',
      label: 'again',
      sentinelFile: '/runs/run-2/sentinel.done',
      onEvent: '/runs/run-2/events.log',
    }),
    rename: async () => ({ outcome: 'renamed' }),
    last: async () => ({ outcome: 'sent', hasRecord: false }),
    peers: async () => ({ peers: [] }),
    ...overrides,
  }
}
const resultToolMock = mock(async () => ({ run_id: 'run-1', label: 'test run', status: 'done', ready: true, output: 'final answer' }))
const shutdownToolMock = mock(async () => ({ ok: true }))
const killToolMock = mock(async () => ({ killed: true, run_id: 'run-1' }))
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
const workflowControllerStatusToolMock = mock(async () => ({ controller_id: 'ctl', desired_state: 'enabled' }))
const workflowReadyToolMock = mock(async () => ({}))

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
  THINKING_LEVELS,
  spawnTool: spawnToolMock,
  validateSpawnSelection,
  statusTool: statusToolMock,
  eventsTool: eventsToolMock,
  answerPermissionTool: answerPermissionToolMock,
  messageTool: realMessageTool,
  peersTool: realPeersTool,
  peersDetails,
  resultOutput,
  createHostMessageProvider: (options: { onResumed?: MessageProvider['onResumed'] }) => {
    const provider = providerFactory()
    if (options?.onResumed) provider.onResumed = options.onResumed
    return provider
  },
  getSupervisorClient: getSupervisorClientMock,
  ToolError,
  renderPeerList,
  boundFinalOutput,
  extractTurnText,
  normalizeTurnStopReason,
  workflowCancelTool: workflowCancelToolMock,
  resultTool: resultToolMock,
  shutdownTool: shutdownToolMock,
  killTool: killToolMock,
  inspectTool: inspectToolMock,
  workflowStatusTool: mock(async () => ({})),
  workflowWaitTool: mock(async () => ({})),
  workflowInspectTool: mock(async () => ({})),
  workflowEventsTool: mock(async () => ({})),
  workflowCompleteTool: mock(async () => ({})),
  workflowGateTool: mock(async () => ({})),
  workflowControllerStatusTool: workflowControllerStatusToolMock,
  workflowReadyTool: workflowReadyToolMock,
  createRunSnapshot,
  extractEventText,
  observeRun,
  dial: dialMock,
  Supervisor: {
    currentInstance: currentSupervisorMock,
  },
}))
const { AvenorPlugin, buildWaitingText, hostIdentityMetadata, terminalStatus } = await import('./plugin.js')
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
  it('preserves roster selectors and effective identity in host metadata', () => {
    expect(hostIdentityMetadata({
      rosterFile: '/repo/roster.json',
      rosterEntry: 'planner',
    }, {
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      effective_agent: 'planner-agent',
      effective_model: 'provider/model',
      effective_backend: 'agy',
    })).toEqual({
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      effective_agent: 'planner-agent',
      effective_model: 'provider/model',
      effective_backend: 'agy',
    })
  })

  beforeEach(() => {
    spawnToolMock.mockClear()
    statusToolMock.mockClear()
    eventsToolMock.mockClear()
    answerPermissionToolMock.mockClear()
    workflowCancelToolMock.mockClear()
    getSupervisorClientMock.mockClear()
    providerFactory = () => makeProvider()
    resultToolMock.mockClear()
    shutdownToolMock.mockClear()
    killToolMock.mockClear()
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
    expect(permissionText).not.toContain('avenor_message')

    const questionText = buildWaitingText(run, makeSnapshot({ phase_label: 'Which file?' }))
    expect(questionText).toContain('Question: Which file?')
    expect(questionText).toContain('avenor_message')
    expect(questionText).toContain('action "resume"')
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
      'avenor_message',
      'avenor_peers',
      'avenor_workflow_cancel',
      'avenor_events',
      'avenor_inspect',
      'avenor_shutdown',
      'avenor_kill',
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

  it('forwards optional direct and roster selectors through the shared spawn tool', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    const spawn = hooks.tool?.avenor_spawn as any
    const context = {
      sessionID: 'orchestrator-session',
      directory: '/tmp/test',
      abort: new AbortController().signal,
    }

    await spawn.execute({ model: 'provider/model', backend: 'agy', wait: false }, context)
    expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({
      model: 'provider/model',
      backend: 'agy',
    })
    expect(spawnToolMock.mock.calls[0]?.[0]).not.toHaveProperty('agent')

    spawnToolMock.mockClear()
    await spawn.execute({
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      wait: false,
    }, context)
    expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({
      rosterFile: '/repo/roster.json',
      rosterEntry: 'planner',
    })
    expect(spawnToolMock.mock.calls[0]?.[0]).not.toHaveProperty('backend')

    await expect(spawn.execute({
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      backend: 'agy',
      wait: false,
    }, context)).rejects.toThrow('direct identity fields are disabled in roster mode')
    expect(spawnToolMock).toHaveBeenCalledTimes(1)
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

    const kill = hooks.tool?.avenor_kill as any
    const killResult = await kill.execute({ run_id: 'run-1', supervisor_id: '/tmp/avenor.sock' }, {})
    await flush()
    expect(killToolMock).toHaveBeenCalledWith({ runId: 'run-1', supervisorId: '/tmp/avenor.sock' })
    expect(killResult).toEqual(expect.objectContaining({
      title: 'Avenor — kill run',
      output: expect.stringContaining('Run: run-1 (runtime unresolved)'),
      metadata: { killed: true, run_id: 'run-1', supervisor_id: '/tmp/avenor.sock' },
    }))

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

  it('enriches controller status details with an advisory candidate count', async () => {
    workflowControllerStatusToolMock.mockImplementation(async () => ({ controller_id: 'ctl', desired_state: 'enabled', inflight: 1 }))
    workflowReadyToolMock.mockImplementation(async () => ({ advisory: true, controller_id: 'ctl', candidates: [{}, {}] }))

    const hooks = await AvenorPlugin(makeCtx() as any)
    const context = { sessionID: 'orchestrator-session', directory: '/tmp/test', abort: new AbortController().signal }
    const output = await (hooks.tool?.avenor_workflow_controller_status as any).execute({ controller_id: 'ctl' }, context)

    expect(output.metadata.candidate_count).toBe(2)
  })

  it('omits the candidate count when the ready query fails', async () => {
    workflowControllerStatusToolMock.mockImplementation(async () => ({ controller_id: 'ctl', desired_state: 'enabled' }))
    workflowReadyToolMock.mockImplementation(async () => {
      throw new Error('-32601 method not found')
    })

    const hooks = await AvenorPlugin(makeCtx() as any)
    const context = { sessionID: 'orchestrator-session', directory: '/tmp/test', abort: new AbortController().signal }
    const output = await (hooks.tool?.avenor_workflow_controller_status as any).execute({ controller_id: 'ctl' }, context)

    expect(output.metadata.candidate_count).toBeUndefined()
    expect(output.metadata.controller_id).toBe('ctl')
  })

  it('forwards controller status to the supervisor client with the requested supervisor', async () => {
    workflowControllerStatusToolMock.mockClear()
    workflowControllerStatusToolMock.mockImplementation(async () => ({ controller_id: 'ctl', desired_state: 'enabled' }))
    workflowReadyToolMock.mockImplementation(async () => ({}))

    const hooks = await AvenorPlugin(makeCtx() as any)
    const context = { sessionID: 'orchestrator-session', directory: '/tmp/test', abort: new AbortController().signal }
    await (hooks.tool?.avenor_workflow_controller_status as any).execute({ controller_id: 'ctl', supervisor_id: 'sup-a' }, context)

    expect(workflowControllerStatusToolMock).toHaveBeenCalledWith({ controllerId: 'ctl', supervisorId: 'sup-a' })
  })

  it('lists all controllers and skips the advisory ready query when controller_id is omitted', async () => {
    workflowControllerStatusToolMock.mockClear()
    workflowReadyToolMock.mockClear()
    workflowControllerStatusToolMock.mockImplementation(async () => ({ controllers: [] }))

    const hooks = await AvenorPlugin(makeCtx() as any)
    const context = { sessionID: 'orchestrator-session', directory: '/tmp/test', abort: new AbortController().signal }
    const output = await (hooks.tool?.avenor_workflow_controller_status as any).execute({ supervisor_id: 'sup-a' }, context)

    expect(workflowControllerStatusToolMock).toHaveBeenCalledWith({ controllerId: undefined, supervisorId: 'sup-a' })
    expect(workflowReadyToolMock).not.toHaveBeenCalled()
    expect(output.title).toBe('Avenor workflow controllers — 0')
  })
})

const contractFixture = conformance as unknown as {
  applicability: Record<string, { permitted: string[] }>
  peers_applicability: Record<string, { permitted: string[] }>
}

function fieldSet(table: Record<string, { permitted: string[] }>): string[] {
  const names = new Set<string>()
  for (const entry of Object.values(table)) {
    for (const name of entry.permitted) names.add(name)
  }
  return [...names].sort()
}

function toolArgs(hooks: Awaited<ReturnType<typeof AvenorPlugin>>, name: string): Record<string, any> {
  return (hooks.tool as Record<string, any>)[name].args
}

function enumValues(schema: any): string[] {
  const values = schema?.options ?? schema?.unwrap?.()?.options ?? []
  return [...values].sort()
}

function toolContext(sessionID: string) {
  return {
    sessionID,
    messageID: 'm-1',
    agent: 'build',
    directory: '/tmp/test',
    worktree: '/tmp/test',
    abort: new AbortController().signal,
    metadata: () => {},
    ask: async () => {},
  }
}

function makeTurnClient(messages: ReturnType<typeof mock>, promptAsync = mock(async () => {})) {
  return { session: { promptAsync, messages } }
}

function turnResponse(info: Record<string, unknown>, parts: unknown[]) {
  return { data: [{ info, parts }] }
}

describe('OpenCode message tool parity', () => {
  it('registers avenor_message and avenor_peers with the contract field sets', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    const message = toolArgs(hooks, 'avenor_message')
    expect(Object.keys(message).sort()).toEqual(fieldSet(contractFixture.applicability))
    expect(enumValues(message.action)).toEqual(['ask', 'control_reply', 'reply', 'resume', 'tell', 'withdraw'])
    expect(message).not.toHaveProperty('supervisor_id')

    const peers = toolArgs(hooks, 'avenor_peers')
    expect(Object.keys(peers).sort()).toEqual(fieldSet(contractFixture.peers_applicability))
    expect(enumValues(peers.action)).toEqual(['last', 'list', 'rename'])
    expect(peers).not.toHaveProperty('supervisor_id')
  })

  it('routes a tell through the shared provider and returns its contract text', async () => {
    providerFactory = () => makeProvider({
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => ({ outcome: 'sent', messageId: 'm-1' }),
    })
    const hooks = await AvenorPlugin(makeCtx() as any)
    const output = await (hooks.tool?.avenor_message as any).execute(
      { action: 'tell', target: 'mesh-a', message: 'hi' },
      toolContext('session-1'),
    )
    expect(output.output).toBe('message "m-1" delivered to "mesh-a"')
  })

  it('returns the shared result objects for every message and peers action', async () => {
    const expected = resultFixture as unknown as {
      message: Record<string, Record<string, unknown>>
      peers: Record<string, Record<string, unknown>>
    }
    const hooks = await AvenorPlugin(makeCtx() as any)

    providerFactory = () => makeProvider({
      resolve: async () => ({ runId: 'mesh-a', label: 'mesh-a', match: 'unique' }),
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => ({ outcome: 'sent', messageId: 'm-1' }),
    })
    const tell = await (hooks.tool?.avenor_message as any).execute(
      { action: 'tell', target: 'mesh-a', message: 'hi' },
      toolContext('session-1'),
    )
    expect(tell.metadata).toEqual(expected.message.tell)
    expect(tell.output).toBe(expected.message.tell.text)

    providerFactory = () => makeProvider({
      resolve: async () => ({ runId: 'mesh-a', label: 'mesh-a', match: 'unique' }),
      state: async () => ({ live: false, resumable: true, status: 'done' }),
      resume: async () => ({
        outcome: 'resumed',
        runId: 'run-2',
        runtimeId: 'rt-2',
        label: 'again',
        sentinelFile: '/runs/run-2/sentinel.done',
        onEvent: '/runs/run-2/events.log',
      }),
    })
    const resume = await (hooks.tool?.avenor_message as any).execute(
      { action: 'resume', target: 'mesh-a', message: 'again' },
      toolContext('session-1'),
    )
    expect(resume.metadata).toEqual(expected.message.resume)

    providerFactory = () => makeProvider()
    const reply = await (hooks.tool?.avenor_message as any).execute(
      { action: 'reply', reply_to: 'a-1', message: 'pong' },
      toolContext('session-1'),
    )
    expect(reply.metadata).toEqual(expected.message.reply_edge_absent)

    providerFactory = () => makeProvider({
      peers: async () => ({
        peers: [
          { run_id: 'rt_2', label: 'mesh-a', kind: 'agent', status: 'running' },
          { run_id: 'rt_3' },
        ],
      }),
      rename: async () => ({ outcome: 'renamed', runId: 'rt_2', label: 'mesh-renamed', previousLabel: 'mesh-a' }),
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({ outcome: 'sent', hasRecord: true, finalOutput: 'OK', stopReason: 'end_turn' }),
    })
    const list = await (hooks.tool?.avenor_peers as any).execute({ action: 'list' }, toolContext('session-1'))
    expect(list.metadata).toEqual(expected.peers.list)
    const rename = await (hooks.tool?.avenor_peers as any).execute(
      { action: 'rename', name: 'mesh-renamed' },
      toolContext('session-1'),
    )
    expect(rename.metadata).toEqual(expected.peers.rename)
    const last = await (hooks.tool?.avenor_peers as any).execute(
      { action: 'last', target: 'mesh-a' },
      toolContext('session-1'),
    )
    expect(last.metadata).toEqual(expected.peers.last)
  })

  it('tracks a resumed run through the provider callback', async () => {
    providerFactory = () => makeProvider({ registerResumed: () => {} })
    statusToolMock.mockImplementation(async () => ({
      run_id: 'run-2',
      runtime_id: 'rt-2',
      label: 'again',
      status: 'done',
    }))
    dialQueue.push(makeEventClient([
      { event: 'session.end', runtime_id: 'rt-2', seq: 1, stop_reason: 'end_turn' },
    ]))
    const hooks = await AvenorPlugin(makeCtx() as any)
    const output = await (hooks.tool?.avenor_message as any).execute(
      { action: 'resume', target: 'run-1', message: 'again' },
      toolContext('session-1'),
    )
    expect(output.title).toBe('again — resumed')
    expect(output.output).toContain('Run: run-2')
    for (let index = 0; index < 4; index++) await flush()
    expect(dialMock).toHaveBeenCalledWith('/tmp/avenor.sock')
  })

  it('forwards the workflow cancel actor and reason', async () => {
    const hooks = await AvenorPlugin(makeCtx() as any)
    await (hooks.tool?.avenor_workflow_cancel as any).execute(
      { workflow_id: 'w-1', actor: 'operator', reason: 'superseded', supervisor_id: 'sup-a' },
      toolContext('session-1'),
    )
    expect(workflowCancelToolMock).toHaveBeenCalledWith({
      workflow_id: 'w-1',
      actor: 'operator',
      reason: 'superseded',
      supervisor_id: 'sup-a',
    })
  })
})

describe('OpenCode peers last', () => {
  it('renders the shared last template through the bound provider', async () => {
    providerFactory = () => makeProvider({
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({
        outcome: 'sent',
        label: 'mesh-a',
        hasRecord: true,
        finalOutput: 'OK',
        stopReason: 'end_turn',
        truncated: false,
      }),
    })
    const hooks = await AvenorPlugin(makeCtx() as any)
    const output = await (hooks.tool?.avenor_peers as any).execute(
      { action: 'last', target: 'mesh-a' },
      toolContext('session-1'),
    )
    expect(output.output).toBe(
      renderLast('header', { run_id: '"rt_7"', label: '"mesh-a"', stop_reason: 'end_turn' }) + '\n\nOK',
    )
    expect(output.metadata.last).toMatchObject({ run_id: 'rt_7', runtime_id: 'rt_7', label: 'mesh-a' })
  })

  it('returns unknown_target without reading the target', async () => {
    const last = mock(async () => ({ outcome: 'sent' }))
    providerFactory = () => makeProvider({
      resolve: async () => ({ runId: '', match: 'none' }),
      last,
    })
    const hooks = await AvenorPlugin(makeCtx() as any)
    const error = await (hooks.tool?.avenor_peers as any)
      .execute({ action: 'last', target: 'ghost' }, toolContext('session-1'))
      .catch((thrown: unknown) => thrown)
    expect(error).toBeInstanceOf(ToolError)
    expect((error as ToolError).code).toBe('unknown_target')
    expect(last).not.toHaveBeenCalled()
  })
})

describe('OpenCode host turn producer', () => {
  async function bindSession(hooks: Awaited<ReturnType<typeof AvenorPlugin>>, sessionID: string) {
    await (hooks.tool?.avenor_peers as any).execute({ action: 'list' }, toolContext(sessionID))
  }

  async function idle(hooks: Awaited<ReturnType<typeof AvenorPlugin>>, sessionID: string) {
    await hooks.event?.({ event: { type: 'session.idle', properties: { sessionID } } as any })
  }

  it('publishes the settled turn once for the bound session', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    providerFactory = () => makeProvider({ recordLastTurn: record })
    const messages = mock(async () => turnResponse(
      { role: 'assistant', finish: 'stop' },
      [{ type: 'text', text: 'the answer' }],
    ))
    const hooks = await AvenorPlugin(makeCtx({ client: makeTurnClient(messages) }) as any)
    await bindSession(hooks, 'session-1')
    await idle(hooks, 'session-1')

    expect(record).toHaveBeenCalledTimes(1)
    expect(record).toHaveBeenCalledWith('the answer', 'end_turn', false)
  })

  it('publishes nothing before any host tool binds the session', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    providerFactory = () => makeProvider({ recordLastTurn: record })
    const messages = mock(async () => turnResponse({ role: 'assistant', finish: 'stop' }, []))
    const hooks = await AvenorPlugin(makeCtx({ client: makeTurnClient(messages) }) as any)

    await idle(hooks, 'session-1')

    expect(record).not.toHaveBeenCalled()
    expect(messages).not.toHaveBeenCalled()
  })

  it('keeps the first binding when another session runs a host tool', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    providerFactory = () => makeProvider({ recordLastTurn: record })
    const messages = mock(async () => turnResponse({ role: 'assistant', finish: 'stop' }, [{ type: 'text', text: 'from one' }]))
    const logged: string[] = []
    const originalError = console.error
    console.error = (...args: unknown[]) => { logged.push(args.map(String).join(' ')) }
    try {
      const hooks = await AvenorPlugin(makeCtx({ client: makeTurnClient(messages) }) as any)
      await bindSession(hooks, 'session-1')
      await bindSession(hooks, 'session-2')
      await idle(hooks, 'session-2')
      expect(record).not.toHaveBeenCalled()

      await idle(hooks, 'session-1')
      expect(record).toHaveBeenCalledWith('from one', 'end_turn', false)
    } finally {
      console.error = originalError
    }
    expect(logged).toContain('opencode: host session already bound to session-1')
  })

  it('publishes a cancelled turn with no text instead of skipping it', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    providerFactory = () => makeProvider({ recordLastTurn: record })
    const messages = mock(async () => turnResponse(
      { role: 'assistant', error: { name: 'MessageAbortedError' } },
      [],
    ))
    const hooks = await AvenorPlugin(makeCtx({ client: makeTurnClient(messages) }) as any)
    await bindSession(hooks, 'session-1')
    await idle(hooks, 'session-1')

    expect(record).toHaveBeenCalledWith('', 'cancelled', false)
  })

  it('carries the truncation flag from the unbounded text', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    providerFactory = () => makeProvider({ recordLastTurn: record })
    const messages = mock(async () => turnResponse(
      { role: 'assistant', error: { name: 'MessageOutputLengthError' } },
      [{ type: 'text', text: 'x'.repeat(5000) }],
    ))
    const hooks = await AvenorPlugin(makeCtx({ client: makeTurnClient(messages) }) as any)
    await bindSession(hooks, 'session-1')
    await idle(hooks, 'session-1')

    const [text, stop, truncated] = record.mock.calls[0] as [string, string, boolean]
    expect(text).toHaveLength(4096)
    expect(stop).toBe('max_tokens')
    expect(truncated).toBe(true)
  })

  it('logs a failing publish without rejecting the idle handler', async () => {
    providerFactory = () => makeProvider({
      recordLastTurn: async () => { throw new Error('broker down') },
    })
    const messages = mock(async () => turnResponse({ role: 'assistant', finish: 'stop' }, [{ type: 'text', text: 'turn' }]))
    const logged: string[] = []
    const originalError = console.error
    console.error = (...args: unknown[]) => { logged.push(args.map(String).join(' ')) }
    try {
      const hooks = await AvenorPlugin(makeCtx({ client: makeTurnClient(messages) }) as any)
      await bindSession(hooks, 'session-1')
      await expect(idle(hooks, 'session-1')).resolves.toBeUndefined()
    } finally {
      console.error = originalError
    }
    expect(logged.join('\n')).toContain('session_last_set failed')
  })
})
