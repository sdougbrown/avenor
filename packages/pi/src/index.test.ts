import { describe, expect, it, mock } from 'bun:test'
import { createHash } from 'node:crypto'
import type { InspectResult, RunSnapshot, StatusResult } from '@dougbots/avenor-core'
import type { ExtensionDeps } from './index.js'
import { RunReducer } from '@dougbots/avenor-core'
import extensionFactory, {
  buildCompletionText,
  buildInspectPayload,
  CHANNEL_POLL_COMPLETED,
  CHANNEL_POLL_ERROR,
  CHANNEL_RUN_TERMINAL,
  compactWhitespace,
  createExtension,
  isTerminalStatus,
  renderStatusLines,
  spawnIdentityMetadata,
  statusSupervisorId,
} from './index.js'
import { findLiveStatusForTrackedRun } from './types.js'

/** Build minimal mock ExtensionDeps with all tools stubbed via bun:test.mock(). */
function buildMockDeps(partial: Partial<ExtensionDeps> = {}): ExtensionDeps {
  return {
    spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock' })),
    statusTool: mock(async () => []),
    eventsTool: mock(async () => ({ events: [] })),
    answerPermissionTool: mock(async () => ({ ok: true })),
    followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
    inspectTool: mock(async () => makeInspectResult()),
    resultTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true })),
    shutdownTool: mock(async () => ({ ok: true })),
    observeRun: mock(() => null),
    dial: mock(async () => ({ close() {} })),
    Supervisor: class {} as any,
    ...partial,
  }
}

function buildSnapshot(): RunSnapshot {
  const reducer = new RunReducer()
  reducer.apply({ event: 'session.start', runtime_id: 'rt-1', run_id: 'run-1', run_label: 'demo', backend: 'pi', agent: 'horse', model: 'model-1', dir: '/tmp/demo' })
  reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'hello ' })
  reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'world' })
  reducer.apply({ event: 'session.end', runtime_id: 'rt-1', seq: 3, final_output: 'hello world', stop_reason: 'end_turn' })
  return reducer.snapshot()
}

async function withoutAmbientAgentProfile<T>(run: () => Promise<T>): Promise<T> {
  const previous = process.env.PI_AGENT_PROFILE
  delete process.env.PI_AGENT_PROFILE
  try {
    return await run()
  } finally {
    if (previous === undefined) delete process.env.PI_AGENT_PROFILE
    else process.env.PI_AGENT_PROFILE = previous
  }
}

function makeInspectResult(statusOverrides: Partial<StatusResult> = {}): InspectResult {
  const snapshot = buildSnapshot()
  const status: StatusResult = {
    run_id: statusOverrides.run_id ?? 'run-1',
    label: statusOverrides.label ?? 'demo',
    status: statusOverrides.status ?? 'done',
    runtime_id: statusOverrides.runtime_id ?? 'rt-1',
    phase: statusOverrides.phase ?? snapshot.phase,
    phase_label: statusOverrides.phase_label ?? snapshot.phase_label,
    pending_permission: statusOverrides.pending_permission ?? snapshot.pending_permission,
    session_id: statusOverrides.session_id ?? 'ses-1',
    stop_reason: statusOverrides.stop_reason ?? snapshot.stop_reason,
    backend: statusOverrides.backend ?? 'pi',
    agent: statusOverrides.agent ?? 'horse',
    model: statusOverrides.model ?? 'model-1',
    dir: statusOverrides.dir ?? '/tmp/demo',
    usage: statusOverrides.usage ?? { total_tokens: 42 },
    latest_seq: statusOverrides.latest_seq ?? snapshot.latest_seq,
    final_output: statusOverrides.final_output ?? 'hello world',
  }

  return {
    run_id: status.run_id,
    label: status.label,
    status,
    snapshot,
    transcript: snapshot.transcript,
    tools: snapshot.tools,
    live_tools: snapshot.live_tools,
    permissions: snapshot.permissions,
    pending_permission: snapshot.pending_permission,
    final_output: status.final_output,
  }
}

async function createHarnessBase(options: {
  deps?: Partial<ExtensionDeps>
  extOptions?: { pollIntervalMs?: number }
  ui?: Record<string, any>
  onEmit?: (channel: string, payload: unknown) => void
  sendUserMessage?: any
}) {
  const registeredTools: Record<string, any> = {}
  const registeredCommands: Record<string, any> = {}
  const eventHandlers: Record<string, any> = {}
  const setStatus = mock((..._args: any[]) => {})
  const setWidget = mock((..._args: any[]) => {})
  const notify = mock((..._args: any[]) => {})
  const sendUserMessage = options.sendUserMessage ?? mock(() => {})
  const emitted: Array<[string, unknown]> = []
  const mockPi = {
    on: (event: string, handler: any) => { eventHandlers[event] = handler },
    registerTool: (definition: { name: string }) => { registeredTools[definition.name] = definition },
    registerCommand: (name: string, definition: any) => { registeredCommands[name] = definition },
    registerMessageRenderer: () => {},
    sendUserMessage,
    events: {
      emit: mock((channel: string, payload: unknown) => {
        emitted.push([channel, payload])
        options.onEmit?.(channel, payload)
      }),
      on: mock(() => () => {}),
    },
  }
  const baseDeps: ExtensionDeps = {
    spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock', runtime_id: 'rt-1' })),
    statusTool: mock(async () => []),
    eventsTool: mock(async () => ({ events: [] })),
    answerPermissionTool: mock(async () => ({ ok: true })),
    followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
    inspectTool: mock(async () => makeInspectResult({ status: 'running' })),
    resultTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true })),
    shutdownTool: mock(async () => ({ ok: true })),
    observeRun: mock(() => null),
    dial: mock(async () => ({ close() {} })),
    Supervisor: class {} as any,
  }
  await createExtension({ ...baseDeps, ...options.deps }, options.extOptions)(mockPi as any)

  const ctx = { cwd: '/tmp', ui: { setStatus, setWidget, notify, ...(options.ui ?? {}) } }
  await eventHandlers.session_start({}, ctx)
  return { ctx, emitted, eventHandlers, notify, registeredCommands, registeredTools, sendUserMessage, setStatus, setWidget }
}

async function createPollingHarness(statusTool: any, observeRun = mock(() => null)) {
  let resolvePollCompleted: (() => void) | undefined
  const waitForPoll = () => new Promise<void>(resolve => { resolvePollCompleted = resolve })
  const h = await createHarnessBase({
    deps: { statusTool, observeRun },
    onEmit: (channel) => {
      if (channel === CHANNEL_POLL_COMPLETED) resolvePollCompleted?.()
    },
  })
  return { ...h, waitForPoll }
}

async function createMultiSupervisorHarness(options: {
  pollIntervalMs?: number
  statusTool: any
  spawnTool?: any
  shutdownTool?: any
  observeRun?: any
  /** Supervisor mock; provides isCurrentInstance for singleton-scope tests. */
  supervisor?: { isCurrentInstance?: (supervisorId: string) => boolean }
}) {
  const confirm = mock(async () => true)
  let pollCount = 0
  const pollWaiters: Array<{ n: number; resolve: () => void }> = []
  const waitPolls = (n: number) => new Promise<void>(resolve => {
    if (pollCount >= n) return resolve()
    pollWaiters.push({ n, resolve })
  })
  const h = await createHarnessBase({
    deps: {
      statusTool: options.statusTool,
      ...(options.spawnTool !== undefined && { spawnTool: options.spawnTool }),
      ...(options.shutdownTool !== undefined && { shutdownTool: options.shutdownTool }),
      ...(options.observeRun !== undefined && { observeRun: options.observeRun }),
      ...(options.supervisor !== undefined && { Supervisor: options.supervisor as any }),
    },
    extOptions: { pollIntervalMs: options.pollIntervalMs ?? 5 },
    ui: { confirm },
    onEmit: (channel) => {
      if (channel === CHANNEL_POLL_COMPLETED) {
        pollCount++
        for (let i = pollWaiters.length - 1; i >= 0; i--) {
          if (pollCount >= pollWaiters[i].n) pollWaiters.splice(i, 1)[0].resolve()
        }
      }
    },
  })
  return { ...h, confirm, waitPolls }
}

describe('Avenor Pi extension', () => {
  it('exports a function', () => {
    expect(typeof extensionFactory).toBe('function')
  })

  it('only reuses a supervisor socket when the caller supplied one', () => {
    expect(statusSupervisorId(undefined, '/tmp/spawned.sock')).toBeUndefined()
    expect(statusSupervisorId('/tmp/requested.sock', '/tmp/spawned.sock')).toBe('/tmp/spawned.sock')
  })

  it('preserves roster selectors and effective identity in tool metadata', () => {
    expect(spawnIdentityMetadata({
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
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

  it('formats structured completion text with final output and inspection guidance', () => {
    const text = buildCompletionText(
      { runId: 'run-1', label: 'demo' },
      { run_id: 'run-1', label: 'demo', status: 'done', final_output: 'finished', session_id: 'ses-1' },
      'final answer',
    )

    expect(text).toBe([
      'Sub-agent "demo" finished with status **done**.',
      '',
      'Final output:',
      '> final answer',
      'Session: `ses-1`',
      '',
      'Call `avenor_inspect` with `run_id: "run-1"` for the bounded transcript/snapshot, or `avenor_follow_up` to iterate.',
    ].join('\n'))
  })

  it('builds bounded inspect payloads with final_output exposed', () => {
    const result = makeInspectResult({ final_output: 'x'.repeat(800) })
    result.transcript = Array.from({ length: 40 }, (_, index) => ({
      kind: 'assistant' as const,
      event: 'agent.message_chunk',
      source_event: 'agent.message_chunk',
      text: `entry-${index}`,
    }))

    const payload = buildInspectPayload(result)
    expect(String(payload.final_output)).toHaveLength(600)
    expect(String(payload.final_output)).toEndWith('…')
    expect(payload.transcript).toHaveLength(24)
    expect((payload.transcript as Array<{ text: string }>)[0].text).toBe('entry-16')
  })

  it('recognizes terminal statuses only', () => {
    expect(['done', 'failed', 'timeout', 'killed'].map(isTerminalStatus)).toEqual([true, true, true, true])
    expect(isTerminalStatus('running')).toBe(false)
    expect(isTerminalStatus(undefined)).toBe(false)
  })

  it('reconciles spawned UUIDs with singleton live runtime identities', () => {
    const live: StatusResult = {
      run_id: 'canonical-run',
      label: 'test-explore',
      status: 'running',
      runtime_id: 'rt-1',
    }

    // Singleton-scoped runs reconcile against the unqualified live list.
    expect(findLiveStatusForTrackedRun({ runId: 'canonical-run' }, [live], true)).toBe(live)
    expect(findLiveStatusForTrackedRun({ runId: 'missing' }, [], true)).toBeUndefined()
    expect(findLiveStatusForTrackedRun({
      runId: 'spawn-uuid',
      runtimeId: 'rt-1',
    }, [live], true)).toBe(live)
    // A run on another supervisor is never reconciled against the singleton
    // live list, even when its recorded id happens to match a live entry.
    expect(findLiveStatusForTrackedRun({
      runId: 'spawn-uuid',
      runtimeId: 'rt-1',
      supervisorId: '/tmp/other.sock',
    }, [live], false)).toBeUndefined()
    expect(findLiveStatusForTrackedRun({
      runId: 'spawn-uuid',
      supervisorId: '/tmp/other.sock',
      lastStatus: live,
    }, [live], false)).toBeUndefined()
    // A shared rt_1 across supervisors stays isolated per namespace.
    expect(findLiveStatusForTrackedRun({
      runId: 'rt_1',
      runtimeId: 'rt_1',
      supervisorId: '/tmp/factory.sock',
    }, [{ ...live, run_id: 'rt_1', runtime_id: 'rt_1' }], false)).toBeUndefined()
  })

  it('renders only active statuses with phase and permission markers', () => {
    expect(renderStatusLines([])).toEqual([])
    expect(renderStatusLines([
      { runId: 'done', label: 'finished', status: 'done', agent: 'horse' },
      { runId: 'active', label: 'worker', status: 'running', phaseLabel: 'reading', agent: 'horse', pendingPermission: true },
    ])).toEqual(['🟢 worker — running [reading] 🔒'])
  })

  it('compacts whitespace and clips bounded progress text', () => {
    expect(compactWhitespace(undefined)).toBeUndefined()
    expect(compactWhitespace('   ')).toBeUndefined()
    expect(compactWhitespace(' one\n\t two   three ')).toBe('one two three')
    expect(compactWhitespace('abcdefgh', 5)).toBe('abcd…')
  })

  it('emits polling errors through the event bus', async () => {
    const statusTool = mock(async () => { throw new Error('socket ended') })
    const harness = await createPollingHarness(statusTool)
    const pollCompleted = harness.waitForPoll()

    await harness.registeredTools.avenor_spawn.execute(
      'tool-1', { agent: 'explore', label: 'demo', wait: false }, undefined, undefined, harness.ctx,
    )
    await pollCompleted

    expect(harness.emitted).toContainEqual([
      CHANNEL_POLL_ERROR,
      expect.objectContaining({ source: 'singleton-list', count: 1 }),
    ])
    expect(harness.emitted).toContainEqual([
      CHANNEL_POLL_ERROR,
      expect.objectContaining({ source: 'run-status', runId: 'run-1', count: 2 }),
    ])
    await harness.eventHandlers.session_shutdown()
  })

  it('shows and clears polling errors in the footer', async () => {
    let available = false
    const statusTool = mock(async (args: { runId?: string } = {}) => {
      if (!available) throw new Error('socket ended')
      return args.runId
        ? { run_id: 'run-1', label: 'demo', status: 'running', runtime_id: 'rt-1' }
        : []
    })
    const harness = await createPollingHarness(statusTool)
    const pollCompleted = harness.waitForPoll()

    await harness.registeredTools.avenor_spawn.execute(
      'tool-1', { agent: 'explore', label: 'demo', wait: false }, undefined, undefined, harness.ctx,
    )
    await pollCompleted
    expect(String(harness.setStatus.mock.calls.filter(([name]) => name === 'avenor').at(-1)?.[1])).toContain('errors:2')

    available = true
    await harness.registeredCommands['avenor-status'].handler('', harness.ctx)
    expect(String(harness.setStatus.mock.calls.filter(([name]) => name === 'avenor').at(-1)?.[1])).not.toContain('errors:')
    await harness.eventHandlers.session_shutdown()
  })

  it('shows recorded polling errors, then clears them', async () => {
    const statusTool = mock(async () => { throw new Error('socket ended') })
    const harness = await createPollingHarness(statusTool)
    const pollCompleted = harness.waitForPoll()

    await harness.registeredTools.avenor_spawn.execute(
      'tool-1', { agent: 'explore', label: 'demo', wait: false }, undefined, undefined, harness.ctx,
    )
    await pollCompleted
    await harness.registeredCommands['avenor-errors'].handler('', harness.ctx)
    expect(harness.setWidget.mock.calls.find(([name]) => name === 'avenor-errors')?.[1]).toEqual([
      expect.stringContaining('failed to list singleton runs'),
      expect.stringContaining('statusTool failed while polling a run'),
    ])

    await harness.registeredCommands['avenor-errors'].handler('', harness.ctx)
    expect(harness.setWidget.mock.calls.filter(([name]) => name === 'avenor-errors').at(-1)?.[1]).toBeUndefined()
    expect(harness.notify).toHaveBeenCalledWith('No recorded avenor polling errors', 'info')
    await harness.eventHandlers.session_shutdown()
  })

  it('reports an empty polling-error history', async () => {
    const harness = await createPollingHarness(mock(async () => []))

    await harness.registeredCommands['avenor-errors'].handler('', harness.ctx)

    expect(harness.setWidget).toHaveBeenCalledWith('avenor-errors', undefined)
    expect(harness.notify).toHaveBeenCalledWith('No recorded avenor polling errors', 'info')
    await harness.eventHandlers.session_shutdown()
  })

  it('resets polling errors when polling stops before a restart', async () => {
    const statusTool = mock(async () => { throw new Error('socket ended') })
    const harness = await createPollingHarness(statusTool)
    let pollCompleted = harness.waitForPoll()

    await harness.registeredTools.avenor_spawn.execute(
      'tool-1', { agent: 'explore', label: 'demo', wait: false }, undefined, undefined, harness.ctx,
    )
    await pollCompleted
    await harness.eventHandlers.session_shutdown()
    await harness.eventHandlers.session_start({}, harness.ctx)

    const emittedBeforeRestart = harness.emitted.length
    pollCompleted = harness.waitForPoll()
    await harness.registeredTools.avenor_spawn.execute(
      'tool-2', { agent: 'mule', label: 'restart', wait: false }, undefined, undefined, harness.ctx,
    )
    await pollCompleted

    const restartErrors = harness.emitted.slice(emittedBeforeRestart)
      .filter(([channel]) => channel === CHANNEL_POLL_ERROR)
    expect(restartErrors[0]?.[1]).toEqual(expect.objectContaining({ count: 1 }))
    await harness.eventHandlers.session_shutdown()
  })

  it('records spawn-status errors while wait:true falls back to polling', async () => {
    const controller = new AbortController()
    let releaseSingleton!: () => void
    const singletonPending = new Promise<void>(resolve => { releaseSingleton = resolve })
    let runStatusCalls = 0
    const statusTool = mock(async (args: { runId?: string } = {}) => {
      if (!args.runId) {
        await singletonPending
        return []
      }
      if (runStatusCalls++ === 1) controller.abort()
      throw new Error('socket ended')
    })
    const harness = await createPollingHarness(statusTool)
    const pollCompleted = harness.waitForPoll()

    await harness.registeredTools.avenor_spawn.execute(
      'tool-1',
      { agent: 'explore', label: 'demo', wait: true },
      controller.signal,
      undefined,
      harness.ctx,
    )
    releaseSingleton()
    await pollCompleted

    expect(harness.emitted).toContainEqual([
      CHANNEL_POLL_ERROR,
      expect.objectContaining({ source: 'spawn-status', runId: 'run-1', count: 1 }),
    ])
    await harness.eventHandlers.session_shutdown()
  })

  it('registers all expected tools, commands, renderers, and event handlers', async () => {
    const registeredTools: Record<string, any> = {}
    const registeredCommands: Record<string, any> = {}
    const registeredRenderers: string[] = []
    const eventHandlers: Map<string, any[]> = new Map()

    const mockBus = {
      emit: mock(() => {}),
      on: mock(() => () => {}),
    }
    const mockPi = {
      on: (event: string, handler: any) => {
        eventHandlers.set(event, [...(eventHandlers.get(event) ?? []), handler])
      },
      registerTool: (def: { name: string }) => {
        registeredTools[def.name] = def
      },
      registerCommand: (name: string, definition: any) => {
        registeredCommands[name] = definition
      },
      registerMessageRenderer: (type: string) => {
        registeredRenderers.push(type)
      },
      sendUserMessage: mock(() => {}),
      events: mockBus,
    }

    const statusToolMock = mock(async (args: { runId?: string } = {}) => {
      if (args.runId === 'run-wait') {
        return { run_id: 'run-wait', label: 'waited', status: 'done', runtime_id: 'rt-wait', backend: 'pi', agent: 'explore' }
      }
      return { run_id: 'run-1', label: 'demo', status: 'running', runtime_id: 'rt-1' }
    })
    const resultToolMock = mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true, output: 'hello world' }))
    const singletonInterruptAndPrompt = mock(async () => {})
    const singletonCancel = mock(async () => {})
    const externalInterruptAndPrompt = mock(async () => {})
    const externalClose = mock(() => {})
    const singletonClient = { close() {}, cancel: singletonCancel, interruptAndPrompt: singletonInterruptAndPrompt, events() { throw new Error('unused') } }
    const externalClient = { close: externalClose, cancel: async () => {}, interruptAndPrompt: externalInterruptAndPrompt, events() { throw new Error('unused') } }

    const spawnToolMock = mock(async (args: { supervisorId?: string; label?: string }) => ({
      run_id: args.label === 'roster-run'
        ? 'run-roster'
        : args.label === 'waited'
          ? 'run-wait'
          : args.supervisorId === '/tmp/external.sock' ? 'run-external' : 'run-1',
      label: args.label ?? 'demo',
      supervisor_id: args.supervisorId ?? '/tmp/sock',
      runtime_id: args.label === 'waited' ? 'rt-wait' : 'rt-1',
    }))

    await createExtension({
      spawnTool: spawnToolMock,
      statusTool: statusToolMock,
      eventsTool: mock(async () => ({ events: [] })),
      answerPermissionTool: mock(async () => ({ ok: true })),
      followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
      inspectTool: mock(async (args: { runId?: string } = {}) => {
        const result = makeInspectResult({ status: 'running' })
        result.snapshot = { ...result.snapshot, ended: false, stop_reason: undefined }
        result.status = { ...result.status, status: 'running', stop_reason: undefined, run_id: args?.runId ?? 'run-1' }
        return result
      }),
      resultTool: resultToolMock,
      shutdownTool: mock(async () => ({ ok: true })),
      observeRun: mock(() => null),
      dial: mock(async () => externalClient),
      Supervisor: class {
        static isCurrentInstance(supervisorId: string) {
          return supervisorId === '/tmp/sock'
        }

        static async get() {
          return { supervisorId: '/tmp/sock', getClient: () => singletonClient }
        }
      } as any,
    })(mockPi as any)

    expect(Object.keys(registeredTools)).toContain('avenor_spawn')
    expect(Object.keys(registeredTools)).toContain('avenor_status')
    expect(Object.keys(registeredTools)).toContain('avenor_result')
    expect(Object.keys(registeredTools)).toContain('avenor_inspect')
    expect(Object.keys(registeredTools)).toContain('avenor_answer_permission')
    expect(Object.keys(registeredTools)).toContain('avenor_follow_up')
    expect(Object.keys(registeredTools)).toContain('avenor_events')
    expect(Object.keys(registeredTools)).toContain('avenor_shutdown')
    for (const name of [
      'avenor_status',
      'avenor_result',
      'avenor_inspect',
      'avenor_answer_permission',
      'avenor_follow_up',
      'avenor_events',
      'avenor_shutdown',
    ]) {
      expect(typeof registeredTools[name]?.renderCall).toBe('function')
      expect(typeof registeredTools[name]?.renderResult).toBe('function')
    }
    expect(typeof registeredTools.avenor_spawn.renderResult).toBe('function')

    expect(Object.keys(registeredCommands)).toContain('avenor-status')
    expect(Object.keys(registeredCommands)).toContain('avenor-errors')
    expect(Object.keys(registeredCommands)).toContain('avenor-watch')
    expect(Object.keys(registeredCommands)).toContain('avenor-cancel')

    await withoutAmbientAgentProfile(async () => {
      await registeredTools.avenor_spawn.execute(
        'tool-1',
        { agent: 'explore', label: '\u001b[31mtest-pi-explore\u001b[0m', thinking: 'high', supervisor_id: '/tmp/sock', wait: false },
        undefined,
        undefined,
        {
          cwd: '/tmp',
          sessionManager: {
            getEntries: () => [
              { type: 'custom', customType: 'pi-agents:profile', data: { name: 'cloud' } },
            ],
          },
        },
      )
      expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({
        backend: 'pi',
        thinking: 'high',
        agentProfile: 'cloud',
      })
    })

    await withoutAmbientAgentProfile(async () => {
      await registeredTools.avenor_spawn.execute(
        'tool-roster',
        {
          roster_file: '/repo/roster.json',
          roster_entry: 'planner',
          label: 'roster-run',
          supervisor_id: '/tmp/sock',
          wait: false,
        },
        undefined,
        undefined,
        {
          cwd: '/tmp',
          sessionManager: {
            getEntries: () => [
              { type: 'custom', customType: 'pi-agents:profile', data: { name: 'cloud' } },
            ],
          },
        },
      )
      expect(spawnToolMock.mock.calls[1]?.[0]).toMatchObject({
        rosterFile: '/repo/roster.json',
        rosterEntry: 'planner',
        agentProfile: 'cloud',
      })
      expect(spawnToolMock.mock.calls[1]?.[0]).not.toHaveProperty('backend', 'pi')
      await expect(registeredTools.avenor_spawn.execute(
        'tool-invalid-roster',
        {
          roster_file: '/repo/roster.json',
          roster_entry: 'planner',
          backend: 'pi',
          wait: false,
        },
        undefined,
        undefined,
        { cwd: '/tmp' },
      )).rejects.toThrow('direct identity fields are disabled in roster mode')
      expect(spawnToolMock).toHaveBeenCalledTimes(2)
    })
    spawnToolMock.mockClear()

    const expectedCompletion = [{
      value: 'run-1',
      label: 'test-pi-explore (explore, run-1)',
    }]
    expect(registeredCommands['avenor-watch'].getArgumentCompletions('expl')).toEqual(expectedCompletion)
    expect(registeredCommands['avenor-cancel'].getArgumentCompletions('expl')).toEqual(expectedCompletion)

    await registeredTools.avenor_status.execute('tool-status', { run_id: 'run-1', view: 'lifecycle' })
    expect(statusToolMock).toHaveBeenCalledWith({ runId: 'run-1', supervisorId: undefined, view: 'lifecycle' })

    async function runWatchAction(runId: string, actionSpy: any, action: (overlay: any) => void): Promise<void> {
      let overlay: any
      let resolveLoaded!: () => void
      const loaded = new Promise<void>(resolve => { resolveLoaded = resolve })
      let renderCount = 0
      let resolveAction!: () => void
      const actionCompleted = new Promise<void>(resolve => { resolveAction = resolve })
      actionSpy.mockImplementation(async () => { resolveAction() })

      await registeredCommands['avenor-watch'].handler(runId, {
        hasUI: true,
        ui: {
          custom: async (factory: any) => {
            overlay = factory({
              terminal: { rows: 24 },
              requestRender() {
                if (++renderCount === 2) resolveLoaded()
              },
            }, {
              fg: (_color: string, text: string) => text,
              bg: (_color: string, text: string) => text,
              bold: (text: string) => text,
              italic: (text: string) => text,
            }, {}, () => {})
          },
        },
      })
      await loaded
      action(overlay)
      await actionCompleted
      overlay.dispose()
    }

    await runWatchAction('run-1', singletonInterruptAndPrompt, overlay => overlay.submit('continue'))
    expect(singletonInterruptAndPrompt).toHaveBeenCalledWith('rt-1', 'continue')
    await runWatchAction('run-1', singletonCancel, overlay => overlay.handleInput('\u0003'))
    expect(singletonCancel).toHaveBeenCalledWith('rt-1')

    await registeredTools.avenor_spawn.execute(
      'tool-external',
      { agent: 'explore', label: 'external', backend: 'opencode-acp', supervisor_id: '/tmp/external.sock', wait: false },
      undefined,
      undefined,
      { cwd: '/tmp' },
    )
    expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({ backend: 'opencode-acp' })

    let resolveExternalClose!: () => void
    const externalClosed = new Promise<void>(resolve => { resolveExternalClose = resolve })
    externalClose.mockImplementation(() => { resolveExternalClose() })
    await runWatchAction('run-external', externalInterruptAndPrompt, overlay => overlay.submit('continue'))
    await externalClosed
    expect(externalInterruptAndPrompt).toHaveBeenCalledWith('rt-1', 'continue')
    expect(externalClose).toHaveBeenCalledTimes(1)

    await registeredTools.avenor_spawn.execute(
      'tool-wait',
      { agent: 'explore', label: 'waited', supervisor_id: '/tmp/sock', wait: true },
      undefined,
      undefined,
      { cwd: '/tmp' },
    )
    expect(mockBus.emit).toHaveBeenCalledWith(
      CHANNEL_RUN_TERMINAL,
      expect.objectContaining({
        runId: 'run-wait',
        runtimeId: 'rt-wait',
        supervisorKey: expect.stringMatching(/^supervisor:[0-9a-f]{16}$/),
        status: 'done',
        backend: 'pi',
      }),
    )
    const waitedTerminalPayload = mockBus.emit.mock.calls
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, payload]) => payload as Record<string, unknown>)
      .find(payload => payload.runId === 'run-wait')
    expect(waitedTerminalPayload).not.toHaveProperty('supervisorId')

    const result = await registeredTools.avenor_result.execute('tool-result', { run_id: 'run-1', timeout: '5m' })
    expect(result.content[0].text).toContain('"output": "hello world"')
    expect(result.details).toEqual({ run_id: 'run-1', label: 'demo', status: 'done', ready: true, output: 'hello world' })
    const renderedResult = registeredTools.avenor_result.renderResult(
      result,
      { expanded: false, isPartial: false },
      { fg: (_color: string, value: string) => value, bold: (value: string) => value },
      { args: { run_id: 'run-1' } },
    ).render(2_000).join('\n').trimEnd()
    expect(renderedResult).toContain('Result: demo — done')
    expect(renderedResult).not.toContain('"output"')
    expect(resultToolMock).toHaveBeenCalledWith({
      runId: 'run-1',
      supervisorId: undefined,
      wait: undefined,
      timeout: '5m',
      signal: undefined,
    })
    expect(mockBus.emit).toHaveBeenCalledWith(
      CHANNEL_RUN_TERMINAL,
      expect.objectContaining({
        runId: 'run-1',
        runtimeId: 'rt-1',
        supervisorKey: expect.stringMatching(/^supervisor:[0-9a-f]{16}$/),
        status: 'done',
      }),
    )
    const resultTerminalPayload = mockBus.emit.mock.calls
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, payload]) => payload as Record<string, unknown>)
      .find(payload => payload.runId === 'run-1')
    expect(resultTerminalPayload).not.toHaveProperty('supervisorId')

    for (const handler of eventHandlers.get('session_shutdown') ?? []) await handler()

    expect(registeredRenderers).toContain('avenor-active-runs')
    expect(eventHandlers.has('session_start')).toBe(true)
    expect(eventHandlers.has('session_shutdown')).toBe(true)
    expect(eventHandlers.has('before_agent_start')).toBe(true)
  })

  it('keeps exact raw JSON payloads and details for every non-spawn tool callback', async () => {
    const registeredTools: Record<string, any> = {}
    const statusPayload = {
      run_id: 'status-run', label: 'status worker', status: 'done', runtime_id: 'rt-status', latest_seq: 1,
    }
    const resultPayload = {
      run_id: 'result-run', label: 'result worker', status: 'done', ready: true, output: 'result output',
    }
    const inspectResult = makeInspectResult({ run_id: 'inspect-run', label: 'inspect worker' })
    const permissionPayload = { ok: true }
    const followUpPayload = { run_id: 'follow-up-run', label: 'follow-up worker' }
    const eventsPayload = { events: [{ event: 'agent.message', seq: 1, delta: 'hello' }] }
    const shutdownPayload = { ok: true, cleaned_up: ['/tmp/one'] }

    const mockPi = {
      on: () => {},
      registerTool: (def: { name: string }) => {
        registeredTools[def.name] = def
      },
      registerCommand: () => {},
      registerMessageRenderer: () => {},
      sendUserMessage: () => {},
      events: { emit: mock(() => {}), on: mock(() => () => {}) },
    }

    await createExtension({
      spawnTool: mock(async () => ({ run_id: 'spawn-run', label: 'spawn worker', supervisor_id: '/tmp/sock', runtime_id: 'rt-spawn' })),
      statusTool: mock(async () => statusPayload),
      resultTool: mock(async () => resultPayload),
      inspectTool: mock(async () => inspectResult),
      answerPermissionTool: mock(async () => permissionPayload),
      followUpTool: mock(async () => followUpPayload),
      eventsTool: mock(async () => eventsPayload),
      shutdownTool: mock(async () => shutdownPayload),
      observeRun: mock(() => null),
      dial: mock(async () => ({ close() {}, cancel: async () => {}, interruptAndPrompt: async () => {}, events() { throw new Error('unused') } })),
      Supervisor: class {} as any,
    })(mockPi as any)

    const status = await registeredTools.avenor_status.execute('tool-status', { run_id: 'status-run' })
    expect(status.content[0]?.text).toBe(JSON.stringify(statusPayload, null, 2))
    expect(status.details).toEqual(statusPayload)

    const runResult = await registeredTools.avenor_result.execute('tool-result', { run_id: 'result-run' })
    expect(runResult.content[0]?.text).toBe(JSON.stringify(resultPayload, null, 2))
    expect(runResult.details).toEqual(resultPayload)

    const inspectPayload = buildInspectPayload(inspectResult)
    const inspect = await registeredTools.avenor_inspect.execute('tool-inspect', { run_id: 'inspect-run' })
    expect(inspect.content[0]?.text).toBe(JSON.stringify(inspectPayload, null, 2))
    expect(inspect.details).toEqual(inspectPayload)

    const permission = await registeredTools.avenor_answer_permission.execute('tool-permission', {
      run_id: 'status-run', option_id: 'allow_once', request_id: 'request-1', message: 'approved',
    })
    expect(permission.content[0]?.text).toBe(JSON.stringify(permissionPayload, null, 2))
    expect(permission.details).toEqual(permissionPayload)

    const followUp = await registeredTools.avenor_follow_up.execute('tool-follow-up', {
      run_id: 'status-run', message: 'continue', label: 'follow-up worker',
    })
    expect(followUp.content[0]?.text).toBe(JSON.stringify(followUpPayload, null, 2))
    expect(followUp.details).toEqual(followUpPayload)

    const events = await registeredTools.avenor_events.execute('tool-events', {
      run_id: 'status-run', types: ['agent.message'], limit: 10,
    })
    expect(events.content[0]?.text).toBe(JSON.stringify(eventsPayload, null, 2))
    expect(events.details).toEqual(eventsPayload)

    const shutdown = await registeredTools.avenor_shutdown.execute('tool-shutdown', { supervisor_id: '/tmp/sock', force: true })
    expect(shutdown.content[0]?.text).toBe(JSON.stringify(shutdownPayload, null, 2))
    expect(shutdown.details).toEqual(shutdownPayload)
  })

  it('registers avenor_inspect with bounded JSON output', async () => {
    let inspectToolDef: any
    const mockPi = {
      on: () => {},
      registerTool: (def: any) => {
        if (def.name === 'avenor_inspect') inspectToolDef = def
      },
      registerCommand: () => {},
      registerMessageRenderer: () => {},
      sendUserMessage: () => {},
      events: { emit: mock(() => {}), on: mock(() => () => {}) },
    }

    await createExtension({
      ...({
        spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock', runtime_id: 'rt-1' })),
        statusTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', runtime_id: 'rt-1', final_output: 'answer' })),
        eventsTool: mock(async () => ({ events: [] })),
        answerPermissionTool: mock(async () => ({ ok: true })),
        followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
        inspectTool: mock(async () => makeInspectResult()),
        resultTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true, output: 'hello world' })),
        shutdownTool: mock(async () => ({ ok: true })),
        observeRun: mock(() => null),
        dial: mock(async () => ({ close() {}, cancel: async () => {}, prompt: async () => {}, events() { throw new Error('unused') } })),
        Supervisor: class {
          static async get() {
            return { supervisorId: '/tmp/sock' }
          }
        } as any,
      })
    })(mockPi as any)

    const result = await inspectToolDef.execute('tool-1', { run_id: 'run-1' })
    expect(result.content[0].text).toContain('"final_output": "hello world"')
    expect(result.details).toEqual(buildInspectPayload(makeInspectResult()))
    expect(result.details.snapshot.identity.run_id).toBe('run-1')
    const rendered = inspectToolDef.renderResult(
      result,
      { expanded: false, isPartial: false },
      { fg: (_color: string, value: string) => value, bold: (value: string) => value },
      { args: { run_id: 'run-1' } },
    ).render(2_000).join('\n').trimEnd()
    expect(rendered).toContain('Inspect: demo — done')
    expect(rendered).not.toContain('"snapshot"')
  })

  it('tracks colliding run ids from two supervisors independently', async () => {
    const FACTORY = '/tmp/avenor-mcp-factory-retro.sock'
    const ADVISOR = '/tmp/avenor-mcp-advisor.sock'

    const factoryStatus: StatusResult = {
      run_id: 'rt_1', label: 'factory', status: 'running', runtime_id: 'rt_1',
      session_id: '019fc5c7-b8c0-761e-a23a-0295e921aaf6',
    }
    const advisorRunning: StatusResult = {
      run_id: 'rt_1', label: 'advisor', status: 'running', runtime_id: 'rt_1',
      session_id: '019fc5c7-f8e5-76c0-9075-9ec6c836d3de',
    }
    const statusCalls: Array<{ runId?: string; supervisorId?: string }> = []
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      statusCalls.push(args)
      if (!args.runId) return []                                                                              // unqualified list = current singleton (no runs)
      if (args.supervisorId === FACTORY) return factoryStatus
      if (args.supervisorId === ADVISOR) return advisorRunning
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })

    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async (args: { supervisorId?: string; label?: string }) => ({
        run_id: 'rt_1',
        label: args.label ?? 'spawned',
        supervisor_id: args.supervisorId ?? '/tmp/sock',
        runtime_id: 'rt_1',
      })),
    })

    // Both supervisors independently expose a run_id of rt_1.
    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'factory', supervisor_id: FACTORY, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'advisor', supervisor_id: ADVISOR, wait: false }, undefined, undefined, h.ctx)

    // Both runs stay tracked and are polled through their own supervisor; the
    // shared run id must not collapse them into one tracking slot.
    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    const detail = h.setWidget.mock.calls.filter(([name]) => name === 'avenor-status-detail').at(-1)?.[1] as string[]
    expect(detail).toHaveLength(2)
    expect(detail.join('\n')).toContain('factory')
    expect(detail.join('\n')).toContain('advisor')
    expect(statusCalls).toContainEqual({ runId: 'rt_1', supervisorId: FACTORY })
    expect(statusCalls).toContainEqual({ runId: 'rt_1', supervisorId: ADVISOR })

    await h.eventHandlers.session_shutdown()
  })

  it('routes the completion message to the right supervisor for colliding run ids', async () => {
    const FACTORY = '/tmp/avenor-mcp-factory-retro.sock'
    const ADVISOR = '/tmp/avenor-mcp-advisor.sock'

    const factoryStatus: StatusResult = {
      run_id: 'rt_1', label: 'factory', status: 'running', runtime_id: 'rt_1',
      session_id: '019fc5c7-b8c0-761e-a23a-0295e921aaf6',
    }
    const advisorRunning: StatusResult = {
      run_id: 'rt_1', label: 'advisor', status: 'running', runtime_id: 'rt_1',
      session_id: '019fc5c7-f8e5-76c0-9075-9ec6c836d3de',
    }
    let advisorDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return []
      if (args.supervisorId === FACTORY) return factoryStatus
      if (args.supervisorId === ADVISOR) {
        return advisorDone ? { ...advisorRunning, status: 'done', final_output: 'advisor final answer' } : advisorRunning
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })

    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async (args: { supervisorId?: string; label?: string }) => ({
        run_id: 'rt_1',
        label: args.label ?? 'spawned',
        supervisor_id: args.supervisorId ?? '/tmp/sock',
        runtime_id: 'rt_1',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'factory', supervisor_id: FACTORY, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'advisor', supervisor_id: ADVISOR, wait: false }, undefined, undefined, h.ctx)

    // The advisor run finishes: its completion must combine the advisor label,
    // output, and session without touching the still-running factory run.
    let resolveCompletion!: () => void
    const completionSent = new Promise<void>(resolve => { resolveCompletion = resolve })
    // Install the sendUserMessage hook BEFORE flipping advisorDone so no
    // completion can slip past the no-op mock and leave the test hanging.
    h.sendUserMessage.mockImplementation(() => resolveCompletion())
    advisorDone = true
    await completionSent

    // Only the advisor reaches terminal status, so the last message is its
    // completion. Use at(-1) so future permission notifications cannot shift it.
    const [completionText] = h.sendUserMessage.mock.calls.at(-1) ?? []
    expect(String(completionText)).toContain('Sub-agent "advisor" finished')
    expect(String(completionText)).toContain('> advisor final answer')
    expect(String(completionText)).toContain('`019fc5c7-f8e5-76c0-9075-9ec6c836d3de`')
    expect(String(completionText)).not.toContain('019fc5c7-b8c0-761e-a23a-0295e921aaf6')

    const terminal = h.emitted.filter(([c]) => c === CHANNEL_RUN_TERMINAL).map(([, p]) => p as Record<string, unknown>)
    const advisorTerminal = terminal.find(t => t.status === 'done' && t.label === 'advisor')
    expect(advisorTerminal).toMatchObject({ runId: 'rt_1', label: 'advisor', status: 'done' })
    const supervisorKey = (id: string) => `supervisor:${createHash('sha256').update(id).digest('hex').slice(0, 16)}`
    expect(advisorTerminal?.supervisorKey).toBe(supervisorKey(ADVISOR))

    // The factory run is untouched and remains tracked and active.
    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    const after = h.setWidget.mock.calls.filter(([name]) => name === 'avenor-status-detail').at(-1)?.[1] as string[]
    expect(after).toHaveLength(1)
    expect(after[0]).toContain('factory')
    expect(after[0]).not.toContain('advisor')

    await h.eventHandlers.session_shutdown()
  })

  it('avenor_shutdown removes only the tracked runs for the target supervisor', async () => {
    const A = '/tmp/supervisor-a.sock'
    const B = '/tmp/supervisor-b.sock'
    const runA: StatusResult = { run_id: 'rt_1', label: 'a-run', status: 'running', runtime_id: 'rt_1' }
    const runB: StatusResult = { run_id: 'rt_1', label: 'b-run', status: 'running', runtime_id: 'rt_1' }
    const statusCalls: Array<{ runId?: string; supervisorId?: string }> = []
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      statusCalls.push(args)
      if (!args.runId) return []
      if (args.supervisorId === A) return runA
      if (args.supervisorId === B) return runB
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const shutdownTool = mock(async () => ({ ok: true }))

    const h = await createMultiSupervisorHarness({
      statusTool,
      shutdownTool,
      spawnTool: mock(async (args: { supervisorId?: string; label?: string }) => ({
        run_id: 'rt_1',
        label: args.label ?? 'spawned',
        supervisor_id: args.supervisorId ?? '/tmp/sock',
        runtime_id: 'rt_1',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'a-run', supervisor_id: A, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'b-run', supervisor_id: B, wait: false }, undefined, undefined, h.ctx)
    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    expect(h.setWidget.mock.calls.filter(([n]) => n === 'avenor-status-detail').at(-1)?.[1]).toHaveLength(2)

    const bBefore = statusCalls.filter(c => c.supervisorId === B).length
    const aBefore = statusCalls.filter(c => c.supervisorId === A).length
    await h.registeredTools.avenor_shutdown.execute('t-shutdown', { supervisor_id: B })
    expect(shutdownTool).toHaveBeenCalledWith({ supervisorId: B, force: undefined })

    // The B supervisor run is gone; the A run survives and keeps being polled.
    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    const detail = h.setWidget.mock.calls.filter(([n]) => n === 'avenor-status-detail').at(-1)?.[1] as string[]
    expect(detail).toHaveLength(1)
    expect(detail[0]).toContain('a-run')
    expect(statusCalls.filter(c => c.supervisorId === B).length).toBe(bBefore)
    expect(statusCalls.filter(c => c.supervisorId === A).length).toBeGreaterThan(aBefore)

    await h.eventHandlers.session_shutdown()
  })
  it('avenor_shutdown of the singleton keeps custom-supervisor runs', async () => {
    const A = '/tmp/supervisor-a.sock'
    const SINGLETON = '/tmp/avenor-mcp-singleton.sock'
    const runA: StatusResult = { run_id: 'rt_1', label: 'a-run', status: 'running', runtime_id: 'rt_1' }
    const runS: StatusResult = { run_id: 'rt_2', label: 'singleton-run', status: 'running', runtime_id: 'rt_2' }
    const statusCalls: Array<{ runId?: string; supervisorId?: string }> = []
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      statusCalls.push(args)
      if (!args.runId) return []
      if (args.supervisorId === A) return runA
      // The singleton answers for both its implicit (undefined) and explicit socket.
      if (args.supervisorId === undefined || args.supervisorId === SINGLETON) return runS
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const shutdownTool = mock(async () => ({ ok: true }))

    const h = await createMultiSupervisorHarness({
      statusTool,
      shutdownTool,
      supervisor: {
        isCurrentInstance: (supervisorId: string) => supervisorId === SINGLETON,
      },
      spawnTool: mock(async (args: { supervisorId?: string; label?: string }) => ({
        run_id: args.label === 'singleton-run' ? 'rt_2' : 'rt_1',
        label: args.label ?? 'spawned',
        supervisor_id: args.supervisorId ?? SINGLETON,
        runtime_id: args.label === 'singleton-run' ? 'rt_2' : 'rt_1',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'a-run', supervisor_id: A, wait: false }, undefined, undefined, h.ctx)
    // One singleton run without an explicit socket (undefined namespace) and one
    // carrying the singleton's explicit socket; both belong to the singleton.
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'singleton-run', wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t3', { agent: 'explore', label: 'singleton-run', supervisor_id: SINGLETON, wait: false }, undefined, undefined, h.ctx)
    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    expect(h.setWidget.mock.calls.filter(([n]) => n === 'avenor-status-detail').at(-1)?.[1]).toHaveLength(3)

    // Shut down the singleton: only the singleton-scoped runs disappear; the
    // custom-supervisor run stays tracked and keeps being polled.
    const aBefore = statusCalls.filter(c => c.supervisorId === A).length
    await h.registeredTools.avenor_shutdown.execute('t-shutdown', {})
    expect(shutdownTool).toHaveBeenCalledWith({ supervisorId: undefined, force: undefined })

    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    const detail = h.setWidget.mock.calls.filter(([n]) => n === 'avenor-status-detail').at(-1)?.[1] as string[]
    expect(detail).toHaveLength(1)
    expect(detail[0]).toContain('a-run')
    expect(detail[0]).not.toContain('singleton-run')
    expect(statusCalls.filter(c => c.supervisorId === SINGLETON).length).toBeGreaterThan(0)
    expect(statusCalls.filter(c => c.supervisorId === A).length).toBeGreaterThan(aBefore)

    await h.eventHandlers.session_shutdown()
  })
})
