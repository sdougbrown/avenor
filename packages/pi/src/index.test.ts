import { describe, expect, it, mock } from 'bun:test'
import { createHash } from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { Supervisor, messageTool as realMessageTool, type StatusResult, type WorkflowResult } from '@dougbots/avenor-core'
import extensionFactory, {
  type ExtensionDeps,
  buildCompletionText,
  buildInspectPayload,
  CHANNEL_DELIVERY_ERROR,
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
import { makeInspectResult, makeMessageProvider, settleWithin, startExtensionHarness, waitFor } from './test-fixtures.js'

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

async function createPollingHarness(statusTool: any, observeRun = mock(() => null)) {
  let resolvePollCompleted: (() => void) | undefined
  const waitForPoll = () => new Promise<void>(resolve => { resolvePollCompleted = resolve })
  const h = await startExtensionHarness({
    deps: { statusTool, observeRun },
    onEmit: (channel) => {
      if (channel === CHANNEL_POLL_COMPLETED) resolvePollCompleted?.()
    },
  })
  return { ...h, waitForPoll }
}

async function createMultiSupervisorHarness(options: {
  pollIntervalMs?: number
  idleSendTimeoutMs?: number
  statusTool: any
  spawnTool?: any
  shutdownTool?: any
  observeRun?: any
  resultTool?: any
  inspectTool?: any
  brokerReceiveTool?: any
  dial?: any
  subAgentEnv?: { brokerUrl: string; runId: string; token: string }
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
  const matchWaiters: Array<{ match: (payload: any) => boolean; resolve: () => void }> = []
  /** Resolve once a poll completes whose payload satisfies `match` (checked
   * against the frozen public payload, e.g. `payload.entries`). Deterministic
   * alternative to sleeping on a timer. */
  const waitPollMatching = (match: (payload: any) => boolean) => new Promise<void>(resolve => {
    matchWaiters.push({ match, resolve })
  })
  const h = await startExtensionHarness({
    deps: {
      statusTool: options.statusTool,
      ...(options.spawnTool !== undefined && { spawnTool: options.spawnTool }),
      ...(options.shutdownTool !== undefined && { shutdownTool: options.shutdownTool }),
      ...(options.observeRun !== undefined && { observeRun: options.observeRun }),
      ...(options.resultTool !== undefined && { resultTool: options.resultTool }),
      ...(options.inspectTool !== undefined && { inspectTool: options.inspectTool }),
      ...(options.brokerReceiveTool !== undefined && { brokerReceiveTool: options.brokerReceiveTool }),
      ...(options.dial !== undefined && { dial: options.dial }),
      ...(options.supervisor !== undefined && { Supervisor: options.supervisor as any }),
    },
    extOptions: {
      pollIntervalMs: options.pollIntervalMs ?? 5,
      ...(options.idleSendTimeoutMs !== undefined && { idleSendTimeoutMs: options.idleSendTimeoutMs }),
      ...(options.subAgentEnv !== undefined && { subAgentEnv: options.subAgentEnv }),
    },
    ui: { confirm },
    onEmit: (channel, payload) => {
      if (channel === CHANNEL_POLL_COMPLETED) {
        pollCount++
        for (let i = pollWaiters.length - 1; i >= 0; i--) {
          if (pollCount >= pollWaiters[i].n) pollWaiters.splice(i, 1)[0].resolve()
        }
        for (let i = matchWaiters.length - 1; i >= 0; i--) {
          if (matchWaiters[i].match(payload)) matchWaiters.splice(i, 1)[0].resolve()
        }
      }
    },
  })
  return { ...h, confirm, waitPolls, waitPollMatching }
}

/** Confirm an idle completion the way Pi does: the prompt we sent comes back
 * through message_end as a user message. */
function confirmDelivery(h: { eventHandlers: Record<string, any> }, text: string) {
  return h.eventHandlers.message_end({
    type: 'message_end',
    message: { role: 'user', content: text, timestamp: Date.now() },
  })
}

/** Open the watch inspector for a run and send one follow-up through it. */
async function watchResume(
  harness: Awaited<ReturnType<typeof startExtensionHarness>>,
  runId: string,
  text: string,
) {
  let overlay: any
  await harness.registeredCommands['avenor-watch'].handler(runId, {
    hasUI: true,
    ui: {
      custom: async (factory: any) => {
        overlay = factory(
          { terminal: { rows: 24 }, requestRender() {} },
          {
            fg: (_color: string, text: string) => text,
            bg: (_color: string, text: string) => text,
            bold: (text: string) => text,
            italic: (text: string) => text,
          },
          {},
          () => {},
        )
      },
    },
  })
  await waitFor(() => Boolean(overlay?.state?.status))
  overlay.submit(text)
  return overlay
}

describe('Avenor Pi extension', () => {
  it('exports a function', () => {
    expect(typeof extensionFactory).toBe('function')
  })

  it('routes avenor_message ask and withdraw through the bound provider', async () => {
    const calls: unknown[] = []
    const harness = await startExtensionHarness({
      deps: {
        messageTool: mock(async (args: unknown) => {
          calls.push(args)
          return { action: (args as { action?: string }).action, outcome: 'replied', messageId: 'msg-1', reply: 'ok', text: 'ok' }
        }),
      },
    })
    const result = await harness.registeredTools.avenor_message.execute(
      'tool-ask',
      { action: 'ask', target: 'run-1', message: 'hello', timeout_ms: 2500 },
    )
    expect(calls.at(-1)).toEqual({ action: 'ask', target: 'run-1', message: 'hello', timeout_ms: 2500 })
    expect(result).toEqual({
      content: [{ type: 'text', text: 'ok' }],
      details: { action: 'ask', outcome: 'replied', text: 'ok', message_id: 'msg-1', reply: 'ok' },
    })

    const withdrawn = await harness.registeredTools.avenor_message.execute(
      'tool-withdraw',
      { action: 'withdraw', message_id: 'msg-1' },
    )
    expect(calls.at(-1)).toEqual({ action: 'withdraw', message_id: 'msg-1' })
    expect(withdrawn.details.action).toBe('withdraw')
  })

  it('propagates a message tool rejection', async () => {
    const harness = await startExtensionHarness({
      deps: {
        messageTool: mock(async () => {
          throw new Error('no pending ask')
        }),
      },
    })
    await expect(
      harness.registeredTools.avenor_message.execute('tool-reply', { action: 'reply', reply_to: 'ask-1', message: 'x' }),
    ).rejects.toThrow('no pending ask')
  })

  it('forwards the merged workflow call to the shared runner unchanged', async () => {
    const runTool = mock(async () => ({ action: 'status', value: { status: 'active' } }) as WorkflowResult)
    const harness = await startExtensionHarness({ deps: { runWorkflowTool: runTool } })
    const tool = harness.registeredTools.avenor_workflow
    const outcome = await tool.execute('tool-1', { action: 'status', workflow_id: 'wf-1', supervisor_id: 'sup-a' })
    expect(runTool).toHaveBeenCalledWith({ action: 'status', workflow_id: 'wf-1', supervisor_id: 'sup-a' })
    expect(JSON.parse(outcome.content[0].text)).toEqual({ status: 'active' })
    expect(outcome.details).toEqual({ status: 'active' })
  })

  it('enriches controller status details with an advisory candidate count', async () => {
    const runTool = mock(async (args: any) => {
      if (args.action === 'ready') return { action: 'ready', value: { candidates: [{}, {}] } } as WorkflowResult
      return { action: 'controller_status', value: { controller_id: 'ctl', desired_state: 'enabled', inflight: 1 } } as WorkflowResult
    })
    const harness = await startExtensionHarness({ deps: { runWorkflowTool: runTool } })
    const tool = harness.registeredTools.avenor_workflow
    const outcome = await tool.execute('tool-1', { action: 'controller_status', controller_id: 'ctl' })
    expect(outcome.details.candidate_count).toBe(2)
    expect(outcome.details.controller_id).toBe('ctl')
  })

  it('omits the candidate count when the ready query fails', async () => {
    const runTool = mock(async (args: any) => {
      if (args.action === 'ready') throw new Error('-32601 method not found')
      return { action: 'controller_status', value: { controller_id: 'ctl', desired_state: 'enabled' } } as WorkflowResult
    })
    const harness = await startExtensionHarness({ deps: { runWorkflowTool: runTool } })
    const tool = harness.registeredTools.avenor_workflow
    const outcome = await tool.execute('tool-1', { action: 'controller_status', controller_id: 'ctl' })
    expect(outcome.details.candidate_count).toBeUndefined()
    expect(outcome.details.controller_id).toBe('ctl')
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
      'Call `avenor_inspect` with `run_id: "run-1"` for the bounded transcript/snapshot, or `avenor_message` with `action: "resume"` to iterate.',
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
    const harness = await createPollingHarness(mock(async () => ({ runs: [], count: 0 })))

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

    // The still-tracked run restarts polling at session start; after the stop
    // reset it, the error count must begin again from 1.
    const emittedBeforeRestart = harness.emitted.length
    pollCompleted = harness.waitForPoll()
    await harness.eventHandlers.session_start({}, harness.ctx)
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
        return { runs: [], count: 0 }
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

  it('routes a watch resume through the bound provider and tracks the resumed run', async () => {
    const resumeSpy = mock(async () => ({
      outcome: 'resumed' as const,
      runId: 'run-2',
      runtimeId: 'rt-2',
      label: 'follow-up',
      sentinelFile: '/runs/run-2/sentinel.done',
      onEvent: '/runs/run-2/events.log',
    }))
    const provider = makeMessageProvider({
      resume: resumeSpy,
      registerResumed: () => {},
    })
    const hostProviderFor = mock(async (_supervisorId?: string) => provider)
    const statusTool = mock(async (args: { runId?: string } = {}) => args.runId === 'run-2'
      ? { run_id: 'run-2', label: 'follow-up', status: 'done', runtime_id: 'rt-2' }
      : { run_id: 'run-1', label: 'demo', status: 'done', runtime_id: 'rt-1' })
    const harness = await startExtensionHarness({
      deps: { hostProviderFor, statusTool, messageTool: realMessageTool },
      extOptions: { subAgentEnv: null },
    })
    const overlay = await watchResume(harness, 'run-1', 'continue')
    await waitFor(() => statusTool.mock.calls.some(call => (call[0] as { runId?: string })?.runId === 'run-2'))
    expect(hostProviderFor).toHaveBeenCalledWith(undefined)
    expect(resumeSpy).toHaveBeenCalledWith(expect.objectContaining({ target: 'run-1', message: 'continue' }))
    overlay.dispose()
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('closes the client a non-singleton binding dialled when avenor_shutdown stops it', async () => {
    const OTHER = '/tmp/other.sock'
    const close = mock(() => {})
    const otherClient = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-other' })),
      close,
    }
    const getSupervisorClient = mock(async () => ({
      client: otherClient,
      isSingleton: false,
      sup: null,
      supervisorId: OTHER,
    }))
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        spawnTool: mock(async () => ({
          run_id: 'run_1',
          label: 'demo',
          supervisor_id: OTHER,
          runtime_id: 'rt_1',
        })),
      },
      extOptions: { subAgentEnv: null },
    })
    // Only a tracked run's supervisorId reaches a non-default binding, so seed
    // one and let the watch resume dial it.
    await harness.registeredTools.avenor_spawn.execute(
      'tool-1',
      { agent: 'explore', label: 'demo', supervisor_id: OTHER, wait: false },
      undefined,
      undefined,
      harness.ctx,
    )
    const overlay = await watchResume(harness, 'run_1', 'continue')
    await waitFor(() => getSupervisorClient.mock.calls.length === 1)
    expect(getSupervisorClient).toHaveBeenCalledWith(OTHER, { owner: true })
    expect(close).not.toHaveBeenCalled()

    await harness.registeredTools.avenor_shutdown.execute('s', { supervisor_id: OTHER })
    expect(close).toHaveBeenCalledTimes(1)
    // The default binding was never dialled, so the shutdown left it alone.
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    overlay.dispose()
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('resumes a tracked run through the host binding in a child session', async () => {
    const resumeSpy = mock(async () => ({
      outcome: 'resumed' as const,
      runId: 'run-2',
      runtimeId: 'rt-2',
      label: 'follow-up',
      sentinelFile: '/runs/run-2/sentinel.done',
      onEvent: '/runs/run-2/events.log',
    }))
    const provider = makeMessageProvider({ resume: resumeSpy, registerResumed: () => {} })
    const hostProviderFor = mock(async (_supervisorId?: string) => provider)
    const statusTool = mock(async (args: { runId?: string } = {}) => args.runId === 'run-2'
      ? { run_id: 'run-2', label: 'follow-up', status: 'done', runtime_id: 'rt-2' }
      : { run_id: 'run-1', label: 'demo', status: 'done', runtime_id: 'rt-1' })
    const previousFetch = globalThis.fetch
    // The child poll has no broker here; an empty queue keeps it off the network.
    globalThis.fetch = mock(async () => Response.json([])) as unknown as typeof fetch
    const harness = await startExtensionHarness({
      deps: { hostProviderFor, statusTool, messageTool: realMessageTool },
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
    })
    let overlay: any
    try {
      overlay = await watchResume(harness, 'run-1', 'continue')
      // The child provider's resume always throws not_permitted, so a resume
      // that completes and reaches the tracker proves the host binding ran it.
      await waitFor(() => statusTool.mock.calls.some(call => (call[0] as { runId?: string })?.runId === 'run-2'))
      expect(hostProviderFor).toHaveBeenCalledTimes(1)
      expect(resumeSpy).toHaveBeenCalledWith(expect.objectContaining({ target: 'run-1', message: 'continue' }))
    } finally {
      overlay?.dispose()
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('publishes the harness session name and republishes it only when it changes', async () => {
    let sessionName: string | undefined = 'my-session'
    const published: Array<Record<string, unknown>> = []
    let polls = 0
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      if (path === '/session_name') {
        published.push(body)
        return Response.json({ run_id: body.run_id, label: body.name })
      }
      if (path === '/poll-control') {
        polls++
        return Response.json([])
      }
      return Response.json({})
    }) as unknown as typeof fetch
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => sessionName,
    })
    try {
      await waitFor(() => published.length === 1)
      expect(published[0]).toEqual({ name: 'my-session', run_id: 'rt_child', token: 'tok' })

      sessionName = 'renamed'
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => published.length === 2)
      expect(published[1]).toMatchObject({ name: 'renamed' })

      // An unchanged name is not republished. Wait for two more child poll ticks
      // so a republish would have had time to land before asserting it did not.
      const pollsBefore = polls
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => polls >= pollsBefore + 2)
      expect(published).toHaveLength(2)

      // Clearing the name in the harness clears the broker's copy, so the run
      // stops shadowing whatever name the model set.
      sessionName = undefined
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => published.length === 3)
      expect(published[2]).toMatchObject({ name: '' })
    } finally {
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('skips the session-name publish when the context has no session manager', async () => {
    const published: Array<Record<string, unknown>> = []
    let polls = 0
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      if (path === '/session_name') {
        published.push(JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>)
        return Response.json({ run_id: 'rt_child', label: '' })
      }
      if (path === '/poll-control') {
        polls++
        return Response.json([])
      }
      return Response.json({})
    }) as unknown as typeof fetch
    // The session manager is present but has no getSessionName method (the
    // typeof guard, not the absent-manager branch): neither session_start nor a
    // turn may crash, and neither may publish a name (an empty one would
    // clobber whatever label the broker still holds).
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionManager: {},
    })
    try {
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => polls >= 2)
      expect(published).toHaveLength(0)
    } finally {
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('retries a failed session-name publish on the next turn', async () => {
    const published: Array<Record<string, unknown>> = []
    let failNext = true
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      if (path === '/session_name') {
        published.push(body)
        if (failNext) {
          failNext = false
          return Response.json({ error: 'busy' }, { status: 503 })
        }
        return Response.json({ run_id: body.run_id, label: body.name })
      }
      if (path === '/poll-control') return Response.json([])
      return Response.json({})
    }) as unknown as typeof fetch
    const originalError = console.error
    const errorLog = mock(() => {})
    console.error = errorLog as any
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => 'my-session',
    })
    try {
      await waitFor(() => published.length === 1)
      // Wait for the failure to be recorded before the next turn reads the name.
      await waitFor(() => errorLog.mock.calls.length === 1)
      // The failed publish is forgotten, so the next turn sends the same name.
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => published.length === 2)
      expect(published[1]).toMatchObject({ name: 'my-session' })
    } finally {
      console.error = originalError
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('serializes publishes so the latest name lands last', async () => {
    let sessionName: string | undefined = 'first'
    const received: string[] = []
    const gates: Array<() => void> = []
    let polls = 0
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      if (path === '/session_name') {
        received.push(String(body.name))
        // Hold the first publish open so the second is read while it is in flight.
        if (received.length === 1) await new Promise<void>(resolve => gates.push(resolve))
        return Response.json({ run_id: body.run_id, label: body.name })
      }
      if (path === '/poll-control') {
        polls++
        return Response.json([])
      }
      return Response.json({})
    }) as unknown as typeof fetch
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => sessionName,
    })
    try {
      await waitFor(() => received.length === 1)
      sessionName = 'second'
      const pollsBefore = polls
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      // The second publish waits behind the first instead of overtaking it.
      await waitFor(() => polls >= pollsBefore + 2)
      expect(received).toEqual(['first'])
      gates[0]?.()
      await waitFor(() => received.length === 2)
      expect(received).toEqual(['first', 'second'])
    } finally {
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('keeps the newer publish record when an older in-flight publish fails', async () => {
    let sessionName: string | undefined = 'first'
    const received: string[] = []
    const gates: Array<() => void> = []
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      if (path === '/session_name') {
        received.push(String((JSON.parse(String(init?.body ?? '{}')) as { name?: string }).name))
        // Hold the older publish open so the newer name is recorded while it is
        // still in flight, then fail it; every later publish succeeds.
        if (received.length === 1) {
          await new Promise<void>(resolve => gates.push(resolve))
          return Response.json({ error: 'boom' }, { status: 503 })
        }
        return Response.json({ run_id: 'rt_child', label: 'x' })
      }
      if (path === '/poll-control') return Response.json([])
      return Response.json({})
    }) as unknown as typeof fetch
    const originalError = console.error
    const errorLog = mock(() => {})
    console.error = errorLog as any
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => sessionName,
    })
    try {
      await waitFor(() => received.length === 1)
      sessionName = 'second'
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      gates[0]?.()
      // The newer publish lands after the older one failed.
      await waitFor(() => received.length === 2)
      expect(received).toEqual(['first', 'second'])
      // The failure of the older attempt did not clear the newer record, so a
      // turn on the unchanged name does not republish 'second'.
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => errorLog.mock.calls.length >= 1)
      expect(received).toEqual(['first', 'second'])
    } finally {
      console.error = originalError
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('republishes the name after a failed publish is followed by a session start', async () => {
    let sessionName: string | undefined = 'first'
    const received: string[] = []
    const gates: Array<() => void> = []
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      if (path === '/session_name') {
        received.push(String((JSON.parse(String(init?.body ?? '{}')) as { name?: string }).name))
        // The older publish fails once the gate releases; every later publish
        // succeeds.
        if (received.length === 1) {
          await new Promise<void>(resolve => gates.push(resolve))
          return Response.json({ error: 'boom' }, { status: 503 })
        }
        return Response.json({ run_id: 'rt_child', label: 'x' })
      }
      if (path === '/poll-control') return Response.json([])
      return Response.json({})
    }) as unknown as typeof fetch
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => sessionName,
    })
    try {
      // The newer name is recorded while the older publish is in flight, so a
      // failed older publish leaves a record a session start must reset.
      await waitFor(() => received.length === 1)
      sessionName = 'second'
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      gates[0]?.()
      await waitFor(() => received.length === 2)
      expect(received).toEqual(['first', 'second'])
      // The session start resets the published record, so the same name is
      // published again instead of being assumed live.
      await harness.eventHandlers.session_start({}, harness.ctx)
      await waitFor(() => received.length === 3)
      expect(received[2]).toBe('second')
    } finally {
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('republishes the name when a session start follows a failed publish', async () => {
    const received: string[] = []
    let failNext = true
    let polls = 0
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      if (path === '/session_name') {
        received.push(String(body.name))
        if (failNext) {
          failNext = false
          return Response.json({ error: 'busy' }, { status: 503 })
        }
        return Response.json({ run_id: body.run_id, label: body.name })
      }
      if (path === '/poll-control') {
        polls++
        return Response.json([])
      }
      return Response.json({})
    }) as unknown as typeof fetch
    const originalError = console.error
    const errorLog = mock(() => {})
    console.error = errorLog as any
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => 'my-session',
    })
    try {
      // The start's publish fails, so no record is live when the session
      // start event fires.
      await waitFor(() => received.length === 1)
      // Wait for the failure to be recorded before the session start reads the name.
      await waitFor(() => errorLog.mock.calls.length === 1)
      // The session start republishes the name even though the failed
      // publish left no record.
      await harness.eventHandlers.session_start({}, harness.ctx)
      await waitFor(() => received.length === 2)
      expect(received).toEqual(['my-session', 'my-session'])
      // The republished record is live, so a turn on the unchanged name does
      // not publish a third time.
      const pollsBefore = polls
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => polls >= pollsBefore + 2)
      expect(received).toEqual(['my-session', 'my-session'])
    } finally {
      console.error = originalError
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('stops republishing a name the broker refuses, until the name changes', async () => {
    let sessionName: string | undefined = 'two\nlines'
    const published: Array<Record<string, unknown>> = []
    let polls = 0
    const previousFetch = globalThis.fetch
    globalThis.fetch = mock(async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      if (path === '/session_name') {
        published.push(body)
        if (String(body.name).includes('\n')) {
          return Response.json({ code: 'invalid-name', error: 'name must be 1-64 runes' }, { status: 400 })
        }
        return Response.json({ run_id: body.run_id, label: body.name })
      }
      if (path === '/poll-control') {
        polls++
        return Response.json([])
      }
      return Response.json({})
    }) as unknown as typeof fetch
    const originalError = console.error
    const errorLog = mock(() => {})
    console.error = errorLog as any
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
      sessionName: () => sessionName,
    })
    try {
      await waitFor(() => published.length === 1)
      // Wait for the refusal to be recorded before the next turn reads the name.
      await waitFor(() => errorLog.mock.calls.length === 1)
      // The refused name is deterministic: no turn republishes it.
      const pollsBefore = polls
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => polls >= pollsBefore + 2)
      expect(published).toHaveLength(1)
      // A different name is still published.
      sessionName = 'a-valid-name'
      await harness.eventHandlers.before_agent_start({}, harness.ctx)
      await waitFor(() => published.length === 2)
      expect(published[1]).toMatchObject({ name: 'a-valid-name' })
    } finally {
      console.error = originalError
      globalThis.fetch = previousFetch
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('registers all expected tools, commands, renderers, and event handlers', async () => {
    const registeredTools: Record<string, any> = {}
    const registeredCommands: Record<string, any> = {}
    const registeredRenderers: string[] = []
    const eventHandlers: Map<string, any[]> = new Map()

    const mockBus = {
      emit: mock((_channel: string, _payload: unknown) => {}),
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
    const resultToolMock = mock(async (_args: { runId?: string }) => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true, output: 'hello world' }))
    const singletonInterruptAndPrompt = mock(async () => {})
    const singletonCancel = mock(async () => {})
    const ownerInterruptAndPrompt = mock(async () => {})
    const ownerClose = mock(() => {})
    const dialInterruptAndPrompt = mock(async () => {})
    const singletonClient = { close() {}, cancel: singletonCancel, interruptAndPrompt: singletonInterruptAndPrompt, events() { throw new Error('unused') } }
    const ownerClient = { close: ownerClose, cancel: async () => {}, interruptAndPrompt: ownerInterruptAndPrompt, events() { throw new Error('unused') } }
    const dialClient = { close() {}, cancel: async () => {}, interruptAndPrompt: dialInterruptAndPrompt, events() { throw new Error('unused') } }
    const attachOwnerMock = mock(async () => ownerClient)

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
      messageTool: mock(async () => ({ outcome: 'sent', messageId: 'm-1', text: 'sent' })),
      inspectTool: mock(async (args: { runId?: string } = {}) => {
        const result = makeInspectResult({ status: 'running' })
        result.snapshot = { ...result.snapshot, ended: false, stop_reason: undefined }
        result.status = { ...result.status, status: 'running', stop_reason: undefined, run_id: args?.runId ?? 'run-1' }
        return result
      }),
      resultTool: resultToolMock,
      brokerReceiveTool: mock(async () => ({ asks: [] })),
      killTool: mock(async () => ({ killed: true, run_id: 'run-1' })),
      runWorkflowTool: mock(async (args: unknown) => ({ action: 'status', value: {} } as WorkflowResult)),
      shutdownTool: mock(async () => ({ ok: true, cleaned_up: [] })),
      observeRun: mock(() => null),
      dial: mock(async () => dialClient) as unknown as ExtensionDeps['dial'],
      attachOwner: attachOwnerMock as unknown as ExtensionDeps['attachOwner'],
      Supervisor: class {
        static isCurrentInstance(supervisorId: string) {
          return supervisorId === '/tmp/sock'
        }

        static async get() {
          return { supervisorId: '/tmp/sock', getClient: () => singletonClient }
        }
      } as any,
    }, { subAgentEnv: null })(mockPi as any)

    expect(Object.keys(registeredTools)).toContain('avenor_spawn')
    expect(Object.keys(registeredTools)).toContain('avenor_status')
    expect(Object.keys(registeredTools)).toContain('avenor_result')
    expect(Object.keys(registeredTools)).toContain('avenor_inspect')
    expect(Object.keys(registeredTools)).toContain('avenor_answer_permission')
    expect(Object.keys(registeredTools)).toContain('avenor_message')
    expect(Object.keys(registeredTools)).toContain('avenor_peers')
    expect(Object.keys(registeredTools)).toContain('avenor_workflow')
    expect(Object.keys(registeredTools)).not.toContain('avenor_follow_up')
    expect(Object.keys(registeredTools)).not.toContain('avenor_ask')
    expect(Object.keys(registeredTools)).not.toContain('avenor_reply')
    expect(Object.keys(registeredTools)).not.toContain('avenor_cancel')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_cancel')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_status')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_controller_status')
    expect(Object.keys(registeredTools)).toContain('avenor_events')
    expect(Object.keys(registeredTools)).toContain('avenor_shutdown')
    expect(Object.keys(registeredTools)).toContain('avenor_kill')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_controller_list')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_controller_create')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_controller_enable')
    expect(Object.keys(registeredTools)).not.toContain('avenor_workflow_controller_disable')
    for (const name of [
      'avenor_status',
      'avenor_result',
      'avenor_inspect',
      'avenor_answer_permission',
      'avenor_message',
      'avenor_events',
      'avenor_shutdown',
      'avenor_workflow',
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

    let resolveOwnerClose!: () => void
    const ownerClosed = new Promise<void>(resolve => { resolveOwnerClose = resolve })
    ownerClose.mockImplementation(() => { resolveOwnerClose() })
    await runWatchAction('run-external', ownerInterruptAndPrompt, overlay => overlay.submit('continue'))
    await ownerClosed
    // A watch mutation on another process's supervisor goes through the owner
    // attach, never the credential-free dial.
    expect(attachOwnerMock).toHaveBeenCalledWith('/tmp/external.sock')
    expect(ownerInterruptAndPrompt).toHaveBeenCalledWith('rt-1', 'continue')
    expect(ownerClose).toHaveBeenCalledTimes(1)
    expect(dialInterruptAndPrompt).not.toHaveBeenCalled()

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
    expect(resultToolMock.mock.calls[0]?.[0]).toMatchObject({
      runId: 'run-1',
      supervisorId: undefined,
      wait: undefined,
      timeout: '5m',
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
    const messagePayload = {
      action: 'resume',
      outcome: 'resumed',
      runId: 'follow-up-run',
      label: 'follow-up worker',
      text: 'resumed "follow-up worker" as run "follow-up-run"',
    }
    const eventsPayload = { events: [{ event: 'agent.message', seq: 1, delta: 'hello' }] }
    const shutdownPayload = { ok: true, cleaned_up: ['/tmp/one'] }
    const killPayload = { killed: true, run_id: 'run-1' }
    const killCalls: Array<Record<string, unknown>> = []

    const mockPi = {
      on: () => {},
      registerTool: (def: { name: string }) => {
        registeredTools[def.name] = def
      },
      registerCommand: () => {},
      registerMessageRenderer: () => {},
      sendUserMessage: () => {},
      events: { emit: mock((_channel: string, _payload: unknown) => {}), on: mock(() => () => {}) },
    }

    await createExtension({
      spawnTool: mock(async () => ({ run_id: 'spawn-run', label: 'spawn worker', supervisor_id: '/tmp/sock', runtime_id: 'rt-spawn' })),
      statusTool: mock(async () => statusPayload),
      resultTool: mock(async () => resultPayload),
      inspectTool: mock(async () => inspectResult),
      answerPermissionTool: mock(async () => permissionPayload),
      messageTool: mock(async () => messagePayload),
      hostProviderFor: mock(async () => makeMessageProvider()),
      eventsTool: mock(async () => eventsPayload),
      shutdownTool: mock(async () => shutdownPayload),
      killTool: mock(async (args: { runId: string; supervisorId?: string }) => {
        killCalls.push(args)
        return killPayload
      }),
      brokerReceiveTool: mock(async () => ({ asks: [] })),
      runWorkflowTool: mock(async (args: unknown) => ({ action: 'status', value: {} } as WorkflowResult)),
      observeRun: mock(() => null),
      dial: mock(async () => ({ close() {}, cancel: async () => {}, interruptAndPrompt: async () => {}, events() { throw new Error('unused') } })) as unknown as ExtensionDeps['dial'],
      attachOwner: mock(async () => ({ close() {}, cancel: async () => {}, interruptAndPrompt: async () => {}, events() { throw new Error('unused') } })) as unknown as ExtensionDeps['attachOwner'],
      Supervisor: class {} as any,
    }, { subAgentEnv: null })(mockPi as any)

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

    const resumed = await registeredTools.avenor_message.execute('tool-message', {
      action: 'resume', target: 'status-run', message: 'continue', label: 'follow-up worker',
    })
    expect(resumed.content[0]?.text).toBe(messagePayload.text)
    expect(resumed.details).toEqual({
      action: 'resume',
      outcome: 'resumed',
      run_id: 'follow-up-run',
      label: 'follow-up worker',
      text: 'resumed "follow-up worker" as run "follow-up-run"',
    })

    const events = await registeredTools.avenor_events.execute('tool-events', {
      run_id: 'status-run', types: ['agent.message'], limit: 10,
    })
    expect(events.content[0]?.text).toBe(JSON.stringify(eventsPayload, null, 2))
    expect(events.details).toEqual(eventsPayload)

    const shutdown = await registeredTools.avenor_shutdown.execute('tool-shutdown', { supervisor_id: '/tmp/sock', force: true })
    expect(shutdown.content[0]?.text).toBe(JSON.stringify(shutdownPayload, null, 2))
    expect(shutdown.details).toEqual(shutdownPayload)

    const kill = await registeredTools.avenor_kill.execute('tool-kill', { run_id: 'run-1' })
    expect(kill.content[0]?.text).toBe(JSON.stringify(killPayload, null, 2))
    expect(kill.details).toEqual(killPayload)
    expect(killCalls.at(-1)).toEqual({ runId: 'run-1', supervisorId: undefined })
  })

  it('registers avenor_inspect with bounded JSON output', async () => {
    const harness = await startExtensionHarness({
      deps: { inspectTool: mock(async () => makeInspectResult()) },
      extOptions: { subAgentEnv: null },
    })
    const inspectToolDef = harness.registeredTools.avenor_inspect

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

  it('renders a spawn result whose stored details are absent', async () => {
    const harness = await startExtensionHarness({ extOptions: { subAgentEnv: null } })
    // A transcript record carries whatever details the session stored; pi's HTML
    // export hands them to renderResult unvalidated, so an absent value must render.
    const rendered = harness.registeredTools.avenor_spawn.renderResult(
      { content: [{ type: 'text', text: 'spawned run-1' }], details: undefined },
      { expanded: false, isPartial: false },
      { fg: (_color: string, value: string) => value, bold: (value: string) => value },
      { args: {} },
    ).render(2_000).join('\n').trimEnd()
    expect(rendered).toBe('spawned run-1')
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
      if (!args.runId) return { runs: [], count: 0 }                                                                              // unqualified list = current singleton (no runs)
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
      if (!args.runId) return { runs: [], count: 0 }
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
    // The idle send only stops being tracked once Pi confirms it landed.
    await confirmDelivery(h, String(completionText))

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

  it('delivers the completion for the last active run even when polling stops', async () => {
    const SOCK = '/tmp/single-run.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'last answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })

    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    let resolveCompletion!: () => void
    const completionSent = new Promise<void>(resolve => { resolveCompletion = resolve })
    h.sendUserMessage.mockImplementation(() => resolveCompletion())
    done = true
    await completionSent

    // The completion is delivered on the same terminal tick even though this
    // is the only run (polling stops right after). The brief notify also fires.
    const [completionText] = h.sendUserMessage.mock.calls.at(-1) ?? []
    expect(String(completionText)).toContain('Sub-agent "worker" finished')
    expect(String(completionText)).toContain('last answer')

    await h.eventHandlers.session_shutdown()
  })

  it('does not notify a run whose result avenor_result is awaiting or consumed', async () => {
    const SOCK = '/tmp/consumed.sock'
    let done = false
    let resolveResult!: (v: unknown) => void
    const resultGate = new Promise(resolve => { resolveResult = resolve })
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'consumed answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const resultTool = mock(async () => {
      const value = await resultGate
      return value
    })

    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
      resultTool,
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    // The agent calls avenor_result while the run is still pending; this sets
    // blocking=true for the duration of the awaited call.
    const resultPromise = h.registeredTools.avenor_result.execute('t2', { run_id: 'r1', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    // Run reaches terminal while avenor_result is awaiting: polling ticks must
    // neither post the brief notify nor the full completion message. Wait
    // deterministically for the tick that first observes the run as done.
    done = true
    await h.waitPollMatching(payload =>
      payload.entries.some((e: { label: string; status: string }) => e.label === 'worker' && e.status === 'done'))
    const completions = h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))
    const notifies = h.notify.mock.calls.map(([c]) => String(c[0])).filter(c => c.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(0)
    expect(notifies).toHaveLength(0)

    // avenor_result then resolves successfully: marks consumed + deletes. The
    // run is removed from tracking and the poll loop already stopped (it was
    // the only, blocking, run), so nothing else can inject a completion.
    resolveResult({ run_id: 'r1', label: 'worker', status: 'done', ready: true, output: 'consumed answer' })
    await resultPromise
    const completionsAfter = h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completionsAfter).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('does not consume the run when avenor_result returns non-ready (completion still delivered)', async () => {
    const SOCK = '/tmp/interrupted.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'interrupted answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    // avenor_result returns non-ready (aborted/timeout before the result): it
    // must NOT mark the run consumed, so the completion is still delivered.
    const resultTool = mock(async () => ({ run_id: 'r1', label: 'worker', status: 'running', ready: false }))

    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
      resultTool,
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const interrupted = await h.registeredTools.avenor_result.execute('t2', { run_id: 'r1', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    expect(interrupted.details).toMatchObject({ ready: false })

    // The run later reaches terminal with no avenor_result waiting (blocking
    // was reset on interrupt) and no consumed flag: the completion must flow.
    let resolveCompletion!: () => void
    const completionSent = new Promise<void>(resolve => { resolveCompletion = resolve })
    h.sendUserMessage.mockImplementation(() => resolveCompletion())
    done = true
    await completionSent
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(1)
    expect(completions[0]).toContain('interrupted answer')

    await h.eventHandlers.session_shutdown()
  })

  it('consumes a pending completion by label and canonical result run id', async () => {
    const SOCK = '/tmp/queue-label.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    // The caller passes the label; the result tool answers with the canonical
    // run id. Both must land on the same tracked completion notice.
    const resultTool = mock(async () => ({ run_id: 'r1', label: 'worker', status: 'done', ready: true, output: 'answer' }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'worker' && e.status === 'done'))
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const result = await h.registeredTools.avenor_result.execute('t2', { run_id: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    expect(result.details).toMatchObject({ run_id: 'r1', ready: true })
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('batches only unread notices and keeps unread visible with zero active runs', async () => {
    const SOCK = '/tmp/queue-batch.sock'
    let allDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        const final = allDone ? 'done' : 'running'
        if (args.runId === 'ra') return { run_id: 'ra', label: 'alpha', status: final, runtime_id: 'rt-a', final_output: allDone ? 'alpha answer' : undefined }
        return { run_id: 'rb', label: 'beta', status: final, runtime_id: 'rt-b', final_output: allDone ? 'beta answer' : undefined }
      }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
    const resultTool = mock(async () => ({ run_id: 'ra', label: 'alpha', status: 'done', ready: true, output: 'alpha answer' }))
    const spawnTool = mock(async (args: { label?: string }) => ({
      run_id: args.label === 'alpha' ? 'ra' : 'rb',
      label: args.label ?? 'spawned',
      supervisor_id: SOCK,
      runtime_id: args.label === 'alpha' ? 'rt-a' : 'rt-b',
    }))
    const h = await createMultiSupervisorHarness({ statusTool, resultTool, spawnTool })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    allDone = true
    await h.waitPollMatching(payload => payload.entries.filter((e: { status: string }) => e.status === 'done').length === 2)

    // Polling stops with no active runs, but both terminal completions remain visible.
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:2'))
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:2')
    expect(h.setWidget.mock.calls.at(-1)).toEqual(['avenor-status', undefined])

    await h.registeredTools.avenor_result.execute('t3', { run_id: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled?.continue).toBe(true)
    expect(settled?.entries).toHaveLength(1)
    const content = String(settled?.entries?.[0]?.content ?? '')
    expect(content).toContain('Sub-agent "beta" finished')
    expect(content).toContain('beta answer')
    expect(content).not.toContain('alpha answer')
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "'))
    expect(completions).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('clears an unread pending notice when a session starts', async () => {
    const SOCK = '/tmp/session-start-clears.sock'
    let done = false
    let gate: Promise<void> | undefined
    let releaseGate!: () => void
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (gate) await gate
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))

    // Park the next poll so the session-start clear is observable before polling
    // can recompute the notice for the still-tracked terminal run.
    gate = new Promise<void>(resolve => { releaseGate = resolve })
    await h.eventHandlers.session_start({}, h.ctx)

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()

    releaseGate()
    await h.eventHandlers.session_shutdown()
  })

  it('restarts polling at session start so a cleared notice is rebuilt', async () => {
    const SOCK = '/tmp/session-start-repoll.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'repoll answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    // The lone terminal run queues its notice and stops the poll loop.
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))

    // A session replacement clears the queued notice; only a restarted poll
    // loop can rebuild it, with no new spawn or resume to restart polling.
    const statusCallsBefore = h.setStatus.mock.calls.length
    await h.eventHandlers.session_start({}, h.ctx)
    await waitFor(() => h.setStatus.mock.calls.length > statusCallsBefore)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    // The rebuilt notice is still delivered at the next completed settlement.
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled?.entries).toHaveLength(1)
    expect(String(settled?.entries[0]?.content)).toContain('repoll answer')

    await h.eventHandlers.session_shutdown()
  })

  it('retains a notice after a non-ready result and does not flush it on an aborted settlement', async () => {
    const SOCK = '/tmp/queue-nonready.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const resultTool = mock(async () => ({ run_id: 'r1', label: 'worker', status: 'running', ready: false }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'worker' && e.status === 'done'))
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const result = await h.registeredTools.avenor_result.execute('t2', { run_id: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    expect(result.details).toMatchObject({ ready: false })
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const aborted = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'aborted' },
      h.ctx,
    )
    expect(aborted).toBeUndefined()
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled?.continue).toBe(true)
    expect(String(settled?.entries?.[0]?.content ?? '')).toContain('answer')
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')

    await h.eventHandlers.session_shutdown()
  })

  it('does not deliver a notice whose result was consumed during final-output resolution', async () => {
    const SOCK = '/tmp/queue-inspect-race.sock'
    let done = false
    let inspectStarted!: () => void
    const inspectStartedGate = new Promise<void>(resolve => { inspectStarted = resolve })
    let releaseInspect!: () => void
    const inspectGate = new Promise<void>(resolve => { releaseInspect = resolve })
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const inspectTool = mock(async () => {
      inspectStarted()
      await inspectGate
      return makeInspectResult({ status: 'done', final_output: 'late answer' })
    })
    const resultTool = mock(async () => ({ run_id: 'r1', label: 'worker', status: 'done', ready: true, output: 'answer' }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      inspectTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await inspectStartedGate

    const result = await h.registeredTools.avenor_result.execute('t2', { run_id: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    expect(result.details).toMatchObject({ ready: true })
    const statusCallsBefore = h.setStatus.mock.calls.length
    releaseInspect()
    await waitFor(() => h.setStatus.mock.calls.length > statusCallsBefore)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    // Exactly one terminal event: the stale poll path that resumes after the
    // inspect resolves must not emit a second one.
    const terminals = h.emitted
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, payload]) => payload as Record<string, unknown>)
      .filter(payload => payload.runId === 'r1')
    expect(terminals).toHaveLength(1)

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('releases a label-resolved tracked run when the result reconciles to another run', async () => {
    const SOCK = '/tmp/result-reconcile-other.sock'
    let alphaDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId !== SOCK) return { run_id: args.runId, label: args.runId, status: 'running' }
      if (args.runId === 'ra') {
        return alphaDone
          ? { run_id: 'ra', label: 'alpha', status: 'done', runtime_id: 'rt-a', final_output: 'alpha answer' }
          : { run_id: 'ra', label: 'alpha', status: 'running', runtime_id: 'rt-a' }
      }
      return { run_id: 'rb', label: 'beta', status: 'done', runtime_id: 'rt-b', final_output: 'beta answer' }
    })
    // The caller asks for the label alpha; the result tool reconciles to run rb.
    const resultTool = mock(async () => ({ run_id: 'rb', label: 'beta', status: 'done', ready: true, output: 'beta answer' }))
    const spawnTool = mock(async (args: { label?: string }) => ({
      run_id: args.label === 'alpha' ? 'ra' : 'rb',
      label: args.label ?? 'spawned',
      supervisor_id: SOCK,
      runtime_id: args.label === 'alpha' ? 'rt-a' : 'rt-b',
    }))
    const h = await createMultiSupervisorHarness({ statusTool, resultTool, spawnTool })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const result = await h.registeredTools.avenor_result.execute('t3', { run_id: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    expect(result.details).toMatchObject({ run_id: 'rb', ready: true })
    const betaTerminal = h.emitted
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, payload]) => payload as Record<string, unknown>)
      .filter(payload => payload.runId === 'rb')
    expect(betaTerminal).toHaveLength(1)

    // alpha must not stay blocking forever: its own later completion is delivered.
    alphaDone = true
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('Sub-agent "alpha" finished')))
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "'))
    expect(completions.filter(t => t.includes('Sub-agent "alpha" finished'))).toHaveLength(1)
    expect(completions.filter(t => t.includes('Sub-agent "beta" finished'))).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('reuses a cached completion notice instead of re-inspecting a terminal run', async () => {
    const SOCK = '/tmp/notice-cache.sock'
    let workerDone = false
    const inspectTool = mock(async () => makeInspectResult({ status: 'done' }))
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId !== SOCK) return { run_id: args.runId, label: args.runId, status: 'running' }
      if (args.runId === 'r1') {
        return workerDone
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: 'r2', label: 'other', status: 'running', runtime_id: 'rt2' }
    })
    const spawnTool = mock(async (args: { label?: string }) => ({
      run_id: args.label === 'worker' ? 'r1' : 'r2',
      label: args.label ?? 'spawned',
      supervisor_id: SOCK,
      runtime_id: args.label === 'worker' ? 'rt1' : 'rt2',
    }))
    const h = await createMultiSupervisorHarness({ statusTool, inspectTool, spawnTool })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'other', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    workerDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'worker' && e.status === 'done'))
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))
    // Keep polling the second, still-running run so the terminal run is revisited.
    await waitFor(() => statusTool.mock.calls.filter(call => call[0]?.runId === 'r1').length >= 3)

    expect(inspectTool).toHaveBeenCalledTimes(1)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    await h.eventHandlers.session_shutdown()
  })

  it('does not let one landed prompt confirm a colliding sibling without a send', async () => {
    const A = '/tmp/confirm-collision-a.sock'
    const B = '/tmp/confirm-collision-b.sock'
    let aDone = false
    let bDone = false
    // Identical run id, label, and output: the notice texts collide exactly.
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      const done = args.supervisorId === A ? aDone : bDone
      return {
        run_id: 'rt_1',
        label: 'worker',
        status: done ? 'done' : 'running',
        runtime_id: 'rt_1',
        ...(done && { final_output: 'same answer' }),
      }
    })
    const spawnTool = mock(async (args: { supervisorId?: string }) => ({
      run_id: 'rt_1',
      label: 'worker',
      supervisor_id: args.supervisorId,
      runtime_id: 'rt_1',
    }))
    const h = await createMultiSupervisorHarness({ statusTool, spawnTool })

    const spawnA = { agent: 'explore', label: 'worker', supervisor_id: A, wait: false }
    const spawnB = { agent: 'explore', label: 'worker', supervisor_id: B, wait: false }

    // A finishes while the session is idle: its notice is sent as a prompt.
    await h.registeredTools.avenor_spawn.execute('t1', spawnA, undefined, undefined, h.ctx)
    aDone = true
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('same answer')))

    // B finishes while the session is busy: its notice queues, never sent.
    h.ctx.isIdle = () => false
    await h.registeredTools.avenor_spawn.execute('t2', spawnB, undefined, undefined, h.ctx)
    bDone = true
    await h.waitPollMatching(payload => payload.entries.filter((e: { status: string }) => e.status === 'done').length === 2)
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:2'))

    // The one landed prompt confirms only the run whose send is in flight; the
    // colliding sibling's unsent notice survives.
    const sent = h.sendUserMessage.mock.calls.map(([t]) => String(t)).find(t => t.includes('same answer'))!
    await confirmDelivery(h, sent)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    // The surviving notice is still delivered at settlement.
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled?.entries).toHaveLength(1)
    expect(String(settled?.entries[0]?.content)).toContain('same answer')

    await h.eventHandlers.session_shutdown()
  })

  it('does not consume a colliding label without an explicit supervisor', async () => {
    const A = '/tmp/queue-collision-a.sock'
    const B = '/tmp/queue-collision-b.sock'
    let allDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === A) {
        return allDone
          ? { run_id: 'rt_1', label: 'worker', status: 'done', runtime_id: 'rt_1', final_output: 'A answer' }
          : { run_id: 'rt_1', label: 'worker', status: 'running', runtime_id: 'rt_1' }
      }
      if (args.supervisorId === B) {
        return allDone
          ? { run_id: 'rt_1', label: 'worker', status: 'done', runtime_id: 'rt_1', final_output: 'B answer' }
          : { run_id: 'rt_1', label: 'worker', status: 'running', runtime_id: 'rt_1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const resultTool = mock(async () => ({ run_id: 'rt_1', label: 'worker', status: 'done', ready: true, output: 'answer' }))
    const spawnTool = mock(async (args: { supervisorId?: string }) => ({
      run_id: 'rt_1',
      label: 'worker',
      supervisor_id: args.supervisorId ?? '/tmp/sock',
      runtime_id: 'rt_1',
    }))
    const h = await createMultiSupervisorHarness({ statusTool, resultTool, spawnTool })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: A, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'worker', supervisor_id: B, wait: false }, undefined, undefined, h.ctx)
    allDone = true
    await h.waitPollMatching(payload => payload.entries.filter((e: { status: string }) => e.status === 'done').length === 2)
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:2'))

    const terminalPayloads = () => h.emitted
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, payload]) => payload as Record<string, unknown>)
    const supervisorKey = (id: string) => `supervisor:${createHash('sha256').update(id).digest('hex').slice(0, 16)}`
    const beforeUntracked = terminalPayloads()
    expect(beforeUntracked).toHaveLength(2)

    // The same label and run id exist on both supervisors. Without an explicit
    // supervisor, no tracked run may be consumed.
    await h.registeredTools.avenor_result.execute('t3', { run_id: 'worker', wait: false }, undefined, undefined, h.ctx)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:2')
    // An untracked result owns no terminal event: polling or a bound result
    // path owns delivery, so this call must not invent a singleton terminal.
    const afterUntracked = terminalPayloads()
    expect(afterUntracked).toHaveLength(beforeUntracked.length)
    expect(afterUntracked.every(payload => payload.supervisorKey !== 'singleton')).toBe(true)

    await h.registeredTools.avenor_result.execute('t4', { run_id: 'worker', supervisor_id: A, wait: false }, undefined, undefined, h.ctx)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')
    const afterTracked = terminalPayloads()
    expect(afterTracked).toHaveLength(beforeUntracked.length)
    expect(afterTracked.filter(payload => payload.supervisorKey === supervisorKey(A))).toHaveLength(1)

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    const content = String(settled?.entries?.[0]?.content ?? '')
    expect(settled?.entries).toHaveLength(1)
    expect(content).toContain('B answer')
    expect(content).not.toContain('A answer')

    await h.eventHandlers.session_shutdown()
  })

  it('delivers an idle completion immediately exactly once', async () => {
    const SOCK = '/tmp/queue-idle.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'idle answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'worker' && e.status === 'done'))
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('idle answer')))

    // An idle send is unconfirmed until it lands, so the notice stays unread
    // while the prompt is in flight.
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(1)
    expect(completions[0]).toContain('idle answer')

    await confirmDelivery(h, completions[0]!)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')

    await h.eventHandlers.session_shutdown()
  })

  it('delivers a waiting spawn completion without an explicit settlement', async () => {
    const SOCK = '/tmp/waiting-handback.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (done) return { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'waiting answer' }
        return {
          run_id: 'r1', label: 'worker', status: 'waiting', runtime_id: 'rt1',
          pending_permission: { description: 'allow write' },
        }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const observeRun = mock(async () => ({
      subscribe: (cb: (snapshot: unknown) => void) => {
        cb({ last_event: undefined, transcript: [], tools: [], live_tools: [], pending_permission: true, ended: false })
        return () => {}
      },
      snapshot: () => ({ last_event: undefined, transcript: [], tools: [], live_tools: [], pending_permission: true, ended: false }),
      close: async () => {},
    }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      observeRun,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    const spawned = await h.registeredTools.avenor_spawn.execute(
      't1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx,
    )
    expect(spawned.details).toMatchObject({ status: 'waiting' })

    // The spawn handed control back at a stopping point, so it no longer owns
    // delivery: the completion must arrive on its own.
    done = true
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('Sub-agent "worker" finished')))
    const completion = String(h.sendUserMessage.mock.calls.map(([t]) => String(t)).find(t => t.includes('Sub-agent "worker" finished')))
    expect(completion).toContain('waiting answer')

    await h.eventHandlers.session_shutdown()
  })

  it('delivers a non-terminal spawn fallthrough completion without an explicit settlement', async () => {
    const SOCK = '/tmp/running-handback.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'fallthrough answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const observeRun = mock(async () => ({
      subscribe: (cb: (snapshot: unknown) => void) => {
        cb({ last_event: undefined, transcript: [], tools: [], live_tools: [], pending_permission: false, ended: true })
        return () => {}
      },
      snapshot: () => ({ last_event: undefined, transcript: [], tools: [], live_tools: [], pending_permission: false, ended: true }),
      close: async () => {},
    }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      observeRun,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    const spawned = await h.registeredTools.avenor_spawn.execute(
      't1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx,
    )
    expect(spawned.details).toMatchObject({ status: 'running' })

    done = true
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('Sub-agent "worker" finished')))
    const completion = String(h.sendUserMessage.mock.calls.map(([t]) => String(t)).find(t => t.includes('Sub-agent "worker" finished')))
    expect(completion).toContain('fallthrough answer')

    await h.eventHandlers.session_shutdown()
  })

  it('starts an idle send for every terminal run without waiting for the first', async () => {
    const SOCK = '/tmp/idle-concurrent.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (args.runId === 'ra') {
          return done
            ? { run_id: 'ra', label: 'alpha', status: 'done', runtime_id: 'rt-a', final_output: 'alpha answer' }
            : { run_id: 'ra', label: 'alpha', status: 'running', runtime_id: 'rt-a' }
        }
        return done
          ? { run_id: 'rb', label: 'beta', status: 'done', runtime_id: 'rt-b', final_output: 'beta answer' }
          : { run_id: 'rb', label: 'beta', status: 'running', runtime_id: 'rt-b' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async (args: { label?: string }) => ({
        run_id: args.label === 'alpha' ? 'ra' : 'rb',
        label: args.label ?? 'spawned',
        supervisor_id: SOCK,
        runtime_id: args.label === 'alpha' ? 'rt-a' : 'rt-b',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    done = true

    // The send is fire-and-forget, so both terminal runs in one poll cycle must
    // start their own delivery instead of parking behind the first.
    await waitFor(() => h.sendUserMessage.mock.calls.filter(([t]) => String(t).includes('Sub-agent "')).length >= 2)
    const completions = h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "') && t.includes('finished'))
    expect(completions.filter(t => t.includes('"alpha"'))).toHaveLength(1)
    expect(completions.filter(t => t.includes('"beta"'))).toHaveLength(1)
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:2'))
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:2')

    for (const completion of completions) await confirmDelivery(h, completion)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')

    // No notice was left queued for a later settlement, and no duplicate send
    // is pending after both completions were confirmed.
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()
    expect(h.sendUserMessage.mock.calls.filter(([t]) => String(t).includes('Sub-agent "')).length).toBe(2)

    await h.eventHandlers.session_shutdown()
  })

  it('includes nestedCount and backend in a bound result terminal payload', async () => {
    const SOCK = '/tmp/result-payload.sock'
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return { run_id: 'rc', label: 'solo', status: 'running', runtime_id: 'rt-c', backend: 'pi', pid: 424242 }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const resultTool = mock(async () => ({ run_id: 'rc', label: 'solo', status: 'done', ready: true, output: 'solo answer' }))
    const dial = mock(async () => ({ list: async () => [{ status: 'running' }], close: () => {} }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      dial,
      spawnTool: mock(async () => ({ run_id: 'rc', label: 'solo', supervisor_id: SOCK, runtime_id: 'rt-c' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'solo', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.waitPollMatching(payload => payload.entries.some((e: { nestedCount?: number }) => e.nestedCount === 1))

    await h.registeredTools.avenor_result.execute('t2', { run_id: 'solo', wait: false }, undefined, undefined, h.ctx)
    const payload = h.emitted
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, value]) => value as Record<string, unknown>)
      .find(value => value.runId === 'rc')
    expect(payload).toMatchObject({ runId: 'rc', label: 'solo', status: 'done', backend: 'pi', nestedCount: 1 })

    await h.eventHandlers.session_shutdown()
  })

  it('flushes a nested orchestrator completion in sub-agent broker mode', async () => {
    const SOCK = '/tmp/child-completion.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'child answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const previousFetch = globalThis.fetch
    // The child broker poll has no server here; an empty queue keeps it off the network.
    globalThis.fetch = mock(async () => Response.json([])) as unknown as typeof fetch
    const h = await createMultiSupervisorHarness({
      statusTool,
      subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' },
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })
    try {
      h.ctx.isIdle = () => false
      await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
      done = true
      await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))

      // Children that spawn runs keep their own queue; a completed settlement
      // must flush it even though turn_end recording stays host-only.
      const settled = await h.eventHandlers.agent_before_settle(
        { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
        h.ctx,
      )
      expect(settled?.continue).toBe(true)
      expect(String(settled?.entries?.[0]?.content ?? '')).toContain('Sub-agent "worker" finished')
      expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
      const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
      expect(completions).toHaveLength(0)
    } finally {
      globalThis.fetch = previousFetch
      await h.eventHandlers.session_shutdown()
    }
  })

  it('does not emit a second terminal event when avenor_result consumes a run a blocking spawn still waits on', async () => {
    const SOCK = '/tmp/race-consume-spawn.sock'
    let done = false
    let emitEnded: (() => void) | undefined
    const observeRun = mock(async () => ({
      subscribe: (cb: (snapshot: unknown) => void) => {
        emitEnded = () => cb({ last_event: { event: 'session.end' }, transcript: [], tools: [], live_tools: [], pending_permission: false, ended: true })
        return () => {}
      },
      snapshot: () => ({ last_event: undefined, transcript: [], tools: [], live_tools: [], pending_permission: false, ended: false }),
      close: async () => {},
    }))
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'race answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const resultTool = mock(async () => ({ run_id: 'r1', label: 'worker', status: 'done', ready: true, output: 'race answer' }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      observeRun,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    // The blocking spawn holds its wait open while avenor_result resolves the
    // same run and consumes it.
    const spawned = h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    await waitFor(() => Boolean(emitEnded))
    await h.registeredTools.avenor_result.execute('t2', { run_id: 'worker' }, undefined, undefined, h.ctx)
    expect(h.emitted.filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)).toHaveLength(1)

    // The spawn wait then resolves with the terminal status; the consumed run
    // must not be resurrected, or the once-guard would be lost and the event
    // would fire a second time.
    done = true
    emitEnded?.()
    await spawned
    expect(h.emitted.filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)).toHaveLength(1)

    await h.eventHandlers.session_shutdown()
  })

  it('emits one terminal event when a blocking spawn replaces an already-notified run', async () => {
    const SOCK = '/tmp/terminal-once.sock'
    let done = false
    let emitEnded: (() => void) | undefined
    const observeRun = mock(async () => ({
      subscribe: (cb: (snapshot: unknown) => void) => {
        emitEnded = () => cb({ last_event: { event: 'session.end' }, transcript: [], tools: [], live_tools: [], pending_permission: false, ended: true })
        return () => {}
      },
      snapshot: () => ({ last_event: undefined, transcript: [], tools: [], live_tools: [], pending_permission: false, ended: false }),
      close: async () => {},
    }))
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'blocking answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      observeRun,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    const spawned = h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    await waitFor(() => Boolean(emitEnded))
    done = true
    // Polling observes the terminal run first and emits the event; the blocking
    // spawn then resolves and upserts the same run with its own status.
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'worker' && e.status === 'done'))
    emitEnded?.()
    await spawned

    const terminal = h.emitted.filter(([channel]) => channel === CHANNEL_RUN_TERMINAL).map(([, payload]) => payload as Record<string, unknown>)
    expect(terminal.filter(t => t.label === 'worker')).toHaveLength(1)

    await h.eventHandlers.session_shutdown()
  })

  it('does not resurrect a notice when the run is consumed while its idle send is in flight', async () => {
    const SOCK = '/tmp/consumed-during-send.sock'
    let done = false
    let sendStarted!: () => void
    const sendStartedGate = new Promise<void>(resolve => { sendStarted = resolve })
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const resultTool = mock(async () => ({ run_id: 'r1', label: 'worker', status: 'done', ready: true, output: 'answer' }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    h.sendUserMessage.mockImplementation(() => { sendStarted() })
    done = true
    await sendStartedGate

    // Consume the result while the prompt is in flight: that deletes the run
    // and its notice. The late delivery confirmation must not re-insert either.
    const result = await h.registeredTools.avenor_result.execute('t2', { run_id: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    expect(result.details).toMatchObject({ run_id: 'r1', ready: true })
    await confirmDelivery(h, String(h.sendUserMessage.mock.calls.at(-1)?.[0]))

    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    await h.registeredCommands['avenor-status'].handler('', h.ctx)
    const after = h.setWidget.mock.calls.filter(([name]) => name === 'avenor-status-detail').at(-1)?.[1] as string[] | undefined
    expect(after ?? []).toHaveLength(0)
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(1)

    await h.eventHandlers.session_shutdown()
  })

  it('keeps an unconfirmed idle send unread until message_end confirms it', async () => {
    const SOCK = '/tmp/idle-unconfirmed.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'unconfirmed answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('unconfirmed answer')))

    // Accepted by the void API but not yet confirmed: the notice stays unread
    // and a settlement must not deliver the same completion again.
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))
    const settledWhileInFlight = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settledWhileInFlight).toBeUndefined()

    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(1)

    await confirmDelivery(h, completions[0]!)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()
    expect(h.sendUserMessage.mock.calls.filter(([t]) => String(t).includes('Sub-agent "worker" finished'))).toHaveLength(1)

    await h.eventHandlers.session_shutdown()
  })

  it('re-arms an idle send that is accepted but never confirmed', async () => {
    const SOCK = '/tmp/idle-rearm.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'rearm answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      idleSendTimeoutMs: 100,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true

    const deliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))

    // The void send is accepted but never comes back through message_end. The
    // notice stays unread and the timeout re-sends instead of leaving the lone
    // terminal run skipped forever.
    await waitFor(() => deliveries().length >= 1)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')
    // Only the re-send depends on the timeout, so this is the one assertion
    // that must wait for a second delivery.
    await waitFor(() => deliveries().length >= 2)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    // The late confirmation of the re-send clears the notice exactly once and
    // leaves nothing for settlement to deliver.
    const delivered = deliveries().length
    await confirmDelivery(h, deliveries().at(-1)!)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    expect(deliveries()).toHaveLength(delivered)
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()

    await h.eventHandlers.session_shutdown()
  })

  it('clears the notice when a late confirmation arrives after a re-arm', async () => {
    const SOCK = '/tmp/idle-late-confirm.sock'
    let workerDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (args.runId === 'r2') return { run_id: 'r2', label: 'steady', status: 'running', runtime_id: 'rt2' }
        return workerDone
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'late answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      idleSendTimeoutMs: 60,
      spawnTool: mock(async (args: { label?: string }) => ({
        run_id: args.label === 'steady' ? 'r2' : 'r1',
        label: args.label ?? 'worker',
        supervisor_id: SOCK,
        runtime_id: args.label === 'steady' ? 'rt2' : 'rt1',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'steady', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    workerDone = true

    const deliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))

    await waitFor(() => deliveries().length >= 1)
    // The send stays accepted-but-unconfirmed and the session turns busy, so
    // the timeout must hand delivery back to the poll without re-sending. The
    // steady sibling keeps the loop alive, so a poll completing after the send
    // timeout has certainly run the re-arm branch against the busy session.
    h.ctx.isIdle = () => false
    const busyAt = Date.now()
    await waitFor(() => h.emitted.some(([channel, payload]) =>
      channel === CHANNEL_POLL_COMPLETED
      && ((payload as { timestamp?: number }).timestamp ?? 0) >= busyAt + 60))
    expect(deliveries()).toHaveLength(1)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    // The confirmation matches on the notice text alone, so it can arrive
    // while the send is still marked in flight.
    await confirmDelivery(h, deliveries()[0]!)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    expect(deliveries()).toHaveLength(1)

    // The confirmed notice must not be delivered a second time at settlement.
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()

    await h.eventHandlers.session_shutdown()
  })

  it('stops re-arming idle sends after the attempt cap and leaves the notice unread', async () => {
    const SOCK = '/tmp/idle-attempt-cap.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'capped answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      idleSendTimeoutMs: 20,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true

    const deliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))
    const footer = () => String(h.setStatus.mock.calls.at(-1)?.[1])

    await waitFor(() => deliveries().length >= 3)
    // The cap stops further sends and reports the exhausted delivery on its
    // own channel; the notice stays unread and the poll-error footer stays
    // clean, since this is not a polling failure.
    await waitFor(() => h.emitted.some(([channel, payload]) =>
      channel === CHANNEL_DELIVERY_ERROR
      && (payload as { source?: string }).source === 'idle-send'
      && (payload as { runId?: string }).runId === 'r1'))
    expect(footer()).toContain('unread:1')
    expect(footer()).not.toContain('errors:')
    expect(h.emitted.some(([channel]) => channel === CHANNEL_POLL_ERROR)).toBe(false)

    // The cap tick leaves no active run and no in-flight send, so the poll
    // loop stops and nothing can re-send: the unread notice is still
    // delivered at settlement.
    expect(deliveries()).toHaveLength(3)
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled?.entries).toHaveLength(1)
    expect(String(settled?.entries[0]?.content)).toContain('capped answer')

    await h.eventHandlers.session_shutdown()
  })

  it('resets the idle-send retry budget when a session start hands delivery back to the poll', async () => {
    const SOCK = '/tmp/idle-cap-session-start.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'capped again answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      idleSendTimeoutMs: 20,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true

    const deliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))
    const deliveryErrors = () => h.emitted
      .filter(([channel]) => channel === CHANNEL_DELIVERY_ERROR)
      .map(([, payload]) => payload as { runId?: string; attempts?: number })

    await waitFor(() => deliveries().length >= 3)
    await waitFor(() => deliveryErrors().some(payload => payload.runId === 'r1' && payload.attempts === 3))

    // The session replacement clears the capped notice and hands delivery back
    // to the poll with a fresh retry budget: the rebuilt notice is sent again
    // instead of staying capped.
    await h.eventHandlers.session_start({}, h.ctx)
    await waitFor(() => deliveries().length >= 4)

    // The fresh budget is bounded the same way: three more unconfirmed sends
    // exhaust it and the delivery error repeats.
    await waitFor(() => deliveryErrors().filter(payload => payload.attempts === 3).length >= 2)
    expect(deliveries()).toHaveLength(6)

    await h.eventHandlers.session_shutdown()
  })

  it('keeps the idle-send attempt cap while a sibling run keeps polling alive', async () => {
    const SOCK = '/tmp/idle-cap-sibling.sock'
    let workerDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (args.runId === 'r2') return { run_id: 'r2', label: 'steady', status: 'running', runtime_id: 'rt2' }
        return workerDone
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'sibling cap answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      idleSendTimeoutMs: 20,
      spawnTool: mock(async (args: { label?: string }) => ({
        run_id: args.label === 'steady' ? 'r2' : 'r1',
        label: args.label ?? 'worker',
        supervisor_id: SOCK,
        runtime_id: args.label === 'steady' ? 'rt2' : 'rt1',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'steady', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    workerDone = true

    const workerDeliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))
    const polls = () => h.emitted.filter(([channel]) => channel === CHANNEL_POLL_COMPLETED).length
    const footer = () => String(h.setStatus.mock.calls.at(-1)?.[1])

    // The steady sibling keeps the poll loop alive, so the spent retry budget
    // must stop re-sends on every later tick, not just the capping one.
    await waitFor(() => workerDeliveries().length >= 3)
    await waitFor(() => h.emitted.some(([channel, payload]) =>
      channel === CHANNEL_DELIVERY_ERROR
      && (payload as { runId?: string }).runId === 'r1'
      && (payload as { attempts?: number }).attempts === 3))
    expect(footer()).toContain('unread:1')

    const pollsAtCap = polls()
    await waitFor(() => polls() >= pollsAtCap + 3)
    expect(workerDeliveries()).toHaveLength(3)

    // The capped notice is still delivered at the next completed settlement.
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled?.entries).toHaveLength(1)
    expect(String(settled?.entries[0]?.content)).toContain('sibling cap answer')

    await h.eventHandlers.session_shutdown()
  })

  it('caps an idle send whose timeout is preempted by compaction', async () => {
    const SOCK = '/tmp/idle-cap-compaction.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'compacted answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      idleSendTimeoutMs: 200,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true

    const deliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))
    const polls = () => h.emitted.filter(([channel]) => channel === CHANNEL_POLL_COMPLETED).length

    await waitFor(() => deliveries().length >= 3)
    // Compaction finishes before the third send's timeout can fire, so the
    // hand-back is what must latch the exhausted budget.
    await h.eventHandlers.session_before_compact()
    await h.eventHandlers.session_compact()
    expect(h.emitted.some(([channel, payload]) =>
      channel === CHANNEL_DELIVERY_ERROR
      && (payload as { runId?: string; attempts?: number }).runId === 'r1'
      && (payload as { attempts?: number }).attempts === 3)).toBe(true)

    // The next idle tick cannot send past the latched cap.
    const pollsAtResume = polls()
    await waitFor(() => polls() > pollsAtResume)
    expect(deliveries()).toHaveLength(3)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    await h.eventHandlers.session_shutdown()
  })

  it('re-sends a permission notice the compaction window rejected', async () => {
    const SOCK = '/tmp/permission-compaction.sock'
    let permission = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return permission
          ? { run_id: 'r1', label: 'worker', status: 'waiting', runtime_id: 'rt1', pending_permission: { description: 'allow write' } }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async () => ({ run_id: 'r1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt1' })),
    })
    h.ctx.isIdle = () => false

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const waitingNotices = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('is waiting for input'))

    // The permission notice is sent exactly once while the run stays blocked.
    permission = true
    await waitFor(() => waitingNotices().length >= 1)
    await waitFor(() => statusTool.mock.calls.filter(call => call[0]?.runId === 'r1' && call[0]?.supervisorId === SOCK).length >= 2)
    expect(waitingNotices()).toHaveLength(1)

    // The compaction window rejects any in-flight prompt; the hand-back must
    // re-arm the notice so the still-blocked run's ask is delivered again.
    await h.eventHandlers.session_before_compact()
    await h.eventHandlers.session_compact()
    await waitFor(() => waitingNotices().length >= 2)
    expect(waitingNotices()).toHaveLength(2)

    await h.eventHandlers.session_shutdown()
  })

  it('re-sends an idle completion after a new session generation interrupts the send', async () => {
    const SOCK = '/tmp/idle-generation.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (args.runId === 'r2') return { run_id: 'r2', label: 'steady', status: 'running', runtime_id: 'rt2' }
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'generation answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async (args: { label?: string }) => ({
        run_id: args.label === 'steady' ? 'r2' : 'r1',
        label: args.label ?? 'worker',
        supervisor_id: SOCK,
        runtime_id: args.label === 'steady' ? 'rt2' : 'rt1',
      })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    // The idle send is still unconfirmed when the session restarts.
    await waitFor(() => h.sendUserMessage.mock.calls.filter(([t]) => String(t).includes('"worker"')).length === 1)

    await h.eventHandlers.session_start({}, h.ctx)

    // Polling stopped when the only tracked run went terminal; a still-running
    // sibling restarts it. The terminal run must be re-sent instead of being
    // skipped forever by a stale in-flight flag.
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'steady', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await waitFor(() => h.sendUserMessage.mock.calls.filter(([t]) => String(t).includes('"worker"')).length >= 2)

    await h.eventHandlers.session_shutdown()
  })

  it('defers an idle completion until compaction finishes', async () => {
    const SOCK = '/tmp/idle-compaction.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (args.runId === 'r2') return { run_id: 'r2', label: 'steady', status: 'running', runtime_id: 'rt2' }
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'deferred answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async (args: { label?: string }) => ({
        run_id: args.label === 'steady' ? 'r2' : 'r1',
        label: args.label ?? 'worker',
        supervisor_id: SOCK,
        runtime_id: args.label === 'steady' ? 'rt2' : 'rt1',
      })),
    })

    h.ctx.isIdle = () => false
    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    // A running sibling keeps polling alive while the worker is terminal.
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'steady', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await waitFor(() => String(h.setStatus.mock.calls.at(-1)?.[1]).includes('unread:1'))

    // Compaction rejects prompts, so an idle session must wait for it to finish.
    await h.eventHandlers.session_before_compact({ type: 'session_before_compact' })
    h.ctx.isIdle = () => true
    // Two completed poll cycles: the first emit lands before its entry loop runs,
    // so the terminal run's branch has definitely been evaluated by the second.
    await h.waitPollMatching(() => true)
    await h.waitPollMatching(() => true)
    expect(h.sendUserMessage).not.toHaveBeenCalled()
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    await h.eventHandlers.session_compact({ type: 'session_compact' })
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('deferred answer')))
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(1)
    await confirmDelivery(h, completions[0]!)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')

    await h.eventHandlers.session_shutdown()
  })

  it('re-delivers an idle send that compaction rejected', async () => {
    const SOCK = '/tmp/idle-compaction-rearm.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        if (args.runId === 'r2') return { run_id: 'r2', label: 'steady', status: 'running', runtime_id: 'rt2' }
        return done
          ? { run_id: 'r1', label: 'worker', status: 'done', runtime_id: 'rt1', final_output: 'rejected answer' }
          : { run_id: 'r1', label: 'worker', status: 'running', runtime_id: 'rt1' }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      spawnTool: mock(async (args: { label?: string }) => ({
        run_id: args.label === 'steady' ? 'r2' : 'r1',
        label: args.label ?? 'worker',
        supervisor_id: SOCK,
        runtime_id: args.label === 'steady' ? 'rt2' : 'rt1',
      })),
    })

    const deliveries = () => h.sendUserMessage.mock.calls
      .map(([t]) => String(t))
      .filter(t => t.includes('Sub-agent "worker" finished'))

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    // A running sibling keeps polling alive while the worker is terminal.
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'steady', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    done = true
    await waitFor(() => deliveries().length === 1)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    // Compaction rejected the prompt in flight, so no confirmation can arrive.
    // Resuming must clear the flag and deliver the still-unread notice again.
    await h.eventHandlers.session_before_compact({ type: 'session_before_compact' })
    await h.eventHandlers.session_compact({ type: 'session_compact' })
    await waitFor(() => deliveries().length >= 2)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).toContain('unread:1')

    const delivered = deliveries().length
    await confirmDelivery(h, deliveries().at(-1)!)
    expect(String(h.setStatus.mock.calls.at(-1)?.[1])).not.toContain('unread:')
    expect(deliveries()).toHaveLength(delivered)
    const settled = await h.eventHandlers.agent_before_settle(
      { type: 'agent_before_settle', entries: [], continue: false, outcome: 'completed' },
      h.ctx,
    )
    expect(settled).toBeUndefined()

    await h.eventHandlers.session_shutdown()
  })

  interface TwoRunFlags {
    alphaDone?: boolean
    alphaWaiting?: boolean
    betaDone?: boolean
    betaPermission?: boolean
  }

  /** Status + spawn fixtures for the two-run (alpha/beta) interrupt tests.
   * `flags` is a getter so tests can flip run state between polls. */
  function makeTwoRunStatusTool(sock: string, flags: () => TwoRunFlags) {
    return mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (args.supervisorId === sock) {
        const f = flags()
        if (args.runId === 'rb') {
          if (f.betaDone) return { run_id: 'rb', label: 'beta', status: 'done', runtime_id: 'rt-b', final_output: 'beta answer' }
          if (f.betaPermission) return { run_id: 'rb', label: 'beta', status: 'waiting', runtime_id: 'rt-b', pending_permission: { description: 'allow write' } }
          return { run_id: 'rb', label: 'beta', status: 'running', runtime_id: 'rt-b' }
        }
        if (f.alphaDone) return { run_id: 'ra', label: 'alpha', status: 'done', runtime_id: 'rt-a', final_output: 'alpha answer' }
        if (f.alphaWaiting) return { run_id: 'ra', label: 'alpha', status: 'waiting', runtime_id: 'rt-a', pending_permission: { description: 'allow write' } }
        return { run_id: 'ra', label: 'alpha', status: 'running', runtime_id: 'rt-a' }
      }
      if (!args.runId) return { runs: [], count: 0 }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
  }

  /** observeRun mock that holds a spawn's blocking wait open (never emits a
   * snapshot), so the wait resolves only via the await registry or abort. */
  function makeDormantObserver() {
    return mock(async () => ({
      subscribe: (cb: (snapshot: unknown) => void) => {
        void cb
        return () => {}
      },
      snapshot: () => ({ last_event: undefined, transcript: [], pending_permission: false, ended: false }),
      close: async () => {},
    }))
  }

  /** resultTool mock that blocks until the wait's signal aborts, mirroring
   * the real resultTool's abort behavior so tests exercise the interrupt path. */
  function makeBlockingResultTool() {
    return mock((args: { signal?: AbortSignal }) => new Promise((_resolve, reject) => {
      args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
    }))
  }

  /** brokerReceiveTool mock that withholds its single ask until `deliver`
   * flips, so a test can open a blocking wait before the ask arrives. */
  function makeAskBrokerReceiveTool() {
    let deliverAsk = false
    const tool = mock(async () => ({
      asks: deliverAsk
        ? [{ message_id: 'ask-1', from_run_id: 'rt_1', message: 'may I proceed?', role: 'agent' }]
        : [],
    }))
    return { tool, deliver: () => { deliverAsk = true } }
  }

  function makeTwoRunSpawnTool(sock: string) {
    return mock(async (args: { label?: string }) => ({
      run_id: args.label === 'alpha' ? 'ra' : 'rb',
      label: args.label ?? 'spawned',
      supervisor_id: sock,
      runtime_id: args.label === 'alpha' ? 'rt-a' : 'rt-b',
    }))
  }

  it('interrupts a blocking avenor_result when a sibling run requests permission', async () => {
    const SOCK = '/tmp/sibling-permission.sock'
    let betaPermission = false
    let alphaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ betaPermission, alphaDone }))
    // First call blocks until aborted (the interrupt). Re-issued calls block
    // until released, and reject if the transition gate wrongly re-interrupts.
    let resolveReissue!: (v: unknown) => void
    const reissueGate = new Promise(resolve => { resolveReissue = resolve })
    const resultTool = mock((args: { signal?: AbortSignal }) => {
      if (resultTool.mock.calls.length === 1) {
        return new Promise((_resolve, reject) => {
          args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
        })
      }
      return new Promise((resolve, reject) => {
        args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
        void reissueGate.then(resolve)
      })
    })

    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const resultPromise = h.registeredTools.avenor_result.execute('t3', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    betaPermission = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; pendingPermission: boolean }) => e.label === 'beta' && e.pendingPermission))
    const interrupted = await resultPromise
    expect(interrupted.details).toMatchObject({
      run_id: 'ra',
      status: 'running',
      ready: false,
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'waiting' },
    })

    // Re-issuing while beta's permission persists must not livelock: the
    // persisting stopping point is not a new transition, so the wait holds.
    const reissued = h.registeredTools.avenor_result.execute('t4', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    for (let i = 0; i < 3; i++) {
      await h.waitPollMatching(payload => payload.entries.some((e: { label: string; pendingPermission: boolean }) => e.label === 'beta' && e.pendingPermission))
    }
    expect(resultTool).toHaveBeenCalledTimes(2)

    // The re-issued wait was never interrupted; once it returns (unconsumed),
    // alpha's later completion flows through auto-delivery again.
    resolveReissue({ run_id: 'ra', label: 'alpha', status: 'running', ready: false })
    const reissuedResult = await reissued
    expect(reissuedResult.details).not.toHaveProperty('interrupted_by')

    alphaDone = true
    let resolveCompletion!: () => void
    const completionSent = new Promise<void>(resolve => { resolveCompletion = resolve })
    h.sendUserMessage.mockImplementation(() => resolveCompletion())
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'alpha' && e.status === 'done'))
    await completionSent
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "alpha" finished'))
    expect(completions).toHaveLength(1)

    await h.eventHandlers.session_shutdown()
  })

  it('carries the last-known nestedCount into a poll-tick terminal event', async () => {
    const SOCK = '/tmp/poll-terminal-nested.sock'
    let done = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      if (args.supervisorId === SOCK) {
        return done
          ? { run_id: 'rc', label: 'solo', status: 'done', runtime_id: 'rt-c', backend: 'pi', final_output: 'solo answer' }
          : { run_id: 'rc', label: 'solo', status: 'running', runtime_id: 'rt-c', backend: 'pi', pid: 424242 }
      }
      return { run_id: args.runId, label: args.runId, status: 'done' }
    })
    const dial = mock(async () => ({ list: async () => [{ status: 'running' }], close: () => {} }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      dial,
      spawnTool: mock(async () => ({ run_id: 'rc', label: 'solo', supervisor_id: SOCK, runtime_id: 'rt-c' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'solo', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    // The running poll stores the nested count on the tracked run; the terminal
    // poll cannot recompute it (the run has no live pid), so the carried value
    // must reach the terminal payload.
    await h.waitPollMatching(payload => payload.entries.some((e: { nestedCount?: number }) => e.nestedCount === 1))
    done = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'solo' && e.status === 'done'))
    await waitFor(() => h.emitted.some(([channel, payload]) =>
      channel === CHANNEL_RUN_TERMINAL && (payload as { runId?: string }).runId === 'rc'))
    const payload = h.emitted
      .filter(([channel]) => channel === CHANNEL_RUN_TERMINAL)
      .map(([, value]) => value as Record<string, unknown>)
      .find(value => value.runId === 'rc')
    expect(payload).toMatchObject({ runId: 'rc', nestedCount: 1, backend: 'pi' })

    await h.eventHandlers.session_shutdown()
  })

  it('emits a terminal event when a result is delivered for an untracked run', async () => {
    const resultTool = mock(async () => ({ run_id: 'ghost', label: 'ghost', status: 'done', ready: true, output: 'ghost answer' }))
    const h = await createMultiSupervisorHarness({
      statusTool: mock(async () => ({ runs: [], count: 0 })),
      resultTool,
    })

    await h.registeredTools.avenor_result.execute('t1', { run_id: 'ghost', wait: false }, undefined, undefined, h.ctx)
    expect(h.emitted).toContainEqual([
      CHANNEL_RUN_TERMINAL,
      expect.objectContaining({ runId: 'ghost', label: 'ghost', status: 'done' }),
    ])

    await h.eventHandlers.session_shutdown()
  })

  it('re-probes the caller-supplied reference when a wait is interrupted', async () => {
    const SOCK = '/tmp/interrupt-canonical.sock'
    let alphaDone = false
    let betaDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      // The caller's label 'alpha' now resolves canonically to the newer run rb.
      if (args.runId === 'alpha') return { run_id: 'rb', label: 'beta', status: 'running', runtime_id: 'rt-b' }
      if (args.runId === 'ra') return { run_id: 'ra', label: 'alpha', status: alphaDone ? 'done' : 'running', runtime_id: 'rt-a' }
      if (args.runId === 'rb') return { run_id: 'rb', label: 'beta', status: betaDone ? 'done' : 'running', runtime_id: 'rt-b' }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
    const resultTool = mock(async (args: { runId?: string; signal?: AbortSignal }) => {
      if (args.runId === 'alpha') {
        // The original wait blocks until the interrupt aborts it.
        return new Promise((_resolve, reject) => {
          args.signal?.addEventListener('abort', () => reject(new Error('result wait interrupted')))
        })
      }
      // The old redelivery target: the label-resolved run's own id.
      if (args.runId === 'ra') return { run_id: 'ra', label: 'alpha', status: 'done', ready: true, output: 'alpha answer' }
      throw new Error(`unexpected resultTool call for ${String(args.runId)}`)
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const resultPromise = h.registeredTools.avenor_result.execute('t3', { run_id: 'alpha', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const interrupted = await resultPromise

    // The interruption reports the canonical resolution, not the label-resolved
    // run's own terminal state — the caller re-issues for rb. The old code
    // re-probed ra, redelivered alpha's result as ready, and consumed it.
    expect(interrupted.details).toMatchObject({ run_id: 'rb', status: 'running', ready: false })
    expect(resultTool.mock.calls.filter(call => call[0]?.runId === 'ra')).toHaveLength(0)

    // The label-resolved run was released, not consumed: its completion still
    // flows through the poll tick.
    alphaDone = true
    await waitFor(() => h.emitted.some(([channel, payload]) =>
      channel === CHANNEL_RUN_TERMINAL && (payload as { runId?: string }).runId === 'ra'))
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('Sub-agent "alpha" finished')))

    await h.eventHandlers.session_shutdown()
  })

  it('does not release another call\'s blocking claim when the interrupt reconciles to it', async () => {
    const SOCK = '/tmp/release-ownership.sock'
    let betaDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      // The caller's label 'alpha' canonically resolves to rb, whose canonical
      // view still shows it running.
      if (args.runId === 'alpha') return { run_id: 'rb', label: 'beta', status: 'running', runtime_id: 'rt-b' }
      if (args.runId === 'ra') return { run_id: 'ra', label: 'alpha', status: 'running', runtime_id: 'rt-a' }
      if (args.runId === 'rb') return { run_id: 'rb', label: 'beta', status: betaDone ? 'done' : 'running', runtime_id: 'rt-b' }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
    const resultTool = mock(async (args: { runId?: string; signal?: AbortSignal }) => {
      if (args.runId === 'rb') {
        // The first call owns beta and holds until its caller aborts.
        return new Promise(resolve => {
          args.signal?.addEventListener('abort', () => resolve({ run_id: 'rb', label: 'beta', status: 'running', ready: false }))
        })
      }
      if (args.runId === 'alpha') {
        return new Promise((_resolve, reject) => {
          args.signal?.addEventListener('abort', () => reject(new Error('result wait interrupted')))
        })
      }
      throw new Error(`unexpected resultTool call for ${String(args.runId)}`)
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const controller = new AbortController()
    const call1 = h.registeredTools.avenor_result.execute('t3', { run_id: 'rb', supervisor_id: SOCK }, controller.signal, undefined, h.ctx)
    const call2 = h.registeredTools.avenor_result.execute('t4', { run_id: 'alpha', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const interrupted = await call2

    // The interruption reconciles to rb and reports the canonical resolution.
    expect(interrupted.details).toMatchObject({ run_id: 'rb', status: 'running', ready: false })

    // Beta is still owned by the first call: its completion must not be
    // delivered while that wait holds, across further poll ticks.
    const pollsAtClaim = h.emitted.filter(([channel]) => channel === CHANNEL_POLL_COMPLETED).length
    await waitFor(() => h.emitted.filter(([channel]) => channel === CHANNEL_POLL_COMPLETED).length >= pollsAtClaim + 3)
    expect(h.sendUserMessage.mock.calls.filter(([t]) => String(t).includes('Sub-agent "beta" finished'))).toHaveLength(0)

    // Releasing the first call hands beta's delivery back to the poll.
    controller.abort()
    await waitFor(() => h.sendUserMessage.mock.calls.some(([t]) => String(t).includes('Sub-agent "beta" finished')))

    await h.eventHandlers.session_shutdown()
  })

  it('interrupts a blocking avenor_result when a sibling run completes', async () => {
    const SOCK = '/tmp/sibling-terminal.sock'
    let betaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ betaDone }))
    const resultTool = makeBlockingResultTool()

    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const resultPromise = h.registeredTools.avenor_result.execute('t3', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const interrupted = await resultPromise
    expect(interrupted.details).toMatchObject({
      run_id: 'ra',
      status: 'running',
      ready: false,
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'done' },
    })

    await h.eventHandlers.session_shutdown()
  })

  it('interrupts a blocking avenor_result when the awaited sub-agent asks the host a question', async () => {
    const SOCK = '/tmp/ask-interrupt-host.sock'
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      return { run_id: 'rt_1', label: 'worker', status: 'running', runtime_id: 'rt-1' }
    })
    const resultTool = makeBlockingResultTool()
    const { tool: brokerReceiveTool, deliver } = makeAskBrokerReceiveTool()
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      brokerReceiveTool,
      spawnTool: mock(async () => ({ run_id: 'rt_1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt-1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise: Promise<{ content: Array<{ text: string }>; details?: unknown }> = h.registeredTools.avenor_result.execute('t2', { run_id: 'rt_1', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    await waitFor(() => resultTool.mock.calls.length === 1)

    deliver()
    await waitFor(() => h.sendUserMessage.mock.calls.length > 0)
    const interrupted = await settleWithin(
      resultPromise,
      'blocking avenor_result did not return after the awaited run asked a question',
    )
    expect(interrupted.details).toMatchObject({
      run_id: 'rt_1',
      status: 'running',
      ready: false,
      interrupted_by: { run_id: 'rt_1', status: 'asking' },
    })

    await h.eventHandlers.session_shutdown()
  })

  it('holds a blocking avenor_result when the ask body cannot be delivered', async () => {
    const SOCK = '/tmp/ask-undeliverable.sock'
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      return { run_id: 'rt_1', label: 'worker', status: 'running', runtime_id: 'rt-1' }
    })
    const resultTool = makeBlockingResultTool()
    const { tool: brokerReceiveTool, deliver } = makeAskBrokerReceiveTool()
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      brokerReceiveTool,
      spawnTool: mock(async () => ({ run_id: 'rt_1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt-1' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise: Promise<{ content: Array<{ text: string }>; details?: unknown }> = h.registeredTools.avenor_result.execute('t2', { run_id: 'rt_1', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    await waitFor(() => resultTool.mock.calls.length === 1)

    let outcome: 'returned' | 'rejected' | undefined
    resultPromise.then(() => { outcome = 'returned' }, () => { outcome = 'rejected' })

    // The ask body never reaches the session, so the agent has nothing to
    // answer and the wait must keep holding instead of reporting the ask.
    h.sendUserMessage.mockImplementation(() => { throw new Error('session is not accepting messages') })
    const receivesBefore = brokerReceiveTool.mock.calls.length
    deliver()
    await waitFor(() => brokerReceiveTool.mock.calls.length >= receivesBefore + 2)
    expect(outcome).toBeUndefined()

    await h.eventHandlers.session_shutdown()
  })

  it('ignores untracked poll entries as interrupt sources', async () => {
    const SOCK = '/tmp/untracked-source.sock'
    let resolveResult!: (v: unknown) => void
    const resultGate = new Promise(resolve => { resolveResult = resolve })
    // The singleton live list contains a run this session never spawned.
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) {
        return { runs: [{ run_id: 'foreign', label: 'foreign', status: 'done', runtime_id: 'rt-x' }], count: 1 }
      }
      if (args.supervisorId === SOCK) {
        return { run_id: 'ra', label: 'alpha', status: 'running', runtime_id: 'rt-a' }
      }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
    const resultTool = mock(async () => {
      const value = await resultGate
      return value
    })

    const h = await createMultiSupervisorHarness({ statusTool, resultTool })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise = h.registeredTools.avenor_result.execute('t2', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    // Several polls observe the foreign terminal run; the wait must hold.
    for (let i = 0; i < 3; i++) {
      await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'foreign' && e.status === 'done'))
    }
    expect(resultTool).toHaveBeenCalledTimes(1)

    resolveResult({ run_id: 'ra', label: 'alpha', status: 'running', ready: false })
    const result = await resultPromise
    expect(result.details).toMatchObject({ status: 'running', ready: false })
    expect(result.details).not.toHaveProperty('interrupted_by')

    await h.eventHandlers.session_shutdown()
  })

  it('delivers the awaited run\'s real result when it stopped before the sibling interrupt', async () => {
    const SOCK = '/tmp/both-stopped.sock'
    let alphaDone = false
    let betaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ alphaDone, betaDone }))
    // First call blocks until aborted; the post-interrupt redelivery call
    // resolves immediately with alpha's terminal result.
    let firstCall = true
    const resultTool = mock((args: { signal?: AbortSignal }) => {
      if (firstCall) {
        firstCall = false
        return new Promise((_resolve, reject) => {
          args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
        })
      }
      return Promise.resolve({ run_id: 'ra', label: 'alpha', status: 'done', ready: true, output: 'alpha answer' })
    })

    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise = h.registeredTools.avenor_result.execute('t3', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    // Alpha finishes in the same tick that beta's completion interrupts the wait.
    alphaDone = true
    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const result = await resultPromise
    expect(result.details).toMatchObject({
      run_id: 'ra',
      status: 'done',
      ready: true,
      output: 'alpha answer',
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'done' },
    })

    // The delivered result marked the run consumed: no automatic completion
    // message may follow.
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "alpha" finished'))
    expect(completions).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('does not interrupt the avenor_result waiting on the run that stopped', async () => {
    const SOCK = '/tmp/own-stop.sock'
    let alphaWaiting = false
    let resolveResult!: (v: unknown) => void
    const resultGate = new Promise(resolve => { resolveResult = resolve })
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ alphaWaiting }))
    // Abort-aware: the mock must observe the controller's abort, or the
    // own-run exclusion this test pins would be untestable.
    const resultTool = mock((args: { signal?: AbortSignal }) => new Promise((resolve, reject) => {
      args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
      void resultGate.then(resolve)
    }))

    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise = h.registeredTools.avenor_result.execute('t2', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    alphaWaiting = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; pendingPermission: boolean }) => e.label === 'alpha' && e.pendingPermission))
    // Give the tick a chance to (wrongly) interrupt; the wait must hold.
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; pendingPermission: boolean }) => e.label === 'alpha' && e.pendingPermission))
    expect(resultTool).toHaveBeenCalledTimes(1)

    resolveResult({ run_id: 'ra', label: 'alpha', status: 'waiting', ready: false, pending_permission: { description: 'allow write' } })
    const result = await resultPromise
    expect(result.details).toMatchObject({ status: 'waiting', ready: false })
    expect(result.details).not.toHaveProperty('interrupted_by')

    await h.eventHandlers.session_shutdown()
  })

  it('releases blocking waits when the session shuts down', async () => {
    const SOCK = '/tmp/shutdown-release.sock'
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({}))
    const resultTool = makeBlockingResultTool()

    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: mock(async () => ({ run_id: 'ra', label: 'alpha', supervisor_id: SOCK, runtime_id: 'rt-a' })),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise = h.registeredTools.avenor_result.execute('t2', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    // Shutdown aborts the wait without a sibling stopping point: the call
    // fails instead of resolving with a misleading interrupted_by payload.
    await h.eventHandlers.session_shutdown()
    await expect(resultPromise).rejects.toThrow()
  })

  it('interrupts every sibling waiter but never the stopped run\'s own waiter', async () => {
    const SOCK = '/tmp/two-waiters.sock'
    let betaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ betaDone }))
    const resultTool = makeBlockingResultTool()
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const waiterAlpha = h.registeredTools.avenor_result.execute('t3', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)
    const waiterBeta = h.registeredTools.avenor_result.execute('t4', { run_id: 'rb', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const interruptedAlpha = await waiterAlpha
    expect(interruptedAlpha.details).toMatchObject({
      run_id: 'ra',
      ready: false,
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'done' },
    })

    // Beta's own waiter must survive its own stopping point (key-scoped
    // exclusion across multiple registry entries); shutdown releases it.
    expect(resultTool).toHaveBeenCalledTimes(2)
    await h.eventHandlers.session_shutdown()
    await expect(waiterBeta).rejects.toThrow()
  })

  it('releases a blocking spawn wait on shutdown without sibling semantics', async () => {
    const SOCK = '/tmp/shutdown-spawn.sock'
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({}))
    const observeRun = makeDormantObserver()

    const h = await createMultiSupervisorHarness({
      statusTool,
      observeRun,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    const spawnPromise = h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx)

    await h.eventHandlers.session_shutdown()
    const spawnResult = await spawnPromise
    expect(String(spawnResult.content[0].text)).toContain('was interrupted')
    expect(spawnResult.details).not.toHaveProperty('interrupted_by')

    // The tracking entry is removed with the wait, so the run is no longer
    // poll-visible in this session.
    const notify = mock(() => {})
    const setWidget = mock(() => {})
    await h.registeredCommands['avenor-status'].handler('', { cwd: '/tmp', ui: { notify, setWidget } })
    expect(notify).toHaveBeenCalledWith('No active avenor runs', 'info')
    expect(setWidget).not.toHaveBeenCalled()
  })

  it('returns the fallback shape when the post-interrupt status probe fails', async () => {
    const SOCK = '/tmp/probe-failure.sock'
    let betaDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string; view?: string } = {}) => {
      // The post-interrupt probe is the only call with view: 'full'.
      if (args.view === 'full') throw new Error('supervisor gone')
      if (args.supervisorId === SOCK) {
        if (args.runId === 'rb') {
          return betaDone
            ? { run_id: 'rb', label: 'beta', status: 'done', runtime_id: 'rt-b', final_output: 'beta answer' }
            : { run_id: 'rb', label: 'beta', status: 'running', runtime_id: 'rt-b' }
        }
        return { run_id: 'ra', label: 'alpha', status: 'running', runtime_id: 'rt-a' }
      }
      if (!args.runId) return { runs: [], count: 0 }
      return { run_id: args.runId, label: args.runId, status: 'running' }
    })
    const resultTool = makeBlockingResultTool()
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise = h.registeredTools.avenor_result.execute('t3', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const result = await resultPromise
    expect(result.details).toMatchObject({
      run_id: 'ra',
      label: 'ra',
      status: 'running',
      ready: false,
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'done' },
    })
    expect(result.details).not.toHaveProperty('output')

    await h.eventHandlers.session_shutdown()
  })

  it('falls back to the terminal state when the self-stopped redelivery fails', async () => {
    const SOCK = '/tmp/redelivery-failure.sock'
    let alphaDone = false
    let betaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ alphaDone, betaDone }))
    // First call blocks until aborted; the post-interrupt redelivery fails.
    const resultTool = mock((args: { signal?: AbortSignal }) => {
      if (resultTool.mock.calls.length === 1) {
        return new Promise((_resolve, reject) => {
          args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
        })
      }
      return Promise.reject(new Error('redelivery failed'))
    })
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    await h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const resultPromise = h.registeredTools.avenor_result.execute('t3', { run_id: 'ra', supervisor_id: SOCK }, undefined, undefined, h.ctx)

    alphaDone = true
    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const result = await resultPromise
    expect(result.details).toMatchObject({
      run_id: 'ra',
      status: 'done',
      ready: true,
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'done' },
    })
    expect(result.details).not.toHaveProperty('output')

    // The delivered terminal fallback consumed the run: no auto-delivery.
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "alpha" finished'))
    expect(completions).toHaveLength(0)

    await h.eventHandlers.session_shutdown()
  })

  it('delivers inline when the awaited run stops in the same tick as the sibling interrupt', async () => {
    const SOCK = '/tmp/same-tick-spawn.sock'
    let alphaDone = false
    let betaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ alphaDone, betaDone }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      observeRun: makeDormantObserver(),
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    // Both spawns block (wait=true, held by the dormant observer), so both
    // runs' terminal handling takes the synchronous skip path.
    const alphaPromise = h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx)
    const betaPromise = h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx)

    alphaDone = true
    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'alpha' && e.status === 'done'))

    // The sibling interrupt fires while both runs are still marked blocking;
    // each spawn must probe its own run and deliver inline instead of relying
    // on a tick that has already stopped (last active run).
    const alphaResult = await alphaPromise
    expect(String(alphaResult.content[0].text)).not.toContain('was interrupted')
    expect(alphaResult.details).toMatchObject({ run_id: 'ra', status: 'done' })
    const betaResult = await betaPromise
    expect(betaResult.details).toMatchObject({ run_id: 'rb', status: 'done' })

    await h.eventHandlers.session_shutdown()
  })

  it('rejects avenor_result when the signal is already aborted before the call', async () => {
    const SOCK = '/tmp/pre-aborted.sock'
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({}))
    // Mirrors the real resultTool: an already-aborted signal rejects at once.
    const resultTool = mock((args: { signal?: AbortSignal }) => new Promise((_resolve, reject) => {
      if (args.signal?.aborted) {
        reject(new Error('result wait cancelled'))
        return
      }
      args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
    }))
    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const controller = new AbortController()
    controller.abort()
    await expect(h.registeredTools.avenor_result.execute('t2', { run_id: 'ra', supervisor_id: SOCK }, controller.signal, undefined, h.ctx)).rejects.toThrow()

    await h.eventHandlers.session_shutdown()
  })

  it('treats a caller-aborted wait as a plain abort, not a sibling interrupt', async () => {
    const SOCK = '/tmp/caller-abort.sock'
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({}))
    const resultTool = makeBlockingResultTool()
    const observeRun = makeDormantObserver()

    const h = await createMultiSupervisorHarness({
      statusTool,
      resultTool,
      observeRun,
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)

    const controller = new AbortController()
    const resultPromise = h.registeredTools.avenor_result.execute('t2', { run_id: 'ra', supervisor_id: SOCK }, controller.signal, undefined, h.ctx)
    controller.abort()
    await expect(resultPromise).rejects.toThrow()

    const spawnController = new AbortController()
    const spawnPromise = h.registeredTools.avenor_spawn.execute('t3', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: true }, spawnController.signal, undefined, h.ctx)
    spawnController.abort()
    const spawnResult = await spawnPromise
    expect(String(spawnResult.content[0].text)).toContain('was interrupted')
    expect(spawnResult.details).not.toHaveProperty('interrupted_by')

    await h.eventHandlers.session_shutdown()
  })

  it('interrupts a blocking spawn wait when a sibling run stops', async () => {
    const SOCK = '/tmp/sibling-spawn.sock'
    let alphaDone = false
    let betaDone = false
    const statusTool = makeTwoRunStatusTool(SOCK, () => ({ alphaDone, betaDone }))

    const h = await createMultiSupervisorHarness({
      statusTool,
      // Keeps the blocking spawn wait alive until the sibling's stopping point
      // aborts it through the await registry.
      observeRun: makeDormantObserver(),
      spawnTool: makeTwoRunSpawnTool(SOCK),
    })

    await h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'beta', supervisor_id: SOCK, wait: false }, undefined, undefined, h.ctx)
    const spawnPromise = h.registeredTools.avenor_spawn.execute('t2', { agent: 'explore', label: 'alpha', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx)

    betaDone = true
    await h.waitPollMatching(payload => payload.entries.some((e: { label: string; status: string }) => e.label === 'beta' && e.status === 'done'))
    const spawnResult = await spawnPromise
    expect(String(spawnResult.content[0].text)).toContain('was interrupted')
    expect(spawnResult.details).toMatchObject({
      run_id: 'ra',
      interrupted_by: { run_id: 'rb', label: 'beta', status: 'done' },
    })

    // Alpha must stay tracked in auto-delivery mode (blocking=false), so its
    // later completion is delivered as a follow-up message.
    let resolveCompletion!: () => void
    const completionSent = new Promise<void>(resolve => { resolveCompletion = resolve })
    h.sendUserMessage.mockImplementation(() => resolveCompletion())
    alphaDone = true
    await completionSent
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "alpha" finished'))
    expect(completions).toHaveLength(1)

    await h.eventHandlers.session_shutdown()
  })

  it('reports an inbound ask as asked a question when it interrupts a blocking spawn wait', async () => {
    const SOCK = '/tmp/ask-interrupt-spawn.sock'
    let workerDone = false
    const statusTool = mock(async (args: { runId?: string; supervisorId?: string } = {}) => {
      if (!args.runId) return { runs: [], count: 0 }
      return workerDone
        ? { run_id: 'rt_1', label: 'worker', status: 'done', runtime_id: 'rt-1', final_output: 'worker answer' }
        : { run_id: 'rt_1', label: 'worker', status: 'running', runtime_id: 'rt-1' }
    })
    const { tool: brokerReceiveTool, deliver } = makeAskBrokerReceiveTool()
    const h = await createMultiSupervisorHarness({
      statusTool,
      brokerReceiveTool,
      // Keeps the blocking spawn wait alive until the inbound ask aborts it
      // through the await registry.
      observeRun: makeDormantObserver(),
      spawnTool: mock(async () => ({ run_id: 'rt_1', label: 'worker', supervisor_id: SOCK, runtime_id: 'rt-1' })),
    })

    const spawnPromise: Promise<{ content: Array<{ text: string }>; details?: unknown }> = h.registeredTools.avenor_spawn.execute('t1', { agent: 'explore', label: 'worker', supervisor_id: SOCK, wait: true }, undefined, undefined, h.ctx)

    deliver()
    await waitFor(() => h.sendUserMessage.mock.calls.length > 0)
    const spawnResult = await settleWithin(
      spawnPromise,
      'blocking spawn wait did not return after the awaited run asked a question',
    )

    // The interruption notice must describe the synthetic `asking` status in
    // prose; the raw status string must never reach the model.
    const text = String(spawnResult.content[0].text)
    expect(text).toContain('asked a question')
    expect(text).not.toContain('reached asking')
    // The structured payload keeps the raw status the interrupt carried.
    expect(spawnResult.details).toMatchObject({
      run_id: 'rt_1',
      interrupted_by: { run_id: 'rt_1', status: 'asking' },
    })

    // Worker must stay tracked in auto-delivery mode (blocking=false), so its
    // later completion arrives as a follow-up message.
    let resolveCompletion!: () => void
    const completionSent = new Promise<void>(resolve => { resolveCompletion = resolve })
    h.sendUserMessage.mockImplementation(() => resolveCompletion())
    workerDone = true
    await completionSent
    const completions = h.sendUserMessage.mock.calls.map(([t]) => String(t)).filter(t => t.includes('Sub-agent "worker" finished'))
    expect(completions).toHaveLength(1)

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
      if (!args.runId) return { runs: [], count: 0 }
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
      if (!args.runId) return { runs: [], count: 0 }
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

describe('avenor_spawn parent run id', () => {
  it('names the supervisor as the child parent so the child can ask it back', async () => {
    const spawnToolMock = mock(async (_args: { agent?: string; prompt?: string; wait?: boolean; supervisor_id?: string }) => ({
      run_id: 'run-1',
      label: 'child',
      supervisor_id: '/tmp/sock',
      runtime_id: 'rt-1',
    }))
    const h = await startExtensionHarness({
      deps: { spawnTool: spawnToolMock },
      extOptions: { subAgentEnv: null },
    })

    await h.registeredTools.avenor_spawn.execute(
      't-parent',
      { agent: 'mule', prompt: 'do the thing', wait: false },
      undefined,
      undefined,
      { cwd: '/tmp' },
    )

    expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({ parent_run_id: 'supervisor' })
  })

  it('omits parent_run_id for an explicit non-local supervisor but still forwards it', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-pi-parent-note-remote-'))
    const previousHome = process.env.AVENOR_HOME
    const socketsDir = path.join(home, 'sockets')
    fs.mkdirSync(socketsDir, { recursive: true, mode: 0o700 })
    const root = fs.realpathSync(socketsDir)
    const OTHER = path.join(root, 'avenor-mcp-other.sock')
    const singleton = path.join(root, 'avenor-mcp-singleton.sock')
    const spawnToolMock = mock(async (_args: { agent?: string; prompt?: string; wait?: boolean; supervisor_id?: string }) => ({
      run_id: 'run-1',
      label: 'child',
      supervisor_id: OTHER,
      runtime_id: 'rt-1',
    }))
    const h = await startExtensionHarness({
      deps: {
        spawnTool: spawnToolMock,
        Supervisor: { isCurrentInstance: (id: string) => id === singleton } as any,
      },
      extOptions: { subAgentEnv: null },
    })
    try {
      process.env.AVENOR_HOME = home
      await h.registeredTools.avenor_spawn.execute(
        't-parent-remote',
        { agent: 'mule', prompt: 'do the thing', supervisor_id: OTHER, wait: false },
        undefined,
        undefined,
        { cwd: '/tmp' },
      )

      const args = spawnToolMock.mock.calls[0]?.[0] as Record<string, unknown>
      expect(args).not.toHaveProperty('parent_run_id')
      expect(args).toMatchObject({ supervisorId: OTHER })
    } finally {
      await h.eventHandlers.session_shutdown()
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      fs.rmSync(home, { recursive: true, force: true })
    }
  })

  it('names the supervisor as the child parent for an explicit current singleton', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-pi-parent-note-singleton-'))
    const previousHome = process.env.AVENOR_HOME
    const socketsDir = path.join(home, 'sockets')
    fs.mkdirSync(socketsDir, { recursive: true, mode: 0o700 })
    const SINGLETON = path.join(fs.realpathSync(socketsDir), 'avenor-mcp-singleton.sock')
    const spawnToolMock = mock(async (_args: { agent?: string; prompt?: string; wait?: boolean; supervisor_id?: string }) => ({
      run_id: 'run-1',
      label: 'child',
      supervisor_id: SINGLETON,
      runtime_id: 'rt-1',
    }))
    const h = await startExtensionHarness({
      deps: {
        spawnTool: spawnToolMock,
        Supervisor: { isCurrentInstance: (id: string) => id === SINGLETON } as any,
      },
      extOptions: { subAgentEnv: null },
    })
    try {
      process.env.AVENOR_HOME = home
      await h.registeredTools.avenor_spawn.execute(
        't-parent-singleton',
        { agent: 'mule', prompt: 'do the thing', supervisor_id: SINGLETON, wait: false },
        undefined,
        undefined,
        { cwd: '/tmp' },
      )

      expect(spawnToolMock.mock.calls[0]?.[0]).toMatchObject({ parent_run_id: 'supervisor', supervisorId: SINGLETON })
    } finally {
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      fs.rmSync(home, { recursive: true, force: true })
      await h.eventHandlers.session_shutdown()
    }
  })

  it('preserves parent notes and prunes singleton runs through an equivalent socket spelling', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-pi-parent-note-'))
    const previousHome = process.env.AVENOR_HOME
    const previousBin = process.env.AVENOR_BIN
    let sup: Supervisor | undefined
    let h: Awaited<ReturnType<typeof createMultiSupervisorHarness>> | undefined
    try {
      process.env.AVENOR_HOME = home
      process.env.AVENOR_BIN = path.resolve(import.meta.dir, '../../core/test/fixtures/fake-avenor.ts')
      sup = await Supervisor.get()
      const canonical = sup.supervisorId
      const equivalent = `${path.dirname(canonical)}/../sockets/${path.basename(canonical)}`
      const spawnTool = mock(async (args: { supervisorId?: string; label?: string }) => ({
        run_id: args.label!, label: args.label!,
        supervisor_id: args.supervisorId ?? canonical, runtime_id: args.label!,
      }))
      const shutdownTool = mock(async () => ({ ok: true }))
      h = await createMultiSupervisorHarness({
        supervisor: Supervisor,
        spawnTool,
        shutdownTool,
        statusTool: mock(async (args: { runId?: string } = {}) => args.runId
          ? { run_id: args.runId, label: args.runId, status: 'running', runtime_id: args.runId }
          : { runs: [], count: 0 }),
      })
      for (const [label, supervisor_id] of [
        ['alias-child', equivalent], ['implicit-child', undefined],
        ['canonical-child', canonical], ['remote-child', '/tmp/non-local-supervisor.sock'],
      ] as const) {
        await h.registeredTools.avenor_spawn.execute(label,
          { agent: 'mule', prompt: 'do the thing', label, supervisor_id, wait: false },
          undefined, undefined, h.ctx)
      }
      expect(spawnTool.mock.calls[0]?.[0]).toMatchObject({ parent_run_id: 'supervisor', supervisorId: equivalent })
      await h.registeredCommands['avenor-status'].handler('', h.ctx)
      expect(h.setWidget.mock.calls.filter(([n]) => n === 'avenor-status-detail').at(-1)?.[1]).toHaveLength(4)
      await h.registeredTools.avenor_shutdown.execute('shutdown-alias', { supervisor_id: equivalent })
      expect(shutdownTool).toHaveBeenCalledWith({ supervisorId: equivalent, force: undefined })
      await h.registeredCommands['avenor-status'].handler('', h.ctx)
      const detail = h.setWidget.mock.calls.filter(([n]) => n === 'avenor-status-detail').at(-1)?.[1] as string[]
      expect(detail).toHaveLength(1)
      expect(detail[0]).toContain('remote-child')
    } finally {
      await h?.eventHandlers.session_shutdown()
      await sup?.close()
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      if (previousBin === undefined) delete process.env.AVENOR_BIN
      else process.env.AVENOR_BIN = previousBin
      fs.rmSync(home, { recursive: true, force: true })
    }
  }, 20_000)
})
