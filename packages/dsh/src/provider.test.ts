import { describe, it, expect } from 'bun:test'
import {
  createAvenorProvider,
  createCoreSeam,
  promptText,
  type AvenorHandle,
  type AvenorSeam,
  type CoreTools,
} from './provider.js'
import { permissionNotice, type PendingPermission } from './permission.js'
import { diagnosticFor, stopReasonFor, toSubagentResult, type AvenorTerminal } from './stop-reason.js'
import type { ParentAgent, SubagentProvider, SubagentStartRequest, UserMessageLike } from './types.js'

function terminal(overrides: Partial<AvenorTerminal> = {}): AvenorTerminal {
  return { runId: 'run-1', status: 'done', ready: true, output: 'finished', ...overrides }
}

function pending(overrides: Partial<PendingPermission> = {}): PendingPermission {
  return {
    requestId: 'req-1',
    description: 'run `rm -rf build`',
    options: [
      { optionId: 'allow_once', label: 'Allow once', kind: 'allow' },
      { optionId: 'deny', label: 'Deny', kind: 'deny' },
    ],
    ...overrides,
  }
}

interface FakeSeam {
  readonly seam: AvenorSeam
  readonly calls: { spawn: number; cancel: number }
  readonly requests: Array<{ prompt: string; cwd?: string; label?: string }>
  readonly notify: (runId: string, permission: PendingPermission) => void
  readonly settle: (value: AvenorTerminal) => void
}

function fakeSeam(): FakeSeam {
  const calls = { spawn: 0, cancel: 0 }
  const requests: Array<{ prompt: string; cwd?: string; label?: string }> = []
  let notify: (runId: string, permission: PendingPermission) => void = () => {}
  let settle: (value: AvenorTerminal) => void = () => {}

  const seam: AvenorSeam = {
    async spawn(request, onPending) {
      calls.spawn += 1
      requests.push(request)
      notify = onPending
      const done = new Promise<AvenorTerminal>((resolve) => {
        settle = resolve
      })
      const handle: AvenorHandle = {
        runId: 'run-1',
        done,
        cancel: async () => {
          calls.cancel += 1
        },
      }
      return handle
    },
  }

  return {
    seam,
    calls,
    requests,
    notify: (runId, permission) => notify(runId, permission),
    settle: (value) => settle(value),
  }
}

function fakeParent(status: 'idle' | 'running' = 'running') {
  const injected: UserMessageLike[] = []
  const followed: UserMessageLike[] = []
  const parent: ParentAgent = {
    status,
    inject: (message: UserMessageLike) => injected.push(message),
    followup: (message: UserMessageLike) => followed.push(message),
  }
  return { injected, followed, parent }
}

/** `start` is optional on the seam's provider shape; this provider always has it. */
function startRun(provider: SubagentProvider, request: SubagentStartRequest) {
  if (!provider.start) throw new Error('provider has no start()')
  return provider.start(request)
}

function startRequest(prompt: string, extra: { cwd?: string; label?: string } = {}) {
  return {
    prompt: [{ type: 'text' as const, text: prompt }],
    signal: new AbortController().signal,
    parent: fakeParent().parent,
    ...extra,
  }
}

interface ScriptedResult {
  status: string
  ready: boolean
  output?: string
  stop_reason?: string
  timed_out?: boolean
  pending_permission?: {
    request_id?: string
    description?: string
    options?: Array<{ option_id?: string; label?: string; kind?: string }>
  }
}

function scriptedTools(script: ScriptedResult[]) {
  const calls = { spawn: 0, kill: 0 }
  const seen: Array<{ runId: string; wait: boolean }> = []
  let index = 0
  const tools: CoreTools = {
    async spawn() {
      calls.spawn += 1
      return { run_id: 'run-9' }
    },
    async result(request) {
      seen.push(request)
      const value = script[Math.min(index, script.length - 1)]
      index += 1
      return value
    },
    async kill() {
      calls.kill += 1
      return { killed: true }
    },
  }
  return { tools, calls, seen }
}

function hangingTools() {
  const calls = { spawn: 0, kill: 0 }
  const tools: CoreTools = {
    async spawn() {
      calls.spawn += 1
      return { run_id: 'run-9' }
    },
    result: () => new Promise<never>(() => {}),
    async kill() {
      calls.kill += 1
      return { killed: true }
    },
  }
  return { tools, calls }
}

describe('promptText', () => {
  it('joins text blocks and drops non-text blocks', () => {
    expect(
      promptText([
        { type: 'text', text: 'first' },
        { type: 'image', data: '...' },
        { type: 'text', text: 'second' },
      ]),
    ).toBe('first\nsecond')
  })
})

describe('stop reason mapping', () => {
  it('maps a settled run to completed', () => {
    expect(stopReasonFor(terminal())).toBe('completed')
  })

  it('maps a killed run to aborted', () => {
    expect(stopReasonFor(terminal({ status: 'killed', output: undefined }))).toBe('aborted')
  })

  it('maps an unsettled run to error', () => {
    expect(stopReasonFor(terminal({ ready: false, status: 'running' }))).toBe('error')
  })

  it('maps every other terminal status to error', () => {
    for (const status of ['failed', 'timeout', 'unknown']) {
      expect(stopReasonFor(terminal({ status }))).toBe('error')
    }
  })

  it('carries Avenor detail into the diagnostic for a failure', () => {
    const diagnostic = diagnosticFor(terminal({ status: 'timeout', stopReason: 'wall-clock' }))
    expect(diagnostic).toContain('timeout')
    expect(diagnostic).toContain('wall-clock')
  })

  it('omits the diagnostic for a clean completion', () => {
    expect(diagnosticFor(terminal())).toBeUndefined()
  })

  it('never rejects and keeps partial output for a failure', () => {
    const result = toSubagentResult(terminal({ status: 'failed', output: 'partial' }))
    expect(result.stopReason).toBe('error')
    expect(result.output).toEqual([{ type: 'text', text: 'partial' }])
    expect(result.diagnostic).toContain('failed')
  })
})

describe('permission notice', () => {
  it('names the run, the question, and every option id', () => {
    const text = permissionNotice('run-1', pending())
    expect(text).toContain('run-1')
    expect(text).toContain('rm -rf build')
    expect(text).toContain('allow_once')
    expect(text).toContain('deny')
    expect(text).toContain('avenor_answer_permission')
  })
})

describe('avenor subagent provider', () => {
  it('describes itself as an external, non-inheriting backend', () => {
    const provider = createAvenorProvider(fakeSeam().seam)
    expect(provider.name).toBe('avenor')
    expect(provider.inheritsParentContext).toBe(false)
    expect(provider.capabilities).toEqual({
      agentOptions: false,
      agentPreset: false,
      outputSchema: false,
      depthLimit: false,
    })
  })

  it('starts one run and forwards prompt, cwd, and label', async () => {
    const fake = fakeSeam()
    const run = await startRun(createAvenorProvider(fake.seam), 
      startRequest('do the thing', { cwd: '/tmp/work', label: 'thing' }),
    )
    expect(run.id).toBe('run-1')
    expect(fake.requests).toEqual([{ prompt: 'do the thing', cwd: '/tmp/work', label: 'thing' }])
  })

  it('resolves the run with the mapped terminal result', async () => {
    const fake = fakeSeam()
    const run = await startRun(createAvenorProvider(fake.seam), startRequest('work'))
    fake.settle(terminal({ output: 'answer' }))
    const result = await run.result
    expect(result.stopReason).toBe('completed')
    expect(result.output).toEqual([{ type: 'text', text: 'answer' }])
  })

  it('injects a pending permission into a running parent', async () => {
    const fake = fakeSeam()
    const parent = fakeParent('running')
    const provider = createAvenorProvider(fake.seam)
    await startRun(provider, { ...startRequest('work'), parent: parent.parent })

    fake.notify('run-1', pending())

    expect(parent.followed).toHaveLength(0)
    expect(parent.injected).toHaveLength(1)
    const text = parent.injected[0].content[0]
    expect(text.type).toBe('text')
    expect((text as { text: string }).text).toContain('run-1')
    expect((text as { text: string }).text).toContain('allow_once')
  })

  it('wakes an idle parent with a follow-up turn', async () => {
    const fake = fakeSeam()
    const parent = fakeParent('idle')
    const provider = createAvenorProvider(fake.seam)
    await startRun(provider, { ...startRequest('work'), parent: parent.parent })

    fake.notify('run-1', pending())

    expect(parent.injected).toHaveLength(0)
    expect(parent.followed).toHaveLength(1)
    expect((parent.followed[0].content[0] as { text: string }).text).toContain('run-1')
  })

  it('cancels the run on abort and again on dispose', async () => {
    const fake = fakeSeam()
    const provider = createAvenorProvider(fake.seam)
    const controller = new AbortController()
    const run = await startRun(provider, {
      prompt: [{ type: 'text', text: 'work' }],
      signal: controller.signal,
      parent: fakeParent().parent,
    })

    controller.abort()
    expect(fake.calls.cancel).toBe(1)

    await run.dispose()
    expect(fake.calls.cancel).toBe(2)
  })

  it('tracks a started run for inbound delivery until it settles', async () => {
    const fake = fakeSeam()
    const remembered: string[] = []
    const forgotten: string[] = []
    let inbound = 0
    const bound: string[] = []
    const tracker = {
      async bind(sessionId: string) {
        bound.push(sessionId)
        return { runId: 'bound-1' }
      },
      remember(runId: string) {
        remembered.push(runId)
      },
      forget(runId: string) {
        forgotten.push(runId)
      },
      ensureInbound(_sessionId: string) {
        inbound += 1
      },
    }

    const request = {
      ...startRequest('work'),
      parent: { ...fakeParent().parent, session: { id: 'sess-1' } },
    }
    const run = await startRun(createAvenorProvider(fake.seam, tracker), request)
    expect(bound).toEqual(['sess-1'])
    expect(remembered).toEqual(['run-1'])
    expect(inbound).toBe(1)
    expect((fake.requests[0] as { messaging?: boolean; parentRunId?: string }).messaging).toBe(true)
    expect((fake.requests[0] as { parentRunId?: string }).parentRunId).toBe('bound-1')

    fake.settle(terminal())
    await run.result
    expect(forgotten).toEqual(['run-1'])
  })

  it('propagates a start failure instead of minting a run', async () => {
    const seam: AvenorSeam = {
      async spawn() {
        throw new Error('avenor binary not found')
      },
    }
    const provider = createAvenorProvider(seam)
    if (!provider.start) throw new Error('provider has no start()')
    await expect(provider.start(startRequest('work'))).rejects.toThrow('avenor binary not found')
  })
})

describe('core seam', () => {
  it('does not kill a run that already settled', async () => {
    const fake = scriptedTools([{ status: 'done', ready: true, output: 'ok' }])
    const handle = await createCoreSeam({ tools: fake.tools, sleep: async () => {} }).spawn(
      { prompt: 'work' },
      () => {},
    )
    await handle.done
    await handle.cancel()
    expect(fake.calls.kill).toBe(0)
  })

  it('kills an unsettled run once even when asked twice', async () => {
    const fake = hangingTools()
    const handle = await createCoreSeam({ tools: fake.tools, sleep: async () => {} }).spawn(
      { prompt: 'work' },
      () => {},
    )
    await handle.cancel()
    await handle.cancel()
    expect(fake.calls.kill).toBe(1)
  })

  it('maps the broker result onto the terminal contract', async () => {
    const fake = scriptedTools([{ status: 'killed', ready: true, stop_reason: 'user' }])
    const handle = await createCoreSeam({ tools: fake.tools, sleep: async () => {} }).spawn(
      { prompt: 'work' },
      () => {},
    )
    expect(await handle.done).toEqual({
      runId: 'run-9',
      status: 'killed',
      ready: true,
      stopReason: 'user',
    })
  })

  it('forwards the configured selection into the spawn request', async () => {
    const seen: unknown[] = []
    const fake = scriptedTools([{ status: 'done', ready: true }])
    fake.tools.spawn = async (request) => {
      seen.push(request)
      return { run_id: 'run-9' }
    }
    await createCoreSeam({
      selection: { agent: 'codex', model: 'm' },
      tools: fake.tools,
      sleep: async () => {},
    }).spawn({ prompt: 'p', cwd: '/w', label: 'L' }, () => {})
    expect(seen).toEqual([{ prompt: 'p', dir: '/w', label: 'L', agent: 'codex', model: 'm' }])
  })

  it('notifies a blocked run once and waits for the decision instead of failing', async () => {
    const fake = scriptedTools([
      {
        status: 'waiting',
        ready: false,
        pending_permission: {
          request_id: 'req-1',
          description: 'run a command',
          options: [{ option_id: 'allow_once', label: 'Allow once', kind: 'allow' }],
        },
      },
      {
        status: 'waiting',
        ready: false,
        pending_permission: {
          request_id: 'req-1',
          description: 'run a command',
          options: [{ option_id: 'allow_once', label: 'Allow once', kind: 'allow' }],
        },
      },
      { status: 'running', ready: false },
      { status: 'done', ready: true, output: 'ok' },
    ])

    const notices: Array<{ runId: string; permission: PendingPermission }> = []
    const handle = await createCoreSeam({ tools: fake.tools, sleep: async () => {} }).spawn(
      { prompt: 'work' },
      (runId, permission) => notices.push({ runId, permission }),
    )

    expect(await handle.done).toEqual({ runId: 'run-9', status: 'done', ready: true, output: 'ok' })
    expect(notices).toHaveLength(1)
    expect(notices[0].runId).toBe('run-9')
    expect(notices[0].permission.requestId).toBe('req-1')
    expect(notices[0].permission.options.map((option) => option.optionId)).toEqual(['allow_once'])
  })

  it('notifies again when the run replaces its pending request', async () => {
    const withRequest = (id: string): ScriptedResult => ({
      status: 'waiting',
      ready: false,
      pending_permission: {
        request_id: id,
        description: `ask ${id}`,
        options: [{ option_id: 'deny', label: 'Deny', kind: 'deny' }],
      },
    })
    const fake = scriptedTools([
      withRequest('req-1'),
      withRequest('req-1'),
      withRequest('req-2'),
      withRequest('req-2'),
      { status: 'running', ready: false },
      { status: 'done', ready: true, output: 'ok' },
    ])

    const seen: string[] = []
    const handle = await createCoreSeam({ tools: fake.tools, sleep: async () => {} }).spawn(
      { prompt: 'work' },
      (_runId, permission) => seen.push(permission.requestId),
    )
    await handle.done
    expect(seen).toEqual(['req-1', 'req-2'])
  })
})
