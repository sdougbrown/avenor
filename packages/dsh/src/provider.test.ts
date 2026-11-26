import { describe, it, expect } from 'bun:test'
import {
  createAvenorProvider,
  promptText,
  type AvenorHandle,
  type AvenorSeam,
} from './provider.js'
import type { SubagentProvider, SubagentStartRequest } from './types.js'
import { diagnosticFor, stopReasonFor, toSubagentResult, type AvenorTerminal } from './stop-reason.js'

function terminal(overrides: Partial<AvenorTerminal> = {}): AvenorTerminal {
  return { runId: 'run-1', status: 'done', ready: true, output: 'finished', ...overrides }
}

interface FakeSeam {
  readonly seam: AvenorSeam
  readonly calls: { spawn: number; cancel: number }
  readonly requests: Array<{ prompt: string; cwd?: string; label?: string }>
  settle(value: AvenorTerminal): void
}

/** A seam whose run settles only when the test says so. */
function fakeSeam(): FakeSeam {
  const calls = { spawn: 0, cancel: 0 }
  const requests: Array<{ prompt: string; cwd?: string; label?: string }> = []
  let settle = (_value: AvenorTerminal): void => {}

  const seam: AvenorSeam = {
    async spawn(request) {
      calls.spawn += 1
      requests.push(request)
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

  return { seam, calls, requests, settle: (value) => settle(value) }
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
    ...extra,
  }
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
    const run = await startRun(
      createAvenorProvider(fake.seam),
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

  it('cancels the run on abort and again on dispose', async () => {
    const fake = fakeSeam()
    const provider = createAvenorProvider(fake.seam)
    const controller = new AbortController()
    const run = await startRun(provider, {
      prompt: [{ type: 'text', text: 'work' }],
      signal: controller.signal,
    })

    controller.abort()
    expect(fake.calls.cancel).toBe(1)

    await run.dispose()
    expect(fake.calls.cancel).toBe(2)
  })

  it('propagates a start failure instead of minting a run', async () => {
    const seam: AvenorSeam = {
      async spawn() {
        throw new Error('avenor binary not found')
      },
    }
    await expect(startRun(createAvenorProvider(seam), startRequest('work'))).rejects.toThrow(
      'avenor binary not found',
    )
  })
})
