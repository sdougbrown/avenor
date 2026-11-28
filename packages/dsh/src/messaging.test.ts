import { describe, it, expect } from 'bun:test'
import { createMessagingRuntime, deliverFrame, startInboundLoop, type BrokerFrame, type BoundConversation, type MessagingRuntime } from './messaging.js'
import type { MessageProvider } from '@dougbots/avenor-core'
import type { ParentAgent, UserMessageLike } from './types.js'

function frame(): BrokerFrame {
  return {
    id: 'msg-1',
    from_run_id: 'rt_1',
    sender: 'run-public-1',
    payload: { id: 'msg-1', message: 'hello', expects_reply: false },
  }
}

function fakeParent(status: 'idle' | 'running') {
  const injected: UserMessageLike[] = []
  const followed: UserMessageLike[] = []
  const parent: ParentAgent = {
    status,
    inject: (message) => injected.push(message),
    followup: (message) => followed.push(message),
  }
  return { parent, injected, followed }
}

/** A provider that records the calls the router makes; only what a test calls. */
function recordingProvider(calls: string[]): MessageProvider {
  const provider = {
    surface: () => 'dsh',
    role: () => 'agent',
    caller: () => ({ kind: 'owner', runId: 'bound-1' }),
    async peers() {
      calls.push('peers')
      return { peers: [] }
    },
    async resolve(ref: string) {
      return { runId: ref, label: ref, match: 'unique' }
    },
    async tell() {
      calls.push('tell')
      return { outcome: 'sent' }
    },
    async ask() {
      calls.push('ask')
      return { outcome: 'sent' }
    },
    async reply() {
      calls.push('reply')
      return { outcome: 'replied' }
    },
    async withdraw() {
      calls.push('withdraw')
      return { outcome: 'withdrawn' }
    },
    async rename() {
      return { outcome: 'renamed' }
    },
    async last() {
      return { outcome: 'listed' }
    },
  } as unknown as MessageProvider
  return provider
}

function bound(sessionId: string): BoundConversation {
  return { sessionId, runId: 'bound-1', brokerUrl: 'http://127.0.0.1:1', token: 'tok' }
}

describe('messaging runtime', () => {
  it('binds one session once and reuses the run id', async () => {
    let binds = 0
    const runtime = createMessagingRuntime({
      bind: async (sessionId) => {
        binds += 1
        return bound(sessionId)
      },
      poll: async () => [],
    })
    const first = await runtime.bind('sess-1')
    const again = await runtime.bind('sess-1')
    expect(binds).toBe(1)
    expect(first.runId).toBe('bound-1')
    expect(again.runId).toBe('bound-1')
  })

  it('routes a message call as the calling session run', async () => {
    const calls: string[] = []
    const runtime = createMessagingRuntime({
      bind: async (sessionId) => bound(sessionId),
      boundProvider: () => recordingProvider(calls),
      poll: async () => [],
    })
    const result = (await runtime.peers({ action: 'list' }, 'sess-1')) as { peers: unknown[] }
    expect(calls).toEqual(['peers'])
    expect(result.peers).toEqual([])
  })

  it('falls back to the host provider without a session', async () => {
    const calls: string[] = []
    const runtime = createMessagingRuntime({
      hostProvider: async () => recordingProvider(calls),
      poll: async () => [],
    })
    await runtime.peers({ action: 'list' })
    expect(calls).toEqual(['peers'])
  })

  it('keys deliveries by run id and forgets on demand', () => {
    const runtime = createMessagingRuntime({ poll: async () => [] })
    const { parent } = fakeParent('running')
    runtime.remember('run-1', parent)
    expect(runtime.parentFor('run-1')).toBe(parent)
    runtime.forget('run-1')
    expect(runtime.parentFor('run-1')).toBeUndefined()
  })
})

describe('inbound delivery', () => {
  const runtime = (parent?: ParentAgent): MessagingRuntime => {
    const created = createMessagingRuntime({ poll: async () => [] })
    if (parent) created.remember('rt_1', parent)
    return created
  }

  it('injects into a running parent', () => {
    const { parent, injected, followed } = fakeParent('running')
    expect(deliverFrame(runtime(parent), frame())).toBe(true)
    expect(followed).toHaveLength(0)
    expect(injected).toHaveLength(1)
    expect((injected[0].content[0] as { text: string }).text).toContain('hello')
  })

  it('wakes an idle parent with a follow-up', () => {
    const { parent, injected, followed } = fakeParent('idle')
    expect(deliverFrame(runtime(parent), frame())).toBe(true)
    expect(injected).toHaveLength(0)
    expect(followed).toHaveLength(1)
  })

  it('drops a frame from a run this harness did not start', () => {
    expect(deliverFrame(runtime(), frame())).toBe(false)
  })

  it('matches a sender registered by its public run id', () => {
    const { parent, injected } = fakeParent('running')
    const created = runtime()
    created.remember('run-public-1', parent)
    expect(deliverFrame(created, frame())).toBe(true)
    expect(injected).toHaveLength(1)
  })
})

describe('inbound loop', () => {
  it('polls the bound conversation and stops when asked', async () => {
    const { parent, injected } = fakeParent('running')
    const created = createMessagingRuntime({
      bind: async (sessionId) => bound(sessionId),
      poll: async () => [],
    })
    created.remember('rt_1', parent)
    let polls = 0
    const loop = startInboundLoop({
      sessionId: 'sess-1',
      runtime: created,
      poll: async () => {
        polls += 1
        return polls === 1 ? [frame()] : []
      },
      pollIntervalMs: 5,
    })
    const deadline = Date.now() + 2000
    while (injected.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    loop.stop()
    expect(injected).toHaveLength(1)
  })

  it('reports a failed bind and stops', async () => {
    const errors: unknown[] = []
    const created = createMessagingRuntime({
      bind: async () => {
        throw new Error('bind refused')
      },
      poll: async () => [],
    })
    startInboundLoop({
      sessionId: 'sess-1',
      runtime: created,
      poll: async () => [],
      pollIntervalMs: 1,
      onError: (error) => errors.push(error),
    })
    const deadline = Date.now() + 2000
    while (errors.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    expect(errors).toHaveLength(1)
  })
})
