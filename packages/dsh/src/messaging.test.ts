import { describe, it, expect } from 'bun:test'
import { createMessagingHost, createMessagingRuntime, deliverFrame, startInboundLoop } from './messaging.js'
import type { BoundConversation, BrokerFrame, MessagingClient, MessagingHost, MessagingRuntime } from './messaging.js'
import type { ParentAgent, UserMessageLike } from './types.js'

function fakeClient() {
  const calls: string[] = []
  const client = {
    async messagePreflight() {
      calls.push('preflight')
      return { protocol: 1, generation: 'gen-1' }
    },
    async messagePeers() {
      calls.push('peers')
      return { generation: 'gen-1', peers: [{ run_id: 'run-1', label: 'one' }] }
    },
    async messageBind(request: { session_id: string }) {
      calls.push(`bind:${request.session_id}`)
      return {
        run_id: 'bound-1',
        broker_url: 'http://127.0.0.1:1',
        run_token: 'tok',
        generation: 'gen-1',
        created: true,
      }
    },
  } as unknown as MessagingClient
  return { client, calls }
}

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

function runtimeWith(parent: ParentAgent | undefined): MessagingRuntime {
  const runtime = createMessagingRuntime({
    host: async () => fakeHost(),
    bind: async (sessionId) => ({ sessionId, runId: 'bound-1', brokerUrl: 'http://127.0.0.1:1', token: 'tok' }),
    poll: async () => [],
  })
  if (parent) runtime.remember('rt_1', parent)
  return runtime
}

function fakeHost(): MessagingHost {
  return {
    hostRunId: 'supervisor',
    async message() {
      return { outcome: 'sent' }
    },
    async peers() {
      return { outcome: 'listed' }
    },
  }
}

describe('messaging host', () => {
  it('bootstraps the broker generation', async () => {
    const { client, calls } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })
    expect(host.hostRunId).toBe('supervisor')
    expect(calls).toEqual(['preflight'])
  })

  it('lists peers through the control client', async () => {
    const { client } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })
    expect(JSON.stringify(await host.peers({ action: 'list' }))).toContain('run-1')
  })

  it('refuses an unknown action through the shared router', async () => {
    const { client } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })
    await expect(host.message({ action: 'bogus' })).rejects.toThrow()
  })
})

describe('messaging runtime', () => {
  it('binds one session once and reuses the run id', async () => {
    let binds = 0
    const runtime = createMessagingRuntime({
      host: async () => fakeHost(),
      bind: async (sessionId) => {
        binds += 1
        return { sessionId, runId: 'bound-1', brokerUrl: 'http://127.0.0.1:1', token: 'tok' }
      },
      poll: async () => [],
    })
    const first = await runtime.bind('sess-1')
    const again = await runtime.bind('sess-1')
    expect(binds).toBe(1)
    expect(first.runId).toBe('bound-1')
    expect(again.runId).toBe('bound-1')
  })

  it('builds the host once on demand', async () => {
    let builds = 0
    const runtime = createMessagingRuntime({
      host: async () => {
        builds += 1
        return fakeHost()
      },
    })
    await runtime.host()
    await runtime.host()
    expect(builds).toBe(1)
  })

  it('keys deliveries by run id and forgets on demand', () => {
    const runtime = createMessagingRuntime({ host: async () => fakeHost() })
    const { parent } = fakeParent('running')
    runtime.remember('run-1', parent)
    expect(runtime.parentFor('run-1')).toBe(parent)
    runtime.forget('run-1')
    expect(runtime.parentFor('run-1')).toBeUndefined()
  })
})

describe('inbound delivery', () => {
  it('injects into a running parent', () => {
    const { parent, injected, followed } = fakeParent('running')
    expect(deliverFrame(runtimeWith(parent), frame())).toBe(true)
    expect(followed).toHaveLength(0)
    expect(injected).toHaveLength(1)
    expect((injected[0].content[0] as { text: string }).text).toContain('hello')
  })

  it('wakes an idle parent with a follow-up', () => {
    const { parent, injected, followed } = fakeParent('idle')
    expect(deliverFrame(runtimeWith(parent), frame())).toBe(true)
    expect(injected).toHaveLength(0)
    expect(followed).toHaveLength(1)
  })

  it('drops a frame from a run this harness did not start', () => {
    expect(deliverFrame(runtimeWith(undefined), frame())).toBe(false)
  })

  it('matches a sender registered by its public run id', () => {
    const { parent, injected } = fakeParent('running')
    const runtime = runtimeWith(undefined)
    runtime.remember('run-public-1', parent)
    expect(deliverFrame(runtime, frame())).toBe(true)
    expect(injected).toHaveLength(1)
  })
})

describe('inbound loop', () => {
  it('polls the bound conversation and stops when asked', async () => {
    const { parent, injected } = fakeParent('running')
    const runtime = runtimeWith(parent)
    let polls = 0
    const loop = startInboundLoop({
      sessionId: 'sess-1',
      runtime,
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
    const runtime = createMessagingRuntime({
      host: async () => fakeHost(),
      bind: async () => {
        throw new Error('bind refused')
      },
      poll: async () => [],
    })
    startInboundLoop({ sessionId: 'sess-1', runtime, poll: async () => [], pollIntervalMs: 1, onError: (e) => errors.push(e) })
    const deadline = Date.now() + 2000
    while (errors.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5))
    }
    expect(errors).toHaveLength(1)
  })
})
