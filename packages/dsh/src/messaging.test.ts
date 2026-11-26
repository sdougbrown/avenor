import { describe, it, expect } from 'bun:test'
import { createMessagingHost, HOST_RUN_ID, type MessagingClient } from './messaging.js'

function fakeClient() {
  const calls: string[] = []
  const client = {
    async messagePreflight() {
      calls.push('preflight')
      return { protocol: 1, generation: 'gen-1' }
    },
    async messagePeers() {
      calls.push('peers')
      return {
        generation: 'gen-1',
        peers: [{ run_id: 'run-1', label: 'one', status: 'running' }],
      }
    },
  } as unknown as MessagingClient
  return { client, calls }
}

describe('messaging host', () => {
  it('bootstraps the broker generation and answers as the supervisor', async () => {
    const { client, calls } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })

    expect(host.hostRunId).toBe(HOST_RUN_ID)
    expect(HOST_RUN_ID).toBe('supervisor')
    expect(calls).toEqual(['preflight'])
  })

  it('lists peers through the control client', async () => {
    const { client } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })

    const result = await host.peers({ action: 'list' })
    expect(JSON.stringify(result)).toContain('run-1')
  })

  it('refuses an unknown action through the shared router', async () => {
    const { client } = fakeClient()
    const host = await createMessagingHost({ client, supervisorId: 'in-process' })

    await expect(host.message({ action: 'bogus' })).rejects.toThrow()
  })
})

import { createMessagingRuntime, deliverInbound, startInboundLoop } from './messaging.js'
import type { MessagingHost, MessagingRuntime } from './messaging.js'
import type { ParentAgent, UserMessageLike } from './types.js'
import type { InboundAsk } from '@dougbots/avenor-core'

function ask(overrides: Partial<InboundAsk> = {}): InboundAsk {
  return {
    from_run_id: 'run-1',
    message_id: 'msg-1',
    message: 'may I delete the build directory?',
    expects_reply: true,
    ...overrides,
  }
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

function deliveryRuntime(parent: ParentAgent | undefined) {
  const asks: InboundAsk[] = []
  const runtime: MessagingRuntime = {
    host: async () => fakeHost(),
    remember() {},
    forget() {},
    parentFor: () => parent,
    registerAsk: (value) => asks.push(value),
    ensureInbound() {},
    stop() {},
  }
  return { runtime, asks }
}

async function waitFor(condition: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for condition')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

describe('messaging runtime', () => {
  it('builds the host once on demand', async () => {
    let builds = 0
    const runtime = createMessagingRuntime({
      host: async () => {
        builds += 1
        return fakeHost()
      },
    })
    expect(builds).toBe(0)
    await runtime.host()
    await runtime.host()
    expect(builds).toBe(1)
  })

  it('remembers the agent that started a run and forgets it on demand', () => {
    const runtime = createMessagingRuntime({ host: async () => fakeHost() })
    const { parent } = fakeParent('running')
    runtime.remember('run-1', parent)
    expect(runtime.parentFor('run-1')).toBe(parent)
    runtime.forget('run-1')
    expect(runtime.parentFor('run-1')).toBeUndefined()
  })

  it('starts the receive loop only once', async () => {
    let polls = 0
    const runtime = createMessagingRuntime({
      host: async () => fakeHost(),
      pollTimeoutMs: 5,
      receive: async () => {
        polls += 1
        await new Promise((resolve) => setTimeout(resolve, 5))
        return { asks: [], generation: 'gen-1' }
      },
    })
    runtime.ensureInbound()
    runtime.ensureInbound()
    await waitFor(() => polls > 1)
    runtime.stop()
  })
})

describe('inbound delivery', () => {
  it('injects into a running parent and registers the ask', () => {
    const { parent, injected, followed } = fakeParent('running')
    const { runtime, asks } = deliveryRuntime(parent)

    expect(deliverInbound(runtime, ask())).toBe(true)

    expect(followed).toHaveLength(0)
    expect(injected).toHaveLength(1)
    const text = (injected[0].content[0] as { text: string }).text
    expect(text).toContain('run-1')
    expect(text).toContain('reply_to "msg-1"')
    expect(asks).toHaveLength(1)
  })

  it('wakes an idle parent with a follow-up', () => {
    const { parent, injected, followed } = fakeParent('idle')
    const { runtime } = deliveryRuntime(parent)
    expect(deliverInbound(runtime, ask())).toBe(true)
    expect(injected).toHaveLength(0)
    expect(followed).toHaveLength(1)
  })

  it('drops a frame from a run this harness did not start', () => {
    const { runtime, asks } = deliveryRuntime(undefined)
    expect(deliverInbound(runtime, ask())).toBe(false)
    expect(asks).toHaveLength(0)
  })

  it('does not register a reply edge for a tell', () => {
    const { parent, injected } = fakeParent('running')
    const { runtime, asks } = deliveryRuntime(parent)

    expect(deliverInbound(runtime, ask({ expects_reply: false }))).toBe(true)
    expect(asks).toHaveLength(0)
    const text = (injected[0].content[0] as { text: string }).text
    expect(text).toContain('msg-1')
    expect(text).not.toContain('reply_to')
  })
})

describe('inbound loop', () => {
  it('delivers each frame and stops when asked', async () => {
    const seen: InboundAsk[] = []
    let loop: { stop(): void }
    loop = startInboundLoop({
      receive: async () => ({ asks: [ask()], generation: 'gen-1' }),
      pollTimeoutMs: 5,
      onAsk: (value) => {
        seen.push(value)
        loop.stop()
      },
    })
    await waitFor(() => seen.length === 1)
    expect(seen[0].message_id).toBe('msg-1')
  })

  it('reports a failed poll and keeps polling', async () => {
    const errors: unknown[] = []
    let attempts = 0
    let loop: { stop(): void }
    loop = startInboundLoop({
      receive: async () => {
        attempts += 1
        if (attempts < 2) throw new Error('broker unavailable')
        loop.stop()
        return { asks: [], generation: 'gen-1' }
      },
      pollTimeoutMs: 1,
      onAsk: () => {},
      onError: (error) => errors.push(error),
    })
    await waitFor(() => attempts >= 2)
    expect(errors).toHaveLength(1)
  })
})
