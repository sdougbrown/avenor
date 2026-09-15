import { describe, expect, it, mock } from 'bun:test'
import type { ExtensionDeps } from './index.js'
import { createExtension } from './index.js'
import type { SubAgentEnv } from './subagent-broker.js'
import { buildMockDeps } from './test-fixtures.js'

const subAgentEnv = { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } as const

interface FetchCall {
  path: string
  body: Record<string, unknown>
}

/** Stub global fetch with canned /poll-control responses; records /send bodies. */
function stubBrokerFetch(pollResponses: unknown[], sendFailures = 0, pollDelayMs = 0, pollFailures = 0) {
  const calls: FetchCall[] = []
  let pollIndex = 0
  let pendingSendFailures = sendFailures
  let pendingPollFailures = pollFailures
  let inflight = 0
  const previous = globalThis.fetch
  globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
    inflight++
    try {
      const href = String(url)
      const path = new URL(href).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      calls.push({ path, body })
      if (path === '/poll-control') {
        if (pollDelayMs > 0) await new Promise(resolve => setTimeout(resolve, pollDelayMs))
        if (pendingPollFailures > 0) {
          pendingPollFailures--
          return { ok: false, status: 503, json: async () => ({}), text: async () => 'unavailable' } as Response
        }
        const value = pollIndex < pollResponses.length ? pollResponses[pollIndex] : []
        pollIndex++
        return { ok: true, status: 200, json: async () => value, text: async () => '' } as Response
      }
      if (pendingSendFailures > 0) {
        pendingSendFailures--
        return { ok: false, status: 500, json: async () => ({}), text: async () => 'boom' } as Response
      }
      return { ok: true, status: 200, json: async () => ({ queued: true }), text: async () => '' } as Response
    } finally {
      inflight--
    }
  }) as typeof fetch
  return {
    calls,
    getInflight: () => inflight,
    restore: () => {
      globalThis.fetch = previous
    },
  }
}

async function startSubAgentExtension(opts: {
  fetchStub: ReturnType<typeof stubBrokerFetch>
  subAgentEnv?: SubAgentEnv | null
  deps?: Partial<ExtensionDeps>
}) {
  const registeredTools: Record<string, any> = {}
  const eventHandlers: Record<string, any> = {}
  const sendUserMessage = mock(async () => {})
  const mockPi = {
    on: (event: string, handler: any) => {
      eventHandlers[event] = handler
    },
    registerTool: (definition: { name: string }) => {
      registeredTools[definition.name] = definition
    },
    registerCommand: () => {},
    registerMessageRenderer: () => {},
    events: { emit: mock(() => {}), on: mock(() => () => {}) },
  }
  // null must stay null: it is the declared way to force non-sub-agent mode.
  const subAgentEnvOption = opts.subAgentEnv === undefined ? subAgentEnv : opts.subAgentEnv
  await createExtension(buildMockDeps(opts.deps), { pollIntervalMs: 5, subAgentEnv: subAgentEnvOption })(mockPi as any)
  const ctx = { cwd: '/tmp', sendUserMessage, ui: { setStatus: mock(() => {}), setWidget: mock(() => {}), notify: mock(() => {}) } }
  await eventHandlers.session_start({}, ctx)
  return { ctx, eventHandlers, registeredTools, sendUserMessage }
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for condition')
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

describe('sub-agent broker mode (integration)', () => {
  it('surfaces an inbound ask as a steer and avenor_reply answers the asker with reply_to', async () => {
    const fetchStub = stubBrokerFetch([
      [
        {
          type: 'agent_message',
          payload: { id: 'ask-1', from_run_id: 'supervisor', message: 'is the build green?' },
        },
      ],
    ])
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const { registeredTools, sendUserMessage } = harness

      await waitFor(() => sendUserMessage.mock.calls.length > 0)
      const [body, options] = sendUserMessage.mock.calls[0] as [string, { deliverAs: string }]
      expect(body).toContain('Inbound ask')
      expect(body).toContain('is the build green?')
      expect(body).toContain('"ask-1"')
      expect(options.deliverAs).toBe('steer')

      const result = await registeredTools.avenor_reply.execute('t1', { message: 'yes, green' })
      expect(result.content[0].text).toBe('Reply sent')

      const send = fetchStub.calls.find(call => call.path === '/send')
      expect(send).toBeDefined()
      expect(send!.body).toMatchObject({ run_id: 'rt_child', token: 'tok', type: 'agent_message' })
      const payload = send!.body.payload as Record<string, unknown>
      expect(payload).toMatchObject({
        from_run_id: 'rt_child',
        to_run_id: 'supervisor',
        reply_to: 'ask-1',
        message: 'yes, green',
        role: 'agent',
      })

      // The pending ask is cleared after replying.
      const second = await registeredTools.avenor_reply.execute('t2', { message: 'again?' })
      expect(second.details).toEqual({ error: true })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('tolerates a JSON null poll response (empty queue) and keeps polling', async () => {
    const fetchStub = stubBrokerFetch([
      null,
      [{ type: 'agent_message', payload: { id: 'ask-2', from_run_id: 'supervisor', message: 'later ask' } }],
    ])
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const { sendUserMessage } = harness
      await waitFor(() => sendUserMessage.mock.calls.length > 0)
      const [body] = sendUserMessage.mock.calls[0] as [string]
      expect(body).toContain('later ask')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('keeps polling after a non-ok /poll-control response', async () => {
    const fetchStub = stubBrokerFetch(
      [[{ type: 'agent_message', payload: { id: 'ask-e', from_run_id: 'supervisor', message: 'after the error' } }]],
      0,
      0,
      1,
    )
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const { sendUserMessage } = harness
      // The first tick gets a 503; the loop must reschedule and still surface
      // the ask on a later tick.
      await waitFor(() => sendUserMessage.mock.calls.length > 0)
      const polls = fetchStub.calls.filter(call => call.path === '/poll-control').length
      expect(polls).toBeGreaterThanOrEqual(2)
      const [body] = sendUserMessage.mock.calls[0] as [string]
      expect(body).toContain('after the error')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('answers a shared ask at most once under concurrent avenor_reply calls', async () => {
    const fetchStub = stubBrokerFetch(
      [[{ type: 'agent_message', payload: { id: 'ask-c', from_run_id: 'supervisor', message: 'race me' } }]],
    )
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const { registeredTools, sendUserMessage } = harness
      await waitFor(() => sendUserMessage.mock.calls.length > 0)

      const [first, second] = await Promise.all([
        registeredTools.avenor_reply.execute('t1', { message: 'A' }),
        registeredTools.avenor_reply.execute('t2', { message: 'B' }),
      ])
      const texts = [first.content[0].text, second.content[0].text].sort()
      expect(texts).toEqual([
        'No pending ask found to reply to. Pass from_run_id and/or reply_to_message_id.',
        'Reply sent',
      ])
      expect(fetchStub.calls.filter(call => call.path === '/send')).toHaveLength(1)
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('keeps the pending ask retryable when /send fails', async () => {
    const fetchStub = stubBrokerFetch(
      [[{ type: 'agent_message', payload: { id: 'ask-r', from_run_id: 'supervisor', message: 'retry me' } }]],
      1,
    )
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const { registeredTools, sendUserMessage } = harness
      await waitFor(() => sendUserMessage.mock.calls.length > 0)

      await expect(registeredTools.avenor_reply.execute('t1', { message: 'first try' })).rejects.toThrow('broker /send: 500 boom')

      const result = await registeredTools.avenor_reply.execute('t2', { message: 'retry' })
      expect(result.content[0].text).toBe('Reply sent')
      const payload = fetchStub.calls.filter(call => call.path === '/send').at(-1)!.body.payload as Record<string, unknown>
      expect(payload).toMatchObject({ to_run_id: 'supervisor', reply_to: 'ask-r' })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('routes avenor_reply through the host broker tool outside sub-agent mode', async () => {
    const fetchStub = stubBrokerFetch([])
    const brokerReplyTool = mock(async () => ({ ok: true }))
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub, subAgentEnv: null, deps: { brokerReplyTool } })
      const result = await harness.registeredTools.avenor_reply.execute('t1', {
        from_run_id: 'rt_sub',
        reply_to_message_id: 'ask-h',
        message: 'host answer',
      })
      expect(result.content[0].text).toBe('Reply sent')
      expect(brokerReplyTool).toHaveBeenCalledTimes(1)
      expect(brokerReplyTool.mock.calls[0][0]).toMatchObject({ toRunId: 'rt_sub', replyTo: 'ask-h', message: 'host answer' })
      // Sub-agent polling must not run in host mode.
      expect(fetchStub.calls.some(call => call.path === '/poll-control')).toBe(false)
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('does not reschedule when shutdown lands during an in-flight poll', async () => {
    const fetchStub = stubBrokerFetch([], 0, 40)
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      // Catch a tick mid-flight, then shut down; its completion must not
      // schedule another poll.
      await waitFor(() => fetchStub.getInflight() > 0)
      await harness.eventHandlers.session_shutdown({}, {})
      await waitFor(() => fetchStub.getInflight() === 0)
      const after = fetchStub.calls.filter(call => call.path === '/poll-control').length
      await new Promise(resolve => setTimeout(resolve, 60))
      const later = fetchStub.calls.filter(call => call.path === '/poll-control').length
      expect(later).toBe(after)
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('stops polling on session_shutdown', async () => {
    const fetchStub = stubBrokerFetch([])
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      await waitFor(() => fetchStub.calls.some(call => call.path === '/poll-control'))
      await harness.eventHandlers.session_shutdown({}, {})
      // Drain any in-flight tick first (the barrier), then assert no new poll
      // starts. Counting only after quiescence removes the timing flake.
      await waitFor(() => fetchStub.getInflight() === 0)
      const after = fetchStub.calls.filter(call => call.path === '/poll-control').length
      await new Promise(resolve => setTimeout(resolve, 60))
      const later = fetchStub.calls.filter(call => call.path === '/poll-control').length
      expect(later).toBe(after)
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })
})
