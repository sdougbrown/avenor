import { describe, expect, it, mock } from 'bun:test'
import type { ExtensionDeps } from './index.js'
import type { SubAgentEnv } from './subagent-broker.js'
import { messageTool as realMessageTool } from '@dougbots/avenor-core'
import { makeMessageProvider, settleWithin, startExtensionHarness, waitFor, waitUntilAborted } from './test-fixtures.js'

const subAgentEnv = { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } as const

/** The broker's peer listing a child resolves its targets through. */
const sessionsBody = {
  sessions: [{ run_id: 'supervisor', label: 'supervisor', status: 'running', kind: 'supervisor' }],
  generation: 'gen-fixture',
  messaging_protocol: 1,
}

const supervisorSessions = () => Response.json(sessionsBody)

interface FetchCall {
  path: string
  body: Record<string, unknown>
}

type BrokerHandler = (init: RequestInit | undefined) => Response | Promise<Response>

/**
 * Stub global fetch with canned /poll-control responses; records every call.
 * A per-path handler replaces the canned response for that path.
 */
function stubBrokerFetch(
  pollResponses: unknown[],
  sendFailures = 0,
  pollDelayMs = 0,
  pollFailures = 0,
  handlers: Record<string, BrokerHandler> = {},
) {
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
      const handler = handlers[path]
      if (handler) return await handler(init)
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
      if (path === '/sessions') {
        return { ok: true, status: 200, json: async () => sessionsBody, text: async () => '' } as Response
      }
      if (path === '/send' && pendingSendFailures > 0) {
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
    setHandler: (path: string, handler: BrokerHandler) => {
      handlers[path] = handler
    },
    restore: () => {
      globalThis.fetch = previous
    },
  }
}

async function startSubAgentExtension(opts: {
  fetchStub: ReturnType<typeof stubBrokerFetch>
  subAgentEnv?: SubAgentEnv | null
  deps?: Partial<ExtensionDeps>
  pollIntervalMs?: number
}) {
  // null must stay null: it is the declared way to force non-sub-agent mode.
  return startExtensionHarness({
    // Every action routes through the one message handler; the child provider
    // the extension builds is what makes the broker calls.
    deps: { messageTool: realMessageTool, ...opts.deps },
    extOptions: {
      subAgentEnv: opts.subAgentEnv === undefined ? subAgentEnv : opts.subAgentEnv,
      ...(opts.pollIntervalMs !== undefined && { pollIntervalMs: opts.pollIntervalMs }),
    },
  })
}

describe('sub-agent broker mode (integration)', () => {
  it('surfaces an inbound ask as a steer and avenor_message answers the asker with reply_to', async () => {
    const fetchStub = stubBrokerFetch([
      [
        {
          type: 'agent_message',
          payload: { id: 'ask-1', from_run_id: 'supervisor', message: 'is the build green?' },
        },
      ],
    ], 0, 0, 0, {
      '/send': () => Response.json({ queued: true, outcome: 'replied' }),
    })
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

      const result = await registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'ask-1',
        message: 'yes, green',
      })
      expect(result.details.outcome).toBe('replied')
      expect(result.content[0].text).toBe('reply to "ask-1" delivered')

      const send = fetchStub.calls.find(call => call.path === '/send')
      expect(send).toBeDefined()
      expect(send!.body).toMatchObject({ run_id: 'rt_child', token: 'tok', type: 'agent_message' })
      const payload = send!.body.payload as Record<string, unknown>
      expect(typeof payload.id).toBe('string')
      expect(payload).toEqual({
        id: payload.id,
        from: 'rt_child',
        from_run_id: 'rt_child',
        to_run_id: 'supervisor',
        message: 'yes, green',
        role: 'agent',
        reply_to: 'ask-1',
      })

      // The pending ask is cleared after replying.
      const second = await registeredTools.avenor_message.execute('t2', {
        action: 'reply',
        reply_to: 'ask-1',
        message: 'again?',
      })
      expect(second.details.outcome).toBe('edge-absent')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('interrupts a blocking avenor_result when the sub-agent receives an ask', async () => {
    const fetchStub = stubBrokerFetch([])
    const resultTool = mock((args: { signal?: AbortSignal }) => new Promise<never>((_resolve, reject) => {
      args.signal?.addEventListener('abort', () => reject(new Error('result wait cancelled')))
    }))
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub, deps: { resultTool }, pollIntervalMs: 5 })
      const resultPromise: Promise<{ details?: unknown }> = harness.registeredTools.avenor_result.execute('t1', { run_id: 'supervisor' })
      await waitFor(() => resultTool.mock.calls.length === 1)

      fetchStub.setHandler('/poll-control', () => Response.json([
        { type: 'agent_message', payload: { id: 'ask-block', from_run_id: 'supervisor', message: 'still there?' } },
      ]))
      await waitFor(() => harness!.sendUserMessage.mock.calls.length > 0)
      const interrupted = await settleWithin(
        resultPromise,
        'blocking avenor_result did not return after the sub-agent received an ask',
      )
      expect(interrupted.details).toMatchObject({
        run_id: 'supervisor',
        status: 'running',
        ready: false,
        interrupted_by: { run_id: 'supervisor', status: 'asking' },
      })
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
      const [body] = sendUserMessage.mock.calls[0]!
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
      const [body] = sendUserMessage.mock.calls[0]!
      expect(body).toContain('after the error')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('answers a shared ask at most once under concurrent replies', async () => {
    const fetchStub = stubBrokerFetch(
      [[{ type: 'agent_message', payload: { id: 'ask-c', from_run_id: 'supervisor', message: 'race me' } }]],
      0, 0, 0, { '/send': () => Response.json({ queued: true, outcome: 'replied' }) },
    )
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const { registeredTools, sendUserMessage } = harness
      await waitFor(() => sendUserMessage.mock.calls.length > 0)

      const [first, second] = await Promise.all([
        registeredTools.avenor_message.execute('t1', { action: 'reply', reply_to: 'ask-c', message: 'A' }),
        registeredTools.avenor_message.execute('t2', { action: 'reply', reply_to: 'ask-c', message: 'B' }),
      ])
      const outcomes = [first.details.outcome, second.details.outcome].sort()
      expect(outcomes).toEqual(['edge-absent', 'replied'])
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

      const failed = await registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'ask-r',
        message: 'first try',
      })
      expect(failed.details.outcome).toBe('possibly-pending')
      fetchStub.setHandler('/send', () => Response.json({ queued: true, outcome: 'replied' }))

      const result = await registeredTools.avenor_message.execute('t2', {
        action: 'reply',
        reply_to: 'ask-r',
        message: 'retry',
      })
      expect(result.details.outcome).toBe('replied')
      const payload = fetchStub.calls.filter(call => call.path === '/send').at(-1)!.body.payload as Record<string, unknown>
      expect(payload).toMatchObject({ to_run_id: 'supervisor', reply_to: 'ask-r' })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('reports an absent child withdrawal as edge-absent, not an error', async () => {
    const fetchStub = stubBrokerFetch([], 0, 0, 0, {
      '/cancel_message': () => new Response('no pending ask for this message id', { status: 404 }),
    })
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'withdraw',
        message_id: 'ask-1',
      })
      expect(result.details.outcome).toBe('edge-absent')
      expect(result.content[0].text).toBe('no ask "ask-1" is pending; nothing was withdrawn')
      expect(fetchStub.calls.find(call => call.path === '/cancel_message')?.body).toMatchObject({
        run_id: 'rt_child',
        token: 'tok',
        cancel_message_id: 'ask-1',
      })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('reports a child withdrawal transport failure with the broker text', async () => {
    const fetchStub = stubBrokerFetch([], 0, 0, 0, {
      '/cancel_message': () => new Response('broker unavailable', { status: 503 }),
    })
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const error = await harness.registeredTools.avenor_message
        .execute('t1', { action: 'withdraw', message_id: 'ask-1' })
        .catch((caught: unknown) => caught)
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toContain('broker unavailable')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('routes a reply through the host provider outside sub-agent mode', async () => {
    const fetchStub = stubBrokerFetch([])
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-h', from_run_id: 'rt_sub', message: 'host question', role: 'agent' }],
    }))
    const reply = mock(async (_asker: string, _id: string, _message: string) => ({
      outcome: 'replied' as const,
      messageId: 'ask-h',
    }))
    const provider = makeMessageProvider({ reply })
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({
        fetchStub,
        subAgentEnv: null,
        deps: {
          brokerReceiveTool,
          messageTool: realMessageTool,
          hostProviderFor: mock(async () => provider),
        },
      })
      await waitFor(() => harness!.sendUserMessage.mock.calls.length > 0)
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'ask-h',
        message: 'host answer',
      })
      expect(result.details.outcome).toBe('replied')
      expect(reply).toHaveBeenCalledTimes(1)
      expect(reply.mock.calls[0]).toEqual(['ask-h', 'host answer', 'rt_sub'])
      // Sub-agent polling must not run in host mode.
      expect(fetchStub.calls.some(call => call.path === '/poll-control')).toBe(false)
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('does not start a host supervisor from a sub-agent session', async () => {
    const fetchStub = stubBrokerFetch([])
    const brokerReceiveTool = mock(async () => ({ asks: [] }))
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub, deps: { brokerReceiveTool } })
      await new Promise(resolve => setTimeout(resolve, 50))
      expect(fetchStub.calls.some(call => call.path === '/poll-control')).toBe(true)
      expect(brokerReceiveTool).not.toHaveBeenCalled()
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('does not deliver an old sub-agent receive into a restarted session', async () => {
    const fetchStub = stubBrokerFetch([])
    const delegate = globalThis.fetch
    const receive = Promise.withResolvers<Response>()
    let polls = 0
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      if (new URL(String(url)).pathname === '/poll-control' && ++polls === 1) return receive.promise
      return delegate(url, init)
    }) as typeof fetch
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      await waitFor(() => polls === 1)
      await harness.eventHandlers.session_shutdown({}, {})
      await harness.eventHandlers.session_start({}, harness.ctx)
      receive.resolve(new Response(JSON.stringify([
        { type: 'agent_message', payload: { id: 'old-child-ask', from_run_id: 'supervisor', message: 'old session question' } },
      ])))
      await waitFor(() => polls > 1)
      expect(harness.sendUserMessage).not.toHaveBeenCalled()
      const reply = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'old-child-ask',
        message: 'stale answer',
      })
      expect(reply.details.outcome).toBe('edge-absent')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      receive.resolve(new Response('[]'))
      fetchStub.restore()
    }
  })

  it('does not restore a sub-agent ask when its failed send outlives the session', async () => {
    const fetchStub = stubBrokerFetch(
      [[{ type: 'agent_message', payload: { id: 'old-child-reply', from_run_id: 'supervisor', message: 'reply before shutdown' } }]],
      0, 0, 0, { '/send': () => Response.json({ queued: true, outcome: 'replied' }) },
    )
    const delegate = globalThis.fetch
    const send = Promise.withResolvers<Response>()
    let sendCalls = 0
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      if (new URL(String(url)).pathname === '/send' && ++sendCalls === 1) {
        return send.promise
      }
      return delegate(url, init)
    }) as typeof fetch
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      await waitFor(() => harness!.sendUserMessage.mock.calls.length > 0)
      const reply = harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'old-child-reply',
        message: 'late answer',
      })
      await waitFor(() => sendCalls === 1)
      await harness.eventHandlers.session_shutdown({}, {})
      await harness.eventHandlers.session_start({}, harness.ctx)
      send.resolve(new Response('broker down', { status: 503 }))
      expect((await reply).details.outcome).toBe('possibly-pending')
      const retry = await harness.registeredTools.avenor_message.execute('t2', {
        action: 'reply',
        reply_to: 'old-child-reply',
        message: 'stale retry',
      })
      expect(retry.details.outcome).toBe('edge-absent')
      expect(sendCalls).toBe(1)
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      send.resolve(new Response('{}'))
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

  it('asks through the inherited broker as the child run', async () => {
    const fetchStub = stubBrokerFetch([], 0, 0, 0, {
      '/sessions': supervisorSessions,
      '/wait_reply': () => Response.json({ outcome: 'replied', from_run_id: 'supervisor', payload: { message: 'green' } }),
    })
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'ask',
        target: 'supervisor',
        message: 'is the build green?',
      })
      expect(result.content).toEqual([{ type: 'text', text: 'green' }])
      expect(result.details.outcome).toBe('replied')
      expect(result.details.reply).toBe('green')

      const send = fetchStub.calls.find(call => call.path === '/send')
      expect(send?.body).toMatchObject({
        run_id: 'rt_child',
        token: 'tok',
        from_run_id: 'rt_child',
        to_run_id: 'supervisor',
        type: 'agent_message',
      })
      const payload = send?.body.payload as Record<string, unknown>
      expect(typeof payload.id).toBe('string')
      expect(payload).toEqual({
        id: payload.id,
        from: 'rt_child',
        from_run_id: 'rt_child',
        to_run_id: 'supervisor',
        message: 'is the build green?',
        role: 'agent',
        expects_reply: true,
      })
      expect(fetchStub.calls.find(call => call.path === '/wait_reply')?.body).toMatchObject({
        run_id: 'rt_child',
        token: 'tok',
        waiting_for: payload.id,
      })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('reports an ask with no reply as timed-out', async () => {
    const fetchStub = stubBrokerFetch([], 0, 0, 0, {
      '/sessions': supervisorSessions,
      '/wait_reply': () => Response.json({ outcome: 'timed-out' }),
    })
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'ask',
        target: 'supervisor',
        message: 'is the build green?',
      })
      expect(result.details.outcome).toBe('timed-out')
      expect(result.content[0].text).toContain('withdraw it with avenor_message action="withdraw"')
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('withdraws a child ask when the tool call aborts', async () => {
    const fetchStub = stubBrokerFetch([], 0, 0, 0, {
      '/sessions': supervisorSessions,
      '/wait_reply': waitUntilAborted,
      '/cancel_message': () => Response.json({ outcome: 'withdrawn' }),
    })
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    const controller = new AbortController()
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const pending = harness.registeredTools.avenor_message
        .execute('t1', { action: 'ask', target: 'supervisor', message: 'anyone there?' }, controller.signal)
      await waitFor(() => fetchStub.calls.some(call => call.path === '/wait_reply'))
      controller.abort()

      const result = await pending
      expect(result.details.outcome).toBe('withdrawn')
      const messageId = String(
        (fetchStub.calls.find(call => call.path === '/send')?.body.payload as Record<string, unknown>).id,
      )
      expect(fetchStub.calls.find(call => call.path === '/cancel_message')?.body).toMatchObject({
        run_id: 'rt_child',
        token: 'tok',
        cancel_message_id: messageId,
      })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
    }
  })

  it('withdraws through the broker without starting a supervisor', async () => {
    const fetchStub = stubBrokerFetch([], 0, 0, 0, {
      '/cancel_message': () => Response.json({ cancelled: true, outcome: 'withdrawn' }),
    })
    // The host path resolves a supervisor through AVENOR_BIN; pointing it at
    // nothing makes any fall-through fail here instead of spawning one.
    const previousBin = process.env.AVENOR_BIN
    process.env.AVENOR_BIN = '/nonexistent/avenor-test-guard'
    let harness: Awaited<ReturnType<typeof startSubAgentExtension>> | undefined
    try {
      harness = await startSubAgentExtension({ fetchStub })
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'withdraw',
        message_id: 'ask-c1',
      })
      expect(result.details.outcome).toBe('withdrawn')
      expect(result.content[0].text).toBe('ask "ask-c1" withdrawn')
      expect(fetchStub.calls.find(call => call.path === '/cancel_message')?.body).toMatchObject({
        run_id: 'rt_child',
        token: 'tok',
        cancel_message_id: 'ask-c1',
      })
    } finally {
      await harness?.eventHandlers.session_shutdown({}, {})
      fetchStub.restore()
      if (previousBin === undefined) delete process.env.AVENOR_BIN
      else process.env.AVENOR_BIN = previousBin
    }
  })
})

describe('avenor_spawn parent run id in sub-agent mode', () => {
  it('leaves the parent unnamed: a nested supervisor has no run for this session', async () => {
    // A child that spawns starts its own supervisor, whose broker never
    // registered the child's own run id. Naming it would prepend a note
    // pointing at a run nothing reads, so the asker would wait out its timeout.
    const spawnToolMock = mock(async (_args: { agent?: string; prompt?: string; wait?: boolean }) => ({
      run_id: 'run-1',
      label: 'grandchild',
      supervisor_id: '/tmp/sock',
      runtime_id: 'rt-1',
    }))
    const h = await startExtensionHarness({
      deps: { spawnTool: spawnToolMock },
      extOptions: { subAgentEnv },
    })

    await h.registeredTools.avenor_spawn.execute(
      't-nested',
      { agent: 'mule', prompt: 'do the thing', wait: false },
      undefined,
      undefined,
      { cwd: '/tmp' },
    )

    expect(spawnToolMock.mock.calls[0]?.[0]).not.toHaveProperty('parent_run_id')
  })
})
