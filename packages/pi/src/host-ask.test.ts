import { describe, expect, it, mock } from 'bun:test'
import { messageTool as realMessageTool, RpcError } from '@dougbots/avenor-core'
import { makeMessageProvider, startExtensionHarness, waitFor } from './test-fixtures.js'

interface HostAsk {
  message_id: string
  from_run_id: string
  message: string
  role?: string
}

/** Record every provider reply and answer it as delivered. */
function replyingProvider(replySpy: (args: Record<string, unknown>) => Promise<unknown>) {
  return makeMessageProvider({
    reply: async (inboundAskId: string, message: string, askerRunId: string) => {
      await replySpy({ askerRunId, inboundAskId, message })
      return { outcome: 'replied', messageId: inboundAskId }
    },
  })
}

async function startHostExtension(
  brokerReceiveTool: any,
  replySpy: (args: Record<string, unknown>) => Promise<unknown> = mock(async (_args: Record<string, unknown>) => {}),
  extOptions: { pollIntervalMs?: number } = {},
  provider = replyingProvider(replySpy),
) {
  return startExtensionHarness({
    deps: {
      brokerReceiveTool,
      messageTool: realMessageTool,
      hostProviderFor: mock(async () => provider),
    },
    extOptions: { subAgentEnv: null, ...extOptions },
  })
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, reject, resolve }
}

/** Drain a queue the way the broker does: each ask is delivered to at most one poll. */
function drainOnce(asks: HostAsk[]) {
  return mock(async () => ({ asks: asks.splice(0, asks.length) }))
}

describe('host inbound ask delivery', () => {
  it('surfaces a broker ask via pi.sendUserMessage as a steer', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-1', from_run_id: 'rt_child', message: 'is the build green?', role: 'agent' }],
    }))
    const harness = await startHostExtension(brokerReceiveTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)
      const [body, options] = harness.sendUserMessage.mock.calls[0] as [string, { deliverAs: string }]
      expect(body).toContain('Ask from sub-agent')
      expect(body).toContain('is the build green?')
      expect(body).toContain('avenor_message({ action: "reply", reply_to: "ask-1"')
      expect(options.deliverAs).toBe('steer')
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('binds the generation the first receive reports', async () => {
    const brokerReceiveTool = mock(async (args: { generation?: string, timeout_ms?: number }) => ({
      asks: [],
      generation: args.generation ?? 'gen-1',
    }))
    const harness = await startHostExtension(brokerReceiveTool)
    try {
      await waitFor(() => brokerReceiveTool.mock.calls.length >= 2)
      expect(brokerReceiveTool.mock.calls[0][0]).toEqual({ timeout_ms: 5 })
      expect(brokerReceiveTool.mock.calls[1][0]).toEqual({ generation: 'gen-1', timeout_ms: 5 })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('rebinds once after a generation mismatch', async () => {
    const seen: string[] = []
    let unbound = 0
    const brokerReceiveTool = mock(async (args: { generation?: string }) => {
      seen.push(args.generation ?? '')
      if (args.generation === 'gen-1') {
        throw new RpcError(-32000, 'messaging: generation mismatch', {
          code: 'generation-mismatch',
          outcome: 'not-sent',
          wire_code: 'generation-mismatch',
        })
      }
      const generation = args.generation ?? (unbound++ === 0 ? 'gen-1' : 'gen-2')
      return { asks: [], generation }
    })
    const harness = await startHostExtension(brokerReceiveTool)
    try {
      await waitFor(() => seen.includes('gen-2'))
      expect(seen.slice(0, 4)).toEqual(['', 'gen-1', '', 'gen-2'])
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('recovers a transient broker receive failure on the next poll', async () => {
    const brokerReceiveTool = mock(async () => {
      if (brokerReceiveTool.mock.calls.length === 1) throw new Error('broker down')
      return {
        asks: [{ message_id: 'ask-recover', from_run_id: 'rt_child', message: 'recover me', role: 'agent' }],
      }
    })
    const harness = await startHostExtension(brokerReceiveTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0, 500)
      expect(brokerReceiveTool.mock.calls.length).toBeGreaterThanOrEqual(2)
      const [body, options] = harness.sendUserMessage.mock.calls[0] as [string, { deliverAs: string }]
      expect(body).toContain('Ask from sub-agent')
      expect(body).toContain('recover me')
      expect(options.deliverAs).toBe('steer')
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('answers the polled ask it is addressed to', async () => {
    const brokerReceiveTool = drainOnce([
      { message_id: 'f-old', from_run_id: 'rt_child', message: 'old question', role: 'agent' },
      { message_id: '0-new', from_run_id: 'rt_child', message: 'need a decision', role: 'agent' },
    ])
    const replySpy = mock(async (_args: Record<string, unknown>) => {})
    const harness = await startHostExtension(brokerReceiveTool, replySpy)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length === 2)
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: '0-new',
        message: 'go ahead',
      })
      expect(result.details.outcome).toBe('replied')
      expect(replySpy.mock.calls[0][0]).toMatchObject({
        askerRunId: 'rt_child',
        inboundAskId: '0-new',
        message: 'go ahead',
      })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('answers a shared ask at most once under concurrent replies', async () => {
    const brokerReceiveTool = drainOnce([
      { message_id: 'ask-host-c', from_run_id: 'rt_child', message: 'race me', role: 'agent' },
    ])
    const replySpy = mock(async (_args: Record<string, unknown>) => {})
    const harness = await startHostExtension(brokerReceiveTool, replySpy)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length === 1)
      const [first, second] = await Promise.all([
        harness.registeredTools.avenor_message.execute('t1', { action: 'reply', reply_to: 'ask-host-c', message: 'A' }),
        harness.registeredTools.avenor_message.execute('t2', { action: 'reply', reply_to: 'ask-host-c', message: 'B' }),
      ])
      const outcomes = [first, second].map(result => result.details.outcome).sort()
      expect(outcomes).toEqual(['edge-absent', 'replied'])
      expect(replySpy).toHaveBeenCalledTimes(1)
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('keeps the pending ask retryable when the host broker send fails', async () => {
    const brokerReceiveTool = drainOnce([
      { message_id: 'ask-host-r', from_run_id: 'rt_child', message: 'retry me', role: 'agent' },
    ])
    const replySpy = mock(async (_args: Record<string, unknown>): Promise<void> => {
      throw new Error('broker down')
    })
    const harness = await startHostExtension(brokerReceiveTool, replySpy)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length === 1)
      await expect(
        harness.registeredTools.avenor_message.execute('t1', { action: 'reply', reply_to: 'ask-host-r', message: 'first try' }),
      ).rejects.toThrow('broker down')
      replySpy.mockImplementation(async () => {})
      const result = await harness.registeredTools.avenor_message.execute('t2', {
        action: 'reply',
        reply_to: 'ask-host-r',
        message: 'retry',
      })
      expect(result.details.outcome).toBe('replied')
      expect(replySpy).toHaveBeenCalledTimes(2)
      expect(replySpy.mock.calls[1][0]).toMatchObject({
        askerRunId: 'rt_child',
        inboundAskId: 'ask-host-r',
        message: 'retry',
      })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('rejects a reply to an ask this surface never received', async () => {
    const replySpy = mock(async (_args: Record<string, unknown>) => {})
    const harness = await startHostExtension(mock(async () => ({ asks: [] })), replySpy)
    try {
      const result = await harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'ask-host-i',
        message: 'host answer',
      })
      expect(result.details.outcome).toBe('edge-absent')
      expect(replySpy).not.toHaveBeenCalled()
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('does not resurrect a claimed ask when its failed send outlives the session', async () => {
    const brokerReceiveTool = drainOnce([
      { message_id: 'ask-old', from_run_id: 'rt_child', message: 'old session ask', role: 'agent' },
    ])
    const send = deferred<{ outcome: string }>()
    const replySpy = mock(() => send.promise)
    const harness = await startHostExtension(brokerReceiveTool, replySpy, { pollIntervalMs: 10 })
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length === 1)
      const inflight = harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'ask-old',
        message: 'late answer',
      })
      await waitFor(() => replySpy.mock.calls.length === 1)
      await harness.eventHandlers.session_shutdown({}, {})
      send.reject(new Error('session gone'))
      await expect(inflight).rejects.toThrow('session gone')

      const afterRestart = await harness.registeredTools.avenor_message.execute('t2', {
        action: 'reply',
        reply_to: 'ask-old',
        message: 'retry',
      })
      expect(afterRestart.details.outcome).toBe('edge-absent')
      expect(replySpy).toHaveBeenCalledTimes(1)
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('keeps a claimed ask restorable when another supervisor is disposed', async () => {
    const brokerReceiveTool = drainOnce([
      { message_id: 'ask-kept', from_run_id: 'rt_child', message: 'keep my claim', role: 'agent' },
    ])
    const send = deferred<{ outcome: string }>()
    let sends = 0
    const replySpy = mock((_args: Record<string, unknown>) => {
      sends++
      return sends === 1 ? send.promise : Promise.resolve({ outcome: 'replied' })
    })
    // hostProviderFor serves a provider per supervisor, so each key has its own claim map.
    const harness = await startExtensionHarness({
      deps: {
        brokerReceiveTool,
        messageTool: realMessageTool,
        hostProviderFor: mock(async (supervisorId: string | undefined) => makeMessageProvider({
          reply: async (inboundAskId: string, message: string, askerRunId: string) => {
            await replySpy({ supervisorId, askerRunId, inboundAskId, message })
            return { outcome: 'replied', messageId: inboundAskId }
          },
        })),
      },
      extOptions: { subAgentEnv: null, pollIntervalMs: 10 },
    })
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length === 1)
      const inflight = harness.registeredTools.avenor_message.execute('t1', {
        action: 'reply',
        reply_to: 'ask-kept',
        message: 'first try',
      })
      await waitFor(() => replySpy.mock.calls.length === 1)
      await harness.registeredTools.avenor_shutdown.execute('t2', { supervisor_id: 'sup-b' })
      send.reject(new Error('send failed'))
      await expect(inflight).rejects.toThrow('send failed')

      const retried = await harness.registeredTools.avenor_message.execute('t3', {
        action: 'reply',
        reply_to: 'ask-kept',
        message: 'retry',
      })
      expect(retried.details.outcome).toBe('replied')
      expect(replySpy).toHaveBeenCalledTimes(2)
      expect(replySpy.mock.calls[1][0]).toMatchObject({
        askerRunId: 'rt_child',
        inboundAskId: 'ask-kept',
        message: 'retry',
      })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('keeps pending asks and polling owned by each extension instance', async () => {
    const asksA = [{ message_id: 'ask-a', from_run_id: 'rt_a', message: 'for A', role: 'agent' }]
    const asksB = [{ message_id: 'ask-b', from_run_id: 'rt_b', message: 'for B', role: 'agent' }]
    const replyA = mock(async (_args: Record<string, unknown>) => {})
    const replyB = mock(async (_args: Record<string, unknown>) => {})
    const a = await startHostExtension(drainOnce(asksA), replyA, { pollIntervalMs: 10 })
    const b = await startHostExtension(drainOnce(asksB), replyB, { pollIntervalMs: 10 })
    try {
      await waitFor(() => a.sendUserMessage.mock.calls.length === 1)
      await waitFor(() => b.sendUserMessage.mock.calls.length === 1)
      await a.registeredTools.avenor_message.execute('t1', { action: 'reply', reply_to: 'ask-a', message: 'answer A' })
      expect(replyA.mock.calls[0][0]).toMatchObject({ askerRunId: 'rt_a', inboundAskId: 'ask-a' })
      expect(replyB).not.toHaveBeenCalled()
    } finally {
      await a.eventHandlers.session_shutdown({}, {})
      await b.eventHandlers.session_shutdown({}, {})
    }
  })
})
