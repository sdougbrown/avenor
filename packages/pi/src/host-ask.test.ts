import { describe, expect, it, mock } from 'bun:test'
import { startExtensionHarness, waitFor } from './test-fixtures.js'

interface HostAsk {
  message_id: string
  from_run_id: string
  message: string
  role?: string
}

async function startHostExtension(
  brokerReceiveTool: any,
  brokerReplyTool?: any,
  extOptions: { pollIntervalMs?: number } = {},
) {
  return startExtensionHarness({
    deps: { brokerReceiveTool, brokerReplyTool: brokerReplyTool ?? mock(async () => ({ ok: true })) },
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

function sentBodies(harness: { sendUserMessage: { mock: { calls: any[][] } } }): string[] {
  return harness.sendUserMessage.mock.calls.map(call => call[0] as string)
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
      expect(body).toContain('"ask-1"')
      expect(options.deliverAs).toBe('steer')
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

  it('lets avenor_reply answer the latest polled ask without an explicit message ID', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [
        { message_id: 'f-old', from_run_id: 'rt_child', message: 'old question', role: 'agent' },
        { message_id: '0-new', from_run_id: 'rt_child', message: 'need a decision', role: 'agent' },
      ],
    }))
    const brokerReplyTool = mock(async () => ({ ok: true }))
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)
      const result = await harness.registeredTools.avenor_reply.execute('t1', { from_run_id: 'rt_child', message: 'go ahead' })
      expect(result.content[0].text).toBe('Reply sent')
      expect(brokerReplyTool.mock.calls[0][0]).toMatchObject({ toRunId: 'rt_child', replyTo: '0-new', message: 'go ahead' })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('answers a shared ask at most once under concurrent avenor_reply calls', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-host-c', from_run_id: 'rt_child', message: 'race me', role: 'agent' }],
    }))
    const brokerReplyTool = mock(async () => ({ ok: true }))
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)

      const [first, second] = await Promise.all([
        harness.registeredTools.avenor_reply.execute('t1', { from_run_id: 'rt_child', message: 'A' }),
        harness.registeredTools.avenor_reply.execute('t2', { from_run_id: 'rt_child', message: 'B' }),
      ])
      const results = [first, second]
      expect(results.filter(result => result.details?.error === true)).toHaveLength(1)
      expect(results.filter(result => result.content[0].text === 'Reply sent')).toHaveLength(1)
      expect(brokerReplyTool).toHaveBeenCalledTimes(1)
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('keeps the pending ask retryable when the host broker send fails', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-host-r', from_run_id: 'rt_child', message: 'retry me', role: 'agent' }],
    }))
    const brokerReplyTool = mock(async (): Promise<{ ok: boolean }> => {
      throw new Error('broker down')
    })
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)

      await expect(harness.registeredTools.avenor_reply.execute('t1', { from_run_id: 'rt_child', message: 'first try' })).rejects.toThrow('broker down')

      brokerReplyTool.mockImplementation(async () => ({ ok: true }))
      const result = await harness.registeredTools.avenor_reply.execute('t2', { from_run_id: 'rt_child', message: 'retry' })
      expect(result.content[0].text).toBe('Reply sent')
      expect(brokerReplyTool).toHaveBeenCalledTimes(2)
      expect(brokerReplyTool.mock.calls[1][0]).toMatchObject({ toRunId: 'rt_child', replyTo: 'ask-host-r', message: 'retry' })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('rejects an explicit reply_to_message_id that is not a pending ask', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-host-u', from_run_id: 'rt_child', message: 'real ask', role: 'agent' }],
    }))
    const brokerReplyTool = mock(async () => ({ ok: true }))
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)

      const result = await harness.registeredTools.avenor_reply.execute('t1', {
        from_run_id: 'rt_child',
        reply_to_message_id: 'ghost-ask',
        message: 'host answer',
      })
      expect(result.details?.error).toBe(true)
      expect(brokerReplyTool).not.toHaveBeenCalled()
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('routes an explicit reply_to_message_id without from_run_id via the pending ask', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-host-i', from_run_id: 'rt_child', message: 'id only', role: 'agent' }],
    }))
    const brokerReplyTool = mock(async () => ({ ok: true }))
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)

      const result = await harness.registeredTools.avenor_reply.execute('t1', {
        reply_to_message_id: 'ask-host-i',
        message: 'host answer',
      })
      expect(result.content[0].text).toBe('Reply sent')
      expect(brokerReplyTool.mock.calls[0][0]).toMatchObject({ toRunId: 'rt_child', replyTo: 'ask-host-i', message: 'host answer' })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('does not resurrect a claimed ask when its failed send rejects after a restart', async () => {
    const brokerReceiveTool = drainOnce([{ message_id: 'ask-old', from_run_id: 'rt_child', message: 'old session ask', role: 'agent' }])
    const send = deferred<{ ok: boolean }>()
    const brokerReplyTool = mock(() => send.promise)
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool, { pollIntervalMs: 10 })
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)
      const inflight = harness.registeredTools.avenor_reply
        .execute('t1', { from_run_id: 'rt_child', message: 'late answer' })
        .then(() => ({ ok: true }), (error: Error) => ({ error }))
      await waitFor(() => brokerReplyTool.mock.calls.length === 1)

      await harness.eventHandlers.session_shutdown({}, {})
      await harness.eventHandlers.session_start({}, harness.ctx)
      send.reject(new Error('broker down'))

      expect(await inflight).toEqual({ error: expect.objectContaining({ message: 'broker down' }) })
      const retry = await harness.registeredTools.avenor_reply.execute('t2', { from_run_id: 'rt_child', message: 'retry' })
      expect(retry.details?.error).toBe(true)
      expect(brokerReplyTool).toHaveBeenCalledTimes(1)
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('drops an in-flight receive that lands after shutdown and does not reschedule it', async () => {
    const first = deferred<{ asks: HostAsk[] }>()
    let calls = 0
    const brokerReceiveTool = mock(async () => {
      calls++
      if (calls === 1) return first.promise
      return { asks: [] }
    })
    const harness = await startHostExtension(brokerReceiveTool, undefined, { pollIntervalMs: 10 })
    try {
      await waitFor(() => calls === 1)
      await harness.eventHandlers.session_shutdown({}, {})
      first.resolve({ asks: [{ message_id: 'ask-late', from_run_id: 'rt_child', message: 'too late', role: 'agent' }] })

      await new Promise(resolve => setTimeout(resolve, 80))
      expect(sentBodies(harness)).toEqual([])
      expect(calls).toBe(1)
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('drops an in-flight receive from the previous session after a restart', async () => {
    const first = deferred<{ asks: HostAsk[] }>()
    let calls = 0
    const brokerReceiveTool = mock(async () => {
      calls++
      if (calls === 1) return first.promise
      return { asks: [] }
    })
    const harness = await startHostExtension(brokerReceiveTool, undefined, { pollIntervalMs: 10 })
    try {
      await waitFor(() => calls === 1)
      await harness.eventHandlers.session_shutdown({}, {})
      await harness.eventHandlers.session_start({}, harness.ctx)
      first.resolve({ asks: [{ message_id: 'ask-stale', from_run_id: 'rt_child', message: 'stale question', role: 'agent' }] })

      await waitFor(() => calls > 1)
      await new Promise(resolve => setTimeout(resolve, 40))
      expect(sentBodies(harness)).toEqual([])
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('keeps pending asks and polling owned by each extension instance', async () => {
    const receiveA = drainOnce([{ message_id: 'ask-a', from_run_id: 'rt_child', message: 'for instance A', role: 'agent' }])
    const receiveB = mock(async () => ({ asks: [] as HostAsk[] }))
    const replyA = mock(async () => ({ ok: true }))
    const replyB = mock(async () => ({ ok: true }))
    const a = await startHostExtension(receiveA, replyA, { pollIntervalMs: 10 })
    const b = await startHostExtension(receiveB, replyB, { pollIntervalMs: 10 })
    try {
      await waitFor(() => a.sendUserMessage.mock.calls.length > 0)
      await b.eventHandlers.session_shutdown({}, {})

      const result = await a.registeredTools.avenor_reply.execute('t1', { from_run_id: 'rt_child', message: 'answer A' })
      expect(result.content[0].text).toBe('Reply sent')
      expect(replyA.mock.calls[0][0]).toMatchObject({ toRunId: 'rt_child', replyTo: 'ask-a', message: 'answer A' })
      expect(replyB).not.toHaveBeenCalled()
    } finally {
      await a.eventHandlers.session_shutdown({}, {})
      await b.eventHandlers.session_shutdown({}, {})
    }
  })

  it('serializes polls so a slow receive cannot overlap or reorder asks', async () => {
    let calls = 0
    let inFlight = 0
    let maxInFlight = 0
    const brokerReceiveTool = mock(async () => {
      calls++
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      try {
        if (calls === 1) {
          await new Promise(resolve => setTimeout(resolve, 40))
          return { asks: [{ message_id: 'ask-1', from_run_id: 'rt_child', message: 'first ask', role: 'agent' }] }
        }
        if (calls === 2) {
          return { asks: [{ message_id: 'ask-2', from_run_id: 'rt_child', message: 'second ask', role: 'agent' }] }
        }
        return { asks: [] }
      } finally {
        inFlight--
      }
    })
    const harness = await startHostExtension(brokerReceiveTool, undefined, { pollIntervalMs: 10 })
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length >= 2)
      const bodies = sentBodies(harness)
      expect(bodies[0]).toContain('first ask')
      expect(bodies[1]).toContain('second ask')
      expect(maxInFlight).toBe(1)
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })
})
