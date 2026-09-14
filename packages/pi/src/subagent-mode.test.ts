import { describe, expect, it, mock } from 'bun:test'
import type { ExtensionDeps } from './index.js'
import { createExtension } from './index.js'

const subAgentEnv = { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } as const

function buildMockDeps(partial: Partial<ExtensionDeps> = {}): ExtensionDeps {
  return {
    spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock' })),
    statusTool: mock(async () => []),
    eventsTool: mock(async () => ({ events: [] })),
    answerPermissionTool: mock(async () => ({ ok: true })),
    followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
    inspectTool: mock(async () => ({ status: 'running' })),
    resultTool: mock(async () => ({ run_id: 'run-1', status: 'done', ready: true })),
    brokerReceiveTool: mock(async () => ({ asks: [] })),
    brokerReplyTool: mock(async () => ({ ok: true })),
    shutdownTool: mock(async () => ({ ok: true })),
    workflowStatusTool: mock(async () => ({})),
    workflowWaitTool: mock(async () => ({})),
    workflowInspectTool: mock(async () => ({})),
    workflowEventsTool: mock(async () => ({})),
    workflowCompleteTool: mock(async () => ({})),
    workflowGateTool: mock(async () => ({})),
    observeRun: mock(() => null),
    dial: mock(async () => ({ close() {} })),
    Supervisor: class {} as any,
    ...partial,
  }
}

interface FetchCall {
  path: string
  body: Record<string, unknown>
}

/** Stub global fetch with canned /poll-control responses; records /send bodies. */
function stubBrokerFetch(pollResponses: unknown[], sendFailures = 0) {
  const calls: FetchCall[] = []
  let pollIndex = 0
  let pendingSendFailures = sendFailures
  const previous = globalThis.fetch
  globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
    const href = String(url)
    const path = new URL(href).pathname
    const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
    calls.push({ path, body })
    if (path === '/poll-control') {
      const value = pollIndex < pollResponses.length ? pollResponses[pollIndex] : []
      pollIndex++
      return { ok: true, status: 200, json: async () => value, text: async () => '' } as Response
    }
    if (pendingSendFailures > 0) {
      pendingSendFailures--
      return { ok: false, status: 500, json: async () => ({}), text: async () => 'boom' } as Response
    }
    return { ok: true, status: 200, json: async () => ({ queued: true }), text: async () => '' } as Response
  }) as typeof fetch
  return {
    calls,
    restore: () => {
      globalThis.fetch = previous
    },
  }
}

async function startSubAgentExtension(opts: {
  fetchStub: ReturnType<typeof stubBrokerFetch>
  subAgentEnv?: { brokerUrl: string; runId: string; token: string } | null
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
  await createExtension(buildMockDeps(), { pollIntervalMs: 5, subAgentEnv: opts.subAgentEnv ?? subAgentEnv })(mockPi as any)
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

  it('stops polling on session_shutdown', async () => {
    const fetchStub = stubBrokerFetch([])
    try {
      const { eventHandlers } = await startSubAgentExtension({ fetchStub })
      await waitFor(() => fetchStub.calls.some(call => call.path === '/poll-control'))
      await eventHandlers.session_shutdown({}, {})
      // Let any in-flight tick land, then assert no further polls arrive.
      await new Promise(resolve => setTimeout(resolve, 30))
      const after = fetchStub.calls.filter(call => call.path === '/poll-control').length
      await new Promise(resolve => setTimeout(resolve, 60))
      const later = fetchStub.calls.filter(call => call.path === '/poll-control').length
      expect(later).toBe(after)
    } finally {
      fetchStub.restore()
    }
  })
})