import { describe, expect, it, mock } from 'bun:test'
import { createExtension } from './index.js'
import { buildMockDeps } from './test-fixtures.js'

async function startHostExtension(brokerReceiveTool: any, brokerReplyTool?: any) {
  const registeredTools: Record<string, any> = {}
  const eventHandlers: Record<string, any> = {}
  const sendUserMessage = mock(() => {})
  const mockPi = {
    on: (event: string, handler: any) => {
      eventHandlers[event] = handler
    },
    registerTool: (definition: { name: string }) => {
      registeredTools[definition.name] = definition
    },
    registerCommand: () => {},
    registerMessageRenderer: () => {},
    sendUserMessage,
    events: { emit: mock(() => {}), on: mock(() => () => {}) },
  }
  // Explicit host mode ignores inherited sub-agent broker credentials.
  await createExtension(
    buildMockDeps({ brokerReceiveTool, brokerReplyTool: brokerReplyTool ?? mock(async () => ({ ok: true })) }),
    { pollIntervalMs: 5, subAgentEnv: null },
  )(mockPi as any)
  const ctx = { cwd: '/tmp', ui: { setStatus: mock(() => {}), setWidget: mock(() => {}), notify: mock(() => {}) } }
  await eventHandlers.session_start({}, ctx)
  return { eventHandlers, registeredTools, sendUserMessage, brokerReplyTool: brokerReplyTool ?? null }
}

async function waitFor(predicate: () => boolean, timeoutMs = 6000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for condition')
    await new Promise(resolve => setTimeout(resolve, 25))
  }
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

  it('lets avenor_reply answer a polled ask without an explicit message ID', async () => {
    const brokerReceiveTool = mock(async () => ({
      asks: [{ message_id: 'ask-2', from_run_id: 'rt_child', message: 'need a decision', role: 'agent' }],
    }))
    const brokerReplyTool = mock(async () => ({ ok: true }))
    const harness = await startHostExtension(brokerReceiveTool, brokerReplyTool)
    try {
      await waitFor(() => harness.sendUserMessage.mock.calls.length > 0)
      const result = await harness.registeredTools.avenor_reply.execute('t1', { from_run_id: 'rt_child', message: 'go ahead' })
      expect(result.content[0].text).toBe('Reply sent')
      expect(brokerReplyTool.mock.calls[0][0]).toMatchObject({ toRunId: 'rt_child', replyTo: 'ask-2', message: 'go ahead' })
    } finally {
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })
})
