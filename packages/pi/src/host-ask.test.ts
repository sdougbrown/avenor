import { describe, expect, it, mock } from 'bun:test'
import { startExtensionHarness, waitFor } from './test-fixtures.js'

async function startHostExtension(brokerReceiveTool: any, brokerReplyTool?: any) {
  return startExtensionHarness({
    deps: { brokerReceiveTool, brokerReplyTool: brokerReplyTool ?? mock(async () => ({ ok: true })) },
    extOptions: { subAgentEnv: null },
  })
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
})
