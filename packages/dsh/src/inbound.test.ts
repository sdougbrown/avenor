import { describe, it, expect } from 'bun:test'
import { inboundFrameToAsk, inboundMessage } from './inbound.js'
import type { BrokerFrame } from './messaging.js'

function frame(overrides: Partial<BrokerFrame> = {}): BrokerFrame {
  return {
    id: 'av1.msg.1',
    from_run_id: 'rt_1',
    sender: 'run-public-1',
    payload: {
      id: 'av1.msg.1',
      message: 'ship it?',
      expects_reply: true,
    },
    ...overrides,
  }
}

const textOf = (message: ReturnType<typeof inboundMessage>) =>
  (message.content[0] as { text: string }).text

describe('inbound frame', () => {
  it('normalizes the broker frame, preferring the payload fields', () => {
    expect(inboundFrameToAsk(frame())).toEqual({
      fromRunId: 'rt_1',
      messageId: 'av1.msg.1',
      message: 'ship it?',
      expectsReply: true,
    })
  })

  it('drops a frame that carries no message text', () => {
    expect(inboundFrameToAsk(frame({ payload: { id: 'x' } }))).toBeUndefined()
  })

  it('treats a missing expects_reply as a tell', () => {
    expect(inboundFrameToAsk(frame({ payload: { id: 'x', message: 'hi' } }))?.expectsReply).toBe(false)
  })
})

describe('inbound message', () => {
  it('names the sender and frames the text as a peer, not the operator', () => {
    const text = textOf(inboundMessage(inboundFrameToAsk(frame())!))
    expect(text).toContain('rt_1')
    expect(text).toContain('not from the operator')
    expect(text).toContain('ship it?')
  })

  it('tells an asker how to answer, with the reply target', () => {
    const text = textOf(inboundMessage(inboundFrameToAsk(frame())!))
    expect(text).toContain('reply_to "av1.msg.1"')
  })

  it('omits the reply instruction for a tell and shows its message id', () => {
    const tell = inboundFrameToAsk(frame({ payload: { id: 'av1.msg.1', message: 'hi' } }))!
    const text = textOf(inboundMessage(tell))
    expect(text).not.toContain('reply_to')
    expect(text).toContain('av1.msg.1')
  })

  it('produces a user-role message with a fresh id', () => {
    const ask = inboundFrameToAsk(frame())!
    const first = inboundMessage(ask)
    const second = inboundMessage(ask)
    expect(first.role).toBe('user')
    expect(first.source).toEqual({ kind: 'user' })
    expect(first.id).not.toBe(second.id)
  })
})
