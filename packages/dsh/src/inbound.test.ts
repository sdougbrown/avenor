import { describe, it, expect } from 'bun:test'
import { inboundMessage } from './inbound.js'
import type { InboundAsk } from '@dougbots/avenor-core'

function ask(overrides: Partial<InboundAsk> = {}): InboundAsk {
  return {
    from_run_id: 'run-1',
    message_id: 'msg-1',
    message: 'ship it?',
    expects_reply: true,
    ...overrides,
  }
}

const textOf = (message: ReturnType<typeof inboundMessage>) =>
  (message.content[0] as { text: string }).text

describe('inbound message', () => {
  it('names the sender and frames the text as a peer, not the operator', () => {
    const text = textOf(inboundMessage(ask()))
    expect(text).toContain('run-1')
    expect(text).toContain('not from the operator')
    expect(text).toContain('ship it?')
  })

  it('tells an asker how to answer, with the reply target', () => {
    const text = textOf(inboundMessage(ask()))
    expect(text).toContain('reply_to "msg-1"')
  })

  it('omits the reply instruction for a tell and shows its message id', () => {
    const text = textOf(inboundMessage(ask({ expects_reply: false })))
    expect(text).not.toContain('reply_to')
    expect(text).toContain('msg-1')
  })

  it('produces a user-role message with a fresh id', () => {
    const first = inboundMessage(ask())
    const second = inboundMessage(ask())
    expect(first.role).toBe('user')
    expect(first.source).toEqual({ kind: 'user' })
    expect(first.id).not.toBe(second.id)
  })
})
