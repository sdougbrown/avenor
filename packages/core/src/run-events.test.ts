import { describe, expect, it } from 'bun:test'
import { extractEventText, normalizeRunEvent } from './run-events.js'

describe('extractEventText', () => {
  it('bounds recursion for hostile nested event payloads', () => {
    let nested: unknown = 'hidden too deeply'
    for (let i = 0; i < 20; i++) nested = { content: nested }
    expect(extractEventText(nested)).toBeUndefined()
  })

  it('bounds array traversal while preserving normal content arrays', () => {
    expect(extractEventText([{ text: 'hello' }, { text: ' world' }])).toBe('hello world')
    expect(extractEventText([
      ...Array.from({ length: 64 }, () => ({})),
      { text: 'outside bound' },
    ])).toBeUndefined()
  })
})

describe('normalizeRunEvent', () => {
  it('accepts legacy type payloads', () => {
    const event = normalizeRunEvent({
      type: 'agent.message_chunk',
      sessionID: 'ses-1',
      runtime_id: 'rt-1',
      seq: 3,
      content: { text: 'hello' },
    })

    expect(event).not.toBeNull()
    expect(event?.event).toBe('agent.message_chunk')
    expect(event?.session_id).toBe('ses-1')
    expect(event?.delta).toBe('hello')
  })

  it('normalizes pi message aliases', () => {
    const event = normalizeRunEvent({
      event: 'avenor.message.update',
      assistantMessageEvent: {
        type: 'thinking_delta',
        text: 'plan',
      },
    })

    expect(event?.event).toBe('agent.thought_chunk')
    expect(event?.delta).toBe('plan')
    expect(event?.alias).toBe(true)
  })

  it('normalizes pi tool aliases', () => {
    const event = normalizeRunEvent({
      event: 'avenor.tool.end',
      toolName: 'bash',
      id: 'tool-1',
      result: { text: 'ok' },
    })

    expect(event?.event).toBe('tool.call_update')
    expect(event?.tool?.id).toBe('tool-1')
    expect(event?.tool?.title).toBe('bash')
    expect(event?.tool?.status).toBe('completed')
  })

  it('preserves canonical permission and session fields', () => {
    const event = normalizeRunEvent({
      event: 'session.end',
      runtime_id: 'rt-2',
      stop_reason: 'end_turn',
      final_output: 'done',
      usage: { total_tokens: 7 },
    })

    expect(event?.event).toBe('session.end')
    expect(event?.stop_reason).toBe('end_turn')
    expect(event?.final_output).toBe('done')
    expect(event?.usage).toEqual({ total_tokens: 7 })
  })
})
