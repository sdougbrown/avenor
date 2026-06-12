import { describe, it, expect } from 'bun:test'
import { isChunkEvent, extractChunkText, Accumulator } from './accumulation.js'

describe('isChunkEvent', () => {
  it('returns true for chunk event types', () => {
    expect(isChunkEvent('agent.thought_chunk')).toBe(true)
    expect(isChunkEvent('agent.message_chunk')).toBe(true)
    expect(isChunkEvent('user.message_chunk')).toBe(true)
  })

  it('returns false for non-chunk event types', () => {
    expect(isChunkEvent('tool.call')).toBe(false)
    expect(isChunkEvent('agent.status')).toBe(false)
    expect(isChunkEvent('session.start')).toBe(false)
    expect(isChunkEvent('permission.request')).toBe(false)
  })
})

describe('extractChunkText', () => {
  it('extracts content field', () => {
    expect(extractChunkText({ content: 'hello world' })).toBe('hello world')
  })

  it('extracts text field as fallback', () => {
    expect(extractChunkText({ text: 'fallback text' })).toBe('fallback text')
  })

  it('prefers content over text', () => {
    expect(extractChunkText({ content: 'primary', text: 'secondary' })).toBe('primary')
  })

  it('returns empty string for missing content', () => {
    expect(extractChunkText({})).toBe('')
    expect(extractChunkText(null)).toBe('')
    expect(extractChunkText('just a string')).toBe('')
  })

  it('returns empty string for empty content', () => {
    expect(extractChunkText({ content: '' })).toBe('')
  })
})

describe('Accumulator', () => {
  it('returns null for initial flush', () => {
    const acc = new Accumulator()
    expect(acc.flush()).toBeNull()
  })

  it('returns null when ingesting empty chunk text', () => {
    const acc = new Accumulator()
    expect(acc.ingest('agent.thought_chunk', {}, Date.now())).toBeNull()
  })

  it('accumulates consecutive chunks of the same type', () => {
    const acc = new Accumulator()
    expect(acc.ingest('agent.thought_chunk', { content: 'hello' }, 1)).toBeNull()
    expect(acc.ingest('agent.thought_chunk', { content: ' world' }, 2)).toBeNull()
    expect(acc.ingest('agent.thought_chunk', { content: '!' }, 3)).toBeNull()
    expect(acc.isBuffered).toBe(true)

    const flushed = acc.flush()
    expect(flushed).not.toBeNull()
    expect(flushed!.type).toBe('agent.thought_chunk')
    expect(flushed!.content).toBe('hello world!')
  })

  it('flushes previous buffer when type changes', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: 'first' }, 1)
    acc.ingest('agent.thought_chunk', { content: ' second' }, 2)

    const flushed = acc.ingest('agent.message_chunk', { content: 'new type' }, 3)
    expect(flushed).not.toBeNull()
    expect(flushed!.type).toBe('agent.thought_chunk')
    expect(flushed!.content).toBe('first second')

    expect(acc.isBuffered).toBe(true)
    const next = acc.flush()
    expect(next).not.toBeNull()
    expect(next!.type).toBe('agent.message_chunk')
    expect(next!.content).toBe('new type')
  })

  it('flushes and returns on non-chunk events', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: 'accumulated' }, 1)

    const flushed = acc.ingest('tool.call', { tool_name: 'bash' }, 2)
    expect(flushed).not.toBeNull()
    expect(flushed!.type).toBe('agent.thought_chunk')
    expect(flushed!.content).toBe('accumulated')
    expect(acc.isBuffered).toBe(false)
  })

  it('returns null for non-chunk event when buffer is empty', () => {
    const acc = new Accumulator()
    expect(acc.ingest('tool.call', { tool_name: 'bash' }, 1)).toBeNull()
  })

  it('normalizes whitespace in accumulated text', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: ' hello  world ' }, 1)
    acc.ingest('agent.thought_chunk', { content: '  foo\nbar' }, 2)

    const flushed = acc.flush()
    expect(flushed!.content).toBe('hello world foo bar')
  })

  it('preserves order for multiple same-type chunk sequences', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: 'first' }, 1)
    acc.ingest('agent.thought_chunk', { content: ' second' }, 2)

    const f1 = acc.ingest('tool.call', { tool_name: 'bash' }, 3)
    expect(f1!.content).toBe('first second')

    acc.ingest('agent.thought_chunk', { content: 'third' }, 4)

    const f2 = acc.ingest('agent.status', { phase: 'idle' }, 5)
    expect(f2!.content).toBe('third')
  })

  it('resets after flush and can be reused', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: 'before' }, 1)
    acc.flush()
    expect(acc.isBuffered).toBe(false)
    acc.ingest('agent.thought_chunk', { content: 'after' }, 2)
    const flushed = acc.flush()
    expect(flushed!.content).toBe('after')
  })

  it('handles message_chunk accumulation', () => {
    const acc = new Accumulator()
    acc.ingest('agent.message_chunk', { content: 'say ' }, 1)
    acc.ingest('agent.message_chunk', { content: 'hello' }, 2)

    const flushed = acc.flush()
    expect(flushed!.type).toBe('agent.message_chunk')
    expect(flushed!.content).toBe('say hello')
  })

  it('treats different chunk types independently', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: 'think' }, 1)
    // Ingesting a different chunk type flushes the previous buffer
    const f1 = acc.ingest('agent.message_chunk', { content: 'say' }, 2)
    expect(f1!.content).toBe('think')

    // Non-chunk event flushes the current buffer
    const f2 = acc.ingest('tool.call', { tool_name: 'bash' }, 3)
    expect(f2!.content).toBe('say')
  })

  it('uses extractChunkText with text field', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { text: 'from text field' }, 1)
    acc.ingest('agent.thought_chunk', { text: ' again' }, 2)

    const flushed = acc.flush()
    expect(flushed!.content).toBe('from text field again')
  })

  it('drops whitespace-only chunks and ignores them', () => {
    const acc = new Accumulator()
    acc.ingest('agent.thought_chunk', { content: 'hello' }, 1)
    // Whitespace-only content should be dropped (extractChunkText returns '')
    acc.ingest('agent.thought_chunk', { content: '   \n  ' }, 2)
    acc.ingest('agent.thought_chunk', { content: ' world' }, 3)

    const flushed = acc.flush()
    expect(flushed!.content).toBe('hello world')
  })
})
