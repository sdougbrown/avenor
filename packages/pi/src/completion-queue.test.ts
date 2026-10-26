import { describe, expect, it } from 'bun:test'
import { CompletionQueue, type CompletionNotice } from './completion-queue.js'

function notice(key: string, text = key): CompletionNotice {
  return {
    key,
    runId: key,
    label: key,
    status: 'done',
    text,
  }
}

describe('CompletionQueue', () => {
  it('keeps one unread notice per canonical key, last write wins', () => {
    const queue = new CompletionQueue()
    queue.set(notice('supervisor\u0000r1', 'first'))
    queue.set(notice('supervisor\u0000r1', 'second'))

    expect(queue.size).toBe(1)
    expect(queue.entries()).toEqual([notice('supervisor\u0000r1', 'second')])
  })

  it('removes only the consumed notice', () => {
    const queue = new CompletionQueue()
    queue.set(notice('a\u0000r1'))
    queue.set(notice('b\u0000r1'))

    expect(queue.delete('a\u0000r1')).toBe(true)
    expect(queue.delete('a\u0000r1')).toBe(false)
    expect(queue.size).toBe(1)
    expect(queue.has('b\u0000r1')).toBe(true)
  })

  it('clears all unread notices when the session ends', () => {
    const queue = new CompletionQueue()
    queue.set(notice('a\u0000r1'))
    queue.set(notice('b\u0000r1'))

    queue.clear()

    expect(queue.size).toBe(0)
    expect(queue.entries()).toEqual([])
  })
})
