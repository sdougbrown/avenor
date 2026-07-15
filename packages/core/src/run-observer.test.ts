import { describe, expect, it, mock } from 'bun:test'
import { observeRun } from './run-observer.js'

function delayedIterable(events: unknown[]) {
  let index = 0
  const returnMock = mock(async () => ({ value: undefined, done: true as const }))
  return {
    iterator: {
      async next() {
        if (index >= events.length) {
          await new Promise((resolve) => setTimeout(resolve, 5))
          return { value: undefined, done: true as const }
        }
        const value = events[index++]
        await new Promise((resolve) => setTimeout(resolve, 1))
        return { value, done: false as const }
      },
      return: returnMock,
      [Symbol.asyncIterator]() {
        return this
      },
    },
    returnMock,
  }
}

describe('observeRun', () => {
  it('observes replay+live events in order and filters by runtime', async () => {
    const { iterator } = delayedIterable([
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'A' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'A' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 3, delta: 'B' },
      { event: 'session.end', runtime_id: 'rt-1', seq: 4, stop_reason: 'end_turn' },
    ])

    const eventsMock = mock(() => iterator)
    const observer = observeRun({ events: eventsMock }, { runtime_id: 'rt-1', after_seq: 1, limit: 8 })
    const final = await observer.next((snapshot) => snapshot.ended)

    expect(eventsMock).toHaveBeenCalledWith({ runtime_id: 'rt-1', replay: true, limit: 8, after_seq: 1 })
    expect(final?.assistant_text).toBe('AB')
    expect(final?.latest_seq).toBe(4)
    await observer.close()
  })

  it('keeps an inspected transcript while appending events after its latest sequence', async () => {
    const { iterator } = delayedIterable([
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 6, delta: ' live' },
      { event: 'session.end', runtime_id: 'rt-1', seq: 7, stop_reason: 'end_turn' },
    ])
    const observer = observeRun(
      { events: mock(() => iterator) },
      {
        runtime_id: 'rt-1',
        after_seq: 5,
        initial_snapshot: {
          identity: { runtime_id: 'rt-1', run_id: 'run-1' },
          latest_seq: 5,
          assistant_text: 'history',
          transcript: [{
            kind: 'assistant',
            event: 'agent.message_chunk',
            source_event: 'agent.message_chunk',
            text: 'history',
            seq: 5,
          }],
        },
      },
    )

    const final = await observer.next((snapshot) => snapshot.ended)
    expect(final?.assistant_text).toBe('history live')
    expect(final?.transcript[0]?.text).toStartWith('history')
    expect(final?.identity.run_id).toBe('run-1')
    await observer.close()
  })

  it('preserves explicit replay behavior without a runtime filter', async () => {
    const { iterator } = delayedIterable([])
    const eventsMock = mock(() => iterator)

    const observer = observeRun({ events: eventsMock }, { replay: true })
    await observer.close()

    expect(eventsMock).toHaveBeenCalledWith({ replay: true })
  })

  it('defaults replay to live when no runtime filter is supplied', async () => {
    const { iterator } = delayedIterable([])
    const eventsMock = mock(() => iterator)

    const observer = observeRun({ events: eventsMock })
    await observer.close()

    expect(eventsMock).toHaveBeenCalledWith({})
  })

  it('notifies subscribers of snapshots', async () => {
    const { iterator } = delayedIterable([
      { event: 'tool.call', runtime_id: 'rt-2', seq: 1, id: 'tool-1', title: 'read' },
      { event: 'tool.call_update', runtime_id: 'rt-2', seq: 2, id: 'tool-1', status: 'completed' },
    ])

    const snapshots: string[] = []
    const observer = observeRun({ events: mock(() => iterator) })
    const unsubscribe = observer.subscribe((snapshot) => {
      snapshots.push(snapshot.last_event ?? '')
    })

    await observer.next((snapshot) => snapshot.live_tools.length === 0 && snapshot.tools.length === 1)
    unsubscribe()
    await observer.close()

    expect(snapshots).toEqual(['tool.call', 'tool.call_update'])
  })

  it('closes its client after the event stream fails', async () => {
    const close = mock(() => {})
    const observer = observeRun({
      events: mock(() => ({
        async next() {
          throw new Error('stream failed')
        },
        [Symbol.asyncIterator]() {
          return this
        },
      })),
      close,
    }, { close_client: true })

    await expect(observer.next()).rejects.toThrow('stream failed')
    await observer.close()
    await observer.close()
    expect(close).toHaveBeenCalledTimes(1)
  })

  it('cleans up the underlying iterator on close', async () => {
    const { iterator, returnMock } = delayedIterable([])
    const observer = observeRun({ events: mock(() => iterator) })
    await observer.close()
    expect(returnMock).toHaveBeenCalledTimes(1)
  })
})
