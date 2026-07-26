import { describe, expect, it } from 'bun:test'
import {
  createRunSnapshot,
  reduceRunSnapshot,
  RunReducer,
} from './run-reducer.js'

describe('RunReducer', () => {
  it('accumulates mixed backend chunks without duplicating pi aliases', () => {
    const reducer = new RunReducer()
    reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'Hel' })
    reducer.apply({ event: 'avenor.message.delta', runtime_id: 'rt-1', seq: 2, text: 'Hel' })
    reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 3, delta: 'lo' })

    const snapshot = reducer.snapshot()
    expect(snapshot.assistant_text).toBe('Hello')
    expect(snapshot.transcript.filter((entry) => entry.kind === 'assistant')).toHaveLength(1)
    expect(snapshot.latest_seq).toBe(3)
  })

  it('tracks reasoning and user chunks separately in the transcript', () => {
    const reducer = new RunReducer()
    reducer.apply({ event: 'agent.thought_chunk', runtime_id: 'rt-chunks', seq: 1, delta: 'considering' })
    reducer.apply({ event: 'user.message_chunk', runtime_id: 'rt-chunks', seq: 2, delta: 'please continue' })

    const snapshot = reducer.snapshot()
    expect(snapshot.thought_text).toBe('considering')
    expect(snapshot.user_text).toBe('please continue')
    expect(snapshot.transcript.map((entry) => [entry.kind, entry.text])).toEqual([
      ['thought', 'considering'],
      ['user', 'please continue'],
    ])
  })

  it('tracks tool lifecycle and live tool state', () => {
    const reducer = new RunReducer()
    reducer.apply({ event: 'tool.call', runtime_id: 'rt-2', seq: 1, id: 'tool-1', title: 'bash', status: 'running', input: 'echo hi' })
    reducer.apply({ event: 'tool.call_update', runtime_id: 'rt-2', seq: 2, id: 'tool-1', status: 'completed', output: 'hi' })

    const snapshot = reducer.snapshot()
    expect(snapshot.tools).toHaveLength(1)
    expect(snapshot.tools[0].status).toBe('completed')
    expect(snapshot.tools[0].live).toBe(false)
    expect(snapshot.live_tools).toHaveLength(0)
  })

  it('tracks permissions and clears pending state on response', () => {
    const reducer = new RunReducer()
    reducer.apply({
      event: 'permission.request',
      runtime_id: 'rt-3',
      seq: 1,
      request_id: 'req-1',
      question: 'allow?',
      options: [{ optionId: 'write_in', name: 'Other', kind: 'allow', requiresMessage: true }],
    })
    expect(reducer.snapshot().pending_permission?.options).toEqual([
      { option_id: 'write_in', label: 'Other', kind: 'allow', requires_message: true },
    ])
    reducer.apply({
      event: 'permission.response',
      runtime_id: 'rt-3',
      seq: 2,
      request_id: 'req-1',
      kind: 'allow_once',
    })

    const snapshot = reducer.snapshot()
    expect(snapshot.pending_permission).toBeUndefined()
    expect(snapshot.permissions).toHaveLength(1)
    expect(snapshot.permissions[0].request_id).toBe('req-1')
    expect(snapshot.phase).toBe('waiting')
  })

  it('deduplicates replay/live overlap by runtime sequence', () => {
    let snapshot = createRunSnapshot()
    snapshot = reduceRunSnapshot(snapshot, { event: 'agent.message_chunk', runtime_id: 'rt-4', seq: 10, delta: 'A' })
    snapshot = reduceRunSnapshot(snapshot, { event: 'agent.message_chunk', runtime_id: 'rt-4', seq: 10, delta: 'A' })
    snapshot = reduceRunSnapshot(snapshot, { event: 'agent.message_chunk', runtime_id: 'rt-4', seq: 11, delta: 'B' })

    expect(snapshot.assistant_text).toBe('AB')
    expect(snapshot.transcript).toHaveLength(1)
    expect(snapshot.latest_seq).toBe(11)
  })

  it('reconstructs sequence deduplication from an initial snapshot', () => {
    const initial = new RunReducer()
    initial.apply({ event: 'agent.message_chunk', runtime_id: 'rt-seed', seq: 5, delta: 'history' })

    const resumed = new RunReducer(undefined, initial.snapshot())
    resumed.apply({ event: 'agent.message_chunk', runtime_id: 'rt-seed', seq: 5, delta: 'duplicate' })
    resumed.apply({ event: 'agent.message_chunk', runtime_id: 'rt-seed', seq: 6, delta: ' live' })

    expect(resumed.snapshot().assistant_text).toBe('history live')
  })

  it('does not mutate snapshots returned by earlier apply calls', () => {
    const reducer = new RunReducer()
    const first = reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-copy', seq: 1, delta: 'first' })
    reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-copy', seq: 2, delta: ' second' })

    expect(first.assistant_text).toBe('first')
    expect(first.transcript).toHaveLength(1)
    expect(first.latest_seq).toBe(1)
  })

  it('uses bounded fallback keys for unsequenced legacy events', () => {
    let snapshot = createRunSnapshot({ legacy_keys: 8 })
    snapshot = reduceRunSnapshot(snapshot, { type: 'agent.message_chunk', runtime_id: 'rt-5', text: 'same', ts: 1 })
    snapshot = reduceRunSnapshot(snapshot, { type: 'agent.message_chunk', runtime_id: 'rt-5', text: 'same', ts: 1 })

    expect(snapshot.assistant_text).toBe('same')
    expect(snapshot.transcript).toHaveLength(1)
  })

  it('keeps retained text and transcript bounded', () => {
    const snapshot = new RunReducer({ text_chars: 5, transcript_entries: 2, tools: 1, tool_preview_chars: 4 })
    snapshot.apply({ event: 'agent.message_chunk', runtime_id: 'rt-6', seq: 1, delta: 'hello' })
    snapshot.apply({ event: 'agent.message_chunk', runtime_id: 'rt-6', seq: 2, delta: ' world' })
    snapshot.apply({ event: 'tool.call', runtime_id: 'rt-6', seq: 3, id: 'tool-a', title: 'bash', input: '123456' })
    snapshot.apply({ event: 'tool.call', runtime_id: 'rt-6', seq: 4, id: 'tool-b', title: 'read', input: 'abcdef' })
    snapshot.apply({ event: 'session.end', runtime_id: 'rt-6', seq: 5, stop_reason: 'end_turn' })

    const result = snapshot.snapshot()
    expect(result.assistant_text).toBe('world')
    expect(result.transcript.length).toBeLessThanOrEqual(2)
    expect(result.tools).toHaveLength(1)
    expect(result.tools[0].preview).toBe('cdef')
    expect(result.final_output).toBe('world')
  })

  it('captures final assistant output from session.end metadata', () => {
    const reducer = new RunReducer()
    reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-7', seq: 1, delta: 'draft' })
    reducer.apply({ event: 'session.end', runtime_id: 'rt-7', seq: 2, final_output: 'final answer', stop_reason: 'end_turn' })

    const snapshot = reducer.snapshot()
    expect(snapshot.final_output).toBe('final answer')
    expect(snapshot.ended).toBe(true)
    expect(snapshot.stop_reason).toBe('end_turn')
  })
})
