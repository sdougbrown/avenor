import { afterEach, describe, expect, it, mock } from 'bun:test'

const historyMock = mock(async () => ({
  runtime_id: 'rt-inspect',
  latest_seq: 3,
  events: [
    { event: 'agent.message_chunk', runtime_id: 'rt-inspect', seq: 1, delta: 'hello ' },
    { event: 'agent.message_chunk', runtime_id: 'rt-inspect', seq: 2, delta: 'world' },
    { event: 'session.end', runtime_id: 'rt-inspect', seq: 3, final_output: 'hello world', stop_reason: 'end_turn' },
  ],
}))

const statusMock = mock(async (runtimeId?: string) => {
  if (runtimeId === 'friendly-inspect') {
    throw new Error('label is not a runtime id')
  }
  return {
    run_id: 'friendly-inspect',
    runtime_id: 'rt-inspect',
    label: 'friendly-inspect',
    session_id: 'ses-inspect',
    phase: 'done',
    backend: 'pi',
    final_output: 'hello world',
    latest_seq: 3,
  }
})

const listMock = mock(async () => ([
  {
    run_id: 'friendly-inspect',
    runtime_id: 'rt-inspect',
    label: 'friendly-inspect',
    event_path: '/tmp/events.ndjson',
    phase: 'done',
  },
]))

const closeMock = mock(() => {})
const getSupervisorClientMock = mock(async () => ({
  client: {
    history: historyMock,
    status: statusMock,
    list: listMock,
    close: closeMock,
  },
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-inspect.sock',
}))

mock.module('./get-supervisor-client.js', () => ({
  getSupervisorClient: getSupervisorClientMock,
}))

const { inspectTool } = await import('./inspect.js')

describe('inspectTool', () => {
  afterEach(() => {
    historyMock.mockClear()
    statusMock.mockClear()
    listMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
  })

  it('returns bounded snapshot details and final output for explicit supervisors', async () => {
    const result = await inspectTool({
      runId: 'friendly-inspect',
      supervisorId: '/tmp/avenor-inspect.sock',
      limit: 10,
    })

    expect(result.run_id).toBe('friendly-inspect')
    expect(result.status.runtime_id).toBe('rt-inspect')
    expect(result.snapshot.identity.backend).toBe('pi')
    expect(result.transcript[0]?.text).toBe('hello world')
    expect(result.final_output).toBe('hello world')
    expect((result.snapshot as any)._seen_seq_by_scope).toBeUndefined()
    expect(closeMock).toHaveBeenCalled()
  })
})
