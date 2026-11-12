import { afterEach, describe, expect, it, mock } from 'bun:test'
import { RpcError, type Client } from '../client.js'

const historyMock = mock<Client['history']>(async () => ({
  runtime_id: 'rt-inspect',
  latest_seq: 3,
  events: [
    { event: 'agent.message_chunk', runtime_id: 'rt-inspect', seq: 1, delta: 'hello ' },
    { event: 'agent.message_chunk', runtime_id: 'rt-inspect', seq: 2, delta: 'world' },
    { event: 'session.end', runtime_id: 'rt-inspect', seq: 3, final_output: 'hello world', stop_reason: 'end_turn' },
  ],
}))

const statusMock = mock<Client['status']>(async (runtimeId?: string) => {
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

const listMock = mock<Client['list']>(async () => ([
  {
    run_id: 'friendly-inspect',
    runtime_id: 'rt-inspect',
    label: 'friendly-inspect',
    event_path: '/tmp/events.ndjson',
    phase: 'done',
  },
]))

const closeMock = mock<Client['close']>(() => {})
const preflightMock = mock(async () => ({ protocol: 1, generation: 'gen-1' }))
const peersMock = mock(async (_params: { generation: string }): Promise<any> => ({ peers: [] }))
const getSupervisorClientMock = mock(async (_supervisorId?: string) => ({
  client: {
    history: historyMock,
    status: statusMock,
    list: listMock,
    close: closeMock,
    messagePreflight: preflightMock,
    messagePeers: peersMock,
  } as any,
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-inspect.sock',
}))

const { createStatusTool } = await import('./status.js')
const { createEventsTool } = await import('./events.js')
const { createInspectTool } = await import('./inspect.js')
const inspectTool = createInspectTool({
  status: createStatusTool(getSupervisorClientMock),
  events: createEventsTool(getSupervisorClientMock),
})

describe('inspectTool', () => {
  afterEach(() => {
    historyMock.mockClear()
    statusMock.mockClear()
    listMock.mockClear()
    closeMock.mockClear()
    preflightMock.mockReset()
    preflightMock.mockImplementation(async () => ({ protocol: 1, generation: 'gen-1' }))
    peersMock.mockReset()
    peersMock.mockImplementation(async () => ({ peers: [] }))
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

  it('rejects a broker-only run with the broker peer refusal', async () => {
    statusMock.mockRejectedValueOnce(new RpcError(-32000, 'runtime "claude-9" not found'))
    statusMock.mockRejectedValueOnce(new RpcError(-32000, 'runtime "claude-9" not found'))
    const listing = {
      peers: [{ run_id: 'claude-9', session: { run_id: 'claude-9', label: 'planner', status: 'running' } }],
    }
    peersMock.mockResolvedValueOnce(listing)
    peersMock.mockResolvedValueOnce(listing)

    await expect(inspectTool({ runId: 'claude-9', supervisorId: '/tmp/avenor-inspect.sock' }))
      .rejects.toThrow('this run is a broker peer with no event log; peers action last reads its last agent message')
    expect(historyMock).not.toHaveBeenCalled()
  })

  it('rejects an unknown run as not found instead of inspecting a stub', async () => {
    statusMock.mockRejectedValueOnce(new RpcError(-32000, 'runtime "ghost-9" not found'))

    await expect(inspectTool({ runId: 'ghost-9', supervisorId: '/tmp/avenor-inspect.sock' }))
      .rejects.toThrow(new Error('runtime "ghost-9" not found'))
    expect(historyMock).not.toHaveBeenCalled()
  })
})
