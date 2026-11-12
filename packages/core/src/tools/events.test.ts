import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { RpcError, type Client } from '../client.js'

const historyMock = mock<Client['history']>(async () => ({ events: [] }))
const statusMock = mock<Client['status']>(async () => {
  throw new Error('status unavailable')
})
const listMock = mock<Client['list']>(async () => [])
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
  supervisorId: '/tmp/avenor-events-test.sock',
}))

const { registerExternalRun, forgetExternalRuns, externalRunMetadataPath } = await import('./run-registry.js')
const { createEventsTool } = await import('./events.js')
const { Supervisor } = await import('../supervisor.js')
const eventsTool = createEventsTool(getSupervisorClientMock)

describe('eventsTool', () => {
  let logPath = ''

  afterEach(() => {
    historyMock.mockReset()
    statusMock.mockReset()
    listMock.mockReset()
    closeMock.mockReset()
    getSupervisorClientMock.mockClear()
    forgetExternalRuns('/tmp/avenor-events-test.sock')
    if (logPath) {
      try {
        fs.unlinkSync(logPath)
      } catch {
        // ignore
      }
      logPath = ''
    }
  })

  it('rejects traversal before acquiring an external supervisor client', async () => {
    await expect(eventsTool({
      runId: '../other-run',
      supervisorId: '/tmp/avenor-events-test.sock',
    })).rejects.toThrow('invalid run_id')

    expect(getSupervisorClientMock).not.toHaveBeenCalled()
  })

  it('uses control-plane history for explicit supervisors', async () => {
    statusMock.mockResolvedValueOnce({
      runtime_id: 'rt-events-test',
      event_path: '/tmp/unused.ndjson',
    })
    historyMock.mockResolvedValueOnce({
      runtime_id: 'rt-events-test',
      latest_seq: 4,
      events: [
        { event: 'agent.message_chunk', seq: 2, delta: 'A' },
        { event: 'session.end', seq: 4, stop_reason: 'end_turn' },
      ],
    })

    const result = await eventsTool({
      runId: 'rt-events-test',
      supervisorId: '/tmp/avenor-events-test.sock',
      limit: 10,
      after_seq: 1,
    })

    expect(getSupervisorClientMock).toHaveBeenCalled()
    expect(historyMock).toHaveBeenCalledWith({
      runtime_id: 'rt-events-test',
      limit: 10,
      after_seq: 1,
    })
    expect(closeMock).toHaveBeenCalledTimes(1)
    expect(result.events.map((event) => event.event)).toEqual([
      'agent.message_chunk',
      'session.end',
    ])
  })

  it('prefers the durable log when matching events have rotated out of live history', async () => {
    logPath = path.join(os.tmpdir(), `avenor-events-durable-${process.pid}-${Date.now()}.ndjson`)
    fs.writeFileSync(
      logPath,
      [
        JSON.stringify({ event: 'agent.message_chunk', seq: 1, delta: 'older answer' }),
        JSON.stringify({ event: 'tool.call', seq: 300, title: 'recent tool' }),
      ].join('\n') + '\n',
    )
    statusMock.mockResolvedValueOnce({
      runtime_id: 'rt-events-test',
      event_path: logPath,
    })
    historyMock.mockResolvedValueOnce({
      runtime_id: 'rt-events-test',
      latest_seq: 300,
      events: [],
    })

    const result = await eventsTool({
      runId: 'rt-events-test',
      supervisorId: '/tmp/avenor-events-test.sock',
      types: ['agent.message_chunk'],
      limit: 10,
    })

    expect(result.events).toEqual([
      expect.objectContaining({ event: 'agent.message_chunk', seq: 1 }),
    ])
    expect(historyMock).not.toHaveBeenCalled()
  })

  it('reports lookup failures when status and list fallbacks both fail', async () => {
    statusMock.mockRejectedValueOnce(new Error('status offline'))
    listMock.mockRejectedValueOnce(new Error('list offline'))

    await expect(eventsTool({
      runId: 'missing-run',
      supervisorId: '/tmp/avenor-events-test.sock',
    })).rejects.toThrow('unable to resolve run missing-run')
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('does not use shared runtime history when a registered turn log is missing', async () => {
    const runId = `events-old-turn-${crypto.randomUUID()}`
    const metadataDir = path.dirname(externalRunMetadataPath(runId))
    fs.mkdirSync(metadataDir, { recursive: true })
    try {
      registerExternalRun({
        runId, label: 'worker', supervisorId: '/tmp/avenor-events-test.sock',
        sentinelPath: '/missing/old.done', eventLogPath: '/missing/old.ndjson', runtimeId: 'rt-shared',
      })
      historyMock.mockResolvedValueOnce({ events: [{ event: 'session.end', text: 'new turn' }] })

      await expect(eventsTool({ runId, supervisorId: '/tmp/avenor-events-test.sock' }))
        .rejects.toThrow(`unable to read events for run ${runId}`)
      expect(historyMock).not.toHaveBeenCalled()
    } finally {
      fs.rmSync(metadataDir, { recursive: true, force: true })
    }
  })

  it('reports event-source failures after exhausting durable and live history', async () => {
    statusMock.mockResolvedValueOnce({
      runtime_id: 'rt-events-test',
      event_path: '/path/that/does/not/exist/events.ndjson',
    })
    historyMock.mockRejectedValueOnce(new Error('history offline'))

    await expect(eventsTool({
      runId: 'rt-events-test',
      supervisorId: '/tmp/avenor-events-test.sock',
    })).rejects.toThrow('unable to read events for run rt-events-test')
    expect(historyMock).toHaveBeenCalledTimes(1)
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('reads complete ndjson lines on file fallback and honors filters', async () => {
    logPath = path.join(os.tmpdir(), `avenor-events-${process.pid}-${Date.now()}.ndjson`)
    statusMock.mockRejectedValueOnce(new Error('status unavailable'))
    listMock.mockResolvedValueOnce([
      {
        runtime_id: 'rt-events-test',
        label: 'friendly-label',
        event_path: logPath,
      },
    ])
    historyMock.mockRejectedValueOnce(new Error('history unavailable'))

    const giantPrefix = 'x'.repeat(128 * 1024)
    fs.writeFileSync(
      logPath,
      [
        JSON.stringify({ type: 'agent.message_chunk', seq: 1, text: 'ignore' }),
        JSON.stringify({ type: 'tool.call', seq: 2, output: giantPrefix + 'tail-output' }),
        JSON.stringify({ event: 'session.end', seq: 3, stop_reason: 'end_turn' }),
      ].join('\n') + '\n',
    )

    const result = await eventsTool({
      runId: 'friendly-label',
      supervisorId: '/tmp/avenor-events-test.sock',
      types: ['tool.call', 'session.end'],
      limit: 2,
      after_seq: 1,
    })

    expect(result.events).toHaveLength(2)
    expect(result.events[0].type).toBe('tool.call')
    expect(String(result.events[0].output)).toEndWith('tail-output')
    expect(String(result.events[0].output).length).toBeLessThanOrEqual(4000)
    expect(result.events[1].event).toBe('session.end')
    expect(closeMock).toHaveBeenCalled()
  })
})

describe('eventsTool broker peers', () => {
  const supervisorId = '/tmp/avenor-events-test.sock'
  const refusal = 'this run is a broker peer with no event log; peers action last reads its last agent message'
  const peerClient = {
    status: statusMock,
    list: listMock,
    history: historyMock,
    close: closeMock,
    messagePreflight: preflightMock,
    messagePeers: peersMock,
  }

  function absentRuntime(runtimeId: string) {
    return new RpcError(-32000, `runtime "${runtimeId}" not found`)
  }

  function peerRow(runId: string, session: Record<string, string>) {
    return { run_id: runId, session: { run_id: runId, ...session } }
  }

  function listed(...rows: Array<ReturnType<typeof peerRow>>) {
    peersMock.mockResolvedValue({ peers: rows })
  }

  function brokerNotRead() {
    expect(preflightMock).not.toHaveBeenCalled()
    expect(peersMock).not.toHaveBeenCalled()
  }

  async function onSingleton<T>(run: () => Promise<T>): Promise<T> {
    const previousSupervisorGet = Supervisor.get
    Supervisor.get = mock(async () => ({
      getClient: () => peerClient,
      getRunByReference: () => undefined,
    })) as unknown as typeof Supervisor.get
    try {
      return await run()
    } finally {
      Supervisor.get = previousSupervisorGet
    }
  }

  beforeEach(() => {
    for (const m of [historyMock, statusMock, listMock, closeMock, preflightMock, peersMock]) m.mockReset()
    preflightMock.mockImplementation(async () => ({ protocol: 1, generation: 'gen-1' }))
    peersMock.mockImplementation(async () => ({ peers: [] }))
    statusMock.mockImplementation(async (runtimeId?: string) => { throw absentRuntime(runtimeId ?? '') })
    listMock.mockImplementation(async () => [])
    historyMock.mockImplementation(async () => ({ events: [] }))
  })

  it('refuses a broker-only run when the runtime list does not hold it', async () => {
    listed(peerRow('claude-1', { label: 'planner', status: 'running' }))

    await expect(eventsTool({ runId: 'claude-1', supervisorId })).rejects.toThrow(new Error(refusal))
    expect(peersMock).toHaveBeenCalledWith({ generation: 'gen-1' })
    expect(historyMock).not.toHaveBeenCalled()
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('refuses a broker-only run by a label that names one peer', async () => {
    listed(peerRow('claude-1', { label: 'planner', status: 'running' }))

    await expect(eventsTool({ runId: 'planner', supervisorId })).rejects.toThrow(new Error(refusal))
  })

  it('refuses a broker-only run on the singleton path', async () => {
    listed(peerRow('claude-1', { label: 'planner', status: 'running' }))

    await expect(onSingleton(() => eventsTool({ runId: 'claude-1' }))).rejects.toThrow(new Error(refusal))
  })

  it('keeps the aggregate error when the runtime list cannot be read', async () => {
    listMock.mockRejectedValueOnce(new Error('list offline'))
    listed(peerRow('claude-1', { label: 'planner', status: 'running' }))

    await expect(eventsTool({ runId: 'claude-1', supervisorId })).rejects.toThrow('unable to resolve run claude-1')
    brokerNotRead()
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  describe('reports a label two peers share as ambiguous', () => {
    const twins = () => listed(
      peerRow('rt_a', { label: 'twin', status: 'running' }),
      peerRow('rt_b', { label: 'twin', status: 'running' }),
    )
    const ambiguity = '"twin" matches 2 runs: "rt_a", "rt_b"'

    it('on the supervisorId path', async () => {
      twins()

      await expect(eventsTool({ runId: 'twin', supervisorId })).rejects.toThrow(new Error(ambiguity))
      expect(historyMock).not.toHaveBeenCalled()
      expect(closeMock).toHaveBeenCalledTimes(1)
    })

    it('on the singleton path', async () => {
      twins()

      await expect(onSingleton(() => eventsTool({ runId: 'twin' }))).rejects.toThrow(new Error(ambiguity))
    })
  })

  describe('keeps the existing error for an unknown run', () => {
    const existing = 'unable to resolve run ghost-1: rpc error [-32000]: runtime "ghost-1" not found'
    const listings: Array<[string, () => void]> = [
      ['the run is on no listing', () => listed(peerRow('claude-1', { label: 'planner', status: 'running' }))],
      ['the listing is empty', () => listed()],
      ['the listing cannot be read', () => peersMock.mockRejectedValue(new Error('broker offline'))],
      ['the listing is partial', () => peersMock.mockResolvedValue({
        peers: [peerRow('ghost-1', { label: 'ghost-1', status: 'running' })],
        peers_error: 'the broker could not read every peer',
      })],
    ]

    for (const [name, arrange] of listings) {
      it(`when ${name} (supervisorId path)`, async () => {
        arrange()

        await expect(eventsTool({ runId: 'ghost-1', supervisorId })).rejects.toThrow(existing)
        expect(peersMock).toHaveBeenCalledTimes(1)
        expect(closeMock).toHaveBeenCalledTimes(1)
      })

      it(`when ${name} (singleton path)`, async () => {
        arrange()

        await expect(onSingleton(() => eventsTool({ runId: 'ghost-1' }))).rejects.toThrow(existing)
        expect(peersMock).toHaveBeenCalledTimes(1)
      })
    }
  })

  describe('does not read the broker unless the supervisor reports the run absent', () => {
    const failures: Array<[string, () => Error, string]> = [
      ['a transport error', () => new Error('socket closed'), 'socket closed'],
      ['a refusal', () => new RpcError(-32001, 'permission denied'), 'permission denied'],
      ['another runtime absent answer', () => new RpcError(-32000, 'runtime "someone-else" not found'), 'someone-else'],
    ]

    for (const [name, failure, detail] of failures) {
      it(`after ${name} (supervisorId path)`, async () => {
        statusMock.mockRejectedValueOnce(failure())
        listed(peerRow('claude-1', { label: 'planner', status: 'running' }))

        await expect(eventsTool({ runId: 'claude-1', supervisorId })).rejects.toThrow(detail)
        brokerNotRead()
        expect(closeMock).toHaveBeenCalledTimes(1)
      })

      it(`after ${name} (singleton path)`, async () => {
        statusMock.mockRejectedValueOnce(failure())
        listed(peerRow('claude-1', { label: 'planner', status: 'running' }))

        await expect(onSingleton(() => eventsTool({ runId: 'claude-1' })))
          .rejects.toThrow(`unable to resolve run claude-1`)
        brokerNotRead()
      })
    }
  })

  it('does not read the broker for a run the supervisor knows', async () => {
    statusMock.mockResolvedValueOnce({ runtime_id: 'rt-known' })
    historyMock.mockResolvedValueOnce({ events: [{ event: 'session.end', seq: 1 }] })
    listed(peerRow('rt-known', { label: 'planner', status: 'running' }))

    const result = await eventsTool({ runId: 'rt-known', supervisorId })

    expect(result.events).toHaveLength(1)
    brokerNotRead()
  })
})
