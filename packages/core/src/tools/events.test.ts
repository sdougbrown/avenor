import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

const historyMock = mock(async () => ({ events: [] }))
const statusMock = mock(async () => {
  throw new Error('status unavailable')
})
const listMock = mock(async () => [])
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
  supervisorId: '/tmp/avenor-events-test.sock',
}))

mock.module('./get-supervisor-client.js', () => ({
  getSupervisorClient: getSupervisorClientMock,
}))

const { eventsTool } = await import('./events.js')

describe('eventsTool', () => {
  let logPath = ''

  afterEach(() => {
    historyMock.mockReset()
    statusMock.mockReset()
    listMock.mockReset()
    closeMock.mockReset()
    getSupervisorClientMock.mockClear()
    if (logPath) {
      try {
        fs.unlinkSync(logPath)
      } catch {
        // ignore
      }
      logPath = ''
    }
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
