import { afterAll, afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

// The run registry resolves its storage root from AVENOR_HOME at import time,
// so the temp home must exist before the tool module is loaded below.
const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-kill-test-'))
process.env.AVENOR_HOME = home

const cancelMock = mock(async () => {})
const closeMock = mock(() => {})

function makeClientMock(overrides: Record<string, unknown> = {}) {
  return mock(async () => ({
    client: { cancel: cancelMock, close: closeMock },
    isSingleton: false,
    sup: null,
    supervisorId: '/tmp/avenor-kill-test.sock',
    ...overrides,
  }))
}

const getSupervisorClientMock = makeClientMock()

const { createKillTool } = await import('./kill.js')
const { registerExternalRun, forgetExternalRuns } = await import('./run-registry.js')
const killTool = createKillTool(getSupervisorClientMock)

describe('killTool', () => {
  afterEach(() => {
    cancelMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
    forgetExternalRuns('/tmp/avenor-kill-test.sock')
  })

  afterAll(() => {
    fs.rmSync(home, { recursive: true, force: true })
  })

  it('cancels by run id and returns { killed: true }', async () => {
    const res = await killTool({ runId: 'rt_1', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(cancelMock).toHaveBeenCalledWith('rt_1')
    expect(res).toEqual({ killed: true, run_id: 'rt_1' })
  })

  it('resolves a registered run id to its runtime id before cancelling', async () => {
    fs.mkdirSync(path.join(home, 'runs', 'run-abc'), { recursive: true })
    registerExternalRun({
      runId: 'run-abc',
      label: 'worker',
      supervisorId: '/tmp/avenor-kill-test.sock',
      sentinelPath: '/tmp/does-not-matter/sentinel.done',
      eventLogPath: '/tmp/does-not-matter/events.ndjson',
      runtimeId: 'rt_7',
    })
    const res = await killTool({ runId: 'run-abc', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(cancelMock).toHaveBeenCalledWith('rt_7')
    expect(res).toEqual({ killed: true, run_id: 'run-abc', runtime_id: 'rt_7' })
  })

  it('propagates errors from cancel', async () => {
    cancelMock.mockRejectedValueOnce(new Error('runtime "rt_x" not found'))
    await expect(
      killTool({ runId: 'rt_x', supervisorId: '/tmp/avenor-kill-test.sock' }),
    ).rejects.toThrow('runtime "rt_x" not found')
  })

  it('closes a non-singleton client in the finally block on success', async () => {
    await killTool({ runId: 'rt_1', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(closeMock).toHaveBeenCalled()
  })

  it('closes a non-singleton client in the finally block on error', async () => {
    cancelMock.mockRejectedValueOnce(new Error('boom'))
    await expect(
      killTool({ runId: 'rt_1', supervisorId: '/tmp/avenor-kill-test.sock' }),
    ).rejects.toThrow('boom')
    expect(closeMock).toHaveBeenCalled()
  })
})
