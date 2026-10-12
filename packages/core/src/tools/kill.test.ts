import { afterAll, afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

// The run registry resolves its storage root from AVENOR_HOME on each call,
// so the temp home must exist before the tool module is loaded below. The
// previous value is restored in afterAll: bun test shares the process env
// across test files, and a dangling AVENOR_HOME breaks later socket tests.
const previousHome = process.env.AVENOR_HOME
const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-kill-test-'))
process.env.AVENOR_HOME = home

const cancelMock = mock(async () => {})
const closeMock = mock(() => {})
const listMock = mock(async () => [] as Array<Record<string, unknown>>)
const client = { cancel: cancelMock, close: closeMock, list: listMock }

// The tool routes both supervisor modes through getSupervisorClient, so one
// injectable mock covers the external path (isSingleton: false) and the
// singleton path (isSingleton: true, sup set) without module mocking.
function setMode(mode: 'external' | 'singleton', runs: Map<string, { runtimeId: string }>) {
  getSupervisorClientMock.mockImplementation(async () => ({
    client,
    isSingleton: mode === 'singleton',
    sup: mode === 'singleton'
      ? {
          supervisorId: '/tmp/avenor-singleton.sock',
          getClient: () => client,
          getRunByReference: (reference: string) => runs.get(reference),
        }
      : null,
    supervisorId: '/tmp/avenor-kill-test.sock',
  }))
}

const getSupervisorClientMock = mock(async () => ({
  client,
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-kill-test.sock',
}))

const { createKillTool } = await import('./kill.js')
const { registerExternalRun, forgetExternalRuns } = await import('./run-registry.js')
const killTool = createKillTool(getSupervisorClientMock)

const singletonRuns = new Map<string, { runtimeId: string }>()

describe('killTool', () => {
  afterEach(() => {
    cancelMock.mockClear()
    closeMock.mockClear()
    listMock.mockClear()
    listMock.mockImplementation(async () => [])
    getSupervisorClientMock.mockClear()
    getSupervisorClientMock.mockImplementation(async () => ({
      client,
      isSingleton: false,
      sup: null,
      supervisorId: '/tmp/avenor-kill-test.sock',
    }))
    singletonRuns.clear()
    forgetExternalRuns('/tmp/avenor-kill-test.sock')
  })

  afterAll(() => {
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
    fs.rmSync(home, { recursive: true, force: true })
  })

  it('cancels by runtime id via the live list when the registry has no entry', async () => {
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_1', label: 'worker' }])
    const res = await killTool({ runId: 'rt_1', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(cancelMock).toHaveBeenCalledWith('rt_1')
    expect(res).toEqual({ killed: true, run_id: 'rt_1', runtime_id: 'rt_1' })
  })

  it('resolves a label through the live list', async () => {
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_2', label: 'verify-kill' }])
    const res = await killTool({ runId: 'verify-kill', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(cancelMock).toHaveBeenCalledWith('rt_2')
    expect(res).toEqual({ killed: true, run_id: 'verify-kill', runtime_id: 'rt_2' })
  })

  it('resolves a run id through the live list session_id fallback', async () => {
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_3', session_id: 'ses-1' }])
    const res = await killTool({ runId: 'ses-1', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(cancelMock).toHaveBeenCalledWith('rt_3')
    expect(res).toEqual({ killed: true, run_id: 'ses-1', runtime_id: 'rt_3' })
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

  it('falls back to the live list when a registered run has no runtime id', async () => {
    fs.mkdirSync(path.join(home, 'runs', 'run-no-rt'), { recursive: true })
    registerExternalRun({
      runId: 'run-no-rt',
      label: 'stale-worker',
      supervisorId: '/tmp/avenor-kill-test.sock',
      sentinelPath: '/tmp/does-not-matter/sentinel.done',
      eventLogPath: '/tmp/does-not-matter/events.ndjson',
      runtimeId: undefined,
    })
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_8', run_id: 'run-no-rt' }])
    const res = await killTool({ runId: 'run-no-rt', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(cancelMock).toHaveBeenCalledWith('rt_8')
    expect(res).toEqual({ killed: true, run_id: 'run-no-rt', runtime_id: 'rt_8' })
  })

  it('errors on an unresolved run instead of sending the raw reference', async () => {
    await expect(
      killTool({ runId: 'ghost-run', supervisorId: '/tmp/avenor-kill-test.sock' }),
    ).rejects.toThrow('run not found: ghost-run')
    expect(cancelMock).not.toHaveBeenCalled()
  })

  it('rejects an empty run id before contacting the supervisor', async () => {
    await expect(
      killTool({ runId: '', supervisorId: '/tmp/avenor-kill-test.sock' }),
    ).rejects.toThrow('invalid run_id')
    expect(cancelMock).not.toHaveBeenCalled()
    expect(listMock).not.toHaveBeenCalled()
  })

  it('cancels on the singleton path without a supervisorId', async () => {
    setMode('singleton', singletonRuns)
    singletonRuns.set('run-single', { runtimeId: 'rt_9' })
    const res = await killTool({ runId: 'run-single' })
    expect(cancelMock).toHaveBeenCalledWith('rt_9')
    expect(res).toEqual({ killed: true, run_id: 'run-single', runtime_id: 'rt_9' })
  })

  it('keeps the singleton client open and falls back to the list for unknown references', async () => {
    setMode('singleton', singletonRuns)
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_4', label: 'live' }])
    const res = await killTool({ runId: 'live' })
    expect(cancelMock).toHaveBeenCalledWith('rt_4')
    expect(res).toEqual({ killed: true, run_id: 'live', runtime_id: 'rt_4' })
    expect(closeMock).not.toHaveBeenCalled()
  })

  it('validates the run id on the singleton path too', async () => {
    setMode('singleton', singletonRuns)
    await expect(killTool({ runId: 'not safe!' })).rejects.toThrow('invalid run_id')
    expect(cancelMock).not.toHaveBeenCalled()
  })

  it('propagates errors from cancel', async () => {
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_x' }])
    cancelMock.mockRejectedValueOnce(new Error('runtime "rt_x" not found'))
    await expect(
      killTool({ runId: 'rt_x', supervisorId: '/tmp/avenor-kill-test.sock' }),
    ).rejects.toThrow('runtime "rt_x" not found')
  })

  it('closes a non-singleton client in the finally block on success', async () => {
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_1' }])
    await killTool({ runId: 'rt_1', supervisorId: '/tmp/avenor-kill-test.sock' })
    expect(closeMock).toHaveBeenCalled()
  })

  it('closes a non-singleton client in the finally block on error', async () => {
    listMock.mockImplementation(async () => [{ runtime_id: 'rt_1' }])
    cancelMock.mockRejectedValueOnce(new Error('boom'))
    await expect(
      killTool({ runId: 'rt_1', supervisorId: '/tmp/avenor-kill-test.sock' }),
    ).rejects.toThrow('boom')
    expect(closeMock).toHaveBeenCalled()
  })
})
