import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

const statusMock = mock(async () => {
  throw new Error('status call failed')
})

const closeMock = mock(() => {})

const runInfo = {
  runId: 'singleton-regression-run',
  label: 'Singleton Registry Regression',
  sentinelPath: '',
  eventLogPath: '/tmp/does-not-matter',
  runtimeId: 'runtime-id-regression',
}

const getSupervisorClientMock = mock(async () => ({
  client: {
    status: statusMock,
    close: closeMock,
  },
  isSingleton: true,
  sup: {
    runs: new Map([
      [runInfo.runId, runInfo],
    ]),
  },
  supervisorId: '/tmp/avenor-mcp-test.sock',
}))

mock.module('./get-supervisor-client.js', () => ({
  getSupervisorClient: getSupervisorClientMock,
}))

const { statusTool } = await import('./status.js')

describe('statusTool singleton registry', () => {
  let sentinelPath: string

  afterEach(() => {
    statusMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()

    if (sentinelPath) {
      try {
        fs.unlinkSync(sentinelPath)
      } catch {}
    }
  })

  it('reads terminal sentinel when singleton registry status lookup throws', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\nSESSION=ses-1234\n')
    runInfo.sentinelPath = sentinelPath

    const result = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(statusMock).toHaveBeenCalledTimes(1)
    expect(statusMock).toHaveBeenCalledWith(runInfo.runtimeId)
    expect(closeMock).not.toHaveBeenCalled()
    expect(result).toMatchObject({
      run_id: runInfo.runId,
      label: runInfo.label,
      status: 'done',
      session_id: 'ses-1234',
      stop_reason: 'DONE',
      runtime_id: runInfo.runtimeId,
    })
  })
})
