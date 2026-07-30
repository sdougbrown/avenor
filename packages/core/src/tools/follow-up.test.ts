import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

const spawnMock = mock(async () => ({ runtime_id: 'rt-followup' }))
const statusMock = mock(async () => ({ agent: 'jockey' }))
const closeMock = mock(() => {})

const getSupervisorClientMock = mock(async () => ({
  client: {
    status: statusMock,
    spawn: spawnMock,
    close: closeMock,
  },
  isSingleton: false,
  sup: null,
  supervisorId: '/tmp/avenor-mcp-test.sock',
}))

mock.module('./get-supervisor-client.js', () => ({
  getSupervisorClient: getSupervisorClientMock,
}))

const { followUpTool } = await import('./follow-up.js')

describe('followUpTool with an external supervisor', () => {
  const previousHome = process.env.AVENOR_HOME
  let home = ''

  afterEach(() => {
    spawnMock.mockClear()
    statusMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
    if (home) fs.rmSync(home, { recursive: true, force: true })
  })

  it('returns the runtime_id from the spawned follow-up (sentinel path)', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'original-run')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-original\n')

    const result = await followUpTool({
      runId: 'original-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock).toHaveBeenCalledTimes(1)
    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'jockey',
      prompt: 'continue',
      session_id: 'ses-original',
    })
    expect(result.run_id).toBe('rt-followup')
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('forwards live auto-approval for an explicit supervisor', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'auto-approved-run')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-original\n')
    statusMock.mockResolvedValueOnce({ agent: 'jockey', auto_approve: true })

    await followUpTool({
      runId: 'auto-approved-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({ auto_approve: true })
  })

  it('omits auto-approval for false, undefined, and non-boolean live status', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home

    for (const [runId, autoApprove] of [
      ['auto-approve-false', false],
      ['auto-approve-undefined', undefined],
      ['auto-approve-truthy', 'true'],
    ]) {
      const runDir = path.join(home, 'runs', runId)
      fs.mkdirSync(runDir, { recursive: true })
      fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-original\n')
      statusMock.mockResolvedValueOnce({ agent: 'jockey', auto_approve: autoApprove })

      await followUpTool({
        runId,
        message: 'continue',
        supervisorId: '/tmp/avenor-mcp-test.sock',
      })

      expect(spawnMock.mock.calls.at(-1)?.[0]).not.toHaveProperty('auto_approve')
    }
  })

  it('falls back to liveStatus.session_id when no sentinel SESSION', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'no-sentinel-run')
    fs.mkdirSync(runDir, { recursive: true })
    // No sentinel file at all

    statusMock.mockClear()
    statusMock.mockResolvedValueOnce({
      agent: 'jockey',
      session_id: 'ses-from-live',
      backend: 'pi',
      model: 'test-model',
      agent_profile: 'cloud',
      dir: '/repo/from-original-run',
    })

    const result = await followUpTool({
      runId: 'no-sentinel-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock).toHaveBeenCalledTimes(1)
    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'jockey',
      prompt: 'continue',
      session_id: 'ses-from-live',
      backend: 'pi',
      model: 'test-model',
      agent_profile: 'cloud',
      dir: '/repo/from-original-run',
    })
    expect(result.run_id).toBe('rt-followup')
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('lets live false override stale singleton auto-approval', async () => {
    const sup = {
      runs: new Map([['singleton-run', {
        runId: 'singleton-run',
        label: 'singleton-run',
        sentinelPath: '/tmp/missing-singleton-sentinel',
        eventLogPath: '/tmp/missing-singleton-events',
        runtimeId: 'runtime-singleton-run',
        sessionId: 'ses-from-run-map',
        agent: 'explore',
        autoApprove: true,
      }]]),
    }
    getSupervisorClientMock.mockResolvedValueOnce({
      client: {
        status: statusMock,
        spawn: spawnMock,
        close: closeMock,
      },
      isSingleton: true,
      sup,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })
    statusMock.mockResolvedValueOnce({
      agent: 'explore',
      session_id: 'ses-from-live',
      auto_approve: false,
    })

    await followUpTool({
      runId: 'singleton-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(statusMock).toHaveBeenCalledWith('runtime-singleton-run')
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('auto_approve')
  })

  it('uses singleton run metadata when status and sentinel are unavailable', async () => {
    const sup = {
      runs: new Map([['singleton-run', {
        runId: 'singleton-run',
        label: 'singleton-run',
        sentinelPath: '/tmp/missing-singleton-sentinel',
        eventLogPath: '/tmp/missing-singleton-events',
        sessionId: 'ses-from-run-map',
        agent: 'explore',
        agentProfile: 'cloud',
        backend: 'pi',
        model: 'stored-model',
        dir: '/repo/from-run-map',
        autoApprove: true,
      }]]),
    }
    getSupervisorClientMock.mockResolvedValueOnce({
      client: {
        status: statusMock,
        spawn: spawnMock,
        close: closeMock,
      },
      isSingleton: true,
      sup,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })
    statusMock.mockRejectedValueOnce(new Error('runtime no longer available'))

    await followUpTool({
      runId: 'singleton-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'explore',
      backend: 'pi',
      model: 'stored-model',
      agent_profile: 'cloud',
      dir: '/repo/from-run-map',
      session_id: 'ses-from-run-map',
      auto_approve: true,
    })
    expect(closeMock).not.toHaveBeenCalled()
  })

  it('does not resume with a different default agent when metadata is unavailable', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    statusMock.mockResolvedValueOnce({ session_id: 'ses-without-agent' })

    await expect(followUpTool({
      runId: 'no-agent-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })).rejects.toThrow('run has no agent to resume')

    expect(spawnMock).not.toHaveBeenCalled()
  })
})
