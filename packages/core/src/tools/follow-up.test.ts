import { afterAll, afterEach, beforeAll, describe, expect, it, mock } from 'bun:test'
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

const { createFollowUpTool } = await import('./follow-up.js')
const followUpTool = createFollowUpTool(getSupervisorClientMock)
const { Supervisor } = await import('../supervisor.js')

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

  it('omits auto-approval when live status is false', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'auto-approve-false')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-original\n')
    statusMock.mockResolvedValueOnce({ agent: 'jockey', auto_approve: false })

    await followUpTool({
      runId: 'auto-approve-false',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls.at(-1)?.[0]).not.toHaveProperty('auto_approve')
  })

  it('omits auto-approval when live status is undefined', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'auto-approve-undefined')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-original\n')
    statusMock.mockResolvedValueOnce({ agent: 'jockey' })

    await followUpTool({
      runId: 'auto-approve-undefined',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls.at(-1)?.[0]).not.toHaveProperty('auto_approve')
  })

  it('omits auto-approval when live status is a non-boolean truthy string', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'auto-approve-truthy')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-original\n')
    statusMock.mockResolvedValueOnce({ agent: 'jockey', auto_approve: 'true' })

    await followUpTool({
      runId: 'auto-approve-truthy',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls.at(-1)?.[0]).not.toHaveProperty('auto_approve')
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
      thinking: 'high',
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
      thinking: 'high',
      agent_profile: 'cloud',
      dir: '/repo/from-original-run',
    })
    expect(result.run_id).toBe('rt-followup')
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('lets live false override stale singleton auto-approval', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
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
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
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

describe('followUpTool with a local supervisor (no supervisorId)', () => {
  const localSupRuns = new Map<string, Record<string, unknown>>()
  const localSupSpawnMock = mock(async (params: Record<string, unknown>) => ({
    runId: 'local-followup-run',
    label: params.label as string,
  }))
  const localSupGetClientMock = mock(() => ({
    status: statusMock,
    spawn: spawnMock,
    close: closeMock,
  }))
  const originalSupervisorGet = Supervisor.get

  // Per-test temporary directory so sentinel/event paths are isolated from
  // the shared /tmp namespace and cannot be polluted by stale files (e.g. a
  // leftover /tmp/missing-local-sentinel.done containing SESSION=...).
  let localTmpDir = ''

  beforeAll(() => {
    Supervisor.get = mock(async () => ({
      runs: localSupRuns,
      getClient: localSupGetClientMock,
      spawn: localSupSpawnMock,
      supervisorId: '/tmp/local-supervisor.sock',
    })) as any
  })

  afterAll(() => {
    Supervisor.get = originalSupervisorGet
  })

  afterEach(() => {
    localSupSpawnMock.mockClear()
    localSupGetClientMock.mockClear()
    statusMock.mockClear()
    localSupRuns.clear()
    if (localTmpDir) {
      fs.rmSync(localTmpDir, { recursive: true, force: true })
      localTmpDir = ''
    }
  })

  it('forwards autoApprove from runInfo when live status is unavailable', async () => {
    localTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-local-'))
    const sentinelPath = path.join(localTmpDir, 'sentinel.done')
    const eventLogPath = path.join(localTmpDir, 'events.log')
    localSupRuns.set('local-auto-run', {
      runId: 'local-auto-run',
      label: 'local-auto-run',
      sentinelPath,
      eventLogPath,
      runtimeId: 'rt-local-auto',
      sessionId: 'ses-local-auto',
      agent: 'explore',
      thinking: 'max',
      autoApprove: true,
    })
    statusMock.mockRejectedValueOnce(new Error('runtime unavailable'))

    const result = await followUpTool({
      runId: 'local-auto-run',
      message: 'continue',
    })

    expect(localSupSpawnMock).toHaveBeenCalledTimes(1)
    expect(localSupSpawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'explore',
      prompt: 'continue',
      session_id: 'ses-local-auto',
      thinking: 'max',
      auto_approve: true,
    })
    expect(result.run_id).toBe('local-followup-run')
  })

  it('omits auto_approve when runInfo.autoApprove is false', async () => {
    localTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-local-'))
    const sentinelPath = path.join(localTmpDir, 'sentinel.done')
    const eventLogPath = path.join(localTmpDir, 'events.log')
    localSupRuns.set('local-supervised-run', {
      runId: 'local-supervised-run',
      label: 'local-supervised-run',
      sentinelPath,
      eventLogPath,
      runtimeId: 'rt-local-supervised',
      sessionId: 'ses-local-supervised',
      agent: 'explore',
      autoApprove: false,
    })
    statusMock.mockRejectedValueOnce(new Error('runtime unavailable'))

    await followUpTool({
      runId: 'local-supervised-run',
      message: 'continue',
    })

    expect(localSupSpawnMock.mock.calls[0]?.[0]).not.toHaveProperty('auto_approve')
  })

  it('lets live status auto_approve true override runInfo when both are present', async () => {
    localTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-local-'))
    const sentinelPath = path.join(localTmpDir, 'sentinel.done')
    const eventLogPath = path.join(localTmpDir, 'events.log')
    localSupRuns.set('local-override-run', {
      runId: 'local-override-run',
      label: 'local-override-run',
      sentinelPath,
      eventLogPath,
      runtimeId: 'rt-local-override',
      sessionId: 'ses-local-override',
      agent: 'explore',
      autoApprove: false,
    })
    statusMock.mockResolvedValueOnce({ agent: 'explore', auto_approve: true })

    await followUpTool({
      runId: 'local-override-run',
      message: 'continue',
    })

    expect(localSupSpawnMock.mock.calls[0]?.[0]).toMatchObject({
      auto_approve: true,
    })
  })
})
