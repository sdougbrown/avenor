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
const { createSpawnTool } = await import('./spawn.js')
const { forgetExternalRuns } = await import('./run-registry.js')
const followUpTool = createFollowUpTool(getSupervisorClientMock)
const externalSpawnTool = createSpawnTool(getSupervisorClientMock)
const { Supervisor } = await import('../supervisor.js')

function singletonSupervisor(runs: Map<string, any>): any {
  const aliases = new Map([...runs.values()].map(run => [run.label, run]))
  return {
    runs,
    aliases,
    spawn: async (params: Record<string, unknown>, runId: string) => {
      const result = await spawnMock(params)
      const runInfo = {
        runId,
        label: String(params.label ?? runId),
        sentinelPath: String(params.sentinel_file ?? ''),
        eventLogPath: String(params.on_event ?? ''),
        runtimeId: result.runtime_id,
        sessionId: params.session_id as string | undefined,
      }
      runs.set(runId, runInfo)
      aliases.set(runInfo.label, runInfo)
      return runInfo
    },
  }
}

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

  it('returns a public follow-up ID with the provider runtime ID separately', async () => {
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
    expect(result.run_id).not.toBe('rt-followup')
    expect(result.runtime_id).toBe('rt-followup')
    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      sentinel_file: path.join(home, 'runs', result.run_id, 'sentinel.done'),
      on_event: path.join(home, 'runs', result.run_id, 'events.log'),
    })
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('refuses a rejected provisional session from the spawn-created artifact layout', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const spawned = await externalSpawnTool({
      label: 'conflicted external run',
      prompt: 'start',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })
    const initialSpawn = spawnMock.mock.calls[0]?.[0]
    expect(initialSpawn?.sentinel_file).toBe(
      path.join(home, 'runs', spawned.run_id, 'sentinel.done'),
    )
    fs.writeFileSync(
      String(initialSpawn?.sentinel_file),
      'FAILED\nSESSION=ses-rejected\nSTOP_REASON=session_id_conflict\nEXIT_CODE=1\n',
    )
    spawnMock.mockClear()
    statusMock.mockRejectedValueOnce(new Error('completed runtime removed'))
    forgetExternalRuns('/tmp/avenor-mcp-test.sock')

    await expect(followUpTool({
      runId: spawned.run_id,
      message: 'do not continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })).rejects.toThrow('run is not resumable: session_id_conflict')

    expect(statusMock).toHaveBeenCalledWith(spawned.runtime_id)
    expect(spawnMock).not.toHaveBeenCalled()
    expect(closeMock).toHaveBeenCalledTimes(2)
  })

  it('preserves normal FAILED sentinel resume behavior for non-conflict failures', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'ordinary-failed-run')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(
      path.join(runDir, 'sentinel.done'),
      'FAILED\nSESSION=ses-failed\nSTOP_REASON=refusal\nEXIT_CODE=2\n',
    )

    await followUpTool({
      runId: 'ordinary-failed-run',
      message: 'continue after refusal',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock).toHaveBeenCalledTimes(1)
    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      session_id: 'ses-failed',
      prompt: 'continue after refusal',
    })
  })

  it('uses resolved identity for roster follow-ups without forwarding the mutable selector', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'roster-run')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-roster\n')
    statusMock.mockResolvedValueOnce({
      session_id: 'ses-roster',
      effective_agent: 'resolved-agent',
      effective_model: 'resolved-model',
      effective_backend: 'agy',
      roster_file: '/tmp/mutable-roster.json',
      roster_entry: 'planner',
    })

    await followUpTool({
      runId: 'roster-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'resolved-agent',
      model: 'resolved-model',
      backend: 'agy',
    })
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('roster_file')
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('roster_entry')
  })

  it('supports a resolved model-only roster follow-up', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const runDir = path.join(home, 'runs', 'model-only-run')
    fs.mkdirSync(runDir, { recursive: true })
    fs.writeFileSync(path.join(runDir, 'sentinel.done'), 'DONE\nSESSION=ses-model-only\n')
    statusMock.mockResolvedValueOnce({
      session_id: 'ses-model-only',
      effective_model: 'provider/model',
      effective_backend: 'agy',
    })

    await followUpTool({
      runId: 'model-only-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      model: 'provider/model',
      backend: 'agy',
    })
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('agent')
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
    expect(result.run_id).not.toBe('rt-followup')
    expect(result.runtime_id).toBe('rt-followup')
    expect(closeMock).toHaveBeenCalledTimes(1)
  })

  it('lets live false override stale singleton auto-approval', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const sup = singletonSupervisor(new Map([['singleton-run', {
      runId: 'singleton-run',
      label: 'singleton-run',
      sentinelPath: '/tmp/missing-singleton-sentinel',
      eventLogPath: '/tmp/missing-singleton-events',
      runtimeId: 'runtime-singleton-run',
      sessionId: 'ses-from-run-map',
      agent: 'explore',
      autoApprove: true,
    }]]))
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
    const sup = singletonSupervisor(new Map([['singleton-run', {
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
    }]]))
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

  it('clears a stale run-level model for an authoritative agent-only workflow phase', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    const sup = singletonSupervisor(new Map([['agent-only-workflow', {
      runId: 'agent-only-workflow',
      label: 'agent-only-workflow',
      sentinelPath: '/tmp/missing-agent-only-sentinel',
      eventLogPath: '/tmp/missing-agent-only-events',
      runtimeId: 'rt-agent-only-workflow',
      sessionId: 'stale-session',
      agent: 'run-agent',
      model: 'stale-run-model',
      backend: 'pi',
    }]]))
    getSupervisorClientMock.mockResolvedValueOnce({
      client: { status: statusMock, spawn: spawnMock, close: closeMock },
      isSingleton: true,
      sup,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })
    statusMock.mockResolvedValueOnce({
      session_id: 'final-session',
      effective_agent: 'final-agent',
      effective_model: '',
      effective_backend: 'agy',
    })

    await followUpTool({
      runId: 'agent-only-workflow',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'final-agent',
      backend: 'agy',
      session_id: 'final-session',
    })
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('model')
  })

  it('resumes a valid fully defaulted run using only its stored session', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    statusMock.mockResolvedValueOnce({ session_id: 'ses-without-agent' })

    await followUpTool({
      runId: 'no-agent-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      prompt: 'continue',
      session_id: 'ses-without-agent',
    })
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('agent')
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('model')
  })

  it('resumes a backend-only run with no agent or model', async () => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-test-'))
    process.env.AVENOR_HOME = home
    statusMock.mockResolvedValueOnce({ session_id: 'ses-backend-only', effective_backend: 'agy' })

    await followUpTool({
      runId: 'backend-only-run',
      message: 'continue',
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(spawnMock.mock.calls[0]?.[0]).toMatchObject({
      backend: 'agy',
      session_id: 'ses-backend-only',
    })
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('agent')
    expect(spawnMock.mock.calls[0]?.[0]).not.toHaveProperty('model')
  })
})

describe('followUpTool with a local supervisor (no supervisorId)', () => {
  const localSupRuns = new Map<string, Record<string, unknown>>()
  const localSupAliases = new Map<string, Record<string, unknown>>()
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
      aliases: localSupAliases,
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
    localSupAliases.clear()
    if (localTmpDir) {
      fs.rmSync(localTmpDir, { recursive: true, force: true })
      localTmpDir = ''
    }
  })

  it('retains roster metadata when a local follow-up uses resolved direct identity', async () => {
    localTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-local-'))
    const sentinelPath = path.join(localTmpDir, 'sentinel.done')
    const eventLogPath = path.join(localTmpDir, 'events.log')
    localSupRuns.set('local-roster-run', {
      runId: 'local-roster-run',
      label: 'local-roster-run',
      sentinelPath,
      eventLogPath,
      runtimeId: 'rt-local-roster',
      sessionId: 'ses-local-roster',
      rosterFile: '/tmp/mutable-roster.json',
      rosterEntry: 'planner',
      effectiveAgent: 'resolved-agent',
      effectiveModel: 'resolved-model',
      effectiveBackend: 'agy',
      agent: 'resolved-agent',
      model: 'resolved-model',
      backend: 'agy',
    })
    statusMock.mockRejectedValueOnce(new Error('runtime unavailable'))

    await followUpTool({ runId: 'local-roster-run', message: 'continue' })

    expect(localSupSpawnMock.mock.calls[0]?.[0]).toMatchObject({
      agent: 'resolved-agent',
      model: 'resolved-model',
      backend: 'agy',
    })
    expect(localSupSpawnMock.mock.calls[0]?.[0]).not.toHaveProperty('roster_file')
    expect(localSupSpawnMock.mock.calls[0]?.[0]).not.toHaveProperty('roster_entry')
    expect(localSupSpawnMock.mock.calls[0]?.[2]).toMatchObject({
      rosterFile: '/tmp/mutable-roster.json',
      rosterEntry: 'planner',
      effectiveAgent: 'resolved-agent',
    })
  })

  it('rejects a conflicted provisional sentinel session before local spawn', async () => {
    localTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-local-'))
    const sentinelPath = path.join(localTmpDir, 'sentinel.done')
    fs.writeFileSync(
      sentinelPath,
      'FAILED\nSESSION=ses-local-rejected\nSTOP_REASON=session_id_conflict\nEXIT_CODE=1\n',
    )
    localSupRuns.set('local-conflicted-run', {
      runId: 'local-conflicted-run',
      label: 'local-conflicted-run',
      sentinelPath,
      eventLogPath: path.join(localTmpDir, 'events.log'),
      runtimeId: 'rt-local-conflicted',
      sessionId: 'ses-local-rejected',
      backend: 'pi',
    })

    await expect(followUpTool({
      runId: 'local-conflicted-run',
      message: 'do not continue',
    })).rejects.toThrow('run is not resumable: session_id_conflict')

    expect(localSupGetClientMock).not.toHaveBeenCalled()
    expect(statusMock).not.toHaveBeenCalled()
    expect(localSupSpawnMock).not.toHaveBeenCalled()
  })

  it('resumes a stored backend-only local run when live status is unavailable', async () => {
    localTmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-follow-up-local-'))
    const sentinelPath = path.join(localTmpDir, 'sentinel.done')
    localSupRuns.set('local-backend-only', {
      runId: 'local-backend-only',
      label: 'local-backend-only',
      sentinelPath,
      eventLogPath: path.join(localTmpDir, 'events.log'),
      runtimeId: 'rt-local-backend-only',
      sessionId: 'ses-local-backend-only',
      backend: 'agy',
      effectiveBackend: 'agy',
    })
    statusMock.mockRejectedValueOnce(new Error('runtime unavailable'))

    await followUpTool({ runId: 'local-backend-only', message: 'continue' })

    expect(localSupSpawnMock.mock.calls[0]?.[0]).toMatchObject({
      backend: 'agy',
      session_id: 'ses-local-backend-only',
    })
    expect(localSupSpawnMock.mock.calls[0]?.[0]).not.toHaveProperty('agent')
    expect(localSupSpawnMock.mock.calls[0]?.[0]).not.toHaveProperty('model')
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
