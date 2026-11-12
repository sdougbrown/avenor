import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test'
import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import type { StatusListResult, StatusResult } from './status.js'
import { RpcError, type Client } from '../client.js'
import { Supervisor } from '../supervisor.js'

const statusMock = mock<Client['status']>(async () => {
  throw new Error('status call failed')
})

const closeMock = mock<Client['close']>(() => {})

const runInfo = {
  runId: 'singleton-regression-run',
  label: 'Singleton Registry Regression',
  sentinelPath: '',
  eventLogPath: '/tmp/does-not-matter',
  runtimeId: 'runtime-id-regression',
}

const localRuns = new Map([[runInfo.runId, runInfo]])
const localAliases = new Map([[runInfo.label, runInfo]])

const getSupervisorClientMock = mock(async (_supervisorId?: string) => ({
  client: {
    status: statusMock,
    close: closeMock,
  } as any,
  isSingleton: true,
  sup: {
    runs: localRuns,
    aliases: localAliases,
    getRunByReference: (reference: string) => localRuns.get(reference) ?? localAliases.get(reference),
    getRunByRuntimeId: (runtimeId: string) => {
      for (const info of localRuns.values()) {
        if (info.runtimeId === runtimeId) return info
      }
      return undefined
    },
  } as any,
  supervisorId: '/tmp/avenor-mcp-test.sock',
}))

const { shapeStatusResult, createStatusTool } = await import('./status.js')
const { createSpawnTool } = await import('./spawn.js')
const { forgetExternalRuns } = await import('./run-registry.js')
const statusTool = createStatusTool(getSupervisorClientMock)

function singleRun(result: StatusResult | StatusListResult): StatusResult {
  if ('runs' in result) throw new Error('expected a single run status')
  return result
}

function fullStatus(): StatusResult {
  return {
    run_id: 'run-1',
    label: 'demo',
    status: 'done',
    runtime_id: 'rt-1',
    phase: 'done',
    phase_label: 'Complete',
    latest_seq: 42,
    session_id: 'ses-1',
    usage: { total_tokens: 12 },
    event_path: '/tmp/events.log',
    final_output: 'final answer',
  }
}

describe('status result views', () => {
  it('returns the complete input unchanged when view is omitted', () => {
    const input = fullStatus()
    const result = shapeStatusResult(input)

    expect(result).toBe(input)
    expect(result).toEqual(fullStatus())
  })

  it('returns the complete input unchanged for the full view', () => {
    const input = fullStatus()
    const result = shapeStatusResult(input, 'full')

    expect(result).toBe(input)
    expect(result).toEqual(fullStatus())
  })

  it('keeps lifecycle and permission fields while omitting result and diagnostic data', () => {
    expect(shapeStatusResult({
      run_id: 'run-1',
      label: 'demo',
      status: 'waiting',
      runtime_id: 'rt-1',
      phase: 'waiting',
      phase_label: 'Need approval',
      latest_seq: 42,
      pending_permission: {
        request_id: 'req-1',
        description: 'Allow edit?',
        options: [{ option_id: 'allow_once', label: 'Allow', kind: 'allow_once' }],
      },
      session_id: 'ses-1',
      usage: { total_tokens: 12 },
      event_path: '/tmp/events.log',
      final_output: 'do not include me',
    }, 'lifecycle')).toEqual({
      run_id: 'run-1',
      label: 'demo',
      status: 'waiting',
      runtime_id: 'rt-1',
      phase: 'waiting',
      phase_label: 'Need approval',
      latest_seq: 42,
      pending_permission: {
        request_id: 'req-1',
        description: 'Allow edit?',
        options: [{ option_id: 'allow_once', label: 'Allow', kind: 'allow_once' }],
      },
    })
  })
})

describe('statusTool singleton registry', () => {
  let sentinelPath: string

  afterEach(() => {
    statusMock.mockClear()
    closeMock.mockClear()
    getSupervisorClientMock.mockClear()
    delete (runInfo as any).agentProfile
    delete (runInfo as any).model
    delete (runInfo as any).effectiveModel
    delete (runInfo as any).sessionId

    if (sentinelPath) {
      try {
        fs.unlinkSync(sentinelPath)
      } catch {}
    }
  })

  it('preserves live waiting and permission with a sentinel for the same turn', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-wait-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\n')
    runInfo.sentinelPath = sentinelPath
    statusMock.mockResolvedValueOnce({ runtime_id: runInfo.runtimeId, sentinel_file: sentinelPath, phase: 'waiting', pending_permission: { request_id: 'req', description: 'approve', options: [] } })
    const result = singleRun(await statusTool({ runId: runInfo.runId, supervisorId: '/tmp/avenor-mcp-test.sock' }))
    expect(result.status).toBe('waiting')
    expect(result.pending_permission?.request_id).toBe('req')
  })

  it('isolates an earlier turn from the shared runtime and its newer output', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\nSESSION=old-session\nSTOP_REASON=end_turn\n')
    runInfo.sentinelPath = sentinelPath
    statusMock.mockResolvedValueOnce({
      runtime_id: runInfo.runtimeId, sentinel_file: '/tmp/new-turn.done',
      label: 'new turn', phase: 'waiting', final_output: 'new output',
    })
    const result = singleRun(await statusTool({ runId: runInfo.runId, supervisorId: '/tmp/avenor-mcp-test.sock' }))
    expect(result).toMatchObject({ run_id: runInfo.runId, label: runInfo.label, status: 'done', session_id: 'old-session', runtime_output_available: false })
    expect(result.final_output).toBeUndefined()
    expect(result.pending_permission).toBeUndefined()
  })

  it('keeps the registered session when the live runtime has moved to a newer turn', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-registered-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\nSTOP_REASON=end_turn\n')
    runInfo.sentinelPath = sentinelPath
    ;(runInfo as any).sessionId = 'registered-session'
    statusMock.mockResolvedValueOnce({
      runtime_id: runInfo.runtimeId,
      sentinel_file: '/tmp/newer-turn.done',
      session_id: 'newer-turn-session',
      phase: 'waiting',
      final_output: 'newer output',
    })
    const result = singleRun(await statusTool({ runId: runInfo.runId, supervisorId: '/tmp/avenor-mcp-test.sock' }))
    expect(result).toMatchObject({
      run_id: runInfo.runId,
      status: 'done',
      session_id: 'registered-session',
      runtime_output_available: false,
    })
    expect(result.final_output).toBeUndefined()
  })

  it('does not report the previous terminal result for a queued turn', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-new-${runInfo.runId}.done`)
    runInfo.sentinelPath = sentinelPath
    statusMock.mockResolvedValueOnce({ runtime_id: runInfo.runtimeId, sentinel_file: '/tmp/old-turn.done', status: 'done', final_output: 'old output' })
    const result = singleRun(await statusTool({ runId: runInfo.runId, supervisorId: '/tmp/avenor-mcp-test.sock' }))
    expect(result).toMatchObject({ status: 'running', run_id: runInfo.runId })
    expect(result.final_output).toBeUndefined()
  })

  it('reads terminal sentinel when singleton registry status lookup throws', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\nSESSION=ses-1234\nSTOP_REASON=end_turn\n')
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
      stop_reason: 'end_turn',
      runtime_id: runInfo.runtimeId,
    })
  })

  it('exposes a completed local session conflict from STOP_REASON', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(
      sentinelPath,
      'FAILED\nSESSION=ses-rejected\nSTOP_REASON=session_id_conflict\nEXIT_CODE=1\n',
    )
    runInfo.sentinelPath = sentinelPath

    const result = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(statusMock).toHaveBeenCalledWith(runInfo.runtimeId)
    expect(result).toMatchObject({
      status: 'failed',
      session_id: 'ses-rejected',
      stop_reason: 'session_id_conflict',
    })
  })

  it('preserves stored agent_profile when live status is unavailable', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\nSESSION=ses-profile\n')
    runInfo.sentinelPath = sentinelPath
    ;(runInfo as any).agentProfile = 'stored-profile'

    const result = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(result).toMatchObject({ agent_profile: 'stored-profile' })
  })

  it('uses non-empty status when phase is present but empty', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'FAILED\nSESSION=ses-failed\nSTOP_REASON=refusal\n')
    runInfo.sentinelPath = sentinelPath
    statusMock.mockResolvedValueOnce({
      runtime_id: runInfo.runtimeId,
      session_id: 'ses-failed',
      phase: '',
      status: 'ended',
    })

    const result = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(result).toMatchObject({
      status: 'failed',
      stop_reason: 'refusal',
      session_id: 'ses-failed',
    })
  })

  it('does not restore a stale fallback model when live identity is agent-only', async () => {
    ;(runInfo as any).model = 'stale-model'
    ;(runInfo as any).effectiveModel = 'stale-model'
    statusMock.mockResolvedValueOnce({
      runtime_id: runInfo.runtimeId,
      session_id: 'ses-agent-only',
      status: 'ended',
      effective_agent: 'final-agent',
      effective_model: '',
      effective_backend: 'agy',
    })

    const result = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(result).toMatchObject({ agent: 'final-agent', backend: 'agy' })
    expect((result as StatusResult).model).toBeUndefined()
    expect((result as StatusResult).effective_model).toBeUndefined()

    statusMock.mockRejectedValueOnce(new Error('completed runtime removed'))
    const fallback = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })
    expect(fallback).toMatchObject({ agent: 'final-agent', backend: 'agy' })
    expect((fallback as StatusResult).model).toBeUndefined()
  })

  it('maps richer metadata additively from live status', async () => {
    statusMock.mockResolvedValueOnce({
      run_id: 'parent-run-id-that-must-not-replace-the-tracked-run',
      runtime_id: runInfo.runtimeId,
      session_id: 'ses-live',
      phase: 'waiting',
      phase_label: 'Need approval',
      backend: 'pi',
      agent: 'horse',
      agent_profile: 'live-profile',
      model: 'qwen',
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      effective_backend: 'pi',
      effective_agent: 'horse',
      effective_model: 'qwen',
      dir: '/tmp/work',
      parent_id: 'rt-parent',
      children: ['rt-child'],
      event_path: '/tmp/work/events.ndjson',
      latest_seq: 42,
      final_output: 'final answer',
      usage: { total_tokens: 12 },
      pending_permission: true,
      permission: {
        request_id: 'req-7',
        description: 'Allow edit?',
        options: [
          { optionId: 'allow_once', label: 'Allow', kind: 'allow_once' },
          { optionId: 'write_in', name: 'Other', kind: 'allow', requiresMessage: true },
        ],
      },
    })

    const result = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })

    expect(result).toMatchObject({
      run_id: runInfo.runId,
      status: 'waiting',
      backend: 'pi',
      agent: 'horse',
      agent_profile: 'live-profile',
      model: 'qwen',
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      effective_backend: 'pi',
      effective_agent: 'horse',
      effective_model: 'qwen',
      dir: '/tmp/work',
      parent_id: 'rt-parent',
      children: ['rt-child'],
      event_path: '/tmp/work/events.ndjson',
      latest_seq: 42,
      final_output: 'final answer',
      pending_permission: {
        request_id: 'req-7',
        description: 'Allow edit?',
        options: [
          { option_id: 'allow_once', label: 'Allow', kind: 'allow_once' },
          { option_id: 'write_in', label: 'Other', kind: 'allow', requires_message: true },
        ],
      },
      usage: { total_tokens: 12 },
    })
  })
})

describe('statusTool external sentinel fallback', () => {
  it('uses the public spawn ID to expose STOP_REASON after live status disappears', async () => {
    const previousHome = process.env.AVENOR_HOME
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-status-external-'))
    process.env.AVENOR_HOME = home
    const supervisorId = `/tmp/external-supervisor-${crypto.randomUUID()}.sock`
    let spawnParams: Record<string, unknown> | undefined
    const externalSpawnMock = mock(async (params: Record<string, unknown>) => {
      spawnParams = params
      return { runtime_id: 'rt-external-conflict' }
    })
    const externalStatusMock = mock(async () => {
      throw new Error('completed runtime removed')
    })
    const externalListMock = mock(async () => {
      throw new Error('completed runtime not listed')
    })
    const externalCloseMock = mock(() => {})
    const getExternalClient = mock(async () => ({
      client: {
        spawn: externalSpawnMock,
        status: externalStatusMock,
        list: externalListMock,
        close: externalCloseMock,
      },
      isSingleton: false,
      sup: null,
      supervisorId,
    })) as any
    const externalSpawnTool = createSpawnTool(getExternalClient)
    const externalStatusTool = createStatusTool(getExternalClient)

    try {
      const spawned = await externalSpawnTool({
        label: 'external conflict',
        prompt: 'start',
        supervisorId,
      })
      expect(spawned.run_id).not.toBe(spawned.runtime_id)
      expect(spawnParams?.sentinel_file).toBe(
        path.join(home, 'runs', spawned.run_id, 'sentinel.done'),
      )
      fs.writeFileSync(
        String(spawnParams?.sentinel_file),
        'FAILED\nSESSION=ses-external-rejected\nSTOP_REASON=session_id_conflict\nEXIT_CODE=1\n',
      )
      forgetExternalRuns(supervisorId)

      const result = await externalStatusTool({
        runId: spawned.run_id,
        supervisorId,
      })

      expect(externalStatusMock).toHaveBeenCalledWith(spawned.runtime_id)
      expect(externalListMock).not.toHaveBeenCalled()
      expect(externalCloseMock).toHaveBeenCalledTimes(2)
      expect(result).toMatchObject({
        run_id: spawned.run_id,
        label: 'external conflict',
        runtime_id: spawned.runtime_id,
        status: 'failed',
        session_id: 'ses-external-rejected',
        stop_reason: 'session_id_conflict',
      })
    } finally {
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      fs.rmSync(home, { recursive: true, force: true })
    }
  })
})

describe('statusTool singleton list shape', () => {
  it('returns { runs: [], count: 0 } for an empty supervisor', async () => {
    const previousHome = process.env.AVENOR_HOME
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-status-empty-list-'))
    process.env.AVENOR_HOME = home
    const previousSupervisorGet = Supervisor.get
    const sup = Object.create(Supervisor.prototype) as Supervisor
    ;(sup as any).client = { list: mock(async () => []), isClosed: () => false }
    ;(sup as any).crashed = false
    Supervisor.get = mock(async () => sup) as typeof Supervisor.get
    try {
      const { createStatusTool: createFresh } = await import('./status.js')
      const listTool = createFresh(getSupervisorClientMock)
      const result = await listTool({})
      expect(result).toEqual({ runs: [], count: 0 })
    } finally {
      Supervisor.get = previousSupervisorGet
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      fs.rmSync(home, { recursive: true, force: true })
    }
  })
})

describe('statusTool broker peers', () => {
  const supervisorId = '/tmp/avenor-status-broker-peer.sock'
  const preflightMock = mock(async () => ({ protocol: 1, generation: 'gen-1' }))
  const peersMock = mock(async (_params: { generation: string }): Promise<any> => ({ peers: [] }))
  const peerStatusMock = mock(async (_runtimeId?: string): Promise<Record<string, unknown>> => {
    throw absentRuntime('')
  })
  const peerListMock = mock(async (): Promise<Array<Record<string, unknown>>> => [])
  const peerCloseMock = mock(() => {})
  const getPeerClient = mock(async (_supervisorId?: string) => ({
    client: peerClient,
    isSingleton: false,
    sup: null,
    supervisorId,
  })) as any
  const peerStatusTool = createStatusTool(getPeerClient)
  const peerClient = {
    status: peerStatusMock,
    list: peerListMock,
    close: peerCloseMock,
    messagePreflight: preflightMock,
    messagePeers: peersMock,
  }

  function absentRuntime(runtimeId: string) {
    return new RpcError(-32000, `runtime "${runtimeId}" not found`)
  }

  function peerRow(runId: string, session: Record<string, string>) {
    return { run_id: runId, session: { run_id: runId, ...session } }
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
    for (const m of [preflightMock, peersMock, peerStatusMock, peerListMock, peerCloseMock, getPeerClient]) m.mockReset()
    preflightMock.mockImplementation(async () => ({ protocol: 1, generation: 'gen-1' }))
    peersMock.mockImplementation(async () => ({ peers: [] }))
    peerStatusMock.mockImplementation(async (runtimeId?: string) => { throw absentRuntime(runtimeId ?? '') })
    peerListMock.mockImplementation(async () => [])
    getPeerClient.mockImplementation(async () => ({ client: peerClient, isSingleton: false, sup: null, supervisorId }))
  })

  afterEach(() => {
    forgetExternalRuns(supervisorId)
  })

  it('answers a broker-only run by id with the row identity and the translated status', async () => {
    peersMock.mockResolvedValueOnce({
      peers: [peerRow('claude-1', {
        label: 'planner', status: 'idle', backend: 'claude', model: 'opus', dir: '/work/repo', kind: 'claude-code',
      })],
    })

    const result = singleRun(await peerStatusTool({ runId: 'claude-1', supervisorId }))

    expect(peersMock).toHaveBeenCalledWith({ generation: 'gen-1' })
    expect(result).toEqual({
      run_id: 'claude-1',
      label: 'planner',
      status: 'running',
      backend: 'claude',
      model: 'opus',
      dir: '/work/repo',
      runtime_output_available: false,
    })
    expect(peerCloseMock).toHaveBeenCalledTimes(1)
  })

  it('answers a broker-only run by a label that names one peer', async () => {
    peersMock.mockResolvedValueOnce({
      peers: [peerRow('claude-1', { label: 'planner', status: 'running' }), peerRow('claude-2', { label: 'other', status: 'running' })],
    })

    const result = singleRun(await peerStatusTool({ runId: 'planner', supervisorId }))

    expect(result).toMatchObject({ run_id: 'claude-1', label: 'planner', status: 'running' })
  })

  it('marks a broker peer as having no runtime output to harvest', async () => {
    peersMock.mockResolvedValueOnce({ peers: [peerRow('claude-2', { label: 'finished', status: 'ended' })] })

    const result = singleRun(await peerStatusTool({ runId: 'claude-2', supervisorId }))

    expect(result.runtime_output_available).toBe(false)
  })

  it('reports an ended broker peer as done', async () => {
    peersMock.mockResolvedValueOnce({ peers: [peerRow('claude-2', { label: 'finished', status: 'ended' })] })

    const result = singleRun(await peerStatusTool({ runId: 'claude-2', supervisorId }))

    expect(result.status).toBe('done')
  })

  it('omits identity fields the row does not carry and labels the run by its id', async () => {
    peersMock.mockResolvedValueOnce({ peers: [peerRow('claude-3', { status: 'running' })] })

    const result = singleRun(await peerStatusTool({ runId: 'claude-3', supervisorId }))

    expect(result).toEqual({ run_id: 'claude-3', label: 'claude-3', status: 'running', runtime_output_available: false })
  })

  it('shapes a broker peer by the lifecycle view', async () => {
    peersMock.mockResolvedValueOnce({
      peers: [peerRow('claude-1', { label: 'planner', status: 'ended', backend: 'claude', model: 'opus', dir: '/work/repo' })],
    })

    const result = singleRun(await peerStatusTool({ runId: 'claude-1', supervisorId, view: 'lifecycle' }))

    expect(result).toEqual({ run_id: 'claude-1', label: 'planner', status: 'done' })
  })

  it('answers a broker-only run on the singleton path', async () => {
    peersMock.mockResolvedValueOnce({ peers: [peerRow('claude-1', { label: 'planner', status: 'ended', backend: 'claude' })] })

    const result = singleRun(await onSingleton(() => statusTool({ runId: 'claude-1' })))

    expect(result).toEqual({ run_id: 'claude-1', label: 'planner', status: 'done', backend: 'claude', runtime_output_available: false })
  })

  describe('reports an unknown run as not found', () => {
    const notFound = 'runtime "ghost-1" not found'
    const listings: Array<[string, () => void]> = [
      ['the run is on no listing', () => peersMock.mockResolvedValueOnce({ peers: [peerRow('claude-1', { label: 'planner', status: 'ended' })] })],
      ['the listing cannot be read', () => peersMock.mockRejectedValueOnce(new Error('broker offline'))],
      ['the listing is partial', () => peersMock.mockResolvedValueOnce({
        peers: [peerRow('ghost-1', { label: 'ghost-1', status: 'ended' })],
        peers_error: 'the broker could not read every peer',
      })],
    ]

    for (const [name, arrange] of listings) {
      it(`when ${name} (supervisorId path)`, async () => {
        arrange()

        await expect(peerStatusTool({ runId: 'ghost-1', supervisorId })).rejects.toThrow(new Error(notFound))
        expect(peersMock).toHaveBeenCalledTimes(1)
        expect(peerCloseMock).toHaveBeenCalledTimes(1)
      })

      it(`when ${name} (singleton path)`, async () => {
        arrange()

        await expect(onSingleton(() => statusTool({ runId: 'ghost-1' }))).rejects.toThrow(new Error(notFound))
        expect(peersMock).toHaveBeenCalledTimes(1)
      })
    }

    it('when the broker preflight fails', async () => {
      preflightMock.mockRejectedValueOnce(new Error('no broker'))

      await expect(peerStatusTool({ runId: 'ghost-1', supervisorId })).rejects.toThrow(new Error(notFound))
      expect(peersMock).not.toHaveBeenCalled()
    })

    it('with the sentence the absence gate recognizes as the supervisor\'s own', async () => {
      const { statusReportsNoRuntime } = await import('./message-provider.js')
      peersMock.mockResolvedValueOnce({ peers: [] })

      const thrown = await peerStatusTool({ runId: 'ghost-1', supervisorId }).catch((error: unknown) => error)

      expect(thrown).toBeInstanceOf(Error)
      expect(statusReportsNoRuntime(new RpcError(-32000, (thrown as Error).message), 'ghost-1')).toBe(true)
    })
  })

  describe('reports a label two peers share as ambiguous', () => {
    const twins = () => peersMock.mockResolvedValueOnce({
      peers: [peerRow('rt_a', { label: 'twin', status: 'idle' }), peerRow('rt_b', { label: 'twin', status: 'idle' })],
    })
    const ambiguity = '"twin" matches 2 runs: "rt_a", "rt_b"'

    it('on the supervisorId path', async () => {
      twins()

      await expect(peerStatusTool({ runId: 'twin', supervisorId })).rejects.toThrow(new Error(ambiguity))
      expect(peerCloseMock).toHaveBeenCalledTimes(1)
    })

    it('on the singleton path', async () => {
      twins()

      await expect(onSingleton(() => statusTool({ runId: 'twin' }))).rejects.toThrow(new Error(ambiguity))
    })
  })

  describe('does not read the broker unless the supervisor reports the run absent', () => {
    const failures: Array<[string, () => Error]> = [
      ['a transport error', () => new Error('socket closed')],
      ['a refusal', () => new RpcError(-32001, 'permission denied')],
      ['another runtime absent answer', () => new RpcError(-32000, 'runtime "someone-else" not found')],
    ]

    for (const [name, failure] of failures) {
      it(`after ${name} (supervisorId path)`, async () => {
        peerStatusMock.mockRejectedValueOnce(failure())
        peersMock.mockResolvedValue({ peers: [peerRow('claude-1', { label: 'planner', status: 'ended' })] })

        expect(await peerStatusTool({ runId: 'claude-1', supervisorId })).toMatchObject({
          run_id: 'claude-1', label: 'claude-1', status: 'running',
        })
        brokerNotRead()
      })

      it(`after ${name} (singleton path)`, async () => {
        peerStatusMock.mockRejectedValueOnce(failure())
        peersMock.mockResolvedValue({ peers: [peerRow('claude-1', { label: 'planner', status: 'ended' })] })

        expect(await onSingleton(() => statusTool({ runId: 'claude-1' }))).toMatchObject({
          run_id: 'claude-1', label: 'claude-1', status: 'running',
        })
        brokerNotRead()
      })
    }

    it('when the runtime list cannot be read', async () => {
      peerListMock.mockRejectedValueOnce(new Error('list offline'))
      peersMock.mockResolvedValue({ peers: [peerRow('claude-1', { label: 'planner', status: 'ended' })] })

      expect(await peerStatusTool({ runId: 'claude-1', supervisorId })).toMatchObject({
        run_id: 'claude-1', label: 'claude-1', status: 'running',
      })
      brokerNotRead()
    })
  })

  describe('on the singleton path, finds a live run on the runtime list', () => {
    function liveByRunId() {
      peerStatusMock.mockImplementation(async (runtimeId?: string) => {
        if (runtimeId === 'rt-9') return { runtime_id: 'rt-9', phase: 'waiting' }
        throw absentRuntime(runtimeId ?? '')
      })
      peerListMock.mockResolvedValue([{ runtime_id: 'rt-9', run_id: 'live-run', label: 'worker' }])
    }

    it('by its run id, without reading the broker', async () => {
      liveByRunId()

      const result = singleRun(await onSingleton(() => statusTool({ runId: 'live-run' })))

      expect(result).toMatchObject({ run_id: 'live-run', runtime_id: 'rt-9', status: 'waiting' })
      brokerNotRead()
    })

    it('keeping the stub when the list cannot be read', async () => {
      peerListMock.mockRejectedValueOnce(new Error('list offline'))
      peersMock.mockResolvedValue({ peers: [peerRow('live-run', { label: 'planner', status: 'ended' })] })

      expect(await onSingleton(() => statusTool({ runId: 'live-run' }))).toMatchObject({
        run_id: 'live-run', label: 'live-run', status: 'running',
      })
      brokerNotRead()
    })
  })

  it('does not read the broker for a run the supervisor knows', async () => {
    peerStatusMock.mockResolvedValueOnce({ runtime_id: 'rt-live', phase: 'waiting' })
    peersMock.mockResolvedValue({ peers: [peerRow('rt-live', { label: 'planner', status: 'ended' })] })

    const result = singleRun(await peerStatusTool({ runId: 'rt-live', supervisorId }))

    expect(result).toMatchObject({ runtime_id: 'rt-live', status: 'waiting' })
    brokerNotRead()
  })

  describe('does not read the broker for a run with a sentinel', () => {
    async function withSentinel<T>(run: () => Promise<T>): Promise<T> {
      const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-status-broker-sentinel-'))
      const previousHome = process.env.AVENOR_HOME
      process.env.AVENOR_HOME = home
      try {
        fs.mkdirSync(path.join(home, 'runs', 'sentinel-run'), { recursive: true })
        fs.writeFileSync(path.join(home, 'runs', 'sentinel-run', 'sentinel.done'), 'FAILED\nSTOP_REASON=refusal\n')
        peersMock.mockResolvedValue({ peers: [peerRow('sentinel-run', { label: 'planner', status: 'running' })] })
        return await run()
      } finally {
        if (previousHome === undefined) delete process.env.AVENOR_HOME
        else process.env.AVENOR_HOME = previousHome
        fs.rmSync(home, { recursive: true, force: true })
      }
    }

    it('on the supervisorId path', async () => {
      const result = await withSentinel(() => peerStatusTool({ runId: 'sentinel-run', supervisorId }))

      expect(result).toMatchObject({ run_id: 'sentinel-run', status: 'failed', stop_reason: 'refusal' })
      brokerNotRead()
    })

    it('on the singleton path', async () => {
      const result = await withSentinel(() => onSingleton(() => statusTool({ runId: 'sentinel-run' })))

      expect(result).toMatchObject({ run_id: 'sentinel-run', status: 'failed', stop_reason: 'refusal' })
      brokerNotRead()
    })
  })
})
