import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import type { StatusResult } from './status.js'

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
    aliases: new Map([
      [runInfo.label, runInfo],
    ]),
  },
  supervisorId: '/tmp/avenor-mcp-test.sock',
}))

const { shapeStatusResult, createStatusTool } = await import('./status.js')
const { createSpawnTool } = await import('./spawn.js')
const { forgetExternalRuns } = await import('./run-registry.js')
const statusTool = createStatusTool(getSupervisorClientMock)

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

    if (sentinelPath) {
      try {
        fs.unlinkSync(sentinelPath)
      } catch {}
    }
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
    expect(result.model).toBeUndefined()
    expect(result.effective_model).toBeUndefined()

    statusMock.mockRejectedValueOnce(new Error('completed runtime removed'))
    const fallback = await statusTool({
      runId: runInfo.runId,
      supervisorId: '/tmp/avenor-mcp-test.sock',
    })
    expect(fallback).toMatchObject({ agent: 'final-agent', backend: 'agy' })
    expect(fallback.model).toBeUndefined()
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
