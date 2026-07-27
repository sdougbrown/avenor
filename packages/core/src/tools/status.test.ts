import { afterEach, describe, expect, it, mock } from 'bun:test'
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
  },
  supervisorId: '/tmp/avenor-mcp-test.sock',
}))

mock.module('./get-supervisor-client.js', () => ({
  getSupervisorClient: getSupervisorClientMock,
}))

const { shapeStatusResult, statusTool } = await import('./status.js')

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

  it('uses non-empty status when phase is present but empty', async () => {
    sentinelPath = path.join(os.tmpdir(), `avenor-run-${runInfo.runId}.done`)
    fs.writeFileSync(sentinelPath, 'FAILED\nSESSION=ses-failed\n')
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
      stop_reason: 'FAILED',
      session_id: 'ses-failed',
    })
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
      model: 'qwen',
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
      model: 'qwen',
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
