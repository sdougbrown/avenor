import { describe, expect, it, mock } from 'bun:test'
import { createResultTool, type ResultResult } from './result.js'
import type { StatusResult } from './status.js'

function status(overrides: Partial<StatusResult> = {}): StatusResult {
  return {
    run_id: 'run-1',
    label: 'demo',
    status: 'running',
    runtime_id: 'rt-1',
    ...overrides,
  }
}

function deps(statuses: StatusResult[], pollIntervalMs = 1_000) {
  const queue = [...statuses]
  const statusMock = mock(async () => queue.shift() ?? statuses[statuses.length - 1]!)
  const inspectMock = mock(async () => ({
    final_output: 'inspected fallback',
    snapshot: { final_output: 'inspected fallback', assistant_text: 'inspected fallback' },
  }))
  let fakeTime = 0
  const sleepMock = mock(async (ms: number) => {
    fakeTime += ms
  })
  const nowMock = mock(() => fakeTime)
  return {
    value: { status: statusMock, inspect: inspectMock, sleep: sleepMock, now: nowMock, pollIntervalMs },
    statusMock,
    inspectMock,
    sleepMock,
  }
}

describe('resultTool', () => {
  it('returns only terminal result fields and the final output', async () => {
    const testDeps = deps([status({
      status: 'done',
      session_id: 'ses-1',
      stop_reason: 'end_turn',
      final_output: 'final answer',
      usage: { total_tokens: 50 },
      event_path: '/tmp/events.log',
    })])

    const result = await createResultTool(testDeps.value)({ runId: 'run-1' })

    expect(result).toEqual({
      run_id: 'run-1',
      label: 'demo',
      status: 'done',
      ready: true,
      runtime_id: 'rt-1',
      session_id: 'ses-1',
      stop_reason: 'end_turn',
      output: 'final answer',
    })
    expect(testDeps.statusMock).toHaveBeenCalledWith({
      runId: 'run-1',
      supervisorId: undefined,
      view: 'full',
    })
    expect(testDeps.inspectMock).not.toHaveBeenCalled()
  })

  it('falls back to inspection when terminal status has no final output', async () => {
    const testDeps = deps([status({ status: 'done' })])

    const result = await createResultTool(testDeps.value)({ runId: 'run-1' })

    expect(result).toMatchObject({ ready: true, output: 'inspected fallback' })
    expect(testDeps.inspectMock).toHaveBeenCalledWith({ runId: 'run-1', supervisorId: undefined })
  })

  it('waits through running states until a result is ready', async () => {
    const testDeps = deps([
      status(),
      status({ status: 'done', final_output: 'finished' }),
    ], 5)

    const result = await createResultTool(testDeps.value)({ runId: 'run-1' })

    expect(result.output).toBe('finished')
    expect(testDeps.statusMock).toHaveBeenCalledTimes(2)
    expect(testDeps.sleepMock).toHaveBeenCalledOnce()
  })

  it('returns immediately when non-blocking (wait=false)', async () => {
    const runningDeps = deps([status()])
    const running = await createResultTool(runningDeps.value)({ runId: 'run-1', wait: false })
    expect(running).toMatchObject({ status: 'running', ready: false })
    expect(runningDeps.sleepMock).not.toHaveBeenCalled()
  })

  it('returns immediately when waiting for permission', async () => {
    const waitingDeps = deps([status({
      status: 'waiting',
      pending_permission: { request_id: 'req-1', description: 'Allow?', options: [] },
    })])
    const waiting = await createResultTool(waitingDeps.value)({ runId: 'run-1' })
    expect(waiting).toMatchObject({
      status: 'waiting',
      ready: false,
      pending_permission: { request_id: 'req-1' },
    } satisfies Partial<ResultResult>)
    expect(waitingDeps.sleepMock).not.toHaveBeenCalled()
  })

  it('returns terminal output before timeout deadline', async () => {
    const testDeps = deps([
      status(),
      status({ status: 'done', final_output: 'before-deadline' }),
    ], 5)

    const result = await createResultTool(testDeps.value)({ runId: 'run-1', timeout: '2s' })

    expect(result.output).toBe('before-deadline')
    expect(result.timed_out).toBeUndefined()
    expect(testDeps.statusMock).toHaveBeenCalledTimes(2)
    expect(testDeps.sleepMock).toHaveBeenCalledOnce()
  })

  it('returns the latest lifecycle state when its wait timeout expires', async () => {
    const testDeps = deps([status(), status()])

    const result = await createResultTool(testDeps.value)({ runId: 'run-1', timeout: '1s' })

    expect(result).toMatchObject({ status: 'running', ready: false, timed_out: true })
    expect(testDeps.statusMock).toHaveBeenCalledTimes(2)
    expect(testDeps.sleepMock).toHaveBeenCalledOnce()
  })

  it('honors cancellation before polling', async () => {
    const controller = new AbortController()
    controller.abort()
    const testDeps = deps([status()])

    await expect(createResultTool(testDeps.value)({ runId: 'run-1', signal: controller.signal })).rejects.toMatchObject({ name: 'AbortError' })
    expect(testDeps.statusMock).not.toHaveBeenCalled()
  })
})
