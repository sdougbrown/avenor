import { describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { answerPermissionTool } from './answer-permission.js'
import { createEventsTool } from './events.js'
import { createFollowUpTool } from './follow-up.js'
import { findLocalRunByReference } from './run-resolution.js'
import { createStatusTool } from './status.js'

function run(runId: string, label: string): RunInfo {
  return {
    runId,
    label,
    sentinelPath: `/runs/${runId}/sentinel.done`,
    eventLogPath: `/runs/${runId}/events.log`,
  }
}

function localSupervisor(
  runs: Array<RunInfo>,
  aliases: Array<RunInfo> = runs,
): Supervisor {
  const sup = Object.create(Supervisor.prototype) as Supervisor
  ;(sup as any).runs = new Map(runs.map(info => [info.runId, info]))
  ;(sup as any).aliases = new Map(aliases.map(info => [info.label, info]))
  return sup
}

describe('local run resolution', () => {
  it('prefers an exact public ID over a conflicting label alias', () => {
    const exact = run('public-run', 'friendly')
    const alias = run('other-run', 'public-run')
    const sup = localSupervisor([exact, alias])

    expect(findLocalRunByReference(sup, 'public-run')).toBe(exact)
  })

  it('falls back to a label only when no public ID exists', () => {
    const labeled = run('public-run', 'friendly')
    const sup = localSupervisor([labeled])

    expect(findLocalRunByReference(sup, 'friendly')).toBe(labeled)
  })

  it('returns no run when neither a public ID nor label matches', () => {
    const sup = localSupervisor([run('public-run', 'friendly')])

    expect(findLocalRunByReference(sup, 'missing')).toBeUndefined()
  })

  it('keeps a registered public ID canonical across local tools after a label collision', async () => {
    const previousHome = process.env.AVENOR_HOME
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-local-run-resolution-'))
    process.env.AVENOR_HOME = home

    const runtimes = new Map<string, { label: string; sessionId: string }>()
    let nextRuntime = 0
    const spawn = mock(async (params: Record<string, unknown>) => {
      const runtimeId = `runtime-${++nextRuntime}`
      const sessionId = typeof params.session_id === 'string'
        ? params.session_id
        : `session-${nextRuntime}`
      runtimes.set(runtimeId, { label: String(params.label), sessionId })
      return {
        runtime_id: runtimeId,
        session_id: sessionId,
        label: params.label,
        sentinel_file: params.sentinel_file,
        on_event: params.on_event,
      }
    })
    const status = mock(async (runtimeId: string) => {
      const runtime = runtimes.get(runtimeId)
      if (!runtime) throw new Error(`unknown runtime: ${runtimeId}`)
      return {
        runtime_id: runtimeId,
        session_id: runtime.sessionId,
        label: runtime.label,
        status: 'waiting',
        permission: {
          request_id: `permission-${runtimeId}`,
          description: `permission for ${runtimeId}`,
          options: [],
        },
      }
    })
    const list = mock(async () => (
      [...runtimes.entries()].map(([runtimeId, runtime]) => ({
        runtime_id: runtimeId,
        session_id: runtime.sessionId,
        label: runtime.label,
        status: 'waiting',
      }))
    ))
    const history = mock(async () => ({ events: [] }))
    const answerPermission = mock(async () => {})
    const client = {
      spawn,
      status,
      list,
      history,
      answerPermission,
      isClosed: () => false,
    }
    const sup = Object.create(Supervisor.prototype) as Supervisor
    ;(sup as any).client = client
    ;(sup as any).crashed = false
    ;(sup as any).runs = new Map()
    ;(sup as any).aliases = new Map()

    try {
      const canonical = await sup.spawn({
        agent: 'explore',
        backend: 'pi',
        label: 'first-run',
        prompt: 'first',
      }, 'canonical-run')
      const colliding = await sup.spawn({
        agent: 'explore',
        backend: 'pi',
        label: canonical.runId,
        prompt: 'second',
      }, 'colliding-run')

      expect((sup as any).runs.get(canonical.runId)).toBe(canonical)
      expect((sup as any).aliases.get(canonical.runId)).toBe(colliding)
      expect(findLocalRunByReference(sup, canonical.runId)).toBe(canonical)

      fs.writeFileSync(
        canonical.eventLogPath,
        JSON.stringify({
          event: 'canonical-event',
          runtime_id: canonical.runtimeId,
          seq: 1,
        }) + '\n',
      )
      fs.writeFileSync(
        colliding.eventLogPath,
        JSON.stringify({
          event: 'colliding-event',
          runtime_id: colliding.runtimeId,
          seq: 1,
        }) + '\n',
      )

      const getSupervisorClient = mock(async () => ({
        client,
        isSingleton: true,
        sup,
        supervisorId: '/tmp/avenor-mcp-local-run-resolution.sock',
      })) as any
      const statusTool = createStatusTool(getSupervisorClient)
      const eventsTool = createEventsTool(getSupervisorClient)
      const followUpTool = createFollowUpTool(getSupervisorClient)

      const statusResult = await statusTool({
        runId: canonical.runId,
        supervisorId: '/tmp/avenor-mcp-local-run-resolution.sock',
      })
      expect(statusResult).toMatchObject({
        run_id: canonical.runId,
        runtime_id: canonical.runtimeId,
        label: canonical.label,
      })
      expect(status).toHaveBeenLastCalledWith(canonical.runtimeId)

      const originalStatusSupervisorGet = Supervisor.get
      Supervisor.get = mock(async () => sup) as typeof Supervisor.get
      try {
        const statusList = await statusTool({})
        expect(statusList).toHaveLength(2)
        expect(statusList).toEqual(expect.arrayContaining([
          expect.objectContaining({
            run_id: canonical.runId,
            runtime_id: canonical.runtimeId,
            label: canonical.label,
          }),
          expect.objectContaining({
            run_id: colliding.runId,
            runtime_id: colliding.runtimeId,
            label: colliding.label,
          }),
        ]))
      } finally {
        Supervisor.get = originalStatusSupervisorGet
      }

      const eventsResult = await eventsTool({
        runId: canonical.runId,
        supervisorId: '/tmp/avenor-mcp-local-run-resolution.sock',
      })
      expect(eventsResult.events).toEqual([
        expect.objectContaining({
          event: 'canonical-event',
          runtime_id: canonical.runtimeId,
        }),
      ])
      expect(history).not.toHaveBeenCalled()

      const spawnCallsBeforeFollowUp = spawn.mock.calls.length
      const followUpResult = await followUpTool({
        runId: canonical.runId,
        message: 'continue canonical run',
        supervisorId: '/tmp/avenor-mcp-local-run-resolution.sock',
      })
      expect(spawn).toHaveBeenCalledTimes(spawnCallsBeforeFollowUp + 1)
      const followUpParams = spawn.mock.calls[spawnCallsBeforeFollowUp]?.[0]
      expect(followUpParams).toMatchObject({
        session_id: canonical.sessionId,
        label: `${canonical.runId}-followup`,
      })
      expect(followUpResult.runtime_id).not.toBe(colliding.runtimeId)

      const originalSupervisorGet = Supervisor.get
      Supervisor.get = mock(async () => sup) as typeof Supervisor.get
      try {
        await answerPermissionTool({
          runId: canonical.runId,
          optionId: 'allow-once',
        })
      } finally {
        Supervisor.get = originalSupervisorGet
      }
      expect(answerPermission).toHaveBeenCalledWith(
        canonical.runtimeId,
        `permission-${canonical.runtimeId}`,
        'allow-once',
        undefined,
      )
    } finally {
      if (previousHome === undefined) delete process.env.AVENOR_HOME
      else process.env.AVENOR_HOME = previousHome
      fs.rmSync(home, { recursive: true, force: true })
    }
  })
})
