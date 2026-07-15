import { describe, expect, it, mock } from 'bun:test'
import type { InspectResult, RunSnapshot, StatusResult } from '@dougbots/avenor-core'
import { RunReducer } from '@dougbots/avenor-core'
import extensionFactory, {
  buildCompletionText,
  buildInspectPayload,
  createExtension,
  statusSupervisorId,
} from './index.js'

function buildSnapshot(): RunSnapshot {
  const reducer = new RunReducer()
  reducer.apply({ event: 'session.start', runtime_id: 'rt-1', run_id: 'run-1', run_label: 'demo', backend: 'pi', agent: 'horse', model: 'model-1', dir: '/tmp/demo' })
  reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'hello ' })
  reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'world' })
  reducer.apply({ event: 'session.end', runtime_id: 'rt-1', seq: 3, final_output: 'hello world', stop_reason: 'end_turn' })
  return reducer.snapshot()
}

function makeInspectResult(statusOverrides: Partial<StatusResult> = {}): InspectResult {
  const snapshot = buildSnapshot()
  const status: StatusResult = {
    run_id: statusOverrides.run_id ?? 'run-1',
    label: statusOverrides.label ?? 'demo',
    status: statusOverrides.status ?? 'done',
    runtime_id: statusOverrides.runtime_id ?? 'rt-1',
    phase: statusOverrides.phase ?? snapshot.phase,
    phase_label: statusOverrides.phase_label ?? snapshot.phase_label,
    pending_permission: statusOverrides.pending_permission ?? snapshot.pending_permission,
    session_id: statusOverrides.session_id ?? 'ses-1',
    stop_reason: statusOverrides.stop_reason ?? snapshot.stop_reason,
    backend: statusOverrides.backend ?? 'pi',
    agent: statusOverrides.agent ?? 'horse',
    model: statusOverrides.model ?? 'model-1',
    dir: statusOverrides.dir ?? '/tmp/demo',
    usage: statusOverrides.usage ?? { total_tokens: 42 },
    latest_seq: statusOverrides.latest_seq ?? snapshot.latest_seq,
    final_output: statusOverrides.final_output ?? 'hello world',
  }

  return {
    run_id: status.run_id,
    label: status.label,
    status,
    snapshot,
    transcript: snapshot.transcript,
    tools: snapshot.tools,
    live_tools: snapshot.live_tools,
    permissions: snapshot.permissions,
    pending_permission: snapshot.pending_permission,
    final_output: status.final_output,
  }
}

describe('Avenor Pi extension', () => {
  it('exports a function', () => {
    expect(typeof extensionFactory).toBe('function')
  })

  it('only reuses a supervisor socket when the caller supplied one', () => {
    expect(statusSupervisorId(undefined, '/tmp/spawned.sock')).toBeUndefined()
    expect(statusSupervisorId('/tmp/requested.sock', '/tmp/spawned.sock')).toBe('/tmp/spawned.sock')
  })

  it('includes final output in completion text', () => {
    const text = buildCompletionText(
      { runId: 'run-1', label: 'demo' },
      { run_id: 'run-1', label: 'demo', status: 'done', final_output: 'finished', session_id: 'ses-1' },
      'final answer',
    )

    expect(text).toContain('Final output:')
    expect(text).toContain('final answer')
    expect(text).toContain('avenor_inspect')
  })

  it('builds bounded inspect payloads with final_output exposed', () => {
    const payload = buildInspectPayload(makeInspectResult({ final_output: 'useful answer' }))
    expect(payload.final_output).toBe('useful answer')
    expect((payload.transcript as any[]).length).toBeGreaterThan(0)
  })

  it('registers all expected tools, commands, renderers, and event handlers', async () => {
    const registeredTools: Record<string, any> = {}
    const registeredCommands: string[] = []
    const registeredRenderers: string[] = []
    const eventHandlers: Map<string, any[]> = new Map()

    const mockPi = {
      on: (event: string, handler: any) => {
        eventHandlers.set(event, [...(eventHandlers.get(event) ?? []), handler])
      },
      registerTool: (def: { name: string }) => {
        registeredTools[def.name] = def
      },
      registerCommand: (name: string) => {
        registeredCommands.push(name)
      },
      registerMessageRenderer: (type: string) => {
        registeredRenderers.push(type)
      },
      sendUserMessage: mock(() => {}),
    }

    await createExtension({
      spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock', runtime_id: 'rt-1' })),
      statusTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', runtime_id: 'rt-1', final_output: 'answer' })),
      eventsTool: mock(async () => ({ events: [] })),
      answerPermissionTool: mock(async () => ({ ok: true })),
      followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
      inspectTool: mock(async () => makeInspectResult()),
      shutdownTool: mock(async () => ({ ok: true })),
      observeRun: mock(() => ({
        closed: false,
        snapshot: () => buildSnapshot(),
        subscribe: () => () => {},
        next: async () => buildSnapshot(),
        close: async () => {},
      })),
      dial: mock(async () => ({ close() {}, cancel: async () => {}, prompt: async () => {}, events() { throw new Error('unused') } })),
      Supervisor: class {
        static async get() {
          return { supervisorId: '/tmp/sock' }
        }
      } as any,
    })(mockPi as any)

    expect(Object.keys(registeredTools)).toContain('avenor_spawn')
    expect(Object.keys(registeredTools)).toContain('avenor_status')
    expect(Object.keys(registeredTools)).toContain('avenor_inspect')
    expect(Object.keys(registeredTools)).toContain('avenor_answer_permission')
    expect(Object.keys(registeredTools)).toContain('avenor_follow_up')
    expect(Object.keys(registeredTools)).toContain('avenor_events')
    expect(Object.keys(registeredTools)).toContain('avenor_shutdown')

    expect(registeredCommands).toContain('avenor-status')
    expect(registeredCommands).toContain('avenor-watch')
    expect(registeredCommands).toContain('avenor-cancel')

    expect(registeredRenderers).toContain('avenor-active-runs')
    expect(eventHandlers.has('session_start')).toBe(true)
    expect(eventHandlers.has('session_shutdown')).toBe(true)
    expect(eventHandlers.has('before_agent_start')).toBe(true)
  })

  it('registers avenor_inspect with bounded JSON output', async () => {
    let inspectToolDef: any
    const mockPi = {
      on: () => {},
      registerTool: (def: any) => {
        if (def.name === 'avenor_inspect') inspectToolDef = def
      },
      registerCommand: () => {},
      registerMessageRenderer: () => {},
      sendUserMessage: () => {},
    }

    await createExtension({
      ...({
        spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock', runtime_id: 'rt-1' })),
        statusTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', runtime_id: 'rt-1', final_output: 'answer' })),
        eventsTool: mock(async () => ({ events: [] })),
        answerPermissionTool: mock(async () => ({ ok: true })),
        followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
        inspectTool: mock(async () => makeInspectResult()),
        shutdownTool: mock(async () => ({ ok: true })),
        observeRun: mock(() => null),
        dial: mock(async () => ({ close() {}, cancel: async () => {}, prompt: async () => {}, events() { throw new Error('unused') } })),
        Supervisor: class {
          static async get() {
            return { supervisorId: '/tmp/sock' }
          }
        } as any,
      })
    })(mockPi as any)

    const result = await inspectToolDef.execute('tool-1', { run_id: 'run-1' })
    expect(result.content[0].text).toContain('"final_output": "hello world"')
    expect(result.details.snapshot.identity.run_id).toBe('run-1')
  })
})
