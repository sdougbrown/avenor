import { describe, expect, it, mock } from 'bun:test'
import type { InspectResult, RunSnapshot, StatusResult } from '@dougbots/avenor-core'
import { RunReducer } from '@dougbots/avenor-core'
import extensionFactory, {
  buildCompletionText,
  buildInspectPayload,
  compactWhitespace,
  createExtension,
  isTerminalStatus,
  renderStatusLines,
  statusSupervisorId,
} from './index.js'
import { findLiveStatusForTrackedRun } from './types.js'

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

  it('formats structured completion text with final output and inspection guidance', () => {
    const text = buildCompletionText(
      { runId: 'run-1', label: 'demo' },
      { run_id: 'run-1', label: 'demo', status: 'done', final_output: 'finished', session_id: 'ses-1' },
      'final answer',
    )

    expect(text).toBe([
      'Sub-agent "demo" finished with status **done**.',
      '',
      'Final output:',
      '> final answer',
      'Session: `ses-1`',
      '',
      'Call `avenor_inspect` with `run_id: "run-1"` for the bounded transcript/snapshot, or `avenor_follow_up` to iterate.',
    ].join('\n'))
  })

  it('builds bounded inspect payloads with final_output exposed', () => {
    const result = makeInspectResult({ final_output: 'x'.repeat(800) })
    result.transcript = Array.from({ length: 40 }, (_, index) => ({
      kind: 'assistant' as const,
      event: 'agent.message_chunk',
      source_event: 'agent.message_chunk',
      text: `entry-${index}`,
    }))

    const payload = buildInspectPayload(result)
    expect(String(payload.final_output)).toHaveLength(600)
    expect(String(payload.final_output)).toEndWith('…')
    expect(payload.transcript).toHaveLength(24)
    expect((payload.transcript as Array<{ text: string }>)[0].text).toBe('entry-16')
  })

  it('recognizes terminal statuses only', () => {
    expect(['done', 'failed', 'timeout', 'killed'].map(isTerminalStatus)).toEqual([true, true, true, true])
    expect(isTerminalStatus('running')).toBe(false)
    expect(isTerminalStatus(undefined)).toBe(false)
  })

  it('reconciles spawned UUIDs with singleton live runtime identities', () => {
    const live: StatusResult = {
      run_id: 'canonical-run',
      label: 'test-explore',
      status: 'running',
      runtime_id: 'rt-1',
    }

    expect(findLiveStatusForTrackedRun({ runId: 'canonical-run' }, [live])).toBe(live)
    expect(findLiveStatusForTrackedRun({ runId: 'missing' }, [])).toBeUndefined()
    expect(findLiveStatusForTrackedRun({
      runId: 'spawn-uuid',
      runtimeId: 'rt-1',
    }, [live])).toBe(live)
    expect(findLiveStatusForTrackedRun({
      runId: 'spawn-uuid',
      runtimeId: 'rt-1',
      supervisorId: '/tmp/other.sock',
    }, [live])).toBeUndefined()
    expect(findLiveStatusForTrackedRun({
      runId: 'spawn-uuid',
      supervisorId: '/tmp/other.sock',
      lastStatus: live,
    }, [live])).toBe(live)
  })

  it('renders only active statuses with phase and permission markers', () => {
    expect(renderStatusLines([])).toEqual([])
    expect(renderStatusLines([
      { runId: 'done', label: 'finished', status: 'done', agent: 'horse' },
      { runId: 'active', label: 'worker', status: 'running', phaseLabel: 'reading', agent: 'horse', pendingPermission: true },
    ])).toEqual(['🟢 worker — running [reading] 🔒'])
  })

  it('compacts whitespace and clips bounded progress text', () => {
    expect(compactWhitespace(undefined)).toBeUndefined()
    expect(compactWhitespace('   ')).toBeUndefined()
    expect(compactWhitespace(' one\n\t two   three ')).toBe('one two three')
    expect(compactWhitespace('abcdefgh', 5)).toBe('abcd…')
  })

  it('registers all expected tools, commands, renderers, and event handlers', async () => {
    const registeredTools: Record<string, any> = {}
    const registeredCommands: Record<string, any> = {}
    const registeredRenderers: string[] = []
    const eventHandlers: Map<string, any[]> = new Map()

    const mockPi = {
      on: (event: string, handler: any) => {
        eventHandlers.set(event, [...(eventHandlers.get(event) ?? []), handler])
      },
      registerTool: (def: { name: string }) => {
        registeredTools[def.name] = def
      },
      registerCommand: (name: string, definition: any) => {
        registeredCommands[name] = definition
      },
      registerMessageRenderer: (type: string) => {
        registeredRenderers.push(type)
      },
      sendUserMessage: mock(() => {}),
    }

    const statusToolMock = mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'running', runtime_id: 'rt-1' }))
    const resultToolMock = mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true, output: 'hello world' }))
    const singletonPrompt = mock(async () => {})
    const singletonCancel = mock(async () => {})
    const externalPrompt = mock(async () => {})
    const externalClose = mock(() => {})
    const singletonClient = { close() {}, cancel: singletonCancel, prompt: singletonPrompt, events() { throw new Error('unused') } }
    const externalClient = { close: externalClose, cancel: async () => {}, prompt: externalPrompt, events() { throw new Error('unused') } }

    await createExtension({
      spawnTool: mock(async (args: { supervisorId?: string }) => ({
        run_id: args.supervisorId === '/tmp/external.sock' ? 'run-external' : 'run-1',
        label: 'demo',
        supervisor_id: args.supervisorId ?? '/tmp/sock',
        runtime_id: 'rt-1',
      })),
      statusTool: statusToolMock,
      eventsTool: mock(async () => ({ events: [] })),
      answerPermissionTool: mock(async () => ({ ok: true })),
      followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
      inspectTool: mock(async () => {
        const result = makeInspectResult({ status: 'running' })
        result.snapshot = { ...result.snapshot, ended: false, stop_reason: undefined }
        result.status = { ...result.status, status: 'running', stop_reason: undefined }
        return result
      }),
      resultTool: resultToolMock,
      shutdownTool: mock(async () => ({ ok: true })),
      observeRun: mock(() => null),
      dial: mock(async () => externalClient),
      Supervisor: class {
        static isCurrentInstance(supervisorId: string) {
          return supervisorId === '/tmp/sock'
        }

        static async get() {
          return { supervisorId: '/tmp/sock', getClient: () => singletonClient }
        }
      } as any,
    })(mockPi as any)

    expect(Object.keys(registeredTools)).toContain('avenor_spawn')
    expect(Object.keys(registeredTools)).toContain('avenor_status')
    expect(Object.keys(registeredTools)).toContain('avenor_result')
    expect(Object.keys(registeredTools)).toContain('avenor_inspect')
    expect(Object.keys(registeredTools)).toContain('avenor_answer_permission')
    expect(Object.keys(registeredTools)).toContain('avenor_follow_up')
    expect(Object.keys(registeredTools)).toContain('avenor_events')
    expect(Object.keys(registeredTools)).toContain('avenor_shutdown')

    expect(Object.keys(registeredCommands)).toContain('avenor-status')
    expect(Object.keys(registeredCommands)).toContain('avenor-watch')
    expect(Object.keys(registeredCommands)).toContain('avenor-cancel')

    await registeredTools.avenor_spawn.execute(
      'tool-1',
      { agent: 'explore', label: '\u001b[31mtest-pi-explore\u001b[0m', supervisor_id: '/tmp/sock', wait: false },
      undefined,
      undefined,
      { cwd: '/tmp' },
    )
    const expectedCompletion = [{
      value: 'run-1',
      label: 'test-pi-explore (explore, run-1)',
    }]
    expect(registeredCommands['avenor-watch'].getArgumentCompletions('expl')).toEqual(expectedCompletion)
    expect(registeredCommands['avenor-cancel'].getArgumentCompletions('expl')).toEqual(expectedCompletion)

    await registeredTools.avenor_status.execute('tool-status', { run_id: 'run-1', view: 'lifecycle' })
    expect(statusToolMock).toHaveBeenCalledWith({ runId: 'run-1', supervisorId: undefined, view: 'lifecycle' })

    async function runWatchAction(runId: string, actionSpy: any, action: (overlay: any) => void): Promise<void> {
      let overlay: any
      let resolveLoaded!: () => void
      const loaded = new Promise<void>(resolve => { resolveLoaded = resolve })
      let renderCount = 0
      let resolveAction!: () => void
      const actionCompleted = new Promise<void>(resolve => { resolveAction = resolve })
      actionSpy.mockImplementation(async () => { resolveAction() })

      await registeredCommands['avenor-watch'].handler(runId, {
        hasUI: true,
        ui: {
          custom: async (factory: any) => {
            overlay = factory({
              terminal: { rows: 24 },
              requestRender() {
                if (++renderCount === 2) resolveLoaded()
              },
            }, {
              fg: (_color: string, text: string) => text,
              bg: (_color: string, text: string) => text,
              bold: (text: string) => text,
              italic: (text: string) => text,
            }, {}, () => {})
          },
        },
      })
      await loaded
      action(overlay)
      await actionCompleted
      overlay.dispose()
    }

    await runWatchAction('run-1', singletonPrompt, overlay => overlay.submit('continue'))
    expect(singletonPrompt).toHaveBeenCalledWith('rt-1', 'continue')
    await runWatchAction('run-1', singletonCancel, overlay => overlay.handleInput('\u0003'))
    expect(singletonCancel).toHaveBeenCalledWith('rt-1')

    await registeredTools.avenor_spawn.execute(
      'tool-external',
      { agent: 'explore', label: 'external', supervisor_id: '/tmp/external.sock', wait: false },
      undefined,
      undefined,
      { cwd: '/tmp' },
    )
    let resolveExternalClose!: () => void
    const externalClosed = new Promise<void>(resolve => { resolveExternalClose = resolve })
    externalClose.mockImplementation(() => { resolveExternalClose() })
    await runWatchAction('run-external', externalPrompt, overlay => overlay.submit('continue'))
    await externalClosed
    expect(externalPrompt).toHaveBeenCalledWith('rt-1', 'continue')
    expect(externalClose).toHaveBeenCalledTimes(1)

    const result = await registeredTools.avenor_result.execute('tool-result', { run_id: 'run-1', timeout: '5m' })
    expect(result.content[0].text).toContain('"output": "hello world"')
    expect(resultToolMock).toHaveBeenCalledWith({
      runId: 'run-1',
      supervisorId: undefined,
      wait: undefined,
      timeout: '5m',
      signal: undefined,
    })

    for (const handler of eventHandlers.get('session_shutdown') ?? []) await handler()

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
        resultTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true, output: 'hello world' })),
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
