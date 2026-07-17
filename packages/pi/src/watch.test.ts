import { describe, expect, it, mock } from 'bun:test'
import { RunReducer, type InspectResult, type RunObserver, type RunSnapshot, type StatusResult } from '@dougbots/avenor-core'
import { visibleWidth } from '@earendil-works/pi-tui'
import {
  RunInspectorOverlay,
  clipText,
  sanitizeText,
  openRunInspector,
  type WatchDeps,
  type WatchRunRef,
} from './watch.js'

const flush = async () => {
  await Promise.resolve()
  await Promise.resolve()
}

const theme = {
  fg: (_color: string, text: string) => text,
  bg: (_color: string, text: string) => text,
  bold: (text: string) => text,
  italic: (text: string) => text,
}

const tui = {
  terminal: { rows: 24 },
  requestRender: mock(() => {}),
}

const keybindings = {}

function buildSnapshot(events: unknown[]): RunSnapshot {
  const reducer = new RunReducer({ transcript_entries: 32, text_chars: 2000, tools: 8, tool_preview_chars: 200 })
  for (const event of events) {
    reducer.apply(event)
  }
  return reducer.snapshot()
}

function makeInspectResult(snapshot: RunSnapshot, status: Partial<StatusResult> = {}): InspectResult {
  const mergedStatus: StatusResult = {
    run_id: status.run_id ?? snapshot.identity.run_id ?? 'run-1',
    label: status.label ?? snapshot.identity.run_label ?? 'run-1',
    status: status.status ?? (snapshot.ended ? 'done' : 'running'),
    runtime_id: status.runtime_id ?? snapshot.identity.runtime_id,
    phase: status.phase ?? snapshot.phase,
    phase_label: status.phase_label ?? snapshot.phase_label,
    pending_permission: status.pending_permission ?? snapshot.pending_permission,
    session_id: status.session_id ?? snapshot.identity.session_id,
    stop_reason: status.stop_reason ?? snapshot.stop_reason,
    backend: status.backend ?? snapshot.identity.backend,
    agent: status.agent ?? snapshot.identity.agent,
    model: status.model ?? snapshot.identity.model,
    dir: status.dir ?? snapshot.identity.dir,
    usage: status.usage ?? snapshot.usage,
    latest_seq: status.latest_seq ?? snapshot.latest_seq,
    final_output: status.final_output ?? snapshot.final_output,
  }

  return {
    run_id: mergedStatus.run_id,
    label: mergedStatus.label,
    status: mergedStatus,
    snapshot,
    transcript: snapshot.transcript,
    tools: snapshot.tools,
    live_tools: snapshot.live_tools,
    permissions: snapshot.permissions,
    pending_permission: snapshot.pending_permission,
    final_output: mergedStatus.final_output,
  }
}

class FakeObserver implements RunObserver {
  closed = false
  private listeners = new Set<(snapshot: RunSnapshot, event: any) => void>()
  closeMock = mock(async () => {
    this.closed = true
  })

  constructor(private current: RunSnapshot) {}

  snapshot(): RunSnapshot {
    return this.current
  }

  subscribe(listener: (snapshot: RunSnapshot, event: any) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  next(): Promise<RunSnapshot | null> {
    return Promise.resolve(this.current)
  }

  async close(): Promise<void> {
    await this.closeMock()
  }

  emit(snapshot: RunSnapshot): void {
    this.current = snapshot
    for (const listener of this.listeners) {
      listener(snapshot, { event: snapshot.last_event ?? 'event' })
    }
  }
}

function makeDeps(overrides: Partial<WatchDeps> = {}): WatchDeps {
  return {
    inspectRun: overrides.inspectRun ?? mock(async (run: WatchRunRef) => makeInspectResult(buildSnapshot([
      { event: 'session.start', runtime_id: run.runtimeId ?? 'rt-1', run_id: run.runId, run_label: run.label ?? run.runId, agent: run.agent ?? 'horse', model: 'test-model', backend: 'pi' },
      { event: 'agent.message_chunk', runtime_id: run.runtimeId ?? 'rt-1', seq: 1, delta: 'hello world' },
    ]), { status: 'running' })),
    observeRun: overrides.observeRun ?? mock(async () => null),
    cancelRun: overrides.cancelRun ?? mock(async () => {}),
    promptRun: overrides.promptRun ?? mock(async () => {}),
    followUpRun: overrides.followUpRun ?? mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
  }
}

describe('watch helpers', () => {
  it('sanitizes ANSI, tabs, and control characters', () => {
    expect(sanitizeText('\u001b[31mred\u001b[0m\tline\u0007')).toBe('red  line')
  })

  it('clips sanitized text at the requested boundary', () => {
    expect(clipText(undefined, 5)).toBeUndefined()
    expect(clipText('\t\u001b[31mred\u001b[0m ', 10)).toBe('red')
    expect(clipText('abcde', 5)).toBe('abcde')
    expect(clipText('abcdef', 5)).toBe('abcd…')
  })

  it('uses shared reducer normalization for both event and legacy type fields', async () => {
    const snapshot = buildSnapshot([
      { type: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'hello ' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'world' },
    ])

    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({ inspectRun: mock(async () => makeInspectResult(snapshot)) }),
      { runId: 'run-1', runtimeId: 'rt-1' },
      () => {},
    )

    await flush()
    const rendered = overlay.render(40).join('\n')
    expect(rendered).toContain('hello world')
    overlay.dispose()
  })
})

describe('RunInspectorOverlay', () => {
  it('renders inspection failures from reload', async () => {
    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({ inspectRun: mock(async () => { throw new Error('inspection failed') }) }),
      { runId: 'run-error' },
      () => {},
    )

    await flush()
    expect(overlay.render(40).join('\n')).toContain('inspection failed')
    overlay.dispose()
  })

  it('renders rejected run actions and accepts another action afterward', async () => {
    const snapshot = buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-error', run_id: 'run-error', backend: 'pi' },
    ])
    const promptRun = mock(async () => { throw new Error('prompt rejected') })
    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({
        inspectRun: mock(async () => makeInspectResult(snapshot, { status: 'running' })),
        promptRun,
      }),
      { runId: 'run-error', runtimeId: 'rt-error' },
      () => {},
    )

    await flush()
    ;(overlay as any).submit('continue')
    await flush()
    expect(overlay.render(40).join('\n')).toContain('prompt rejected')

    ;(overlay as any).submit('retry')
    await flush()
    expect(promptRun).toHaveBeenCalledTimes(2)
    overlay.dispose()
  })

  it('requests re-render on observer updates and cleans up observer on dispose', async () => {
    tui.requestRender.mockClear()
    const initial = buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-1', run_id: 'run-1', run_label: 'demo', backend: 'pi', agent: 'horse' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'one' },
    ])
    const observer = new FakeObserver(initial)
    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({
        inspectRun: mock(async () => makeInspectResult(initial, { status: 'running' })),
        observeRun: mock(async () => observer),
      }),
      { runId: 'run-1', runtimeId: 'rt-1' },
      () => {},
    )

    await flush()
    const updated = buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-1', run_id: 'run-1', run_label: 'demo', backend: 'pi', agent: 'horse' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'one' },
      { event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: ' two' },
    ])
    observer.emit(updated)

    expect(tui.requestRender).toHaveBeenCalled()

    overlay.dispose()
    await flush()
    expect(observer.closeMock).toHaveBeenCalledTimes(1)
  })

  it('renders width-safe status, permission, tools, and transcript lines with scrolling', async () => {
    const snapshot = buildSnapshot([
      {
        event: 'session.start',
        runtime_id: 'rt-2',
        run_id: 'run-2',
        run_label: 'wide',
        backend: 'pi',
        agent: 'horse',
        model: 'demo-model',
        dir: '/tmp/demo',
      },
      {
        event: 'permission.request',
        runtime_id: 'rt-2',
        seq: 1,
        request_id: 'perm-1',
        description: '\u001b[31mAllow access\u001b[0m to /tmp/demo?',
        options: [
          { option_id: 'allow_once', label: 'Allow once', kind: 'allow_once' },
          { option_id: 'deny', label: 'Deny', kind: 'deny' },
        ],
      },
      { event: 'tool.call', runtime_id: 'rt-2', seq: 2, id: 'tool-1', title: 'bash', status: 'running', input: 'echo\tvery very very long command that should clip' },
      { event: 'agent.message_chunk', runtime_id: 'rt-2', seq: 3, delta: 'assistant line one' },
      { event: 'agent.message_chunk', runtime_id: 'rt-2', seq: 4, delta: ' and line two' },
      ...Array.from({ length: 10 }, (_, index) => ({
        event: index % 2 === 0 ? 'user.message_chunk' : 'agent.thought_chunk',
        runtime_id: 'rt-2',
        seq: index + 5,
        delta: `scroll line ${index}`,
      })),
    ])

    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({ inspectRun: mock(async () => makeInspectResult(snapshot, { status: 'waiting' })) }),
      { runId: 'run-2', runtimeId: 'rt-2' },
      () => {},
    )

    await flush()
    const lines = overlay.render(40)
    expect(lines.some(line => line.includes('permission:'))).toBe(true)
    expect(lines.some(line => line.includes('live tools:'))).toBe(true)
    expect(lines.some(line => line.includes('scroll line 9'))).toBe(true)
    expect(lines.every(line => visibleWidth(line) <= 40)).toBe(true)

    overlay.handleInput('\u001b[A')
    const scrolled = overlay.render(40)
    expect(scrolled.some(line => line.includes('below'))).toBe(true)
    overlay.dispose()
  })

  it('dispatches prompt and cancel actions for active runs', async () => {
    const promptRun = mock(async () => {})
    const cancelRun = mock(async () => {})
    const snapshot = buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-3', run_id: 'run-3', run_label: 'active', backend: 'pi', agent: 'horse' },
      { event: 'agent.message_chunk', runtime_id: 'rt-3', seq: 1, delta: 'working' },
    ])
    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({
        inspectRun: mock(async () => makeInspectResult(snapshot, { status: 'running' })),
        promptRun,
        cancelRun,
      }),
      { runId: 'run-3', runtimeId: 'rt-3' },
      () => {},
    )

    await flush()
    ;(overlay as any).submit('keep going')
    await flush()
    expect(promptRun).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-3' }), 'rt-3', 'keep going')

    overlay.handleInput('\u0003')
    await flush()
    expect(cancelRun).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-3' }), 'rt-3')
    overlay.dispose()
  })

  it('dispatches follow-up actions for completed runs and switches to the new run', async () => {
    const completed = buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-4', run_id: 'run-4', run_label: 'done-run', backend: 'pi', agent: 'horse' },
      { event: 'agent.message_chunk', runtime_id: 'rt-4', seq: 1, delta: 'draft' },
      { event: 'session.end', runtime_id: 'rt-4', seq: 2, final_output: 'final answer', stop_reason: 'end_turn' },
    ])
    const next = buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-5', run_id: 'run-5', run_label: 'follow-up', backend: 'pi', agent: 'horse' },
      { event: 'agent.message_chunk', runtime_id: 'rt-5', seq: 1, delta: 'continued' },
    ])
    const inspectRun = mock(async (run: WatchRunRef) =>
      run.runId === 'run-5'
        ? makeInspectResult(next, { status: 'running' })
        : makeInspectResult(completed, { status: 'done' }),
    )
    const followUpRun = mock(async () => ({ run_id: 'run-5', label: 'follow-up' }))

    const overlay = new RunInspectorOverlay(
      tui as any,
      theme as any,
      keybindings as any,
      makeDeps({ inspectRun, followUpRun }),
      { runId: 'run-4', runtimeId: 'rt-4' },
      () => {},
    )

    await flush()
    ;(overlay as any).submit('more detail')
    await flush()
    expect(followUpRun).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-4' }), 'more detail')
    expect(inspectRun).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-5', label: 'follow-up' }))
    overlay.dispose()
  })
})

describe('openRunInspector', () => {
  it('warns and returns when TUI mode is unavailable', async () => {
    const notify = mock(() => {})
    const custom = mock(async () => {})

    await openRunInspector({ hasUI: false, ui: { notify, custom } } as any, {
      trackedRuns: [],
      deps: makeDeps(),
    })

    expect(notify).toHaveBeenCalledWith('avenor-watch requires TUI mode', 'warning')
    expect(custom).not.toHaveBeenCalled()
  })

  it('treats a whitespace selector as empty and warns when no tracked runs are available', async () => {
    const notify = mock(() => {})
    const select = mock(async () => 'unused')
    const custom = mock(async () => {})

    await openRunInspector({ hasUI: true, ui: { notify, select, custom } } as any, {
      trackedRuns: [],
      initialRunId: '   ',
      deps: makeDeps(),
    })

    expect(notify).toHaveBeenCalledWith('No tracked runs', 'warning')
    expect(select).not.toHaveBeenCalled()
    expect(custom).not.toHaveBeenCalled()
  })

  it('returns when run selection is cancelled', async () => {
    const select = mock(async () => undefined)
    const custom = mock(async () => {})

    await openRunInspector({
      hasUI: true,
      ui: { notify: mock(() => {}), select, custom },
    } as any, {
      trackedRuns: [{ runId: 'run-1' }],
      deps: makeDeps(),
    })

    expect(select).toHaveBeenCalled()
    expect(custom).not.toHaveBeenCalled()
  })

  it('opens a uniquely matched agent name without showing the selector', async () => {
    const inspectRun = mock(async () => makeInspectResult(buildSnapshot([]), { status: 'running' }))
    const select = mock(async () => undefined)

    await openRunInspector({
      hasUI: true,
      ui: {
        notify: mock(() => {}),
        select,
        custom: mock(async (factory: any) => {
          const component = factory(tui, theme, keybindings, mock(() => {})) as RunInspectorOverlay
          component.dispose()
        }),
      },
    } as any, {
      trackedRuns: [{ runId: 'run-explore', label: 'test-explore', agent: 'explore' }],
      initialRunId: 'explore',
      deps: makeDeps({ inspectRun }),
    })

    expect(select).not.toHaveBeenCalled()
    expect(inspectRun).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-explore', agent: 'explore' }))
  })

  it('uses the picker to disambiguate multiple runs for one agent', async () => {
    const inspectRun = mock(async () => makeInspectResult(buildSnapshot([]), { status: 'running' }))
    const select = mock(async (_title: string, choices: string[]) => {
      expect(choices).toEqual([
        'first · explore · run-one',
        'second · explore · run-two',
      ])
      return choices[1]
    })

    await openRunInspector({
      hasUI: true,
      ui: {
        notify: mock(() => {}),
        select,
        custom: mock(async (factory: any) => {
          const component = factory(tui, theme, keybindings, mock(() => {})) as RunInspectorOverlay
          component.dispose()
        }),
      },
    } as any, {
      trackedRuns: [
        { runId: 'run-one', label: '\u001b[31mfirst\u001b[0m', agent: 'explore' },
        { runId: 'run-two', label: 'second', agent: 'explore' },
      ],
      initialRunId: 'explore',
      deps: makeDeps({ inspectRun }),
    })

    expect(select).toHaveBeenCalledTimes(1)
    expect(inspectRun).toHaveBeenCalledWith(expect.objectContaining({ runId: 'run-two', label: 'second' }))
  })

  it('uses the display string returned by ctx.ui.select()', async () => {
    const inspectRun = mock(async (run: WatchRunRef) => makeInspectResult(buildSnapshot([
      { event: 'session.start', runtime_id: 'rt-select', run_id: run.runId, run_label: run.runId, backend: 'pi', agent: 'horse' },
    ]), { status: 'running' }))

    let component: RunInspectorOverlay | undefined
    const done = mock(() => {})
    await openRunInspector({
      hasUI: true,
      ui: {
        notify: mock(() => {}),
        select: mock(async () => 'picked · run-sele'),
        custom: mock(async (factory: any) => {
          component = factory(tui, theme, keybindings, done)
          expect(component).toBeInstanceOf(RunInspectorOverlay)
          component.dispose()
        }),
      },
    } as any, {
      trackedRuns: [{ runId: 'run-selected', label: 'picked', runtimeId: 'rt-select' }],
      deps: makeDeps({ inspectRun }),
    })

    expect(component).toBeDefined()
    expect(inspectRun).toHaveBeenCalledWith({ runId: 'run-selected', label: 'picked', runtimeId: 'rt-select' })
    expect(done).not.toHaveBeenCalled()
  })
})
