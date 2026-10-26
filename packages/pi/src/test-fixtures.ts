// Shared test fixtures for the pi extension test suites. Kept out of the
// *.test.ts files so every suite builds its ExtensionDeps mock from one set.
import { mock } from 'bun:test'
import { getSupervisorClient, type InspectResult, type MessageProvider, type RunSnapshot, type StatusResult } from '@dougbots/avenor-core'
import { RunReducer } from '@dougbots/avenor-core'
import type { ExtensionDeps } from './index.js'
import { createExtension } from './index.js'
import type { SubAgentEnv } from './subagent-broker.js'

/** Keep message delivery on Pi so context mocks cannot hide API misuse. */
export async function startExtensionHarness(options: {
  deps?: Partial<ExtensionDeps>
  extOptions?: { pollIntervalMs?: number; subAgentEnv?: SubAgentEnv | null }
  ui?: Record<string, any>
  /** The harness's user-defined session name, as pi's session manager reports it. */
  sessionName?: () => string | undefined
  /** Replaces the whole session-manager mock; without it the default reports `sessionName`. */
  sessionManager?: unknown
  onEmit?: (channel: string, payload: unknown) => void
  sendUserMessage?: any
} = {}) {
  const registeredTools: Record<string, any> = {}
  const registeredCommands: Record<string, any> = {}
  const eventHandlers: Record<string, any> = {}
  const setStatus = mock((..._args: any[]) => {})
  const setWidget = mock((..._args: any[]) => {})
  const notify = mock((..._args: any[]) => {})
  const sendUserMessage = options.sendUserMessage ?? mock(() => {})
  const emitted: Array<[string, unknown]> = []
  const mockPi = {
    on: (event: string, handler: any) => { eventHandlers[event] = handler },
    registerTool: (definition: { name: string }) => { registeredTools[definition.name] = definition },
    registerCommand: (name: string, definition: any) => { registeredCommands[name] = definition },
    registerMessageRenderer: () => {},
    sendUserMessage,
    events: {
      emit: mock((channel: string, payload: unknown) => {
        emitted.push([channel, payload])
        options.onEmit?.(channel, payload)
      }),
      on: mock(() => () => {}),
    },
  }
  const baseDeps: ExtensionDeps = {
    spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock', runtime_id: 'rt-1' })),
    statusTool: mock(async () => ({ runs: [], count: 0 })),
    eventsTool: mock(async () => ({ events: [] })),
    answerPermissionTool: mock(async () => ({ ok: true })),
    messageTool: mock(async () => ({
      action: 'resume',
      outcome: 'resumed',
      runId: 'run-2',
      label: 'follow-up',
      text: 'resumed "follow-up" as run "run-2"',
    })),
    inspectTool: mock(async () => makeInspectResult({ status: 'running' })),
    resultTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true })),
    brokerReceiveTool: mock(async () => ({ asks: [] })),
    shutdownTool: mock(async () => ({ ok: true })),
    killTool: mock(async () => ({ killed: true, run_id: 'run-1' })),
    workflowStatusTool: mock(async () => ({})),
    workflowWaitTool: mock(async () => ({})),
    workflowInspectTool: mock(async () => ({})),
    workflowEventsTool: mock(async () => ({})),
    workflowCompleteTool: mock(async () => ({})),
    workflowGateTool: mock(async () => ({})),
    workflowControllerStatusTool: mock(async () => ({})),
    workflowReadyTool: mock(async () => ({})),
    workflowCancelTool: mock(async () => ({})),
    hostProviderFor: mock(async () => makeMessageProvider()),
    getSupervisorClient: getSupervisorClient as any,
    observeRun: mock(() => null),
    dial: mock(async () => ({ close() {} })),
    attachOwner: mock(async () => ({ close() {} })),
    // A started session shares an in-process supervisor unless a test says otherwise.
    Supervisor: class {
      static currentInstance() {
        return { supervisorId: 'sup' }
      }
    } as any,
  }
  await createExtension({ ...baseDeps, ...options.deps }, { pollIntervalMs: 5, subAgentEnv: null, ...options.extOptions })(mockPi as any)

  const ctx = {
    cwd: '/tmp',
    sessionManager: options.sessionManager ?? { getSessionName: options.sessionName ?? (() => undefined) },
    ui: { setStatus, setWidget, notify, ...(options.ui ?? {}) },
  }
  await eventHandlers.session_start({}, ctx)
  return { ctx, emitted, eventHandlers, notify, registeredCommands, registeredTools, sendUserMessage, setStatus, setWidget }
}

/** A bound provider fake; host-mode tools pass one to the core handlers. */
export function makeMessageProvider(overrides: Partial<MessageProvider> = {}): MessageProvider {
  const provider: MessageProvider = {
    caller: () => ({ kind: 'owner', runId: 'supervisor' }),
    surface: () => 'pi-host',
    role: () => 'host',
    resolve: async ref => ({ runId: ref, label: ref, match: 'unique' }),
    state: async () => ({ live: false, resumable: true, status: 'done' }),
    tell: async () => ({ outcome: 'sent', messageId: 'msg-1' }),
    ask: async () => ({ outcome: 'replied', messageId: 'msg-1', reply: 'OK' }),
    reply: async () => ({ outcome: 'replied', messageId: 'msg-1' }),
    withdraw: async () => ({ outcome: 'withdrawn', messageId: 'msg-1' }),
    resume: async () => ({
      outcome: 'resumed',
      runId: 'run-2',
      runtimeId: 'rt-2',
      label: 'follow-up',
      sentinelFile: '/runs/run-2/sentinel.done',
      onEvent: '/runs/run-2/events.log',
    }),
    rename: async () => ({ outcome: 'renamed', runId: 'rt-1', label: 'renamed', previousLabel: 'demo' }),
    last: async () => ({ outcome: 'sent', hasRecord: false }),
    peers: async () => ({ peers: [] }),
    ...overrides,
  }
  return provider
}

export async function waitFor(predicate: () => boolean, timeoutMs = 2000, intervalMs = 5): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for condition')
    await new Promise(resolve => setTimeout(resolve, intervalMs))
  }
}

/** Fail fast on a blocking tool call that never returns: resolves the promise
 * or rejects with `message` after `timeoutMs`, so a missing interrupt surfaces
 * as a test failure instead of hanging the suite. */
export async function settleWithin<T>(promise: Promise<T>, message: string, timeoutMs = 1000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs)
        timer.unref?.()
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/** Reject the way fetch does when its signal aborts, so abort paths are real. */
export function waitUntilAborted(init: RequestInit | undefined): Promise<Response> {
  return new Promise((_resolve, reject) => {
    const signal = init?.signal
    if (!signal) throw new Error('request must receive an abort signal')
    if (signal.aborted) return reject(signal.reason)
    signal.addEventListener('abort', () => reject(signal.reason), { once: true })
  })
}

export function buildSnapshot(): RunSnapshot {
  const reducer = new RunReducer()
  reducer.apply({ event: 'session.start', runtime_id: 'rt-1', run_id: 'run-1', run_label: 'demo', backend: 'pi', agent: 'horse', model: 'model-1', dir: '/tmp/demo' })
  reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 1, delta: 'hello ' })
  reducer.apply({ event: 'agent.message_chunk', runtime_id: 'rt-1', seq: 2, delta: 'world' })
  reducer.apply({ event: 'session.end', runtime_id: 'rt-1', seq: 3, final_output: 'hello world', stop_reason: 'end_turn' })
  return reducer.snapshot()
}

export function makeInspectResult(statusOverrides: Partial<StatusResult> = {}): InspectResult {
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