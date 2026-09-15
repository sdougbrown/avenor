// Shared test fixtures for the pi extension test suites. Kept out of the
// *.test.ts files so every suite builds its ExtensionDeps mock from one set.
import { mock } from 'bun:test'
import type { InspectResult, RunSnapshot, StatusResult } from '@dougbots/avenor-core'
import { RunReducer } from '@dougbots/avenor-core'
import type { ExtensionDeps } from './index.js'

/** Build minimal mock ExtensionDeps with all tools stubbed via bun:test.mock(). */
export function buildMockDeps(partial: Partial<ExtensionDeps> = {}): ExtensionDeps {
  return {
    spawnTool: mock(async () => ({ run_id: 'run-1', label: 'demo', supervisor_id: '/tmp/sock' })),
    statusTool: mock(async () => []),
    eventsTool: mock(async () => ({ events: [] })),
    answerPermissionTool: mock(async () => ({ ok: true })),
    followUpTool: mock(async () => ({ run_id: 'run-2', label: 'follow-up' })),
    inspectTool: mock(async () => makeInspectResult()),
    resultTool: mock(async () => ({ run_id: 'run-1', label: 'demo', status: 'done', ready: true })),
    brokerReceiveTool: mock(async () => ({ asks: [] })),
    brokerReplyTool: mock(async () => ({ ok: true })),
    shutdownTool: mock(async () => ({ ok: true })),
    workflowStatusTool: mock(async () => ({})),
    workflowWaitTool: mock(async () => ({})),
    workflowInspectTool: mock(async () => ({})),
    workflowEventsTool: mock(async () => ({})),
    workflowCompleteTool: mock(async () => ({})),
    workflowGateTool: mock(async () => ({})),
    observeRun: mock(() => null),
    dial: mock(async () => ({ close() {} })),
    Supervisor: class {} as any,
    ...partial,
  }
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