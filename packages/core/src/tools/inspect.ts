import { RunReducer, type RunSnapshot } from '../run-reducer.js'
import { eventsTool } from './events.js'
import { statusTool, type StatusResult } from './status.js'

function publicSnapshot(snapshot: RunSnapshot): Omit<RunSnapshot, '_seen_seq_by_scope' | '_recent_legacy_keys' | '_last_semantic_fingerprint' | '_last_source_event'> {
  const {
    _seen_seq_by_scope: _seen,
    _recent_legacy_keys: _recent,
    _last_semantic_fingerprint: _fingerprint,
    _last_source_event: _source,
    ...rest
  } = snapshot
  return rest
}

export interface InspectResult {
  run_id: string
  label: string
  status: StatusResult
  snapshot: Omit<RunSnapshot, '_seen_seq_by_scope' | '_recent_legacy_keys' | '_last_semantic_fingerprint' | '_last_source_event'>
  transcript: RunSnapshot['transcript']
  tools: RunSnapshot['tools']
  live_tools: RunSnapshot['live_tools']
  permissions: RunSnapshot['permissions']
  pending_permission?: RunSnapshot['pending_permission']
  final_output?: string
}

export async function inspectTool(args: {
  runId: string
  supervisorId?: string
  limit?: number
  after_seq?: number
}): Promise<InspectResult> {
  const rawStatus = await statusTool({
    runId: args.runId,
    supervisorId: args.supervisorId,
  })
  const status = Array.isArray(rawStatus) ? rawStatus[0] : rawStatus
  if (!status) {
    throw new Error(`run not found: ${args.runId}`)
  }

  const reducer = new RunReducer()
  const { events } = await eventsTool({
    runId: args.runId,
    supervisorId: args.supervisorId,
    limit: args.limit,
    after_seq: args.after_seq,
  })

  for (const event of events) {
    reducer.apply(event)
  }

  const snapshot = reducer.snapshot()
  snapshot.identity = {
    ...snapshot.identity,
    runtime_id: status.runtime_id ?? snapshot.identity.runtime_id,
    session_id: status.session_id ?? snapshot.identity.session_id,
    run_id: status.run_id ?? snapshot.identity.run_id,
    run_label: status.label ?? snapshot.identity.run_label,
    backend: status.backend ?? snapshot.identity.backend,
    agent: status.agent ?? snapshot.identity.agent,
    model: status.model ?? snapshot.identity.model,
    dir: status.dir ?? snapshot.identity.dir,
    parent_id: status.parent_id ?? snapshot.identity.parent_id,
    children: status.children ?? snapshot.identity.children,
    event_path: status.event_path ?? snapshot.identity.event_path,
  }
  snapshot.phase = status.phase ?? snapshot.phase
  snapshot.phase_label = status.phase_label ?? snapshot.phase_label
  snapshot.latest_seq = status.latest_seq ?? snapshot.latest_seq
  snapshot.usage = status.usage ?? snapshot.usage
  snapshot.stop_reason = status.stop_reason ?? snapshot.stop_reason
  snapshot.final_output = status.final_output ?? snapshot.final_output ?? snapshot.assistant_text
  snapshot.pending_permission = status.pending_permission ?? snapshot.pending_permission

  const clean = publicSnapshot(snapshot)
  return {
    run_id: status.run_id,
    label: status.label,
    status,
    snapshot: clean,
    transcript: clean.transcript,
    tools: clean.tools,
    live_tools: clean.live_tools,
    permissions: clean.permissions,
    pending_permission: clean.pending_permission,
    final_output: clean.final_output,
  }
}
