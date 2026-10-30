import type { Client } from '../client.js'
import { assertWorkflowInput, executeWorkflow, workflowSupervisorId, type WorkflowProvider, type WorkflowResult } from '../workflow.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { withWorkflowClient, type GetSupervisorClient } from './workflow-client.js'

export { WorkflowToolError } from '../workflow.js'
export type { WorkflowAction, WorkflowInput, WorkflowProvider, WorkflowResult } from '../workflow.js'

/** The provider over one supervisor client. */
function clientWorkflowProvider(client: Client): WorkflowProvider {
  return {
    status: workflowId => client.workflowStatus(workflowId),
    wait: (workflowId, timeoutMs) => client.workflowWait(workflowId, timeoutMs),
    inspect: workflowId => client.workflowInspect(workflowId),
    events: (workflowId, opts) => client.workflowEvents(workflowId, opts),
    complete: params => client.workflowComplete(params),
    gate: params => client.workflowGate(params),
    cancel: (workflowId, actor, reason) => client.workflowCancel(workflowId, actor, reason),
    controllerStatus: controllerId => client.workflowControllerStatus(controllerId),
    controllerList: () => client.workflowControllerList(),
    ready: (controllerId, limit) => client.workflowReady(controllerId, limit),
  }
}

/**
 * The one entry point a surface binds: read the supervisor_id from the call,
 * acquire that supervisor's client, and route the call through the shared
 * router. External supervisors are dialed on demand and closed again; the
 * singleton client is reused. A router tool error is thrown as a typed Error,
 * which each surface reports as its own error result.
 */
export async function runWorkflowTool(
  args: unknown,
  getSupervisorClient: GetSupervisorClient = realGetSupervisorClient,
): Promise<WorkflowResult> {
  // Admit before acquiring a client, the way the Go surface does: a malformed
  // call must answer with the repairable sentence, not a dial error for a
  // supervisor it should never have reached.
  assertWorkflowInput(args)
  return withWorkflowClient(
    workflowSupervisorId(args),
    client => executeWorkflow(clientWorkflowProvider(client), args),
    getSupervisorClient,
  )
}

/**
 * The controller-status view reports how many candidates are claimable. `ready`
 * is advisory: a supervisor without workflow support, or a failed query, leaves
 * the count absent and the status view renders without it.
 */
export async function withCandidateCount(
  run: (args: unknown) => Promise<WorkflowResult>,
  args: Record<string, unknown>,
  value: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  if (args.action !== 'controller_status' || typeof args.controller_id !== 'string' || !args.controller_id) {
    return value
  }
  // Only carry supervisor_id when the caller set one: a present-but-undefined
  // key is a wire-type violation the router refuses, and the advisory catch
  // would then swallow it and never attach the count.
  const readyArgs: Record<string, unknown> = { action: 'ready', controller_id: args.controller_id }
  if (typeof args.supervisor_id === 'string') readyArgs.supervisor_id = args.supervisor_id
  try {
    const ready = await run(readyArgs)
    if (Array.isArray(ready.value.candidates)) {
      return { ...value, candidate_count: ready.value.candidates.length }
    }
  } catch {
    // Advisory only — the status view renders without it.
  }
  return value
}
