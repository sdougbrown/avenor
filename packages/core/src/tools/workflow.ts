import type { Client } from '../client.js'
import { executeWorkflow, workflowSupervisorId, type WorkflowProvider, type WorkflowResult } from '../workflow.js'
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
  return withWorkflowClient(
    workflowSupervisorId(args),
    client => executeWorkflow(clientWorkflowProvider(client), args),
    getSupervisorClient,
  )
}
