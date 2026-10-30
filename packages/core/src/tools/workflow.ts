import type { Client } from '../client.js'
import { executeWorkflow, workflowSupervisorId, type WorkflowProvider, type WorkflowResult } from '../workflow.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { withWorkflowClient, type GetSupervisorClient } from './workflow-client.js'

export { WorkflowToolError } from '../workflow.js'
export type { WorkflowAction, WorkflowInput, WorkflowProvider, WorkflowResult } from '../workflow.js'

/**
 * The one avenor_workflow handler: every surface routes through it, so the nine
 * actions and their failure text exist once. A router tool error is thrown as a
 * typed Error, which each surface reports as its own error result.
 */
export async function workflowTool(args: unknown, provider: WorkflowProvider): Promise<WorkflowResult> {
  return executeWorkflow(provider, args)
}

/** Bind the handler to one provider. */
export function createWorkflowTool(provider: WorkflowProvider): (args: unknown) => Promise<WorkflowResult> {
  return args => workflowTool(args, provider)
}

/** The provider over one supervisor client. */
export function clientWorkflowProvider(client: Client): WorkflowProvider {
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
 * Run `work` against the provider for the given supervisor, or the singleton
 * when none is supplied. External supervisors are dialed on demand and closed
 * again; the singleton client is reused.
 */
export async function withWorkflowProvider<T>(
  supervisorId: string | undefined,
  work: (provider: WorkflowProvider) => Promise<T>,
  getSupervisorClient: GetSupervisorClient = realGetSupervisorClient,
): Promise<T> {
  return withWorkflowClient(supervisorId, client => work(clientWorkflowProvider(client)), getSupervisorClient)
}

/**
 * The one entry point a surface binds: read the supervisor_id from the call,
 * acquire that supervisor's client, and route the call through the shared
 * handler.
 */
export async function runWorkflowTool(
  args: unknown,
  getSupervisorClient: GetSupervisorClient = realGetSupervisorClient,
): Promise<WorkflowResult> {
  return withWorkflowProvider(workflowSupervisorId(args), provider => workflowTool(args, provider), getSupervisorClient)
}
