import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowCancelArgs {
  workflow_id: string
  actor: string
  reason: string
  supervisorId?: string
}

export type WorkflowCancelResult = Record<string, unknown>

/**
 * Cancel a workflow. The manager validates the actor before the reason, so the
 * handler checks them in the same order and forwards all three unchanged.
 */
export function createWorkflowCancelTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowCancelArgs) => Promise<WorkflowCancelResult> {
  return async args => {
    if (!args.workflow_id) throw new Error('workflow_id is required')
    if (!args.actor) throw new Error('actor is required')
    if (!args.reason) throw new Error('reason is required')
    return withWorkflowClient(
      args.supervisorId,
      client => client.workflowCancel(args.workflow_id, args.actor, args.reason),
      getSupervisorClient,
    )
  }
}

export const workflowCancelTool = createWorkflowCancelTool(realGetSupervisorClient)
