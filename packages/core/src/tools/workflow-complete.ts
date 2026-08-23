import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import type { WorkflowCompleteParams } from '../client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowCompleteToolArgs extends WorkflowCompleteParams {
  supervisorId?: string
}

export type WorkflowCompleteResult = Record<string, unknown>

export function createWorkflowCompleteTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowCompleteToolArgs) => Promise<WorkflowCompleteResult> {
  return async args => {
    if (!args.workflow_id) throw new Error('workflow_id is required')
    if (!args.node_id) throw new Error('node_id is required')
    if (!args.activation_id) throw new Error('activation_id is required')
    if (!args.attempt_id) throw new Error('attempt_id is required')
    if (!args.lease_id) throw new Error('lease_id is required')
    if (!args.owner_token) throw new Error('owner_token is required')
    if (!args.outcome) throw new Error('outcome is required')

    return withWorkflowClient(args.supervisorId, client => client.workflowComplete(args), getSupervisorClient)
  }
}

export const workflowCompleteTool = createWorkflowCompleteTool(realGetSupervisorClient)
