import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowStatusToolArgs {
  workflowId: string
  supervisorId?: string
}

export type WorkflowStatusResult = Record<string, unknown>

export function createWorkflowStatusTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowStatusToolArgs) => Promise<WorkflowStatusResult> {
  return async args => {
    if (!args.workflowId) throw new Error('workflowId is required')
    return withWorkflowClient(args.supervisorId, client => client.workflowStatus(args.workflowId), getSupervisorClient)
  }
}

export const workflowStatusTool = createWorkflowStatusTool(realGetSupervisorClient)
