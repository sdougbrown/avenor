import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowControllerStatusToolArgs {
  controllerId?: string
  supervisorId?: string
}

export type WorkflowControllerStatusResult = Record<string, unknown>

export function createWorkflowControllerStatusTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowControllerStatusToolArgs) => Promise<WorkflowControllerStatusResult> {
  return async args => {
    if (!args.controllerId) {
      return withWorkflowClient(args.supervisorId, client => client.workflowControllerList(), getSupervisorClient)
    }
    return withWorkflowClient(args.supervisorId, client => client.workflowControllerStatus(args.controllerId!), getSupervisorClient)
  }
}

export const workflowControllerStatusTool = createWorkflowControllerStatusTool(realGetSupervisorClient)
