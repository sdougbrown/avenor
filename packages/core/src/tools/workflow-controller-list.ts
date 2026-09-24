import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowControllerListToolArgs {
  supervisorId?: string
}

export type WorkflowControllerListResult = Record<string, unknown>

export function createWorkflowControllerListTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowControllerListToolArgs) => Promise<WorkflowControllerListResult> {
  return async args => {
    return withWorkflowClient(args.supervisorId, client => client.workflowControllerList(), getSupervisorClient)
  }
}

export const workflowControllerListTool = createWorkflowControllerListTool(realGetSupervisorClient)
