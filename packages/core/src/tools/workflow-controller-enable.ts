import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowControllerEnableToolArgs {
  controllerId: string
  supervisorId?: string
}

export type WorkflowControllerEnableResult = Record<string, unknown>

export function createWorkflowControllerEnableTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowControllerEnableToolArgs) => Promise<WorkflowControllerEnableResult> {
  return async args => {
    if (!args.controllerId) throw new Error('controllerId is required')
    return withWorkflowClient(args.supervisorId, client => client.workflowControllerEnable(args.controllerId), getSupervisorClient)
  }
}

export const workflowControllerEnableTool = createWorkflowControllerEnableTool(realGetSupervisorClient)
