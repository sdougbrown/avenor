import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowControllerDisableToolArgs {
  controllerId: string
  reason: string
  supervisorId?: string
}

export type WorkflowControllerDisableResult = Record<string, unknown>

export function createWorkflowControllerDisableTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowControllerDisableToolArgs) => Promise<WorkflowControllerDisableResult> {
  return async args => {
    if (!args.controllerId) throw new Error('controllerId is required')
    if (!args.reason) throw new Error('reason is required')
    return withWorkflowClient(args.supervisorId, client => client.workflowControllerDisable(args.controllerId, args.reason), getSupervisorClient)
  }
}

export const workflowControllerDisableTool = createWorkflowControllerDisableTool(realGetSupervisorClient)
