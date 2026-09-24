import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowReadyToolArgs {
  controllerId: string
  limit?: number
  supervisorId?: string
}

export type WorkflowReadyResult = Record<string, unknown>

export function createWorkflowReadyTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowReadyToolArgs) => Promise<WorkflowReadyResult> {
  return async args => {
    if (!args.controllerId) throw new Error('controllerId is required')
    return withWorkflowClient(args.supervisorId, client => client.workflowReady(args.controllerId, args.limit), getSupervisorClient)
  }
}

export const workflowReadyTool = createWorkflowReadyTool(realGetSupervisorClient)
