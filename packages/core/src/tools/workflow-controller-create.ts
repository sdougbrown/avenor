import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowControllerCreateToolArgs {
  controllerId: string
  maxInflight: number
  supervisorId?: string
}

export type WorkflowControllerCreateResult = Record<string, unknown>

export function createWorkflowControllerCreateTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowControllerCreateToolArgs) => Promise<WorkflowControllerCreateResult> {
  return async args => {
    if (!args.controllerId) throw new Error('controllerId is required')
    if (!Number.isInteger(args.maxInflight) || args.maxInflight <= 0) {
      throw new Error('maxInflight must be a positive integer')
    }
    return withWorkflowClient(
      args.supervisorId,
      client => client.workflowControllerCreate({ controller_id: args.controllerId, max_inflight: args.maxInflight }),
      getSupervisorClient,
    )
  }
}

export const workflowControllerCreateTool = createWorkflowControllerCreateTool(realGetSupervisorClient)
