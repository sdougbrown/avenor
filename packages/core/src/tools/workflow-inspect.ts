import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowInspectToolArgs {
  workflowId: string
  supervisorId?: string
}

export type WorkflowInspectResult = Record<string, unknown>

export function createWorkflowInspectTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowInspectToolArgs) => Promise<WorkflowInspectResult> {
  return async args => {
    if (!args.workflowId) throw new Error('workflowId is required')
    return withWorkflowClient(args.supervisorId, client => client.workflowInspect(args.workflowId), getSupervisorClient)
  }
}

export const workflowInspectTool = createWorkflowInspectTool(realGetSupervisorClient)
