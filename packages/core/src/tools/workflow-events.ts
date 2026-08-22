import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowEventsToolArgs {
  workflowId: string
  afterSeq?: number
  limit?: number
  supervisorId?: string
}

export type WorkflowEventsResult = Record<string, unknown>

export function createWorkflowEventsTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowEventsToolArgs) => Promise<WorkflowEventsResult> {
  return async args => {
    if (!args.workflowId) throw new Error('workflowId is required')
    return withWorkflowClient(args.supervisorId, client => client.workflowEvents(args.workflowId, { afterSeq: args.afterSeq, limit: args.limit }), getSupervisorClient)
  }
}

export const workflowEventsTool = createWorkflowEventsTool(realGetSupervisorClient)
