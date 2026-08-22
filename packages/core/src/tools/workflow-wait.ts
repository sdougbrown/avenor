import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import { validateTimeout } from './validate.js'
import { type Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowWaitToolArgs {
  workflowId: string
  timeout?: string
  supervisorId?: string
}

export type WorkflowWaitResult = Record<string, unknown>

export function createWorkflowWaitTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowWaitToolArgs) => Promise<WorkflowWaitResult> {
  return async args => {
    if (!args.workflowId) throw new Error('workflowId is required')
    const timeoutMs = args.timeout ? validateTimeout(args.timeout) * 1000 : 30_000
    return withWorkflowClient(args.supervisorId, client => client.workflowWait(args.workflowId, timeoutMs), getSupervisorClient)
  }
}

export const workflowWaitTool = createWorkflowWaitTool(realGetSupervisorClient)
