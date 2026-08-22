import type { GetSupervisorClient } from './workflow-client.js'
import { withWorkflowClient } from './workflow-client.js'
import type { WorkflowGateParams } from '../client.js'
import { type Client } from '../client.js'
import { validateObservedAt } from './validate.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export interface WorkflowGateToolArgs extends WorkflowGateParams {
  supervisorId?: string
}

export type WorkflowGateResult = Record<string, unknown>

export function createWorkflowGateTool(
  getSupervisorClient: GetSupervisorClient,
): (args: WorkflowGateToolArgs) => Promise<WorkflowGateResult> {
  return async args => {
    if (!args.workflow_id) throw new Error('workflow_id is required')
    if (!args.node_id) throw new Error('node_id is required')
    if (!args.gate_id) throw new Error('gate_id is required')
    if (!args.activation_id) throw new Error('activation_id is required')
    if (!args.operation) throw new Error('operation is required')
    if (!['satisfy', 'reject', 'waive', 'external_result'].includes(args.operation)) {
      throw new Error(`unknown gate operation "${args.operation}" (allowed: satisfy, reject, waive, external_result)`)
    }
    if (args.observed_at) validateObservedAt(args.observed_at)

    return withWorkflowClient(args.supervisorId, client => client.workflowGate(args), getSupervisorClient)
  }
}

export const workflowGateTool = createWorkflowGateTool(realGetSupervisorClient)
