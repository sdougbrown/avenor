// The TypeScript router for the merged avenor_workflow tool. It runs the same
// admission phase as internal/workflowtool: the action enum, the action's
// permitted and required field sets, and each field's declared wire type,
// enum, item type and limits. The applicability table, repair sentences and
// conformance cases come from schemas/, so both languages reject and render
// identical inputs identically.
import workflowInputSchema from '../../../schemas/workflow.input.json'
import workflowConformance from '../../../schemas/workflow_route.conformance.json'
import repairsBase from '../../../schemas/repairs.json'
import type { WorkflowCompleteParams, WorkflowGateParams, WorkflowGateOperation } from './client.js'
import { compileConstraints, validateFields, type ApplicabilityEntry } from './admission.js'
import { refused, ToolError } from './message.js'
import { quote, quoteList, substitute } from './messaging-text.js'
import { validateObservedAt, validateTimeout } from './tools/validate.js'

/** The nine workflow actions. The spellings are the schema's action enum. */
export type WorkflowAction =
  | 'status'
  | 'inspect'
  | 'events'
  | 'wait'
  | 'ready'
  | 'controller_status'
  | 'gate'
  | 'complete'
  | 'cancel'

/** One avenor_workflow call. Admission has already decided the action's field set. */
export interface WorkflowInput {
  action: WorkflowAction
  workflow_id?: string
  controller_id?: string
  supervisor_id?: string
  after_seq?: number
  limit?: number
  timeout?: string
  node_id?: string
  gate_id?: string
  activation_id?: string
  attempt_id?: string
  lease_id?: string
  owner_token?: string
  operation?: WorkflowGateOperation
  actor?: string
  reason?: string
  outcome?: string
  subject?: unknown
  poll_id?: string
  source?: string
  result?: string
  response_hash?: string
  observed_at?: string
  evidence_ids?: string[]
  outputs?: unknown
  artifacts?: unknown
}

/**
 * The control surface the router dispatches to. A surface binds it to the
 * supervisor client it acquired, so the router never reaches a transport.
 */
export interface WorkflowProvider {
  status(workflowId: string): Promise<Record<string, unknown>>
  wait(workflowId: string, timeoutMs: number): Promise<Record<string, unknown>>
  inspect(workflowId: string): Promise<Record<string, unknown>>
  events(workflowId: string, opts?: { afterSeq?: number; limit?: number }): Promise<Record<string, unknown>>
  complete(params: WorkflowCompleteParams): Promise<Record<string, unknown>>
  gate(params: WorkflowGateParams): Promise<Record<string, unknown>>
  cancel(workflowId: string, actor: string, reason: string): Promise<Record<string, unknown>>
  controllerStatus(controllerId: string): Promise<Record<string, unknown>>
  controllerList(): Promise<Record<string, unknown>>
  ready(controllerId: string, limit?: number): Promise<Record<string, unknown>>
}

/** One tool call's outcome: the action that ran and the control result, unchanged. */
export interface WorkflowResult {
  action: WorkflowAction
  value: Record<string, unknown>
}

/**
 * The workflow router raises the shared tool error, so one surface renderer
 * handles both routers. The alias keeps this module's spelling.
 */
export const WorkflowToolError = ToolError
export type WorkflowToolError = ToolError

const fixture = workflowConformance as unknown as {
  repairs: Record<string, string>
  applicability: Record<string, ApplicabilityEntry>
}

// The shared repair sentences (schemas/repairs.json) plus the tool's own codes
// and overrides - the fixture wins, exactly like the Go generator merges them.
const repairs = {
  ...(repairsBase as { repairs: Record<string, string> }).repairs,
  ...fixture.repairs,
}
const applicability = fixture.applicability

const constraints = compileConstraints(workflowInputSchema)

function repair(code: string, facts: Record<string, string> = {}): string {
  return substitute(repairs[code], facts)
}

function invalidAction(): WorkflowToolError {
  return new WorkflowToolError('invalid_action', repair('workflow_invalid_action'))
}

function invalidRequest(action: string, detail: string): WorkflowToolError {
  const facts = { action: quote(action), detail }
  return new WorkflowToolError('invalid_request', repair('invalid_request', facts), facts)
}

/**
 * Phase 1 for avenor_workflow: the action enum, the action's permitted and
 * required field sets, and each field's declared wire type, enum, item type and
 * limits. The table is the one the Go router validates against.
 */
export function assertWorkflowInput(input: unknown): asserts input is WorkflowInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw invalidAction()
  }
  const fields = input as Record<string, unknown>
  const action = fields.action
  if (typeof action !== 'string') throw invalidAction()
  const entry = applicability[action]
  if (!entry) throw invalidAction()
  const detail = validateFields(fields, entry, constraints, repair)
  if (detail) throw invalidRequest(action, detail)
}

/** The supervisor_id a call names, or undefined. The surface reads it before dispatch. */
export function workflowSupervisorId(input: unknown): string | undefined {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) return undefined
  const value = (input as Record<string, unknown>).supervisor_id
  return typeof value === 'string' ? value : undefined
}

/** Route one avenor_workflow call: admit it, then dispatch to the provider. */
export async function executeWorkflow(provider: WorkflowProvider, input: unknown): Promise<WorkflowResult> {
  assertWorkflowInput(input)
  const fields = input as WorkflowInput
  try {
    return await dispatchWorkflow(provider, fields)
  } catch (error) {
    // A transport failure reaches the model as the shared refusal sentence, the
    // same way the Go router wraps every client call. A router denial passes
    // through unchanged.
    throw refused(error, repair('refused_io'))
  }
}

async function dispatchWorkflow(provider: WorkflowProvider, fields: WorkflowInput): Promise<WorkflowResult> {
  switch (fields.action) {
    case 'status':
      return { action: 'status', value: await provider.status(fields.workflow_id!) }
    case 'inspect':
      return { action: 'inspect', value: await provider.inspect(fields.workflow_id!) }
    case 'events':
      return {
        action: 'events',
        value: await provider.events(fields.workflow_id!, { afterSeq: fields.after_seq, limit: fields.limit }),
      }
    case 'wait': {
      const timeoutMs = fields.timeout ? parseTimeout(fields.timeout) : 30_000
      return { action: 'wait', value: await provider.wait(fields.workflow_id!, timeoutMs) }
    }
    case 'ready':
      return { action: 'ready', value: await provider.ready(fields.controller_id!, fields.limit) }
    case 'controller_status': {
      const value = fields.controller_id
        ? await provider.controllerStatus(fields.controller_id)
        : await provider.controllerList()
      if (fields.controller_id) await attachCandidateCount(provider, fields.controller_id, value)
      return { action: 'controller_status', value }
    }
    case 'gate':
      return { action: 'gate', value: await provider.gate(gateParams(fields)) }
    case 'complete':
      return { action: 'complete', value: await provider.complete(completeParams(fields)) }
    case 'cancel':
      return { action: 'cancel', value: await provider.cancel(fields.workflow_id!, fields.actor!, fields.reason!) }
  }
  throw invalidAction()
}

/**
 * The largest whole-second wait that fits a signed 64-bit nanosecond duration,
 * the same bound the Go router applies. A larger request would overflow the
 * conversion and dispatch a negative budget.
 */
const MAX_WAIT_SECONDS = 9_223_372_036

/**
 * The controller-status view reports how many candidates are claimable. `ready`
 * is advisory: a supervisor without workflow support, or a failed query, leaves
 * the count absent and the status view renders without it. It runs on the same
 * acquired client, so a surface does not dial the supervisor twice.
 */
async function attachCandidateCount(provider: WorkflowProvider, controllerId: string, value: Record<string, unknown>): Promise<void> {
  try {
    const ready = await provider.ready(controllerId)
    if (Array.isArray(ready.candidates)) {
      value.candidate_count = ready.candidates.length
    }
  } catch {
    // Advisory only — the status view renders without it.
  }
}

function parseTimeout(value: string): number {
  let seconds: number
  try {
    seconds = validateTimeout(value)
  } catch {
    throw invalidRequest('wait', repair('invalid_timeout', { value: quote(value) }))
  }
  if (seconds > MAX_WAIT_SECONDS) {
    throw invalidRequest('wait', repair('invalid_timeout', { value: quote(value) }))
  }
  return seconds * 1000
}

function assertObservedAt(value: string): void {
  try {
    validateObservedAt(value)
  } catch {
    throw invalidRequest('gate', repair('invalid_observed_at', { name: quote('observed_at') }))
  }
}

function gateParams(fields: WorkflowInput): WorkflowGateParams {
  if (fields.observed_at) assertObservedAt(fields.observed_at)
  const params: WorkflowGateParams = {
    workflow_id: fields.workflow_id!,
    node_id: fields.node_id!,
    gate_id: fields.gate_id!,
    activation_id: fields.activation_id!,
    operation: fields.operation!,
  }
  // The Go router omits a field whose value is the empty string, so an empty
  // string must not reach the wire here either.
  if (fields.actor !== undefined && fields.actor !== '') params.actor = fields.actor
  if (fields.reason !== undefined && fields.reason !== '') params.reason = fields.reason
  if (fields.outcome !== undefined && fields.outcome !== '') params.outcome = fields.outcome
  if (fields.subject !== undefined) params.subject = fields.subject
  if (fields.poll_id !== undefined && fields.poll_id !== '') params.poll_id = fields.poll_id
  if (fields.source !== undefined && fields.source !== '') params.source = fields.source
  if (fields.result !== undefined && fields.result !== '') params.result = fields.result
  if (fields.response_hash !== undefined && fields.response_hash !== '') params.response_hash = fields.response_hash
  if (fields.observed_at !== undefined && fields.observed_at !== '') params.observed_at = fields.observed_at
  if (fields.evidence_ids !== undefined) params.evidence_ids = fields.evidence_ids
  return params
}

function completeParams(fields: WorkflowInput): WorkflowCompleteParams {
  const params: WorkflowCompleteParams = {
    workflow_id: fields.workflow_id!,
    node_id: fields.node_id!,
    activation_id: fields.activation_id!,
    attempt_id: fields.attempt_id!,
    lease_id: fields.lease_id!,
    owner_token: fields.owner_token!,
    outcome: fields.outcome!,
  }
  if (fields.outputs !== undefined) params.outputs = fields.outputs
  if (fields.artifacts !== undefined) params.artifacts = fields.artifacts
  return params
}
