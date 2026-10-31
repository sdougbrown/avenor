// The TypeScript router for the merged avenor_workflow tool. It runs the same
// admission phase as internal/workflowtool: the action enum, the action's
// permitted and required field sets, and each field's declared wire type,
// enum, item type and limits. The applicability table, repair sentences and
// conformance cases come from schemas/, so both languages reject and render
// identical inputs identically.
import workflowInputSchema from '../../../schemas/workflow.input.json'
import workflowConformance from '../../../schemas/workflow_route.conformance.json'
import type { WorkflowCompleteParams, WorkflowGateParams, WorkflowGateOperation } from './client.js'
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

interface ApplicabilityEntry {
  permitted: string[]
  required?: string[]
  types?: Record<string, string>
}

interface StringConstraints {
  minLength?: number
  maxLength?: number
  pattern?: RegExp
}

interface IntegerConstraints {
  minimum?: number
  maximum?: number
}

interface ArrayConstraints {
  itemType?: string
}

interface Constraints {
  strings: Record<string, StringConstraints>
  integers: Record<string, IntegerConstraints>
  arrays: Record<string, ArrayConstraints>
  enums: Record<string, string[]>
}

const fixture = workflowConformance as unknown as {
  repairs: Record<string, string>
  applicability: Record<string, ApplicabilityEntry>
}

const repairs = fixture.repairs
const applicability = fixture.applicability

function repair(code: string, facts: Record<string, string> = {}): string {
  return substitute(repairs[code], facts)
}

/**
 * Compile the per-property limits the published JSON schema declares, so the
 * router and the schema cannot disagree about length, characters, bounds,
 * enums or array item types.
 */
function schemaConstraints(schema: unknown): Constraints {
  const strings: Record<string, StringConstraints> = {}
  const integers: Record<string, IntegerConstraints> = {}
  const arrays: Record<string, ArrayConstraints> = {}
  const enums: Record<string, string[]> = {}
  const variants = (schema as { oneOf?: Array<{ properties?: Record<string, Record<string, unknown>> }> }).oneOf ?? []
  for (const variant of variants) {
    for (const [name, property] of Object.entries(variant.properties ?? {})) {
      if (Array.isArray(property.enum)) {
        enums[name] = property.enum.filter((value): value is string => typeof value === 'string')
      }
      if (property.type === 'array') {
        const items = property.items as { type?: unknown } | undefined
        arrays[name] = { itemType: typeof items?.type === 'string' ? items.type : undefined }
      }
      if (property.type === 'string' && (typeof property.minLength === 'number' || typeof property.maxLength === 'number' || typeof property.pattern === 'string')) {
        strings[name] = {
          minLength: typeof property.minLength === 'number' ? property.minLength : undefined,
          maxLength: typeof property.maxLength === 'number' ? property.maxLength : undefined,
          pattern: typeof property.pattern === 'string' ? new RegExp(property.pattern, 'u') : undefined,
        }
      }
      if (property.type === 'integer' && (typeof property.minimum === 'number' || typeof property.maximum === 'number')) {
        integers[name] = {
          minimum: typeof property.minimum === 'number' ? property.minimum : undefined,
          maximum: typeof property.maximum === 'number' ? property.maximum : undefined,
        }
      }
    }
  }
  return { strings, integers, arrays, enums }
}

const constraints = schemaConstraints(workflowInputSchema)

function hasWireType(value: unknown, kind: string): boolean {
  switch (kind) {
    case 'json':
      return true
    case 'string':
      return typeof value === 'string'
    case 'integer':
      return typeof value === 'number' && Number.isInteger(value)
    case 'object':
      return value !== null && typeof value === 'object' && !Array.isArray(value)
    case 'array':
      return Array.isArray(value)
    default:
      return false
  }
}

function wrongTypeRepair(kind: string, name: string): string {
  switch (kind) {
    case 'integer':
      return repair('wrong_type_integer', { name: quote(name) })
    case 'object':
      return repair('wrong_type_object', { name: quote(name) })
    case 'array':
      return repair('wrong_type_array', { name: quote(name) })
    default:
      return repair('wrong_type_string', { name: quote(name) })
  }
}

function validateFields(fields: Record<string, unknown>, entry: ApplicabilityEntry): string | null {
  const permitted = new Set(entry.permitted)
  for (const name of Object.keys(fields).sort()) {
    if (!permitted.has(name)) return repair('unknown_property', { name: quote(name) })
  }
  for (const name of entry.required ?? []) {
    if (!(name in fields)) return repair('missing_required_property', { name: quote(name) })
  }
  for (const name of entry.permitted) {
    if (!(name in fields)) continue
    const value = fields[name]
    const kind = entry.types?.[name] ?? 'string'
    if (!hasWireType(value, kind)) return wrongTypeRepair(kind, name)
    if (kind === 'array') {
      const constraint = constraints.arrays[name]
      if (constraint?.itemType) {
        for (const item of value as unknown[]) {
          if (!hasWireType(item, constraint.itemType)) {
            return repair('array_item_type', { name: quote(name) })
          }
        }
      }
    }
    const enumValues = constraints.enums[name]
    if (enumValues && !enumValues.includes(value as string)) {
      return repair('invalid_enum', { name: quote(name), values: quoteList(enumValues) })
    }
    if (kind === 'string') {
      const constraint = constraints.strings[name]
      if (!constraint) continue
      const length = Array.from(value as string).length
      if (constraint.minLength !== undefined && length < constraint.minLength) {
        return repair('string_too_short', { name: quote(name), min: String(constraint.minLength) })
      }
      if (constraint.maxLength !== undefined && length > constraint.maxLength) {
        return repair('string_too_long', { name: quote(name), max: String(constraint.maxLength) })
      }
      if (constraint.pattern && !constraint.pattern.test(value as string)) {
        return repair('string_pattern', { name: quote(name) })
      }
    }
    if (kind === 'integer') {
      const constraint = constraints.integers[name]
      // A value beyond the int64 wire range names the declared bound, or the
      // int64 limit where the schema declares none — the sentence the Go router
      // renders for the same input. A bound at the int64 limit does not survive
      // JSON.parse exactly, so the limit is named literally there.
      const number = value as number
      // JSON.parse rounds the maximum valid int64 up to 2**63, so only a value
      // strictly beyond it is out of range.
      if (number > 2 ** 63 || number < -(2 ** 63)) {
        const bound = number > 2 ** 63 ? 'integer_too_large' : 'integer_too_small'
        const declaredMax = constraint?.maximum
        const declaredMin = constraint?.minimum
        return repair(bound, {
          name: quote(name),
          max: declaredMax !== undefined && Number.isSafeInteger(declaredMax) ? String(declaredMax) : '9223372036854775807',
          min: declaredMin !== undefined && Number.isSafeInteger(declaredMin) ? String(declaredMin) : '-9223372036854775808',
        })
      }
      if (!constraint) continue
      if (constraint.minimum !== undefined && (value as number) < constraint.minimum) {
        return repair('integer_too_small', { name: quote(name), min: String(constraint.minimum) })
      }
      if (constraint.maximum !== undefined && (value as number) > constraint.maximum) {
        return repair('integer_too_large', { name: quote(name), max: String(constraint.maximum) })
      }
    }
  }
  return null
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
  const detail = validateFields(fields, entry)
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
    case 'controller_status':
      return fields.controller_id
        ? { action: 'controller_status', value: await provider.controllerStatus(fields.controller_id) }
        : { action: 'controller_status', value: await provider.controllerList() }
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
