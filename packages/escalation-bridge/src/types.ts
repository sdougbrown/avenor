/** Shape of a gate definition as declared in a workflow template JSON. */
export interface GateDefinition {
  id: string
  name?: string
  type?: string
  required?: boolean
  subject_type?: string
  allowed_outcomes?: string[]
  [key: string]: unknown
}

/** A workflow activation as exposed by `workflow.inspect`. */
export interface Activation {
  activation_id: string
  node_id: string
  status: string
  selected_outcome?: string
  [key: string]: unknown
}

/** A recorded gate instance as exposed by `workflow.inspect`. */
export interface GateInstance {
  activation_id?: string
  gate_id?: string
  status?: string
  [key: string]: unknown
}

/** A workflow output value as exposed by `workflow.inspect`. */
export interface OutputEntry {
  definition_id?: string
  revision?: number
  value?: string | null
  [key: string]: unknown
}

/** The flat detail payload returned by `workflow.inspect`. */
export interface WorkflowDetail {
  instance?: { status?: string; terminal_outcome?: string; [key: string]: unknown }
  activations?: Activation[] | null
  gates?: GateInstance[] | null
  outputs?: OutputEntry[] | null
  [key: string]: unknown
}

/** The subject carried by a decision when the gate declares a subject_type. */
export interface DecisionSubject {
  type: string
  [key: string]: unknown
}

/** A human decision as written by the transport into a decision file. */
export interface Decision {
  decision: string
  actor: string
  reason: string
  evidence?: string
  subject?: DecisionSubject
  outcome?: string
  [key: string]: unknown
}

/** The `op: "gate"` command payload submitted through `workflow.command`. */
export interface GateCommand {
  op: 'gate'
  node_id: string
  activation_id: string
  gate_id: string
  operation: 'satisfy' | 'reject'
  actor: string
  reason: string
  source: string
  response_hash: string
  evidence_ids: string[]
  subject?: DecisionSubject
  outcome?: string
}

/**
 * Minimal subset of `Client` from @dougbots/avenor-core that the bridge
 * needs. Structural so tests can stub it and hosts can supply any
 * compatible control-socket client.
 */
export interface ControlClient {
  call(method: string, params?: unknown): Promise<unknown>
  isClosed(): boolean
  close(): void
}
