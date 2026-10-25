import * as net from 'node:net'
import * as readline from 'node:readline'

import type { ThinkingLevel } from './thinking-policy.js'

export interface JsonRpcRequest {
  jsonrpc: '2.0'
  id?: number
  method: string
  params?: unknown
}

export interface JsonRpcResponse {
  jsonrpc: '2.0'
  id: number
  result?: unknown
  error?: { code: number; message: string; data?: unknown }
}

export interface Event {
  event: string
  session_id?: string
  runtime_id?: string
  [key: string]: unknown
}

export interface SubscribeOptions {
  runtime_id?: string
  replay?: boolean
  limit?: number
  after_seq?: number
}

export interface HistoryOptions {
  runtime_id: string
  limit?: number
  after_seq?: number
}

export interface HistoryResult<TEvent = Event> {
  runtime_id?: string
  events: TEvent[]
  latest_seq?: number
}

export type { ThinkingLevel }

export interface SpawnParams {
  agent?: string
  prompt_file?: string
  prompt?: string
  dir?: string
  label?: string
  timeout?: number
  model?: string
  thinking?: ThinkingLevel
  agent_profile?: string
  backend?: string
  roster_file?: string
  roster_entry?: string
  server_url?: string
  session_id?: string
  sentinel_file?: string
  workflow_id?: string
  node_id?: string
  activation_id?: string
  attempt_id?: string
  on_event?: string
  auto_approve?: boolean
  [key: string]: unknown
}

export interface SpawnResult {
  runtime_id?: string
  session_id?: string
  on_event?: string
  sentinel_file?: string
  broker_url?: string
  parent_token?: string
  agent?: string
  agent_profile?: string
  model?: string
  backend?: string
  roster_file?: string
  roster_entry?: string
  effective_agent?: string
  effective_model?: string
  effective_backend?: string
  [key: string]: unknown
}

export interface ExecutionIdentity {
  supervisor_id?: string
  workflow_id?: string
  node_id?: string
  activation_id?: string
  attempt_id?: string
  run_id?: string
  runtime_id?: string
  session_id?: string
}

export type WorkflowGateOperation = 'satisfy' | 'reject' | 'waive' | 'external_result'

export interface WorkflowCompleteParams {
  workflow_id: string
  node_id: string
  activation_id: string
  attempt_id: string
  lease_id: string
  owner_token: string
  outcome: string
  outputs?: unknown
  artifacts?: unknown
}

export interface WorkflowGateParams {
  workflow_id: string
  node_id: string
  gate_id: string
  activation_id: string
  operation: WorkflowGateOperation
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
}

export interface WorkflowHeartbeatParams {
  workflow_id: string
  node_id: string
  activation_id?: string
  lease_id: string
  owner_token: string
}

export interface WorkflowControllerCreateParams {
  controller_id: string
  max_inflight: number
}

/** The shared wait window a zero message timeout means, in milliseconds. */
export const MESSAGE_MAX_WAIT_MS = 600_000

export interface MessageSendParams {
  to_run_id: string
  message_id: string
  generation: string
  body: string
  no_reply?: boolean
}

export interface MessageSendResult {
  outcome: string
  message_id?: string
  generation?: string
}

export interface MessageWaitParams {
  message_id: string
  generation: string
  timeout_ms?: number
}

export interface MessageInbound {
  message_id: string
  from_run_id: string
  sender?: string
  kind?: string
  payload?: unknown
}

export interface MessageWaitResult {
  outcome: string
  message_id?: string
  reply?: MessageInbound
}

export interface MessageWithdrawParams {
  message_id: string
  generation: string
}

export interface MessageFence {
  sender: string
  message_id: string
  generation: string
  deadline: number
  terminal: boolean
}

export interface MessageWithdrawResult {
  outcome: string
  fence?: MessageFence
}

export interface MessageReceiveParams {
  generation: string
  timeout_ms?: number
  limit?: number
}

export interface MessageReceiveResult {
  messages: MessageInbound[]
  generation?: string
}

export interface MessagePeersParams {
  generation: string
}

export interface MessagePeer {
  run_id: string
  session?: unknown
}

export interface MessagePeersResult {
  peers: MessagePeer[]
  generation?: string
}

export interface MessagePreflightResult {
  protocol: number
  generation: string
}

export interface ResumeParams {
  run_id: string
  target: string
  message: string
  label?: string
  idempotency_key?: string
  sentinel_file?: string
  on_event?: string
}

export interface ResumeResult {
  run_id: string
  label: string
  runtime_id: string
  session_id: string
  sentinel_file: string
  on_event: string
}

export interface SessionRenameResult {
  run_id: string
  label: string
  previous_label: string
}

export interface SessionLastResult {
  run_id: string
  label?: string
  has_record: boolean
  final_output: string
  stop_reason?: string
  final_output_truncated?: boolean
  at?: number
}

type PendingCall = {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout>
}

type EventResolver = {
  resolve: (value: IteratorResult<Event>) => void
  active: boolean
}

function normalizeSubscribeOptions(options?: SubscribeOptions): SubscribeOptions | undefined {
  if (!options) return undefined
  const normalized: SubscribeOptions = {}
  if (options.runtime_id) normalized.runtime_id = options.runtime_id
  if (options.replay !== undefined) normalized.replay = options.replay
  if (options.limit !== undefined) {
    normalized.limit = Math.max(1, Math.min(256, Math.trunc(options.limit)))
  }
  if (options.after_seq !== undefined) {
    normalized.after_seq = Math.max(0, Math.trunc(options.after_seq))
  }
  return Object.keys(normalized).length > 0 ? normalized : undefined
}

function subscribeOptionsKey(options?: SubscribeOptions): string {
  return JSON.stringify(normalizeSubscribeOptions(options) ?? {})
}

function removeResolver(resolvers: EventResolver[], target: EventResolver): void {
  const index = resolvers.indexOf(target)
  if (index >= 0) {
    resolvers.splice(index, 1)
  }
}

export class RpcError extends Error {
  readonly code: number
  readonly data?: unknown

  constructor(code: number, message: string, data?: unknown) {
    super(`rpc error [${code}]: ${message}`)
    this.name = 'RpcError'
    this.code = code
    this.data = data
  }
}

/**
 * A call or connect that produced no result inside the caller's timeout. It is
 * a transport outcome, not a peer's structured rejection.
 */
export class CallTimeoutError extends Error {
  readonly code = 'ETIMEDOUT'

  constructor(message = 'read response: timeout') {
    super(message)
    this.name = 'CallTimeoutError'
  }
}

/**
 * A transport failure that marks the control client closed: a send refused
 * because the socket is already closed, a pending call rejected because the
 * read side closed, or a socket write failure. A peer's structured rejection
 * (RpcError) or a call timeout do not close the client and are not classified
 * here.
 */
export class ClosedSocketError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ClosedSocketError'
  }
}

/** True when the error is a transport failure that closed the client. */
export function isClosedSocketError(error: unknown): boolean {
  return error instanceof ClosedSocketError
}

const OWNER_AUTH_VERSION = 1
export const OWNER_UNAUTHENTICATED_CODE = -32011
export const UNSUPPORTED_OWNER_AUTH_CODE = -32012
export const OWNER_AUTH_TIMEOUT_CODE = -32013

export type OwnerAuthFailureKind = 'unauthenticated' | 'unsupported' | 'timeout'

/** A classified owner-authentication rejection, not generic transport failure. */
export class OwnerAuthError extends Error {
  constructor(
    readonly kind: OwnerAuthFailureKind,
    options: { code?: number, reason?: string } = {},
  ) {
    const label = kind === 'unauthenticated'
      ? 'owner authentication rejected'
      : kind === 'unsupported'
        ? 'unsupported owner authentication'
        : 'owner authentication timed out'
    super(options.reason ? `${label}: ${options.reason}` : label)
    this.name = 'OwnerAuthError'
  }
}

/**
 * Map authenticate_owner failures onto the three client-side classes. A peer
 * that never answered the handshake call timed the capability out, which is an
 * authentication verdict; a dial that never connected rejects before this
 * mapping runs.
 */
function classifyOwnerAuthError(err: unknown): unknown {
  if (err instanceof CallTimeoutError) {
    return new OwnerAuthError('timeout')
  }
  if (err instanceof RpcError) {
    const reason = ownerAuthReason(err.data)
    switch (err.code) {
      case OWNER_UNAUTHENTICATED_CODE:
        return new OwnerAuthError('unauthenticated', { code: err.code, reason })
      case UNSUPPORTED_OWNER_AUTH_CODE:
        return new OwnerAuthError('unsupported', { code: err.code, reason })
      case OWNER_AUTH_TIMEOUT_CODE:
        return new OwnerAuthError('timeout', { code: err.code, reason })
      default:
        return new OwnerAuthError('unsupported', { code: err.code, reason })
    }
  }
  return err
}

function ownerAuthReason(data: unknown): string | undefined {
  if (typeof data !== 'object' || data === null) return undefined
  const reason = (data as Record<string, unknown>).reason
  return typeof reason === 'string' ? reason : undefined
}

export class Client {
  private socket: net.Socket
  private rl: readline.Interface
  private closed = false
  private nextID = 0
  private pending = new Map<number, PendingCall>()
  private started = false
  private callTimeout: number

  private eventQueue: Event[] = []
  private eventResolvers: EventResolver[] = []
  private eventDone = false
  private dropped = 0
  private subscribed = false
  private subscriptionKey = subscribeOptionsKey()

  constructor(
    socket: net.Socket,
    opts?: { callTimeoutMs?: number },
  ) {
    this.socket = socket
    this.rl = readline.createInterface({ input: socket, crlfDelay: Infinity })
    this.callTimeout = opts?.callTimeoutMs ?? 30_000
    this.socket.on('error', (err: Error) => {
      this.closed = true
      this.pushEvent({ event: 'protocol-error', message: err.message })
    })
    this.socket.on('close', () => {
      this.closed = true
    })
  }

  private startReadLoop(): void {
    if (this.started) return
    this.started = true

    this.rl.on('line', (line: string) => {
      let parsed: any
      try {
        parsed = JSON.parse(line)
      } catch {
        return
      }

      if (parsed.id !== undefined && parsed.id !== null) {
        const id = Number(parsed.id)
        const pc = this.pending.get(id)
        if (pc) {
          clearTimeout(pc.timer)
          this.pending.delete(id)
          if (parsed.error) {
            pc.reject(
              new RpcError(parsed.error.code, parsed.error.message, parsed.error.data),
            )
          } else {
            pc.resolve(parsed.result)
          }
        }
        return
      }

      if (parsed.method === 'event' && parsed.params) {
        const ev: Event = { ...parsed.params }
        if (this.dropped > 0) {
          const lag = this.dropped
          this.dropped = 0
          this.pushEvent({ event: 'client.lagged', dropped_count: lag })
        }
        this.pushEvent(ev)
      }
    })

    this.rl.on('close', () => {
      this.closed = true
      for (const [, pc] of this.pending) {
        clearTimeout(pc.timer)
        pc.reject(new ClosedSocketError('read response: connection closed'))
      }
      this.pending.clear()
      this.eventDone = true
      for (const resolver of this.eventResolvers) {
        resolver.active = false
        resolver.resolve({ value: undefined, done: true })
      }
      this.eventResolvers = []
    })
  }

  private pushEvent(ev: Event): void {
    while (this.eventResolvers.length > 0) {
      const resolver = this.eventResolvers.shift()!
      if (!resolver.active) continue
      resolver.active = false
      resolver.resolve({ value: ev, done: false })
      return
    }

    if (this.eventQueue.length < 256) {
      this.eventQueue.push(ev)
    } else {
      this.dropped++
    }
  }

  async call(method: string, params?: unknown, opts?: { timeoutMs?: number }): Promise<unknown> {
    // A per-call timeout_ms bounds the server-side wait; the client must not
    // fire first. The +10s margin lets the server's structured ask-error
    // response (written after bounded cleanup) arrive before the timer fires.
    // Node's setTimeout clamps values > 2^31-1 to 1ms, so we must keep the
    // effective delay within range.
    const MAX_TIMER_MS = 2_147_483_647 // 2^31 - 1
    const requested =
      opts?.timeoutMs !== undefined
        ? (!Number.isFinite(opts.timeoutMs) || opts.timeoutMs > MAX_TIMER_MS - 10_000
            ? MAX_TIMER_MS
            : Math.max(this.callTimeout, opts.timeoutMs + 10_000))
        : this.callTimeout
    const timeout = Number.isFinite(requested) ? Math.min(requested, MAX_TIMER_MS) : MAX_TIMER_MS
    return this.sendCall(method, params, timeout)
  }

  /**
   * Send a credential-free identity probe. The control server returns the
   * startup token only after it has published its own owner record.
   */
  async identity(timeoutMs?: number): Promise<string> {
    const result = await this.sendCall('identity', undefined, timeoutMs ?? this.callTimeout) as
      | { token?: unknown }
      | null
    return typeof result?.token === 'string' ? result.token : ''
  }

  /**
   * Present the owner capability on this connection. The caller must already
   * have bound the connection to a trusted peer: an owner attach checks the
   * socket's ownership and identity, and an owned start proves the child with
   * its attempt token.
   */
  async authenticateOwner(
    capability: { id: string, secret: string },
    timeoutMs?: number,
  ): Promise<void> {
    let result: unknown
    try {
      result = await this.sendCall(
        'authenticate_owner',
        { version: OWNER_AUTH_VERSION, capability_id: capability.id, secret: capability.secret },
        timeoutMs ?? this.callTimeout,
      )
    } catch (err) {
      throw classifyOwnerAuthError(err)
    }
    const value = result as Record<string, unknown> | null
    if (
      !value ||
      value.authenticated !== true ||
      value.version !== OWNER_AUTH_VERSION ||
      value.capability_id !== capability.id
    ) {
      throw new OwnerAuthError('unsupported', { reason: 'unexpected-result' })
    }
  }

  private sendCall(method: string, params: unknown, timeout: number): Promise<unknown> {
    if (this.isClosed()) {
      return Promise.reject(new ClosedSocketError('control socket is closed'))
    }
    this.startReadLoop()

    const id = ++this.nextID
    const req: JsonRpcRequest = { jsonrpc: '2.0', id, method }
    if (params !== undefined) {
      req.params = params
    }

    const data = JSON.stringify(req) + '\n'

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new CallTimeoutError())
      }, timeout)

      this.pending.set(id, { resolve, reject, timer })

      this.socket.write(data, (err?: Error | null) => {
        if (err) {
          this.closed = true
          clearTimeout(timer)
          this.pending.delete(id)
          reject(new ClosedSocketError(`write request: ${err.message}`))
        }
      })
    })
  }

  async subscribe(options?: SubscribeOptions): Promise<void> {
    const key = subscribeOptionsKey(options)
    if (this.subscribed) {
      if (key !== subscribeOptionsKey() && key !== this.subscriptionKey) {
        throw new Error('client already subscribed with different options')
      }
      return
    }
    this.subscribed = true
    this.subscriptionKey = key
    await this.call('subscribe', normalizeSubscribeOptions(options))
  }

  events(options?: SubscribeOptions): AsyncIterableIterator<Event> {
    let active = true
    let pendingResolver: EventResolver | null = null
    let subscribedPromise: Promise<void> | null = null

    const ensureSubscribed = async (): Promise<void> => {
      if (!subscribedPromise) {
        subscribedPromise = this.subscribe(options)
      }
      await subscribedPromise
    }

    const next = async (): Promise<IteratorResult<Event>> => {
      if (!active || this.eventDone) {
        return { value: undefined, done: true }
      }

      try {
        await ensureSubscribed()
      } catch (error) {
        active = false
        throw error
      }

      if (!active || this.eventDone) {
        return { value: undefined, done: true }
      }

      if (this.eventQueue.length > 0) {
        return { value: this.eventQueue.shift()!, done: false }
      }

      return new Promise<IteratorResult<Event>>((resolve) => {
        pendingResolver = { resolve, active: true }
        this.eventResolvers.push(pendingResolver)
      })
    }

    const stop = async (): Promise<IteratorResult<Event>> => {
      if (!active) {
        return { value: undefined, done: true }
      }
      active = false
      if (pendingResolver) {
        pendingResolver.active = false
        removeResolver(this.eventResolvers, pendingResolver)
        pendingResolver.resolve({ value: undefined, done: true })
        pendingResolver = null
      }
      return { value: undefined, done: true }
    }

    return {
      next,
      return: stop,
      async throw(error?: unknown): Promise<IteratorResult<Event>> {
        await stop()
        throw error
      },
      [Symbol.asyncIterator]() {
        return this
      },
    }
  }

  async history(options: HistoryOptions): Promise<HistoryResult> {
    const params: HistoryOptions = {
      runtime_id: options.runtime_id,
      limit: options.limit !== undefined ? Math.max(1, Math.min(256, Math.trunc(options.limit))) : undefined,
      after_seq: options.after_seq !== undefined ? Math.max(0, Math.trunc(options.after_seq)) : undefined,
    }
    return this.call('history', params) as Promise<HistoryResult>
  }

  async status(runtimeId?: string): Promise<Record<string, unknown>> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    return this.call('status', params) as Promise<Record<string, unknown>>
  }

  // Result is the lossless terminal-output path. Status and history stay
  // bounded so polling and observers cannot accidentally attach large replies.
  async result(runtimeId?: string): Promise<Record<string, unknown>> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    return this.call('result', params) as Promise<Record<string, unknown>>
  }

  async list(): Promise<Array<Record<string, unknown>>> {
    return this.call('list') as Promise<Array<Record<string, unknown>>>
  }

  async spawn(params: SpawnParams): Promise<SpawnResult> {
    return this.call('spawn', params) as Promise<SpawnResult>
  }

  async shutdown(mode: string): Promise<void> {
    await this.call('shutdown', { mode })
  }

  async answerPermission(
    runtimeId: string,
    requestId: string,
    optionId: string,
    message?: string,
  ): Promise<void> {
    const params: Record<string, string> = {
      runtime_id: runtimeId,
      request_id: requestId,
      option_id: optionId,
    }
    if (message) {
      params.message = message
    }
    await this.call('answer_permission', params)
  }

  async prompt(runtimeId: string, text: string): Promise<void> {
    await this.call('prompt', { runtime_id: runtimeId, text })
  }

  async interruptAndPrompt(runtimeId: string, text: string, keepQueue = false): Promise<void> {
    await this.call('interrupt_and_prompt', {
      runtime_id: runtimeId,
      text,
      keep_queue: keepQueue,
    })
  }

  async cancel(runtimeId?: string): Promise<void> {
    const params = runtimeId ? { runtime_id: runtimeId } : undefined
    await this.call('cancel', params)
  }

  async workflowStatus(workflowId: string): Promise<Record<string, unknown>> {
    return this.call('workflow.status', { workflow_id: workflowId }) as Promise<Record<string, unknown>>
  }

  async workflowWait(workflowId: string, timeoutMs?: number): Promise<Record<string, unknown>> {
    const params: Record<string, unknown> = { workflow_id: workflowId }
    if (timeoutMs !== undefined) params.timeout_ms = timeoutMs
    return this.call('workflow.wait', params) as Promise<Record<string, unknown>>
  }

  async workflowInspect(workflowId: string): Promise<Record<string, unknown>> {
    return this.call('workflow.inspect', { workflow_id: workflowId }) as Promise<Record<string, unknown>>
  }

  async workflowEvents(workflowId: string, opts?: { afterSeq?: number; limit?: number }): Promise<Record<string, unknown>> {
    const params: Record<string, unknown> = { workflow_id: workflowId }
    if (opts?.afterSeq !== undefined) params.after_seq = opts.afterSeq
    if (opts?.limit !== undefined) params.limit = opts.limit
    return this.call('workflow.events', params) as Promise<Record<string, unknown>>
  }

  async workflowCommand(workflowId: string, command: unknown): Promise<Record<string, unknown>> {
    return this.call('workflow.command', { workflow_id: workflowId, command }) as Promise<Record<string, unknown>>
  }

  async workflowComplete(params: WorkflowCompleteParams): Promise<Record<string, unknown>> {
    const command: Record<string, unknown> = {
      op: 'complete',
      node_id: params.node_id,
      activation_id: params.activation_id,
      attempt_id: params.attempt_id,
      lease_id: params.lease_id,
      owner_token: params.owner_token,
      outcome: params.outcome,
    }
    if (params.outputs !== undefined) command.outputs = params.outputs
    if (params.artifacts !== undefined) command.artifacts = params.artifacts
    return this.workflowCommand(params.workflow_id, command)
  }

  async workflowGate(params: WorkflowGateParams): Promise<Record<string, unknown>> {
    const command: Record<string, unknown> = {
      op: 'gate',
      node_id: params.node_id,
      activation_id: params.activation_id,
      gate_id: params.gate_id,
      operation: params.operation,
    }
    const optional: Array<[string, unknown]> = [
      ['actor', params.actor],
      ['reason', params.reason],
      ['outcome', params.outcome],
      ['subject', params.subject],
      ['poll_id', params.poll_id],
      ['source', params.source],
      ['result', params.result],
      ['response_hash', params.response_hash],
      ['observed_at', params.observed_at],
      ['evidence_ids', params.evidence_ids],
    ]
    for (const [key, value] of optional) {
      if (value !== undefined) command[key] = value
    }
    return this.workflowCommand(params.workflow_id, command)
  }

  async workflowHeartbeat(params: WorkflowHeartbeatParams): Promise<Record<string, unknown>> {
    const command: Record<string, unknown> = {
      op: 'heartbeat',
      node_id: params.node_id,
      lease_id: params.lease_id,
      owner_token: params.owner_token,
    }
    if (params.activation_id !== undefined) command.activation_id = params.activation_id
    return this.workflowCommand(params.workflow_id, command)
  }

  async workflowControllerStatus(controllerId: string): Promise<Record<string, unknown>> {
    return this.call('workflow.controller.status', { controller_id: controllerId }) as Promise<Record<string, unknown>>
  }

  // workflow.ready is advisory: it reports claimable auto-dispatch candidates
  // and grants no lease. A missing limit lets the server apply its default.
  async workflowReady(controllerId: string, limit?: number): Promise<Record<string, unknown>> {
    const params: Record<string, unknown> = { controller_id: controllerId }
    if (limit !== undefined) params.limit = limit
    return this.call('workflow.ready', params) as Promise<Record<string, unknown>>
  }

  async workflowControllerList(): Promise<Record<string, unknown>> {
    return this.call('workflow.controller.list') as Promise<Record<string, unknown>>
  }

  async workflowControllerCreate(params: WorkflowControllerCreateParams): Promise<Record<string, unknown>> {
    return this.call('workflow.controller.create', {
      controller_id: params.controller_id,
      max_inflight: params.max_inflight,
    }) as Promise<Record<string, unknown>>
  }

  async workflowControllerEnable(controllerId: string): Promise<Record<string, unknown>> {
    return this.call('workflow.controller.enable', { controller_id: controllerId }) as Promise<Record<string, unknown>>
  }

  async workflowControllerDisable(controllerId: string, reason: string): Promise<Record<string, unknown>> {
    return this.call('workflow.controller.disable', { controller_id: controllerId, reason }) as Promise<Record<string, unknown>>
  }

  // The merged messaging control verbs. Each decodes the snake_case control DTO
  // internal/control/message_dto.go declares.
  async messageSend(params: MessageSendParams): Promise<MessageSendResult> {
    return this.call('message_send', params) as Promise<MessageSendResult>
  }

  // A blocking message verb answers after at most MESSAGE_MAX_WAIT_MS; the
  // client must not fire before the server's own bounded wait can.
  async messageWait(params: MessageWaitParams): Promise<MessageWaitResult> {
    const requested = params.timeout_ms !== undefined && params.timeout_ms > 0
      ? params.timeout_ms
      : MESSAGE_MAX_WAIT_MS
    return this.call('message_wait', params, { timeoutMs: requested }) as Promise<MessageWaitResult>
  }

  async messageReply(params: {
    to_run_id: string
    message_id: string
    correlation_id?: string
    body: string
  }): Promise<MessageSendResult> {
    return this.call('message_reply', params) as Promise<MessageSendResult>
  }

  async messageWithdraw(params: MessageWithdrawParams): Promise<MessageWithdrawResult> {
    return this.call('message_withdraw', params) as Promise<MessageWithdrawResult>
  }

  async messageReceive(params: MessageReceiveParams): Promise<MessageReceiveResult> {
    const requested = params.timeout_ms !== undefined && params.timeout_ms > 0
      ? params.timeout_ms
      : MESSAGE_MAX_WAIT_MS
    return this.call('message_receive', params, { timeoutMs: requested }) as Promise<MessageReceiveResult>
  }

  async messagePeers(params: MessagePeersParams): Promise<MessagePeersResult> {
    return this.call('message_peers', params) as Promise<MessagePeersResult>
  }

  async messagePreflight(): Promise<MessagePreflightResult> {
    return this.call('message_preflight') as Promise<MessagePreflightResult>
  }

  async resume(params: ResumeParams): Promise<ResumeResult> {
    return this.call('resume', params) as Promise<ResumeResult>
  }

  async sessionRename(sessionId: string, name: string): Promise<SessionRenameResult> {
    return this.call('session_rename', { session_id: sessionId, name }) as Promise<SessionRenameResult>
  }

  async sessionLast(runtimeId: string): Promise<SessionLastResult> {
    return this.call('session_last', { runtime_id: runtimeId }) as Promise<SessionLastResult>
  }

  async sessionLastSet(
    runtimeId: string,
    finalOutput: string,
    stopReason: string,
    truncated: boolean,
  ): Promise<void> {
    await this.call('session_last_set', {
      runtime_id: runtimeId,
      final_output: finalOutput,
      stop_reason: stopReason,
      final_output_truncated: truncated,
    })
  }

  async workflowCancel(
    workflowId: string,
    actor: string,
    reason: string,
  ): Promise<Record<string, unknown>> {
    return this.call('workflow.cancel', {
      workflow_id: workflowId,
      actor,
      reason,
    }) as Promise<Record<string, unknown>>
  }

  isClosed(): boolean {
    return this.closed || this.socket.destroyed || this.socket.writableEnded
  }

  close(): void {
    this.closed = true
    this.rl.close()
    this.socket.destroy()
  }
}

export function dial(
  socketPath: string,
  opts?: { callTimeoutMs?: number, connectTimeoutMs?: number },
): Promise<Client> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath })
    let timer: ReturnType<typeof setTimeout> | undefined

    const cleanup = () => {
      if (timer !== undefined) clearTimeout(timer)
      socket.removeListener('connect', onConnect)
      socket.removeListener('error', onError)
    }
    const onConnect = () => {
      cleanup()
      resolve(new Client(socket, opts))
    }
    const onError = (err: Error) => {
      cleanup()
      reject(new Error(`dial control socket: ${err.message}`))
    }

    if (opts?.connectTimeoutMs !== undefined) {
      // A connect that never completes would otherwise outlive any caller
      // deadline, because an unconnected socket emits neither event.
      timer = setTimeout(() => {
        cleanup()
        // cleanup() drops the real error listener, and destroy() can still emit a
        // connect error that was already queued.
        socket.on('error', () => {})
        socket.destroy()
        reject(new CallTimeoutError(`dial control socket: timed out after ${opts.connectTimeoutMs}ms`))
      }, opts.connectTimeoutMs)
    }

    socket.on('connect', onConnect)
    socket.on('error', onError)
  })
}
