// Sub-agent broker mode.
//
// When the avenor pi provider launches a pi sub-process it injects broker
// credentials via AVENOR_BROKER_URL / AVENOR_RUN_ID / AVENOR_BROKER_TOKEN. In
// that mode the extension polls its own run for inbound asks (a host calling
// the message tool on it), and the child broker below carries those actions to
// the broker; 356-13 moves the per-action tools onto it, and until then they
// still go through the legacy path. These helpers are kept free of
// extension-context dependencies so they can be exercised by unit tests.

import { randomUUID } from 'node:crypto'

import { extractReplyMessage, normalizeAskTimeoutMs, type MessageOutcome } from '@dougbots/avenor-core'
import type { ChildBroker, PeerRecord, ProviderResult } from '@dougbots/avenor-core'

/** Deadline for a cleanup call whose caller's own deadline has already expired. */
const ASK_CLEANUP_TIMEOUT_MS = 5000

export interface SubAgentEnv {
  brokerUrl: string
  runId: string
  token: string
}

export function readSubAgentEnv(env: NodeJS.ProcessEnv = process.env): SubAgentEnv | null {
  const brokerUrl = env.AVENOR_BROKER_URL ?? ''
  const runId = env.AVENOR_RUN_ID ?? ''
  const token = env.AVENOR_BROKER_TOKEN ?? ''
  if (brokerUrl && runId && token) return { brokerUrl, runId, token }
  return null
}

/** A top-level session keeps this pair in memory for its entire lifetime. */
export function createTopLevelPeerEnv(brokerUrl: string): SubAgentEnv {
  const url = new URL(brokerUrl)
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new Error('AVENOR_BROKER_URL must be an HTTP(S) URL without credentials, query, or fragment')
  }
  return { brokerUrl: url.href.replace(/\/$/, ''), runId: `pi-${randomUUID()}`, token: randomUUID() }
}

export async function registerTopLevelPeer(env: SubAgentEnv, fetchImpl: typeof fetch = fetch, signal?: AbortSignal): Promise<void> {
  const result = await postBroker(env, '/register', { peer: true }, fetchImpl, signal) as Record<string, unknown> | null
  if (result?.peer !== true || result?.run_id !== env.runId || result?.token !== env.token) {
    throw new Error('the broker did not confirm the chosen peer identity')
  }
}

export interface InboundAsk {
  from_run_id: string
  message_id: string
  message: string
  role?: string
}

/** Parse a broker control message that is an agent_message into an InboundAsk. */
export function parseControlMessage(msg: Record<string, unknown>): InboundAsk | null {
  if (msg.type !== 'agent_message') return null
  let payload = msg.payload
  if (typeof payload === 'string') {
    try {
      payload = JSON.parse(payload)
    } catch {
      return null
    }
  }
  const p = payload as Record<string, unknown> | undefined
  if (!p || typeof p.message !== 'string' || !p.message) return null
  const messageId = String(p.id ?? '')
  if (!messageId) return null
  // An ask with no asker could never be answered; drop it instead of
  // surfacing a reply target that routes to an empty run id.
  const fromRunId = String(p.from_run_id ?? p.from ?? '')
  if (!fromRunId) return null
  return {
    from_run_id: fromRunId,
    message_id: messageId,
    message: p.message,
    role: typeof p.role === 'string' ? p.role : undefined,
  }
}

export interface InboundAskBodyOptions {
  /** Which surface received the ask; the two modes have different readers. */
  heading: 'Ask from sub-agent' | 'Inbound ask'
  role?: string
}

/**
 * Format an inbound ask as a steer body. Both modes share the formatter; only
 * the heading and the optional role metadata differ.
 */
export function formatInboundAskBody(ask: InboundAsk, options: InboundAskBodyOptions): string {
  const sender = ask.from_run_id
  const role = options.role ?? ask.role
  const from = sender
    ? role && role !== 'agent'
      ? ` from \`${sender}\` (${role})`
      : ` from \`${sender}\``
    : ''
  return (
    `**📨 ${options.heading}**${from}\n\n` +
    `${ask.message}\n\n` +
    `To answer, use avenor_message({ action: "reply", reply_to: ${JSON.stringify(ask.message_id)}, message: "..." }).`
  )
}

/** A broker HTTP response error, so callers can classify by status instead of
 * matching digits in the request text. */
export class BrokerHttpError extends Error {
  readonly status: number
  readonly code: string
  readonly body: string

  constructor(path: string, status: number, body: string, code = '') {
    super(`broker ${path}: ${status} ${body}`)
    this.name = 'BrokerHttpError'
    this.status = status
    this.code = code
    this.body = body
  }
}

export interface AgentMessageFields {
  fromRunId: string
  toRunId: string
  messageId: string
  message: string
  role: string
  /** Ask message id being answered; set only on a reply. */
  replyTo?: string
  /** Set only on an ask, so the broker tracks the waiting reply. */
  expectsReply?: boolean
  /** The pinned broker generation a versioned message id was minted for. */
  generation?: string
}

/** Build the `/send` body for one agent_message, as the broker child tools do. */
export function buildAgentMessage(fields: AgentMessageFields): Record<string, unknown> {
  return {
    ...(fields.generation !== undefined ? { generation: fields.generation } : {}),
    from_run_id: fields.fromRunId,
    to_run_id: fields.toRunId,
    type: 'agent_message',
    payload: {
      id: fields.messageId,
      from: fields.fromRunId,
      from_run_id: fields.fromRunId,
      to_run_id: fields.toRunId,
      message: fields.message,
      role: fields.role,
      ...(fields.expectsReply ? { expects_reply: true } : {}),
      ...(fields.replyTo !== undefined ? { reply_to: fields.replyTo } : {}),
    },
  }
}

async function codedError(path: string, response: Response): Promise<BrokerHttpError> {
  const body = (await response.text()).trim()
  let code = ''
  try {
    const parsed = JSON.parse(body) as Record<string, unknown>
    if (typeof parsed.code === 'string') code = parsed.code
  } catch {
    // A non-JSON body carries no wire code.
  }
  return new BrokerHttpError(path, response.status, body, code)
}

export async function postBroker(
  env: SubAgentEnv,
  path: string,
  body: Record<string, unknown>,
  fetchImpl: typeof fetch = fetch,
  signal?: AbortSignal,
): Promise<unknown> {
  const res = await fetchImpl(`${env.brokerUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    // Spread the body first so a caller cannot override the credentials.
    body: JSON.stringify({ ...body, run_id: env.runId, token: env.token }),
    ...(signal ? { signal } : {}),
  })
  if (!res.ok) throw await codedError(path, res)
  return res.json()
}

/**
 * Publish the harness's own user-defined session name for this run, or clear it
 * with an empty name. The broker stores it apart from any model-set rename and
 * prefers it, so peers see the name the human gave the session.
 */
export async function publishHarnessName(
  env: SubAgentEnv,
  name: string | undefined,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  await postBroker(env, '/session_name', { name: name ?? '' }, fetchImpl, AbortSignal.timeout(5000))
}

/** Authenticated GET; broker credentials travel in headers, never the URL. */
export async function getBroker(
  env: SubAgentEnv,
  path: string,
  fetchImpl: typeof fetch = fetch,
  signal?: AbortSignal,
): Promise<unknown> {
  const res = await fetchImpl(`${env.brokerUrl}${path}`, {
    method: 'GET',
    headers: {
      'X-Avenor-Run-Id': env.runId,
      'X-Avenor-Run-Token': env.token,
    },
    ...(signal ? { signal } : {}),
  })
  if (!res.ok) throw await codedError(path, res)
  return res.json()
}

/** The broker generation a child pins once, read the way the Go child's Prepare reads it. */
export type BrokerGenerationPin = () => Promise<string>

/** Read the ask generation `/sessions` advertises, or empty for none. */
async function readGeneration(env: SubAgentEnv, fetchImpl: typeof fetch): Promise<string> {
  const body = await getBroker(env, '/sessions', fetchImpl) as Record<string, unknown> | null
  return typeof body?.generation === 'string' ? body.generation : ''
}

/**
 * Pin the generation on first use and reuse it, so one child binds to one broker
 * generation. The broker rotates it when its ask store resets; a pinned child is
 * then refused instead of being retargeted. A broker that advertises no
 * generation is not pinned, so the next call reads it again.
 */
function pinGeneration(env: SubAgentEnv, fetchImpl: typeof fetch): BrokerGenerationPin {
  let pinned: Promise<string> | undefined
  return () => {
    pinned ??= readGeneration(env, fetchImpl).then(
      generation => {
        if (!generation) pinned = undefined
        return generation
      },
      error => {
        pinned = undefined
        throw error
      },
    )
    return pinned
  }
}

export interface BrokerAskArgs {
  toRunId: string
  message: string
  /** Bound on the wait for a reply; ignores a non-positive or non-finite value. */
  timeoutMs?: number
  /** Caller cancellation; an abort withdraws the ask before reporting. */
  signal?: AbortSignal
}

function combineSignals(...signals: Array<AbortSignal | undefined>): AbortSignal | undefined {
  const active = signals.filter((signal): signal is AbortSignal => signal !== undefined)
  if (active.length === 0) return undefined
  if (active.length === 1) return active[0]
  return AbortSignal.any(active)
}

/**
 * Withdraw a pending ask through the broker as this run. The broker's own error
 * propagates so a caller reporting to a model shows its exact text.
 */
export async function withdrawPendingAsk(
  env: SubAgentEnv,
  messageId: string,
  generation: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ outcome: MessageOutcome }> {
  // A fresh deadline: the caller's own signal may already have aborted the
  // wait this cleanup follows, and abandoning cleanup would lose the ask.
  const body = await postBroker(
    env,
    '/cancel_message',
    { cancel_message_id: messageId, generation },
    fetchImpl,
    AbortSignal.timeout(ASK_CLEANUP_TIMEOUT_MS),
  ) as Record<string, unknown> | null
  if (typeof body?.outcome === 'string' && body.outcome) {
    return { outcome: body.outcome as MessageOutcome }
  }
  return { outcome: 'withdrawn' }
}

/**
 * Withdraw a pending ask and classify whether the edge may still exist on the
 * broker: true means the cleanup could not be confirmed, false means it is gone.
 */
async function withdrawAsk(
  env: SubAgentEnv,
  messageId: string,
  generation: string,
  fetchImpl: typeof fetch,
): Promise<{ pending: boolean; outcome: MessageOutcome }> {
  try {
    const withdrawn = await withdrawPendingAsk(env, messageId, generation, fetchImpl)
    return { pending: false, outcome: withdrawn.outcome }
  } catch (error) {
    // The broker clears the edge itself when a cancelled wait_reply request
    // disconnects, so a 404 from /cancel_message means the ask is already gone.
    // A fence-capacity failure means a delayed send can still be admitted.
    if (error instanceof BrokerHttpError) {
      if (error.code === 'fence-capacity') return { pending: true, outcome: 'possibly-pending' }
      if (error.status === 404) return { pending: false, outcome: 'edge-absent' }
      return { pending: true, outcome: 'possibly-pending' }
    }
    return { pending: true, outcome: 'possibly-pending' }
  }
}

/** Fetch causes for a dial that never opened, so no request reached the broker. */
const NEVER_REACHED_CODES = new Set([
  'ECONNREFUSED',
  'ConnectionRefused',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'UND_ERR_CONNECT_TIMEOUT',
  'ERR_INVALID_URL',
])

/**
 * Whether a fetch failure carries only dial-level causes, so no request was
 * carried to the broker and no ask edge can exist. Undici aggregates one error
 * per dialled address, so every attempt must have failed at the dial.
 */
function dialNeverOpened(error: unknown, depth = 0): boolean {
  if (depth > 5 || error === null || error === undefined) return false
  const node = error as { code?: unknown; cause?: unknown; errors?: unknown }
  if (typeof node.code === 'string' && NEVER_REACHED_CODES.has(node.code)) return true
  if (Array.isArray(node.errors) && node.errors.length > 0) {
    return node.errors.every(cause => dialNeverOpened(cause, depth + 1))
  }
  return dialNeverOpened(node.cause, depth + 1)
}

/** Whether the failure came from a caller or timeout abort, not from the wire. */
function abortedBeforeResponse(error: unknown, depth = 0): boolean {
  if (depth > 5 || error === null || error === undefined) return false
  const node = error as { name?: unknown; cause?: unknown; errors?: unknown }
  if (node.name === 'AbortError' || node.name === 'TimeoutError') return true
  if (Array.isArray(node.errors) && node.errors.some(cause => abortedBeforeResponse(cause, depth + 1))) return true
  return abortedBeforeResponse(node.cause, depth + 1)
}

/** Deadline for the reachability probe, short enough to precede a cleanup. */
const BROKER_PROBE_TIMEOUT_MS = 250

/**
 * Whether the broker still answers HTTP at all. The endpoint is unauthenticated
 * and any response counts, so a live broker that rejects the request is still
 * reachable. Only a dial that never opened means the broker is gone: the
 * probe's own deadline, a reset, and every other transport failure are
 * inconclusive, so a slow broker that already processed the send still counts
 * as reachable and the caller withdraws rather than calling the ask gone.
 */
async function brokerReachable(env: SubAgentEnv, fetchImpl: typeof fetch): Promise<boolean> {
  try {
    await fetchImpl(`${env.brokerUrl}/health`, { signal: AbortSignal.timeout(BROKER_PROBE_TIMEOUT_MS) })
    return true
  } catch (error) {
    return !dialNeverOpened(error)
  }
}

/**
 * Whether a failed send provably never reached the broker: a dial that never
 * opened or an unusable broker address carries no request, and a send that drew
 * no HTTP response while the broker no longer accepts a dial cannot have left
 * its edge in a broker that is gone.
 */
async function sendNeverReachedBroker(
  error: unknown,
  env: SubAgentEnv,
  fetchImpl: typeof fetch,
): Promise<boolean> {
  if (error instanceof BrokerHttpError) return false
  if (dialNeverOpened(error)) return true
  if (abortedBeforeResponse(error)) return false
  return !(await brokerReachable(env, fetchImpl))
}

function brokerOutcome(body: unknown): MessageOutcome {
  const record = body !== null && typeof body === 'object' ? body as Record<string, unknown> : {}
  return typeof record.outcome === 'string' && record.outcome
    ? record.outcome as MessageOutcome
    : 'sent'
}

/**
 * The outcome the wire contract documents for a known code, or undefined for a
 * code outside it. This mirrors the Go transport's wireErrorClasses table: a
 * code whose documented outcome is a credential or edge refusal keeps it, and
 * every other code names a request that was never admitted.
 */
function knownCodeOutcome(code: string): MessageOutcome | undefined {
  switch (code) {
    case 'auth-rejected':
    case 'sender-mismatch':
      return 'auth-rejected'
    case 'not-ask-sender':
    case 'not-ask-target':
      return 'edge-absent'
    case 'invalid-request':
    case 'generation-mismatch':
    case 'unsupported-protocol':
    case 'malformed-message-id':
    case 'message-id-expired':
    case 'message-id-horizon':
    case 'duplicate-message-id':
    case 'mutual-ask':
    case 'fence-capacity':
    case 'target-unavailable':
    case 'transport-closed':
    case 'unsupported-operation':
      return 'not-sent'
    default:
      return undefined
  }
}

/**
 * The outcome of a response the messaging envelope does not describe, as the Go
 * local transport classifies it: only a 401 there is a credential rejection, a
 * code-less 403 is the router's not-ask-target refusal, and every other status
 * leaves transmission uncertain.
 */
function unenvelopedOutcome(status: number): MessageOutcome {
  if (status === 401) return 'auth-rejected'
  if (status === 403) return 'edge-absent'
  return 'possibly-pending'
}

/**
 * Classify a broker HTTP rejection the way the Go local transport does: the
 * body's outcome wins, then a known wire code, then the status. A rejection that
 * names neither leaves transmission uncertain, so it is never reported
 * `not-sent`; only a send that provably never left the surface is.
 */
function brokerErrorOutcome(error: unknown): MessageOutcome | undefined {
  if (!(error instanceof BrokerHttpError)) return undefined
  try {
    const parsed = JSON.parse(error.body) as Record<string, unknown> | null
    if (parsed && typeof parsed.outcome === 'string' && parsed.outcome) {
      return parsed.outcome as MessageOutcome
    }
  } catch {
    // A body that is not JSON carries no envelope of its own.
  }
  if (error.code) return knownCodeOutcome(error.code) ?? 'possibly-pending'
  return unenvelopedOutcome(error.status)
}

/**
 * The outcome of a generation pin that failed before any message was sent: no
 * request reached the send route, so only a credential refusal keeps its own
 * outcome and everything else is a message that was never sent.
 */
function pinFailureOutcome(error: unknown): MessageOutcome {
  return brokerErrorOutcome(error) === 'auth-rejected' ? 'auth-rejected' : 'not-sent'
}

async function brokerAsk(
  env: SubAgentEnv,
  args: BrokerAskArgs,
  messageId: string,
  fetchImpl: typeof fetch,
  pin: BrokerGenerationPin,
): Promise<ProviderResult> {
  const role = 'agent'
  const timeoutMs = normalizeAskTimeoutMs(args.timeoutMs)
  const signal = combineSignals(args.signal, timeoutMs !== undefined ? AbortSignal.timeout(timeoutMs) : undefined)

  let generation: string
  try {
    generation = await pin()
  } catch (error) {
    return { outcome: pinFailureOutcome(error), messageId }
  }

  try {
    await postBroker(
      env,
      '/send',
      buildAgentMessage({
        fromRunId: env.runId,
        toRunId: args.toRunId,
        messageId,
        message: args.message,
        role,
        expectsReply: true,
        generation,
      }),
      fetchImpl,
      signal,
    )
  } catch (error) {
    // A send that never reached the broker cannot have registered an edge, so
    // report it not-sent instead of spending a cancel on it.
    if (await sendNeverReachedBroker(error, env, fetchImpl)) {
      return { outcome: 'not-sent', messageId }
    }
    // A broker that rejected the send classified it itself; Go's ask client
    // reports that rejection rather than the outcome of withdrawing an edge
    // the rejected send never created.
    const rejected = brokerErrorOutcome(error)
    if (rejected) return { outcome: rejected, messageId }
    const cleaned = await withdrawAsk(env, messageId, generation, fetchImpl)
    return { outcome: cleaned.outcome, messageId }
  }

  let reply: unknown
  try {
    reply = await postBroker(env, '/wait_reply', { waiting_for: messageId, generation }, fetchImpl, signal)
  } catch (error) {
    const timedOut = (error instanceof Error && error.name === 'TimeoutError')
      || brokerErrorOutcome(error) === 'timed-out'
    const cleaned = await withdrawAsk(env, messageId, generation, fetchImpl)
    if (timedOut) return { outcome: 'timed-out', messageId }
    const outcome = brokerErrorOutcome(error)
    if (outcome) return { outcome, messageId }
    if (cleaned.pending) return { outcome: 'possibly-pending', messageId }
    return { outcome: cleaned.outcome, messageId }
  }

  const record = reply !== null && typeof reply === 'object' ? reply as Record<string, unknown> : {}
  const outcome = brokerOutcome(record)
  const result: ProviderResult = { outcome, messageId }
  if (outcome === 'replied') {
    result.reply = extractReplyMessage(record)
  }
  return result
}

function peerRecords(body: unknown): PeerRecord[] {
  const sessions = body !== null && typeof body === 'object'
    ? ((body as Record<string, unknown>).sessions ?? [])
    : []
  if (!Array.isArray(sessions)) return []
  return sessions.map(raw => {
    const session = raw !== null && typeof raw === 'object' ? raw as Record<string, unknown> : {}
    const record: PeerRecord = { run_id: typeof session.run_id === 'string' ? session.run_id : '' }
    for (const key of ['label', 'kind', 'status', 'backend', 'model', 'dir'] as const) {
      const value = session[key]
      if (typeof value === 'string' && value) record[key] = value
    }
    return record
  }).filter(record => record.run_id !== '')
}

function wireCode(code: string): string | undefined {
  switch (code) {
    case 'not-permitted':
      return 'not_permitted'
    case 'invalid-name':
      return 'invalid_name'
    case 'unknown-target':
      return 'unknown_target'
    case 'ambiguous-name':
      return 'ambiguous_name'
    default:
      return undefined
  }
}

/** A broker refusal as the router's typed tool error. */
function brokerToolError(error: unknown): Error {
  if (error instanceof BrokerHttpError) {
    const code = wireCode(error.code)
      ?? (error.status === 403 ? 'not_permitted' : error.status === 404 ? 'unknown_target' : error.status === 409 ? 'ambiguous_name' : 'refused')
    const text = (() => {
      try {
        const parsed = JSON.parse(error.body) as Record<string, unknown>
        if (typeof parsed.error === 'string') return parsed.error
      } catch {
        // Fall through to the raw body.
      }
      return error.body || error.message
    })()
    const typed = new Error(text) as Error & { code: string; facts: Record<string, string> }
    typed.name = 'ToolError'
    typed.code = code
    typed.facts = {}
    return typed
  }
  return error instanceof Error ? error : new Error(String(error))
}

/**
 * The inherited-broker implementation of the core ChildBroker contract. Every
 * request authenticates as this run; a transport failure keeps its own text and
 * becomes a refusal at the router boundary.
 */
export function createBrokerChildBroker(
  env: SubAgentEnv,
  fetchImpl: typeof fetch = fetch,
  options: { strictPeers?: boolean; requestTimeoutMs?: number } = {},
): ChildBroker {
  const requestTimeoutMs = options.requestTimeoutMs
  const transport: typeof fetch = requestTimeoutMs === undefined ? fetchImpl : Object.assign(
    (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      const requestUrl = input instanceof Request ? input.url : String(input)
      if (requestUrl === `${env.brokerUrl}/wait_reply`) return fetchImpl(input, init)
      const deadline = AbortSignal.timeout(requestTimeoutMs)
      const signal = init?.signal ? AbortSignal.any([init.signal, deadline]) : deadline
      return fetchImpl(input, { ...init, signal })
    },
    { preconnect: fetchImpl.preconnect },
  )
  const pin = pinGeneration(env, transport)
  return {
    async tell(targetRunId, message, messageId) {
      let generation: string
      try {
        generation = await pin()
      } catch (error) {
        return { outcome: pinFailureOutcome(error), messageId }
      }
      let body: Record<string, unknown> | null
      try {
        body = await postBroker(
          env,
          '/send',
          buildAgentMessage({
            fromRunId: env.runId,
            toRunId: targetRunId,
            messageId,
            message,
            role: 'agent',
            generation,
          }),
          transport,
        ) as Record<string, unknown> | null
      } catch (error) {
        const outcome = brokerErrorOutcome(error)
          ?? (await sendNeverReachedBroker(error, env, transport) ? 'not-sent' : 'possibly-pending')
        return { outcome, messageId }
      }
      return {
        outcome: brokerOutcome(body),
        messageId: typeof body?.message_id === 'string' && body.message_id ? body.message_id : messageId,
      }
    },

    ask(targetRunId, message, messageId, timeoutMs, signal) {
      return brokerAsk(env, { toRunId: targetRunId, message, timeoutMs, signal }, messageId, transport, pin)
    },

    // A reply carries no generation: the broker completes the edge by looking it
    // up in its own current generation, exactly as the Go transport's Reply does.
    async reply(askerRunId, inboundAskId, message) {
      let body: Record<string, unknown> | null
      try {
        body = await postBroker(
          env,
          '/send',
          buildAgentMessage({
            fromRunId: env.runId,
            toRunId: askerRunId,
            messageId: `${inboundAskId}.reply`,
            message,
            role: 'agent',
            replyTo: inboundAskId,
          }),
          transport,
        ) as Record<string, unknown> | null
      } catch (error) {
        const outcome = brokerErrorOutcome(error)
          ?? (await sendNeverReachedBroker(error, env, transport) ? 'not-sent' : 'possibly-pending')
        return { outcome, messageId: inboundAskId }
      }
      return { outcome: brokerOutcome(body), messageId: inboundAskId }
    },

    async withdraw(messageId) {
      let generation: string
      try {
        generation = await pin()
      } catch (error) {
        throw brokerToolError(error)
      }
      try {
        const body = await postBroker(
          env,
          '/cancel_message',
          { cancel_message_id: messageId, generation },
          transport,
        ) as Record<string, unknown> | null
        return { outcome: brokerOutcome(body) }
      } catch (error) {
        if (error instanceof BrokerHttpError) {
          if (error.code === 'fence-capacity') return { outcome: 'possibly-pending' }
          if (error.status === 404) {
            try {
              const parsed = JSON.parse(error.body) as Record<string, unknown>
              if (typeof parsed.outcome === 'string' && parsed.outcome) {
                return { outcome: parsed.outcome as MessageOutcome }
              }
            } catch {
              // A 404 without a body is still an absent edge.
            }
            return { outcome: 'edge-absent' }
          }
        }
        throw brokerToolError(error)
      }
    },

    async peers() {
      const body = await getBroker(env, '/sessions', transport)
      if (options.strictPeers) {
        const rows = body !== null && typeof body === 'object' ? (body as Record<string, unknown>).sessions : undefined
        if (!Array.isArray(rows) || rows.some(row => !row || typeof row.run_id !== 'string' || !row.run_id)) {
          throw new Error('the central peer listing is incomplete or malformed')
        }
      }
      const peers = peerRecords(body)
      const detail = body !== null && typeof body === 'object'
        ? (body as Record<string, unknown>).peers_error
        : undefined
      // A partial listing still arrives with peers; the error rides along so
      // the router can say the list is incomplete instead of dropping it.
      return typeof detail === 'string' && detail ? { peers, detail } : { peers }
    },

    async rename(req) {
      try {
        const body = await postBroker(
          env,
          '/rename',
          { session_id: req.sessionId ?? '', name: req.name },
          transport,
        ) as Record<string, unknown> | null
        return {
          outcome: 'renamed',
          runId: typeof body?.run_id === 'string' ? body.run_id : '',
          label: typeof body?.label === 'string' ? body.label : '',
          previousLabel: typeof body?.previous_label === 'string' ? body.previous_label : '',
        }
      } catch (error) {
        throw brokerToolError(error)
      }
    },

    async last(runId) {
      try {
        const body = await getBroker(env, `/last_message?target=${encodeURIComponent(runId)}`, transport) as Record<string, unknown> | null
        return {
          outcome: 'sent',
          label: typeof body?.label === 'string' ? body.label : undefined,
          hasRecord: body?.has_record === true,
          finalOutput: typeof body?.final_output === 'string' ? body.final_output : '',
          stopReason: typeof body?.stop_reason === 'string' ? body.stop_reason : undefined,
          truncated: body?.final_output_truncated === true,
        }
      } catch (error) {
        throw brokerToolError(error)
      }
    },
  }
}
