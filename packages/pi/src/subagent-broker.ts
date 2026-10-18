// Sub-agent broker mode.
//
// When the avenor pi provider launches a pi sub-process it injects broker
// credentials via AVENOR_BROKER_URL / AVENOR_RUN_ID / AVENOR_BROKER_TOKEN. In
// that mode the extension polls its own run for inbound asks (a host calling
// avenor_ask on it), can reply as its own run, and asks its parent through the
// same broker. These helpers are kept free of extension-context dependencies so
// they can be exercised by unit tests.

import { randomUUID } from 'node:crypto'
import { extractReplyMessage, formatAskFailure } from '@dougbots/avenor-core'
import type { AskResult } from '@dougbots/avenor-core'

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

export interface InboundAsk {
  from_run_id: string
  message_id: string
  message: string
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
  }
}

export function formatInboundAskBody(ask: InboundAsk): string {
  const from = ask.from_run_id ? ` from \`${ask.from_run_id}\`` : ''
  return `**📨 Inbound ask**${from}\n\n${ask.message}\n\nTo answer, use avenor_reply({ reply_to_message_id: ${JSON.stringify(ask.message_id)}, message: "..." }).`
}

/** A broker HTTP response error, so callers can classify by status instead of
 * matching digits in the request text. */
export class BrokerHttpError extends Error {
  readonly status: number

  constructor(path: string, status: number, body: string) {
    super(`broker ${path}: ${status} ${body}`)
    this.name = 'BrokerHttpError'
    this.status = status
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
}

/** Build the `/send` body for one agent_message, as the broker child tools do. */
export function buildAgentMessage(fields: AgentMessageFields): Record<string, unknown> {
  return {
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
  if (!res.ok) throw new BrokerHttpError(path, res.status, (await res.text()).trim())
  return res.json()
}

/**
 * Resolve which pending ask to answer. Prefers an explicit message id (and its
 * asker); otherwise the most recent pending ask matching fromRunId.
 */
export function resolveReplyTarget(
  pending: Map<string, { from_run_id: string; message_id: string }>,
  fromRunId: string | undefined,
  replyToMessageId: string | undefined,
): { from_run_id: string; message_id: string } | null {
  if (replyToMessageId) {
    // The id must be a pending ask; answering an unknown id would be sent
    // fire-and-forget and never reach a waiting asker.
    const hit = pending.get(replyToMessageId)
    if (!hit) return null
    const from = fromRunId ?? hit.from_run_id
    if (!from) return null
    return { from_run_id: from, message_id: replyToMessageId }
  }
  // Message ids are random tokens, so recency comes from Map insertion
  // order: the last inserted match is the most recent ask.
  let match: { from_run_id: string; message_id: string } | undefined
  for (const [, entry] of pending) {
    if (!fromRunId || entry.from_run_id === fromRunId) match = entry
  }
  return match ?? null
}

export interface BrokerAskArgs {
  toRunId: string
  message: string
  role?: string
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

function describeFailure(error: unknown): string {
  if (error instanceof Error) {
    if (error.name === 'TimeoutError') return 'timed out'
    if (error.name === 'AbortError') return 'cancelled'
    return error.message || error.name
  }
  return String(error)
}

/**
 * Withdraw a pending ask through the broker as this run. The broker's own error
 * propagates so a caller reporting to a model shows its exact text.
 */
export async function withdrawPendingAsk(
  env: SubAgentEnv,
  messageId: string,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  // A fresh deadline: the caller's own signal may already have aborted the
  // wait this cleanup follows, and abandoning cleanup would lose the ask.
  await postBroker(
    env,
    '/cancel_message',
    { cancel_message_id: messageId },
    fetchImpl,
    AbortSignal.timeout(ASK_CLEANUP_TIMEOUT_MS),
  )
}

/**
 * Withdraw a pending ask and classify whether the edge may still exist on the
 * broker: true means the cleanup could not be confirmed, false means it is gone.
 */
async function withdrawAsk(
  env: SubAgentEnv,
  messageId: string,
  fetchImpl: typeof fetch,
): Promise<boolean> {
  try {
    await withdrawPendingAsk(env, messageId, fetchImpl)
    return false
  } catch (error) {
    // The broker clears the edge itself when a cancelled wait_reply request
    // disconnects, so a 404 from /cancel_message means the ask is already gone.
    // Anything else, including a transport failure that drew no response, may
    // leave the edge behind.
    if (error instanceof BrokerHttpError) {
      return error.status !== 404
    }
    return true
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

/**
 * Whether a failed send provably never reached the broker, mirroring the host's
 * rule: a dial that never opened or an unusable broker address carries no
 * request; an HTTP response, a reset, or an abort may have been processed and
 * still needs a withdraw.
 */
function sendNeverReachedBroker(error: unknown): boolean {
  return !(error instanceof BrokerHttpError) && dialNeverOpened(error)
}

/**
 * Ask another run through the inherited broker as this run, then wait for the
 * correlated reply. A failed send, wait, timeout, or abort withdraws the ask
 * and reports its message id plus whether it is still pending, matching the
 * host-side ask tool's contract.
 */
export async function askViaBroker(
  env: SubAgentEnv,
  args: BrokerAskArgs,
  fetchImpl: typeof fetch = fetch,
): Promise<AskResult> {
  const messageId = randomUUID()
  const role = args.role ?? 'agent'
  const timeoutMs =
    args.timeoutMs !== undefined && Number.isFinite(args.timeoutMs) && args.timeoutMs > 0
      ? args.timeoutMs
      : undefined
  const signal = combineSignals(args.signal, timeoutMs !== undefined ? AbortSignal.timeout(timeoutMs) : undefined)

  const fail = async (detail: string): Promise<never> => {
    const pending = await withdrawAsk(env, messageId, fetchImpl)
    throw new Error(formatAskFailure(messageId, pending, detail))
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
      }),
      fetchImpl,
      signal,
    )
  } catch (error) {
    // A send that never opened a connection cannot have registered an edge, so
    // report it withdrawn instead of spending a cancel on it.
    if (sendNeverReachedBroker(error)) {
      throw new Error(formatAskFailure(messageId, false, `send ask: ${describeFailure(error)}`))
    }
    return fail(`send ask: ${describeFailure(error)}`)
  }

  let reply: unknown
  try {
    reply = await postBroker(env, '/wait_reply', { waiting_for: messageId }, fetchImpl, signal)
  } catch (error) {
    return fail(`wait for reply: ${describeFailure(error)}`)
  }

  const record = reply && typeof reply === 'object' ? (reply as Record<string, unknown>) : {}
  return {
    reply: extractReplyMessage(record),
    from_run_id: (record.from_run_id as string) ?? '',
  }
}
