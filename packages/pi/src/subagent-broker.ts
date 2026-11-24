// Sub-agent broker mode.
//
// When the avenor pi provider launches a pi sub-process it injects broker
// credentials via AVENOR_BROKER_URL / AVENOR_RUN_ID / AVENOR_BROKER_TOKEN. In
// that mode the extension polls its own run for inbound asks. The shared
// HTTP adapter in core sends messages using the same credentials.

import { randomUUID } from 'node:crypto'

import { PEER_MESSAGE_NOTICE, postBroker, sanitize } from '@dougbots/avenor-core'
import type { SubAgentEnv } from '@dougbots/avenor-core'

// The shared HTTP broker adapter, its request helpers, and its error type.
export {
  BrokerHttpError,
  createBrokerChildBroker,
  isInvalidNameRefusal,
  postBroker,
  publishHarnessName,
  publishLastTurn,
} from '@dougbots/avenor-core'
export type { LastTurnRecord } from '@dougbots/avenor-core'
export type { SubAgentEnv } from '@dougbots/avenor-core'

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
    throw new Error(`the broker URL must be an HTTP(S) URL without credentials, query, or fragment: ${brokerUrl}`)
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
  /** Mirrors the payload's expects_reply: true only for an ask, which is the sole decider for the reply instruction. */
  expects_reply: boolean
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
  // A message id is what an answer correlates against, so only an ask needs one:
  // the broker refuses an expects_reply send without it. A tell opens no edge.
  // The broker refuses a send whose payload and envelope ids disagree, so a row
  // carrying both agrees. A row queued with the id on the envelope only still
  // names the id a reply correlates against.
  const payloadId = String(p.id ?? '')
  const messageId = payloadId !== ''
    ? payloadId
    : typeof msg.id === 'string' ? msg.id : ''
  // The sender is the broker's stamp on the queued row, which the sender cannot
  // write. The payload's copy is only a fallback for a row queued without one:
  // reading it first would let any peer announce itself as another run, which
  // the frame above the body would then repeat as fact.
  const stamped = typeof msg.from_run_id === 'string' && msg.from_run_id !== '' ? msg.from_run_id : undefined
  const fromRunId = stamped ?? String(p.from_run_id ?? p.from ?? '')
  // An ask with no asker could never be answered; drop it instead of
  // surfacing a reply target that routes to an empty run id.
  if (!fromRunId) return null
  const expects_reply = p.expects_reply === true
  return { from_run_id: fromRunId, message_id: messageId, message: p.message, expects_reply }
}

export interface InboundAskBodyOptions {
  /** The ask reader's surface: each mode has a different reader, so each names
   * its own. A tell is always a peer's message, so it renders the peer heading
   * regardless of the surface that received it. */
  heading: 'Ask from sub-agent' | 'Inbound ask' | 'Message from peer'
}

/**
 * Format an inbound peer message as a steer body. Every mode shares the
 * formatter, so the heading, the provenance notice and the reply instruction
 * cannot drift between the surfaces that surface one.
 *
 * The notice is not decoration: a peer writes the text below it, and the reader
 * is a model that will otherwise read a peer's words in the strongest position it
 * has, indistinguishable from the operator's own message. No role is rendered:
 * nothing on the wire is stamped with one, so printing it would print the
 * sender's own claim about its authority.
 */
export function formatInboundAskBody(ask: InboundAsk, options: InboundAskBodyOptions): string {
  const { from, idSegment } = inboundAskSegments(ask)
  const body =
    `**📨 ${options.heading}**${from}${idSegment}\n\n` +
    `${PEER_MESSAGE_NOTICE}\n\n` +
    `${ask.message}`
  if (!ask.expects_reply) return body
  return body + `\n\nTo answer, use avenor_message({ action: "reply", reply_to: ${JSON.stringify(ask.message_id)}, message: "..." }).`
}

/** The header segments every inbound frame shares: the sender segment, and the
 * tell-only id segment — a tell opens no edge, so its header is the only place
 * the id a reply names can appear, while an ask already carries its id in the
 * reply instruction. The model-facing body and the display header both build
 * from these, so the two halves cannot drift. */
function inboundAskSegments(ask: InboundAsk): { from: string; idSegment: string } {
  return {
    from: ask.from_run_id ? ` from \`${ask.from_run_id}\`` : '',
    idSegment: !ask.expects_reply && ask.message_id !== ''
      ? ` (message id \`${sanitize(ask.message_id)}\`)` : '',
  }
}

/** The custom message type every inbound peer frame is delivered as. */
export const PEER_MESSAGE_CUSTOM_TYPE = 'avenor-peer-message'

/** The display half of an inbound peer frame: what the session log shows for a
 * message whose model-facing content keeps the provenance notice and the reply
 * instruction. Built from the same segments as the body so the two halves
 * cannot drift. */
export interface InboundAskDisplay {
  /** The plain-text header line: heading, sender, and (for a tell) its id. */
  header: string
  /** The sender's own message, exactly as the model reads it. */
  body: string
}

export function inboundAskDisplay(ask: InboundAsk, options: InboundAskBodyOptions): InboundAskDisplay {
  const { from, idSegment } = inboundAskSegments(ask)
  return { header: `📨 ${options.heading}${from}${idSegment}`, body: ask.message }
}

