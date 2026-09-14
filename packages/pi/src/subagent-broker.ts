// Sub-agent broker mode.
//
// When the avenor pi provider launches a pi sub-process it injects broker
// credentials via AVENOR_BROKER_URL / AVENOR_RUN_ID / AVENOR_BROKER_TOKEN. In
// that mode the extension polls its own run for inbound asks (a host calling
// avenor_ask on it) and can reply as its own run. These helpers are kept free
// of extension-context dependencies so they can be exercised by unit tests.

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
  return {
    from_run_id: String(p.from_run_id ?? p.from ?? ''),
    message_id: messageId,
    message: p.message,
  }
}

export function formatInboundAskBody(ask: InboundAsk): string {
  const from = ask.from_run_id ? ` from \`${ask.from_run_id}\`` : ''
  return `**📨 Inbound ask**${from}\n\n${ask.message}\n\nTo answer, use avenor_reply({ reply_to_message_id: ${JSON.stringify(ask.message_id)}, message: "..." }).`
}

export async function postBroker(
  env: SubAgentEnv,
  path: string,
  body: Record<string, unknown>,
  fetchImpl: typeof fetch = fetch,
): Promise<unknown> {
  const res = await fetchImpl(`${env.brokerUrl}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ run_id: env.runId, token: env.token, ...body }),
  })
  if (!res.ok) throw new Error(`broker ${path}: ${res.status} ${(await res.text()).trim()}`)
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
    const hit = pending.get(replyToMessageId)
    const from = fromRunId ?? hit?.from_run_id ?? ''
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
