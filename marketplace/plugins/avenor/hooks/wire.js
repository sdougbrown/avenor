// Pure wire helpers for the avenor broker's messaging contract
// (docs/messaging.md): the message-id minter, the /send envelope, the queued
// agent_message payload and the reply extraction. The shapes mirror
// packages/core/src/messaging-text.ts (newMsgID, extractReplyMessage) and
// packages/pi/src/subagent-broker.ts (buildAgentMessage, parseControlMessage),
// so a mod, a pi child and a Go child all put the same bytes on the wire.
// Nothing here touches the engine call object.

const MAX_WAIT_MS = 600000

function safeParse(text) {
  try {
    return JSON.parse(text)
  } catch (err) {
    return null
  }
}

export function randomHex(nBytes) {
  const bytes = new Uint8Array(nBytes)
  globalThis.crypto.getRandomValues(bytes)
  let out = ""
  for (const byte of bytes) {
    out += byte.toString(16).padStart(2, "0")
  }
  return out
}

/** A run-id suffix for a self-minted identity, and a disambiguator for names. */
export function shortHex(nBytes) {
  return randomHex(nBytes)
}

/**
 * A versioned message id whose encoded acceptance deadline spans the shared
 * wait window, exactly like messaging.NewMsgID / newMsgID.
 */
export function mintMessageID(now) {
  return "av1." + Math.trunc(now + MAX_WAIT_MS).toString(36) + "." + randomHex(16)
}

/** Reduce a caller-supplied ask timeout to what the broker accepts. */
export function normalizeAskTimeoutMs(ms) {
  const value = typeof ms === "number" && Number.isFinite(ms) ? Math.trunc(ms) : 0
  return value > 0 && value <= MAX_WAIT_MS ? value : MAX_WAIT_MS
}

/**
 * The /send body for one agent_message — the TS child child-broker's
 * buildAgentMessage shape: fire-and-forget when no expectsReply and no
 * replyTo, an ask when expectsReply, a reply when replyTo names the ask.
 */
export function buildAgentMessage(fields) {
  const payload = {
    id: fields.messageId,
    from: fields.fromRunId,
    from_run_id: fields.fromRunId,
    to_run_id: fields.toRunId,
    message: fields.message,
    role: fields.role,
  }
  if (fields.expectsReply) {
    payload.expects_reply = true
  }
  if (fields.replyTo) {
    payload.reply_to = fields.replyTo
  }
  // Credentials (run_id/token) are spread around this body by the poster,
  // mirroring the TS child's postBroker, which never lets a caller override them.
  return {
    from_run_id: fields.fromRunId,
    to_run_id: fields.toRunId,
    type: "agent_message",
    message_id: fields.messageId,
    payload: payload,
    generation: fields.generation || undefined,
  }
}

/**
 * A queued control row parsed to the peer-message facts. `message_id` is
 * empty for a tell (SendBody carries none), which still frames a reply
 * instruction but cannot complete an edge. Null when the row is not a
 * readable peer message: control rows (continue, cancel, …) are not.
 */
export function parseControlMessage(msg) {
  const entry = msg === null || typeof msg !== "object" ? {} : msg
  const isAgentMessage = entry.type === "agent_message"
  if (!isAgentMessage) return null
  let payload = entry.payload
  if (typeof payload === "string") {
    payload = safeParse(payload)
  }
  const body = payload === null || typeof payload !== "object" ? {} : payload
  const fromRunId = typeof body.from_run_id === "string" ? body.from_run_id
    : typeof body.from === "string" ? body.from : ""
  const message = typeof body.message === "string" ? body.message : ""
  const messageId = typeof body.id === "string" ? body.id : ""
  if (fromRunId === "" || message === "") return null
  const sender = typeof entry.sender === "string" && entry.sender !== ""
    ? entry.sender
    : typeof body.from === "string" ? body.from : fromRunId
  const role = typeof body.role === "string" ? body.role : ""
  return { fromRunId, sender, messageId, message, role }
}

/**
 * The text an inbound peer frame carries. The sender is the broker's own
 * display name for the run (server-stamped), never a field of the message
 * text; the reply instruction points at the broker run id.
 */
export function frameInboundAsk(ask) {
  const sender = ask.sender !== "" ? ask.sender : ask.fromRunId
  let frame = "**📨 Message from avenor peer `" + sender + "`** (broker run `" + ask.fromRunId + "`"
  frame += ask.messageId === "" ? "" : ", message id `" + ask.messageId + "`"
  if (ask.role !== "") {
    frame += ", role `" + ask.role + "`"
  }
  frame += ")\n\n"
  frame += ask.message + "\n\n"
  frame += "Reply with SendMessage to `avenor:" + ask.fromRunId + "` to answer this peer."
  return frame
}

/** The reply text out of a /wait_reply result row, as the TS host reads it. */
export function extractReplyMessage(result) {
  const entry = result === null || typeof result !== "object" ? {} : result
  if (entry.timeout === true) return "(timeout: no reply received)"
  if (entry.cancelled === true) return "(cancelled)"
  const payload = typeof entry.payload === "string" ? safeParse(entry.payload) : entry.payload
  const body = payload === null || typeof payload !== "object" ? {} : payload
  const message = typeof body.message === "string" ? body.message : ""
  return message !== "" ? message : JSON.stringify(entry)
}