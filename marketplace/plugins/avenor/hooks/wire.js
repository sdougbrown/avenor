// Pure wire helpers for the avenor broker's messaging contract
// (docs/messaging.md): the message-id minter, the /send envelope, the queued
// agent_message payload, the reply extraction and the ask failure sentences.
// The shapes mirror packages/core/src/messaging-text.ts (newMsgID,
// extractReplyMessage, renderAskFailure, quote) and
// packages/pi/src/subagent-broker.ts (buildAgentMessage, parseControlMessage),
// so a mod, a pi child and a Go child all put the same bytes on the wire.
// A hooks module imports its own files and "claude-code", nothing else, so the
// ask templates and their escaping are copied from the schemas rather than
// imported; change schemas/message.outcome.json and messaging-text.ts first.
// Nothing here touches the engine call object.

const MAX_WAIT_MS = 600000

const NON_PRINTABLE = /[\p{Cf}\p{Zl}\p{Zp}]/u

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
 * instruction but cannot complete an edge; `expectsReply` mirrors the
 * payload's expects_reply, the caller's signal that the edge is real and
 * worth correlating a reply against. Null when the row is not a readable
 * peer message: control rows (continue, cancel, …) are not.
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
  const expectsReply = body.expects_reply === true
  return { fromRunId, sender, messageId, message, role, expectsReply }
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

/**
 * Escape a value the way messaging-text.ts `escapeNonPrintable` does, so a
 * label or run id a peer controls cannot carry a bidi override or a zero-width
 * character into tool text. Byte-identical to internal/messaging.Quote.
 */
function escapeNonPrintable(value) {
  let out = ""
  for (const character of value) {
    const code = character.codePointAt(0)
    if (character === '"') {
      out += '\\"'
      continue
    }
    if (character === "\\") {
      out += "\\\\"
      continue
    }
    if (character === "\b") {
      out += "\\b"
      continue
    }
    if (character === "\f") {
      out += "\\f"
      continue
    }
    if (character === "\n") {
      out += "\\n"
      continue
    }
    if (character === "\r") {
      out += "\\r"
      continue
    }
    if (character === "\t") {
      out += "\\t"
      continue
    }
    const unprintable = code < 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f) ||
      NON_PRINTABLE.test(character)
    if (unprintable) {
      out += code > 0xffff
        ? "\\u" + (0xd800 + ((code - 0x10000) >> 10)).toString(16).padStart(4, "0") +
          "\\u" + (0xdc00 + ((code - 0x10000) & 0x3ff)).toString(16).padStart(4, "0")
        : "\\u" + code.toString(16).padStart(4, "0")
      continue
    }
    out += character
  }
  return out
}

/** A value as one quoted template fact, as messaging-text.ts `quote`. */
export function quote(value) {
  return '"' + escapeNonPrintable(typeof value === "string" ? value : String(value)) + '"'
}

/**
 * The ask column of schemas/message.outcome.json, verbatim. Templates carry no
 * literal quotes: the facts arrive pre-quoted, exactly as
 * internal/messaging.Repair takes them.
 */
const ASK_TEMPLATES = {
  "timed-out": 'no reply to {message_id} from {target} before the timeout; withdraw it with avenor_message action="withdraw" message_id={message_id}',
  "not-sent": "ask {message_id} was not sent to {target}; nothing is pending",
  "possibly-pending": 'ask {message_id} to {target} may still be pending; if no reply arrives, withdraw it with avenor_message action="withdraw" message_id={message_id}',
  "lost": "ask {message_id} to {target} is lost and cannot be withdrawn; nothing is pending",
  "edge-absent": "no ask {message_id} is pending on {target}; nothing is pending",
}

function substitute(template, facts) {
  return template.replace(/\{([a-z_]+)\}/g, (whole, key) =>
    Object.prototype.hasOwnProperty.call(facts, key) ? facts[key] : whole)
}

/**
 * The shared ask failure sentence, as messaging-text.ts `renderAskFailure`
 * renders it, so a Claude peer reads the same sentence and the same withdraw
 * guidance every other surface gives instead of raw wire text. An outcome with
 * no ask template renders the possibly-pending sentence, the contract's own
 * fallback for an unclassified failure.
 */
export function renderAskFailure(messageId, target, outcome) {
  const known = Object.prototype.hasOwnProperty.call(ASK_TEMPLATES, outcome)
  const template = known ? ASK_TEMPLATES[outcome] : ASK_TEMPLATES["possibly-pending"]
  return substitute(template, { message_id: quote(messageId), target: quote(target) })
}

/**
 * Classify a failed /wait_reply from what the broker said, never from the
 * error's prose: the row's `outcome` field is authoritative, the 504 body's
 * `timeout: true` flag marks the bounded wait expiring, and the status is the
 * fallback when the body cannot be read at all. Only those two signals make an
 * ask timed-out, so an unrelated failure is never reported as one.
 */
export function askFailureOutcome(status, body) {
  const entry = body === null || typeof body !== "object" ? {} : body
  const declared = typeof entry.outcome === "string" ? entry.outcome : ""
  const terminal = declared === "timed-out" || declared === "lost" || declared === "edge-absent"
  if (terminal) return declared
  if (entry.timeout === true || status === 504) return "timed-out"
  return "possibly-pending"
}