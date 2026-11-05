// Avenor peer layer for Claude Code (issue #391, PR 1).
//
// Makes the session that loads this plugin a native avenor broker peer:
//
// - Outbound: a `session.send` hook intercepts SendMessage addressed to
//   `avenor:<run id or label>`, resolves it against the broker's `GET
//   /sessions`, delivers through `POST /send`, and answers
//   `{ isDelivered: true }` only after the broker queues the message. A tell
//   pins the generation that listing advertised, because its minted id is
//   versioned and the broker refuses a versioned send that names none. A send
//   whose text answers a queued inbound ask from the same peer rides that
//   ask's reply edge (`reply_to`), so the asker's own wait unblocks.
// - Inbound: a long-poll subscription on `POST /poll-control` (agent rows
//   only) runs for the session's life. The broker holds each poll at most
//   its two-second poll window, so the loop re-polls without a timer
//   service. A queued peer ask is submitted as `$.prompt.submit` when the
//   session is idle (the host queues it and starts the turn) or appended as
//   a row when a main-loop turn is running.
// - Outbound message actions: a `tool.call` hook on `mcp__avenor__message`
//   serves tells and asks through this session's broker credentials, so they
//   come from the conversation that loaded the plugin instead of the MCP
//   server's own supervisor. A tell pins the listing's generation and never
//   rides an ask edge; an ask blocks on `/wait_reply` and a failed wait leaves
//   it open for a late answer, as Pi and core do. The hook also answers inbound
//   asks or tells by message id using its local correlations. A tell's answer
//   omits the target; the broker resolves its recorded return route. Other
//   actions pass through to the MCP server.
//   The hook, not
//   `$.tool.register`:
//   the plugin also ships an MCP server named "avenor", and a registered tool
//   under `mcp__avenor__*` would be refused as a replacement for it. A failed
//   ask renders the shared ask sentences (wire.js mirrors
//   schemas/message.outcome.json), never raw wire text.
//
// - Identity: a run id and an attach credential in the environment are a
//   provisioned identity — a spawned child joins the broker as the run its
//   launcher already holds, with neither a `/register` call nor a
//   `/session_name` write. The credential is a one-shot grant: the layer trades
//   it at `POST /attach` for the run's own credential, so a run has one inbound
//   consumer and a second session holding the same environment (a nested
//   session spawned from a provisioned one's shell) is refused with the
//   broker's reason. Without the pair the session self-registers through the
//   broker's open `/register` route as a top-level peer of its own, and names
//   itself from `AVENOR_SESSION_NAME` (falling back to the launch's
//   `AVENOR_LABEL`).
//
// The hook grammar is what Claude Code 2.1.311's module scan enforces; the
// mechanics come from the #391 spike (391-spike-findings.md). Branches over
// event data are ternaries; helpers that take the engine call object `d` are
// top-level function declarations; pass-through is returning undefined,
// never calling `next`.

import { parseAvenorAddress, parseMessageTarget, parseTellTarget, matchSession } from "./addresses.js"
import {
  mintMessageID,
  randomHex,
  buildAgentMessage,
  parseControlMessage,
  extractReplyMessage,
  frameInboundAsk,
  normalizeAskTimeoutMs,
  renderAskFailure,
  renderReplyText,
  renderTellText,
  askFailureOutcome,
  tellFailureOutcome,
  quote,
} from "./wire.js"

// The tested Claude Code release. Below it the layer disables itself instead
// of erroring; plugin.json has no engines field, so the floor is enforced here.
const FLOOR_MAJOR = 2
const FLOOR_MINOR = 1
const FLOOR_PATCH = 311

// The plugin's own state, module-scoped. A hot reload replaces it — and with
// it the poll loop; the old run record stays registered until the broker
// rotates, so reloads are a dev-time operation.
const OFF = { enabled: false, reason: "the avenor peer layer has not started" }
const layer = { ...OFF }
let broker = null
let busyTurns = 0
const pendingReplies = new Map()
const MAX_REMEMBERED_MESSAGES = 128

export async function register(on) {
  await on("session.send", async (d, e) => deliverIfAvenor(d, e))

  await on("session.start", async (d, e) => startPeerLayer(d, e))

  await on("turn.start", async (d, e) => turnTicked(d, e, 1))

  await on("turn.complete", async (d, e) => turnTicked(d, e, -1))

  await on("tool.call", { tool: "mcp__avenor__message" }, async (d, e) => serveMessageHook(d, e))
  await on("tool.call", { tool: "mcp__plugin_avenor_avenor__message" }, async (d, e) => serveMessageHook(d, e))
}

// --- session.send: outbound brokerage ---

async function deliverIfAvenor(d, e) {
  const ref = parseAvenorAddress(e.to)
  if (ref === null) {
    // Not our address space: pass through, so the normal pipeline delivers.
    return undefined
  }
  if (!layer.enabled || broker === null) {
    // Disabled means disabled: even an avenor:* address goes to the normal
    // pipeline, whose own refusal is the honest answer.
    return undefined
  }
  try {
    return await deliverOutbound(d, ref, e.text)
  } catch (err) {
    await logLayer(d, "outbound to " + ref + " failed: " + errorText(err))
    return undelivered("the avenor broker is unreachable")
  }
}

async function deliverOutbound(d, ref, text) {
  const target = await resolveTarget(d, ref)
  const refusal = targetRefusal(target, ref)
  if (refusal !== null) {
    return undelivered(refusal.text)
  }
  // A peer this session owes an ask to takes the send as the ask's reply, so
  // the asker's own wait unblocks; every other send is a plain tell. The
  // correlation is the most recent inbound ask from that peer, and it is
  // consumed only once the broker queue confirms: a failed send keeps the
  // edge pending, so the model's retry still rides it.
  const askId = peekReply(target.runId)
  const hasReply = askId !== null
  // Versioned tells require the generation read during target resolution.
  // Replies omit it because the broker resolves the original edge's generation.
  const body = hasReply
    ? buildAgentMessage({
        fromRunId: broker.runID,
        toRunId: target.runId,
        messageId: askId + ".reply",
        message: text,
        role: "agent",
        replyTo: askId,
      })
    : buildAgentMessage({
        fromRunId: broker.runID,
        toRunId: target.runId,
        messageId: mintMessageID(await d.clock.now()),
        message: text,
        role: "agent",
        generation: target.generation,
      })
  let response
  try {
    response = await brokerPost(d, "/send", body)
  } catch (err) {
    await logLayer(d, "send to " + target.runId + " failed: " + errorText(err))
    return undelivered("the avenor broker refused the send: " + errorText(err))
  }
  const queued = response.queued === true
  if (!queued) {
    return undelivered("the broker answered " + JSON.stringify(response))
  }
  if (hasReply) {
    dropReply(askId)
  }
  return { isDelivered: true }
}

// --- session.start: version gate, self-registration, inbound loop ---

async function startPeerLayer(d, e) {
  await gateOnVersion(d)
  if (!layer.enabled) {
    return undefined
  }
  const started = await selfRegister(d)
  if (!started) {
    // Every refusal to register sets the reason; the layer explains itself
    // once, the way the version gate does.
    await logLayer(d, layer.reason)
    return { cwd: e.cwd }
  }
  runInboundLoop(d)
  return { cwd: e.cwd }
}

async function gateOnVersion(d) {
  let release
  try {
    release = await d.session.version()
  } catch (err) {
    disableLayer("the Claude Code version is unreadable: " + errorText(err))
    return
  }
  const answer = release === null || typeof release !== "object" ? {} : release
  const base = typeof answer.base === "string" && answer.base !== "" ? answer.base : ""
  const spelled = base !== ""
    ? base
    : typeof answer.version === "string" ? answer.version : ""
  const below = belowFloor(spelled)
  if (below) {
    disableLayer("Claude Code " + (spelled !== "" ? spelled : "(unknown version)") +
      " is below the tested floor 2.1.311 — the avenor peer layer stays off")
    await logLayer(d, layer.reason)
    return
  }
  layer.enabled = true
  layer.reason = ""
}

async function selfRegister(d) {
  const brokerURL = await envBrokerURL(d)
  if (brokerURL === "") {
    disableLayer("no avenor broker is configured: set AVENOR_BROKER_URL")
    return false
  }
  const envRunID = await envRunId(d)
  const envToken = await envBrokerToken(d)
  // A run id and a credential together are a provisioned identity: whoever
  // spawned this session already holds the run, and the record is the broker's.
  // The credential is this launch's, good for one attach handshake, so joining
  // is a round trip and the open /register route is not the way in — it would
  // mint a record for a run the launcher no longer holds (a supervisor
  // restart), leaving behind a peer nothing routes to.
  const provisioned = envRunID !== "" && envToken !== ""
  try {
    const registered = provisioned
      ? await attachRun(d, brokerURL, envRunID, envToken)
      : await registerRun(d, brokerURL, envRunID)
    if (registered === null) {
      return false
    }
    broker = registered
  } catch (err) {
    disableLayer("registering with the avenor broker " + brokerURL + " failed: " + errorText(err))
    return false
  }
  // A provisioned run keeps its supervisor-assigned label. A self-registered
  // run uses /rename so its environment name does not shadow later renames.
  if (!provisioned) {
    const name = await envSessionName(d)
    if (name !== "") {
      try {
        await brokerPost(d, "/rename", { name: name })
      } catch (err) {
        await logLayer(d, "rename failed: " + errorText(err))
      }
    }
  }
  const path = provisioned ? "provisioned run " : "self-registered run "
  await logLayer(d, "peer layer on (" + path + broker.runID + ", broker " + broker.url + ")")
  return true
}

/**
 * The provisioned fast path: a one-shot attach handshake. The broker spends
 * this launch's credential on the first session that presents it and answers
 * with the run's own, which is what the layer uses for the rest of the
 * session. A refusal means the run already has its inbound consumer — a
 * nested session spawned from a provisioned one inherits the same
 * environment — so the layer disables itself with the broker's reason instead
 * of polling a queue another session is serving. Null reports that
 * disable-with-reason; a transport failure is thrown for the caller to report.
 */
async function attachRun(d, url, runID, credential) {
  let answer
  try {
    answer = await postJSON(d, url, "/attach", { run_id: runID, token: credential })
  } catch (err) {
    const status = httpStatus(err)
    const refused = status === 401 || status === 404 || status === 409
    if (!refused) {
      throw err
    }
    const body = httpBody(err)
    const reason = typeof body.error === "string" && body.error !== "" ? body.error : errorText(err)
    disableLayer("the avenor broker refused this launch's attach for " + runID + ": " + reason)
    return null
  }
  const mintedToken = answer === null || typeof answer !== "object" ? "" : answer.token
  const mintedRunID = answer !== null && typeof answer === "object" &&
    typeof answer.run_id === "string" && answer.run_id !== "" ? answer.run_id : runID
  if (typeof mintedToken !== "string" || mintedToken === "") {
    disableLayer("the broker answered the attach for " + mintedRunID + " with no run credential")
    return null
  }
  return { url: url, runID: mintedRunID, token: mintedToken }
}

/**
 * One self-registration through the broker's open /register route, or the
 * retry of a self-minted identity: a run id from the environment is used as
 * given; a self-minted one retries once with another suffix when the broker
 * holds a stale record the session cannot authenticate against. Null reports a
 * disable-with-reason the caller logs.
 */
async function registerRun(d, url, envRunID) {
  let runID = envRunID
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (!runID) {
      runID = "claude-" + randomHex(3)
    }
    try {
      const answer = await postJSON(d, url, "/register", { run_id: runID })
      const mintedToken = answer === null || typeof answer !== "object" ? "" : answer.token
      const mintedRunID = answer !== null && typeof answer === "object" &&
        typeof answer.run_id === "string" && answer.run_id !== "" ? answer.run_id : runID
      if (typeof mintedToken !== "string" || mintedToken === "") {
        disableLayer("the broker minted no run token for " + mintedRunID)
        return null
      }
      return { url: url, runID: mintedRunID, token: mintedToken }
    } catch (err) {
      const retryable = envRunID === "" && attempt === 0 &&
        errorText(err).indexOf("already registered") !== -1
      if (!retryable) {
        throw err
      }
      // A self-minted name hit a stale record from an earlier session; mint
      // another and try once more.
      runID = ""
    }
  }
  return null
}

// --- the inbound subscription ---

/**
 * The subscription runs for the session's life: one /poll-control request at
 * a time. The broker's poll window is 2 s, so each request is short and the
 * loop is the pacing; the loop outlives the session.start hook that started
 * it, like the documented process-spawn loop. Every failure pauses it; no
 * iteration kills it, and the loop dies with the session the module lives in.
 */
async function runInboundLoop(d) {
  for (;;) {
    let quiet = false
    try {
      quiet = await pollOnce(d)
    } catch (err) {
      quiet = true
    }
    if (quiet) {
      await pause(d)
    }
  }
}

async function pollOnce(d) {
  try {
    const response = await d.http.fetch(broker.url + "/poll-control", {
      method: "POST",
      body: JSON.stringify({ run_id: broker.runID, token: broker.token, agent_only: true }),
    })
    if (response.ok !== true) {
      // An unreadable answer (a replaced broker, a stale run) pauses rather
      // than re-polling hot; the next success resumes.
      return true
    }
    const rows = parseJSON(response.text)
    const readable = Array.isArray(rows) ? rows : []
    for (const row of readable) {
      const ask = parseControlMessage(row)
      if (ask !== null) {
        await deliverInbound(d, ask)
      }
    }
    return false
  } catch (err) {
    return true
  }
}

async function deliverInbound(d, ask) {
  rememberReply(ask)
  const framed = frameInboundAsk(ask)
  if (busyTurns > 0) {
    await appendInbound(d, framed)
    return
  }
  try {
    // An idle session: the submitted prompt starts its own turn — the host
    // queues it until idle and runs it in this plugin's name.
    await d.prompt.submit({ text: framed })
  } catch (err) {
    await appendInbound(d, framed)
  }
}

async function appendInbound(d, text) {
  try {
    await d.session.append({
      message: { type: "user", content: [{ type: "text", text: text }] },
    })
  } catch (err) {
    await logLayer(d, "an inbound peer message could not be delivered: " + errorText(err))
  }
}

// --- turn axis: idle vs busy ---

/**
 * Only the main conversation's turns count: a subagent's turn carries its
 * agentId and never gates the peer layer.
 */
async function turnTicked(d, e, delta) {
  const main = e.agentId === undefined
  busyTurns = busyTurns + (main ? delta : 0)
  if (busyTurns < 0) {
    busyTurns = 0
  }
  return undefined
}

/**
 * The turn events pass through unchanged (their hooks rewrite nothing); the
 * main-loop axis only counts.
 */

// --- the avenor message tool's peer actions, served by the peer layer ---

/**
 * Serve the message tool's peer actions through this session's broker identity.
 * A tell, an ask and a reply all belong to the conversation that loaded the
 * plugin, but the stock MCP server owns its own supervisor and resolves against
 * that supervisor's universe. The hook is the session identity boundary, so it
 * serves those three and passes every other action through untouched. A tell
 * is a plain send: it never rides an ask edge the way SendMessage does, because
 * the model named a new message rather than answering one.
 */
async function serveMessageHook(d, e) {
  const isTell = e.action === "tell"
  const isAsk = e.action === "ask"
  const isReply = e.action === "reply"
  if (!isTell && !isAsk && !isReply) {
    // Every other action belongs to the MCP surface's own supervisor.
    return undefined
  }
  if (!layer.enabled || broker === null) {
    // Without the layer the MCP server serves the action itself, against the
    // supervisor it is attached to.
    return undefined
  }
  if (isReply) {
    return serveReply(d, e)
  }
  if (isTell) {
    // A tell's target and text are canonically admitted as strings: labels may
    // contain spaces, and the message may be empty. Both stay on this session's
    // peer client; only an absent or non-string field is left to the MCP
    // surface's admission.
    const tellRef = parseTellTarget(e.target)
    const tellMessage = typeof e.message === "string" ? e.message : null
    if (tellRef === null || tellMessage === null) {
      return undefined
    }
    return serveTell(d, tellRef, tellMessage)
  }
  const ref = parseMessageTarget(e.target)
  if (ref === null) {
    return askOutcome("not-sent", null, "ask target must be a run id, label, or self without whitespace")
  }
  const message = typeof e.message === "string" ? e.message : ""
  if (message === "") {
    return askOutcome("not-sent", null, "ask message must be a non-empty string")
  }
  return serveAsk(d, ref, message, e.timeout_ms)
}

/**
 * One explicit tell through this session's broker credentials. The generation
 * comes from the same fresh listing that resolved the target, because a
 * versioned tell is refused when it names none. No correlation table is read:
 * an explicit tell is a new message, never an answer to a queued ask.
 */
async function serveTell(d, ref, message) {
  let target
  try {
    target = await resolveTarget(d, ref, true)
  } catch (err) {
    await logLayer(d, "tell target resolution failed: " + errorText(err))
    return toolError(listingRefusal("tell", ref))
  }
  const refusal = targetRefusal(target, ref)
  if (refusal !== null) {
    return toolError(refusal)
  }
  const messageId = mintMessageID(await d.clock.now())
  let response
  try {
    response = await brokerPost(d, "/send", buildAgentMessage({
      fromRunId: broker.runID,
      toRunId: target.runId,
      messageId: messageId,
      message: message,
      role: "agent",
      generation: target.generation,
    }))
  } catch (err) {
    const outcome = tellFailureOutcome(httpBody(err))
    await logLayer(d, "tell " + messageId + " was not sent: " + errorText(err))
    return tellOutcome(outcome, messageId, target.label)
  }
  if (response.queued !== true) {
    const outcome = tellFailureOutcome(response)
    await logLayer(d, "tell " + messageId + " was not queued: the broker answered " + JSON.stringify(response))
    return tellOutcome(outcome, messageId, target.label)
  }
  const acked = typeof response.message_id === "string" && response.message_id !== ""
    ? response.message_id
    : messageId
  return tellOutcome("sent", acked, target.label)
}

async function serveAsk(d, ref, message, rawTimeout) {
  let target
  try {
    target = await resolveTarget(d, ref, true)
  } catch (err) {
    await logLayer(d, "ask target resolution failed: " + errorText(err))
    return toolError(listingRefusal("ask", ref))
  }
  const refusal = targetRefusal(target, ref)
  if (refusal !== null) {
    return toolError(refusal)
  }
  return runAsk(d, target, ref, message, rawTimeout)
}

/**
 * One ask against the resolved target, rendered with the contract's own
 * sentences. The generation the send and the wait both pin comes from the
 * listing the resolution already read, so an ask costs one listing.
 */
async function runAsk(d, target, ref, message, rawTimeout) {
  const messageId = mintMessageID(await d.clock.now())
  try {
    await brokerPost(d, "/send", buildAgentMessage({
      fromRunId: broker.runID,
      toRunId: target.runId,
      messageId: messageId,
      message: message,
      role: "agent",
      expectsReply: true,
      generation: target.generation,
    }))
  } catch (err) {
    await logLayer(d, "ask " + messageId + " was not queued: " + errorText(err))
    return askOutcome("not-sent", messageId, renderAskFailure(messageId, ref, "not-sent"))
  }
  let result
  try {
    result = await brokerPost(d, "/wait_reply", {
      waiting_for: messageId,
      generation: target.generation,
      timeout_ms: normalizeAskTimeoutMs(rawTimeout),
    })
  } catch (err) {
    const outcome = askFailureOutcome(httpStatus(err), httpBody(err))
    await logLayer(d, "ask " + messageId + " wait reported " + outcome + ": " + errorText(err))
    return askOutcome(outcome, messageId, renderAskFailure(messageId, ref, outcome))
  }
  return askOutcome("replied", messageId, extractReplyMessage(result))
}

/**
 * Answer an inbound peer message this session received, addressed by its message
 * id. An ask names the edge's owner so the broker finds that edge; a tell or an
 * answer names no target, and the broker resolves the recorded return route, so
 * answering never needs a fresh listing that may hide the peer. The correlation
 * table, not the broker's edge store, is where those messages live, so the hook
 * sends the reply itself along the same reply-edge send `session.send` uses. A
 * `reply_to` the table does not hold passes through to the MCP server, whose own
 * refusal is then the honest answer.
 */
async function serveReply(d, e) {
  const replyTo = typeof e.reply_to === "string" ? e.reply_to : ""
  const answer = pendingReplies.get(replyTo)
  if (answer === undefined) {
    return undefined
  }
  const message = typeof e.message === "string" ? e.message : ""
  if (message === "") {
    await logLayer(d, "reply to " + replyTo + " was not sent: the reply text was empty")
    return replyOutcome("not-sent", replyTo)
  }
  let queued = false
  try {
    const response = await brokerPost(d, "/send", buildAgentMessage({
      fromRunId: broker.runID,
      toRunId: answer.expectsReply ? answer.peer : "",
      messageId: replyTo + ".reply",
      message: message,
      role: "agent",
      replyTo: replyTo,
    }))
    queued = response.queued === true
    if (!queued) {
      await logLayer(d, "reply to " + replyTo + " was not queued: the broker answered " +
        JSON.stringify(response))
    }
  } catch (err) {
    await logLayer(d, "reply to " + replyTo + " was not sent: " + errorText(err))
  }
  if (!queued) {
    // A refused reply keeps the edge, so the model's retry still rides it.
    return replyOutcome("not-sent", replyTo)
  }
  dropReply(replyTo)
  return replyOutcome("replied", replyTo)
}

/**
 * The router's own ask result (schemas/message_route.result.json:
 * snake_case keys, `reply` only when the outcome is replied), rendered the
 * way the MCP surface returns it — as the tool's text.
 */
function askOutcome(outcome, messageId, replyText) {
  const shape = {
    action: "ask",
    outcome: outcome,
    ...(messageId !== null ? { message_id: messageId } : {}),
    text: replyText,
    ...(outcome === "replied" ? { reply: replyText } : {}),
  }
  return { result: JSON.stringify(shape) }
}

/** The reply action's result, in the same router shape. */
function replyOutcome(outcome, messageId) {
  return {
    result: JSON.stringify({
      action: "reply",
      outcome: outcome,
      message_id: messageId,
      text: renderReplyText(messageId, outcome),
    }),
  }
}

/** The tell action's result, in the same router shape. */
function tellOutcome(outcome, messageId, label) {
  return {
    result: JSON.stringify({
      action: "tell",
      outcome: outcome,
      message_id: messageId,
      label: label,
      text: renderTellText(messageId, label, outcome),
    }),
  }
}

/**
 * A router tool error (schemas/message_route.result.json `error`): the fixed
 * code and its rendered sentence, never raw wire text. The tool.call return
 * channel carries it as content, so the model reads the same error object the
 * MCP surface's failed call publishes.
 */
function toolError(refusal) {
  return { result: JSON.stringify({ code: refusal.code, text: refusal.text }) }
}

// --- broker client ---

/**
 * Resolution calls for one fresh /sessions listing — no cache — so a spawn
 * that just landed is immediately addressable. That listing also carries the
 * generation an ask pins, so resolving and pinning are one round-trip.
 *
 * A listing the broker flagged partial (`peers_error`) refuses every ref
 * outright: a subset cannot prove a run absent, nor that a label match is the
 * only one, so matching against it would resolve to the wrong peer. The Go and
 * TS resolvers apply the same rule to the same field.
 */
async function resolveTarget(d, ref, allowSelf = false) {
  const body = await brokerGet(d, "/sessions")
  const answer = body === null || typeof body !== "object" ? {} : body
  const incomplete = typeof answer.peers_error === "string" ? answer.peers_error : ""
  if (incomplete !== "") {
    return { match: "incomplete", runId: "", candidates: [], generation: "", incomplete: incomplete }
  }
  const sessions = Array.isArray(answer.sessions) ? answer.sessions : []
  const targetRef = allowSelf && ref === "self" && broker !== null ? broker.runID : ref
  const excludeRunId = allowSelf || broker === null ? "" : broker.runID
  const hit = matchSession(sessions, targetRef, excludeRunId)
  return {
    match: hit.match,
    runId: hit.runId,
    candidates: hit.candidates,
    label: hit.match === "unique" ? hit.label : "",
    generation: typeof answer.generation === "string" ? answer.generation : "",
    incomplete: "",
  }
}

/** The refusal sentence for a ref resolved against a partial peer listing. */
function listingIncomplete(detail, ref) {
  return "the peer listing is incomplete: " + detail + "; " + quote(ref) +
    " was not resolved against it, because a row it missed can shadow the run or label that ref names"
}

/**
 * The one gate every outbound peer action shares: only a target that resolved to
 * exactly one run may send. The refusal carries the router's error code and its
 * rendered sentence, so SendMessage, tell and ask report the same failure the
 * same way; a match state this layer does not know is refused rather than sent
 * with no target.
 */
function targetRefusal(target, ref) {
  if (target.match === "unique") {
    return null
  }
  if (target.match === "none") {
    return { code: "unknown_target", text: "no run matches " + quote(ref) }
  }
  if (target.match === "ambiguous") {
    return {
      code: "ambiguous_name",
      text: quote(ref) + " matches " + target.candidates.length + " runs: " +
        target.candidates.map(quote).join(", "),
    }
  }
  if (target.match === "incomplete") {
    return { code: "refused", text: listingIncomplete(target.incomplete, ref) }
  }
  return { code: "refused", text: quote(ref) + " was not sent: the target did not resolve to exactly one run" }
}

/** A resolution that never reached the listing, in the same error shape. */
function listingRefusal(action, ref) {
  return {
    code: "refused",
    text: action + " to " + quote(ref) + " was not sent: the avenor broker did not answer the run listing",
  }
}

async function brokerGet(d, path) {
  const response = await d.http.fetch(broker.url + path, {
    headers: { "X-Avenor-Run-Id": broker.runID, "X-Avenor-Run-Token": broker.token },
  })
  if (response.ok !== true) {
    throw httpFailure("GET " + path, response)
  }
  return parseJSON(response.text)
}

/** The self-registration credentials spread around every POST body. */
async function brokerPost(d, path, body) {
  return postJSON(d, broker.url, path, {
    ...body,
    run_id: broker.runID,
    token: broker.token,
  })
}

async function postJSON(d, url, path, bodyObj) {
  const response = await d.http.fetch(url + path, {
    method: "POST",
    body: JSON.stringify(bodyObj),
  })
  if (response.ok !== true) {
    throw httpFailure("POST " + path, response)
  }
  return parseJSON(response.text)
}

/**
 * A refused broker response as an error carrying the facts a caller has to
 * classify from — the HTTP status and the parsed body — so an outcome is never
 * inferred from the error's prose. The message keeps the raw text for the log
 * line; no model-facing text renders it.
 */
function httpFailure(label, response) {
  const failure = new Error(label + ": " + response.status + ": " + response.text)
  failure.status = typeof response.status === "number" ? response.status : 0
  const parsed = parseJSON(response.text)
  failure.body = parsed === null || typeof parsed !== "object" ? {} : parsed
  return failure
}

function httpStatus(err) {
  return typeof err === "object" && err !== null && typeof err.status === "number" ? err.status : 0
}

function httpBody(err) {
  const body = typeof err === "object" && err !== null ? err.body : null
  return body === null || typeof body !== "object" ? {} : body
}

/**
 * The session's environment, four names apart: `$.env.get` must take its
 * variable name as a string literal at the call site (the scan lists what a
 * module reads), so each name gets its own helper rather than one that is
 * handed the name.
 */
async function envBrokerURL(d) {
  let value
  try {
    value = await d.env.get("AVENOR_BROKER_URL")
  } catch (err) {
    return ""
  }
  return typeof value === "string" ? value : ""
}

async function envRunId(d) {
  let value
  try {
    value = await d.env.get("AVENOR_RUN_ID")
  } catch (err) {
    return ""
  }
  return typeof value === "string" ? value : ""
}

async function envBrokerToken(d) {
  let value
  try {
    value = await d.env.get("AVENOR_BROKER_TOKEN")
  } catch (err) {
    return ""
  }
  return typeof value === "string" ? value : ""
}

async function envSessionName(d) {
  let value
  try {
    value = await d.env.get("AVENOR_SESSION_NAME")
  } catch (err) {
    value = ""
  }
  if (typeof value === "string" && value !== "") {
    return value
  }
  // AVENOR_LABEL is the launch identity's own spelling: avenor exports it for
  // every launch, while AVENOR_SESSION_NAME above is the retired
  // peer-credential copy that only a hand-exported shell still sets.
  try {
    value = await d.env.get("AVENOR_LABEL")
  } catch (err) {
    return ""
  }
  return typeof value === "string" ? value : ""
}

async function pause(d) {
  try {
    await d.clock.sleep(2000)
  } catch (err) {
    // A refused wait leaves the loop to its next request, which itself paces
    // the poll.
  }
}

async function logLayer(d, text) {
  try {
    // The host prefixes the line with the plugin's name on its own.
    await d.ui.log(text)
  } catch (err) {
    // Nothing can carry a log-line failure anywhere; keep going.
  }
}

function parseJSON(text) {
  try {
    return JSON.parse(text)
  } catch (err) {
    return null
  }
}

function errorText(err) {
  return typeof err === "object" && err !== null && typeof err.message === "string"
    ? err.message
    : String(err)
}

function undelivered(reason) {
  return { isDelivered: false, reason: reason }
}

/**
 * The inbound messages this session can still answer, keyed by the message's id
 * and holding the peer it came from and whether it opened an edge. Only an ask
 * lets a plain send ride its edge; a tell is still remembered so the reply
 * action can answer it by id without resolving the peer. The table is bounded
 * because a session can receive messages it never answers: an entry that opened
 * no edge is forgotten first, so a flood of tells cannot forget a live ask, and
 * asks go oldest first only when every entry is an ask.
 */
function rememberReply(ask) {
  if (ask.messageId !== "") {
    pendingReplies.set(ask.messageId, {
      peer: ask.fromRunId,
      expectsReply: ask.expectsReply === true,
    })
    forgetOldestMessage()
  }
}

function forgetOldestMessage() {
  if (pendingReplies.size <= MAX_REMEMBERED_MESSAGES) {
    return
  }
  let victim = pendingReplies.keys().next().value
  for (const [pendingId, entry] of pendingReplies) {
    if (entry.expectsReply !== true) {
      victim = pendingId
      break
    }
  }
  pendingReplies.delete(victim)
}

/** The newest ask a peer is still owed, so a plain send to it rides that edge. */
function peekReply(runId) {
  let askId = null
  for (const [pendingId, entry] of pendingReplies) {
    askId = entry.peer === runId && entry.expectsReply ? pendingId : askId
  }
  return askId
}

function dropReply(askId) {
  pendingReplies.delete(askId)
}

function disableLayer(reason) {
  layer.enabled = false
  layer.reason = reason
}

/**
 * The version floor: the tested Claude Code release, compared numerically. A
 * spelling that does not parse (a dev build, a future scheme) counts as
 * below, so the layer disables instead of guessing.
 */
function belowFloor(spelled) {
  const parts = typeof spelled === "string" ? spelled.split(".") : []
  if (parts.length !== 3) {
    return true
  }
  const major = Number(parts[0])
  const minor = Number(parts[1])
  const patch = Number(parts[2])
  const numeric = Number.isFinite(major) && Number.isFinite(minor) && Number.isFinite(patch)
  if (!numeric) {
    return true
  }
  if (major > FLOOR_MAJOR) return false
  if (major < FLOOR_MAJOR) return true
  if (minor > FLOOR_MINOR) return false
  if (minor < FLOOR_MINOR) return true
  if (patch > FLOOR_PATCH) return false
  if (patch < FLOOR_PATCH) return true
  return false
}
