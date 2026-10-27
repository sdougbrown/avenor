// Avenor peer layer for Claude Code (issue #391, PR 1).
//
// Makes the session that loads this plugin a native avenor broker peer:
//
// - Outbound: a `session.send` hook intercepts SendMessage addressed to
//   `avenor:<run id or label>`, resolves it against the broker's `GET
//   /sessions`, delivers through `POST /send`, and answers
//   `{ isDelivered: true }` only after the broker queues the message. A send
//   whose text answers a queued inbound ask from the same peer rides that
//   ask's reply edge (`reply_to`), so the asker's own wait unblocks.
// - Inbound: a long-poll subscription on `POST /poll-control` (agent rows
//   only) runs for the session's life. The broker holds each poll at most
//   its two-second poll window, so the loop re-polls without a timer
//   service. A queued peer ask is submitted as `$.prompt.submit` when the
//   session is idle (the host queues it and starts the turn) or appended as
//   a row when a main-loop turn is running.
// - Outbound asks: the avenor MCP surface's own `message` tool already
//   carries an `ask` action; a `tool.call` hook on `mcp__avenor__message`
//   serves that action through this session's broker credentials (an
//   expects_reply send blocking on `/wait_reply` in the hook, the same shape
//   the avenor surfaces' ask action takes) and passes every other action
//   through to the MCP server. The hook, not `$.tool.register`: the plugin
//   also ships an MCP server named "avenor", and a registered tool under
//   `mcp__avenor__*` would be refused as a replacement for it.
//
// The hook grammar is what Claude Code 2.1.311's module scan enforces; the
// mechanics come from the #391 spike (391-spike-findings.md). Branches over
// event data are ternaries; helpers that take the engine call object `d` are
// top-level function declarations; pass-through is returning undefined,
// never calling `next`.

import { parseAvenorAddress, matchSession } from "./addresses.js"
import {
  mintMessageID,
  shortHex,
  buildAgentMessage,
  parseControlMessage,
  extractReplyMessage,
  frameInboundAsk,
  normalizeAskTimeoutMs,
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
let generation = ""
let busyTurns = 0
const pendingReplies = new Map()

export async function register(on) {
  await on("session.send", async (d, e) => deliverIfAvenor(d, e))

  await on("session.start", async (d, e) => startPeerLayer(d, e))

  await on("turn.start", async (d, e) => turnTicked(d, e, 1))

  await on("turn.complete", async (d, e) => turnTicked(d, e, -1))

  await on("tool.call", { tool: "mcp__avenor__message" }, async (d, e) => serveAskHook(d, e))
}

// --- session.send: outbound brokerage ---

async function deliverIfAvenor(d, e) {
  const ref = parseAvenorAddress(e.to)
  if (ref === null) {
    // Not our address space: pass through, so the normal pipeline delivers.
    return undefined
  }
  if (!layer.enabled) {
    return undelivered(layer.reason)
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
  if (target.match === "none") {
    return undelivered('no avenor run matches "' + ref + '"')
  }
  const ambiguous = target.match === "ambiguous"
  if (ambiguous) {
    return undelivered('"' + ref + '" names several avenor runs (' + target.candidates.join(", ") + ")")
  }
  // A peer this session owes an ask to takes the send as the ask's reply, so
  // the asker's own wait unblocks; every other send is a plain tell. The
  // correlation is the most recent inbound ask from that peer.
  const askId = popReply(target.runId)
  const hasReply = askId !== null
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
      })
  const response = await brokerPost(d, "/send", body)
  const queued = response.queued === true
  return queued
    ? { isDelivered: true }
    : undelivered("the broker answered " + JSON.stringify(response))
}

// --- session.start: version gate, self-registration, inbound loop ---

async function startPeerLayer(d, e) {
  await gateOnVersion(d)
  if (!layer.enabled) {
    return undefined
  }
  const started = await selfRegister(d)
  if (!started) {
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
  try {
    const registered = await registerRun(d, brokerURL, envRunID, envToken)
    if (registered === null) {
      return false
    }
    broker = registered
  } catch (err) {
    disableLayer("registering with the avenor broker " + brokerURL + " failed: " + errorText(err))
    return false
  }
  const name = (await envSessionName(d)) || broker.runID
  try {
    await brokerPost(d, "/session_name", { name: name })
  } catch (err) {
    // The name is cosmetic: an unnamed run is still addressable by run id.
    await logLayer(d, "session_name failed: " + errorText(err))
  }
  await logLayer(d, "peer layer on (run " + broker.runID + ", broker " + broker.url + ")")
  return true
}

/**
 * One registration (or the retry of a self-minted identity): a run id from
 * the environment is used as given, so a token supplied alongside it is the
 * provisioned credential; a self-minted id retries once with another suffix
 * when the broker holds a stale record the session cannot authenticate
 * against. Null reports a disable-with-reason the caller logs.
 */
async function registerRun(d, url, envRunID, envToken) {
  let runID = envRunID
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (!runID) {
      runID = "claude-" + shortHex(3)
    }
    const registration = envToken !== "" ? { run_id: runID, token: envToken } : { run_id: runID }
    try {
      const answer = await postJSON(d, url, "/register", registration)
      const mintedToken = answer === null || typeof answer !== "object" ? "" : answer.token
      const mintedRunID = answer !== null && typeof answer === "object" &&
        typeof answer.run_id === "string" && answer.run_id !== "" ? answer.run_id : runID
      if (typeof mintedToken !== "string" || mintedToken === "") {
        disableLayer("the broker minted no run token for " + mintedRunID)
        return null
      }
      return { url: url, runID: mintedRunID, token: mintedToken }
    } catch (err) {
      const retryable = envRunID === "" && envToken === "" && attempt === 0 &&
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
      // than re-polling hot; the next success resumes, with the generation
      // re-pinned because a replaced broker rotates it.
      generation = ""
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
    generation = ""
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

// --- the avenor message tool's ask action, served by the peer layer ---

async function serveAskHook(d, e) {
  const isAsk = e.action === "ask"
  if (!isAsk) {
    // Every other action belongs to the MCP surface's own supervisor.
    return undefined
  }
  if (!layer.enabled || broker === null) {
    // Without the layer the MCP server serves the ask itself, against the
    // supervisor it is attached to.
    return undefined
  }
  const ref = parseAvenorAddress(e.target === undefined || e.target === null ? "" : e.target)
  if (ref === null) {
    return askOutcome("not-sent", null, "ask target must be an avenor:<run id or label> address")
  }
  const message = typeof e.message === "string" ? e.message : ""
  if (message === "") {
    return askOutcome("not-sent", null, "ask message must be a non-empty string")
  }
  return serveAsk(d, ref, message, e.timeout_ms)
}

async function serveAsk(d, ref, message, rawTimeout) {
  const target = await resolveTarget(d, ref)
  if (target.match === "none") {
    return askOutcome("not-sent", null, 'no avenor run matches "' + ref + '"')
  }
  if (target.match === "ambiguous") {
    return askOutcome("not-sent", null, '"' + ref + '" names several avenor runs (' + target.candidates.join(", ") + ")")
  }
  return runAsk(d, target.runId, message, rawTimeout)
}

async function runAsk(d, runId, message, rawTimeout) {
  const ref = runId === "" ? '""' : '"' + runId + '"'
  const pinned = await pinnedGeneration(d)
  if (pinned === "") {
    return askOutcome("not-sent", null, "the avenor broker is unreachable")
  }
  const messageId = mintMessageID(await d.clock.now())
  try {
    await brokerPost(d, "/send", buildAgentMessage({
      fromRunId: broker.runID,
      toRunId: runId,
      messageId: messageId,
      message: message,
      role: "agent",
      expectsReply: true,
      generation: pinned,
    }))
  } catch (err) {
    return askOutcome("not-sent", messageId, "ask " + messageId + " was not sent to " + ref + ": " + errorText(err))
  }
  let result
  try {
    result = await brokerPost(d, "/wait_reply", { waiting_for: messageId, generation: pinned, timeout_ms: normalizeAskTimeoutMs(rawTimeout) })
  } catch (err) {
    const text = errorText(err)
    const timedOut = text.indexOf("timed-out") !== -1 || text.indexOf("504") !== -1
    const outcome = timedOut ? "timed-out" : "possibly-pending"
    return askOutcome(outcome, messageId, "ask " + messageId + " " + outcome + ": " + text)
  }
  const reply = extractReplyMessage(result)
  return askOutcome("replied", messageId, reply)
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

// --- broker client ---

/**
 * Resolution calls for one fresh /sessions listing — no cache — so a spawn
 * that just landed is immediately addressable.
 */
async function resolveTarget(d, ref) {
  const body = await brokerGet(d, "/sessions")
  const answer = body === null || typeof body !== "object" ? {} : body
  const sessions = Array.isArray(answer.sessions) ? answer.sessions : []
  return matchSession(sessions, ref, broker === null ? "" : broker.runID)
}

/**
 * One preflight of the broker's generation, memoized for the session's asks.
 * Only asks carry versioned ids; tells and replies need nothing here, and a
 * failed poll clears the pin so a replaced broker is re-read.
 */
async function pinnedGeneration(d) {
  if (generation !== "") {
    return generation
  }
  try {
    const body = await brokerGet(d, "/sessions")
    const answer = body === null || typeof body !== "object" ? {} : body
    const fresh = typeof answer.generation === "string" ? answer.generation : ""
    generation = fresh
  } catch (err) {
    return ""
  }
  return generation
}

async function brokerGet(d, path) {
  const response = await d.http.fetch(broker.url + path, {
    headers: { "X-Avenor-Run-Id": broker.runID, "X-Avenor-Run-Token": broker.token },
  })
  if (response.ok !== true) {
    throw new Error("GET " + path + ": " + response.status + ": " + response.text)
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
    throw new Error("POST " + path + ": " + response.status + ": " + response.text)
  }
  return parseJSON(response.text)
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

/** A pending inbound ask correlation, keyed by the peer's run id: the most
 * recent ask wins, so a reply sent to that peer rides its edge. */
function rememberReply(ask) {
  if (ask.messageId !== "") {
    pendingReplies.set(ask.fromRunId, ask.messageId)
  }
}

function popReply(runId) {
  const askId = pendingReplies.get(runId)
  if (typeof askId === "string") {
    pendingReplies.delete(runId)
    return askId
  }
  return null
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