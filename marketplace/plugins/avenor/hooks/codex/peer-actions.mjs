// The peer actions one Codex hook invocation owns. Identity comes from the
// session record the caller passes in, never from the model's tool arguments,
// so no tool call can select another session's broker credential.

import fs from "node:fs";
import path from "node:path";

import { matchSession } from "../addresses.js";
import {
  askFailureOutcome,
  buildAgentMessage,
  extractReplyMessage,
  mintMessageID,
  normalizeAskTimeoutMs,
  renderAskFailure,
} from "../wire.js";
import { asksDirFor, getAuthed, postAuthed } from "./common.mjs";

export const PASS = null;

/**
 * Serve one message/peers call. Returns the rendered result text, or PASS when
 * the MCP server's own owner path should handle it.
 *
 * deps: postAuthed, getAuthed, now, claimAsk, restoreAsk, dropClaim — injected
 * so the dispatch is testable without a broker.
 */
export async function servePeerCall({ record, sessionID, toolName, args, deps = {} }) {
  const post = deps.postAuthed || postAuthed;
  const get = deps.getAuthed || getAuthed;
  const now = deps.now || (() => Date.now());
  const asks = {
    claim: deps.claimAsk || ((id, messageID) => claimAsk(sessionID, id, messageID)),
    restore: deps.restoreAsk || ((messageID) => restoreAsk(sessionID, messageID)),
    drop: deps.dropClaim || ((messageID) => dropClaim(sessionID, messageID)),
  };
  const peersTool = toolName.endsWith("__peers");
  const action = typeof args.action === "string" && args.action !== ""
    ? args.action
    : peersTool ? "list" : "";

  if (peersTool) {
    if (action === "rename") return rename(record, args, post);
    if (action === "list") return list(record, get);
    if (action === "last") return last(record, args, get);
    return PASS;
  }
  if (action === "tell") return tell(record, args, post, get, now);
  if (action === "ask") return ask(record, args, post, get, now);
  if (action === "reply") return reply(record, args, post, asks);
  if (action === "withdraw") return withdraw(record, args, post);
  return PASS;
}

async function tell(record, args, post, get, now) {
  const target = await resolve(record, args.target, get);
  const id = mintMessageID(now());
  const answer = await post(record.broker_url, "/send", record, buildAgentMessage({
    fromRunId: record.run_id,
    toRunId: target.runId,
    messageId: id,
    message: textArg(args.message),
    role: "agent",
  }));
  if (!queued(answer)) return refusal("tell", answer);
  return `message "${id}" delivered to "${target.label}"`;
}

async function ask(record, args, post, get, now) {
  const target = await resolve(record, args.target, get);
  const id = mintMessageID(now());
  const timeoutMs = normalizeAskTimeoutMs(args.timeout_ms);
  const sent = await post(record.broker_url, "/send", record, buildAgentMessage({
    fromRunId: record.run_id,
    toRunId: target.runId,
    messageId: id,
    message: textArg(args.message),
    role: "agent",
    expectsReply: true,
    generation: target.generation,
  }));
  if (!queued(sent)) return renderAskFailure(id, target.label, "not-sent");

  const waited = await post(record.broker_url, "/wait_reply", record, {
    waiting_for: id,
    generation: target.generation,
    timeout_ms: timeoutMs,
  }, timeoutMs + 5000);
  if (!waited.ok) {
    const outcome = askFailureOutcome(waited.status, waited.body);
    try {
      await post(record.broker_url, "/cancel_message", record, {
        cancel_message_id: id,
        generation: target.generation,
      });
    } catch {
      // The ask's own outcome is already decided.
    }
    return renderAskFailure(id, target.label, outcome);
  }
  return `ask "${id}" replied: ` + extractReplyMessage(waited.body);
}

async function reply(record, args, post, asks) {
  const id = typeof args.reply_to === "string" ? args.reply_to : "";
  const asker = id === "" ? PASS : asks.claim(id);
  if (asker === null || asker === undefined || asker === "") return PASS;
  const answer = await post(record.broker_url, "/send", record, buildAgentMessage({
    fromRunId: record.run_id,
    toRunId: asker,
    messageId: id + ".reply",
    message: textArg(args.message),
    role: "agent",
    replyTo: id,
  }));
  if (!queued(answer)) {
    asks.restore(id);
    return refusal("reply", answer);
  }
  asks.drop(id);
  return `reply to "${id}" delivered`;
}

async function withdraw(record, args, post) {
  const id = typeof args.message_id === "string" ? args.message_id : "";
  const answer = await post(record.broker_url, "/cancel_message", record, { cancel_message_id: id });
  if (!answer.ok) return refusal("withdraw", answer);
  return `withdrew message "${id}"`;
}

async function list(record, get) {
  const listing = await get(record.broker_url, "/sessions", record);
  if (!listing.ok) throw new Error("the broker did not answer the run listing (status " + listing.status + ")");
  const body = listing.body || {};
  if (typeof body.peers_error === "string" && body.peers_error !== "") {
    throw new Error("the broker's peer listing is partial: " + body.peers_error);
  }
  const rows = Array.isArray(body.sessions) ? body.sessions : [];
  if (rows.length === 0) return "no peers are visible";
  const lines = rows.map((row) => {
    const label = row.label || row.run_id;
    const status = typeof row.status === "string" && row.status !== "" ? " [" + row.status + "]" : "";
    return "- " + label + " (" + row.run_id + ")" + status;
  });
  return "Peers (" + rows.length + "):\n" + lines.join("\n");
}

async function last(record, args, get) {
  const target = await resolve(record, args.target, get);
  const answer = await get(record.broker_url, "/last_message", record,
    "?target=" + encodeURIComponent(target.runId));
  if (!answer.ok) throw new Error("the broker did not answer the last-message read (status " + answer.status + ")");
  const body = answer.body || {};
  if (body.has_record !== true) return "no last message for " + target.label;
  const text = typeof body.final_output === "string" && body.final_output !== "" ? body.final_output : "(empty)";
  return target.label + " (" + target.runId + "): " + text + (body.final_output_truncated === true ? " …(truncated)" : "");
}

async function rename(record, args, post) {
  const name = typeof args.name === "string" ? args.name : "";
  const answer = await post(record.broker_url, "/rename", record, { name });
  if (!answer.ok) return refusal("rename", answer);
  const body = answer.body || {};
  return `renamed run "${body.run_id}" from "${body.previous_label}" to "${body.label}"`;
}

async function resolve(record, raw, get) {
  const ref = typeof raw === "string" ? raw : "";
  if (ref === "" || ref === "self") {
    return { runId: record.run_id, label: record.label || record.run_id, generation: "" };
  }
  const listing = await get(record.broker_url, "/sessions", record);
  if (!listing.ok) throw new Error("the broker did not answer the run listing (status " + listing.status + ")");
  const body = listing.body || {};
  const sessions = Array.isArray(body.sessions) ? body.sessions : [];
  if (typeof body.peers_error === "string" && body.peers_error !== "") {
    throw new Error("the broker's peer listing is partial: " + body.peers_error);
  }
  const match = matchSession(sessions, ref, record.run_id);
  if (match.match === "none") throw new Error('no avenor run matches "' + ref + '"');
  if (match.match === "ambiguous") {
    throw new Error('"' + ref + '" names several avenor runs (' + match.candidates.join(", ") + ")");
  }
  const row = sessions.find((s) => s && s.run_id === match.runId) || {};
  return { runId: match.runId, label: row.label || match.runId, generation: body.generation || "" };
}

/** Take one pending ask by renaming its record. A rename is the handoff, so a
 * retry after this one fails restores it and a second consumer cannot claim it. */
function claimAsk(sessionID, messageID) {
  const dir = asksDirFor(undefined, sessionID);
  if (dir === "" || messageID === "") return "";
  const src = path.join(dir, messageID + ".json");
  const dst = path.join(dir, messageID + ".claimed");
  try {
    fs.renameSync(src, dst);
  } catch {
    return "";
  }
  try {
    const asker = JSON.parse(fs.readFileSync(dst, "utf8")).asker_run_id;
    if (typeof asker === "string" && asker !== "") return asker;
  } catch {
    // Unusable claim; drop it and let the owner surface answer edge-absent.
  }
  fs.rmSync(dst, { force: true });
  return "";
}

function restoreAsk(sessionID, messageID) {
  const dir = asksDirFor(undefined, sessionID);
  if (dir === "") return;
  try {
    // Rename the claim back: the record already holds the asker, so no write
    // sits between the rename pair for a concurrent claim to lose.
    fs.renameSync(path.join(dir, messageID + ".claimed"), path.join(dir, messageID + ".json"));
  } catch {
    // Best effort: the caller reports the failed reply.
  }
}

function dropClaim(sessionID, messageID) {
  const dir = asksDirFor(undefined, sessionID);
  if (dir === "") return;
  try {
    fs.rmSync(path.join(dir, messageID + ".claimed"), { force: true });
  } catch {
    // Already gone.
  }
}

function queued(answer) {
  return answer.ok === true && answer.body !== null && typeof answer.body === "object" && answer.body.queued === true;
}

function refusal(verb, answer) {
  const body = answer.body && typeof answer.body === "object" ? answer.body : {};
  const reason = typeof body.error === "string" ? body.error : "status " + answer.status;
  return verb + " was refused: " + reason;
}

function textArg(value) {
  if (typeof value !== "string" || value === "") throw new Error("the message text must be a non-empty string");
  return value;
}
