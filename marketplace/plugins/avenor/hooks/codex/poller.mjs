// The session's one inbound consumer. Codex owns this background hook and
// cancels it when the session ends, so there is no detached process to reap.
// Each broker control message is queued into the thread with `codex queue`:
// busy sessions hold it until the next safe point, idle sessions start a turn.

import fs from "node:fs";
import path from "node:path";

import { parseControlMessage } from "../wire.js";
import {
  asksDirFor,
  framePeerMessage,
  logLine,
  peerDirFor,
  postAuthed,
  queueMessage,
  readHookInput,
  readRecord,
  recordPathFor,
} from "./common.mjs";

const POLL_TIMEOUT_MS = 5000;
const IDENTITY_WAIT_MS = 30000;

const input = await readHookInput();
const sessionID = typeof input.session_id === "string" ? input.session_id : "";
if (peerDirFor(undefined, sessionID) === "") process.exit(0);

const record = await waitForRecord(sessionID);
if (record === null) process.exit(0);

const dir = peerDirFor(undefined, sessionID);
const lock = acquireLock(dir);
if (lock === null) process.exit(0);
installCleanup(dir, lock);

for (;;) {
  if (!fs.existsSync(recordPathFor(process.env.AVENOR_HOME || undefined, sessionID))) break;
  let answer;
  try {
    answer = await postAuthed(record.broker_url, "/poll-control", record, { agent_only: true }, POLL_TIMEOUT_MS);
  } catch {
    await pause(2000);
    continue;
  }
  if (answer.status === 401 || answer.status === 403 || answer.status === 404) {
    logLine(sessionID, "poller stopped: broker refused the run credential (" + answer.status + ")");
    break;
  }
  if (!answer.ok) {
    await pause(2000);
    continue;
  }
  const rows = Array.isArray(answer.body) ? answer.body : [];
  for (const row of rows) {
    const ask = parseControlMessage(row);
    if (ask !== null) await deliver(sessionID, dir, ask);
  }
}

releaseLock(dir, lock);
process.exit(0);

async function waitForRecord(id) {
  const deadline = Date.now() + IDENTITY_WAIT_MS;
  for (;;) {
    const found = readRecord(id);
    if (found !== null) return found;
    if (Date.now() >= deadline) return null;
    await pause(250);
  }
}

async function deliver(id, sessionDir, ask) {
  if (ask.expectsReply && ask.messageId !== "") rememberAsk(sessionDir, ask);
  const frame = framePeerMessage(ask);
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const queued = await queueMessage(id, frame);
    if (queued.ok) return;
    if (isGone(queued.reason)) {
      logLine(id, "poller stopped: Codex no longer knows thread " + id + " (" + queued.reason + ")");
      process.exit(0);
    }
    if (attempt < 2) await pause(500 * (attempt + 1));
    else {
      logLine(id, "inbound message " + (ask.messageId || "(no id)") + " could not be queued: " + queued.reason);
      appendUndelivered(sessionDir, ask, frame);
    }
  }
}

/** One file per pending ask, written only while the poller owns the delivery.
 * The MCP server claims it by rename before answering, so two processes never
 * both believe the same ask is theirs. */
function rememberAsk(sessionDir, ask) {
  const dir = asksDirFor(undefined, path.basename(sessionDir));
  if (dir === "") return;
  try {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const file = path.join(dir, ask.messageId + ".json");
    fs.writeFileSync(file, JSON.stringify({ message_id: ask.messageId, asker_run_id: ask.fromRunId }) + "\n", {
      mode: 0o600,
      flag: "wx",
    });
  } catch {
    // A duplicate row for an ask already recorded is fine.
  }
}

function appendUndelivered(sessionDir, ask, frame) {
  try {
    fs.appendFileSync(path.join(sessionDir, "undelivered.jsonl"),
      JSON.stringify({ at: Date.now(), message_id: ask.messageId, from_run_id: ask.fromRunId, text: frame }) + "\n",
      { mode: 0o600 });
  } catch {
    // Best effort.
  }
}

function isGone(reason) {
  const text = reason.toLowerCase();
  return text.includes("not found") || text.includes("no such") || text.includes("unknown thread") ||
    text.includes("unknown session");
}

function acquireLock(sessionDir) {
  const file = path.join(sessionDir, "poller.lock");
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const fd = fs.openSync(file, "wx", 0o600);
      fs.writeSync(fd, JSON.stringify({ pid: process.pid, started_at: Date.now() }));
      fs.closeSync(fd);
      return file;
    } catch {
      let pid = 0;
      try {
        pid = JSON.parse(fs.readFileSync(file, "utf8")).pid;
      } catch {
        pid = 0;
      }
      if (pid > 0 && processAlive(pid)) return null;
      try {
        fs.unlinkSync(file);
      } catch {
        return null;
      }
    }
  }
  return null;
}

function processAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function releaseLock(sessionDir, file) {
  try {
    fs.unlinkSync(file);
  } catch {
    // Already gone.
  }
}

function installCleanup(sessionDir, file) {
  const stop = () => {
    releaseLock(sessionDir, file);
    process.exit(0);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  process.on("exit", () => releaseLock(sessionDir, file));
}

function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
