// Shared state and transport for the Codex peer layer. Command hooks cannot
// share module memory the way Claude's mods layer does, so the session's
// identity lives in a 0600 record under the avenor home and every hook reads it
// from there. Broker routes and message framing stay in ../wire.js; the only
// thing Codex needs differently is how an idle session is woken.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile } from "node:child_process";

export const RECORD_VERSION = 1;
export const QUEUE_TIMEOUT_MS = 20000;

const SESSION_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/** One JSON object from the hook's stdin, already closed by Codex. */
export async function readHookInput() {
  let raw = "";
  for await (const chunk of process.stdin) raw += chunk;
  try {
    const parsed = JSON.parse(raw || "{}");
    return parsed !== null && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

/** The avenor home both the hooks and the Go surfaces resolve. */
export function avenorHome() {
  const configured = process.env.AVENOR_HOME;
  return configured !== "" && configured !== undefined
    ? configured
    : path.join(os.homedir(), ".avenor");
}

export function peerRoot(home = avenorHome()) {
  return path.join(home, "peers", "codex");
}

export function peerDirFor(home, sessionID) {
  if (typeof sessionID !== "string" || !SESSION_ID.test(sessionID)) return "";
  return path.join(peerRoot(home), sessionID);
}

export function recordPathFor(home, sessionID) {
  const dir = peerDirFor(home, sessionID);
  return dir === "" ? "" : path.join(dir, "identity.json");
}

export function asksDirFor(home, sessionID) {
  const dir = peerDirFor(home, sessionID);
  return dir === "" ? "" : path.join(dir, "asks");
}

export function readRecord(sessionID, home = avenorHome()) {
  const file = recordPathFor(home, sessionID);
  if (file === "") return null;
  try {
    const record = JSON.parse(fs.readFileSync(file, "utf8"));
    return record !== null && typeof record === "object" ? record : null;
  } catch {
    return null;
  }
}

/** Write one record atomically, owner-only: a half-written identity is worse
 * than none, and every later hook trusts this file without re-deriving it. */
export function writeRecord(record, home = avenorHome()) {
  const dir = peerDirFor(home, record.session_id);
  if (dir === "") throw new Error("invalid Codex session id");
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, "identity.json");
  const temp = file + ".tmp-" + process.pid;
  fs.writeFileSync(temp, JSON.stringify(record, null, 2) + "\n", { mode: 0o600 });
  fs.renameSync(temp, file);
  return file;
}

/** Append one line to the session's own log. A hook never fails the host
 * because it could not write a diagnostic. */
export function logLine(sessionID, text, home = avenorHome()) {
  const dir = peerDirFor(home, sessionID);
  if (dir === "") return;
  try {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.appendFileSync(path.join(dir, "layer.log"), new Date().toISOString() + " " + text + "\n");
  } catch {
    // Diagnostics are best effort.
  }
}

/**
 * The identity a session's environment selects. Kept pure so the ladder is
 * testable without a broker: a provisioned pair is a one-shot attach, a bare
 * run id or nothing self-registers, and a launcher-owned runtime keeps its
 * identity with the launcher rather than minting a duplicate.
 */
export function identityPlan(env) {
  const url = typeof env.AVENOR_BROKER_URL === "string" ? env.AVENOR_BROKER_URL : "";
  const runID = typeof env.AVENOR_RUN_ID === "string" ? env.AVENOR_RUN_ID : "";
  const token = typeof env.AVENOR_BROKER_TOKEN === "string" ? env.AVENOR_BROKER_TOKEN : "";
  const launchLabel = typeof env.AVENOR_LABEL === "string" && env.AVENOR_LABEL !== ""
    ? env.AVENOR_LABEL
    : typeof env.AVENOR_SESSION_NAME === "string" ? env.AVENOR_SESSION_NAME : "";
  if (url === "") {
    return { kind: "disabled", reason: "no avenor broker is configured: set AVENOR_BROKER_URL" };
  }
  if (runID !== "" && token !== "") {
    return { kind: "attach", url, runID, token, label: launchLabel, provisioned: true };
  }
  if (runID === "" && typeof env.AVENOR_RUNTIME_ID === "string" && env.AVENOR_RUNTIME_ID !== "") {
    return {
      kind: "disabled",
      reason: "this Codex session belongs to an avenor-launched run; its launcher owns the peer identity (see #486 step 5)",
    };
  }
  return { kind: "register", url, runID, label: launchLabel, provisioned: false };
}

export async function postJSON(url, route, body, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url.replace(/\/+$/, "") + route, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    const text = await response.text();
    let parsed = null;
    try {
      parsed = text === "" ? null : JSON.parse(text);
    } catch {
      parsed = null;
    }
    return { status: response.status, ok: response.ok, body: parsed, text };
  } finally {
    clearTimeout(timer);
  }
}

export async function postAuthed(url, route, record, body, timeoutMs) {
  return postJSON(url, route, { ...body, run_id: record.run_id, token: record.run_token }, timeoutMs);
}

/** One authenticated read: the broker authenticates a GET from headers. */
export async function getAuthed(url, route, record, query = "", timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url.replace(/\/+$/, "") + route + query, {
      headers: {
        "Accept": "application/json",
        "X-Avenor-Run-Id": record.run_id,
        "X-Avenor-Run-Token": record.run_token,
      },
      signal: controller.signal,
    });
    const text = await response.text();
    let parsed = null;
    try {
      parsed = text === "" ? null : JSON.parse(text);
    } catch {
      parsed = null;
    }
    return { status: response.status, ok: response.ok, body: parsed, text };
  } finally {
    clearTimeout(timer);
  }
}

export class BrokerRefusal extends Error {}

/**
 * Trade the environment's identity at the broker. Provisioned sessions attach
 * once; everything else registers its own run and names it. A refusal is a
 * reason the layer reports, not a crash.
 */
export async function registerOrAttach(plan, fetchPost = postJSON) {
  if (plan.kind === "attach") {
    const answer = await fetchPost(plan.url, "/attach", { run_id: plan.runID, token: plan.token });
    if (!answer.ok) {
      const reason = answer.body !== null && typeof answer.body.error === "string"
        ? answer.body.error
        : "attach refused with status " + answer.status;
      throw new BrokerRefusal("the avenor broker refused this launch's attach for " + plan.runID + ": " + reason);
    }
    const token = answer.body !== null && typeof answer.body.token === "string" ? answer.body.token : "";
    if (token === "") throw new BrokerRefusal("the broker answered the attach with no run credential");
    return {
      broker_url: plan.url,
      run_id: plan.runID,
      run_token: token,
      label: plan.label,
      provisioned: true,
    };
  }
  let runID = plan.runID;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (runID === "") runID = "codex-" + randomSuffix();
    const answer = await fetchPost(plan.url, "/register", { run_id: runID });
    if (answer.ok) {
      const token = answer.body !== null && typeof answer.body.token === "string" ? answer.body.token : "";
      if (token === "") throw new BrokerRefusal("the broker minted no run token for " + runID);
      return {
        broker_url: plan.url,
        run_id: runID,
        run_token: token,
        label: plan.label,
        provisioned: false,
      };
    }
    const retryable = plan.runID === "" && attempt === 0 && answer.status === 409;
    if (!retryable) throw new BrokerRefusal("the broker refused registration for " + runID + " with status " + answer.status);
    runID = "";
  }
  throw new BrokerRefusal("the broker refused every registration attempt");
}

function randomSuffix() {
  const bytes = new Uint8Array(3);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** A record that still authenticates can be reused after resume, clear or
 * compaction. A heartbeat is the cheapest authenticated route. */
export async function recordIsLive(record, fetchPost = postJSON) {
  if (record === null || typeof record !== "object") return false;
  if (typeof record.broker_url !== "string" || record.broker_url === "") return false;
  if (typeof record.run_id !== "string" || record.run_id === "") return false;
  if (typeof record.run_token !== "string" || record.run_token === "") return false;
  try {
    const answer = await fetchPost(record.broker_url, "/heartbeat", {
      run_id: record.run_id,
      token: record.run_token,
    });
    return answer.ok;
  } catch {
    return false;
  }
}

/**
 * Wake a Codex thread with a peer message. `codex queue` accepts both the busy
 * and idle cases: Codex holds the message until the next safe point, and starts
 * a turn when the session is idle.
 */
export function queueMessage(sessionID, text, timeoutMs = QUEUE_TIMEOUT_MS) {
  const bin = process.env.AVENOR_CODEX_BIN || "codex";
  return new Promise((resolve) => {
    execFile(
      bin,
      ["queue", "--thread", sessionID, "--message", text],
      { timeout: timeoutMs, maxBuffer: 1024 * 1024 },
      (error, _stdout, stderr) => {
        if (error === null) {
          resolve({ ok: true, reason: "" });
          return;
        }
        const detail = (stderr || "").trim() || error.message;
        resolve({ ok: false, reason: detail });
      },
    );
  });
}

/**
 * The text a Codex session receives for one inbound peer message. The avenor
 * message tool is the reply path here: Codex has no SendMessage hook, and the
 * broker-stamped sender is the only identity the frame trusts.
 */
export function framePeerMessage(ask) {
  const sender = ask.sender !== "" ? ask.sender : ask.fromRunId;
  let frame = "**📨 Message from avenor peer `" + sender + "`** (broker run `" + ask.fromRunId + "`";
  frame += ask.messageId === "" ? "" : ", message id `" + ask.messageId + "`";
  if (typeof ask.role === "string" && ask.role !== "") frame += ", role `" + ask.role + "`";
  frame += ")\n\n" + ask.message + "\n\n";
  frame += ask.messageId === ""
    ? "No reply is required; answer only if the message itself asks you to."
    : "Answer with the avenor message tool: `action: \"reply\"`, `reply_to: \"" + ask.messageId + "\"`, `message: \"…\"`.";
  return frame;
}


/** One writer for the register-and-record step: two hooks can fire together on
 * the first prompt (SessionStart and UserPromptSubmit), and a second writer
 * would leave outbound on one run and the poller on another. The holder is a
 * pid, so a dead holder's lock is reclaimed. */
export async function withSessionLock(sessionID, fn) {
  const dir = peerDirFor(undefined, sessionID);
  if (dir === "") return fn();
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, "session.lock");
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      const fd = fs.openSync(file, "wx", 0o600);
      fs.writeSync(fd, String(process.pid));
      fs.closeSync(fd);
      try {
        return await fn();
      } finally {
        try {
          fs.rmSync(file, { force: true });
        } catch {
          // Released by a later attempt.
        }
      }
    } catch {
      let pid = 0;
      try {
        pid = Number(fs.readFileSync(file, "utf8"));
      } catch {
        pid = 0;
      }
      if (!(pid > 0 && processAlive(pid))) {
        try {
          fs.rmSync(file, { force: true });
        } catch {
          return undefined;
        }
        continue;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  return undefined;
}

function processAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
