// SessionStart and UserPromptSubmit: give this Codex thread one broker identity
// and leave the record every other hook reads. Resume, clear and compaction
// re-enter here with the same thread id, so a live record is reused instead of
// registered twice; a fork arrives with a new id and mints its own peer.

import {
  identityPlan,
  logLine,
  postAuthed,
  readHookInput,
  readRecord,
  recordIsLive,
  recordPathFor,
  registerOrAttach,
  withSessionLock,
  writeRecord,
  RECORD_VERSION,
} from "./common.mjs";

const input = await readHookInput();
const sessionID = typeof input.session_id === "string" ? input.session_id : "";
const source = typeof input.source === "string" ? input.source : "startup";
// Codex does not emit SessionStart for `codex resume`, so the same idempotent
// registration also runs before every prompt. A resumed thread re-arms there.
const prompted = input.hook_event_name === "UserPromptSubmit";
const eventName = prompted ? "UserPromptSubmit" : "SessionStart";

if (sessionID === "" || recordPathFor(process.env.AVENOR_HOME || undefined, sessionID) === "") {
  process.stdout.write(JSON.stringify({}));
  process.exit(0);
}

const before = readRecord(sessionID);
if (before !== null && (await recordIsLive(before))) {
  reuse(before);
}

let outcome;
try {
  outcome = await withSessionLock(sessionID, registerAndWrite);
} catch (err) {
  disabled(err instanceof Error ? err.message : String(err));
}
if (outcome === undefined) {
  // Another invocation held the lock. It writes the record; report it if it is
  // there, and otherwise stay quiet rather than duplicate the registration.
  const recorded = readRecord(sessionID);
  if (recorded === null) {
    process.stdout.write(JSON.stringify({}));
    process.exit(0);
  }
  reuse(recorded);
}
reuse(outcome.record, outcome.reused ? "reused" : "registered");

async function registerAndWrite() {
  const existing = readRecord(sessionID);
  if (existing !== null && (await recordIsLive(existing))) return { record: existing, reused: true };

  const plan = identityPlan(process.env);
  if (plan.kind === "disabled") throw new Error(plan.reason);
  if (existing !== null && existing.provisioned === true) {
    throw new Error("the provisioned run " + existing.run_id + " already has its inbound consumer; restart the launcher");
  }
  // A stale self-registered record means the broker no longer holds the run (a
  // restart) or holds one this thread cannot authenticate. Mint a fresh one.
  if (existing !== null) plan.runID = "";

  const registered = await registerOrAttach(plan);
  const record = {
    version: RECORD_VERSION,
    session_id: sessionID,
    broker_url: registered.broker_url,
    run_id: registered.run_id,
    run_token: registered.run_token,
    label: registered.label,
    provisioned: registered.provisioned,
    created_at: Date.now(),
  };
  try {
    writeRecord(record);
  } catch (err) {
    throw new Error("the peer identity could not be written: " + (err instanceof Error ? err.message : String(err)));
  }
  // Name a self-registered run through /rename, the same label field a later
  // `peers rename` writes. /session_name would record a harness name that the
  // broker resolves ahead of the label and a rename could never replace.
  if (!record.provisioned && record.label !== "") {
    try {
      await postAuthed(record.broker_url, "/rename", record, { name: record.label });
    } catch (err) {
      logLine(sessionID, "initial rename failed: " + (err instanceof Error ? err.message : String(err)));
    }
  }
  return { record, reused: false };
}

function reuse(record, how = "reused") {
  logLine(sessionID, "peer layer on (" + how + " run " + record.run_id + ", source " + source + ")");
  process.stdout.write(JSON.stringify(context(describe(record))));
  process.exit(0);
}

function disabled(reason) {
  logLine(sessionID, "peer layer off: " + reason);
  process.stdout.write(JSON.stringify(
    !prompted && source === "startup" ? { systemMessage: "avenor peer layer off: " + reason } : {},
  ));
  process.exit(0);
}

function describe(r) {
  const name = typeof r.label === "string" && r.label !== "" ? r.label : r.run_id;
  return "this session is broker peer `" + name + "` (run `" + r.run_id + "`). " +
    "Peers reach it with the avenor message tool; inbound peer messages arrive as Codex-queued turns.";
}

function context(text) {
  return { hookSpecificOutput: { hookEventName: eventName, additionalContext: "Avenor peer layer on: " + text } };
}
