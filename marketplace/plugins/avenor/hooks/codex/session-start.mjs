// SessionStart: give this Codex thread one broker identity and leave the record
// every other hook reads. Resume, clear and compaction re-enter here with the
// same thread id, so a live record is reused instead of registered twice; a
// fork arrives with a new id and mints its own peer.

import {
  identityPlan,
  logLine,
  postAuthed,
  readHookInput,
  readRecord,
  recordIsLive,
  recordPathFor,
  registerOrAttach,
  writeRecord,
  RECORD_VERSION,
} from "./common.mjs";

const input = await readHookInput();
const sessionID = typeof input.session_id === "string" ? input.session_id : "";
const source = typeof input.source === "string" ? input.source : "startup";
// Codex does not emit SessionStart for `codex resume`, so the same idempotent
// registration also runs before every prompt. A resumed thread re-arms there.
const prompted = input.hook_event_name === "UserPromptSubmit";

if (sessionID === "" || recordPathFor(process.env.AVENOR_HOME || undefined, sessionID) === "") {
  process.stdout.write(JSON.stringify({}));
  process.exit(0);
}

const existing = readRecord(sessionID);
if (existing !== null && (await recordIsLive(existing))) {
  if (!prompted) {
    logLine(sessionID, "peer layer on (reused run " + existing.run_id + ", source " + source + ")");
    process.stdout.write(JSON.stringify(context(describe(existing))));
  } else {
    process.stdout.write(JSON.stringify({}));
  }
  process.exit(0);
}

const plan = identityPlan(process.env);
if (plan.kind === "disabled") {
  logLine(sessionID, "peer layer off: " + plan.reason);
  process.stdout.write(JSON.stringify(!prompted && source === "startup" ? { systemMessage: "avenor peer layer off: " + plan.reason } : {}));
  process.exit(0);
}

if (existing !== null && existing.provisioned === true) {
  // The launch's attach credential was spent by the session that owned this
  // record; this thread can no longer join that run.
  const reason = "the provisioned run " + existing.run_id + " already has its inbound consumer; restart the launcher";
  logLine(sessionID, "peer layer off: " + reason);
  process.stdout.write(JSON.stringify(!prompted && source === "startup" ? { systemMessage: "avenor peer layer off: " + reason } : {}));
  process.exit(0);
}

// A stale self-registered record means the broker no longer holds the run (a
// restart) or holds one this thread cannot authenticate. Mint a fresh one.
if (existing !== null) plan.runID = "";

let registered;
try {
  registered = await registerOrAttach(plan);
} catch (err) {
  const reason = err instanceof Error ? err.message : String(err);
  logLine(sessionID, "peer layer off: " + reason);
  process.stdout.write(JSON.stringify(!prompted && source === "startup" ? { systemMessage: "avenor peer layer off: " + reason } : {}));
  process.exit(0);
}

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
  const reason = "the peer identity could not be written: " + (err instanceof Error ? err.message : String(err));
  logLine(sessionID, "peer layer off: " + reason);
  process.stdout.write(JSON.stringify(!prompted && source === "startup" ? { systemMessage: "avenor peer layer off: " + reason } : {}));
  process.exit(0);
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

logLine(sessionID, "peer layer on (" + (record.provisioned ? "provisioned" : "self-registered") +
  " run " + record.run_id + ", source " + source + ")");
process.stdout.write(JSON.stringify(context(describe(record))));

function describe(r) {
  const name = typeof r.label === "string" && r.label !== "" ? r.label : r.run_id;
  return "this session is broker peer `" + name + "` (run `" + r.run_id + "`). " +
    "Peers reach it with the avenor message tool; inbound peer messages arrive as Codex-queued turns.";
}

function context(text) {
  return { hookSpecificOutput: { hookEventName: "SessionStart", additionalContext: "Avenor peer layer on: " + text } };
}
