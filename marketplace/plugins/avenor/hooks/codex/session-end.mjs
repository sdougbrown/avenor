// SessionEnd: record that the peer's turn-producing session is over. The
// background poller is cancelled by Codex, so this hook owns no process; it
// keeps the identity record so a later resume can reuse a still-live run.

import { logLine, postAuthed, readHookInput, readRecord, peerDirFor } from "./common.mjs";

const input = await readHookInput();
const sessionID = typeof input.session_id === "string" ? input.session_id : "";
if (peerDirFor(undefined, sessionID) === "") {
  process.stdout.write(JSON.stringify({}));
  process.exit(0);
}

const record = readRecord(sessionID);
if (record !== null) {
  try {
    await postAuthed(record.broker_url, "/finish", record, {
      status: "ended",
      summary: "codex session ended",
    });
  } catch (err) {
    logLine(sessionID, "finish failed: " + (err instanceof Error ? err.message : String(err)));
  }
}
logLine(sessionID, "session ended");
process.stdout.write(JSON.stringify({}));
