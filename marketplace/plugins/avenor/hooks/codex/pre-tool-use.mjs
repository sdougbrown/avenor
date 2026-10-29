// PreToolUse: bind the avenor message and peers tools to this Codex thread.
// The model's own arguments are passed through untouched; the added record path
// is the only thing the MCP server reads, and it is not a credential.

import { peerDirFor, readHookInput, readRecord, recordPathFor } from "./common.mjs";

const input = await readHookInput();
const sessionID = typeof input.session_id === "string" ? input.session_id : "";
const toolName = typeof input.tool_name === "string" ? input.tool_name : "";

if (peerDirFor(undefined, sessionID) === "" || !/__(message|peers)$/.test(toolName)) {
  passThrough();
}

const record = readRecord(sessionID);
if (record === null) passThrough();

const args = input.tool_input !== null && typeof input.tool_input === "object" ? input.tool_input : {};
// Resume is the supervisor's own authority; a broker-authenticated run cannot
// continue a run it did not spawn, so the unbound surface keeps serving it.
if (toolName.endsWith("__message") && args.action === "resume") passThrough();

process.stdout.write(JSON.stringify({
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "allow",
    updatedInput: { ...args, codex_peer_file: recordPathFor(process.env.AVENOR_HOME || undefined, sessionID) },
  },
}));

function passThrough() {
  process.stdout.write(JSON.stringify({}));
  process.exit(0);
}
