// PreToolUse: hand the peer actions this session owns to the hook, and pass
// everything else to the MCP server. The hook learns its identity from the
// record its own session wrote, never from a tool argument.

import { servePeerCall, PASS } from "./peer-actions.mjs";
import { peerDirFor, readHookInput, readRecord } from "./common.mjs";

const input = await readHookInput();
const sessionID = typeof input.session_id === "string" ? input.session_id : "";
const toolName = typeof input.tool_name === "string" ? input.tool_name : "";
const args = input.tool_input !== null && typeof input.tool_input === "object" ? input.tool_input : {};

if (peerDirFor(undefined, sessionID) === "" || !/__(message|peers)$/.test(toolName)) passThrough();
const record = readRecord(sessionID);
if (record === null) passThrough();

let result;
try {
  result = await servePeerCall({ record, sessionID, toolName, args });
} catch (err) {
  result = "avenor peer request failed: " + (err instanceof Error ? err.message : String(err));
}
if (result === PASS) passThrough();

process.stdout.write(JSON.stringify({
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "deny",
    permissionDecisionReason: result + "\n\n(This result is final; do not retry the tool call.)",
  },
}));

function passThrough() {
  process.stdout.write(JSON.stringify({}));
  process.exit(0);
}
