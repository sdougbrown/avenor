// The Codex PreToolUse hook binds avenor's message and peers tools to the
// thread's record and refuses a call whose model supplied any host's record
// path, so a model cannot point the server at another session's record.

import assert from "node:assert/strict"
import { spawn } from "node:child_process"
import { mkdtemp } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

import { recordPathFor, writeRecord } from "../hooks/codex/common.mjs"

const HOOK = fileURLToPath(new URL("../hooks/codex/pre-tool-use.mjs", import.meta.url))
const SESSION = "01a1-pretool"

async function run({ tool = "mcp__avenor__peers", args, bound }) {
  const home = await mkdtemp(join(tmpdir(), "codex-pretool-"))
  if (bound) {
    writeRecord({ version: 1, session_id: SESSION, broker_url: "http://127.0.0.1:1", run_id: "codex-a", run_token: "t", label: "" }, home)
  }
  const child = spawn(process.execPath, [HOOK], {
    env: { ...Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith("AVENOR_"))), AVENOR_HOME: home },
    stdio: ["pipe", "pipe", "ignore"],
  })
  let out = ""
  child.stdout.on("data", (chunk) => { out += chunk })
  child.stdin.end(JSON.stringify({ session_id: SESSION, hook_event_name: "PreToolUse", tool_name: tool, tool_input: args }))
  await new Promise((resolve) => child.on("close", resolve))
  return { output: JSON.parse(out), home }
}

test("a bound call gets the thread's record path", async () => {
  const { output, home } = await run({ args: { action: "list" }, bound: true })
  assert.equal(output.hookSpecificOutput.permissionDecision, "allow")
  assert.deepEqual(output.hookSpecificOutput.updatedInput, { action: "list", codex_peer_file: recordPathFor(home, SESSION) })
})

test("a call the model bound itself is refused, whichever host's path it names", async () => {
  for (const name of ["claude_peer_file", "codex_peer_file"]) {
    for (const bound of [false, true]) {
      const { output } = await run({ args: { action: "list", [name]: "/tmp/peers/claude/x/identity.json" }, bound })
      assert.equal(output.hookSpecificOutput.permissionDecision, "deny", name + " bound=" + bound)
    }
  }
})

test("an unbound call without a binding passes through, and so does another server's tool", async () => {
  assert.deepEqual((await run({ args: { action: "list" }, bound: false })).output, {})
  assert.deepEqual((await run({ tool: "mcp__other__thing", args: { claude_peer_file: "/x" }, bound: false })).output, {})
})

test("the message tool is bound, refused and passed through like the peers tool", async () => {
  const tool = "mcp__avenor__message"
  const bound = await run({ tool, args: { action: "tell", target: "x", message: "y" }, bound: true })
  assert.equal(bound.output.hookSpecificOutput.permissionDecision, "allow")
  assert.deepEqual(bound.output.hookSpecificOutput.updatedInput, {
    action: "tell",
    target: "x",
    message: "y",
    codex_peer_file: recordPathFor(bound.home, SESSION),
  })

  // Resume is the supervisor's authority: the unbound surface keeps serving it.
  assert.deepEqual((await run({ tool, args: { action: "resume", target: "x", message: "y" }, bound: true })).output, {})

  for (const name of ["claude_peer_file", "codex_peer_file"]) {
    const refused = await run({ tool, args: { action: "tell", target: "x", message: "y", [name]: "/x" }, bound: true })
    assert.equal(refused.output.hookSpecificOutput.permissionDecision, "deny", name)
  }
})
