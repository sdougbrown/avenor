// A self-registered session binds its peers calls to a record of its own
// broker identity, so the MCP server answers as the session and not as the
// supervisor. The record is written by the real shell command the hook runs.

import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { existsSync, readFileSync, statSync } from "node:fs"
import { mkdtemp } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

const PLUGIN = fileURLToPath(new URL("..", import.meta.url))
const SESSION = "3f7e02b6-a704-40c3-8e42-1801ed21afbf"
const RUN = "claude-abc123"
const BROKER = "http://127.0.0.1:8787"

async function session(tag, { env = {}, sessionId = SESSION, runExit = 0, brokerUrl = BROKER, rejects = false } = {}) {
  const home = await mkdtemp(join(tmpdir(), "peers-binding-"))
  const hooks = {}
  const calls = []
  const fullEnv = { AVENOR_BROKER_URL: brokerUrl, AVENOR_HOME: home, ...env }
  const d = {
    env: { get: async (name) => fullEnv[name] },
    session: { version: async () => ({ version: "2.1.311", base: "2.1.311" }), id: async () => sessionId, root: async () => "/work/x" },
    process: {
      run: async (argv, init) => {
        calls.push({ argv, init })
        if (rejects && argv[0] !== "git") throw new Error("the host refused the process")
        if (argv[0] === "git") return { exitCode: 1, stdout: "", stderr: "" }
        if (runExit !== 0) return { exitCode: runExit, stdout: "", stderr: "denied" }
        const ran = spawnSync(argv[0], argv.slice(1), { input: init?.stdin ?? "", encoding: "utf8" })
        return { exitCode: ran.status ?? 1, stdout: ran.stdout, stderr: ran.stderr }
      },
    },
    fs: { read: async () => { throw new Error("no transcript") } },
    clock: { now: async () => Date.now(), sleep: () => new Promise(() => {}) },
    ui: { log: async () => {} },
    http: {
      fetch: async (url, init = {}) => {
        const path = String(url).slice(brokerUrl.length)
        const ok = (body) => ({ status: 200, ok: true, headers: {}, text: JSON.stringify(body) })
        if (path === "/register") {
          const posted = JSON.parse(init.body).run_id
          return ok({ run_id: posted.startsWith("claude-") ? RUN : posted, token: "tok-secret" })
        }
        if (path === "/attach") return ok({ run_id: "rt_child", token: "own" })
        if (path === "/rename" || path === "/session_name") return ok({})
        if (path === "/sessions") return ok({ sessions: [] })
        return { status: 401, ok: false, headers: {}, text: "unauthorized" }
      },
    },
  }
  const mod = await import(PLUGIN + "/hooks/register.js?peers-binding-" + tag)
  const peersHooks = []
  await mod.register((event, matcher, hook) => {
    if (event === "tool.call") {
      peersHooks.push({ tool: matcher.tool, hook })
    } else {
      hooks[event] = typeof matcher === "function" ? matcher : hook
    }
  })
  await hooks["session.start"](d, { cwd: "/work/x", isInteractive: true })
  await new Promise((resolve) => setImmediate(resolve))
  const peers = peersHooks.find((h) => h.tool === "mcp__plugin_avenor_avenor__peers").hook
  const next = async (input) => ({ forwarded: input })
  return { d, hooks, home, calls, peers: (e) => peers(d, e, next), peersHooks }
}

const file = (home) => join(home, "peers", "claude", RUN, "identity.json")

test("the record is owner-only and carries the broker identity the MCP server reads", async () => {
  const s = await session("record")
  const record = JSON.parse(readFileSync(file(s.home), "utf8"))
  assert.deepEqual(record, {
    version: 1,
    session_id: RUN,
    broker_url: BROKER,
    run_id: RUN,
    run_token: "tok-secret",
    label: "",
  })
  assert.equal((statSync(file(s.home)).mode & 0o777).toString(8), "600")
  assert.equal((statSync(join(s.home, "peers", "claude", RUN)).mode & 0o777).toString(8), "700")
  assert.ok(s.calls.every((c) => !JSON.stringify(c.argv).includes("tok-secret")), "the token rode in an argument")
})

test("a peers call is bound to the session's record on both tool names", async () => {
  const s = await session("bound")
  assert.deepEqual(s.peersHooks.map((h) => h.tool).filter((t) => t.endsWith("__peers")).sort(), ["mcp__avenor__peers", "mcp__plugin_avenor_avenor__peers"])
  const out = await s.peers({ tool: "mcp__plugin_avenor_avenor__peers", action: "last", target: "self" })
  assert.deepEqual(out, { forwarded: { tool: "mcp__plugin_avenor_avenor__peers", action: "last", target: "self", claude_peer_file: file(s.home) } })
})

test("a record path the model supplied is replaced, and other hosts' bindings are dropped", async () => {
  const s = await session("supplied")
  const out = await s.peers({ action: "list", claude_peer_file: "/elsewhere/identity.json", codex_peer_file: "/other/identity.json" })
  assert.deepEqual(out, { forwarded: { action: "list", claude_peer_file: file(s.home) } })
})

test("a provisioned run is not bound, and a supplied path is still dropped", async () => {
  const s = await session("provisioned", { env: { AVENOR_RUN_ID: "rt_child", AVENOR_BROKER_TOKEN: "grant" } })
  assert.equal(existsSync(join(s.home, "peers")), false)
  assert.equal(await s.peers({ action: "list" }), undefined)
  assert.deepEqual(await s.peers({ action: "list", claude_peer_file: "/x" }), { forwarded: { action: "list" } })
})

test("a record that cannot be written leaves peers calls on the supervisor", async () => {
  const s = await session("failed", { runExit: 1 })
  assert.equal(existsSync(file(s.home)), false)
  assert.equal(await s.peers({ action: "list" }), undefined)
})

test("a run id that is not a safe path segment writes nothing", async () => {
  const s = await session("badid", { env: { AVENOR_RUN_ID: "../escape" } })
  assert.equal(existsSync(join(s.home, "peers")), false)
})

test("a plaintext broker URL that is not loopback gets no record, so the supervisor still answers", async () => {
  const s = await session("lan", { brokerUrl: "http://10.0.0.5:8787" })
  assert.equal(existsSync(join(s.home, "peers")), false)
  assert.equal(await s.peers({ action: "list" }), undefined)
})

test("an https broker URL on another host gets a record", async () => {
  const s = await session("https", { brokerUrl: "https://broker.example.com" })
  assert.equal(existsSync(file(s.home)), true)
})

test("a host that refuses the shell leaves session start, peers and session end intact", async () => {
  const s = await session("rejects", { rejects: true })
  assert.equal(await s.peers({ action: "list" }), undefined)
  assert.deepEqual(await s.hooks["session.end"](s.d, { reason: "other", sessionId: SESSION }), { sessionId: SESSION })
})

test("a removal the host refuses still ends the session and unbinds", async () => {
  const s = await session("rm-rejects")
  s.d.process.run = async () => { throw new Error("the host refused the process") }
  assert.deepEqual(await s.hooks["session.end"](s.d, { reason: "other", sessionId: SESSION }), { sessionId: SESSION })
  assert.equal(await s.peers({ action: "list" }), undefined)
})

test("the record is removed when the session ends", async () => {
  const s = await session("end")
  assert.equal(existsSync(file(s.home)), true)
  const ended = await s.hooks["session.end"](s.d, { reason: "prompt_input_exit", sessionId: SESSION })
  assert.deepEqual(ended, { sessionId: SESSION })
  assert.equal(existsSync(join(s.home, "peers", "claude", RUN)), false)
  assert.equal(await s.peers({ action: "list" }), undefined)
})

test("a /clear or a resume keeps the record: the run goes on under another session id", async () => {
  for (const reason of ["clear", "resume"]) {
    const s = await session("goes-on-" + reason)
    await s.hooks["session.end"](s.d, { reason, sessionId: SESSION })
    assert.equal(existsSync(file(s.home)), true, reason)
    assert.ok(await s.peers({ action: "list" }), reason)
  }
})

// The same rows as TestReadCodexPeerRecordBrokerURLs in internal/mcpserver:
// the hook writes a record only for a URL the server will accept.
const URL_ROWS = [
  ["http://127.0.0.1:8080", true],
  ["HTTP://127.0.0.1:8080", true],
  ["http://127.255.255.254", true],
  ["http://localhost:8787", true],
  ["http://[::1]:8080", true],
  ["https://broker.example.com", true],
  ["http://Localhost:8787", false],
  ["http://LOCALHOST:8787", false],
  ["http://127.000.000.001:8080", false],
  ["http://127.0.0.256", false],
  ["http://128.0.0.1", false],
  ["http://10.0.0.5:8787", false],
  ["http://broker.example.com", false],
]

for (const [url, accepted] of URL_ROWS) {
  test("a broker URL of " + url + (accepted ? " gets a record" : " gets none"), async () => {
    const s = await session("url-" + Buffer.from(url).toString("hex"), { brokerUrl: url })
    assert.equal(existsSync(file(s.home)), accepted)
  })
}
