// A self-registered session with no environment name takes a label from the
// work it is in, and a person's name from the environment still wins.

import assert from "node:assert/strict"
import { fileURLToPath } from "node:url"
import test from "node:test"

const PLUGIN = fileURLToPath(new URL("..", import.meta.url))

async function session(tag, { git, listing, env = {}, provisioned = false, fs, argvs = [], sessions, rootRejects = false, logs = [] }) {
  const posts = []
  const hooks = {}
  const fullEnv = {
    AVENOR_BROKER_URL: "http://broker.test",
    ...(provisioned ? { AVENOR_RUN_ID: "rt_child", AVENOR_BROKER_TOKEN: "grant" } : {}),
    ...env,
  }
  const d = {
    env: { get: async (name) => fullEnv[name] },
    session: {
      version: async () => ({ version: "2.1.311", base: "2.1.311" }),
      root: async () => {
        if (rootRejects) throw new Error("the host has no root")
        return "/work/root"
      },
      id: async () => "no-transcript",
    },
    process: {
      run: async (argv, init) => {
        argvs.push({ argv, init })
        const answer = git(argv.slice(3).join(" "))
        return { exitCode: answer === null ? 1 : 0, stdout: answer ?? "", stderr: "" }
      },
    },
    fs: fs ?? { read: async () => { throw new Error("no transcript") } },
    clock: { now: async () => Date.now(), sleep: () => new Promise(() => {}) },
    ui: { log: async (line) => logs.push(line) },
    http: {
      fetch: async (url, init = {}) => {
        const path = String(url).slice("http://broker.test".length)
        if (init.method === "POST") {
          posts.push({ path, body: JSON.parse(init.body) })
        }
        const ok = (body) => ({ status: 200, ok: true, headers: {}, text: JSON.stringify(body) })
        if (path === "/register") return ok({ run_id: "claude-abc123", token: "tok" })
        if (path === "/attach") return ok({ run_id: "rt_child", token: "own" })
        if (path === "/rename") return ok({})
        if (path === "/sessions") return sessions === undefined ? ok(listing) : sessions()
        return { status: 401, ok: false, headers: {}, text: "unauthorized" }
      },
    },
  }
  const mod = await import(PLUGIN + "/hooks/register.js?default-label-" + tag)
  await mod.register((event, matcher, hook) => {
    hooks[event] = typeof matcher === "function" ? matcher : hook
  })
  await hooks["session.start"](d, { cwd: "/work/avenor", isInteractive: true })
  await new Promise((resolve) => setImmediate(resolve))
  return posts.filter((p) => p.path === "/rename").map((p) => p.body.name)
}

const onMain = (args) => (args === "rev-parse --show-toplevel" ? "/work/avenor" : args === "branch --show-current" ? "main" : null)

test("the label is the host and the repository", async () => {
  assert.deepEqual(await session("plain", { git: onMain, listing: { sessions: [] } }), ["claude-avenor"])
})

test("a branch off main is carried", async () => {
  const git = (args) => (args === "branch --show-current" ? "feat/x" : onMain(args))
  assert.deepEqual(await session("branch", { git, listing: { sessions: [] } }), ["claude-avenor@feat-x"])
})

test("a label another peer holds takes a suffix", async () => {
  const listing = { sessions: [{ run_id: "claude-1", label: "claude-avenor" }] }
  assert.deepEqual(await session("taken", { git: onMain, listing }), ["claude-avenor-2"])
})

test("a partial listing keeps the base label", async () => {
  const listing = { sessions: [{ run_id: "claude-1", label: "claude-avenor" }], peers_error: "one supervisor unreachable" }
  assert.deepEqual(await session("partial", { git: onMain, listing }), ["claude-avenor"])
})

test("outside a repository the directory names the work", async () => {
  assert.deepEqual(await session("norepo", { git: () => null, listing: { sessions: [] } }), ["claude-root"])
})

test("an environment name wins over the derived one", async () => {
  const env = { AVENOR_SESSION_NAME: "chosen" }
  assert.deepEqual(await session("env", { git: onMain, listing: { sessions: [] }, env }), ["chosen"])
})

test("a provisioned run keeps the supervisor's label", async () => {
  assert.deepEqual(await session("prov", { git: onMain, listing: { sessions: [] }, provisioned: true }), [])
})

test("git runs in the session root with a bounded wait", async () => {
  const argvs = []
  await session("argv", { git: onMain, listing: { sessions: [] }, argvs })
  assert.deepEqual(argvs[0], {
    argv: ["git", "-C", "/work/root", "rev-parse", "--show-toplevel"],
    init: { timeoutMs: 5000 },
  })
  assert.deepEqual(argvs[1].argv, ["git", "-C", "/work/root", "branch", "--show-current"])
})

test("a top-level with no usable name falls back to the directory's name", async () => {
  const git = (args) => (args === "rev-parse --show-toplevel" ? "/" : null)
  assert.deepEqual(await session("slash", { git, listing: { sessions: [] } }), ["claude-root"])
})

test("a resumed session with a title posts no default label", async () => {
  const fs = { read: async () => '{"type":"custom-title","customTitle":"mine","sessionId":"no-transcript"}\n' }
  assert.deepEqual(await session("resumed", { git: onMain, listing: { sessions: [] }, fs }), [])
})

test("an unreadable listing keeps the base label", async () => {
  const cases = {
    "a failed read": () => ({ status: 500, ok: false, headers: {}, text: "boom" }),
    "a body that is not JSON": () => ({ status: 200, ok: true, headers: {}, text: "<html>" }),
    "a body with no sessions array": () => ({ status: 200, ok: true, headers: {}, text: JSON.stringify({ sessions: "none" }) }),
    "a body that is null": () => ({ status: 200, ok: true, headers: {}, text: "null" }),
    "a transport failure": () => { throw new Error("connection reset") },
  }
  for (const [name, sessions] of Object.entries(cases)) {
    assert.deepEqual(await session("unreadable-" + name.replace(/\W+/g, "-"), { git: onMain, sessions }), ["claude-avenor"], name)
  }
})

test("a host that cannot name the session root still starts the layer without a label", async () => {
  const logs = []
  assert.deepEqual(await session("norootd", { git: onMain, listing: { sessions: [] }, rootRejects: true, logs }), [])
  assert.ok(logs.some((line) => line.includes("peer layer on (self-registered run")), logs.join("\n"))
})
