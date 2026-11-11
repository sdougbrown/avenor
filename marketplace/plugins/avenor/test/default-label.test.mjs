// A self-registered session with no environment name takes a label from the
// work it is in, and a person's name from the environment still wins.

import assert from "node:assert/strict"
import { fileURLToPath } from "node:url"
import test from "node:test"

const PLUGIN = fileURLToPath(new URL("..", import.meta.url))
const RUN = "claude-a001f5"

async function session(tag, {
  toplevel = "/work/avenor",
  listing = { sessions: [] },
  sessions,
  env = {},
  provisioned = false,
  fs,
  argvs = [],
  rootRejects = false,
  logs = [],
  brokerUrl = "http://127.0.0.1:8787",
  hostname = "fenwick",
} = {}) {
  const posts = []
  const hooks = {}
  const fullEnv = {
    AVENOR_BROKER_URL: brokerUrl,
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
        if (argv[0] === "hostname") return { exitCode: 0, stdout: hostname + "\n", stderr: "" }
        const args = argv.slice(3).join(" ")
        const answer = args === "rev-parse --show-toplevel" && toplevel !== null ? toplevel : null
        return { exitCode: answer === null ? 1 : 0, stdout: answer ?? "", stderr: "" }
      },
    },
    fs: fs ?? { read: async () => { throw new Error("no transcript") } },
    clock: { now: async () => Date.now(), sleep: () => new Promise(() => {}) },
    ui: { log: async (line) => logs.push(line) },
    http: {
      fetch: async (url, init = {}) => {
        const path = String(url).slice(brokerUrl.length)
        if (init.method === "POST") {
          posts.push({ path, body: JSON.parse(init.body) })
        }
        const ok = (body) => ({ status: 200, ok: true, headers: {}, text: JSON.stringify(body) })
        if (path === "/register") return ok({ run_id: RUN, token: "tok" })
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

test("the label is the agent and the repository, with no branch", async () => {
  const argvs = []
  assert.deepEqual(await session("plain", { argvs }), ["claude-avenor"])
  assert.deepEqual(argvs.map((a) => a.argv.slice(3).join(" ")), ["rev-parse --show-toplevel"])
})

test("a broker on another machine puts the hostname first", async () => {
  const brokerUrl = "https://broker.example.com"
  const argvs = []
  assert.deepEqual(await session("remote", { brokerUrl, argvs }), ["fenwick-claude-avenor"])
  assert.ok(argvs.some((a) => a.argv[0] === "hostname"))
})

test("a broker on this machine's own name adds no prefix", async () => {
  assert.deepEqual(await session("self-named", { brokerUrl: "https://fenwick.lan:8443" }), ["claude-avenor"])
})

test("a label another peer holds gets the run tail", async () => {
  const listing = { sessions: [{ run_id: "claude-1", label: "claude-avenor" }] }
  assert.deepEqual(await session("taken", { listing }), ["claude-avenor-a001f5"])
})

test("a listing that cannot be trusted gets the run tail", async () => {
  const cases = {
    "a partial listing": () => ({ status: 200, ok: true, headers: {}, text: JSON.stringify({ sessions: [], peers_error: "one supervisor unreachable" }) }),
    "a failed read": () => ({ status: 500, ok: false, headers: {}, text: "boom" }),
    "a body that is not JSON": () => ({ status: 200, ok: true, headers: {}, text: "<html>" }),
    "a body with no sessions array": () => ({ status: 200, ok: true, headers: {}, text: JSON.stringify({ sessions: "none" }) }),
    "a body that is null": () => ({ status: 200, ok: true, headers: {}, text: "null" }),
    "a transport failure": () => { throw new Error("connection reset") },
  }
  for (const [name, sessions] of Object.entries(cases)) {
    assert.deepEqual(await session("unreadable-" + name.replace(/\W+/g, "-"), { sessions }), ["claude-avenor-a001f5"], name)
  }
})

test("outside a repository the directory names the work and the run tail follows", async () => {
  assert.deepEqual(await session("norepo", { toplevel: null }), ["claude-root-a001f5"])
})

test("a top-level with no usable name falls back to the directory", async () => {
  assert.deepEqual(await session("slash", { toplevel: "/" }), ["claude-root-a001f5"])
})

test("git runs in the session root with a bounded wait", async () => {
  const argvs = []
  await session("argv", { argvs })
  assert.deepEqual(argvs[0], {
    argv: ["git", "-C", "/work/root", "rev-parse", "--show-toplevel"],
    init: { timeoutMs: 5000 },
  })
})

test("an environment name wins over the derived one", async () => {
  assert.deepEqual(await session("env", { env: { AVENOR_SESSION_NAME: "chosen" } }), ["chosen"])
})

test("a provisioned run keeps the supervisor's label", async () => {
  assert.deepEqual(await session("prov", { provisioned: true }), [])
})

test("a resumed session with a title posts no default label", async () => {
  const fs = { read: async () => '{"type":"custom-title","customTitle":"mine","sessionId":"no-transcript"}\n' }
  assert.deepEqual(await session("resumed", { fs }), [])
})

test("a host that cannot name the session root still starts the layer without a label", async () => {
  const logs = []
  assert.deepEqual(await session("norootd", { rootRejects: true, logs }), [])
  assert.ok(logs.some((line) => line.includes("peer layer on (self-registered run")), logs.join("\n"))
})
