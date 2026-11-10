// The peer layer follows the host's session name: /rename publishes the
// transcript's title to the broker as the harness name, and a resumed session
// publishes the title it already has.

import assert from "node:assert/strict"
import { mkdtemp, mkdir, writeFile } from "node:fs/promises"
import { readFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

const PLUGIN = fileURLToPath(new URL("..", import.meta.url))
const ROOT = "/work/my.repo"
const SESSION = "11111111-2222-3333-4444-555555555555"

async function fixture(tag, transcript, options = {}) {
  const home = await mkdtemp(join(tmpdir(), "rename-follow-"))
  const dir = join(home, ".claude", "projects", "-work-my-repo")
  await mkdir(dir, { recursive: true })
  if (transcript !== null) {
    await writeFile(join(dir, SESSION + ".jsonl"), transcript)
  }
  const posts = []
  const hooks = {}
  const env = { AVENOR_BROKER_URL: "http://broker.test", HOME: home, ...(options.env ?? {}) }
  const d = {
    env: { get: async (name) => env[name] },
    session: {
      version: async () => ({ version: "2.1.311", base: "2.1.311" }),
      id: async () => SESSION,
      root: async () => ROOT,
    },
    fs: { read: async (path) => readFile(path, "utf8") },
    clock: { now: async () => Date.now(), sleep: () => new Promise(() => {}) },
    ui: { log: async () => {} },
    http: {
      fetch: async (url, init = {}) => {
        const path = String(url).slice(env.AVENOR_BROKER_URL.length)
        if (init.method === "POST") {
          posts.push({ path, body: JSON.parse(init.body) })
        }
        if (path === "/register") {
          return { status: 200, ok: true, headers: {}, text: JSON.stringify({ run_id: "claude-abc123", token: "tok" }) }
        }
        if (path === "/attach") {
          return { status: 200, ok: true, headers: {}, text: JSON.stringify({ run_id: "rt_child", token: "run-own" }) }
        }
        if (path === "/session_name") {
          return options.refuseName === true
            ? { status: 400, ok: false, headers: {}, text: JSON.stringify({ error: "invalid name", code: "invalid-name" }) }
            : { status: 200, ok: true, headers: {}, text: "{}" }
        }
        return { status: 401, ok: false, headers: {}, text: "unauthorized" }
      },
    },
  }
  const mod = await import(PLUGIN + "/hooks/register.js?rename-follow-" + tag)
  await mod.register((event, matcher, hook) => {
    hooks[event] = typeof matcher === "function" ? matcher : hook
  })
  await hooks["session.start"](d, { cwd: ROOT, isInteractive: true })
  await new Promise((resolve) => setImmediate(resolve))
  const published = () => posts.filter((p) => p.path === "/session_name").map((p) => p.body.name)
  return { d, hooks, home, posts, published }
}

function title(name) {
  return JSON.stringify({ type: "custom-title", customTitle: name, sessionId: SESSION }) + "\n"
}

test("a bare /rename publishes the generated title from the transcript, after the command ran", async () => {
  const f = await fixture("bare", title("old-name"))
  assert.deepEqual(f.published(), ["old-name"], "a resumed session publishes its title at start")
  const order = []
  await f.hooks["command.run"](f.d, { command: "rename", args: "" }, async () => {
    order.push("next")
    await writeFile(
      join(f.home, ".claude", "projects", "-work-my-repo", SESSION + ".jsonl"),
      title("old-name") + title("fix-the-thing \"quoted\""),
    )
    return { ref: 1 }
  })
  assert.deepEqual(order, ["next"])
  assert.deepEqual(f.published(), ["old-name", "fix-the-thing \"quoted\""])
  assert.equal(f.posts.at(-1).body.run_id, "claude-abc123")
  assert.equal(f.posts.at(-1).body.token, "tok")
})

test("/rename <name> falls back to the typed name when the transcript has no title", async () => {
  const f = await fixture("typed", null)
  assert.deepEqual(f.published(), [])
  const ran = await f.hooks["command.run"](f.d, { command: "rename", args: "  mrclaude " }, async () => ({ ref: 7 }))
  assert.deepEqual(ran, { ref: 7 })
  assert.deepEqual(f.published(), ["mrclaude"])
})

test("a session that was never named publishes nothing", async () => {
  const f = await fixture("unnamed", "")
  await f.hooks["command.run"](f.d, { command: "rename", args: "" }, async () => ({ ref: 1 }))
  assert.deepEqual(f.published(), [])
})

test("a title past the broker's 64-character rule is cut, not dropped", async () => {
  const f = await fixture("long", title("x".repeat(80)))
  assert.deepEqual(f.published(), ["x".repeat(64)])
})

test("a title cut at the limit keeps an astral character whole", async () => {
  const f = await fixture("astral", title("x".repeat(63) + "\u{1F40E}tail"))
  assert.deepEqual(f.published(), ["x".repeat(63) + "\u{1F40E}"])
})

test("a refused name leaves /rename's own result untouched", async () => {
  const f = await fixture("refused", title("first"), { refuseName: true })
  const ran = await f.hooks["command.run"](f.d, { command: "rename", args: "" }, async () => ({ ref: 3 }))
  assert.deepEqual(ran, { ref: 3 })
})

test("a record that only quotes the field is not a title", async () => {
  const forged = JSON.stringify({ type: "user", content: { "\"customTitle": "x" } }) + "\n" +
    '{"type":"custom-title","customTitle":"other","sessionId":"another-session"}\n'
  const f = await fixture("forged", forged)
  assert.deepEqual(f.published(), [])
})

test("a provisioned run keeps the supervisor's label and publishes nothing", async () => {
  const f = await fixture("provisioned", title("launch-label"), {
    env: { AVENOR_RUN_ID: "rt_child", AVENOR_BROKER_TOKEN: "launch-grant" },
  })
  await f.hooks["command.run"](f.d, { command: "rename", args: "named-by-hand" }, async () => ({ ref: 1 }))
  assert.deepEqual(f.published(), [])
  assert.ok(f.posts.some((p) => p.path === "/attach"))
})
