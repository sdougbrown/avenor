// The attach response is simulated; broker grant semantics are tested in Go.

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import test from "node:test";

const PLUGIN = fileURLToPath(new URL("..", import.meta.url));

test("a successful attach joins as the run and writes no name", async () => {
  const posts = [];
  const logs = []
  const hooks = {}
  const env = {
    AVENOR_BROKER_URL: "http://broker.test",
    AVENOR_RUN_ID: "rt_child",
    AVENOR_BROKER_TOKEN: "launch-grant",
    AVENOR_LABEL: "child-a",
  }
  const d = {
    env: { get: async (name) => env[name] },
    session: { version: async () => ({ version: "2.1.311", base: "2.1.311" }) },
    clock: {
      now: async () => Date.now(),
      // Suspend the polling loop without keeping the process alive.
      sleep: () => new Promise(() => {}),
    },
    ui: { log: async (line) => logs.push(line) },
    http: {
      fetch: async (url, init = {}) => {
        const path = String(url).slice(env.AVENOR_BROKER_URL.length)
        if (init.method === "POST") {
          posts.push({ path, body: init.body === undefined ? null : JSON.parse(init.body) })
        }
        if (path === "/attach") {
          return { status: 200, ok: true, headers: {}, text: JSON.stringify({ run_id: "rt_child", token: "run-own" }) }
        }
        return { status: 401, ok: false, headers: {}, text: "unauthorized" }
      },
    },
    tool: { register: async () => { throw new Error("no tool registration expected") } },
  }

  // A query string gives the fixture its own module instance: the layer's
  // state (broker, armed flag, poll loop) is module-scoped.
  const mod = await import(PLUGIN + "/hooks/register.js?attach-fixture")
  await mod.register((event, matcher, hook) => {
    hooks[event] = typeof matcher === "function" ? matcher : hook
  })
  await hooks["session.start"](d, { cwd: "/tmp", isInteractive: true })
  // One macrotask so the poll loop's first request has landed before the
  // assertions read the ledger.
  await new Promise((resolve) => setImmediate(resolve))

  assert.deepEqual(posts.map((p) => p.path), ["/attach", "/poll-control"], JSON.stringify(posts))
  assert.deepEqual(posts[0].body, { run_id: "rt_child", token: "launch-grant" })
  assert.deepEqual(posts[1].body, { run_id: "rt_child", token: "run-own", agent_only: true })
  assert.ok(logs.join("\n").includes("peer layer on (provisioned run rt_child"), logs.join("\n"))
  assert.ok(posts.every((p) => p.path !== "/rename" && p.path !== "/session_name"))
  assert.equal(JSON.stringify(posts).includes("child-a"), false, JSON.stringify(posts))
})
