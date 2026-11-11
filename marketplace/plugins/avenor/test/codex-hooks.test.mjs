import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import http from "node:http";
import { execFileSync, spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { runTail, shortHost } from "../hooks/label.js";
import {
  defaultSessionLabel,
  framePeerMessage,
  identityPlan,
  peerDirFor,
  readRecord,
  recordIsLive,
  threadGone,
  withSessionLock,
  recordPathFor,
  registerOrAttach,
  writeRecord,
} from "../hooks/codex/common.mjs";

const baseEnv = { AVENOR_BROKER_URL: "http://127.0.0.1:4242" };

test("identity ladder chooses attach, register or off", () => {
  const attach = identityPlan({
    ...baseEnv,
    AVENOR_RUN_ID: "rt_child",
    AVENOR_BROKER_TOKEN: "grant",
    AVENOR_LABEL: "child-a",
  });
  assert.equal(attach.kind, "attach");
  assert.equal(attach.provisioned, true);
  assert.equal(attach.label, "child-a");

  assert.equal(identityPlan({ ...baseEnv, AVENOR_RUN_ID: "rt_hand" }).kind, "register");
  assert.equal(identityPlan(baseEnv).kind, "register");

  const noBroker = identityPlan({});
  assert.equal(noBroker.kind, "disabled");
  assert.match(noBroker.reason, /AVENOR_BROKER_URL/);

  const launcherOwned = identityPlan({ ...baseEnv, AVENOR_RUNTIME_ID: "rt_1" });
  assert.equal(launcherOwned.kind, "disabled");
  assert.match(launcherOwned.reason, /launcher owns/);
});

test("attach keeps the provisioned identity and never registers", async () => {
  const calls = [];
  const record = await registerOrAttach(
    { kind: "attach", url: baseEnv.AVENOR_BROKER_URL, runID: "rt_child", token: "grant", label: "child-a" },
    async (url, route, body) => {
      calls.push({ url, route, body });
      return { status: 200, ok: true, body: { run_id: "rt_child", token: "run-own" } };
    },
  );
  assert.deepEqual(calls.map((c) => c.route), ["/attach"]);
  assert.equal(record.run_id, "rt_child");
  assert.equal(record.run_token, "run-own");
  assert.equal(record.provisioned, true);
});

test("an attach refusal is reported with the broker's reason", async () => {
  await assert.rejects(
    () => registerOrAttach(
      { kind: "attach", url: baseEnv.AVENOR_BROKER_URL, runID: "rt_child", token: "spent", label: "" },
      async () => ({ status: 409, ok: false, body: { error: "attach-consumed" } }),
    ),
    /refused this launch's attach for rt_child: attach-consumed/,
  );
  await assert.rejects(
    () => registerOrAttach(
      { kind: "attach", url: baseEnv.AVENOR_BROKER_URL, runID: "rt_child", token: "grant", label: "" },
      async () => ({ status: 200, ok: true, body: {} }),
    ),
    /no run credential/,
  );
});

test("self-registration mints a fresh id when the broker holds the first one", async () => {
  const ids = [];
  let attempt = 0;
  const record = await registerOrAttach(
    { kind: "register", url: baseEnv.AVENOR_BROKER_URL, runID: "", label: "" },
    async (_url, route, body) => {
      assert.equal(route, "/register");
      ids.push(body.run_id);
      attempt += 1;
      return attempt === 1
        ? { status: 409, ok: false, body: null }
        : { status: 200, ok: true, body: { run_id: body.run_id, token: "minted" } };
    },
  );
  assert.equal(ids.length, 2);
  assert.notEqual(ids[0], ids[1]);
  assert.match(record.run_id, /^codex-[0-9a-f]{6}$/);
  assert.equal(record.run_token, "minted");
  assert.equal(record.provisioned, false);
});

test("a network failure reading the record is not live", async () => {
  const record = { broker_url: "http://127.0.0.1:4242", run_id: "rt_1", run_token: "tok" };
  assert.equal(await recordIsLive(record, async () => {
    throw new Error("connection refused");
  }), false);
});

test("a non-retryable registration refusal is reported", async () => {
  await assert.rejects(
    () => registerOrAttach(
      { kind: "register", url: baseEnv.AVENOR_BROKER_URL, runID: "", label: "" },
      async () => ({ status: 500, ok: false, body: null }),
    ),
    /refused registration for codex-[0-9a-f]{6} with status 500/,
  );
});

test("a live record survives a resume; a refused heartbeat does not", async () => {
  const record = { broker_url: "http://127.0.0.1:4242", run_id: "rt_1", run_token: "tok" };
  assert.equal(await recordIsLive(record, async (_url, route, body) => {
    assert.equal(route, "/heartbeat");
    assert.equal(body.run_id, "rt_1");
    return { status: 200, ok: true, body: {} };
  }), true);
  assert.equal(await recordIsLive(record, async () => ({ status: 401, ok: false, body: null })), false);
});

test("records are owner-only and reject an unsafe session id", () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "avenor-codex-"));
  const session = "01a1-test";
  writeRecord({
    version: 1,
    session_id: session,
    broker_url: "http://127.0.0.1:4242",
    run_id: "rt_1",
    run_token: "tok",
    label: "",
    provisioned: false,
  }, home);
  const file = recordPathFor(home, session);
  assert.equal(fs.statSync(file).mode & 0o777, 0o600);
  assert.equal(readRecord(session, home).run_id, "rt_1");
  assert.equal(peerDirFor(home, "../escape"), "");
  assert.equal(peerDirFor(home, "a/b"), "");
});

test("an inbound frame names the sender, the ask id and the reply action", () => {
  const frame = framePeerMessage({
    fromRunId: "rt_peer",
    sender: "claude-parent",
    messageId: "av1.ask",
    message: "pong",
    role: "agent",
    expectsReply: true,
  });
  assert.match(frame, /claude-parent/);
  assert.match(frame, /rt_peer/);
  assert.match(frame, /pong/);
  assert.match(frame, /action: "reply"/);
  assert.match(frame, /reply_to: "av1\.ask"/);

  const tell = framePeerMessage({
    fromRunId: "rt_peer", sender: "", messageId: "", message: "hi", role: "", expectsReply: false,
  });
  assert.match(tell, /No reply is required/);
  assert.doesNotMatch(tell, /reply_to/);
});

test("a tell that carries a message id still gets no reply instruction", () => {
  // The reply instruction tracks expectsReply alone, matching frameInboundAsk:
  // a tell with an id is a new message, not an answerable ask.
  const tell = framePeerMessage({
    fromRunId: "rt_peer", sender: "claude-parent", messageId: "av1.tell", message: "fyi", role: "", expectsReply: false,
  });
  assert.match(tell, /av1\.tell/);
  assert.match(tell, /No reply is required/);
  assert.doesNotMatch(tell, /reply_to/);
});

test("thread-gone classification separates a dead thread from a transient failure", () => {
  assert.equal(threadGone("thread not found: 01a1"), true);
  assert.equal(threadGone("unknown session"), true);
  assert.equal(threadGone("connection reset by peer"), false);
  assert.equal(threadGone("timeout"), false);
});

test("the session lock runs its section once and releases", async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "avenor-codex-lock-"));
  const previous = process.env.AVENOR_HOME;
  process.env.AVENOR_HOME = home;
  try {
    let ran = 0;
    const value = await withSessionLock("01a1-lock", async () => {
      ran += 1;
      return "ok";
    });
    assert.equal(value, "ok");
    assert.equal(ran, 1);
    assert.equal(fs.existsSync(path.join(home, "peers", "codex", "01a1-lock", "session.lock")), false);
  } finally {
    if (previous === undefined) delete process.env.AVENOR_HOME;
    else process.env.AVENOR_HOME = previous;
  }
});

test("a stale session lock is reclaimed", async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "avenor-codex-lock-"));
  const previous = process.env.AVENOR_HOME;
  process.env.AVENOR_HOME = home;
  try {
    const dir = path.join(home, "peers", "codex", "01a1-lock");
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(dir, "session.lock"), "999999", { mode: 0o600 });
    let ran = false;
    const value = await withSessionLock("01a1-lock", async () => {
      ran = true;
      return "reclaimed";
    });
    assert.equal(value, "reclaimed");
    assert.equal(ran, true);
    assert.equal(fs.existsSync(path.join(dir, "session.lock")), false);
  } finally {
    if (previous === undefined) delete process.env.AVENOR_HOME;
    else process.env.AVENOR_HOME = previous;
  }
});

const SESSION_START = fileURLToPath(new URL("../hooks/codex/session-start.mjs", import.meta.url));

/** Run the real SessionStart hook against a stub broker; returns what it renamed to. */
async function renamedTo({ cwd, payload = { cwd }, spawnCwd, env = {}, listing = { sessions: [] }, sessions }) {
  const renames = [];
  const bodies = [];
  const broker = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk) => { raw += chunk; });
    req.on("end", () => {
      const reply = (body) => res.end(JSON.stringify(body));
      if (req.url === "/register") return reply({ run_id: JSON.parse(raw).run_id, token: "tok" });
      if (req.url === "/attach") return reply({ run_id: "rt_child", token: "own" });
      if (req.url === "/rename") {
        const body = JSON.parse(raw);
        renames.push(body.name);
        bodies.push(body);
        return reply({});
      }
      if (req.url === "/sessions") {
        if (sessions !== undefined) return sessions(req, res);
        return req.headers["x-avenor-run-token"] === "tok" || req.headers["x-avenor-run-token"] === "own"
          ? reply(listing)
          : (res.statusCode = 401, reply({}));
      }
      res.statusCode = 404;
      reply({});
    });
  });
  await new Promise((resolve) => broker.listen(0, "127.0.0.1", resolve));
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "avenor-codex-start-"));
  const childEnv = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith("AVENOR_")));
  try {
    const child = spawn(process.execPath, [SESSION_START], {
      env: {
        ...childEnv,
        AVENOR_HOME: home,
        AVENOR_BROKER_URL: "http://127.0.0.1:" + broker.address().port,
        ...env,
      },
      cwd: spawnCwd,
      stdio: ["pipe", "ignore", "ignore"],
    });
    child.stdin.end(JSON.stringify({ session_id: "01a1-label", hook_event_name: "SessionStart", source: "startup", ...payload }));
    await new Promise((resolve) => child.on("close", resolve));
    return { renames, bodies, record: readRecord("01a1-label", home) };
  } finally {
    broker.close();
  }
}

function repo(name, branch) {
  const dir = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "avenor-codex-repo-")), name);
  fs.mkdirSync(dir);
  execFileSync("git", ["-C", dir, "init", "-q", "-b", branch]);
  return dir;
}

const tail = (record) => runTail(record.run_id);

test("a self-registered session is named after its repository", async () => {
  const { renames, bodies, record } = await renamedTo({ cwd: repo("avenor", "main") });
  assert.deepEqual(renames, ["codex-avenor"]);
  assert.equal(record.label, "codex-avenor");
  // The derived label says so in the body, so a broker that checks names makes
  // it unique instead of refusing it.
  assert.deepEqual(bodies, [{ name: "codex-avenor", default: true, run_id: record.run_id, token: "tok" }]);
});

test("the branch is not part of the default label", async () => {
  const { renames } = await renamedTo({ cwd: repo("avenor", "feat/x") });
  assert.deepEqual(renames, ["codex-avenor"]);
});

test("a default label another peer holds gets the run tail", async () => {
  const { renames, record } = await renamedTo({
    cwd: repo("avenor", "main"),
    listing: { sessions: [{ label: "codex-avenor" }] },
  });
  assert.deepEqual(renames, ["codex-avenor-" + tail(record)]);
});

test("a listing that cannot be trusted gets the run tail", async () => {
  const partial = await renamedTo({
    cwd: repo("avenor", "main"),
    listing: { sessions: [], peers_error: "peers unavailable" },
  });
  assert.deepEqual(partial.renames, ["codex-avenor-" + tail(partial.record)]);

  const failed = await renamedTo({
    cwd: repo("avenor", "main"),
    sessions: (_req, res) => {
      res.statusCode = 500;
      res.end(JSON.stringify({ sessions: [] }));
    },
  });
  assert.deepEqual(failed.renames, ["codex-avenor-" + tail(failed.record)]);
  assert.equal(failed.record.label, "codex-avenor-" + tail(failed.record));

  const dropped = await renamedTo({
    cwd: repo("avenor", "main"),
    sessions: (req) => req.socket.destroy(),
  });
  assert.deepEqual(dropped.renames, ["codex-avenor-" + tail(dropped.record)]);
});

test("a hook payload without a usable cwd names the session from the hook's own directory", async () => {
  for (const payload of [{}, { cwd: "" }]) {
    const { renames } = await renamedTo({ payload, spawnCwd: repo("avenor", "feat/x") });
    assert.deepEqual(renames, ["codex-avenor"]);
  }
});

test("a directory outside a repository names the session by its own name and the run tail", async () => {
  const dir = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "avenor-codex-plain-")), "scratch");
  fs.mkdirSync(dir);
  const { renames, record } = await renamedTo({ cwd: dir, env: { GIT_CEILING_DIRECTORIES: path.dirname(dir) } });
  assert.deepEqual(renames, ["codex-scratch-" + tail(record)]);
});

test("a broker on another machine puts this machine's hostname first", async () => {
  const record = { broker_url: "https://broker.example.com", run_id: "codex-a001f5", run_token: "t" };
  const git = async () => "/work/avenor";
  const fetchGet = async () => ({ ok: true, body: { sessions: [] } });
  assert.equal(
    await defaultSessionLabel("/work/avenor", record, git, fetchGet),
    shortHost(os.hostname()) + "-codex-avenor",
  );
  assert.equal(
    await defaultSessionLabel("/work/avenor", { ...record, broker_url: "http://127.0.0.1:8787" }, git, fetchGet),
    "codex-avenor",
  );
});

test("a launch label from the environment wins over the derived one", async () => {
  const { renames, bodies, record } = await renamedTo({ cwd: repo("avenor", "main"), env: { AVENOR_LABEL: "chosen" } });
  assert.deepEqual(renames, ["chosen"]);
  assert.equal(record.label, "chosen");
  // A person's name is an explicit rename, so it carries no default marker.
  assert.deepEqual(bodies, [{ name: "chosen", run_id: record.run_id, token: "tok" }]);
});

test("a provisioned run is not renamed", async () => {
  const { renames, record } = await renamedTo({
    cwd: repo("avenor", "main"),
    env: { AVENOR_RUN_ID: "rt_child", AVENOR_BROKER_TOKEN: "grant" },
  });
  assert.deepEqual(renames, []);
  assert.equal(record.provisioned, true);
  assert.equal(record.label, "");
});
