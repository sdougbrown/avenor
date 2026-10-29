import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  framePeerMessage,
  identityPlan,
  peerDirFor,
  readRecord,
  recordIsLive,
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
