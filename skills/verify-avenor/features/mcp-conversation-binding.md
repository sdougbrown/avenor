# MCP conversation launch & inbox — `avenor mcp --bind-host … --bind-session …`

A trusted host that owns one native conversation launches Avenor's MCP server for
that conversation, optionally receiving its messaging identity on a private pipe
it inherited. The launch binds on the supervisor the server owns before it serves
a tool call, so every child the MCP spawn tool forwards names the conversation's
run as its broker parent, `message` (non-`resume`) and `peers` speak as that
bound run, and `avenor://inbox` delivers incoming broker messages through MCP
resource subscriptions (`2025-11-25` `resources/subscribe` and `2026-07-28`
`subscriptions/listen`) under the same advisory lock `avenor feed broker` takes.
See `docs/mcp.md` (Bound conversation inbox), `docs/messaging.md` (MCP
conversation launch), and `docs/control-protocol.md` (`spawn_mcp`).

## The launch flags

```sh
avenor mcp --transport stdio \
  --supervisor-socket <socket> --no-autostart \
  --bind-host poo --bind-session <native-session-id> [--bind-output-fd 3]
```

- `--bind-host` and `--bind-session` travel together, only on `--transport stdio`.
- `--bind-output-fd` is optional alongside them (refused if passed without both).
  When passed, it names an inherited pipe open for writing (`>= 3`; never `0`,
  `1`, or `2`, never a regular file, closed descriptor, or read-only pipe). The
  launch makes it non-blocking, marks it close-on-exec before autostarting any
  supervisor, writes `{"broker_url","run_id","run_token","generation","created"}`
  as one JSON line, and closes it.
- A launch that cannot bind (or hand off when a pipe was given) exits non-zero;
  there is no unbound fallback and no retry.
- When a client subscribes to `avenor://inbox`, the server acquires
  `<XDG_STATE_HOME>/avenor/feed-locks/<BrokerKey>.lock` (`durablefile.TryLock`),
  polls the bound run's broker inbox, records asks in
  `<XDG_STATE_HOME>/avenor/feed-asks/<BrokerKey>`, and emits
  `notifications/resources/updated`. Unsubscribing, cancelling
  `subscriptions/listen`, EOF, or closing the server stops the poller and
  releases the lock. (A deliberately different `avenor feed broker --lock-dir`
  is a separate lock namespace.)

## Typed refusals

| Stage | Exit | `stderr` |
|---|---|---|
| partial conversation declaration | 2 | `--bind-host and --bind-session are supplied together or not at all` |
| descriptor without conversation | 2 | `--bind-output-fd requires --bind-host and --bind-session` |
| shared transport | 2 | `--bind-host is only supported with --transport stdio, a transport that serves one conversation` |
| stdio descriptor | 2 | `--bind-output-fd must name a writable inherited descriptor other than 0, 1 and 2, …` |
| not a pipe | 2 | `--bind-output-fd <fd> is not a pipe; the conversation's credentials are handed over on a private pipe and nowhere else` |
| closed descriptor | 2 | `--bind-output-fd <fd> is not an inherited open descriptor: …` |
| read-only pipe | 2 | `--bind-output-fd <fd> is not writable; the handoff pipe must be open for writing` |
| bind or handoff failure | 1 | `avenor mcp: conversation bind for host "poo" session "…": …` |

No refusal text carries the credential.

## Driving it

Build the binary and run the driver below (`mcp-bind-drive.py`, stdlib only)
against the real `./avenor` binary:

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
mkdir -p "$SCRATCH/home" "$SCRATCH/avenor" "$SCRATCH/state"
EVIDENCE=$(mktemp -d /tmp/avenor-verify-evidence.XXXXXX)
python3 mcp-bind-drive.py ./avenor "$SCRATCH" "$EVIDENCE/frames.ndjson"
```

```python
#!/usr/bin/env python3
"""Drive MCP conversation binding and avenor://inbox over real stdio.

Usage: mcp-bind-drive.py <avenor> <scratch> <frames.ndjson>

Proves:
1. Pipe-mode #696 launch, descriptor refusals, closed-consumer refusal, and
   relaunch identity reuse (learning run_token privately to scan raw output).
2. Pipe-free relaunch of the same conversation (`--bind-host` + `--bind-session`
   without `--bind-output-fd`).
3. MCP 2025-11-25 (`resources/subscribe`) wakeup on real broker tell + ask,
   destructive `resources/read("avenor://inbox")`, MCP `message(action="reply")`
   delivering the answer to the sender, and shared lock exclusion against a live
   `avenor feed broker` sharing `XDG_STATE_HOME`.
4. MCP 2026-07-28 (`server/discover` + `subscriptions/listen` with
   `resourceSubscriptions`), `notifications/subscriptions/acknowledged`,
   overlapping-listen refusal with no ack, `notifications/resources/updated`
   carrying `io.modelcontextprotocol/subscriptionId`, `ttlMs: 0` /
   `cacheScope: "private"` on read, `notifications/cancelled` releasing the
   lock, and `avenor feed broker` taking over afterwards.
5. Zero leakage of `run_token` across every raw MCP frame and stderr file
   (checked BEFORE redacting evidence).
"""
import json, os, select, socket, subprocess, sys, threading, time

BIN, SCRATCH, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
SOCK = os.path.join(SCRATCH, "stable.sock")
ENV = {
    "PATH": os.environ["PATH"],
    "HOME": os.path.join(SCRATCH, "home"),
    "AVENOR_HOME": os.path.join(SCRATCH, "avenor"),
    "XDG_STATE_HOME": os.path.join(SCRATCH, "state"),
    "TMPDIR": SCRATCH,
}
ev = open(OUT, "a")
results = []
raw_frames = []
stderr_paths = []
secrets = []


def redact(obj):
    if isinstance(obj, dict):
        return {
            k: ("<redacted>" if k == "run_token" and isinstance(v, str) and v else redact(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [redact(x) for x in obj]
    if isinstance(obj, str):
        out = obj
        for s in secrets:
            if s:
                out = out.replace(s, "<redacted>")
        return out
    return obj


def check(name, ok, detail):
    results.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " :: " + redact(detail))


def record_frame(direction, name, payload):
    raw_frames.append((direction, name, json.dumps(payload)))
    ev.write(json.dumps({"direction": direction, "check": name, "frame": redact(payload)}) + "\n")


def wait_socket(path, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(path)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def read_pipe_line(fd, timeout):
    buf = b""
    deadline = time.time() + timeout
    while b"\n" not in buf:
        rem = deadline - time.time()
        if rem <= 0:
            return b""
        ready, _, _ = select.select([fd], [], [], rem)
        if not ready:
            return b""
        chunk = os.read(fd, 4096)
        if not chunk:
            return buf
        buf += chunk
    return buf


class BufferedMCPClient:
    """Buffered newline-delimited JSON-RPC client preserving surplus bytes."""

    def __init__(self, proc):
        self.proc = proc
        self.fd = proc.stdout.fileno()
        self.buf = b""
        self.responses = {}
        self.notifications = []

    def send(self, obj, label="send"):
        record_frame("request", label, obj)
        self.proc.stdin.write((json.dumps(obj) + "\n").encode())
        self.proc.stdin.flush()

    def _read_one_frame(self, deadline):
        while b"\n" not in self.buf:
            rem = deadline - time.time()
            if rem <= 0:
                return None
            ready, _, _ = select.select([self.fd], [], [], rem)
            if not ready:
                return None
            chunk = os.read(self.fd, 65536)
            if not chunk:
                return None
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        if not line.strip():
            return self._read_one_frame(deadline)
        msg = json.loads(line.decode())
        record_frame("response", msg.get("method", str(msg.get("id", "frame"))), msg)
        return msg

    def wait_response(self, req_id, timeout=15):
        if req_id in self.responses:
            return self.responses.pop(req_id)
        deadline = time.time() + timeout
        while True:
            msg = self._read_one_frame(deadline)
            if msg is None:
                return None
            if "id" in msg and "method" not in msg:
                if msg["id"] == req_id:
                    return msg
                self.responses[msg["id"]] = msg
            else:
                self.notifications.append(msg)

    def wait_notification(self, method, predicate=None, timeout=15):
        for i, n in enumerate(self.notifications):
            if n.get("method") == method and (predicate is None or predicate(n)):
                return self.notifications.pop(i)
        deadline = time.time() + timeout
        while True:
            msg = self._read_one_frame(deadline)
            if msg is None:
                return None
            if "id" in msg and "method" not in msg:
                self.responses[msg["id"]] = msg
            else:
                if msg.get("method") == method and (predicate is None or predicate(msg)):
                    return msg
                self.notifications.append(msg)

    def rpc(self, req_id, method, params, timeout=15):
        self.send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}, method)
        return self.wait_response(req_id, timeout=timeout)

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=15)
        return self.proc.returncode


def launch_pipe(session, stderr_name):
    read_fd, write_fd = os.pipe()
    if write_fd < 3:
        raise SystemExit("pipe write fd %d < 3" % write_fd)
    err_path = os.path.join(SCRATCH, stderr_name)
    stderr_paths.append(err_path)
    cmd = [
        BIN, "mcp", "--transport", "stdio",
        "--supervisor-socket", SOCK, "--no-autostart",
        "--bind-host", "poo", "--bind-session", session,
        "--bind-output-fd", str(write_fd),
    ]
    err = open(err_path, "wb")
    proc = subprocess.Popen(
        cmd, env=ENV, pass_fds=(write_fd,), bufsize=0,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
    )
    os.close(write_fd)
    line = read_pipe_line(read_fd, 30)
    os.close(read_fd)
    return proc, line


def launch_nopipe(session, stderr_name):
    err_path = os.path.join(SCRATCH, stderr_name)
    stderr_paths.append(err_path)
    cmd = [
        BIN, "mcp", "--transport", "stdio",
        "--supervisor-socket", SOCK, "--no-autostart",
        "--bind-host", "poo", "--bind-session", session,
    ]
    err = open(err_path, "wb")
    proc = subprocess.Popen(
        cmd, env=ENV, bufsize=0,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
    )
    return proc


def sender_env(sender):
    return dict(
        ENV,
        AVENOR_BROKER_URL=sender["broker_url"],
        AVENOR_RUN_ID=sender["run_id"],
        AVENOR_BROKER_TOKEN=sender["run_token"],
    )


def send_tell(sender, target_run_id, msg_text):
    res = subprocess.run(
        [BIN, "message", "--action", "tell", "--target", target_run_id, "--message", msg_text],
        env=sender_env(sender), capture_output=True, text=True, timeout=15,
    )
    if res.returncode != 0:
        raise RuntimeError("avenor message tell failed: %s %s" % (res.stdout, res.stderr))


def send_ask_async(sender, target_run_id, msg_text):
    out = {}

    def runner():
        res = subprocess.run(
            [BIN, "message", "--action", "ask", "--target", target_run_id, "--message", msg_text, "--timeout-ms", "15000"],
            env=sender_env(sender), capture_output=True, text=True, timeout=25,
        )
        out["rc"] = res.returncode
        out["stdout"] = res.stdout.strip()
        out["stderr"] = res.stderr.strip()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return t, out


sup = subprocess.Popen(
    [BIN, "stable", "--control-socket", SOCK], env=ENV,
    stdout=open(os.path.join(SCRATCH, "stable.log"), "wb"),
    stderr=subprocess.STDOUT,
)
try:
    check("supervisor-listens", wait_socket(SOCK), SOCK)

    # 1. Refusal checks
    bad = subprocess.run(
        [BIN, "mcp", "--transport", "stdio", "--supervisor-socket", SOCK, "--no-autostart",
         "--bind-host", "poo", "--bind-session", "ses_fd", "--bind-output-fd", "1"],
        env=ENV, capture_output=True, timeout=30,
    )
    check("stdio-descriptor-refused", bad.returncode == 2 and b"other than 0, 1 and 2" in bad.stderr, bad.stderr.decode(errors="replace").strip())

    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    gone = subprocess.Popen(
        [BIN, "mcp", "--transport", "stdio", "--supervisor-socket", SOCK, "--no-autostart",
         "--bind-host", "poo", "--bind-session", "ses_gone", "--bind-output-fd", str(write_fd)],
        env=ENV, pass_fds=(write_fd,), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    os.close(write_fd)
    gone_err = gone.communicate(timeout=30)[1]
    check("closed-consumer-fails-launch", gone.returncode != 0 and b"hand off to the launch pipe" in gone_err, gone_err.decode(errors="replace").strip())

    # 2. Pipe-mode launch learns identity privately + preserves #696 contract
    proc, line = launch_pipe("ses_drive", "mcp_pipe.err")
    try:
        check("handoff-one-json-line", bool(line) and line.endswith(b"\n") and line.count(b"\n") == 1, "%d bytes" % len(line))
        ident = json.loads(line)
        secrets.append(ident["run_token"])
        check("handoff-fields", all(ident.get(k) for k in ("broker_url", "run_id", "run_token", "generation")) and ident.get("created") is True, json.dumps(redact(ident)))
        cli = BufferedMCPClient(proc)
        init = cli.rpc(1, "initialize", {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "drive", "version": "1"}})
        check("pipe-initialize", (init or {}).get("id") == 1 and ((init.get("result") or {}).get("capabilities") or {}).get("resources", {}).get("subscribe") is True, json.dumps(redact(init)))
        cli.send({"jsonrpc": "2.0", "method": "notifications/initialized"}, "initialized")
        tools = cli.rpc(2, "tools/list", {})
        names = [t.get("name") for t in (tools.get("result") or {}).get("tools", [])]
        check("pipe-tools-list", "spawn" in names and "message" in names, "%d tools" % len(names))
        check("pipe-clean-exit", cli.close() == 0, "exit 0")
    finally:
        if proc.poll() is None:
            proc.kill()

    # Bind a second peer to act as sender
    sproc, sline = launch_pipe("ses_sender", "mcp_sender.err")
    sender = json.loads(sline)
    secrets.append(sender["run_token"])
    sproc.stdin.close()
    sproc.wait(timeout=15)

    # 3. Pipe-free relaunch + MCP 2025-11-25 (resources/subscribe) + shared lock + reply
    p25 = launch_nopipe("ses_drive", "mcp_2025.err")
    try:
        c25 = BufferedMCPClient(p25)
        init25 = c25.rpc(1, "initialize", {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "drive-2025", "version": "1"}})
        check("2025-initialize", (init25 or {}).get("result", {}).get("protocolVersion") == "2025-11-25", json.dumps(redact(init25)))
        c25.send({"jsonrpc": "2.0", "method": "notifications/initialized"}, "initialized")
        rlist = c25.rpc(2, "resources/list", {})
        uris = [r.get("uri") for r in (rlist.get("result") or {}).get("resources", [])]
        check("2025-lists-inbox", "avenor://inbox" in uris, str(uris))
        sub = c25.rpc(3, "resources/subscribe", {"uri": "avenor://inbox"})
        check("2025-subscribe-ok", sub is not None and "error" not in sub, json.dumps(redact(sub)))

        # Competing feed broker sharing XDG_STATE_HOME must exit 0 without polling
        feed_env = dict(ENV, AVENOR_BROKER_URL=ident["broker_url"], AVENOR_RUN_ID=ident["run_id"], AVENOR_BROKER_TOKEN=ident["run_token"])
        comp = subprocess.run([BIN, "feed", "broker"], env=feed_env, capture_output=True, timeout=15)
        check("shared-lock-blocks-feed-broker", comp.returncode == 0 and b"consumer lock is held" in comp.stderr, comp.stderr.decode(errors="replace").strip())

        # Send tell + ask from sender via real `avenor message` CLI
        send_tell(sender, ident["run_id"], "hello-2025")
        ask_thread, ask_out = send_ask_async(sender, ident["run_id"], "question-2025")

        # Drain loop until both arrive
        seen = []
        deadline = time.time() + 15
        while len(seen) < 2 and time.time() < deadline:
            c25.wait_notification("notifications/resources/updated", lambda n: (n.get("params") or {}).get("uri") == "avenor://inbox", timeout=5)
            rd = c25.rpc(100 + len(seen), "resources/read", {"uri": "avenor://inbox"})
            payload = json.loads((rd["result"]["contents"][0])["text"])
            seen.extend(payload["messages"])
        ask_msgs = [m for m in seen if m["text"] == "question-2025"]
        ask_id = ask_msgs[0]["id"] if ask_msgs else ""
        check("2025-received-tell-and-ask", len(seen) == 2 and any(m["text"] == "hello-2025" and not m["expects_reply"] for m in seen) and bool(ask_id) and ask_msgs[0]["expects_reply"] and ask_msgs[0]["answerable"], json.dumps(seen))

        empty = c25.rpc(200, "resources/read", {"uri": "avenor://inbox"})
        check("2025-second-read-empty", json.loads(empty["result"]["contents"][0]["text"]) == {"messages": []}, empty["result"]["contents"][0]["text"])

        rep = c25.rpc(201, "tools/call", {"name": "message", "arguments": {"action": "reply", "reply_to": ask_id, "message": "pong-2025"}})
        ask_thread.join(timeout=20)
        check("2025-mcp-reply-lands", not rep["result"].get("isError") and ask_out.get("rc") == 0 and "pong-2025" in ask_out.get("stdout", ""), json.dumps(ask_out))

        unsub = c25.rpc(202, "resources/unsubscribe", {"uri": "avenor://inbox"})
        check("2025-unsubscribe-ok", unsub is not None and "error" not in unsub, json.dumps(redact(unsub)))
        check("2025-clean-exit", c25.close() == 0, "exit 0")
    finally:
        if p25.poll() is None:
            p25.kill()

    # 4. Pipe-free relaunch + MCP 2026-07-28 (server/discover + subscriptions/listen)
    p26 = launch_nopipe("ses_drive", "mcp_2026.err")
    meta26 = {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "drive-2026", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {},
    }
    try:
        c26 = BufferedMCPClient(p26)
        disc = c26.rpc(1, "server/discover", {"_meta": meta26})
        check("2026-discover", ((disc or {}).get("result") or {}).get("capabilities", {}).get("resources", {}).get("subscribe") is True, json.dumps(redact(disc)))

        c26.send({"jsonrpc": "2.0", "id": 10, "method": "subscriptions/listen", "params": {"_meta": meta26, "notifications": {"resourceSubscriptions": ["avenor://inbox"]}}}, "subscriptions/listen")
        ack = c26.wait_notification("notifications/subscriptions/acknowledged", lambda n: (n.get("params") or {}).get("_meta", {}).get("io.modelcontextprotocol/subscriptionId") == 10, timeout=15)
        check("2026-listen-acked", ack is not None and "avenor://inbox" in ((ack.get("params") or {}).get("notifications") or {}).get("resourceSubscriptions", []), json.dumps(redact(ack)))

        # Overlapping listen on the same session must fail with error frame and no ack
        dup = c26.rpc(11, "subscriptions/listen", {"_meta": meta26, "notifications": {"resourceSubscriptions": ["avenor://inbox"]}})
        check("2026-overlapping-listen-refused", dup is not None and "error" in dup, json.dumps(redact(dup)))

        ask2_thread, ask2_out = send_ask_async(sender, ident["run_id"], "question-2026")
        upd = c26.wait_notification(
            "notifications/resources/updated",
            lambda n: (n.get("params") or {}).get("uri") == "avenor://inbox"
            and (n.get("params") or {}).get("_meta", {}).get("io.modelcontextprotocol/subscriptionId") == 10,
            timeout=15,
        )
        check("2026-update-with-subscription-id", upd is not None, json.dumps(redact(upd)))

        rd26 = c26.rpc(12, "resources/read", {"_meta": meta26, "uri": "avenor://inbox"})
        res26 = (rd26 or {}).get("result") or {}
        msgs26 = json.loads(res26["contents"][0]["text"])["messages"]
        ask2_msgs = [m for m in msgs26 if m["text"] == "question-2026"]
        ask2_id = ask2_msgs[0]["id"] if ask2_msgs else ""
        check("2026-read-private-zero-ttl", res26.get("ttlMs") == 0 and res26.get("cacheScope") == "private" and bool(ask2_id) and ask2_msgs[0]["expects_reply"] and ask2_msgs[0]["answerable"], json.dumps(res26))

        rep26 = c26.rpc(13, "tools/call", {"_meta": meta26, "name": "message", "arguments": {"action": "reply", "reply_to": ask2_id, "message": "pong-2026"}})
        ask2_thread.join(timeout=20)
        check("2026-mcp-reply-lands", not rep26["result"].get("isError") and ask2_out.get("rc") == 0 and "pong-2026" in ask2_out.get("stdout", ""), json.dumps(ask2_out))

        # Cancel subscriptions/listen -> releases lock
        c26.send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"_meta": meta26, "requestId": 10}}, "cancelled")
        listen_res = c26.wait_response(10, timeout=15)
        check("2026-listen-cancelled-cleanly", listen_res is not None and "error" not in listen_res, json.dumps(redact(listen_res)))

        # Feed broker can now acquire the lock and deliver a message
        feed_env = dict(ENV, AVENOR_BROKER_URL=ident["broker_url"], AVENOR_RUN_ID=ident["run_id"], AVENOR_BROKER_TOKEN=ident["run_token"])
        fproc = subprocess.Popen([BIN, "feed", "broker"], env=feed_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            time.sleep(0.3)
            send_tell(sender, ident["run_id"], "after-unsub")
            fline = read_pipe_line(fproc.stdout.fileno(), 15)
            fobj = json.loads(fline) if fline else {}
            check("feed-broker-takes-over-after-cancel", fobj.get("text") == "after-unsub", json.dumps(fobj))
        finally:
            fproc.terminate()
            fproc.wait(timeout=15)

        check("2026-clean-exit", c26.close() == 0, "exit 0")
    finally:
        if p26.poll() is None:
            p26.kill()

    # 5. Raw secret leak check across all raw MCP frames and stderr files
    leaked = []
    for direction, name, raw in raw_frames:
        for s in secrets:
            if s in raw:
                leaked.append("frame:%s:%s" % (direction, name))
    for path in stderr_paths:
        data = open(path, "rb").read()
        for s in secrets:
            if s.encode() in data:
                leaked.append("stderr:%s" % os.path.basename(path))
    check("no-credential-leak-in-frames-or-stderr", len(leaked) == 0, str(leaked))
finally:
    sup.terminate()
    sup.wait(timeout=30)

ev.write(json.dumps({"direction": "summary", "frame": results}) + "\n")
ev.close()
failed = [r for r in results if not r["pass"]]
print("SUMMARY %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
```

**Observable end state (the proof):** every check in the driver passes (`SUMMARY N/N passed`). Pipe-mode `#696` handoff and refusals remain intact; pipe-free relaunch binds the same conversation without `--bind-output-fd`; both `2025-11-25` (`resources/subscribe`) and `2026-07-28` (`server/discover` + `subscriptions/listen` + `notifications/subscriptions/acknowledged` + `notifications/resources/updated` with `subscriptionId` + `ttlMs: 0` / `cacheScope: "private"`) deliver broker tells/asks, answer asks via MCP `message(action: "reply")`, reject overlapping modern listens, block a competing `avenor feed broker` sharing `XDG_STATE_HOME` while subscribed, hand back the lock after unsubscribe/cancel, and leak zero credentials across raw frames and stderr.

## Boundaries

The drive creates its own supervisor, state home, and owner record inside a scratch directory; it never touches an installed config, plugin, or credential. Raw frames and stderr are scanned for `run_token` before writing redacted evidence.
