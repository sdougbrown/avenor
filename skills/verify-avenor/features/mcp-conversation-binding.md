# MCP conversation launch — `avenor mcp --bind-host … --bind-output-fd …`

A trusted host that owns one native conversation launches Avenor's MCP server for
that conversation and receives its messaging identity on a private pipe it
inherited. The launch binds on the supervisor the server owns before it serves a
tool call, so every child the MCP spawn tool forwards names the conversation's run
as its broker parent. See `docs/messaging.md` (MCP conversation launch) for the
contract and `docs/control-protocol.md` (`spawn_mcp`) for the supervisor's
stale-parent refusal.

## The launch flags

```sh
avenor mcp --transport stdio \
  --supervisor-socket <socket> --no-autostart \
  --bind-host poo --bind-session <native-session-id> --bind-output-fd 3
```

- The three bind flags travel together or not at all, only on `--transport stdio`.
- `--bind-output-fd` names an inherited pipe open for writing. stdin, stdout and
  stderr (`0`, `1`, `2`) are refused: they carry the MCP protocol and its
  diagnostics. A regular file, a closed descriptor, or a read-only pipe is
  refused before the bind, so a credential can never be written to a file or to
  the wrong side.
- The launch makes the descriptor non-blocking (so Go registers it with the
  runtime poller and the write has a deadline), marks it close-on-exec before it
  can autostart a supervisor, writes
  `{"broker_url","run_id","run_token","generation","created"}` as one JSON line,
  and closes it. The host reads the line and then EOF: no process the launch
  starts keeps the write end.
- A launch that cannot bind or hand off exits non-zero; there is no unbound
  fallback and no retry. A host that stops reading the pipe fails the launch
  inside the handoff window instead of stalling it.

## Typed refusals

| Stage | Exit | `stderr` |
|---|---|---|
| incomplete declaration | 2 | `--bind-host, --bind-session and --bind-output-fd are supplied together or not at all` |
| shared transport | 2 | `--bind-host is only supported with --transport stdio, a transport that serves one conversation` |
| stdio descriptor | 2 | `--bind-output-fd must name a writable inherited descriptor other than 0, 1 and 2, …` |
| not a pipe | 2 | `--bind-output-fd <fd> is not a pipe; the conversation's credentials are handed over on a private pipe and nowhere else` |
| closed descriptor | 2 | `--bind-output-fd <fd> is not an inherited open descriptor: …` |
| read-only pipe | 2 | `--bind-output-fd <fd> is not writable; the handoff pipe must be open for writing` |
| bind or handoff failure | 1 | `avenor mcp: conversation bind for host "poo" session "…": …` |

No refusal text carries the credential.

## Driving it

Build the binary and start a stable exactly as in
[stable-control.md](stable-control.md). Save the driver below as
`mcp-bind-drive.py` (stdlib only) and run it against the real binary:

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
mkdir -p "$SCRATCH/home" "$SCRATCH/avenor"
EVIDENCE=$(mktemp -d /tmp/avenor-verify-evidence.XXXXXX)
python3 mcp-bind-drive.py ./avenor "$SCRATCH" "$EVIDENCE/frames.ndjson"
```

```python
#!/usr/bin/env python3
"""Drive the MCP conversation launch over real stdio and a private pipe.

Usage: mcp-bind-drive.py <avenor> <scratch> <frames.ndjson>

Starts a real supervisor, launches `avenor mcp` with an inherited pipe as the
handoff, reads the identity before any MCP frame is exchanged, drives
initialize/tools/list over the child's stdin/stdout, and relaunches the same
conversation to prove the identity is reused. Captured frames redact run_token.
"""
import json, os, select, socket, subprocess, sys, time

BIN, SCRATCH, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
SOCK = os.path.join(SCRATCH, "stable.sock")
ENV = {
    "PATH": os.environ["PATH"],
    "HOME": os.path.join(SCRATCH, "home"),
    "AVENOR_HOME": os.path.join(SCRATCH, "avenor"),
    "TMPDIR": SCRATCH,
}
ev = open(OUT, "a")
results = []

def redact(obj):
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k == "run_token" and isinstance(v, str) and v else redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(x) for x in obj]
    return obj

def check(name, ok, detail):
    results.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " :: " + detail)

def frame(direction, name, payload):
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

def launch(session, stderr_path):
    """Launch the MCP server with the handoff on an inherited pipe."""
    read_fd, write_fd = os.pipe()
    if write_fd < 3:
        raise SystemExit("pipe write descriptor %d is not above stderr" % write_fd)
    cmd = [
        BIN, "mcp", "--transport", "stdio",
        "--supervisor-socket", SOCK, "--no-autostart",
        "--bind-host", "poo", "--bind-session", session,
        "--bind-output-fd", str(write_fd),
    ]
    err = open(stderr_path, "wb")
    proc = subprocess.Popen(
        cmd, env=ENV, pass_fds=(write_fd,), bufsize=0,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
    )
    os.close(write_fd)  # the child holds the only writer, so the pipe reaches EOF
    frame("launch", "argv", {"session": session, "bind_output_fd": write_fd})
    line = read_line(read_fd, 30)
    os.close(read_fd)
    return proc, line

def read_line(fd, timeout):
    """Read one newline-terminated line; empty means the pipe closed with none."""
    buf = b""
    deadline = time.time() + timeout
    while b"\n" not in buf:
        remaining = deadline - time.time()
        if remaining <= 0:
            return b""
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            return b""
        chunk = os.read(fd, 4096)
        if not chunk:
            return buf  # EOF: the launch closed its copy
        buf += chunk
    return buf

def rpc(proc, request_id, method, params):
    body = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
    proc.stdin.write((json.dumps(body) + "\n").encode())
    proc.stdin.flush()
    resp = read_line(proc.stdout.fileno(), 30)
    frame("request", method, body)
    frame("response", method, json.loads(resp) if resp else {})
    return json.loads(resp) if resp else {}

def close(proc):
    proc.stdin.close()
    proc.wait(timeout=30)
    return proc.returncode

# A supervisor the launch binds against, in this drive's own scratch state.
sup = subprocess.Popen(
    [BIN, "stable", "--control-socket", SOCK], env=ENV,
    stdout=open(os.path.join(SCRATCH, "stable.log"), "wb"),
    stderr=subprocess.STDOUT,
)
try:
    check("supervisor-listens", wait_socket(SOCK), SOCK)

    # 1. A stdio descriptor is refused before any bind.
    bad = subprocess.run(
        [BIN, "mcp", "--transport", "stdio", "--supervisor-socket", SOCK, "--no-autostart",
         "--bind-host", "poo", "--bind-session", "ses_fd", "--bind-output-fd", "1"],
        env=ENV, capture_output=True, timeout=30,
    )
    check("stdio-descriptor-refused", bad.returncode == 2 and b"other than 0, 1 and 2" in bad.stderr, bad.stderr.decode(errors="replace").strip())

    # 2. A consumer that is gone fails the launch instead of hanging or serving.
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
    check("closed-consumer-fails-the-launch", gone.returncode != 0 and b"hand off to the launch pipe" in gone_err, gone_err.decode(errors="replace").strip())

    # 3. The real launch: identity on the pipe before any MCP frame.
    proc, line = launch("ses_drive", os.path.join(SCRATCH, "mcp.err"))
    try:
        check("handoff-is-one-json-line", bool(line) and line.endswith(b"\n") and line.count(b"\n") == 1, "%d bytes, one newline-terminated line" % len(line))
        ident = json.loads(line)
        frame("handoff", "handoff", ident)
        check("handoff-carries-the-bind-result",
              all(ident.get(k) for k in ("broker_url", "run_id", "run_token", "generation"))
              and ident.get("created") is True and ident.get("run_id") != "supervisor",
              json.dumps(redact(ident)))
        token = ident.get("run_token", "")

        # 4. The protocol frames on stdout stay intact and carry no credential.
        init = rpc(proc, 1, "initialize", {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "mcp-bind-drive", "version": "1"},
        })
        check("initialize-over-stdio", (init.get("result") or {}).get("protocolVersion") and init.get("id") == 1, json.dumps(redact(init)))
        proc.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        tools = rpc(proc, 2, "tools/list", {})
        names = [t.get("name") for t in (tools.get("result") or {}).get("tools", [])]
        check("tools-list-over-stdio", tools.get("id") == 2 and "spawn" in names and "result" in names, json.dumps(redact(tools)))
        check("tool-surface-carries-no-credential", token not in json.dumps(tools), "%d tools" % len(names))

        check("server-exits-on-stdin-close", close(proc) == 0, "exit 0")
        err = open(os.path.join(SCRATCH, "mcp.err"), "rb").read()
        check("stderr-carries-no-credential", token.encode() not in err and b"run_token" not in err, err.decode(errors="replace").strip()[:200])
    finally:
        if proc.poll() is None:
            proc.kill()

    # 5. A relaunch of the same conversation reuses the identity.
    again, again_line = launch("ses_drive", os.path.join(SCRATCH, "mcp2.err"))
    try:
        reused = json.loads(again_line)
        frame("handoff", "relaunch", reused)
        check("relaunch-reuses-the-conversation-run",
              reused.get("run_id") == ident["run_id"] and reused.get("run_token") == ident["run_token"] and reused.get("created") is False,
              json.dumps(redact(reused)))
        check("relaunch-exits-on-stdin-close", close(again) == 0, "exit 0")
    finally:
        if again.poll() is None:
            again.kill()
finally:
    sup.terminate()
    sup.wait(timeout=30)

# The summary repeats check details, so scrub the credential the drive learned
# before it reaches the evidence file.
if token:
    for result in results:
        result["detail"] = result["detail"].replace(token, "<redacted>")
ev.write(json.dumps({"direction": "summary", "frame": results}) + "\n")
ev.close()
failed = [r for r in results if not r["pass"]]
print("SUMMARY %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
```

**Observable end state (the proof):** the driver's summary reports every check
passing. The handoff is exactly one newline-terminated JSON line carrying the
bind result's five fields, with the conversation's own run (`created: true`, not
`supervisor`). `initialize` and `tools/list` round-trip over the child's real
stdin/stdout, so the protocol framing is intact beside the handoff pipe, and
neither the tool surface nor the child's stderr carries the `run_token`. The
server exits 0 when stdin closes, and a relaunch of the same native session
returns the same `run_id` and `run_token` with `created: false`.

**Not covered by this drive.** The launch's bound on a host that stops draining
the pipe (an inherited blocking full pipe, which exits with
`the host is not reading the handoff pipe`), the close-on-exec EOF proof with an
autostarted supervisor, and the read-only descriptor refusal are proved by
`TestMCPSubprocessWithAFullBlockingHandoffPipeExitsBounded`,
`TestMCPSubprocessAutostartsAndHandsOff` and `TestValidateBindHandoff` in
`cmd/avenor`. The child-to-parent delivery path (a spawn's child
asking the bound run and getting an answer) and the supervisor's refusal of a
stale parent after a supervisor restart are proved by
`TestMCPDeclaredConversationIsTheChildParent`,
`TestMCPDeclaredConversationRefusesAStaleBinding`, and by
`TestSpawnMCPNamesALiveBoundParent`, `TestSpawnMCPRefusesAStaleBoundParent` and
`TestSpawnMCPRefusesAnEarlierSupervisorsBinding` in `internal/stable`, which
drive a real supervisor, a real control socket and a real broker with an
in-process provider.

## Boundaries

The drive creates its own supervisor and owner record inside a scratch
`AVENOR_HOME`; it never touches an installed config, plugin or credential. The
captured frames redact `run_token`. Graceful shutdown removes the owner record;
if the operator's credential policy prohibits removal, obtain permission before
cleaning up the scratch directory.
