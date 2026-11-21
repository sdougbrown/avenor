# Bound conversation CLI — the feed owns the conversation's supervisor

A host session that Avenor did not launch — plain `poo` with a feed and
registered commands, for example — reaches one supervisor through its native
host name and native session ID. A socket may be configured explicitly instead.
Only the feed binds. `avenor spawn`, `avenor status`, `avenor result` and
`avenor message` read the live binding per call, so no credential is ever handed
to a command, and no command starts a supervisor.

## Coordinates and the derived socket

| flag | environment | meaning |
|---|---|---|
| `--bind-host` | `AVENOR_BIND_HOST` | native host the conversation belongs to, such as `poo` |
| `--bind-session` | `AVENOR_BIND_SESSION` | native session ID of the conversation |
| `--socket` | `AVENOR_SOCKET` | an explicitly configured supervisor, instead of the derived one |

With the host and session and no socket, Avenor derives one socket per native
conversation under its sockets root — `$AVENOR_HOME/sockets`, or
`~/.avenor/sockets` when `AVENOR_HOME` is unset. The name is an implementation
detail: the feed and every command of that session resolve the same one, and no
flag, environment value or tool argument carries it. Nothing outside the binary
should compute it; a host passes the coordinates and receives the derived path
in a diagnostic when nothing serves it.

Behaviour that follows from the derivation:

- `avenor feed broker --bind-host poo --bind-session ID` derives the path, starts
  the supervisor when nothing serves it, binds the conversation once, and serves
  the inbox. It stops the supervisor it started when it stops serving.
- `spawn`, `status`, `result` and `message` derive the same path and only
  attach. A run before the session's feed has started its supervisor exits 1,
  names the derived path and starts nothing.
- `--socket`/`AVENOR_SOCKET` is attach-only and never derived: a socket that is
  not there is an error.
- A command naming half the conversation, or both coordinates and broker
  credentials, exits 2 without calling any supervisor.

Each derived supervisor runs its own broker, so two sessions with derived
sockets do not share an inbox hub. Cross-session messaging needs them to reach
one broker: an explicit configured socket, or a parent broker link in the feed's
environment.

## Drive and proof

Build the binary (`mise run go-build`), then run the driver below. It needs no
explicit supervisor: it starts two real feeds as a host session would, discovers
their sockets by listing the sockets root (never by computing a name), and
proves ownership, isolation, teardown and the attach-only rules. The scratch path
is short because a unix socket path has to fit the socket name limit.

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
mkdir -p "$SCRATCH/avenor/sockets" "$SCRATCH/home"
EVIDENCE=$(mktemp -d /tmp/avenor-verify-evidence.XXXXXX)
python3 bound-cli-drive.py "$SCRATCH" "$PWD/avenor" "$EVIDENCE"
```

```python
#!/usr/bin/env python3
"""Exercise the per-session supervisor contract with the real binary.

Usage: bound-cli-drive.py <scratch-root> <avenor-binary> <evidence-dir>
"""
import json, os, signal, socket, subprocess, sys, time

SCRATCH, AVENOR, EVID = sys.argv[1], sys.argv[2], sys.argv[3]
AVENOR_HOME = os.path.join(SCRATCH, "avenor")
SOCKETS = os.path.join(AVENOR_HOME, "sockets")
ENV = {k: v for k, v in os.environ.items() if not k.startswith("AVENOR_")}
ENV.update({"HOME": os.path.join(SCRATCH, "home"), "AVENOR_HOME": AVENOR_HOME,
            "TMPDIR": SCRATCH, "XDG_STATE_HOME": os.path.join(SCRATCH, "state")})
FRAMES = open(os.path.join(EVID, "frames.ndjson"), "a")
RESULTS = []
SECRETS = ("run_token", "secret", "token")


def redact(o):
    if isinstance(o, dict):
        return {k: ("<redacted>" if k in SECRETS and isinstance(v, str) and v else redact(v)) for k, v in o.items()}
    if isinstance(o, list):
        return [redact(x) for x in o]
    return o


def check(name, ok, detail):
    detail = detail if isinstance(detail, str) else json.dumps(detail)
    RESULTS.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " :: " + detail)


def sockets():
    """The supervisor sockets the sessions have created, discovered, not computed."""
    if not os.path.isdir(SOCKETS):
        return set()
    return {os.path.join(SOCKETS, n) for n in os.listdir(SOCKETS) if n.endswith(".sock")}


def owner_pid(path):
    return json.loads(open(path + ".owner.json").read())["owner_pid"]


def rpc(path):
    record = json.loads(open(path + ".owner.json").read())
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.connect(path)
    conn.settimeout(20)
    state = {"id": 0}

    def call(name, body):
        state["id"] += 1
        frame = {"jsonrpc": "2.0", "id": state["id"], "method": name, "params": body}
        conn.sendall((json.dumps(frame) + "\n").encode())
        buf = b""
        while b"\n" not in buf:
            buf += conn.recv(65536)
        resp = json.loads(buf.split(b"\n")[0])
        FRAMES.write(json.dumps({"request": redact(frame), "response": redact(resp)}) + "\n")
        FRAMES.flush()
        return resp

    call("authenticate_owner", {"version": 1, "capability_id": record["capability_id"],
                                "secret": record["secret"]})
    return conn, call


def lookup(path, session):
    try:
        conn, call = rpc(path)
    except OSError:
        return None
    try:
        return call("message_bind_lookup", {"host": "poo", "session_id": session})
    finally:
        conn.close()


def error(resp):
    err = (resp or {}).get("error") or {}
    return (err.get("data") or {}).get("code"), err.get("message", "")


def start_feed(session):
    argv = [AVENOR, "feed", "broker", "--bind-host", "poo", "--bind-session", session,
            "--lock-dir", os.path.join(SCRATCH, "locks"), "--request-timeout", "300ms"]
    out = open(os.path.join(EVID, "feed-%s.ndjson" % session), "w")
    log = open(os.path.join(EVID, "feed-%s.stderr" % session), "w")
    return subprocess.Popen(argv, stdout=out, stderr=log, env=ENV, cwd=SCRATCH)


def await_new_socket(before, seconds=30):
    """Wait for a socket the session created and a supervisor answers on."""
    end = time.time() + seconds
    while time.time() < end:
        for path in sockets() - before:
            if lookup(path, "ses_probe") is not None:
                return path
        time.sleep(0.1)
    return None


def await_gone(path, seconds=20):
    end = time.time() + seconds
    while time.time() < end:
        if lookup(path, "ses_probe") is None:
            return True
        time.sleep(0.1)
    return False


def await_binding(paths, session, seconds=20):
    """Wait until one of the supervisors answers this session's live binding."""
    end = time.time() + seconds
    answers = {path: None for path in paths}
    answering = []
    while time.time() < end:
        answers = {path: lookup(path, session) for path in paths}
        answering = [path for path, resp in answers.items() if "result" in (resp or {})]
        if answering:
            break
        time.sleep(0.1)
    return answers, answering


def stop(proc):
    proc.send_signal(signal.SIGTERM)
    return proc.wait(timeout=20)


# 1. No feed has run: a command refuses, names a path under the sockets root, and
#    starts nothing. The path is the binary's own answer, not a computed one.
spawn = subprocess.run([AVENOR, "spawn", "--bind-host", "poo", "--bind-session", "ses_feed_a",
                        "--prompt", "work", "--dir", os.path.join(SCRATCH, "child")],
                       capture_output=True, text=True, env=ENV)
check("a-command-refuses-before-the-feed-starts-the-supervisor",
      spawn.returncode == 1 and SOCKETS in spawn.stderr and "its inbox feed starts it" in spawn.stderr,
      spawn.stderr.strip()[:300])
check("the-refused-command-starts-nothing", sockets() == set(), sorted(sockets()))

# 2. Each feed starts and binds its own supervisor.
before = sockets()
feed_a = start_feed("ses_feed_a")
socket_a = await_new_socket(before)
check("feed-a-starts-a-supervisor-for-its-conversation", socket_a is not None, str(socket_a))
before = sockets()
feed_b = start_feed("ses_feed_b")
socket_b = await_new_socket(before)
check("feed-b-starts-a-second-supervisor", socket_b is not None, str(socket_b))
if not (socket_a and socket_b):
    sys.exit(1)
check("the-two-sessions-get-different-sockets", socket_a != socket_b, socket_a + " vs " + socket_b)
check("the-two-sessions-get-different-supervisor-processes", owner_pid(socket_a) != owner_pid(socket_b),
      "%s vs %s" % (owner_pid(socket_a), owner_pid(socket_b)))

# 3. Each supervisor serves only its own conversation: exactly one answers this
#    session's binding, the other has never bound it.
answers, answering = await_binding((socket_a, socket_b), "ses_feed_a")
refusing = [path for path, resp in answers.items()
            if error(resp)[0] == "refused" and "must bind it first" in error(resp)[1]]
check("the-feed-bound-its-own-conversation", answering == [socket_a],
      redact({"answering": [os.path.basename(p) for p in answering]}))
check("the-other-supervisor-has-not-bound-it", refusing == [socket_b],
      [error(answers[socket_b])[1]][:1])

# 4. Stopping one feed stops only its supervisor; the other keeps serving.
check("feed-a-exits-cleanly", stop(feed_a) == 0, "exit 0")
check("feed-a-takes-its-supervisor-with-it", await_gone(socket_a), socket_a)
check("the-other-supervisor-survives-and-still-serves-its-conversation",
      "result" in (lookup(socket_b, "ses_feed_b") or {}), socket_b)
check("feed-b-exits-cleanly", stop(feed_b) == 0, "exit 0")
check("feed-b-takes-its-supervisor-with-it", await_gone(socket_b), socket_b)
check("no-supervisor-outlives-the-sessions", sockets() == set(), sorted(sockets()))

# 5. An explicit socket stays attach-only: absent means failure, never a launch.
explicit = os.path.join(SCRATCH, "configured.sock")
feed = subprocess.run([AVENOR, "feed", "broker", "--socket", explicit, "--bind-host", "poo",
                       "--bind-session", "ses_explicit", "--lock-dir", os.path.join(SCRATCH, "locks")],
                      capture_output=True, text=True, env=ENV)
check("an-explicit-socket-that-is-absent-is-an-error",
      feed.returncode == 1 and "attach the configured supervisor at " + explicit in feed.stderr,
      feed.stderr.strip()[:300])
check("the-explicit-socket-starts-nothing",
      not os.path.exists(explicit) and sockets() == set(), explicit)

failed = [r for r in RESULTS if not r["pass"]]
json.dump({"checks": RESULTS, "passed": len(RESULTS) - len(failed), "total": len(RESULTS)},
          open(os.path.join(EVID, "summary.json"), "w"), indent=2)
print("SUMMARY %d/%d passed" % (len(RESULTS) - len(failed), len(RESULTS)))
sys.exit(1 if failed else 0)
```

**Observable end state (the proof):** every check passes.

- Before any feed runs, `spawn` with the coordinates alone exits 1, names a path
  under the sockets root and says its inbox feed starts it, and no socket exists
  in that root.
- Two simultaneous feeds create two distinct sockets under the sockets root, each
  served by its own live supervisor process, and each supervisor serves only its
  own conversation: exactly one answers the first session's binding read, and the
  other answers `refused` with `must bind it first`.
- Stopping one feed (SIGTERM) exits 0, takes its supervisor down, and leaves the
  other supervisor live and still answering its conversation.
- Stopping the second feed takes the second supervisor down, leaving no
  supervisor socket behind.
- An explicit `--socket` with nothing listening exits 1 with the configured
  socket named, and starts neither an explicit nor a derived supervisor.

The same contract is covered by Go tests, which also hold the derivation internally
(the path is never a flag, env value or tool argument, so the only place its name
is pinned is the package that derives it):

- `internal/supervisor.TestConversationSocketDerivesTheDocumentedPath` and the
  derivation rules around it (`…SeparatesConversations`,
  `…StaysInsideTheSocketsRoot`, `…FallsBackToTheHomeRoot`,
  `…RefusesAnIncompletePair`).
- `internal/supervisor.TestStartDefaultSocketKeepsTheHomeRoot` — the MCP
  autostart's own default socket stays under `~/.avenor/sockets` and is not moved
  by `AVENOR_HOME`.
- `cmd/avenor.TestConversationFeedsOwnDistinctSupervisors` — two real feeds, two
  live supervisors, one stops, the other survives, then both are gone.
- `cmd/avenor.TestConversationFeedKilledOutrightStopsItsSupervisor` (linux) — a
  SIGKILLed feed's supervisor still shuts itself down, the route a host's
  SIGTERM-then-kill grace leaves.
- `cmd/avenor.TestConversationFeedKeepsAnExplicitSocketAttachOnly`,
  `TestBoundCommandsDeriveTheConversationSocket`,
  `TestBoundCommandsRefuseWhenTheDerivedSupervisorIsNotRunning`,
  `TestBoundCommandsKeepAnExplicitSocketAttachOnly`,
  `TestFeedBrokerStartsAndStopsTheDerivedSupervisor`,
  `TestFeedBrokerLeavesASupervisorItOnlyAttachedTo`,
  `TestFeedBrokerRefusesAnUnservedConversationWithoutLeavingASupervisor`.

## Boundaries

- The supervisor is owner-gated: same-UID owner trust over the control socket.
  It is not a privilege boundary against another process of the same user.
- Only the feed starts a supervisor, and only for a derived socket. A
  supervisor that is already live at a derived path is attached to and left
  running when that feed exits — the process that started it still owns it.
- The derived socket is internal: its name is not an interface. A host passes the
  coordinates; it does not compute or configure a path.
- The parent-death shutdown is the kernel's signal on the supervisor when the
  feed dies outright. It exists on Linux; elsewhere an abruptly killed feed
  leaves its supervisor running.
- A sandboxed host session must give the feed write access to the sockets root
  (for `poo`, the session's read-write binds) because the feed creates the
  socket and its owner record there; commands only read it.
- `status` and `result` read any runtime the conversation's supervisor knows, by
  runtime ID. The tool path cannot choose the supervisor, host or session, and
  cannot spawn as another conversation's child.
- Capture evidence from the redacted frames and feed logs, never from
  `<socket>.owner.json`. Shut supervisors down through their control socket and
  remove only scratch files this drive created.
