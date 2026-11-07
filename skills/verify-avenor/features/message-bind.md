# Owner message bind — `message_bind` over the stable control socket

`message_bind` is how a trusted host adapter turns one native conversation
(host + native session id) into exactly one broker-owned messaging identity.
It is owner-only: a connection without the owner grant is refused before the
handler runs. There is no CLI verb. A JSON-RPC client dials the control socket,
authenticates from the published owner record, and calls `message_bind`. See `docs/control-protocol.md` for the wire
contract.

## Host semantics

- `pi` reuses the supervisor's own broker run. One conversation binds to the
  supervisor singleton; a second pi session is refused, never re-bound, and a
  lost supervisor run fails closed rather than reminting.
- `opencode` mints one broker run per conversation. If the broker confirms a
  bound run is gone, expired, or retired, the next bind mints a fresh identity.
- Any other host is refused, never guessed at.

While a bound run remains available, repeat binds of the same `(host,
session_id)` return the same `run_id` and `run_token`, across reconnects too.
The result uses the fields `broker_url`, `run_id`, `run_token`, `generation`,
and `created`. `created` is `false` for the pi host: the supervisor's run
already exists, so the bind reuses it.

## Typed refusals

A handler refusal is `-32000` with `error.data.code`:

| `data.code` | When | Example message |
|---|---|---|
| `refused` | A bind conflict or an unavailable broker run | `host pi already binds session "sess_1"; one conversation per supervisor singleton` |
| `invalid-request` | Unserved host or invalid session id | `message_bind does not serve host "claude"` / `message_bind requires a native session id of 1-200 bytes …` |

An unauthenticated connection gets `-32010 permission_denied` with no
`data`. The server's owner gate rejects any connection without the grant
before the handler runs, so `-32010` is the only wire outcome for a
non-owner caller. The handler also refuses a non-owner caller with `not_permitted` as a defense
in depth. The server's owner gate prevents this code from reaching the wire.
Like `resume`/`session_*`, the refusal carries the typed `code` alone. It has
no `outcome` or `wire_code`; those fields belong to the ask transport.

## Driving it

Build the binary and start a stable exactly as in
[stable-control.md](stable-control.md). Save the driver below as
`message-bind-drive.py` (any scratch location; it is stdlib-only) and run it
against the live socket. It reads the owner record beside the socket,
authenticates a real owner connection, and exercises the bind matrix over the
real Unix socket:

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
mkdir -p "$SCRATCH/home" "$SCRATCH/avenor"
SOCK="$SCRATCH/stable.sock"
env -i PATH="$PATH" HOME="$SCRATCH/home" TMPDIR="$SCRATCH" \
  AVENOR_HOME="$SCRATCH/avenor" ./avenor stable --control-socket "$SOCK" &> "$SCRATCH/stable.log" &
STABLE_PID=$!
until env -i PATH="$PATH" HOME="$SCRATCH/home" TMPDIR="$SCRATCH" \
  AVENOR_HOME="$SCRATCH/avenor" ./avenor control --socket "$SOCK" list >/dev/null 2>&1; do sleep 0.2; done

EVIDENCE=$(mktemp -d /tmp/avenor-verify-evidence.XXXXXX)
python3 message-bind-drive.py "$SOCK" "$EVIDENCE/frames.ndjson"
```

```python
#!/usr/bin/env python3
"""Exercise the owner-only message_bind matrix over a live control socket.

Usage: message-bind-drive.py <socket> <frames.ndjson>

Reads the owner record beside the socket, authenticates a real owner
connection, and runs the bind matrix. Captured frames are NDJSON with
run_token/secret redacted at capture time; the owner record itself is never
written anywhere.
"""
import json, socket, sys

SOCK, OUT = sys.argv[1], sys.argv[2]

def redact(obj):
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k in ("run_token", "secret") and isinstance(v, str) and v else redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(x) for x in obj]
    return obj

ev = open(OUT, "a")

def rpc(conn, req_id, method, params):
    frame = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
    conn.sendall((json.dumps(frame) + "\n").encode())
    buf = b""
    conn.settimeout(10)
    while b"\n" not in buf:
        chunk = conn.recv(65536)
        if not chunk:
            raise RuntimeError("connection closed")
        buf += chunk
    resp = json.loads(buf.split(b"\n")[0])
    ev.write(json.dumps({"direction": "request", "frame": redact(frame)}) + "\n")
    ev.write(json.dumps({"direction": "response", "frame": redact(resp)}) + "\n")
    return resp

def dial():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(SOCK)
    return s

results = []
def check(name, ok, detail):
    results.append({"check": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " :: " + detail)

# The owner record beside the socket carries the capability the connection
# authenticates with. Read it, use it, never write it out.
record = json.loads(open(SOCK + ".owner.json").read())
auth = {"version": 1, "capability_id": record["capability_id"], "secret": record["secret"]}

# 1. Owner connection: authenticate_owner, then the first pi bind.
c1 = dial()
r = rpc(c1, 1, "authenticate_owner", auth)
check("owner-auth", r.get("result", {}).get("authenticated") is True, json.dumps(redact(r)))
r = rpc(c1, 2, "message_bind", {"host": "pi", "session_id": "sess_verify_pi_1"})
first = r.get("result") or {}
check("first-pi-bind", "error" not in r and first.get("run_id") and first.get("run_token") and first.get("broker_url") and first.get("generation"), json.dumps(redact(r)))
check("pi-bind-reuses-supervisor-run", first.get("run_id") == "supervisor" and first.get("created") is False, json.dumps(redact(r)))
run_id_1 = first.get("run_id")

# 2. Repeat the same bind: same run, same token.
r = rpc(c1, 3, "message_bind", {"host": "pi", "session_id": "sess_verify_pi_1"})
again = r.get("result") or {}
check("repeat-pi-bind-same-run-and-token", "error" not in r and again.get("run_id") == run_id_1 and again.get("run_token") == first.get("run_token"), json.dumps(redact(r)))

# 3. A second pi session is refused (refused).
r = rpc(c1, 4, "message_bind", {"host": "pi", "session_id": "sess_verify_pi_2"})
err = r.get("error") or {}
check("second-pi-session-refused", err.get("code") == -32000 and (err.get("data") or {}).get("code") == "refused" and "already binds session" in err.get("message", ""), json.dumps(redact(r)))

# 4. An unserved host is refused (invalid-request).
r = rpc(c1, 5, "message_bind", {"host": "claude", "session_id": "sess_verify_pi_1"})
err = r.get("error") or {}
check("unserved-host-invalid-request", err.get("code") == -32000 and (err.get("data") or {}).get("code") == "invalid-request", json.dumps(redact(r)))

# 5. An invalid session id is refused (invalid-request).
r = rpc(c1, 6, "message_bind", {"host": "pi", "session_id": "bad/session\u0000id"})
err = r.get("error") or {}
check("invalid-session-invalid-request", err.get("code") == -32000 and (err.get("data") or {}).get("code") == "invalid-request", json.dumps(redact(r)))

# 6. Reconnect: a second owner connection re-binds the same session to the
#    same run.
c2 = dial()
r = rpc(c2, 1, "authenticate_owner", auth)
r = rpc(c2, 2, "message_bind", {"host": "pi", "session_id": "sess_verify_pi_1"})
re = r.get("result") or {}
check("reconnect-owner-rebind-same-run", "error" not in r and re.get("run_id") == run_id_1, json.dumps(redact(r)))
c2.close()

# 7. An unauthenticated connection is -32010 with no data.
c3 = dial()
r = rpc(c3, 1, "message_bind", {"host": "pi", "session_id": "sess_unauth"})
err = r.get("error") or {}
check("unauthenticated-permission-denied", err.get("code") == -32010 and err.get("message") == "permission_denied" and "data" not in err, json.dumps(redact(r)))
c3.close()
c1.close()

ev.write(json.dumps({"direction": "summary", "frame": results}) + "\n")
ev.close()
failed = [r for r in results if not r["pass"]]
print("SUMMARY %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
```

**Observable end state (the proof):** the driver's summary reports every
check passing. Bind results carry all five fields, with the supervisor's
`run_id` and `created: false`. Refusals carry `-32000` and the `data.code` from
the table. The unauthenticated call carries `-32010` with no `data`. The refusal `message` texts name the host or the session-id
rule — they never echo a credential.

## Boundaries

The drive creates an owner credential in `<socket>.owner.json`. Capture evidence
from the redacted frames, not the owner record. Graceful shutdown removes that
record; if the operator's credential policy prohibits removal, obtain
permission before stopping the supervisor or cleaning up its scratch directory.
