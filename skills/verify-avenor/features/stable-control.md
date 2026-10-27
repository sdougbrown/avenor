# Supervisor + control plane — `avenor stable` / `avenor control`

## Sub-features

- Spawning child runs from a live supervisor (`spawn`, with `--label`, `--backend`)
- Inspecting runs (`list`, `status <rt_id>`)
- Interacting with a running child (`prompt <text> [rt_id]` — text first, runtime id optional; omitting the id queues a root prompt instead of addressing the child)
- Cancelling (`cancel <rt_id>`)
- Graceful shutdown of the supervisor (`shutdown graceful`)
- Owner reconnect: every mutating verb loads the published owner record and authenticates on its own connection
- Durable broker address: `--broker-addr` binds a fixed hub listener, and the
  bound URL is published beside the socket for discovery

## How to get to it (user POV)

A user starts `avenor stable --control-socket <path>` as a long-lived
supervisor, then drives it from other shells via `avenor control --socket
<path> <verb>`: spawn children on demand, poll their status, send follow-up
prompts, cancel, and shut the supervisor down.

## Driving it with the avenor binary

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
SOCK="$SCRATCH/stable.sock"
./avenor stable --control-socket "$SOCK" &> "$SCRATCH/stable.log" &
STABLE_PID=$!   # record this — cleanup kills THIS pid only

# Wait until the socket answers (the "ready" signal):
until ./avenor control --socket "$SOCK" list >/dev/null 2>&1; do sleep 0.2; done

./avenor control --socket "$SOCK" spawn \
  --backend pi --label verify-ctl \
  --dir "$SCRATCH" \
  --prompt "Reply with the single word OK and nothing else."

./avenor control --socket "$SOCK" list          # find the runtime_id (rt_N)
# status takes only running, idle, or ended — "done" is a phase value, never
# a status value. Poll until status=idle (turn complete); ended appears once
# the supervisor reaps the child.
./avenor control --socket "$SOCK" status rt_1
./avenor control --socket "$SOCK" shutdown graceful
kill "$STABLE_PID" 2>/dev/null
```

**Observable end state (the proof):** `status rt_1` JSON shows `"status":
"idle"` (or `"ended"` after reap) — not `"done"`, which exists only as a
`phase` value — with `"exit_code": 0`, `"final_output"` containing `OK`, and
`pending_permission: false`; the child's sentinel file (its path is in the
spawn response `sentinel_file` field) contains `DONE`.

### Published record and owner reconnect

While the supervisor runs, four artifacts sit beside the socket: the socket
(`0600`), the owner record `<socket>.owner.json` (`0600`, euid-owned; fields
`version`, `socket`, `socket_identity`, `capability_id`, `secret`, `owner_pid`),
the persistent lock `<socket>.control.lock` (`0600`), and the broker discovery
record `<socket>.broker.url` (`0600`, one line: the `http://` URL of the broker
the supervisor actually bound, ephemeral port included). Confirm them without
printing the secret:

```sh
ls -l "$SCRATCH"
stat -c '%n mode=%a uid=%u type=%F' "$SOCK" "$SOCK.owner.json" "$SOCK.control.lock" "$SOCK.broker.url"
sed 's/"secret":"[0-9a-f]*"/"secret":"<redacted>"/' "$SOCK.owner.json"
cat "$SOCK.broker.url"
```

Every mutating `avenor control` invocation is a separate process, so the recipe
above is already a reconnect test: `spawn`, `prompt`, `cancel` and `shutdown`
each load the record, bind their connection, authenticate, and drop the grant
when the process exits. Run two mutating verbs back to back and both must
succeed. Read-only verbs (`status`, `list`, `tail`) dial without a credential
and work with no record at all.

After `shutdown graceful` the socket, the owner record and the broker URL
record are gone, the lock file and the tombstone remain, and the supervisor has
exited. The child's default artifact directory under
`${TMPDIR:-/tmp}/avenor-stable-$(id -u)/` survives all of this, so its sentinel
and event log are still readable evidence:

```sh
test -e "$SOCK" || echo "socket removed"
test -e "$SOCK.owner.json" || echo "record removed"
test -e "$SOCK.broker.url" || echo "broker URL removed"
test -e "$SOCK.control.lock" && echo "lock retained"
cat "$SOCK.dead"    # STOPPED reason=shutdown pid=<pid> at=<timestamp>
```

### Durable broker address (hub)

`--broker-addr` binds the broker to a concrete interface address and port
instead of the ephemeral loopback default, and the bound URL is what the
`.broker.url` record publishes. Drive a hub and the guards around it without
any model call:

```sh
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
HUB_SOCK="$SCRATCH/hub.sock"
await_path() {  # await_path <path> <seconds> <what>
  _n=0
  until [ -e "$1" ]; do
    _n=$((_n + 1))
    [ "$_n" -le $(($2 * 10)) ] || { echo "FAIL: $3 did not appear within ${2}s: $1" >&2; return 1; }
    sleep 0.1
  done
}
await_gone() {  # await_gone <path> <seconds> <what>
  _n=0
  while [ -e "$1" ]; do
    _n=$((_n + 1))
    [ "$_n" -le $(($2 * 10)) ] || { echo "FAIL: $3 still exists after ${2}s: $1" >&2; return 1; }
    sleep 0.1
  done
}
./avenor stable --control-socket "$HUB_SOCK" --broker-addr "127.0.0.1:$PORT" \
  &> "$SCRATCH/hub.log" &
HUB_PID=$!   # record this
await_path "$HUB_SOCK.broker.url" 15 "hub broker URL record" || exit 1
cat "$HUB_SOCK.broker.url"          # http://127.0.0.1:$PORT

# A second supervisor on the same port fails with exit 1, names the address,
# and binds nothing of its own.
./avenor stable --control-socket "$SCRATCH/competitor.sock" \
  --broker-addr "127.0.0.1:$PORT"; echo "exit=$?"   # exit=1, "address already in use"
test -e "$SCRATCH/competitor.sock" && echo "unexpected socket"

# Use an ephemeral port so the hub cannot mask wildcard validation with a
# port collision. The resolved wildcard is refused before serving.
./avenor stable --control-socket "$SCRATCH/wild.sock" --broker-addr "0.0.0.0:0"; echo "exit=$?"

./avenor control --socket "$HUB_SOCK" shutdown graceful
await_gone "$HUB_SOCK.broker.url" 10 "hub broker URL record" || exit 1
echo "broker URL record removed"

# A restart on the same port publishes the same durable URL.
./avenor stable --control-socket "$HUB_SOCK" --broker-addr "127.0.0.1:$PORT" \
  &> "$SCRATCH/hub-restart.log" &
RESTART_PID=$!   # record this
await_path "$HUB_SOCK.broker.url" 15 "restarted hub broker URL record" || exit 1
test "$(cat "$HUB_SOCK.broker.url")" = "http://127.0.0.1:$PORT" && echo "same durable URL"
./avenor control --socket "$HUB_SOCK" shutdown graceful
await_gone "$HUB_SOCK.broker.url" 10 "restarted hub broker URL record" || exit 1
for pid in "$HUB_PID" "$RESTART_PID"; do
  case "$pid" in ''|*[!0-9]*) continue ;; esac   # unset or not a pid: never kill
  [ "$pid" -gt 0 ] && kill "$pid" 2>/dev/null
done
```

**Observable end state (the proof):** the first and the restarted hub both
publish `http://127.0.0.1:$PORT` (the same fixed port, not a fresh ephemeral
one), the occupied bind exits 1 with an `address already in use` message naming
the address and leaves no socket or broker URL record, the wildcard run exits 1
with `is a wildcard bind`, `concrete interface address` and the requested
address in the message, and each graceful shutdown removes
`<socket>.broker.url` while leaving the tombstone.


## Gotchas

- Save `$STABLE_PID` immediately; cleanup must kill the recorded PID, never
  `pkill avenor`.
- A mutating verb that reports `permission_denied` (`-32010`) did not load or
  present the record. Check that `<socket>.owner.json` exists, is `0600` and is
  owned by your euid. `stale-owner-record` means the record no longer describes
  the live socket, so another process owns that path now.
- Never delete `<socket>.control.lock` or `<socket>.dead` between runs: the lock
  is what serializes a replacement against the live listener.
- `<socket>.broker.url` is written only after the supervisor owns the control
  socket. A supervisor that loses the socket to a live competitor publishes
  nothing, so the competitor's failure cannot overwrite or remove the live
  record; do not treat a missing record on a failed start as a defect.
- `spawn` returns JSON including `runtime_id`, `on_event`, and
  `sentinel_file` — use those paths for evidence rather than guessing.
- The child runs against a real backend; same cheap-prompt rule as single runs.
- The supervisor writes child state under
  `${TMPDIR:-/tmp}/avenor-stable-$(id -u)/run_<supervisor_run_id>/`. The root
  is created `0700` and refused if it is a symlink, owned by another euid, or
  grants any group or other permission; the refusal names the root and says to
  remove it, or to chown and chmod it to `0700`. A child's default artifact
  directory outlives the
  supervisor — its sentinel and event log stay readable after shutdown — and
  prune only considers `run_*` directories, hourly, removing one only once
  nothing in it has been written for 24 hours while its supervisor kept
  refreshing a live run directory. Clean your run's subdir yourself after
  copying evidence rather than waiting for that prune.
- If `spawn` hangs, the backend is the suspect — run the doctor
  (`SKILL.md`) before assuming a control-plane bug.
