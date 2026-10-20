# Supervisor + control plane — `avenor stable` / `avenor control`

## Sub-features

- Spawning child runs from a live supervisor (`spawn`, with `--label`, `--backend`)
- Inspecting runs (`list`, `status <rt_id>`)
- Interacting with a running child (`prompt <text> [rt_id]` — text first, runtime id optional; omitting the id queues a root prompt instead of addressing the child)
- Cancelling (`cancel <rt_id>`)
- Graceful shutdown of the supervisor (`shutdown graceful`)
- Owner reconnect: every mutating verb loads the published owner record and authenticates on its own connection

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

While the supervisor runs, three artifacts sit beside the socket: the socket
(`0600`), the owner record `<socket>.owner.json` (`0600`, euid-owned; fields
`version`, `socket`, `socket_identity`, `capability_id`, `secret`, `owner_pid`),
and the persistent lock `<socket>.control.lock` (`0600`). Confirm them without
printing the secret:

```sh
ls -l "$SCRATCH"
stat -c '%n mode=%a uid=%u type=%F' "$SOCK" "$SOCK.owner.json" "$SOCK.control.lock"
sed 's/"secret":"[0-9a-f]*"/"secret":"<redacted>"/' "$SOCK.owner.json"
```

Every mutating `avenor control` invocation is a separate process, so the recipe
above is already a reconnect test: `spawn`, `prompt`, `cancel` and `shutdown`
each load the record, bind their connection, authenticate, and drop the grant
when the process exits. Run two mutating verbs back to back and both must
succeed. Read-only verbs (`status`, `list`, `tail`) dial without a credential
and work with no record at all.

After `shutdown graceful` the socket and the record are gone, the lock file and
the tombstone remain, and the supervisor has exited. The child's default
artifact directory under `${TMPDIR:-/tmp}/avenor-stable-$(id -u)/` survives all
of this, so its sentinel and event log are still readable evidence:

```sh
test -e "$SOCK" || echo "socket removed"
test -e "$SOCK.owner.json" || echo "record removed"
test -e "$SOCK.control.lock" && echo "lock retained"
cat "$SOCK.dead"    # STOPPED reason=shutdown pid=<pid> at=<timestamp>
```

## Gotchas

- Save `$STABLE_PID` immediately; cleanup must kill the recorded PID, never
  `pkill avenor`.
- A mutating verb that reports `permission_denied` (`-32010`) did not load or
  present the record. Check that `<socket>.owner.json` exists, is `0600` and is
  owned by your euid. `stale-owner-record` means the record no longer describes
  the live socket, so another process owns that path now.
- Never delete `<socket>.control.lock` or `<socket>.dead` between runs: the lock
  is what serializes a replacement against the live listener.
- `spawn` returns JSON including `runtime_id`, `on_event`, and
  `sentinel_file` — use those paths for evidence rather than guessing.
- The child runs against a real backend; same cheap-prompt rule as single runs.
- The supervisor writes child state under
  `${TMPDIR:-/tmp}/avenor-stable-$(id -u)/run_<supervisor_run_id>/`. The root
  is created `0700` and refused if it is a symlink, owned by another euid, or
  group- or other-writable. A child's default artifact directory outlives the
  supervisor — its sentinel and event log stay readable after shutdown — and
  prune only considers `run_*` directories, hourly, removing one only once
  nothing in it has been written for 24 hours while its supervisor kept
  refreshing a live run directory. Clean your run's subdir yourself after
  copying evidence rather than waiting for that prune.
- If `spawn` hangs, the backend is the suspect — run the doctor
  (`SKILL.md`) before assuming a control-plane bug.
