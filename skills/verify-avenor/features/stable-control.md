# Supervisor + control plane — `avenor stable` / `avenor control`

## Sub-features

- Spawning child runs from a live supervisor (`spawn`, with `--label`, `--backend`)
- Inspecting runs (`list`, `status <rt_id>`)
- Interacting with a running child (`prompt <text> [rt_id]` — text first, runtime id optional; omitting the id queues a root prompt instead of addressing the child)
- Cancelling (`cancel <rt_id>`)
- Graceful shutdown of the supervisor (`shutdown graceful`)

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

## Gotchas

- Save `$STABLE_PID` immediately; cleanup must kill the recorded PID, never
  `pkill avenor`.
- `spawn` returns JSON including `runtime_id`, `on_event`, and
  `sentinel_file` — use those paths for evidence rather than guessing.
- The child runs against a real backend; same cheap-prompt rule as single runs.
- The supervisor writes child state under `/tmp/avenor-stable/`; clean your
  run's subdir after copying evidence.
- If `spawn` hangs, the backend is the suspect — run the doctor
  (`SKILL.md`) before assuming a control-plane bug.
