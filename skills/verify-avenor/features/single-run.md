# Single run — `avenor run`

## Sub-features

- Inline prompt vs `--prompt-file`
- Event stream emission (`--on-event`, NDJSON)
- Completion sentinel (`--sentinel-file`: `DONE`, `SESSION=`, `STOP_REASON=`, `RUN=`, `RESULT=` when a result file was written)
- Result file (`--result-file`: the run's final answer as plain text, written before the sentinel)
- Pid file (`--pid-file`: the run's own pid while it is alive, removed on exit)
- Timeout guards (`--timeout`, `--progress-timeout`)
- Label correlation (`--label` appears in events)

## How to get to it (user POV)

`avenor run` (or bare `avenor`) is the front door: start an agent with a
prompt, block until it finishes. Users pick a backend (`--backend`), aim it at
a working directory (`--dir`), and optionally capture events and the sentinel.

## Driving it with the avenor binary

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
./avenor run --backend pi \
  --prompt "Reply with the single word OK and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/events.ndjson" \
  --sentinel-file "$SCRATCH/done.env" \
  --result-file "$SCRATCH/result.txt" \
  --pid-file "$SCRATCH/run.pid" \
  --timeout 90s --label verify-smoke

cat "$SCRATCH/done.env"
cat "$SCRATCH/result.txt"
grep -c '"event"' "$SCRATCH/events.ndjson"
test -e "$SCRATCH/run.pid" && echo "pid file outlived the run"
```

**Observable end state (the proof):** exit code 0; sentinel contains `DONE`,
a `SESSION=` id, `STOP_REASON=end_turn`, and `RESULT=` pointing at the result
file; the result file holds the same text as the terminal `session.end`
`final_output`; the pid file is gone because the run exited; `events.ndjson`
contains a `session.start` and a terminal `session.end` with
`stop_reason: end_turn`, all carrying the `verify-smoke` label.

To prove the pid file tracks the live process rather than a launcher, start
the run under `set -m` (which makes the background job a process-group leader,
so `setsid` forks and `$!` names a shell that exits immediately), read the pid
from the file while the run is alive, and check `kill -0`. The `$!` pid is gone
while the pid-file pid is alive; both are gone after the sentinel appears.

## Gotchas

- Runs are real model calls — keep prompts to one-word replies.
- `--dir` is where the agent works; always point it at a scratch dir, never
  the repo.
- A bare `avenor run` (no `--backend`) defaults to `opencode-acp`, which needs
  `opencode` on PATH — absent here. Always pass `--backend pi`.
- `--timeout` matters: without it a wedged backend hangs the drive forever.
- A provider limit failure ends the run with exit code `6` (`quota_exhausted`)
  or `7` (`rate_limited`) instead of `1`, and the sentinel carries
  `RETRY_AFTER=` when the provider stated a wait. `session.end` carries the
  same `stop_reason`, so check the terminal event before treating a FAILED
  sentinel as an agent failure.
- The permission handler defaults to `file:<sentinel-base>` when
  `--sentinel-file` is set; if you see the run stall, check for a permission
  request file next to the sentinel (see [permissions.md](permissions.md)).
