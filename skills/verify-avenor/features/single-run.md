# Single run — `avenor run`

## Sub-features

- Inline prompt vs `--prompt-file`
- Event stream emission (`--on-event`, NDJSON)
- Completion sentinel (`--sentinel-file`: `DONE`, `SESSION=`, `STOP_REASON=`, `RUN=`, `RESULT=` when a result file was written)
- Result file (`--result-file`: the run's final answer as plain text, written before the sentinel)
- Pid file (`--pid-file`: the run's own pid while it is alive, removed on exit)
- Signal cancellation (`SIGINT`/`SIGTERM`: exit 130, `KILLED` sentinel, no result file)
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

echo "exit=$?"
cat "$SCRATCH/done.env"
cat "$SCRATCH/result.txt"
grep -c '"event"' "$SCRATCH/events.ndjson"
# The sentinel is written before the deferred pid removal, so poll for it.
for i in $(seq 1 50); do [ -e "$SCRATCH/run.pid" ] || break; sleep 0.1; done
[ -e "$SCRATCH/run.pid" ] && echo "pid file outlived the run"
```

**Observable end state (the proof):** `exit=0`; sentinel contains `DONE`,
a `SESSION=` id, `STOP_REASON=end_turn`, and `RESULT=` pointing at the result
file; the result file holds the same text as the terminal `session.end`
`final_output`; the pid file is gone within the polling window; `events.ndjson`
contains a `session.start` and a terminal `session.end` with
`stop_reason: end_turn`, all carrying the `verify-smoke` label.

Then drive the signal path with a run that is still alive when the signal
lands (the essay prompt buys the time; if the run finishes first, repeat with a
longer one):

```sh
./avenor run --backend pi \
  --prompt "Write about 1000 words on tides." \
  --dir "$SCRATCH" \
  --sentinel-file "$SCRATCH/term.done" \
  --result-file "$SCRATCH/term-result.txt" \
  --pid-file "$SCRATCH/term.pid" \
  --timeout 90s --label verify-term &
RUN=$!
for i in $(seq 1 100); do [ -s "$SCRATCH/term.pid" ] && break; sleep 0.1; done
PIDPID=$(cat "$SCRATCH/term.pid" 2>/dev/null || true)
# Prove the pid names this run before signalling it: a bare `kill -0` also
# succeeds for a recycled pid, and a finished run leaves no pid file to read.
if [ -n "$PIDPID" ] && ps -o command= -p "$PIDPID" | grep -q avenor; then
  kill -TERM "$PIDPID"
else
  echo "pid file does not name a live avenor run; repeat with a longer prompt"
fi
wait $RUN; echo "exit=$?"
cat "$SCRATCH/term.done"
for i in $(seq 1 50); do [ -e "$SCRATCH/term.pid" ] || break; sleep 0.1; done
if [ -e "$SCRATCH/term.pid" ]; then echo "pid file outlived the run"; fi
if [ -e "$SCRATCH/term-result.txt" ]; then echo "cancelled run wrote a result file"; fi
```

**Killed run (the proof):** `exit=130`; the sentinel starts with `KILLED` and
carries `STOP_REASON=cancelled` and `EXIT_CODE=130`, with no `RESULT=` line;
there is no result file; the pid file is gone within the polling window.

To prove the pid file tracks the live process rather than a launcher, the block
above reads the pid from the file and checks what `ps -o command= -p <pid>`
names before signalling — a bare `kill -0` also succeeds for a recycled pid, and
a `$!` captured from a `setsid` launcher can name a shell that already exited.
The pid file is gone after the sentinel appears.

## Claude backend: OSC 7501 status reports

On its default PTY launcher the `claude` backend answers Claude Code's OSC 7501
detection query and uses Claude's own status reports for readiness, status, and
completion gating. Prove both the report path and the scraping fallback with
two one-word runs. When driving from inside Claude Code, scrub every `CLAUDE*`
variable from the run's environment: the hosted Claude otherwise inherits the
outer session and the run hangs or misbehaves.

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
SCRUB=$(env | awk -F= '/^CLAUDE/{print "-u " $1}')
env $SCRUB ./avenor run --backend claude --model haiku \
  --prompt "Reply with the single word OK and nothing else." \
  --dir "$SCRATCH" --on-event "$SCRATCH/osc.ndjson" \
  --sentinel-file "$SCRATCH/osc.env" --timeout 120s
echo "exit=$?"
grep -o '"ready_signal":"[a-z0-9]*"' "$SCRATCH/osc.ndjson"
grep -c '"signal":"osc7501"' "$SCRATCH/osc.ndjson"

env $SCRUB AVENOR_CLAUDE_OSC7501=off ./avenor run --backend claude --model haiku \
  --prompt "Reply with the single word OK and nothing else." \
  --dir "$SCRATCH" --on-event "$SCRATCH/off.ndjson" \
  --sentinel-file "$SCRATCH/off.env" --timeout 120s
echo "exit=$?"
grep -o '"ready_signal":"[a-z0-9]*"' "$SCRATCH/off.ndjson"
grep -c '"signal":"osc7501"' "$SCRATCH/off.ndjson"
```

**Report path (the proof):** `exit=0`; `agent.prompt_submitted` carries
`"ready_signal":"osc7501"`; at least one `agent.status` carries
`"signal":"osc7501"` with `"phase":"working"` and `"source":"pty"`, emitted
once (not repeated every 500ms); the terminal `session.end` has
`stop_reason: end_turn` and `final_output` `OK`.

**Fallback (the proof):** `exit=0`; `"ready_signal":"pane"`; zero
`"signal":"osc7501"` events; the run still ends with `stop_reason: end_turn`.
`AVENOR_CLAUDE_TERMINAL=tmux` also gives `ready_signal=pane`: the tmux launcher
and `claude-channel` never answer the query.

**MCP startup wait:** rerun the report-path drive with `--claude-mcp-wait 4s`.
Proof: `agent.prompt_submitted` carries `startup_wait_ms` (about 4000 minus the
time readiness took) and lands about 4s after `session.start`; without the
flag the field is absent. `--claude-mcp-wait 61s` and a non-claude backend with
a non-zero wait exit `1` before launching anything.

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
- A cancelled or timed-out run deliberately writes no result file and no
  `RESULT=` line, so do not read the result path unconditionally after
  `exit=130` or `exit=124`.
- The pid file is removed on exit, including on a trapped signal, but a
  `SIGKILL` leaves it behind — check `ps -o command= -p <pid>`, not `kill -0`.
