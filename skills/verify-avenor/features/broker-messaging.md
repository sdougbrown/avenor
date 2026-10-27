# Broker messages — child ↔ supervisor

## Sub-features

- A spawned `pi` child asks the supervisor and waits for its reply
  (`avenor_message` action `ask`, target `supervisor`)
- The supervisor host drains an inbound ask (the extension's `message_receive`
  poll) and answers it (`avenor_message` action `reply`)
- A host blocked in `avenor_result` on a child gets that wait back with
  `interrupted_by` status `asking` when the child asks it a question, so the
  host answers without the user pressing `esc`
- Broker run addressing: the supervisor is `supervisor`; a child is its runtime
  ID (`rt_N`). Public spawn run IDs and labels are not broker targets.
- Hub and branches: a hub supervisor with a fixed `--broker-addr`, branches that
  join it from `AVENOR_PARENT_BROKER_URL` alone, peer discovery at the hub, and a
  hub child's ask answered by a branch's run (hub→branch; a sibling
  branch→branch ask is not covered here).

## How to get to it (user POV)

A supervisor host session — a `pi` session running the `@dougbots/avenor-pi`
extension — owns the supervisor broker. It spawns a child, the child asks the
supervisor a question, and the host answers the ask the extension delivers into
its context. The child's tool result carries the supervisor's text.

## Driving it with the avenor binary

The drive keeps its own state in one `mktemp -d` scratch directory and derives
every path from `$SCRATCH`. It never copies your Pi credentials and never
writes your Pi configuration. Create the scratch first:

```sh
SCRATCH=$(mktemp -d)
mkdir -p "$SCRATCH/work" "$SCRATCH/evidence"
export AVENOR_HOME="$SCRATCH/avenor-home"   # supervisor socket, records, events
mkdir -p "$AVENOR_HOME"
```

Build the binary the host starts and the extension the host loads, from the repo
root:

```sh
mkdir -p .cache/go-build .cache/go-mod
GOCACHE=$PWD/.cache/go-build GOMODCACHE=$PWD/.cache/go-mod \
  go build -o "$SCRATCH/avenor" ./cmd/avenor
bun install --frozen-lockfile
bun run --cwd packages/core build
bun run --cwd packages/pi build
```

The build writes the two caches under the checkout's `.cache` and refreshes
`node_modules` and `packages/{core,pi}/dist`. The run adds `$SCRATCH` — the
binary, `$AVENOR_HOME/{sockets,runs}` with the socket, owner record and each
run's events — plus one root tree-budget file in `~/.avenor/sockets`, which the
supervisor removes when it shuts down.

Run the host with this checkout's extension named on its command line, against
your normal Pi configuration, which the drive neither copies nor edits: no
extension is loaded from your settings, so the checkout's build is the one that
owns the broker. The extension starts the supervisor from `$SCRATCH/avenor`, and
the supervisor spawns the child on the `pi` backend. The host must stay alive
long enough to receive the ask; a print-mode host that exits immediately ends
the supervisor before the ask arrives.

```sh
AVENOR_BIN=$SCRATCH/avenor timeout -k 10 300 pi --no-session --no-extensions \
  --extension "$PWD/packages/pi/dist/index.js" \
  --no-skills --no-prompt-templates --no-mcp --no-context-files --print \
  "Spawn a pi child with avenor_spawn (wait false, backend pi, dir $SCRATCH/work) whose prompt is: Call avenor_message once with action \"ask\", target \"supervisor\", message \"PING-FROM-CHILD\", timeout_ms 120000, then report the answer and finish. Then keep this turn open: the extension delivers the child's ask into your context (it drains it with message_receive every two seconds), so answer that ask with avenor_message action \"reply\", using the reply_to message id the delivered ask names, and message \"PONG-FROM-SUPERVISOR\". Report the child's final output." \
  > "$SCRATCH/evidence/host.out" 2> "$SCRATCH/evidence/host.err"
```

**Observable end state (the proof):** the child's event log
(`$AVENOR_HOME/runs/<spawn-run-id>/events.log`) contains an `avenor_message`
`tool.call` with `"action": "ask"` and `"target": "supervisor"`, a successful
tool result whose text is the bare reply `PONG-FROM-SUPERVISOR` (the merged tool
renders `ask`/`replied` as the reply alone), and a terminal `session.end`
whose `final_output` contains `PONG-FROM-SUPERVISOR`. The child's sentinel
(`$AVENOR_HOME/runs/<spawn-run-id>/sentinel.done`) carries `DONE` and
`STOP_REASON=end_turn`.

The child's event log is the proof, and it is written while the run happens. The
host's output (`$SCRATCH/evidence/host.out`) shows the delivered ask — the
child's `rt_N` sender and its message id — and the reply it sent, once the host
finishes its turn; a print-mode host that is still attached when `timeout` kills
it flushes nothing, so a successful drive can leave `host.out` empty.

`avenor_status` is not part of this proof: it reports runs, not asks. The ask
reaches the host through the extension's `message_receive` poll, which is what
surfaces it in the host session.

### Blocking `avenor_result` drive

This drive reuses the same `$SCRATCH`, binary, and extension. Its host spawns a
`pi` child with `wait=false`, then blocks in `avenor_result` on that child. The
wait must return with the child named as the stopping point once the child
asks:

```sh
AVENOR_BIN=$SCRATCH/avenor timeout -k 10 300 pi --no-session --no-extensions \
  --extension "$PWD/packages/pi/dist/index.js" \
  --no-skills --no-prompt-templates --no-mcp --no-context-files --print \
  "Spawn a pi child with avenor_spawn (wait false, backend pi, dir $SCRATCH/work) whose prompt is: Call avenor_message once with action \"ask\", target \"supervisor\", message \"PING-FROM-CHILD\", timeout_ms 120000, then report the answer and finish. Then call avenor_result on that spawned run id and block. Do not press esc: the extension interrupts the wait when the child's ask arrives, returning interrupted_by with status \"asking\". Answer the delivered ask with avenor_message action \"reply\", using the reply_to message id the ask names, and message \"PONG-FROM-SUPERVISOR\". Report the interrupted_by payload and the child's final output." \
  > "$SCRATCH/evidence/host-blocking.out" 2> "$SCRATCH/evidence/host-blocking.err"
```

**Observable end state (the proof):** the host's `avenor_result` tool result in
`$SCRATCH/evidence/host-blocking.out` carries `interrupted_by.run_id` equal to
the child's `rt_N`, `interrupted_by.status` `"asking"`, and `ready` `false`,
instead of leaving the host parked. The child's event log
(`$AVENOR_HOME/runs/<spawn-run-id>/events.log`) contains the `avenor_message`
`tool.call` with `"action": "ask"` and `"target": "supervisor"`, a successful
tool result whose text is the bare reply `PONG-FROM-SUPERVISOR`, and a terminal
`session.end` whose `final_output` contains `PONG-FROM-SUPERVISOR`. The child's
sentinel (`$AVENOR_HOME/runs/<spawn-run-id>/sentinel.done`) carries `DONE` and
`STOP_REASON=end_turn`.

Without the interrupt, the host stays parked in `avenor_result` until `timeout`
sends it SIGTERM, so `host-blocking.out` carries no `interrupted_by` payload and
the child times out waiting for its answer.

Tear the drive down: shut the supervisor down through its socket, unset the
scratch env, and remove the scratch alone.

```sh
# The autostarted supervisor listens on $AVENOR_HOME/sockets/avenor-mcp-<pid>.sock.
for sock in "$AVENOR_HOME"/sockets/*.sock; do
  [ -S "$sock" ] && "$SCRATCH/avenor" control --socket "$sock" shutdown graceful
done
unset AVENOR_HOME
[ -n "$SCRATCH" ] && case "$SCRATCH" in
  /tmp/*|"${TMPDIR:-/tmp}"/*) rm -rf "$SCRATCH" ;;
  *) echo "refusing to remove $SCRATCH: not a temp directory" ;;
esac
```

Never kill by process name: shut the supervisor down through its socket and
remove only `$SCRATCH`. A socket with no listener answers `connection refused`;
that instance is already gone.

### Hub plus env-only branches

This drive needs no host session. A hub supervisor binds a fixed broker address;
two branch supervisors join it from `AVENOR_PARENT_BROKER_URL` alone (no
`--broker-parent-url` flag), which is the same resolution the interactive
autostart uses. A child on the hub discovers both branches through its peer
listing and asks one of their runs across the branch boundary. That is the
hub→branch leg; a sibling branch→branch ask is a separate leg and this drive
does not claim it.

```sh
SCRATCH=$(mktemp -d /tmp/avenor-hub.XXXXXX)
mkdir -p "$SCRATCH/work" "$SCRATCH/evidence"
export TMPDIR="$SCRATCH"
# Keep the operator's HOME: the pi backend reads its provider config from it.
BIN="$PWD/avenor"
MODEL=ollama-cloud/deepseek-v4.1-flash
EV="$SCRATCH/evidence"
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
HUB_SOCK="$SCRATCH/hub.sock"
TOKEN=$("$BIN" parent-token)
HUB_PID=; BRANCH_A_PID=; BRANCH_B_PID=   # record every pid you start

await_path() {  # await_path <path> <seconds> <what>
  _n=0
  until [ -e "$1" ]; do
    _n=$((_n + 1))
    [ "$_n" -le $(($2 * 10)) ] || { echo "FAIL: $3 did not appear within ${2}s: $1" >&2; return 1; }
    sleep 0.1
  done
}
await_status() {  # await_status <socket> <rt_id> <seconds> <what>; prints the idle status JSON
  _n=0
  while :; do
    _snap=$("$BIN" control --socket "$1" status "$2" 2>/dev/null)
    case "$_snap" in
      *'"status": "idle"'*|*'"status": "ended"'*) printf '%s\n' "$_snap"; return 0 ;;
    esac
    _n=$((_n + 1))
    [ "$_n" -le $(($3 * 2)) ] || { echo "FAIL: $4 did not reach idle within ${3}s; last status: $_snap" >&2; return 1; }
    sleep 0.5
  done
}
spawn_run() {  # spawn_run <socket> <evidence name> <spawn args...>; prints the runtime id
  _json=$("$BIN" control --socket "$1" spawn "${@:3}") || return 1
  printf '%s\n' "$_json" > "$EV/$2.spawn.json"
  printf '%s' "$_json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["runtime_id"])'
}

AVENOR_PARENT_BROKER_TOKEN=$TOKEN "$BIN" stable --control-socket "$HUB_SOCK" \
  --broker-addr "127.0.0.1:$PORT" &> "$EV/hub.log" &
HUB_PID=$!
await_path "$HUB_SOCK.broker.url" 15 "hub broker URL record" || exit 1
HUB_URL=$(cat "$HUB_SOCK.broker.url")

AVENOR_PARENT_BROKER_URL=$HUB_URL AVENOR_PARENT_BROKER_TOKEN=$TOKEN \
  "$BIN" stable --control-socket "$SCRATCH/branch-a.sock" &> "$EV/branch-a.log" &
BRANCH_A_PID=$!
AVENOR_PARENT_BROKER_URL=$HUB_URL AVENOR_PARENT_BROKER_TOKEN=$TOKEN \
  "$BIN" stable --control-socket "$SCRATCH/branch-b.sock" &> "$EV/branch-b.log" &
BRANCH_B_PID=$!
await_path "$SCRATCH/branch-a.sock" 15 "branch A socket" || exit 1
await_path "$SCRATCH/branch-b.sock" 15 "branch B socket" || exit 1
```

Branch B's run sleeps once so the cross-branch ask can land mid-turn, then
answers it; branch A's run only reports the peers it can see.

```sh
BRANCH_TARGET_RT=$(spawn_run "$SCRATCH/branch-b.sock" branch-target \
  --backend pi --model "$MODEL" --label branch-target --dir "$SCRATCH/work" \
  --sentinel-file "$EV/branch-target.sentinel" \
  --on-event "$EV/branch-target.events.jsonl" \
  --prompt 'Do not finish yet. Call the bash tool once with command sleep 45. A peer ask may be delivered into your context while it runs. After the bash call returns, answer the inbound ask with avenor_message action "reply", reply_to set to its message id, message "PONG-FROM-BRANCH", then report that you replied and finish.') || exit 1

BRANCH_OTHER_RT=$(spawn_run "$SCRATCH/branch-a.sock" branch-other \
  --backend pi --model "$MODEL" --label branch-other --dir "$SCRATCH/work" \
  --sentinel-file "$EV/branch-other.sentinel" \
  --on-event "$EV/branch-other.events.jsonl" \
  --prompt 'Call the avenor_peers tool once and report every peer run id and label it returns, then finish.') || exit 1

HUB_ASKER_RT=$(spawn_run "$HUB_SOCK" hub-asker \
  --backend pi --model "$MODEL" --label hub-asker --dir "$SCRATCH/work" \
  --sentinel-file "$EV/hub-asker.sentinel" \
  --on-event "$EV/hub-asker.events.jsonl" \
  --prompt 'Call the avenor_peers tool once. Find the peer whose label is "branch-target" (its run id contains "@"). Then call the avenor_message tool once with action "ask", target set to that exact run id, message "PING-FROM-HUB", timeout_ms 120000. Report the tool result and finish.') || exit 1

# Bounded: a supervised child parks after a successful turn and writes no
# sentinel while parked, so the turn is proven from the status snapshot and the
# event log, both captured while the supervisors still run.
await_status "$HUB_SOCK" "$HUB_ASKER_RT" 180 "hub asker turn" > "$EV/hub-asker.status.json" || exit 1
await_status "$SCRATCH/branch-b.sock" "$BRANCH_TARGET_RT" 180 "branch target turn" > "$EV/branch-target.status.json" || exit 1
await_status "$SCRATCH/branch-a.sock" "$BRANCH_OTHER_RT" 180 "branch other turn" > "$EV/branch-other.status.json" || exit 1
grep -q '"exit_code": 0' "$EV/hub-asker.status.json" || { echo "FAIL: hub asker exit_code is not 0" >&2; exit 1; }
grep -q '"exit_code": 0' "$EV/branch-target.status.json" || { echo "FAIL: branch target exit_code is not 0" >&2; exit 1; }
grep -q 'PONG-FROM-BRANCH' "$EV/hub-asker.status.json" || { echo "FAIL: hub asker final_output lacks the reply" >&2; exit 1; }
```

**Observable end state (the proof):** `hub-asker.status.json` shows `"status":
"idle"` (a parked supervised run; `ended` only after a reap), `"exit_code": 0`
and a `final_output` carrying `PONG-FROM-BRANCH`, and
`branch-target.status.json` shows the same idle/`0` pair and
`branch-other.status.json` shows it for branch A's run. The hub child's event
log (`$EV/hub-asker.events.jsonl`) carries the peers result naming both
env-only branches — run ids ending in `@branch-a-<digest>` and
`@branch-b-<digest>` (the branch identity is derived from the control socket
path and host) — one `avenor_message` `tool.call` whose result has
`"outcome": "replied"` and `"reply": "PONG-FROM-BRANCH"`, and a terminal
`session.end` with stop reason `end_turn` and exit code `0`; the branch
target's log carries the matching `reply` call. No sentinel exists yet: a run
writes its sentinel only when it ends, so these parked runs' sentinels appear
at teardown as `KILLED` with `STOP_REASON=cancelled` and `EXIT_CODE=130`.
`branch-a.log` and `branch-b.log` carry no `parent broker registration failed`
line, so both joined the hub from the environment alone.

### Interactive extension autostart joins the hub

This drive proves the env-only join for the surface operators actually use: a
`pi` host whose extension autostarts `avenor stable` with no broker flags.
Run the hub from the section above, then start the host with the hub URL and
credential in its environment and the checkout's extension named on the command
line (`--extension "$PWD/packages/pi/dist/index.js"` after building it, or the
installed `@dougbots/avenor-pi` dist: the join is resolved inside the supervisor,
so no extension change is involved). The explicit `--model` keeps the host off
the default model:

```sh
AVENOR_BIN="$PWD/avenor" AVENOR_HOME="$SCRATCH/branch-home" \
  AVENOR_PARENT_BROKER_URL=$HUB_URL AVENOR_PARENT_BROKER_TOKEN=$TOKEN \
  timeout -k 10 240 pi --no-session --no-extensions \
    --extension "$PWD/packages/pi/dist/index.js" \
    --no-skills --no-prompt-templates --no-mcp --no-context-files \
    --model ollama-cloud/deepseek-v4.1-flash --print \
    'Call the avenor_spawn tool exactly once: wait false, backend pi, model ollama-cloud/deepseek-v4.1-flash, label branch-target, dir '$SCRATCH/work', prompt "Do not finish early. Call the bash tool once with command sleep 60. After it returns, answer the inbound ask with avenor_message action reply, reply_to set to the ask message id, message PONG-VIA-AUTOSTART, then report that you replied.". After the spawn returns, report the run id and finish; do not wait for the child.' \
  > "$EV/host.out" 2> "$EV/host.err" &
HOST_PID=$!
BRANCH_HOME_SOCK=""
for _n in $(seq 1 150); do   # ~15 s deadline
  BRANCH_HOME_SOCK=$(ls "$SCRATCH"/branch-home/sockets/*.sock 2>/dev/null | head -1)
  [ -n "$BRANCH_HOME_SOCK" ] && break
  sleep 0.1
done
[ -n "$BRANCH_HOME_SOCK" ] || { echo "FAIL: autostarted branch socket never appeared" >&2; exit 1; }
```

Then repeat the hub-child ask from the previous drive, targeting the peer whose
label is `branch-target` in the hub's peer listing:

```sh
HUB_ASKER_RT=$(spawn_run "$HUB_SOCK" hub-asker-autostart \
  --backend pi --model "$MODEL" --label hub-asker-autostart --dir "$SCRATCH/work" \
  --sentinel-file "$EV/hub-asker-autostart.sentinel" \
  --on-event "$EV/hub-asker-autostart.events.jsonl" \
  --prompt 'Call the avenor_peers tool once. Find the peer whose label is "branch-target" (its run id contains "@"). Then call the avenor_message tool once with action "ask", target set to that exact run id, message "PING-AUTOSTART", timeout_ms 120000. Report the tool result and finish.') || exit 1
await_status "$HUB_SOCK" "$HUB_ASKER_RT" 180 "hub asker autostart turn" > "$EV/hub-asker-autostart.status.json" || exit 1
grep -q '"exit_code": 0' "$EV/hub-asker-autostart.status.json" || { echo "FAIL: autostart asker exit_code is not 0" >&2; exit 1; }
grep -q 'PONG-VIA-AUTOSTART' "$EV/hub-asker-autostart.status.json" || { echo "FAIL: autostart asker final_output lacks the reply" >&2; exit 1; }
```

**Observable end state (the proof):** `hub-asker-autostart.status.json` shows
`"status": "idle"`, `"exit_code": 0` and a `final_output` carrying
`PONG-VIA-AUTOSTART`, and the hub child's peers result in
`$EV/hub-asker-autostart.events.jsonl` contains
`supervisor@avenor-mcp-<pid>-<digest>` and `rt_1@avenor-mcp-<pid>-<digest>`:
the autostarted branch registered its supervisor and its child at the hub even
though no supervisor command line carried a broker flag. The ask to that child
returns `"outcome": "replied"` with `"reply": "PONG-VIA-AUTOSTART"`. Neither
spawned child has a sentinel while parked; the branch's `<socket>.broker.url`
record is removed when the autostarted supervisor is shut down through its
socket, which the teardown checks.

Tear down with the sockets and the recorded pids, never by process name. Every
pid kill is guarded, so a spawn that never happened cannot turn into a `kill 0`
of this shell's process group:

```sh
for s in "$SCRATCH/branch-home"/sockets/*.sock "$SCRATCH/branch-b.sock" \
         "$SCRATCH/branch-a.sock" "$HUB_SOCK"; do
  [ -S "$s" ] && "$BIN" control --socket "$s" shutdown graceful
done
if [ -n "$BRANCH_HOME_SOCK" ] && [ -e "$BRANCH_HOME_SOCK.broker.url" ]; then
  echo "FAIL: autostarted branch broker URL record survived shutdown" >&2
fi
for pid in "$HOST_PID" "$BRANCH_B_PID" "$BRANCH_A_PID" "$HUB_PID"; do
  case "$pid" in ''|*[!0-9]*) continue ;; esac   # unset or not a pid: never kill
  [ "$pid" -gt 0 ] && kill "$pid" 2>/dev/null
done
echo "evidence kept in $EV"
```

## Limitation

**With the installed `@dougbots/avenor-pi`, the child ask leg does not pass
today.** A `pi` child's `avenor_message` ask is refused at the broker (the child
posts `/send` without the broker generation and mints a versioned id), so the
child reports `ask "<id>" was not sent to "supervisor"; nothing is pending`
instead of `PONG-FROM-SUPERVISOR`. The same defects and the drive that does
pass, using a `pi` shim pointed at this checkout's build, is step 3 of
[messaging.md](messaging.md#the-bound-generation).

The child is a plain `pi` sub-process (`pi --mode rpc --no-session`), so it
loads whatever `@dougbots/avenor-pi` your Pi configuration installs. The recipe
does not point it at this checkout: redirecting a child's Pi home means copying
credentials into a scratch home and deleting them again on teardown, and that
teardown can delete the real credentials when it runs out of order.

So this drive proves the child leg only as far as your installed extension
speaks broker mode — an `avenor_message` ask that posts to `AVENOR_BROKER_URL`
instead of going back through a supervisor. With an install that predates that path, the
child starts or attaches to a supervisor of its own, the ask never reaches the
host, and the child's `avenor_message` ask result reports a timeout, a supervisor
startup failure, or a permission error instead of `PONG-FROM-SUPERVISOR`. Update
`@dougbots/avenor-pi` (or install this checkout's built `packages/pi/dist`)
before relying on this drive; do not reintroduce credential copying to work
around it.

The blocking `avenor_result` drive depends on the same child ask leg, so it
cannot run end to end with an installed extension that predates broker mode
either. With a broker-aware install, the wait still has to return
`interrupted_by` status `asking`; that assertion does not weaken.

## Gotchas

- A child's inherited-broker `avenor_message` ask sends its resolved run id
  unchanged. Target the supervisor as `supervisor`; target a sibling as its
  `rt_N`. A public spawn
  run id or a label does not resolve there. `target-unavailable` means the
  target is not a registered broker run, not that the broker is down
  (`docs/messaging.md`).
- A child's `rt_N` is not the supervisor and does not reach its queue. A direct
  child's run stays registered after the child ends, so an ask addressed to that
  `rt_N` is queued with no reader and the asker waits out `timeout_ms` instead
  of failing fast. A loop or team teardown deletes its children's runs, and
  claudechannel deletes its own, so for those children `target-unavailable` can
  mean the run ended (`docs/messaging.md`).
- Keep `$AVENOR_HOME` short. The host's supervisor binds
  `$AVENOR_HOME/sockets/avenor-mcp-<pid>.sock`, and a unix socket path over
  roughly 107 bytes fails to bind (`start control server: ... bind: invalid
  argument`), which the extension reports as `avenor exited with code 1 during
  startup`. A `mktemp -d` under `/tmp` is short enough; a deep `TMPDIR` is not.
- `avenor_spawn` is owner-gated. The host extension must be the version from
  this checkout, which attaches the published owner capability; an older
  installed build can fail the spawn itself with `permission_denied`.
- Do not gate on the exit code: `timeout -k 10 300` may end the host with `124`
  while the supervisor outlives it, and a print-mode host prints only when its
  turn finishes. Read the proof from the child's event log, and shut the
  supervisor down through the socket under `$AVENOR_HOME/sockets/`.
- The child runs with your real Pi configuration, so every extension you have
  installed (the `rm-guard` package included) applies to it. If one of them
  blocks the child's prompt, the drive stalls without reaching the ask.
