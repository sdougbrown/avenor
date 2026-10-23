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
