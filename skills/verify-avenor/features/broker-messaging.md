# Broker messages — child ↔ supervisor

## Sub-features

- A spawned `pi` child asks the supervisor and waits for its reply
  (`avenor_ask`, target `supervisor`)
- The supervisor host drains an inbound ask (`broker_receive`) and answers it
  (`avenor_reply`)
- Broker run addressing: the supervisor is `supervisor`; a child is its runtime
  ID (`rt_N`). Public spawn run IDs and labels are not broker targets.

## How to get to it (user POV)

A supervisor host session — a `pi` session running the `@dougbots/avenor-pi`
extension — owns the supervisor broker. It spawns a child, the child asks the
supervisor a question, and the host answers the ask the extension delivers into
its context. The child's tool result carries the supervisor's text.

## Driving it with the avenor binary

The drive runs entirely outside the checkout: one scratch tree holds the scoped
`HOME`, the broker state, the scratch Pi home, the child's working directory and
the evidence. Create it once and derive every other path from `$SCRATCH`.

```sh
PI_HOME=$HOME/.pi/agent          # your real Pi home, captured before HOME moves
SCRATCH=$(mktemp -d)
mkdir -p "$SCRATCH/home" "$SCRATCH/pi-home" "$SCRATCH/work" "$SCRATCH/evidence"
```

Build the binary into the scratch and the extension the child will load, from
the repo root:

```sh
mkdir -p .cache/go-build .cache/go-mod
GOCACHE=$PWD/.cache/go-build GOMODCACHE=$PWD/.cache/go-mod \
  go build -o "$SCRATCH/avenor" ./cmd/avenor
bun install --frozen-lockfile
bun run --cwd packages/core build
bun run --cwd packages/pi build
```

Copy your Pi credentials and models into the scratch Pi home and point its
`settings.json` at this checkout's built extension. Keeping your `packages`
(including the `rm-guard`) matters for the child: the supervisor's `pi` children
read this settings file, so they load the guard as well as the extension. The
host, spawned next, runs with `--no-extensions` and therefore loads only the
extension named on its command line. The copied settings also keep your default
provider and model, so the host run needs no `--model` unless you want another.

```sh
export HOME="$SCRATCH/home"      # tree-budget files land in $HOME/.avenor/sockets
export AVENOR_HOME="$HOME/.avenor"
export PI_CODING_AGENT_DIR="$SCRATCH/pi-home"
mkdir -p "$AVENOR_HOME"
cp "$PI_HOME/auth.json" "$PI_HOME/models.json" "$PI_CODING_AGENT_DIR/"
cp "$PI_HOME/models-store.json" "$PI_CODING_AGENT_DIR/" 2>/dev/null || true
python3 - "$PI_HOME/settings.json" "$PI_CODING_AGENT_DIR/settings.json" "$PWD/packages/pi/dist/index.js" <<'PY'
import json, sys
source, path, extension = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    settings = json.load(open(source))
except FileNotFoundError:
    settings = {}
settings["extensions"] = [extension]
json.dump(settings, open(path, "w"), indent=2)
PY
```

Run the host with the same extension explicitly. It must stay alive long enough
to receive the ask; a print-mode host that exits immediately ends the supervisor
before the ask arrives.

```sh
AVENOR_BIN=$SCRATCH/avenor timeout -k 10 300 pi --no-session --no-extensions \
  --extension "$PWD/packages/pi/dist/index.js" \
  --no-skills --no-prompt-templates --no-mcp --no-context-files --print \
  "Spawn a pi child with avenor_spawn (wait false, dir $SCRATCH/work) whose prompt is: Call avenor_ask exactly once with to_run_id \"supervisor\", message \"PING-FROM-CHILD\", timeout_ms 120000, then report the answer and finish. Then keep this turn open: the extension delivers the child's ask into your context (it drains it with broker_receive every two seconds), so answer that ask with avenor_reply, using the from_run_id and reply_to_message_id the delivered ask names, and message \"PONG-FROM-SUPERVISOR\". Report the child's final output." \
  > "$SCRATCH/evidence/host.out" 2> "$SCRATCH/evidence/host.err"
```

**Observable end state (the proof):** the child's event log
(`$AVENOR_HOME/runs/<spawn-run-id>/events.log`) contains an `avenor_ask`
`tool.call` with `"to_run_id": "supervisor"`, a successful tool result reading
`Reply from supervisor:\nPONG-FROM-SUPERVISOR`, and a terminal `session.end`
whose `final_output` contains `PONG-FROM-SUPERVISOR`. The host's output
(`$SCRATCH/evidence/host.out`) carries the delivered ask — the child's `rt_N`
sender and its message id — and the reply it sent.

`avenor_status` is not part of this proof: it reports runs, not asks. The ask
reaches the host through the extension's `broker_receive` poll, which is what
surfaces it in the host session.

Tear the drive down, taking the copied credentials with it:

```sh
# The autostarted supervisor listens on $AVENOR_HOME/sockets/avenor-mcp-<pid>.sock.
for sock in "$AVENOR_HOME"/sockets/*.sock; do
  [ -S "$sock" ] && "$SCRATCH/avenor" control --socket "$sock" shutdown graceful
done
rm -f "$PI_CODING_AGENT_DIR"/auth.json "$PI_CODING_AGENT_DIR"/models.json "$PI_CODING_AGENT_DIR"/models-store.json
rm -rf "$SCRATCH"
export HOME=${PI_HOME%/.pi/agent}
unset AVENOR_HOME PI_CODING_AGENT_DIR
```

Never kill by process name: shut the supervisor down through its socket and
remove only `$SCRATCH`. A socket with no listener answers `connection refused`;
that instance is already gone.

## Gotchas

- A child's inherited-broker `avenor_ask` sends `to_run_id` unchanged. Target
  the supervisor as `supervisor`; target a sibling as its `rt_N`. A public spawn
  run id or a label does not resolve there. `404 target-unavailable` means the
  target is not a registered broker run, not that the broker is down.
- A child's `rt_N` is not the supervisor and does not reach its queue. Asking
  another child's `rt_N` reaches that child's run, which a direct spawn never
  unregisters: once that child ends, the ask is queued with no reader and the
  asker waits out `timeout_ms` instead of failing fast.
- The child process must load this checkout's avenor extension too. If only the
  host gets `--extension` and the child discovers an older installed extension,
  the child's `avenor_ask` goes through the parent control connection and fails
  `rpc error [-32010]: permission_denied` instead of using the broker.
- `avenor_spawn` is owner-gated. The host extension must be the version from
  this checkout, which attaches the published owner capability; an older
  installed build can fail the spawn itself with `permission_denied`.
- Keep the host alive until the reply is sent. A print-mode host can remain
  attached to its supervisor after its turn, so bound it with `timeout`: expect
  exit `124` at the 300s bound, and read the proof from `host.out` rather than
  the exit status. If the supervisor outlives the host, shut it down through the
  socket under `$AVENOR_HOME/sockets/`.
- The scratch Pi home starts with declarations but no installed package copies:
  the first `pi` process that loads them installs them under that agent home.
  Offline, drop the `packages` key from the scratch `settings.json` and the drive
  still runs — its prompts never issue a command the guard would block, and the
  host loads only its explicit `--extension` either way.
