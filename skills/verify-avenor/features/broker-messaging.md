# Broker messages — child ↔ supervisor

## Sub-features

- A spawned `pi` child asks the supervisor and waits for its reply
  (`avenor_ask`, target `supervisor`)
- The supervisor host drains the child's ask (`broker_receive`) and answers it
  (`avenor_reply`)
- Broker run addressing: the supervisor is `supervisor`; a child is its runtime
  ID (`rt_N`). Public spawn run IDs and labels are not broker targets.

## How to get to it (user POV)

A supervisor host session — a `pi` session running the `@dougbots/avenor-pi`
extension — owns the supervisor broker. It spawns a child, the child asks the
supervisor a question, and the host answers from the inbound-ask poll. The
child's tool result carries the supervisor's text.

## Driving it with the avenor binary

Build the binary and the extension the child will load:

```sh
mise run go-build
bun install --frozen-lockfile
bun run --cwd packages/core build
bun run --cwd packages/pi build
```

Use a scratch Pi home under `.cache` so the drive neither reads nor writes the
installed extension. Copy your Pi credentials/models into it, then point its
`settings.json` at this checkout's built extension (the supervisor's `pi`
children inherit `PI_CODING_AGENT_DIR`):

```sh
SCRATCH=$PWD/.cache/broker-verify
mkdir -p "$SCRATCH/pi-home" "$SCRATCH/work" "$SCRATCH/evidence"
cp ~/.pi/agent/auth.json ~/.pi/agent/models.json ~/.pi/agent/models-store.json \
  "$SCRATCH/pi-home/"
python3 - "$SCRATCH/pi-home/settings.json" "$PWD/packages/pi/dist/index.js" <<'PY'
import json, sys
path, extension = sys.argv[1], sys.argv[2]
json.dump({"packages": [], "extensions": [extension]}, open(path, "w"))
PY
```

Run the host with the same extension explicitly. The host must stay alive long
enough to poll `broker_receive`; a print-mode host that exits immediately ends
the supervisor before the ask arrives.

```sh
AVENOR_BIN=$PWD/avenor AVENOR_HOME="$SCRATCH/avenor-home" \
PI_CODING_AGENT_DIR="$SCRATCH/pi-home" \
timeout 300 pi --no-session --no-extensions \
  --extension "$PWD/packages/pi/dist/index.js" \
  --no-skills --no-prompt-templates --no-mcp --no-context-files \
  --model deepseek/deepseek-flash --print \
  "Spawn a pi child with avenor_spawn (model deepseek/deepseek-flash, wait false, dir $SCRATCH/work) whose prompt is: Call avenor_ask exactly once with to_run_id \"supervisor\", message \"PING-FROM-CHILD\", timeout_ms 120000, then report the answer and finish. Then sleep and poll avenor_status until the inbound ask appears; answer it with avenor_reply message \"PONG-FROM-SUPERVISOR\"; report the child's final output." \
  > "$SCRATCH/evidence/host.out" 2> "$SCRATCH/evidence/host.err"
```

**Observable end state (the proof):** the child's event log (under
`$AVENOR_HOME/runs/<spawn-run-id>/events.log`) contains an `avenor_ask`
`tool.call` with `"to_run_id": "supervisor"`, a successful tool result reading
`Reply from supervisor:\nPONG-FROM-SUPERVISOR`, and a terminal `session.end`
whose `final_output` contains `PONG-FROM-SUPERVISOR`. The host output names the
inbound `PING-FROM-CHILD` message id and the reply it sent.

## Gotchas

- A child's inherited-broker `avenor_ask` sends `to_run_id` unchanged. Target
  the supervisor as `supervisor`; target a sibling as its `rt_N`. A public spawn
  run id or a label does not resolve there. A `404 target-unavailable` means the
  target is not a registered broker run, not that the broker is down.
- A child's `rt_N` is not the supervisor and does not reach its queue. Asking
  another child's `rt_N` reaches that child; once it has ended, the send fails
  `404 to run not found`.
- The child process must load this checkout's avenor extension too. If only the
  host gets `--extension` and the child discovers an older installed extension,
  the child's `avenor_ask` goes through the parent control connection and fails
  `rpc error [-32010]: permission_denied` instead of using the broker.
- `avenor_spawn` is owner-gated. The host extension must be the version from
  this checkout, which attaches the published owner capability; an older
  installed build can fail the spawn itself with `permission_denied`.
- Keep the host alive until the reply is sent. A print-mode host can remain
  attached to its supervisor after its turn, so bound it with `timeout`; if the
  supervisor outlives the host, shut it down through the socket under
  `$SCRATCH/avenor-home/sockets/` (`avenor control --socket <path> shutdown
  graceful`). Remove the scratch dirs afterwards, and never kill by process
  name.
