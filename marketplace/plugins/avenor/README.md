# Avenor plugin (Claude Code)

Delegate work to independent avenor runs and supervise them without leaving the
conversation. The plugin ships the Avenor MCP server (`avenor mcp`) plus the
skill set that teaches a model how to drive it.

## Native peer layer (issue #391, PR 1)

The `hooks/` directory is a mods layer that makes this Claude session itself a
broker peer, so avenor runs become addressable like teammates:

- **Send:** `SendMessage({to: "avenor:<run id or label>"})` resolves the
  address against the broker's `GET /sessions` and delivers through
  `POST /send`. The hook answers `{isDelivered: true}` only after the broker
  queues the message; otherwise it answers `{isDelivered: false, reason}`
  (nothing is claimed on a failure), and anything not starting with `avenor:`
  passes through untouched. A send that answers a peer's pending inbound ask
  rides that ask's reply edge, unblocking the asker's own wait.
- **Ask:** the avenor MCP surface's `message` tool already carries an `ask`
  action; with the peer layer on, a `tool.call` hook serves that action
  through this session's own broker credentials (an `expects_reply` send
  blocking on `/wait_reply`, the same shape the avenor surfaces' ask action
  takes, bounded by `timeout_ms`, 0–600000 ms). Every other action and every
  call made while the layer is off passes through to the MCP server. The
  mod does not register its own tool under `mcp__avenor__*`: the plugin also
  ships an MCP server named `avenor`, and the host refuses such a
  registration as a replacement for it.
- **Receive:** the mod holds a long-poll subscription on the broker's
  `POST /poll-control` (agent rows only) for the session's life. An inbound
  ask is submitted as a prompt when the session is idle — it starts its own
  turn — and appended into the conversation as the user-role row the turn
  reads like a steer while a turn is running. The sender is the broker's
  own display name for the run, never a field of the message text, and the
  frame tells the model to answer with `SendMessage` to `avenor:<run id>`.

Tested Claude Code version: **2.1.311**. There is no `engines` pin in the
manifest, so the mod gates itself at runtime with `$.session.version()`: on an
older release the layer logs one line (peer layer off) and disables itself
instead of erroring — hooks pass every send through and no subscription runs.

## Bootstrap

Credentials ride the environment, using the same names the avenor launcher
already uses for its peers:

- `AVENOR_BROKER_URL` — http:// base of the `avenor stable` supervisor's
  broker (its own process; the port is ephemeral).
- `AVENOR_RUN_ID` — optional. Set it to pin this session's broker run id
  (the recommended way to make a top-level session deterministically
  addressable, e.g. `claude-parent`); unset, the mod self-mints `claude-<hex>`.
- `AVENOR_BROKER_TOKEN` — optional; set only when provisioning an
  already-registered run (PR 2's launcher path). A top-level session omits it:
  the mod self-registers under its run id through the broker's `/register`
  route and the broker mint token travels only to this process.
- `AVENOR_SESSION_NAME` — optional display name, posted to the broker's
  `/session_name`, which is what `avenor:<label>` resolution and peer lists see.

For a top-level session, export the variables in the shell that launches
`claude`, or set them under `env` in `~/.claude/settings.json` (the session's
own process then carries them). The layer starts at `session.start`: it
self-registers, names its run, and begins the subscription; without
`AVENOR_BROKER_URL` it stays off with one explanatory log line.

Same-UID local tool, per the repo's threat model: the broker's local routes
trust the loopback user, and the mod keeps its minted token in memory only.
Nothing about the layer is hardened beyond that model.

## Spawning runs (the MCP surface)

The MCP tools (`avenor` server in `.mcp.json`) are the unchanged surface:
`spawn`, `status`, `result`, `message`, `peers` and friends. A spawned run on
this machine is addressable by its broker label via `avenor:<label>` as soon
as `GET /sessions` lists it.

## Limitations (PR 1)

- Provisioning (broker URL, run id, token) is what PR 2 will mint at spawn
  for Claude children; until then a spawned Claude run joins as a self-minted
  peer or needs the supervisor's credentials another way.
- The reply correlation is a heuristic: a send to a peer with a pending
  inbound ask answers the most recent one. Multiple concurrent asks from the
  same peer collapse to the last.
- The pass-through `turn.start`/`turn.complete` counters log one benign
  "hook skipped: returned the wrong shape (no result)" notice per turn; the
  turn events themselves proceed untouched.
- A hot `/reload-plugins` re-registers and re-subscribes the session; the
  previous run record stays in the broker until the supervisor restarts.
- A self-registered run's broker record outlives the session (there is no
  run-deregistration route for non-provisioned runs); it stops being a live
  peer when the session ends.