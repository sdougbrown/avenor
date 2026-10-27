# Avenor plugin (Claude Code)

Delegate work to independent avenor runs and supervise them without leaving the
conversation. The plugin ships the Avenor MCP server (`avenor mcp`) plus the
skill set that teaches a model how to drive it.

## Native peer layer (issue #391)

The `hooks/` directory is a mods layer that makes this Claude session itself a
broker peer — a session you started by hand, and a run avenor spawned — so
avenor runs become addressable like teammates:

- **Send:** `SendMessage({to: "avenor:<run id or label>"})` resolves the
  address against the broker's `GET /sessions` and delivers through
  `POST /send`. The hook answers `{isDelivered: true}` only after the broker
  queues the message; otherwise it answers `{isDelivered: false, reason}`
  (nothing is claimed on a failure, and a refused reply keeps its ask
  correlation, so a retry still rides the edge). A `/sessions` answer the
  broker flagged as a partial listing (`peers_error`) refuses the ref
  outright instead of matching a label against the subset it did return —
  the rule the Go and TS resolvers apply to the same field. Everything the
  layer does not own passes through untouched: non-`avenor:` addresses
  always, and even an `avenor:` address while the layer is disabled or
  unconfigured. A send that answers a peer's pending inbound ask
  (`expects_reply`) rides that ask's reply edge, unblocking the asker's own
  wait.
- **Ask:** the avenor MCP surface's `message` tool already carries an `ask`
  action; with the peer layer on, a `tool.call` hook serves that action
  through this session's own broker credentials (an `expects_reply` send
  blocking on `/wait_reply`, the same shape the avenor surfaces' ask action
  takes, bounded by `timeout_ms`, 0–600000 ms). Resolution and the ask's
  generation pin come from one `/sessions` read, not two. A failed ask
  renders the shared ask sentences from `schemas/message.outcome.json`
  (`not-sent`, `timed-out`, `possibly-pending`, …) — the same text and the
  same withdraw guidance a pi or Go peer's ask returns — and classifies the
  failure from the broker's own facts (the row's `outcome`, the 504's
  `timeout: true`), never from the error's prose; the raw wire text only
  reaches the log line. A failed `/wait_reply` also retracts the ask
  (`POST /cancel_message`), as the pi and core ask paths do, so a reply that
  arrives late finds no edge instead of landing where nothing waits; the
  retraction is best-effort and never replaces the ask's own outcome.
  Every other action and every call made while the layer is off passes
  through to the MCP server. The mod does not register its own tool under
  `mcp__avenor__*`: the plugin also ships an MCP server named `avenor`, and
  the host refuses such a registration as a replacement for it.
- **Reply:** an ask this session *received* lives in the hook's own
  correlation table, not in an edge the MCP server can claim, so the hook
  also serves `reply`: `{action: "reply", reply_to: "<message id from the
  frame>", message}` is sent along the same reply-edge send `SendMessage`
  uses, and answers the router's reply result (`reply to "av1…" delivered`).
  A `reply_to` the hook does not hold — an ask it never received, or one it
  already answered — passes through to the MCP server, whose own
  `edge-absent` refusal is then the honest answer.
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
instead of erroring — every send (an `avenor:` address included) passes
through to the normal pipeline and no subscription runs.

## Bootstrap

Credentials ride the environment, using the same names the avenor launcher
already uses for its peers:

- `AVENOR_BROKER_URL` — http:// base of the `avenor stable` supervisor's
  broker (its own process; the port is ephemeral).
- `AVENOR_RUN_ID` — the run this session joins as.
- `AVENOR_BROKER_TOKEN` — the credential for that run.
- `AVENOR_SESSION_NAME` — optional display name, posted to the broker's
  `/session_name` by a session that self-registers; the posted name is what
  `avenor:<label>` resolution and peer lists see. A provisioned run posts
  nothing: the supervisor already registered it under the label peers address,
  and the launch copy of this variable only records the identity as the child
  joined it.

The layer picks one of two identities at `session.start`, and which one it is
turns on the token:

- **Child run (provisioned).** `AVENOR_RUN_ID` and `AVENOR_BROKER_TOKEN`
  together are an identity someone else already holds. This is what a spawned
  Claude child gets: the avenor supervisor provisions a broker run for every
  child it starts and hands the run id, its token, its label and the broker
  URL to the child through the launch environment. The layer then registers as
  that run — no `/register` call, and no `/session_name` write, so the label
  the supervisor gave the run survives. The child appears in peer lists as the
  same run `avenor control list` shows, under the label it was spawned with,
  and its broker record belongs to the supervisor, which retires it when the
  run ends.
- **Top-level session (self-registered).** Without a token (an
  `AVENOR_RUN_ID` alone only pins the id) the session registers itself through
  the broker's open `/register` route and the minted token travels only to
  this process. That is the session you started by hand: export the variables
  in the shell that launches `claude`, or set them under `env` in
  `~/.claude/settings.json`.

A hosted child never inherits a launching session's copy of the four names:
avenor's terminal launcher strips them (a tmux pane would otherwise pick up
whatever the tmux server held) and exports only what it provisions. So a
child is a peer exactly when avenor made it one; a session started by hand in
a shell that exported them is a peer on its own terms.

Without `AVENOR_BROKER_URL` the layer stays off with one explanatory log line,
and so does a registration the broker refuses — the reason reaches the
session's log either way.
Same-UID local tool, per the repo's threat model: the broker's local routes
trust the loopback user, and the mod keeps its token in memory only. Nothing
about the layer is hardened beyond that model.

## Spawning runs (the MCP surface)

The MCP tools (`avenor` server in `.mcp.json`) are the unchanged surface:
`spawn`, `status`, `result`, `message`, `peers` and friends. A spawned run on
this machine is addressable by its broker label via `avenor:<label>` as soon
as `GET /sessions` lists it — and a spawned Claude run lists itself, because
the plugin it loads joins the run the supervisor provisioned for it.

## Limitations

- Provisioning is the default `claude` backend's: a spawned child joins as
  its run and takes inbound asks, but the supervisor still owns the run's
  record and retires it when the run ends.
- The reply correlation is a heuristic: a plain send to a peer with a
  pending inbound ask answers the most recent one. An explicit `reply`
  action names the ask instead, so concurrent asks from one peer can each be
  answered; the table keeps the newest 128 unanswered asks.
- The pass-through `turn.start`/`turn.complete` counters log one benign
  "hook skipped: returned the wrong shape (no result)" notice per turn; the
  turn events themselves proceed untouched.
- A hot `/reload-plugins` does not re-run `session.start` (verified live:
  nothing re-registers, no fresh log line), and the armed layer keeps serving
  the session unchanged across the reload — an inbound ask and an outbound
  send both worked immediately after a live reload. Restart the session to
  run new module code.
- A self-registered run's broker record outlives the session (there is no
  run-deregistration route for non-provisioned runs); it stops being a live
  peer when the session ends.