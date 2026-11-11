# Messaging Transport

Avenor's broker asks — one run sending another a question and waiting for the
reply — run on one shared transport contract. The local broker, the host client
and the in-process implementation all report the same outcomes, the same typed
errors and the same message-ID rules, so an ask behaves the same whichever
transport carries it.

## Packages

| Package | Role |
|---|---|
| `internal/messaging` | Leaf contract: request/result types, the `Transport` and `Seam` interfaces, the outcome vocabulary, the v1 wire error classes, the message-ID minter. Depends on the standard library only. |
| `internal/messaging/askstore` | Ask state: admission, edges, terminal fences, pruning and generation rotation, over an optional delivery backend. |
| `internal/messaging/askclient` | The single Go ask client. Owns send→wait→withdraw ordering and renders results for the tool surfaces. |
| `internal/messaging/inprocess` | Runs the transport in the same process, over the store. |
| `internal/messaging/localhttp` | Drives the local broker router over its authenticated HTTP endpoints. |
| `internal/messaging/hosthttp` | The v1 host client, used against the protocol fixture. |
| `internal/messaging/internal/httpbase` | Shared HTTP request/envelope helpers for the two HTTP transports. |
| `internal/messaging/internal/hostfixture` | Test-only v1 server backed by a real ask store. |
| `internal/messaging/messagetest` | Shared suites: transport contract, store contract, failure matrix. |

`internal/messaging` never imports a sibling, so the dependency direction stays
`askstore`/`askclient`/`inprocess`/`localhttp`/`hosthttp` → `messaging`. A
transport that needs the store takes it as a parameter, and nothing outside
`_test.go` files imports `hostfixture`.

## Addressing runs

A broker message is addressed to a **broker run ID**, the key the broker
registered for the participant. It is not the public spawn run ID and not the
display label.

| Participant | Broker run ID | Registration |
|---|---|---|
| Supervisor | `supervisor` | Eagerly at supervisor start, before any child exists. |
| Spawned child | its runtime ID (`rt_N`) | Before the child's backend starts, so it can poll and reply. |

The supervisor's display label is not its run id: it is derived from the
supervisor's working directory (the base name of the git top-level, plus
`@<branch>` when the checked-out branch is not `main` or `master` and HEAD
is not detached), sanitized to the session-name rule, truncated to 64 runes,
led by the machine's short hostname when the supervisor's parent broker is on
another machine (a loopback address, one of this machine's own addresses, or its
own name adds none), and suffixed `-2`…`-9` when another run already holds it.
Only the supervisor's own label takes the prefix: a run its branch owns reaches
the parent under the label it holds locally. When the
derivation yields nothing usable the label falls back to the working
directory's base name, then to the literal label `session`; the reserved
references (the empty reference, `self`, `supervisor`) are never used as
labels. The supervisor's resolved label follows one precedence: the
harness's session name, then an explicit label, then the work-derived
default, then the run id.

A spawned `pi` child asks through its inherited broker credentials. That path
sends `to_run_id` after resolving a label through the broker's visible peers,
the same ladder the host's message tool ask uses, but it never resolves a
public spawn ID first. A child asking its supervisor therefore has
to use `supervisor`. Asking `rt_N` addresses that child, not the supervisor. A
child asking a sibling uses the sibling's `rt_N`.

A direct child — one the supervisor runs through `runChild` — keeps its broker
run registered for the supervisor's lifetime: a spawn that ends on its own
removes nothing. An ask addressed to that child after it has ended is therefore
still admitted and queued against its run, where nothing reads it, so the asker
waits out its own timeout instead of being told the target is gone. Other
children do remove their runs. A loop or team teardown deletes its children's
runs, and a provider that owns a session-scoped run deletes it when its session
ends. For those runs `target-unavailable` can mean the run ended; for a run
nothing removed, it means the run was never registered on that broker.

Each OpenCode conversation binds through the owner control connection's
`message_bind`: the supervisor maps one (host, native session) pair to exactly
one broker run and returns that run's messaging credentials. The conversation's
messaging tools and its one inbound consumer authenticate as that run and
nothing else. A spawn the conversation starts names its own bound run as
`parent_run_id`, so a child's messages ride the same inbox the conversation's
feed consumes; a conversation whose bind cannot land advertises no parent route
at all, and neither does a spawn explicitly pointed at another supervisor —
that broker holds no record of the bound run, so its child starts without one.
The supervisor reclaims a bound run the broker reports dead and mints a
replacement on the next bind.

The pi extension supplies it too, as `supervisor` — the run id, not the
display label — for host-mode spawns that reach this process's singleton
supervisor: omit `supervisor_id` or supply the singleton's own socket. The
supervisor registers that run eagerly, so the note
reaches the child without the spawner writing the address into the brief.

Spawns to a non-local supervisor omit the field because the host polls only its
singleton's ask queue. A pi session that is itself a child also omits it: its
spawns start a separate supervisor whose broker has no reader for this session.
These children need an address supplied in the brief; the extension does not
forward their asks to the parent.

## Poo feed surface

`SurfaceFeed` is the broker surface used by `avenor feed broker` and `avenor message`. For an enabled poo child, Avenor starts the broker feed as `avenor-inbox`; poo presents incoming messages to the model as `<feed name="avenor-inbox" …>` inputs. The feed drains the broker queue at most once and records incoming asks before delivering them. An ask's input names its message ID and instructs the model to answer through `call_cli`:

```json
{"name":"avenor_message","args":{"action":"reply","reply_to":"MESSAGE_ID","message":"answer"}}
```

The `avenor_message` registry entry invokes `avenor message` as the child, with the entry's `message` argument delivered through stdin (`--message-stdin`) so the text stays out of the command's arguments. See [poo messaging](backends.md#messaging) for the switch, launch checks and credential visibility.

## OpenCode conversations

A bound OpenCode conversation runs the same pieces a poo child does, as the
conversation's own broker run:

- **Inbox** — the plugin starts one `avenor feed broker` per bound conversation,
  authenticated by the run's credentials in the child's environment (never
  argv). The feed's lock keeps a second consumer out, its parent-loss detection
  ends it with the host, and its ask records land in the same claim directory
  the reply path reads, so the feed and the reply share one claim store with no
  second copy.
- **Delivery** — each feed record becomes a prompt into the conversation, one
  record at a time in arrival order: the reader pauses the feed while a prompt
  is open, so a slow turn never reorders the inbox and a replaced or failed
  consumer delivers nothing further. A record a dying feed already wrote is
  still read and delivered; only the process's own close releases the inbox,
  so its replacement never starts into the dead consumer's lock. The
  plugin's frame marks the text as another session's words, never operator
  instructions, and maps the feed's `call_cli` answer instruction onto the
  native `avenor_message` tool: when a record's trailing note says to answer,
  the conversation answers with `action: "reply"` and the record's message id.
  The feed's own refusal wording passes through untouched.
- **Reply** — `avenor_message` with `action: "reply"` runs `avenor message` as
  the conversation, so the Go claim store authorizes exactly the asks the feed
  delivered here; a second reply reports `edge-absent`. The plugin passes reply
  text through stdin, not command arguments. Tell, ask and withdraw use the
  shared broker adapter; resume stays owner-only.
- **Identity** — the conversation's native title publishes as its harness name
  (`/session_name`), and each settled turn publishes through the peer
  `/last_message` route in per-conversation order, so peers' name and last
  reads see the conversation they name.

## Child CLI messages

A child with broker credentials can use `avenor message --action tell`,
`ask`, `reply` or `withdraw` to route messages as its own run. The command uses the same
router as other messaging surfaces; see [avenor message](cli.md#avenor-message).
Use `--message-stdin` instead of `--message` to keep message text out of command
arguments. The command reads up to 1 MiB of valid UTF-8 from stdin and refuses
a read error or non-UTF-8 input without sending; `--message` text must be
valid UTF-8 too, and invalid UTF-8 exits 1 without sending. The broker also
applies a limit to the encoded payload.

A reply to an ask needs the claim record the feed wrote. Use `--asks-dir` if
the feed uses a non-default directory. A second reply, or a reply to an ask
without a record, reports `edge-absent`. The broker resolves a reply to a tell
without a claim record. A `timed-out` ask exits 0 and remains open to a later
reply; use `--action withdraw --message-id MESSAGE_ID` to close it.

Each run gets its own directory under `feed-asks`. The default root is pruned
when a feed starts: records older than thirty days go, and a run directory left
empty goes with them. That window is a tradeoff rather than a wait bound. A
timed-out ask stays answerable until it is withdrawn, so a reply to one older
than the window reports `edge-absent` instead of landing. Losing an answered
ask's record changes nothing visible, because a second reply reports
`edge-absent` either way. Each record answers for its own age, so a run
directory a feed keeps writing to still has its expired records swept. The
directory itself is removed only when it is empty and untouched inside the
window. A feed creating its first record therefore cannot lose the directory to
a sibling's prune. Only the default root is pruned; a directory named by
`--asks-dir` is the caller's to clean.

## Transport contract

```go
type Transport interface {
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	Wait(ctx context.Context, req WaitRequest) (WaitResult, error)
	Reply(ctx context.Context, req ReplyRequest) (SendResult, error)
	Withdraw(ctx context.Context, req WithdrawRequest) (WithdrawResult, error)
	Receive(ctx context.Context, req ReceiveRequest) (ReceiveResult, error)
	Peers(ctx context.Context, req PeersRequest) (PeersResult, error)
	Preflight(ctx context.Context) (PreflightResult, error)
	Close() error
}
```

Every request names the caller's own identity (`RunID` and `Sender`) and, where
it matters, the generation it expects. No transport reads ambient credentials or
infers a sender. `Close` marks the transport closed, ends every in-flight call
and joins its goroutines; a wait interrupted that way reports
`possibly-pending` with its message ID retained, and a second `Close` returns
nil. A wait that ends — timed out, interrupted, or disconnected — never closes
the ask: its edge stays open until it is answered, withdrawn, or its acceptance
deadline passes.

Limits: a wait is capped at ten minutes, a message ID's encoded acceptance
deadline may not exceed twenty minutes beyond the receiver's clock (one more
wait window after the asker's own wait ends), a transport cleanup budget is five
seconds, and a receive that names no limit drains 32 messages.

## Outcomes

`Outcome` values travel in the transport responses and in a failed ask's
`data.outcome`, so the strings never change meaning.

| Outcome | Meaning |
|---|---|
| `sent` | The receiver accepted the message. For an ask, an edge exists; for a reply, the answer was delivered. |
| `replied` | A reply was delivered to the waiting sender. |
| `timed-out` | The bounded wait expired before a reply. The ask stays open for a later one. |
| `withdrawn` | The sender completed a withdrawal. |
| `edge-absent` | No edge or waiter matched the request. |
| `auth-rejected` | The supplied credentials were refused. |
| `not-sent` | The ask provably never reached admittance. |
| `possibly-pending` | Transmission could not be confirmed. Retain the message ID and never re-send. |
| `lost` | The ask is provably gone and cannot appear later. |

A failed ask reaches tool surfaces as a JSON-RPC error whose `data` carries
`code` (the typed tool code), `outcome`, `message_id` and `wire_code`, omitting
the last three where the failure does not know them; `resume` and the
`session_*` verbs report the authority's decision and send `code` alone. What a
caller does next follows from `outcome`: `not-sent` may be re-sent, while
`possibly-pending` means retain the message ID and never re-send it — a wait
whose cleanup withdrawal confirmed a terminal fence is not `possibly-pending`.
Withdrawal is `message_withdraw` (or the message tool's `withdraw` action),
which accepts the retained ID.

Two rules follow from the table and are enforced everywhere:

- **No replay.** An ask that may have been transmitted is never re-sent, and a
  rejection never reclassifies it as `not-sent`. Only a send that provably never
  reached the receiver is `not-sent`.
- **A withdrawal fences first.** Withdrawal installs a terminal fence before it
  reports absence, so a send that is still in flight cannot land afterwards. A
  missing edge or a bare HTTP 404 is never proof of terminal absence.

## Tool input schemas

`schemas/message.input.json` and `schemas/peers.input.json` are the contract
source: one `oneOf` variant per action, each a complete object, and the router's
admission table is compiled from them. MCP surfaces do not publish that union.
They publish `schemas/message.input.flat.json` and
`schemas/peers.input.flat.json`, which `internal/messaging/cmd/genschema`
compiles from the same union: one `type: object` with the action enum, the
union's action default and its per-action descriptions as one summary, every
action's properties with their declared types and limits, no `oneOf`/`anyOf`,
and only a required key that every action requires — `action` for `message`,
nothing for `peers`, whose `list` action takes no arguments.

The split exists because a real MCP client loses argument types from a union:
OpenCode 1.15.5 sends `timeout_ms` and a `payload` object as JSON strings when
the published schema is `oneOf`, and keeps them typed when it is flat. Admission
is unchanged — the router still refuses a field outside the chosen action's set,
a missing field, a wrong type and an out-of-range value with the contract's own
sentence. The flat schema is client guidance; the union stays the contract.

A published schema carries no `pattern` either, for the same reason: every
client validates a tool schema with its own regex dialect, and OpenAI's has no
Unicode property classes. Publishing the `peers` rename pattern
(`^[^\p{Cc}\p{Cf}\p{Zl}\p{Zp}]+$`) made every pi turn on an `openai-codex`
model fail with `Invalid schema for function` before the model ran (avenor#578).
The union and the admission table keep the pattern, and the rename authority
enforces the same rune rule, so dropping it from publication refuses nothing
that was admitted before.

## Wire errors

Every transport error names one of these classes. The code is what the wire
carries, the status is what an HTTP peer answers with, and the outcome is what a
caller reports.

| Code | HTTP status | Outcome |
|---|---|---|
| `auth-rejected` | 401 | `auth-rejected` |
| `invalid-request` | 400 | `not-sent` |
| `generation-mismatch` | 409 | `not-sent` |
| `unsupported-protocol` | 400 | `not-sent` |
| `malformed-message-id` | 400 | `not-sent` |
| `message-id-expired` | 400 | `not-sent` |
| `message-id-horizon` | 400 | `not-sent` |
| `duplicate-message-id` | 409 | `not-sent` |
| `mutual-ask` | 400 | `not-sent` |
| `fence-capacity` | 503 | `not-sent` |
| `sender-mismatch` | 403 | `auth-rejected` |
| `not-ask-sender` | 403 | `edge-absent` |
| `not-ask-target` | 403 | `edge-absent` |
| `target-unavailable` | 503 | `not-sent` |
| `transport-closed` | 400 | `not-sent` |
| `unsupported-operation` | 400 | `not-sent` |

Host endpoints authenticate with the `X-Avenor-Capability` and
`X-Avenor-Secret` headers. Local endpoints authenticate with the caller's own
run ID and token, exactly as the rest of the local router does: in the JSON
body of a POST, and in the `X-Avenor-Run-Id` and `X-Avenor-Run-Token` headers
of a GET. The router never reads credentials from the URL.

The table is the v1 host contract. The local broker router answers
`target-unavailable` with 404 rather than 503, for both an ask naming an
unregistered run and an undeliverable message; a local caller reads the code and
never the status digit.

## Local adapter

The local adapter drives the broker's own router:

| Path | Purpose |
|---|---|
| `/sessions` | Authenticated preflight and peer listing: the broker's generation, its `messaging_protocol` marker, and the peers the installed messenger can see. A listing the messenger could not finish is reported as `peers_error` beside the partial `sessions` array, and the adapter rejects it instead of treating the partial list as complete. The policy is shared across surfaces: a provider that resolves a target or reads a run state from the listing — the Go child and MCP providers and the TypeScript host and child providers alike — refuses the lookup when the listing is incomplete, even when the target is in the partial set; only the peers tool serves the partial list beside the detail. |
| `/send` | Admit one message or ask. A reply (`reply_to` set) may omit `to_run_id`: the broker routes the reply to the sender of the tell or ask the id names, and reports `edge-absent` when no route exists. A reply that names a target but finds no live edge is delivered to that target and reports `sent`, because the answer is already queued for it. A reply that cannot be routed to a local sender relays the way a tell does, so a reply to a cross-branch tell reaches the sender's branch. The reply carries its own route key (`<id>.reply`) and the id it answers, so the recipient can answer the answer; an `ask` result with outcome `replied` exposes that route key as `reply_id`. A message with neither `to_run_id` nor `reply_to` is refused `400 invalid-request`. |
| `/wait_reply` | Wait for the ask's terminal state. |
| `/cancel_message` | Withdraw an ask. |
| `/poll-control` | Drain queued control and agent messages. |
| `/attach` | Spend one launch's attach credential: `run_id` and `token`, answered once with the run's own `{run_id, token}`. The broker mints these per launch (`MintAttach`) and the provider puts one in a provisioned child's environment, so the first session that presents it is the run's inbound consumer; a second presentation of the same credential is refused `409` with code `attach-consumed`, which is what keeps one run to one poller. The route is unauthenticated in the `/register` sense — the credential is the authentication and it is worthless once spent — and it never accepts the run's own token in place of a launch grant. A grant exists only for a record the broker's own supervisor minted, so a record another writer registered never has one to spend. |
| `/rename` | Change a display name. The body's `run_id` must be the authenticated caller, and `session_id` is forwarded to the supervisor's one rename authority, so the supervisor decides whether the caller may rename that target. Answers `{run_id, label, previous_label}`. |
| `/session_name` | Record the harness's own user-defined session name for the authenticated caller's run, or clear it with an empty `name`. The name must satisfy the same 1–64 rune rule as `/rename`; an empty `name` clears. Answers `{run_id, label}` with the resolved label. |
| `/parent/register` | Adopt one child broker's claim on the runs it owns: `child_id`, `endpoint`, `generation` and `runs`, under the broker's link credential rather than a caller's run token. A claim replaces the child's previous run set and retires the runs it stopped naming. Answers `{generation, messaging_protocol}`, plus `run_keys` mapping a run's canonical ID to the run ID this broker addresses it by. |
| `/parent/deregister` | Retire one child broker's claim (`child_id`) and the run records it owned, so a closed branch stops being a routing target. It requires the same link credential and answers `{deregistered, runs}`. |
| `/relay` | Publish an ancestor's ask or tell into the queue of the run that owns it. It authenticates as the target run, refuses a generation other than the broker's pinned one, and records the reply route before exposing the message: an ask records its route in the parent link (a relayed ask owns no second edge — the ancestor's store holds the one the cross-branch ask has), and a tell records its route in this broker's ask store, which is what lets the target's reply climb back to the sender's branch. |
| `/last_message` | Read one run's recorded last agent turn: `{run_id, label, has_record, final_output, stop_reason, final_output_truncated, at}`. It is the one read endpoint whose target need not be the caller — any authenticated run may read any known run — and an unknown target is `target-unavailable`. |

Before the first send on a connection it calls `/sessions` and requires
`messaging_protocol: 1`. A broker that predates the marker, or answers a
messaging request without the result envelope, is reported as
`unsupported-local-protocol` with the guidance to restart the supervisor; the
ask is classified `not-sent` and nothing is retried.

The adapter pins the generation it preflighted onto every request. A rejected
generation follows one rule:

1. Never re-send the ask, and never reclassify an already-transmitted ask as
   `not-sent`.
2. Re-preflight only to learn whether the broker was replaced. A different
   generation proves the pinned instance is gone, so no edge from it can exist:
   the outcome is `lost`. An unchanged generation leaves the ask
   `possibly-pending`.
3. The refreshed generation is what the *next* ask pins.

## Running a hub

A hub is a supervisor whose broker binds a fixed, reachable address instead of
the default ephemeral loopback port:

```sh
avenor stable --control-socket /run/avenor/hub.sock --broker-addr 192.0.2.10:8787 &
```

`--broker-addr` names one concrete interface address and port. A wildcard bind
(`0.0.0.0`, `::`) is refused: the bound address is what children and parent
branches are told to reach, and a wildcard names no reachable destination. On
the hub, set `AVENOR_PARENT_BROKER_TOKEN` to the link credential; its parent
routes are served only while that token is configured. A shell that exports
`AVENOR_PARENT_BROKER_URL` for its branches must leave that variable unset when
it starts the hub, or the hub tries to join the URL too.

Once the supervisor owns its control socket it publishes the broker's actual
URL, ephemeral port included, beside it:

```sh
cat /run/avenor/hub.sock.broker.url
# http://192.0.2.10:8787
```

The record is one line, mode `0600`. A supervisor that loses the control socket
to a live competitor publishes nothing, so it cannot overwrite or remove that
supervisor's record, and a supervisor removes its own record on shutdown or
when a later startup step fails. An abrupt kill can leave a stale record;
its presence is not proof of a live broker. Check the control socket or the
broker's `/health` endpoint before using it.

### Publishing a different address

When a TLS terminator or NAT publishes the broker under another host, port or
scheme, name that address with `--broker-advertise-url` (or
`AVENOR_BROKER_ADVERTISE_URL`; a non-empty flag wins):

```sh
avenor stable --control-socket /run/avenor/hub.sock \
  --broker-addr 127.0.0.1:8787 \
  --broker-advertise-url https://hub.example.com &
cat /run/avenor/hub.sock.broker.url
# https://hub.example.com
```

That URL replaces the bound address in two places, and only those two: the
`<socket>.broker.url` record and the endpoint the broker registers with its
parent, which is where a parent posts its relays. Children and in-process
transports on this host keep the bound address, so a local child never has to
round-trip through the terminator. A branch that a hub must reach through a
terminator publishes its address the same way.

The value must be an absolute `http://` or `https://` root URL naming a host,
with no credentials, query or fragment; one trailing slash is accepted. A path
prefix or query would break every broker route, so a value carrying one is
refused. An `https` URL is dialed with the host's system trust store, so the
terminator's certificate must be one the dialing peer trusts. A value that
fails validation stops startup instead of falling back to the bound address,
because the operator replaced that address on purpose. A value also requires
the concrete `--broker-addr` bind from the example: over the default ephemeral
bind the published URL would hide the actually bound port, so that pair stops
startup too.

The broker is cleartext HTTP with per-run bearer credentials. A trusted
network therefore means every client that can reach the broker port is
trusted: the credentials are not an admission gate for arbitrary network
clients. `POST /register` mints a token for any run id it has not seen without
asking for a parent token, so a client that reaches the port can register
itself and then use its own credential. Its answer is that credential and the
run id, never a launch attach grant: the broker mints one in process, for the
launch it is about to start, because a register call would rotate the grant
already sitting in a running child's environment.

A registered record is also not adoptable. Each record the broker creates
carries the identity of the supervisor that minted it — the control socket path
plus the host, which is the same value a restart of that supervisor comes back
under — and `EnsureRun`, the provisioning seam, adopts only a record carrying
its own identity. A record `POST /register` created carries none, because its
author chose the credential: adopting it would hand that author the identity of
a run the supervisor is about to provision, since the token would travel to the
child's launch and the plugin would then trade an attach grant for it. So a
process that squats a run id before the supervisor provisions that child owns
nothing it can use: `EnsureRun` refuses the record (`ErrRunNotOwned`),
`MintAttach` mints no grant for it, and the launch proceeds with no broker
identity at all rather than with someone else's, reporting the refusal on the
supervisor's stderr. A resume or a relaunch of a run the supervisor itself
minted is unaffected. There is no TLS listener, so
a hub
exposed beyond that network needs a channel that supplies both confidentiality
and admission control; `--broker-advertise-url` names that channel's public
address and changes nothing about what the broker itself accepts.

## Linking a branch

A child branch joins a parent's broker with one shared credential. Mint it
first:

```
avenor parent-token
```

On the parent supervisor, set `AVENOR_PARENT_BROKER_TOKEN` to that value. Its parent
routes are served only when a token is configured; an empty token leaves them
unserved. On each child branch supervisor, name the parent broker either with
`--broker-parent-url` or with `AVENOR_PARENT_BROKER_URL`; an explicit flag wins
over the env var, so an interactive supervisor autostart joins the hub the
environment names without command-line changes. Set the same token in
`AVENOR_PARENT_BROKER_TOKEN`.
The token is a bearer credential for the parent's parent routes, so it travels
by environment variable rather than a command-line flag, whose contents are
world-readable via `/proc/<pid>/cmdline`. `--broker-parent-token` is accepted
as an explicit override when the environment cannot carry the value, but the
env var is preferred. Starting a supervisor with an effective parent URL but no
token (neither the env var nor the flag) exits with an error.

A branch's broker must also be reachable *from* the hub, because the hub relays
an ask into the branch's own endpoint. On one host the default ephemeral
loopback bind is enough. Across hosts, manually launch the hub and each branch
with `--broker-addr` on a concrete interface, plus `--broker-advertise-url`
where a terminator or NAT puts the branch behind another address. Branches must
reach the hub, and the hub must reach each branch. Autostart cannot configure
these addresses and keeps the ephemeral loopback bind; a nested supervisor does
not inherit the advertised address. The parent link is not a tunnel: both
directions of the deployment must be addressable.

The hub can discover and address registered branches' runs. Descendant callers
cannot list sibling branches' runs; joining a hub does not grant sibling
visibility.

The child registers the runs it owns at startup and retracts the claim on
shutdown. The parent issues each descendant run its own credential at
registration, and relays authenticate per run: the link token only bootstraps
the relationship.

## Host v1 contract

`internal/messaging/hosthttp` speaks a versioned host protocol:

| Path | Purpose |
|---|---|
| `/v1/preflight` | Protocol version and expected epoch. |
| `/v1/send` | Admit one message. |
| `/v1/wait` | Wait for a reply. |
| `/v1/reply` | Answer an ask. |
| `/v1/withdraw` | Withdraw an ask. |
| `/v1/receive` | Drain inbound messages. |
| `/v1/peers` | List visible peers. |

The host client takes an endpoint descriptor (`BaseURL`, `CapabilityID`,
`CapabilitySecret`, `ExpectedEpoch`, `Timeout`); it never registers, spawns,
prompts, or looks up ambient credentials. A cleartext endpoint must be loopback,
so real capability material never travels to an unbound address. A response
without a trustable envelope is `possibly-pending`, never `not-sent`.

The v1 host contract in this repository is exercised against the test fixture
only. It is not a deployable authenticated host protocol.

## Linking a branch

A branch broker registers with its parent under a durable identity (`child_id`,
derived from its control socket path and host) and re-claims its runs on every
restart. The parent's `/parent/deregister` route retires one child's claims and
the run records it owned. The body is the registration view the branch holds of
what its parent last accepted — the `child_id`, the `endpoint` and `generation`
it registered, and the runs it claimed — and the retirement applies only while
that registration still matches the view: a same-identity replacement that
re-registered while the deregistration was in flight keeps its claims and
records, and the route answers `409 generation-mismatch`. An unknown
`child_id` is `404 target-unavailable`.

There is no forced deregistration. An earlier draft let an operator post
`{"child_id": "<old child>", "force": true}` to skip the generation check and
retire every claim a dead `child_id` held; that path is gone, and the route
accepts only the registration view, so it never retires against a stale one.
The gap it used to cover — a branch whose durable identity itself changed
across a restart, whose old claims belong to a `child_id` whose generation no
view can pin — is the register-replacement window, deliberately deferred;
until it exists, such orphans are left standing.
