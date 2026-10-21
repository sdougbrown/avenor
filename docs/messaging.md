# Messaging Transport

Avenor's broker asks — one run sending another a question and waiting for the
reply — run on one shared transport contract. The local broker, the host client
and the in-process implementation all report the same outcomes, the same typed
errors and the same message-ID rules, so an ask behaves the same whichever
transport carries it.

## Packages

| Package | Role |
|---|---|
| `internal/messaging` | Leaf contract: request/result types, the `Transport` and `AskRoute` interfaces, the outcome vocabulary, the v1 wire error classes, the message-ID minter. Depends on the standard library only. |
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

A spawned `pi` child asks through its inherited broker credentials. That path
sends `to_run_id` unchanged: unlike the host's `avenor_message` ask, it does not resolve
a public spawn ID or a label first. A child asking its supervisor therefore has
to use `supervisor`. Asking `rt_N` addresses that child, not the supervisor. A
child asking a sibling uses the sibling's `rt_N`.

A direct child — one the supervisor runs through `runChild` — keeps its broker
run registered for the supervisor's lifetime: a spawn that ends on its own
removes nothing. An ask addressed to that child after it has ended is therefore
still admitted and queued against its run, where nothing reads it, so the asker
waits out its own timeout instead of being told the target is gone. Other
children do remove their runs. A loop or team teardown deletes its children's
runs, and the claudechannel provider deletes its own run when its session ends.
For those runs `target-unavailable` can mean the run ended; for a run nothing
removed, it means the run was never registered on that broker.

The OpenCode plugin is the surface that supplies `parent_run_id` on
`avenor_spawn`, a fresh UUID per plugin instance. The control `spawn` method
accepts the field directly. The supervisor registers that parent run and
prepends its broker run ID to the child's prompt. The `pi` extension does not
set it, so a child learns its parent's broker run ID only when the spawner puts
it in the child's prompt.

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
nil. The ask client's cleanup withdrawal runs on its own five-second budget,
separate from the wait it retracts.

Limits: a wait is capped at ten minutes, a message ID's encoded acceptance
deadline may not exceed ten minutes beyond the receiver's clock, a transport
cleanup budget is five seconds, and a receive that names no limit drains 32
messages.

## Outcomes

`Outcome` values travel in the transport responses and in a failed ask's
`data.outcome`, so the strings never change meaning.

| Outcome | Meaning |
|---|---|
| `sent` | The receiver admitted the ask; an edge exists. |
| `replied` | A reply was delivered to the waiting sender. |
| `timed-out` | The bounded wait expired before a reply. |
| `withdrawn` | The sender completed a withdrawal. |
| `edge-absent` | No edge or waiter matched the request. |
| `auth-rejected` | The supplied credentials were refused. |
| `not-sent` | The ask provably never reached admittance. |
| `possibly-pending` | Transmission could not be confirmed. Retain the message ID and never re-send. |
| `lost` | The ask is provably gone and cannot appear later. |

A failed ask reaches tool surfaces as a JSON-RPC error whose `data` carries
`message_id`, `pending`, and — when known — `outcome`. `pending` is true exactly
when the outcome is `possibly-pending`: a wait that failed but whose cleanup
withdrawal confirmed a terminal fence is no longer pending, and
`message_withdraw` (or `avenor_message action="withdraw"`) accepts the retained ID.

Two rules follow from the table and are enforced everywhere:

- **No replay.** An ask that may have been transmitted is never re-sent, and a
  rejection never reclassifies it as `not-sent`. Only a send that provably never
  reached the receiver is `not-sent`.
- **A withdrawal fences first.** Withdrawal installs a terminal fence before it
  reports absence, so a send that is still in flight cannot land afterwards. A
  missing edge or a bare HTTP 404 is never proof of terminal absence.

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
| `/sessions` | Authenticated preflight: the broker's generation and its `messaging_protocol` marker. |
| `/send` | Admit one message or ask. |
| `/wait_reply` | Wait for the ask's terminal state. |
| `/cancel_message` | Withdraw an ask. |
| `/poll-control` | Drain queued control and agent messages. |

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
