# Control Protocol

The control protocol lets you interact with an Avenor process from outside — query its state, cancel runs, send follow-up prompts, and answer permission requests. It lives over a Unix domain socket (or an HTTP adapter for rapid debugging) and speaks JSON-RPC 2.0.

## When You Need It

Fire-and-wait mode is simple: start Avenor with a prompt, point to an event log and a sentinel file, and it runs to completion. You check in later. That works fine for simple tasks.

The control protocol kicks in when you need real-time visibility or external decision-making. Typical cases: a daemon that spawns many child LLM runs and needs to cancel them without killing the parent, a human operator who wants to interrupt a stuck run and inject a new prompt, or a multi-agent orchestrator where one run's completion triggers another. If you just need to wait for a run to surface attention or finish, use `avenor await` instead of composing `status` + `subscribe` by hand.

It's also additive to fire-and-wait mode. When `--control-socket` is absent, the process behaves exactly as before — no socket, no extra goroutines, hot path unchanged.

## Quick Start

### One-Shot Mode (single prompt, live monitoring)

```sh
# Start avenor with a control socket
avenor \
  --control-socket /tmp/avenor.sock \
  --http-debug :8080 \
  --prompt "List the files in this directory and exit." \
  --on-event /tmp/events.ndjson \
  --sentinel-file /tmp/done.env

# In another terminal, inspect status
avenor control --socket /tmp/avenor.sock status

# Tail live events
avenor control --socket /tmp/avenor.sock tail

# Cancel the run
avenor control --socket /tmp/avenor.sock cancel
```

### Stable Mode (multiplexed supervisor)

```sh
# Start the supervisor
avenor stable --control-socket /tmp/avenor-stable.sock

# Spawn a child runtime
avenor control --socket /tmp/avenor-stable.sock spawn \
  --prompt "Review PR #42" \
  --dir /repo/A \
  --label review-42

# List all runtimes
avenor control --socket /tmp/avenor-stable.sock list

# Cancel a specific runtime
avenor control --socket /tmp/avenor-stable.sock cancel rt_1

# Ask children to shut down and wait for them up to --shutdown-timeout
avenor control --socket /tmp/avenor-stable.sock shutdown graceful
```

## Transport

Line-delimited JSON over Unix domain socket. One JSON object per line.

## Request Format

```json
{"jsonrpc":"2.0","id":1,"method":"status","params":{}}
```

- `id` — string or number. Echoed in the response. Required for request/response matching.
- `method` — one of the methods below.
- `params` — method-specific JSON object. May be empty.

## Response Format

### Success

```json
{"jsonrpc":"2.0","id":1,"result":{"phase":"working","session_id":"ses_1"}}
```

### Error

```json
{"jsonrpc":"2.0","id":1,"error":{"code":-32010,"message":"permission_denied"}}
```

Error codes:

| Code | Meaning |
|---|---|
| -32700 | Parse error |
| -32600 | Invalid request |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32000 | Server error (with message) |
| -32001 | No pending permission (tried to answer a request that doesn't exist) |
| -32010 | Permission denied (not owner) |
| -32011 | Owner authentication rejected (`unauthenticated`) |
| -32012 | Owner authentication unsupported (`unsupported owner authentication`) |
| -32013 | Owner authentication timed out. Clients raise it when the handshake gets no answer; the server never sends it |
| -32014 | Control server stopped (`control server stopped`): refused because the server is shutting down |
| -32020 | Backend prompt unsupported |
| -32030 | Idempotency store capacity exceeded |
| -32031 | Idempotency conflict (reused spawn key or re-registered template id+version with different content) |

## Notifications (Server → Client)

```json
{"jsonrpc":"2.0","method":"event","params":{"event":"agent.status","phase":"working",...}}
```

Events match `--on-event` NDJSON format exactly. Calling `subscribe` with no parameters preserves the original behavior: notifications begin with the next live event.

### Event delivery and ordering

A runtime-scoped subscription can replay recent history before switching to live delivery. Replay and concurrently arriving live events are deduplicated and ordered by the runtime-local `seq` field.

Both the per-runtime history window and each subscriber queue are bounded at 256 events. If a subscriber queue fills, Avenor drops its oldest pending event and emits a `subscriber.lagged` notification with `dropped_count`. Further drops are coalesced until the subscriber catches up. Closing the connection removes its subscriptions.

Replay is an attachment aid, not durable storage. The NDJSON file configured by `on_event` remains the authoritative full record after the in-memory window rotates or a subscriber lags.

## Methods

### One-Shot Mode (no `runtime_id`)

These work when avenor is started with `--control-socket` (no stable supervisor).

#### `status`

```json
{"jsonrpc":"2.0","id":1,"method":"status"}
```

Returns the current state snapshot:

```json
{
  "session_id": "ses_1",
  "run_id": "abc123",
  "run_label": "phase-1",
  "phase": "working",
  "phase_label": "go test ./...",
  "last_event": "tool.call",
  "retry_attempt": 1,
  "max_retries": 3,
  "pending_permission": false,
  "permission": null,
  "started_at": 1700000000000,
  "updated_at": 1700000001000,
  "turn_state": "running"
}
```

- `phase` — high-level state: `""`, `"thinking"`, `"working"`, `"waiting"`, `"done"`, or `"unknown"` when the runtime reported a phase outside that set.
- `phase_label` — human-readable description of the current turn (e.g. a test name).
- `last_event` — the event type of the most recent published event.
- `retry_attempt` / `max_retries` — counts for automatic retry after backend errors.
- `pending_permission` — true when a permission request is waiting for an answer. It is cleared when the runtime ends, so an ended runtime never reports a request no live turn can answer.
- `permission` — the full permission request object (all fields from the `permission.request` event), or null.
- `turn_state` — internal turn lifecycle: `"idle"`, `"starting"`, `"running"`, `"ending"`, `"cancelling"`, `"ended"`.
- Times are Unix milliseconds.

#### `result`

```json
{"jsonrpc":"2.0","id":1,"method":"result"}
```

Returns `{"final_output":"..."}` with the complete terminal assistant reply. This is the lossless retrieval path; `status`, history, and subscriptions keep bounded previews for safe polling and display. In stable mode, scope the same method to a child runtime:

```json
{"jsonrpc":"2.0","id":1,"method":"result","params":{"runtime_id":"rt_1"}}
```

#### `subscribe`

```json
{"jsonrpc":"2.0","id":1,"method":"subscribe", "params": {}}
```

Returns `{"subscribed":true}`. Event notifications then arrive on the same connection. Omitting parameters selects the backward-compatible, unfiltered, live-only subscription.

A subscribe that arrives once the server has begun shutting down is refused with `-32014` (`control server stopped`) and the connection is closed, so no event ever arrives on it. Subscribe before the run ends if you need the terminal `session.end`.

To replay recent events from one runtime before following it live:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "subscribe",
  "params": {
    "runtime_id": "rt_1",
    "replay": true,
    "limit": 50,
    "after_seq": 123
  }
}
```

- `runtime_id` (string) — filter live events to this runtime; required when `replay` is true or `after_seq` is non-zero.
- `replay` (bool) — replay buffered history before live delivery. Defaults to false.
- `limit` (int) — replay at most this many events. Defaults to 64 and is capped at 256.
- `after_seq` (int64) — replay and deliver only events whose runtime-local sequence is greater than this value.

#### `history`

```json
{"jsonrpc":"2.0","id":1,"method":"history","params":{"runtime_id":"rt_1","limit":64}}
```

Retrieves a bounded in-memory window of recent events for a specific runtime. It does not read the durable NDJSON file.

Params:
- `runtime_id` (string) — required.
- `limit` (int) — max events to return. Defaults to 64. Max limit is 256.
- `after_seq` (int64) — optional. Only return events with a sequence number greater than this.

Response:

```json
{
  "runtime_id": "rt_1",
  "events": [...],
  "latest_seq": 456
}
```

- `events` — array of event objects.
- `latest_seq` — the latest sequence assigned to the runtime, including when older events have rotated out of the history window.

#### `cancel`

```json
{"jsonrpc":"2.0","id":1,"method":"cancel"}
```

Cancels the run (equivalent to SIGINT). Writes `STOP_REASON=cancelled` to the sentinel file. Requires ownership.

#### `prompt`

```json
{"jsonrpc":"2.0","id":1,"method":"prompt","params":{"text":"Continue with the next step."}}
```

Queues a follow-up prompt. If the session is idle, starts immediately; otherwise starts after the current turn ends. Requires ownership.

The key distinction from `interrupt_and_prompt`: `prompt` never preempts an active turn. The queued text waits for the in-flight `provider.Prompt` to return (whether by `end_turn`, cancellation, or error) before dispatching. Use `interrupt_and_prompt` if you need to cut through the current turn immediately.

#### `answer_permission`

Choose one of the options from the pending request:

```json
{"jsonrpc":"2.0","id":1,"method":"answer_permission","params":{"request_id":"req_17","option_id":"allow"}}
```

The option-only shape remains valid. For a write-in option, include `message`:

```json
{"jsonrpc":"2.0","id":2,"method":"answer_permission","params":{"request_id":"req_18","option_id":"other","message":"Use the staging project."}}
```

`message` is optional unless the selected option has `requiresMessage: true`; those options reject an empty message. Messages must be valid UTF-8 and no larger than 64 KiB. This is how an `agy` question carries an answer that is not one of its listed choices.

Answers the currently pending permission request. Requires ownership. Returns error `-32001` if no permission request is pending or the `request_id` doesn't match the pending one.

#### `interrupt_and_prompt`

```json
{"jsonrpc":"2.0","id":1,"method":"interrupt_and_prompt","params":{"text":"Stop and do this instead.","keep_queue":false}}
```

Interrupts the in-flight turn and queues a new prompt to run immediately after. If `keep_queue` is false, any previously queued prompts are discarded. If true, the interrupt prompt becomes the next-to-run item, and older queued prompts run after. Requires ownership.

### Stable Mode (requires `runtime_id` for scoped operations)

These work with `avenor stable` and manage child runtimes.

#### `spawn`

```json
{
  "jsonrpc":"2.0","id":1,"method":"spawn",
  "params":{
    "prompt":"Review PR #42",
    "dir":"/repo/A",
    "label":"review-42",
    "backend":"opencode-http",
    "server_url":"http://127.0.0.1:4096",
    "auto_approve":true
  }
}
```

Creates and starts a new child runtime. Returns:

```json
{
  "runtime_id": "rt_1",
  "session_id": "ses_xyz",
  "on_event": "/tmp/avenor-stable-1000/run_692b628b/rt_1/events.ndjson",
  "sentinel_file": "/tmp/avenor-stable-1000/run_692b628b/rt_1/sentinel.env"
}
```

Params:
- `prompt` (string) or `prompt_file` (string) — required. If both are omitted but `loop_file` is set, uses the loop config instead.
- `dir` (string) — working directory. Defaults to `"."`.
- `agent` (string) — agent name override.
- `label` (string) — human-readable label for this runtime.
- `model` (string) — model name override.
- `server_url` (string) — backend server URL (e.g. OpenCode HTTP endpoint).
- `backend` (string) — backend class. Defaults to `"opencode-acp"`.
- `on_event` (string) — path to write event NDJSON. If omitted, created under `$TMPDIR/avenor-stable-<euid>/run_<supervisor_run_id>/<runtime_id>/`.
- `sentinel_file` (string) — path to write exit status. If omitted, created under `$TMPDIR/avenor-stable-<euid>/run_<supervisor_run_id>/<runtime_id>/`.
- `permission_handler` (string) — how to handle permission requests (e.g. `"file:/path"` to poll files, or omit to use `auto_approve`).
- `auto_approve` (bool) — auto-resolve all permission requests. Overrides file handler if both are set.
- `timeout` (string) — total run timeout as a Go duration (`30s`, `5m`, etc.).
- `max_retries` (int) — max retries after transient backend errors.
- `loop_file` (string) — path to a loop config JSON file (advanced multi-phase mode).
- `session_id` (string) — resume session ID (for resuming a previous session).
- `launch_wrapper` (array of strings) — opaque argv prefix placed in front of the backend process command (e.g. `["nice", "-n", "5"]`). Avenor does not interpret the wrapper; see [backends.md](backends.md#launch-wrapper). Empty or absent launches the backend unwrapped. Resuming with a different wrapper is rejected like a backend/agent/model mismatch.

Requires ownership.

#### `spawn_mcp`

Identical request and response shape to `spawn`. This is the MCP-gated variant: the supervisor enforces its launch wrapper policy on this verb only — a `launch_wrapper` that does not exactly match an allowlist entry is rejected before any process is created (`launch_wrapper: argv is not allowed by this supervisor`), and a supervisor without a policy rejects every wrapped request (`launch_wrapper: disabled for MCP on this supervisor`). Ordinary `spawn` and the CLI are operator-trusted and not allowlist-gated. See [backends.md](backends.md#mcp-surfaces-operator-allowlist).

Requires ownership.

#### `list`

```json
{"jsonrpc":"2.0","id":1,"method":"list"}
```

Returns all active and recently-completed child runtimes with status summaries. No ownership required.

```json
[
  {
    "runtime_id": "rt_1",
    "session_id": "ses_123",
    "run_id": "run_abc123",
    "label": "review-42",
    "agent": "reviewer",
    "model": "provider/model",
    "backend": "opencode-acp",
    "dir": "/repo/A",
    "status": "running",
    "phase": "working",
    "phase_label": "go test ./...",
    "parent_id": "",
    "children": [],
    "latest_seq": 42,
    "usage": {"total_tokens": 1200},
    "exit_code": 0,
    "on_event": "/tmp/avenor-stable-1000/run_692b628b/rt_1/events.ndjson",
    "event_path": "/tmp/avenor-stable-1000/run_692b628b/rt_1/events.ndjson",
    "sentinel_file": "/tmp/avenor-stable-1000/run_692b628b/rt_1/sentinel.env"
  }
]
```

`status` is one of `"idle"`, `"running"`, or `"ended"`. A live runtime reports `running` from spawn until its run ends, including workflow (loop/team) children before their first attempt and between attempts. `idle` means no attempt is live: a direct run settled a turn and is waiting for the next prompt, or is in a pending retry probe. A workflow runtime reads `idle` only briefly, between its run ending and its completion being published. Metadata fields are additive and may be absent when the backend does not expose them. Completed runtimes can include a bounded `final_output` preview; when it was bounded, `final_output_truncated: true` is included. Use `result` with that runtime's `runtime_id` for the complete reply. Blocked runtimes include their pending permission details.

#### `resume`

`resume` continues a prior run in a new one. Params: `{run_id, target, message, label?, idempotency_key?, sentinel_file?, on_event?}`. `target` is the prior child broker runtime id (`rt_N`), or an exact display name; the supervisor session is not a resume target. A host surface resolves its public run id to a runtime id before calling. It is owner-gated.

`resume` refuses a target whose completion sentinel says `TIMEOUT` or `KILLED` with `not_resumable` (the sentinel's status word is the fact); `DONE`, `FAILED` and `BLOCKED` sentinels continue. It carries the prior run's thinking, directory, agent profile and auto-approve, then starts the new runtime with the prior session id. Identity follows the prior run's selector: a run launched from a roster entry carries only `roster_file` and `roster_entry`, so the entry supplies backend, agent and model, and a roster that changed under the run conflicts with the session's authoritative identity instead of launching a new one; a direct run carries its backend, agent and model. The result reports the `sentinel_file` and `on_event` the spawned run actually writes, including when the request left them empty. A keyed call replays the first run: the stored result's `run_id`, `sentinel_file` and `on_event` win over the freshly minted ones.

#### `session_rename`

`session_rename` renames one run's display name. Params: `{session_id, name}`. `name` is 1-64 runes with no control or formatting characters. It is owner-gated and applies the same one-authority rule as `POST /rename`:

- an owner caller may rename only the supervisor session (`session_id` omitted, `"supervisor"`, or the supervisor's current display name);
- a broker-authenticated caller may rename only itself.

Resolution runs before permission: an exact runtime id first, then the exact display name, so a rename changes what the peers tool, `/sessions`, the outbound sender stamp and later resolution calls report. A stale launch label resolves nowhere. The result carries `{run_id, label, previous_label}` and the rename publishes `session.renamed` after it persists.

#### `session_last`

`session_last` reads a run's recorded last agent turn. Params: `{runtime_id}`. It is read-only and ungated, like `result` and `status`, so any connected client may read any known run. The result is `{run_id, label, has_record, final_output, stop_reason, final_output_truncated, at}`; `label` is the target's current display name, so a rename is reflected. A known run with no completed turn answers `has_record: false` with empty text and a real `run_id`/`label`, while an unknown runtime is a typed `unknown_target` failure: `-32000` with `error.data.code: "unknown_target"`. The text is bounded; `final_output_truncated` is the producer's flag, and is also set whenever the store's bound shortens the text.

#### `session_last_set`

`session_last_set` publishes the supervisor session's last turn. Params: `{runtime_id, final_output, stop_reason?, final_output_truncated?}`. It is owner-gated and accepts `runtime_id: "supervisor"` for an owner caller and nothing else. It publishes no event: the last-message store is a read model, not a lifecycle fact.

#### `message_send`, `message_wait`, `message_reply`, `message_withdraw`

These typed messaging methods delegate to the supervisor's in-process transport and require the owner grant. Their params and results are the snake_case DTOs in `internal/control/message_dto.go` (`to_run_id`/`message_id`/`generation`/`body`/`no_reply` for a send; `message_id`/`generation`/`timeout_ms` for a wait). The owner connection always acts as the supervisor session, so the request params carry no sender or from-run field; inbound results keep `sender` and `from_run_id` as the attribution pair. A failure returns `-32000` with structured `error.data` (`code`, `outcome`, `message_id`, `wire_code`) so clients classify it without parsing text. `message_send` forwards `no_reply` for a fire-and-forget `tell`.

`resume`, `session_rename`, `session_last` and `session_last_set` report what an authority decided rather than what a send did, so their `error.data` carries `code` alone: no `outcome` and no `wire_code` are reported for them.

#### `message_receive`, `message_peers`, `message_preflight`

The read-only half of the typed messaging surface. They are ungated, like `status` and `result`, and answer the same snake_case DTOs (`messages`, `peers`, `protocol`/`generation`). `message_receive` is a blocking verb with the same rules as `message_wait`: a negative `timeout_ms` is `-32602`, a value above the ceiling is clamped to it, and the receive runs off the connection's read loop under the shared per-connection slot budget.

#### `shutdown`

```json
{"jsonrpc":"2.0","id":1,"method":"shutdown","params":{"mode":"graceful"}}
```

Shuts down the supervisor. Mode is one of:
- `"graceful"` (default) — cancels all children and waits up to `--shutdown-timeout` (default 10s).
- `"kill"` — also cancels children, but waits without enforcing the timeout (used for orderly drains).

Requires ownership.

#### Runtime-scoped methods (all require `runtime_id`)

These are the same methods as above but applied to a specific child runtime instead of one-shot mode:

```json
{"jsonrpc":"2.0","id":1,"method":"status","params":{"runtime_id":"rt_1"}}
```

- `status {"runtime_id":"rt_1"}` — runtime-specific status (same shape as stable `list` entry).
- `result {"runtime_id":"rt_1"}` — complete terminal reply for one runtime; do not use the unscoped result method in stable mode.
- `cancel {"runtime_id":"rt_1"}` — cancel one runtime. Requires ownership.
- `prompt {"runtime_id":"rt_1","text":"Continue"}` — send a prompt to one runtime. Requires ownership.
- `interrupt_and_prompt {"runtime_id":"rt_1","text":"Stop and do this","keep_queue":false}` — interrupt and re-prompt. Requires ownership.
- `answer_permission {"runtime_id":"rt_1","request_id":"req_1","option_id":"allow","message":"optional write-in"}` — answer a permission for one runtime. `message` is optional unless the selected option requires it. Requires ownership.

### Workflow methods

The `workflow.*` methods drive a [durable workflow](workflow.md). They are dispatched to an optional workflow handler; when the supervisor has no workflow support (no `--workflow-root` manager), they return `-32601` (`method not found`). They do not use the run-scoped ownership model.

The `workflow.controller.create`, `workflow.controller.enable`, `workflow.controller.disable`, `workflow.controller.status`, `workflow.controller.list`, and `workflow.ready` methods route to an optional workflow-controller handler. When the supervisor has no controller support, they return `-32601` (`method not found`). Like the other `workflow.*` methods, they do not use the run-scoped ownership model. `workflow.ready` is advisory and grants no lease.

| Method | Params | Purpose |
|------|------|---------|
| `workflow.create` | template JSON | Register a versioned template. |
| `workflow.instantiate` | `{template_id, template_version, params?, metadata?}` | Create an instance. `params` must satisfy the template's declared `params`. |
| `workflow.status` | `{workflow_id}` | Snapshot summary. |
| `workflow.wait` | `{workflow_id, timeout_ms?}` | Block until terminal or timeout (default 5000ms). |
| `workflow.inspect` | `{workflow_id}` | Full instance detail. |
| `workflow.events` | `{workflow_id, after_seq?, limit?}` | Event log. |
| `workflow.command` | `{workflow_id, command}` | Route an instance command by its `op` discriminator. |
| `workflow.cancel` | `{workflow_id, actor, reason}` | Cancel the whole instance (terminal `canceled`); the supervisor also terminates its in-flight attempt runtimes. |
| `workflow.controller.create` | `{controller_id, max_inflight}` | Register a controller with its in-flight limit. |
| `workflow.controller.enable` | `{controller_id}` | Set the desired state to enabled and start the leader loop. |
| `workflow.controller.disable` | `{controller_id, reason}` | Set the desired state to disabled (releasing any live lease) and stop the leader loop. |
| `workflow.controller.status` | `{controller_id}` | Snapshot of the controller record. |
| `workflow.controller.list` | none | All controller records, sorted by id. |
| `workflow.ready` | `{controller_id, limit?}` | Advisory candidate list for a controller; grants no lease. |

`workflow.command` routes `op` to the instance command: `claim`, `start`, `heartbeat`, `complete`, `gate`, `skip`, or `unblock`. The command payload carries the op's fields. Example — claim a ready node:

```json
{"jsonrpc":"2.0","id":1,"method":"workflow.command",
 "params":{"workflow_id":"wf_...",
   "command":{"op":"claim","node_id":"intake","activation_id":"act_...","actor":"controller"}}}
```

`claim` returns `lease_id`, `owner_token`, and `expiry`. `start` validates the lease, allocates an `attempt_id`, and dispatches the declared action. `heartbeat` renews the lease with the claim's `(lease_id, owner_token)` pair. `complete`, `gate`, `skip`, and `unblock` carry their operation's fields (see [workflow.md](workflow.md#driving-a-workflow)). `workflow.cancel` requires `actor` and `reason` and terminates the whole instance; it does not go through `workflow.command`.

## Owner Semantics

The control server is constructed with an immutable owner capability that the owning process generates before construction. Ownership is fixed at construction, not granted to the first caller. A connection receives authority only by presenting the capability in `authenticate_owner`; the grant ends when the connection closes, and a failed re-authentication clears only that connection's grant.

### Gated methods

Fourteen methods require a connection's owner grant. A call from a connection without one fails with `-32010` (`permission_denied`) before the handler runs:

`cancel`, `prompt`, `interrupt_and_prompt`, `answer_permission`, `spawn`, `spawn_mcp`, `shutdown`, `message_send`, `message_wait`, `message_reply`, `message_withdraw`, `resume`, `session_rename`, `session_last_set`.

### Ungated methods

Twenty-eight method names are unauthenticated and stay that way. None of them consults the owner grant, so any client that can open the socket may call them:

- Observation: `identity`, `result`, `status`, `subscribe`, `history`, `wait_turn`, `list`, `message_receive`, `message_peers`, `message_preflight`, `session_last`.
- Runner plumbing: `send_to_parent`, `tree_budget`, `wait_for_capacity`.
- Workflow: `workflow.create`, `workflow.instantiate`, `workflow.status`, `workflow.inspect`, `workflow.wait`, `workflow.events`, `workflow.cancel`, `workflow.command`, `workflow.controller.create`, `workflow.controller.enable`, `workflow.controller.disable`, `workflow.controller.status`, `workflow.controller.list`, `workflow.ready`.

The `workflow.*` family is scoped by workflow lease tokens where it is scoped at all (see [workflow.md](workflow.md)); those tokens are independent of the owner capability. `identity` returns this launch's per-attempt identity token, which is informational and grants nothing. Ungated access still exposes state: transcripts, tool input and output, permission details and inbound broker messages are readable by any connected client.

### `authenticate_owner`

```json
{"jsonrpc":"2.0","id":1,"method":"authenticate_owner",
 "params":{"version":1,"capability_id":"…","secret":"…"}}
```

Success:

```json
{"jsonrpc":"2.0","id":1,"result":{"version":1,"authenticated":true,"capability_id":"…"}}
```

Rejections carry the code, a message, and `data: {"version":1,"reason":"<short-token>"}`:

| Code | Message | Reason tokens |
|---|---|---|
| `-32011` | `unauthenticated` | `capability-mismatch` |
| `-32012` | `unsupported owner authentication` | `version-<n>` (including `version-0` for missing params), `malformed-params` |
| `-32013` | `owner authentication timed out` | raised by clients only |

Both halves of the capability are compared in constant time and the secret is never echoed. The exchange is a reusable bearer handshake, not replay protection: every connection authenticates on its own, and any process that can read the published record can authenticate.

### `identity`

```json
{"jsonrpc":"2.0","id":1,"method":"identity"}
```

Returns `{"token":"…"}`. The token is the per-attempt startup identity of this launch, made readable only after the server published its record. It proves which process bound the socket to the launcher that spawned it. It is informational: it authorizes nothing and grants no control.

## Owner record

A server that binds a socket publishes its capability beside it, at `<socket>.owner.json`, before it acknowledges readiness or accepts a connection. The record holds `version`, `socket`, `socket_identity` (`dev` and `ino` as decimal strings), `capability_id`, `secret` and `owner_pid`.

Readers validate the file rather than its directory:

- The record must be a regular file with mode `0600`, owned by the reading euid, no larger than 64 KiB, and not a symlink; its immediate parent directory must not be a symlink. Ancestors above the parent are resolved, not validated.
- `socket` must name the path the record was loaded from, `version` must be supported, and unknown fields are rejected.
- The opened descriptor must still be the file that was inspected (device and inode), so a swap between the check and the read fails closed.
- The live socket must be a socket, and its `dev`/`ino` must equal `socket_identity`.

A live socket that does not match the record is `stale-owner-record`, raised before any credential byte is written and never sent on the wire. A missing record is reported separately so a launcher can retry it while its own child starts.

The containing directory's mode and owner are deliberately unchecked. A control socket may live in `TMPDIR` or under `/tmp`, and the credential boundary is the record's own mode plus euid ownership: a foreign uid can neither create a file owned by this euid nor read a `0600` file it does not own. Publication writes a `0600` temp file with `O_EXCL` and renames it into place; a server that binds but cannot publish rolls the whole attempt back and never becomes usable.

`owner_pid` is a diagnostic and a stale-cleanup hint. It never authorizes attach, unlink, replacement or respawn. The one decision it takes part in is an owned start, where the record must also name the child that attempt spawned — and only after the per-attempt identity token has already proved the child.

## Lock file

Every mutation of a socket path — stale-path removal, bind, publication and cleanup — happens under `<socket>.control.lock`. The lock is a persistent file opened `O_CREATE|O_RDWR|O_NOFOLLOW` with mode `0600` and held with `flock(LOCK_EX)`; a permissive inherited mode is tightened to `0600`. It is created or opened, never opened `O_EXCL` and never unlinked, so a restart reopens the same inode and independent starters serialize instead of splitting the lock. It carries no secret, so leaving it behind is not a credential leak.

## Client binding

A client that wants authority loads the record, checks the live socket against it, dials, and binds the connection before it writes the capability. Generic `dial`/`client.Dial` does neither: it reads no record and sends no credential, which is why the read-only callers keep it.

Binding differs by client runtime because Node exposes no Unix socket peer-credential API:

- **Go** asks the kernel for the connected peer's uid (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED` on macOS) and requires it to equal its own euid. It re-checks the live socket's device and inode after the dial, before the capability is written.
- **TypeScript** binds by filesystem. Before dialing, the socket must be a socket owned by its euid and its immediate parent directory must be owned by that euid or root and must not be group- or other-writable unless it is sticky. After connecting, and before any credential byte, the socket path's device and inode must still be the ones the record names.

Both checks fail closed with a typed foreign-peer error: zero credential bytes reach the peer, nothing is unlinked or replaced, and ownership does not transfer. The whole attach — record load, dial, binding and handshake — is bounded to five seconds or the caller's shorter deadline.

An **owned start** takes a different route on both sides: the launcher proves the child with the per-attempt identity token on the connection it authenticates, so it never needs to trust a path that a competing starter could have won.

## Dead-owner recovery

A launcher decides whether to *attempt* a replacement; only `control.Start`, under the control lock, may unlink a path and bind a new socket.

- A credential-free dial that fails with `ECONNREFUSED` (or an absent path) proves that no listener holds the path, so a replacement may be attempted.
- Any other probe outcome — timeout, permission error, or a listener that answers — is uncertain or live and never authorizes a replacement, unlink or shutdown.
- The tombstone `<socket>.dead` written by `avenor stable` on exit only corroborates a past instance. It is never required to replace a dead owner, and its absence is never liveness: a `SIGKILL` or OOM kill leaves no tombstone. `avenor stable` removes a leftover tombstone when it starts.
- A launcher never unlinks the socket, the record, the lock file or the tombstone. A losing starter retires only the child it captured; a failed startup leaves every artifact in place.

Owned starts attribute their child by the per-attempt identity token, never by a launch-relative timestamp. The launcher mints the token, hands it to the child in its environment, and the child's `ControlServer.Start` exposes it through `identity` only after the child's own record is on disk. The launcher authenticates on the connection that answered with its token, after confirming the record's socket identity matches the live socket and its `owner_pid` is the captured child. A token mismatch means a competing starter won the path: the launcher reaps only its own child and attaches to the winner through the winner's record. While the child is alive, a missing or mismatched record is retryable until the attempt deadline; only a rejection of the child's own capability, the deadline, a spawn error or child exit settles the attempt.

## Ownership is per socket

Ownership is per socket, not per user. The socket file is created with mode `0600`, so filesystem permissions enforce process-level isolation, and same-uid processes are trusted: a same-uid impostor is inside the accepted boundary. Nothing beyond the owner capability is checked.

Multiple read-only connections may observe state simultaneously (`subscribe`, `history`, `status`, `list`). Event subscriptions and history can contain transcript text, tool inputs or outputs, and permission details, so treat read access as sensitive. Connections without a grant continue to receive notifications.

The footgun: a client that loses its owner connection must load the record and authenticate again before it can mutate state.

## Permission Resolution Precedence

When a permission request arrives, Avenor tries these sources in order:

1. **Auto-approve** (`--auto-approve` flag) — resolves ordinary permission requests immediately. Questions marked `requires_user_input` bypass generic auto-approval and still need an answer.
2. **Control socket clients** — when at least one client is connected to the control socket, it is expected to answer via `answer_permission`. By default there is no claim deadline; the request remains with the control path until it is answered or every control client disconnects. Set a non-zero `--permission-claim-timeout` only when unattended automation needs a bounded fallback.
3. **File handler** (`--permission-handler file:<path>`) — used when no control socket client is connected, when a socket claim times out, or when no socket claim can be registered. Writes `.req`, polls `.req.response`.
4. **No resolver** — `permission.request` is emitted, backend waits until context cancellation or backend timeout.

`permission.response` events are emitted for all resolution paths (auto-approve, control, file).

The HTTP debug adapter is **observation-only** with respect to permissions: `POST /answer-permission` works only when `--control-socket` is also active *and a client is claiming requests*. Without an active socket client, permission resolution falls through to the file handler regardless of `--http-debug`. This is intentional — the socket is the source of truth for ownership and claim state. HTTP is for debugging, not for real-time control when a socket client is listening.

## HTTP Debug Adapter

When `--http-debug :8080` is passed, the process starts an HTTP debug adapter bound to localhost:

```
GET  /status                    — current snapshot JSON
GET  /status/<runtime_id>       — per-runtime snapshot (stable mode only)
GET  /events                    — SSE stream of all events.
GET  /events?runtime_id=<id>    — SSE stream filtered to one runtime.
GET  /events?token=<token>     — SSE stream with token.
POST /cancel                    — cancel the run (CLI mode); 400 in stable mode
POST /cancel/<runtime_id>       — cancel a single runtime (stable mode only)
POST /answer-permission         — answer a permission request
```

All endpoints except `/events` require an `X-Avenor-Token` header. The token is printed to stderr at startup (only when stderr is an interactive terminal). `/events` additionally accepts a `?token=` query parameter as a fallback for EventSource clients that cannot set custom headers — do not use the query-param form on POST endpoints (tokens leak into logs and shell history).

### Endpoint details

**`GET /status`** — returns the top-level control state snapshot as JSON. Responds `401` if the token is missing or wrong.

**`GET /status/<runtime_id>`** (stable mode only) — returns the snapshot for a single runtime. Same `X-Avenor-Token` auth as `/status`. Returns `404 runtime not found` if the ID is unknown. In CLI mode (no stable adapter wired) this route always returns `404 not found`.

**`GET /events`** — SSE stream of all events. Accepts auth via `X-Avenor-Token` header or `?token=` query parameter. An optional `?runtime_id=<id>` query parameter filters the stream to events whose `runtime_id` field matches; if omitted, the full unfiltered stream is delivered.

**`POST /cancel`** — in CLI mode, fires the global cancel (equivalent to SIGINT). In stable mode this endpoint returns `400 runtime_id required in stable mode; use POST /cancel/<runtime_id>`. Clients that previously relied on `POST /cancel` in stable mode to cancel everything must migrate to cancelling each runtime individually with `POST /cancel/<runtime_id>`.

**`POST /cancel/<runtime_id>`** (stable mode only) — cancels a single runtime. Returns `404 runtime not found` if the ID is unknown. In CLI mode returns `404 not found`.

**`POST /answer-permission`** — answers the currently pending permission request. Only works when a control socket client is actively claiming requests. See Permission Resolution Precedence above.

### Localhost binding

The HTTP adapter binds only to loopback. Bare `:port` is rewritten to `127.0.0.1:port`. When the `--http-debug` host is a hostname (e.g. `localhost`), the bind address is resolved at startup and rejected unless every resolved IP is a loopback address. `localhost` is hard-coded to `127.0.0.1` to avoid `/etc/hosts` ordering flakiness rather than being passed to DNS. The Unix socket remains the source of truth for ownership, lifecycles, and permission state.

## Socket Lifecycle

- Parent directory created with `0700` if needed.
- Fails fast if another listener is active on the path (dials with 250ms timeout).
- Stale socket unlinked only after a failed dial proves no process is listening.
- Socket file chmod'd to `0600`.
- Socket unlinked on clean shutdown.

## Fire-and-Wait Compatibility

A caller that passes only `--on-event` and `--sentinel-file` without `--control-socket` gets exactly today's behavior. No socket is created, no goroutines are started, and the hot path is unchanged.
