# Control Protocol

The control protocol lets you interact with an Avenor process from outside — query its state, cancel runs, send follow-up prompts, and answer permission requests. It lives over a Unix domain socket (or an HTTP adapter for rapid debugging) and speaks JSON-RPC 2.0.

## When You Need It

Fire-and-wait mode is simple: start Avenor with a prompt, point to an event log and a sentinel file, and it runs to completion. You check in later. That works fine for simple tasks.

The control protocol kicks in when you need real-time visibility or external decision-making. Typical cases: a daemon that spawns many child LLM runs and needs to cancel them without killing the parent, a human operator who wants to interrupt a stuck run and inject a new prompt, or a multi-agent orchestrator where one run's completion triggers another.

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
| -32020 | Backend prompt unsupported |

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

- `phase` — high-level state: `"idle"`, `"working"`, `"ended"`.
- `phase_label` — human-readable description of the current turn (e.g. a test name).
- `last_event` — the event type of the most recent published event.
- `retry_attempt` / `max_retries` — counts for automatic retry after backend errors.
- `pending_permission` — true when a permission request is waiting for an answer.
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
  "on_event": "/tmp/avenor-stable/abc123/rt_1/events.ndjson",
  "sentinel_file": "/tmp/avenor-stable/abc123/rt_1/sentinel.env"
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
- `on_event` (string) — path to write event NDJSON. If omitted, created under `$TMPDIR/avenor-stable/<supervisor_run_id>/<runtime_id>/`.
- `sentinel_file` (string) — path to write exit status. If omitted, created under `$TMPDIR/avenor-stable/<supervisor_run_id>/<runtime_id>/`.
- `permission_handler` (string) — how to handle permission requests (e.g. `"file:/path"` to poll files, or omit to use `auto_approve`).
- `auto_approve` (bool) — auto-resolve all permission requests. Overrides file handler if both are set.
- `timeout` (string) — total run timeout as a Go duration (`30s`, `5m`, etc.).
- `max_retries` (int) — max retries after transient backend errors.
- `loop_file` (string) — path to a loop config JSON file (advanced multi-phase mode).
- `session_id` (string) — resume session ID (for resuming a previous session).

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
    "on_event": "/tmp/avenor-stable/abc123/rt_1/events.ndjson",
    "event_path": "/tmp/avenor-stable/abc123/rt_1/events.ndjson",
    "sentinel_file": "/tmp/avenor-stable/abc123/rt_1/sentinel.env"
  }
]
```

`status` is one of `"idle"`, `"running"`, or `"ended"`. Metadata fields are additive and may be absent when the backend does not expose them. Completed runtimes can include a bounded `final_output` preview; when it was bounded, `final_output_truncated: true` is included. Use `result` with that runtime's `runtime_id` for the complete reply. Blocked runtimes include their pending permission details.

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

## Owner Semantics

The first client connection to issue a mutating method (`cancel`, `prompt`, `interrupt_and_prompt`, `answer_permission`, `spawn`, `shutdown`) becomes the owner. Non-owner calls fail with error code `-32010` (`permission_denied`).

Ownership is per socket, not per user. The socket file is created with mode `0600`, so filesystem permissions enforce process-level isolation. There is no additional token authentication on the Unix socket: any process running as the same account that can open it can observe runs and compete to become the mutating owner. Ownership is released when the owner connection closes, and the next client to call a mutating method becomes the new owner.

Multiple read-only connections may observe state simultaneously (`subscribe`, `history`, `status`, `list`). Event subscriptions and history can contain transcript text, tool inputs or outputs, and permission details, so treat read access as sensitive. Non-owner subscribers continue to receive notifications.

The footgun: if you lose the owner connection, no one else can mutate state. Either be the only client, or have a heartbeat mechanism to hand off ownership gracefully before closing.

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
