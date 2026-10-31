# MCP server

Your MCP client needs to spawn Avenor runs and monitor them. `avenor mcp` is the canonical MCP server that gives your client these tools.

It's built into the Go binary—no Node, Bun, npm, or separate package needed.

```bash
avenor mcp
```

## Transports

### stdio

stdio is the default transport and is the best choice for most MCP clients.

```bash
avenor mcp
```

Example MCP host configuration:

```json
{
  "mcpServers": {
    "avenor": {
      "command": "avenor",
      "args": ["mcp"]
    }
  }
}
```

If `avenor` is not on the host's `PATH`, use the absolute path:

```json
{
  "mcpServers": {
    "avenor": {
      "command": "/path/to/avenor",
      "args": ["mcp"]
    }
  }
}
```

### HTTP

HTTP transport uses MCP Streamable HTTP and is intended for local, long-lived clients.

```bash
MCP_AUTH_TOKEN="my-secret" avenor mcp --transport http
```

By default it binds to `127.0.0.1:3748`. HTTP requires a bearer token, supplied with either `MCP_AUTH_TOKEN` or `--auth-token`:

```bash
avenor mcp --transport http --addr 127.0.0.1:3748 --auth-token "my-secret"
```

Example MCP host configuration:

```json
{
  "mcpServers": {
    "avenor": {
      "url": "http://127.0.0.1:3748",
      "headers": {
        "Authorization": "Bearer my-secret"
      }
    }
  }
}
```

The HTTP server rejects non-loopback hosts and non-loopback browser origins. Keep it on loopback unless you are deliberately putting another authenticated local proxy in front of it.

## Typical workflow

When you (an LLM) are using this MCP server to automate work, a run moves
through a set of lifecycle states. Your supervision code must distinguish them
because each requires a different action.

### Run lifecycle states

| Status | Means | Next action |
|---|---|---|
| `running` | Agent is active; `pending_permission: true` may still interrupt it. | Answer permission if pending; otherwise keep waiting. |
| `done` (has output) | Agent finished and produced a result. Final output and file changes exist. | Report the outcome. |
| `done` (no output) | Agent finished without writing anything. It likely asked a clarifying question. | Inspect, then call `message` with `action: "resume"` and your answer. |
| `failed` | Agent hit an error mid-run. | Report the failure. |
| `timeout` | Run exceeded its timeout. | Report the timeout. |
| `killed` | Run was forcefully terminated. | Report the kill. |
| `waiting` (`pending_permission`) | Agent hit a tool approval gate. It is blocked mid-task. | Answer via `answer_permission`, then resume waiting. |

### Supervision approaches

**Option A: Blocking** — let `result` handle the loop:

```
spawn(agent="jockey", repo_dir="/path/to/repo", prompt="fix the tests")
→ { "run_id": "...", "label": "...", "supervisor_id": "..." }

result(run_id="...")
→ { "status": "done", "ready": true, "output": "..." }

events(run_id="...")
→ { "events": [ { "type": "...", ... }, ... ] }

shutdown()
→ { "ok": true, "cleaned_up": [...] }
```

1. Call `spawn` with an agent name and repository path.
2. Call `result` to wait. It returns on terminal completion or a
   current pending permission. Answer the permission with
   `answer_permission`, then call `result` again.
3. Call `events` only when you need raw recent history.
4. Call `shutdown` when the session is done.

**Option B: Targeted waits** — use `status` when you need lifecycle
control without implementing a caller-side poll loop:

```
spawn(agent="jockey", repo_dir="/path/to/repo", prompt="fix the tests")
→ { "run_id": "...", "label": "...", "supervisor_id": "..." }

status(run_id="...", wait_for="turn_complete", timeout="5m", view="lifecycle")
→ { "status": "done", "phase": "done" }
```

Each wait requires one `run_id`. The server polls internally at a fixed cadence.
A caller does not provide a poll interval. A wait returns when its condition is
met, when a current permission request appears, or when its timeout expires.

### What not to watch for

- **File changes alone.** An agent that ends its turn without writing anything
  has still ended its turn. Do not treat an empty working tree as "still
  running." Check `status` instead.
- **`session.end` alone.** A permission-blocked run never reaches
  `session.end`. Check `pending_permission` separately.

### Continue a run

Call `message` with `action: "resume"` when a `done` run needs more direction.
It spawns a new session continuing from the prior one. Treat the resumed run as
a new run through the same lifecycle. The existing `auto_approve` policy is
inherited, so approved runs remain unattended across continuation turns.

A `failed` or permission-`blocked` run whose backend session persisted can also
be resumed: `resume` continues the recorded session instead of rebuilding
context from scratch. `timeout` and `killed` runs are not resumable.

### Progress notifications

This issue uses long-poll responses instead of MCP progress notifications. The
Go SDK exposes notification capability, but Claude Code host rendering and
useful progress semantics are not established. Wait correctness is independent
of notifications.

For background callers, a shell-side `avenor await` replaces log-tail monitors;
`result` remains the in-protocol equivalent.

## Supervisor lifecycle

By default, `avenor mcp` starts a private child supervisor:

```bash
avenor stable --control-socket <socket> --idle-timeout 30m
```

The socket defaults to `~/.avenor/sockets/avenor-mcp-<pid>.sock`. You can override it:

```bash
avenor mcp --control-socket ~/.avenor/sockets/avenor-mcp.sock
```

To connect to an existing supervisor instead of starting one:

```bash
avenor stable --control-socket /tmp/avenor-stable.sock
avenor mcp --supervisor-socket /tmp/avenor-stable.sock --no-autostart
```

`--supervisor-socket` disables autostart. `--no-autostart` requires `--supervisor-socket`.

The server reaches a supervisor as its owner, not anonymously. An autostarted child hands back its per-attempt identity token, and the server authenticates on the connection that proved it; an existing supervisor is reached by loading the record published beside its socket, binding the connection, and presenting the capability. Read-only calls that do not reuse that connection stay credential-free. See [Control Protocol → Owner Semantics](control-protocol.md#owner-semantics).

Tool arguments can only target a supervisor socket directly inside the sockets root (`AVENOR_HOME/sockets` when set, otherwise `~/.avenor/sockets`) with the `avenor-mcp-*.sock` naming; other paths are rejected with `invalid supervisorId`. The configured `--supervisor-socket` and the autostarted default are exempt. This mirrors the TypeScript core and prevents clients from pointing the server at an arbitrary local Unix socket.

## Flags

| Flag | Default | Description |
|---|---:|---|
| `--transport` | `stdio` | `stdio` or `http` |
| `--addr` | `127.0.0.1:3748` | HTTP bind address |
| `--auth-token` | `MCP_AUTH_TOKEN` | Bearer token required for HTTP |
| `--control-socket` | per-process socket | Socket path for the autostarted child supervisor |
| `--supervisor-socket` | none | Existing supervisor socket to connect to |
| `--no-autostart` | `false` | Require an existing supervisor |
| `--idle-timeout` | `30m` | Idle timeout for the autostarted child supervisor |
| `--allowed-host` | none | Exact hostname (case-insensitive) allowed to connect over HTTP; repeatable; loopback is always allowed |
| `--max-wait` | `25s` (http) / `0` (stdio) | Approximate polling budget for blocking tools; an explicit `0` disables the clamp |
| `--auth-token-file` | none | Mode-0600 token file; precedence flag > file > `MCP_AUTH_TOKEN`; conflicts with `--auth-token` |

## Remote access over a tailnet

To serve remote MCP clients over a tailnet, run `avenor mcp --transport http` with an auth token and an allowed host, backed by a durable `avenor stable` supervisor. The control socket stays Unix-only; the HTTP endpoint is the only network surface.

### Flags

- `--allowed-host` — exact hostname allowed to connect, case-insensitive. Repeatable. Loopback is always allowed. HTTP-only: stdio has no host check.
- `--max-wait` — approximate polling budget for blocking tools. Defaults to 25s for HTTP and 0 (unclamped) for stdio; an explicit `0` disables the clamp.
- `--auth-token-file` — path to a mode-0600 token file. Precedence: `--auth-token` flag > `--auth-token-file` > `MCP_AUTH_TOKEN`. Conflicts with `--auth-token`.

### Reconnect behavior

The MCP server redials an explicit supervisor socket (`--supervisor-socket`) when the connection dies. Startup order between the two processes does not matter: the MCP server recovers when the supervisor comes back.

### Client-side reliability

- Send `idempotency_key` on `spawn` and on `message` with `action: "resume"` so retries never double-spawn. A retried key with different parameters fails with a tool error whose message contains `idempotency key reused with different parameters (key "...")`; an exhausted idempotency store fails with `idempotency capacity exhausted (capacity N)`. These arrive as failed tool calls (error text), not JSON-RPC error codes — the control-plane codes behind them are documented in `control-protocol.md`.
- Poll `events` with `after_seq` starting at `0`, feeding back `latest_seq` to page forward.
- A clamped wait returns `wait_clamped: true`; treat it as "poll again".
- Key namespaces are per supervisor and shared by every client: choose `idempotency_key` values that are unique to your client (for example prefix them with an identifier) — a colliding key with matching parameters returns the other client's run.

For the full deployment walkthrough — service units, token generation, Tailscale exposure, containerized clients (Docker), and smoke tests — see `templates/remote-mcp/README.md` in the repository.

## Tools

This server registers tool names **without** the `avenor_` prefix: `spawn`, `status`, `result`, `answer_permission`, `message`, `peers`, `events`, `kill`, `shutdown`, and `workflow`. An MCP client namespaces its own servers (Claude Code shows `spawn` as `mcp__avenor__spawn`), so the prefix would only repeat itself. The Pi extension and the OpenCode plugin register the same tools with the `avenor_` prefix (`avenor_spawn`, `avenor_message`, and so on). Every tool here is scoped to this MCP process's supervisor.

### `spawn`

Starts a new run.

**Required:**
- `repo_dir` — working directory for the run

**Optional:**
- `agent` — agent to run (e.g., `"jockey"`, `"butler"`)
- `prompt` — initial prompt text
- `prompt_file` — path to file containing the initial prompt
- `label` — human-friendly label (defaults to `run_id`)
- `timeout` — timeout as seconds or duration (e.g., `"90s"`, `"5m"`, `"1h"`)
- `model` — model to use
- `backend` — runtime backend (e.g., `"opencode-http"`, `"opencode-acp"`, `"codex-app-server"`, `"agy"`)
- `roster_file` / `roster_entry` — optional roster selector pair
- `server_url` — server URL for opencode-http backend
- `launch_wrapper` — optional argv prefix; disabled unless the selected supervisor enables an operator allowlist. Must exactly match an approved argv array. No shell expansion (see [backends.md](backends.md#mcp-surfaces-operator-allowlist))
- `supervisor_id` — supervisor socket to use instead of the default. Must be an `avenor-mcp-*.sock` directly inside the sockets root (`AVENOR_HOME/sockets` when set, otherwise `~/.avenor/sockets`), the same containment rule the TypeScript core enforces; a custom supervisor location is reachable only by configuring it as the MCP server's `--supervisor-socket`

**Returns:**
```json
{
  "run_id": "...",
  "label": "...",
  "supervisor_id": "..."
}
```

The returned `run_id` is generated and unique. Pass it to other tools to query or control this run.

### `status`

Queries the status of runs. Use this for lifecycle and permission checks, not final output retrieval.

**Optional:**
- `run_id` — specific run ID or label to query; omit to list all runs
- `view` — `lifecycle` for a compact response or `full` for compatibility (default: `full`)
- `wait_for` — wait for `terminal`, `phase_change`, `turn_complete`, or `permission`
- `timeout` — maximum wait such as `30s`, `5m`, or `1h`; only valid with `wait_for`
- `supervisor_id` — supervisor socket to query (default: the autostarted supervisor)

A wait requires one `run_id`. Without `wait_for`, `status` performs its
existing one-shot query or list operation. The server polls internally at a fixed
one-second cadence; callers do not provide a poll interval.

**Wait conditions:**
- `terminal` — returns on normalized `done`, `failed`, `timeout`, or `killed` status.
- `phase_change` — returns when normalized `phase` differs from the first snapshot,
    or when a terminal status is reached (even if the phase string did not change).
- `turn_complete` — returns on safely normalized completion of the current turn.
  Parked idle runs with a terminal phase count as complete. Active runs with a
  transient terminal phase do not.
- `permission` — returns when a current permission request is pending.

A pending permission interrupts every wait condition, including `terminal`.
Inspect `pending_permission`, answer the request with `answer_permission`,
and then issue another wait. When a permission request is pending, the status
carries a `permission` record with the request details (`request_id`,
`description`, `options`), so callers can answer without a second query. The
legacy `waiting` status remains supported.

**Returns:** One status object if `run_id` is given, or an object with `runs`
(an array of status objects) and `count` if omitted.
A timed-out wait returns the latest status with `timed_out: true`; it does not
cancel the underlying run. Lifecycle view retains `timed_out` and the
`permission` record but omits `final_output` and usage. Use `result` to
harvest complete output.

### `result`

Waits for one run and returns its complete final output without transcript or raw event details.

**Required:**
- `run_id` — run ID or label to await

**Optional:**
- `wait` — wait for a terminal result (default: `true`)
- `timeout` — maximum time to wait, such as `30s` or `5m`
- `supervisor_id` — supervisor socket to query

A terminal response has `ready: true` and includes the complete `output` when the backend exposed final assistant text. A blocked run returns its `pending_permission` immediately, even when its public status is still `running`, together with the `permission` record holding the request details. Answer the request before waiting again. If an older or unavailable control endpoint prevents lossless retrieval and a presentation fallback is returned, `output_truncated: true` and `output_event_path` make its possible truncation explicit; retry `result` or read the durable event path. If the result tool's own timeout expires, it returns the latest state with `ready: false` and `timed_out: true`; the underlying run keeps going.

### `answer_permission`

Responds to a pending permission request.

**Required:**
- `run_id` — run ID or label
- `option_id` — which option to select

**Optional:**
- `request_id` — specific request ID to answer; if omitted, answers the current pending request
- `supervisor_id` — supervisor socket to use

**Returns:**
```json
{ "ok": true }
```

### `events`

Reads historical events from a run's event log.

**Required:**
- `run_id` — run ID or label

**Optional:**
- `types` — event types to filter by (e.g., `["phase"]`)
- `limit` — maximum events to return (default: `50`)
- `supervisor_id` — supervisor socket to use

**Returns:**
```json
{ "events": [ ... ] }
```

The event log skips malformed lines and returns the last N matching events.

### `message`

One tool for every message between runs, with an explicit `action`. The shared router enforces the per-action argument rules, so a property outside the chosen action's field set — or an unknown property — is a tool error rather than a silent no-op. The published input schema is the flat object schema compiled from the contract's per-action union (`schemas/message.input.flat.json`), which keeps `timeout_ms` an integer and `payload` an object in clients that lose those types from a `oneOf`; the router, not the schema, still decides what each action accepts.

| `action` | Required fields | What it does |
|---|---|---|
| `tell` | `target`, `message` | Deliver a message and return; no ask edge is created. |
| `ask` | `target`, `message`, optional `timeout_ms` | Deliver a message and wait for the reply. |
| `reply` | `reply_to`, `message` | Answer an inbound ask this surface received. |
| `resume` | `target`, `message`, optional `label`, `idempotency_key` | Continue a completed run's session in a new run. |
| `withdraw` | `message_id` | Retract a pending ask. |
| `control_reply` | `target`, optional `payload` | Deliver a control reply (no surface can deliver one; the action answers `unsupported_action`) |

`target` is a run ID, a peer label, or `"self"`. `timeout_ms` is an integer no larger than the shared ask cap (ten minutes). The tool takes no `supervisor_id`: it is bound to the supervisor this server knows the addressed run on.

`resume` is the follow-up: it spawns a new runtime that continues the prior session instead of prompting the old one. The default label is `<prior-label>-followup`, an `auto_approve` policy is inherited, and the result carries `run_id`, `label`, `sentinel_file` and `on_event` for the new run. A keyed `resume` whose key already produced a run replays that run rather than spawning a second one.

`reply` answers an ask that was delivered to this surface. This server receives no inbound asks, so `reply` always answers `edge-absent` with the text "no ask … is pending on this surface; nothing was sent" and sends nothing. A surface that does receive asks — a Pi child — replies for real.

`control_reply` is retained in the contract for the retired claude-channel backend's reply path, and no surface can deliver one any more. Every surface rejects it with code `unsupported_action` and the static reason `action "control_reply" is retained in the contract but no surface can deliver one`.

Every result is the same snake_case object on every surface: `action`, `outcome` and `text` always, plus `message_id`, `run_id`, `label`, `reply`, `sentinel_file` and `on_event` when the outcome carries them. The text comes from the shared outcome table, not from this server. A router failure is a failed tool call (`isError: true`) whose text is a fixed sentence and whose structured content carries the error `code`.

| `outcome` | Means |
|---|---|
| `sent` | A tell, reply or control reply was delivered. |
| `replied` | The ask's reply arrived; `reply` holds its text. |
| `timed-out` | No reply before `timeout_ms`. The ask is terminal, so a later `withdraw` reports `timed-out` and retracts nothing. |
| `withdrawn` | The ask was retracted. |
| `edge-absent` | No pending ask matched; nothing was sent or withdrawn. |
| `lost` | The message provably cannot arrive. For `reply`, no receipt survived and the reply may or may not have landed. |
| `possibly-pending` | Delivery could not be confirmed. Keep `message_id` and never re-send; withdraw it if no reply arrives. |
| `not-sent` | The message provably never left the surface. |
| `auth-rejected` | The broker refused the caller's credentials. |
| `resumed` | `resume` started a new run. |

### `peers`

Lists connected agent sessions, renames a session's display name, or reads a peer's last agent message.

| `action` | Fields | What it does |
|---|---|---|
| `list` (default) | — | List the visible peers: `run_id`, `label`, `kind`, `status`, `backend`, `model`, `dir`. |
| `rename` | `name`, optional `session_id` | Change a display name. |
| `last` | `target` | Read the target's last agent message with its stop reason. |

A display name is 1–64 runes with no control or formatting characters. Two permission levels decide a rename: the control owner — which this server is — may rename **only the supervisor session**, and a broker-authenticated run may rename **only itself**. So `rename` with no `session_id` renames this server's supervisor session; naming a child run answers `not_permitted`. The rename persists first and then publishes `session.renamed` on the control stream, and the renamed session's outbound sender name follows. A session whose harness supplied its own user-defined name keeps that name: the harness name outranks a rename, so peers see it until the harness clears it, and the rename is kept underneath for when it does.

`last` reads a bounded peek, not a result channel: `text` is the header plus the recorded message, and the structured result is `{last: {run_id, runtime_id, label, final_output, stop_reason, final_output_truncated}}`. A known run with no completed turn is a success whose text says so; a run that finished with no text reports its stop reason and an empty `final_output`. Full text stays with `result`. Any caller may read any target it can list.

### `kill`

Terminates a single run without shutting down the supervisor. Other runs and
the supervisor itself keep running. The killed run's status becomes `killed`
(exit code 130, stop reason `cancelled`), and its sentinel records `KILLED`.

**Required:**
- `run_id` — run ID or label to terminate

**Optional:**
- `supervisor_id` — supervisor socket to use

**Returns:**
```json
{
  "killed": true,
  "run_id": "...",
  "runtime_id": "rt_1"
}
```

### `shutdown`

Shuts down the supervisor and cleans up MCP-owned files.

**Optional:**
- `supervisor_id` — supervisor socket to shut down (default: the autostarted supervisor)
- `force` — request kill instead of graceful shutdown (default: `false`)

**Returns:**
```json
{
  "ok": true,
  "cleaned_up": [
    "path/to/sentinel/file",
    "path/to/event/log"
  ]
}
```

### Workflow tool

The `workflow` tool drives a [durable workflow](workflow.md) against a supervisor's control socket. It requires the supervisor to have workflow support (a `--workflow-root` manager); otherwise the call fails with a method-not-found error. One `action` selects the verb:

| Action | Purpose |
|--------|---------|
| `status` | Lightweight status for a workflow instance. |
| `wait` | Wait for a workflow to reach a terminal state or until timeout. |
| `inspect` | Full instance detail for a workflow. |
| `events` | Read log events from a workflow instance's event log. |
| `ready` | Read-only: report claimable auto-dispatch candidates for a controller; grants no lease. |
| `controller_status` | Read-only: with `controller_id`, the full status for that controller; without it, a summary list of all controllers. |
| `gate` | Record a gate decision on a parked `awaiting_gate` activation. |
| `complete` | Atomically complete a machine/external handoff activation. |
| `cancel` | Cancel a whole workflow instance (terminal `canceled`). |

Every action takes an optional `supervisor_id` (supervisor socket path). `status`, `inspect`, and `events` take `workflow_id` (and `after_seq`/`limit` for events). `wait` takes `workflow_id` and an optional `timeout` (e.g. `30s`, `5m`). `ready` takes `controller_id` and an optional `limit`. `controller_status` takes an optional `controller_id`. `complete` takes `workflow_id`, `node_id`, `activation_id`, `attempt_id`, `lease_id`, `owner_token`, `outcome`, and optional `outputs`/`artifacts`. `gate` takes `workflow_id`, `node_id`, `gate_id`, `activation_id`, `operation` (`satisfy`, `reject`, `waive`, or `external_result`), and the operation's fields. `cancel` takes `workflow_id`, the required `reason`, and `actor` — who is cancelling. The manager rejects a missing actor before it looks at the reason, so this server supplies the caller's value or defaults it to `mcp`; the Pi, OpenCode and `packages/mcp` tools require `actor` explicitly.

`workflow` is a host-surface tool. The Pi extension (`avenor_workflow`), the OpenCode plugin, the TypeScript MCP server and this server register it; the Go child tool servers (`channeltools`) register no workflow tools at all. A Pi child still sees `avenor_workflow` registered — the extension publishes the whole host tool set in both modes — but it holds no control connection, so a cancel from a child fails.

### Workflow controllers

`workflow` with `action: "controller_status"` is read-only: pass `controller_id` to get that controller's full status, or omit it to list all controllers with summary state. `action: "ready"` reports the claimable auto-dispatch candidates for one controller without granting a lease. Creating, enabling, and disabling a workflow controller are CLI-only, via `avenor workflow controller create|enable|disable`. A controller is registered with `avenor workflow controller create` (`controller_id` and a positive `max_inflight`) and starts **disabled**; `avenor workflow controller enable` turns it on so it dispatches and polls, and `avenor workflow controller disable` (with a `reason`) stops future dispatch and polling but never cancels running attempts. The underlying control methods are documented in [control-protocol.md](control-protocol.md#workflow-methods).

## Registry scope: important limitation

The Go MCP server keeps an in-memory run registry scoped to the MCP process. This registry maps `run_id` (generated by this MCP server) to stable runtime IDs, sentinel files, event logs, and other metadata.

**This means:**

- `events` requires a run spawned by this MCP process (must be in the registry).
- `message` with `action: "resume"` requires the prior run to be in this MCP process's registry.
- `message` and `peers` are answered by the platform that serves the supervisor they are bound to. A run on a non-default supervisor is addressable only after this server has registered a run there, so after an MCP restart the way to reach one is the status-first flow: call `status` with that `supervisor_id` first, then address the run with `message` or `peers`.
- If you spawn a run with one MCP server and then try to query it with another MCP server, it will fail.

Durable cross-process recovery is intentionally out of scope. If you need to connect to an existing supervisor's runs from a different MCP process, use `supervisor_id` to point at that supervisor's control socket and query by stable runtime ID—but you won't be able to read events or resume those runs.

`inspect` is not registered on this server. The bounded transcript snapshot is a TypeScript-surface tool (`avenor_inspect` on Pi and OpenCode); its Go port is tracked separately.
