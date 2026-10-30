# @dougbots/avenor-mcp

> **Note:** Since v0.x, `avenor mcp` is the canonical Go-native MCP server built into the `avenor` binary itself — no Node/Bun required. See [`docs/mcp.md`](../../docs/mcp.md) for the primary MCP setup and tool reference. This Node.js package is an alternative for OpenCode-based setups or when you prefer npm-based tooling.

MCP server that makes [avenor](https://github.com/dougbots/avenor) agent runs available as tools inside any MCP-compatible client (Claude Code, Claude Desktop, Cursor, etc.).

## When you need this

You have `avenor` installed and you want Claude (or another MCP host) to spawn and manage agent runs — kicking off a Codex or Gemini session, checking its status, answering permission prompts, tailing event logs, and resuming completed runs.

This package is the MCP layer. It depends on `@dougbots/avenor-core` for all the actual wiring (supervisor lifecycle, binary discovery, socket protocol). You don't need to know the internals to use it.

## Prerequisites

- **Node 20+** (or Bun — the server runs on Bun but is shipped as ESM)
- **`avenor` binary** on your `PATH`, or set `AVENOR_BIN=/path/to/avenor`

If the binary isn't found at startup, the MCP server will fail to connect.

## Install

```bash
npm install @dougbots/avenor-mcp
```

Or run it on-demand with `npx` (see configuration below).

## Configuration

### stdio transport (default)

Add to your MCP host config. For **Claude Code**, edit `~/.claude/mcp.json`:

```json
{
  "mcpServers": {
    "avenor": {
      "command": "npx",
      "args": ["@dougbots/avenor-mcp"],
      "env": {
        "AVENOR_BIN": "/opt/homebrew/bin/avenor"
      }
    }
  }
}
```

For **Claude Desktop**, use `~/.config/Claude/claude_desktop_config.json` with the same shape.

Skip `AVENOR_BIN` if `avenor` is already on your `PATH`. On a fresh session, the supervisor auto-starts the `avenor` daemon the first time you call a tool.

### SSE transport

When you need the MCP server running as a long-lived HTTP service (multiple clients, remote access via tunneling, etc.), use SSE mode. Set `MCP_TRANSPORT=sse` and provide an auth token:

```bash
MCP_TRANSPORT=sse \
MCP_AUTH_TOKEN="my-secret" \
MCP_PORT=3748 \
npx @dougbots/avenor-mcp
```

Then in your MCP host config:

```json
{
  "mcpServers": {
    "avenor": {
      "url": "http://127.0.0.1:3748/mcp",
      "headers": {
        "Authorization": "Bearer my-secret"
      }
    }
  }
}
```

SSE mode restricts incoming requests to `localhost` / `127.0.0.1` / `[::1]` origins. The auth token is required — the server returns `401` without it.

| Env variable | Default | Notes |
|---|---|---|
| `MCP_TRANSPORT` | (unset → stdio) | Set to `sse` for HTTP |
| `MCP_PORT` | `3748` | Only used in SSE mode |
| `MCP_AUTH_TOKEN` | — | **Required** in SSE mode |
| `AVENOR_BIN` | — | Path to the `avenor` binary (falls back to `PATH`) |

## Tools

This surface registers tool names **without** the `avenor_` prefix — `spawn`, `status`, `result`, `answer_permission`, `message`, `peers`, `events`, `kill`, `shutdown`, and the `workflow_*` set. MCP clients namespace their own servers (Claude Code shows them as `mcp__avenor__spawn`, for example), so the prefix would only repeat itself. The `avenor` binary's own MCP server uses the same names.

### `spawn`

Start a new agent run.

| Parameter | Required | Description |
|---|---|---|
| `agent` | no | Agent to invoke; omission uses the supplied model or runtime defaults |
| `repo_dir` | yes | Working directory for the agent |
| `prompt` | no | Prompt text (mutually exclusive with `prompt_file`) |
| `prompt_file` | no | Path to a prompt file |
| `label` | no | Human-readable label for this run |
| `timeout` | no | Duration string (`5m`, `1h`, `90s`) |
| `model` | no | Model override for the agent |
| `thinking` | no | Run-level reasoning control; unsupported effective backends reject explicit values |
| `backend` | no | Backend override (`opencode-http`, `opencode-acp`, `codex-app-server`, `agy`) |
| `roster_file` | no | Path to a top-level roster map |
| `roster_entry` | no | Key within `roster_file`; selects the complete backend/agent/model identity |
| `server_url` | no | Backend server URL, required by `opencode-http` unless configured by environment |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns `{ run_id, label, supervisor_id }`. The `run_id` is what you pass to the other tools.

#### Direct and roster modes

Use both `roster_file` and `roster_entry` for roster mode. A roster entry requires `backend` plus at least one of `agent` or `model`, and cannot be mixed with direct `agent`, `model`, or `backend` fields. Direct mode permits all three identity fields to be omitted independently, including a backend-only request; omitted values use runtime defaults.

```json
{
  "repo_dir": "/repo",
  "prompt": "Analyze the repository",
  "roster_file": "/repo/roster.json",
  "roster_entry": "planner"
}
```

The roster map is strict and currently supports only `backend`, `agent`, and `model` in each entry. `system` and `thinking` are not roster fields. The `thinking` argument remains run-level and is validated against the effective backend chosen by the direct request or roster entry. A resumed session cannot switch to a different backend.

### `status`

Get the status of one run, or list all runs. Use the compact `lifecycle` view for progress and pending permission checks; `full` remains the compatibility default.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | no | Run ID or label to query. Omit to list all runs. |
| `view` | no | Response detail: `lifecycle` or `full` (default) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns a status object (or array) with `status` being one of `running`, `done`, `failed`, `timeout`, or `killed`. When a run is blocked on a permission request, the response includes a `pending_permission` field with the available options.

### `result`

Wait for a run to finish and return its complete final output without transcript or event details.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label to await |
| `wait` | no | Wait for a terminal result (default `true`) |
| `timeout` | no | Maximum time to wait (for example `30s` or `5m`) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Terminal responses include `ready: true` and `output` when the backend exposed one. A pending permission returns immediately. If the result wait itself expires, the response has `ready: false` and `timed_out: true` without changing the run.

### `answer_permission`

Respond to a permission prompt that paused a run.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label |
| `option_id` | yes | The option to select (from the `pending_permission` shown by `status`) |
| `request_id` | no | Specific permission request ID. Auto-detected from the pending request if omitted. |
| `message` | no | Write-in response text, required only when the selected option declares `requiresMessage: true` |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns `{ ok: true }` on success. Throws if no permission request is pending for the given run.

### `message`

Send a message to another run, withdraw a pending ask, resume a completed run, or deliver a control reply. Cross-variant argument rules are enforced by the shared router, so an argument outside the chosen action's field set is an error rather than a silent no-op.

| `action` | Required fields | What it does |
|---|---|---|
| `tell` | `target`, `message` | Deliver a message without creating an ask edge |
| `ask` | `target`, `message`, optional `timeout_ms` | Deliver a message and wait for a reply |
| `reply` | `reply_to`, `message` | Answer an inbound ask. This server receives no inbound delivery, so the result is always `edge-absent` and nothing is sent |
| `resume` | `target`, `message`, optional `label`, `idempotency_key` | Resume a completed run from its persisted session |
| `withdraw` | `message_id` | Withdraw a pending ask |
| `control_reply` | `target`, optional `payload` | Deliver a control reply where the provider supports it |

`target` is a run id, a peer label, or `"self"`. `timeout_ms` must be an integer no greater than the shared ask cap. This tool does not take `supervisor_id`; it is bound to the default supervisor. A router tool error is returned as an MCP error result carrying the contract sentence and a `code`.

Every successful result is the shared contract object in snake_case (`action`, `outcome`, `text`, and `message_id`, `run_id`, `label`, `reply`, `sentinel_file`, `on_event` when the outcome carries them), returned as the structured content, with the contract sentence as the text content, exactly like the Go MCP host.

### `peers`

List the visible agent sessions, rename a run, or read a run's last agent message.

| `action` | Fields | What it does |
|---|---|---|
| `list` (default) | — | List the visible peers |
| `rename` | `name`, optional `session_id` | Change a display name, self by default |
| `last` | `target` | Read the target's last agent message with its stop reason |

`last` is read-only. This surface never publishes a turn of its own, so `last` of the server's own session returns the shared empty result. This tool does not take `supervisor_id`; it is bound to the default supervisor. The structured result is `{peers}`, `{renamed}` or `{last}` in snake_case, the same object the Go MCP host returns.

### `events`

Fetch the event log for a run. Useful for inspecting what an agent did.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label |
| `types` | no | Filter by event types (array of strings) |
| `limit` | no | Max events to return (default `50`, max `1000`) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns an array of event objects (`{ type, ts, ... }`) in chronological order.

### `kill`

Terminate a run without shutting down the supervisor. Other runs and the supervisor itself keep running.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label of the run to terminate |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

### `shutdown`

Stop the avenor supervisor and clean up sentinel files.

| Parameter | Required | Description |
|---|---|---|
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |
| `force` | no | Force shutdown instead of graceful (`false` by default) |

Returns `{ ok: true, cleaned_up: [...] }`. Use this when you're done with avenor in your session or when the supervisor crashes and needs to be restarted.

### `workflow`

Drive a durable workflow instance. One `action` selects the verb; every action takes an optional `supervisor_id` for multi-supervisor setups. Read-only actions need no permission; `gate`, `complete` and `cancel` keep their authority checks.

| Action | Arguments | Purpose |
|---|---|---|
| `status` | `workflow_id` | Lightweight instance status |
| `inspect` | `workflow_id` | Full instance detail |
| `events` | `workflow_id`, `after_seq`, `limit` | The instance's log events |
| `wait` | `workflow_id`, `timeout` | Wait for a terminal state or a timeout |
| `ready` | `controller_id`, `limit` | Claimable auto-dispatch candidates; grants no lease |
| `controller_status` | `controller_id` | One controller's status, or a summary list without it |
| `gate` | `workflow_id`, `node_id`, `gate_id`, `activation_id`, `operation`, and the operation's fields | Record a gate decision on a parked `awaiting_gate` activation |
| `complete` | `workflow_id`, `node_id`, `activation_id`, `attempt_id`, `lease_id`, `owner_token`, `outcome`, `outputs`, `artifacts` | Complete a machine/external handoff activation |
| `cancel` | `workflow_id`, `actor`, `reason` | Cancel the whole instance (terminal `canceled`) |

The underlying control methods are in [`docs/control-protocol.md`](../../docs/control-protocol.md).

## Typical workflow

1. **Spawn a run:** `spawn(agent: "codex", repo_dir: "/path/to/project", prompt: "audit the API routes for auth gaps")`
2. **Check lifecycle when needed:** `status(run_id: "<id>", view: "lifecycle")` for progress or pending permissions.
3. **If permission requested:** run `answer_permission` with the offered option.
4. **Retrieve the result:** `result(run_id: "<id>")` waits and returns the complete final output.
5. **Inspect history when needed:** `events(run_id: "<id>")` returns raw recent events.
6. **Iterate:** `message(action: "resume", target: "<id>", message: "now write tests for that")`
7. **Clean up:** `shutdown()` when you're finished.

## Package structure

```
@dougbots/avenor-mcp
├── @dougbots/avenor-core  (workspace dep — supervisor, tool implementations)
├── @modelcontextprotocol/sdk  (MCP server + transports)
└── hono  (HTTP framework for SSE transport)
```

The server is a single file (`src/mcp.ts`), compiled to `dist/index.js` by tsdown. The `avenor-mcp` bin entry runs it directly.
