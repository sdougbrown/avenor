# @dougbots/avenor-mcp

> **Note:** Since v0.x, `avenor mcp` is the canonical Go-native MCP server built into the `avenor` binary itself — no Node/Bun required. See [`docs/mcp.md`](../../docs/mcp.md) for the primary MCP setup and tool reference. This Node.js package is an alternative for OpenCode-based setups or when you prefer npm-based tooling.

MCP server that makes [avenor](https://github.com/dougbots/avenor) agent runs available as tools inside any MCP-compatible client (Claude Code, Claude Desktop, Cursor, etc.).

## When you need this

You have `avenor` installed and you want Claude (or another MCP host) to spawn and manage agent runs — kicking off a Codex or Gemini session, checking its status, answering permission prompts, tailing event logs, and resuming completed runs with a follow-up.

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

All seven tools register under the `avenor_` prefix to avoid collisions with other MCP servers.

### `avenor_spawn`

Start a new agent run.

| Parameter | Required | Description |
|---|---|---|
| `agent` | yes | Agent to invoke: `codex`, `claude`, `gemini`, `butler` |
| `repo_dir` | yes | Working directory for the agent |
| `prompt` | no | Prompt text (mutually exclusive with `prompt_file`) |
| `prompt_file` | no | Path to a prompt file |
| `label` | no | Human-readable label for this run |
| `timeout` | no | Duration string (`5m`, `1h`, `90s`) |
| `model` | no | Model override for the agent |
| `backend` | no | Backend override (`opencode-http`, `opencode-acp`, `codex-app-server`) |
| `server_url` | no | Backend server URL, required by `opencode-http` unless configured by environment |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns `{ run_id, label, supervisor_id }`. The `run_id` is what you pass to the other tools.

### `avenor_status`

Get the status of one run, or list all runs. Use the compact `lifecycle` view for progress and pending permission checks; `full` remains the compatibility default.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | no | Run ID or label to query. Omit to list all runs. |
| `view` | no | Response detail: `lifecycle` or `full` (default) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns a status object (or array) with `status` being one of `running`, `done`, `failed`, `timeout`, or `killed`. When a run is blocked on a permission request, the response includes a `pending_permission` field with the available options.

### `avenor_result`

Wait for a run to finish and return its bounded final output without transcript or event details.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label to await |
| `wait` | no | Wait for a terminal result (default `true`) |
| `timeout` | no | Maximum time to wait (for example `30s` or `5m`) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Terminal responses include `ready: true` and `output` when the backend exposed one. A pending permission returns immediately. If the result wait itself expires, the response has `ready: false` and `timed_out: true` without changing the run.

### `avenor_answer_permission`

Respond to a permission prompt that paused a run.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label |
| `option_id` | yes | The option to select (from the `pending_permission` shown by `avenor_status`) |
| `request_id` | no | Specific permission request ID. Auto-detected from the pending request if omitted. |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns `{ ok: true }` on success. Throws if no permission request is pending for the given run.

### `avenor_follow_up`

Resume a completed run with a new message. The agent picks up from the same session, so it retains context from the original run.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label of the *completed* run to resume |
| `message` | yes | Follow-up prompt |
| `label` | no | Label for the follow-up run (defaults to `<original-label>-followup`) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns `{ run_id, label }` for the new follow-up run. Throws if the original run has no persisted session.

### `avenor_events`

Fetch the event log for a run. Useful for inspecting what an agent did.

| Parameter | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label |
| `types` | no | Filter by event types (array of strings) |
| `limit` | no | Max events to return (default `50`, max `1000`) |
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |

Returns an array of event objects (`{ type, ts, ... }`) in chronological order.

### `avenor_shutdown`

Stop the avenor supervisor and clean up sentinel files.

| Parameter | Required | Description |
|---|---|---|
| `supervisor_id` | no | Supervisor ID for multi-supervisor setups |
| `force` | no | Force shutdown instead of graceful (`false` by default) |

Returns `{ ok: true, cleaned_up: [...] }`. Use this when you're done with avenor in your session or when the supervisor crashes and needs to be restarted.

## Typical workflow

1. **Spawn a run:** `avenor_spawn(agent: "codex", repo_dir: "/path/to/project", prompt: "audit the API routes for auth gaps")`
2. **Check lifecycle when needed:** `avenor_status(run_id: "<id>", view: "lifecycle")` for progress or pending permissions.
3. **If permission requested:** run `avenor_answer_permission` with the offered option.
4. **Retrieve the result:** `avenor_result(run_id: "<id>")` waits and returns the bounded final output.
5. **Inspect history when needed:** `avenor_events(run_id: "<id>")` returns raw recent events.
6. **Optional follow-up:** `avenor_follow_up(run_id: "<id>", message: "now write tests for that")`
7. **Clean up:** `avenor_shutdown()` when you're finished.

## Package structure

```
@dougbots/avenor-mcp
├── @dougbots/avenor-core  (workspace dep — supervisor, tool implementations)
├── @modelcontextprotocol/sdk  (MCP server + transports)
└── hono  (HTTP framework for SSE transport)
```

The server is a single file (`src/mcp.ts`), compiled to `dist/index.js` by tsdown. The `avenor-mcp` bin entry runs it directly.
