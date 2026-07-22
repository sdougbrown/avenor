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

When you (an LLM) are using this MCP server to automate work:

```
avenor_spawn(agent="jockey", repo_dir="/path/to/repo", prompt="fix the tests")
→ { "run_id": "...", "label": "...", "supervisor_id": "..." }

avenor_status(run_id="...", view="lifecycle")
→ { "status": "running", ... }

avenor_result(run_id="...")
→ { "status": "done", "ready": true, "output": "..." }

avenor_events(run_id="...")
→ { "events": [ { "type": "...", ... }, ... ] }

avenor_shutdown()
→ { "ok": true, "cleaned_up": [...] }
```

1. Call `avenor_spawn` with an agent name and repository path.
2. Use `avenor_status` with `view="lifecycle"` when you need progress or pending permission details.
3. If `pending_permission` is present, call `avenor_answer_permission` to respond.
4. Call `avenor_result` to wait for and retrieve the complete final output.
5. Call `avenor_events` only when you need raw recent history.
6. Optionally call `avenor_follow_up` to continue from a completed run.
7. Call `avenor_shutdown` when the MCP session is done.

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

## Tools

All tools use the `avenor_` prefix and are scoped to this MCP process's supervisor.

### `avenor_spawn`

Starts a new run.

**Required:**
- `agent` — agent to run (e.g., `"jockey"`, `"butler"`)
- `repo_dir` — working directory for the run

**Optional:**
- `prompt` — initial prompt text
- `prompt_file` — path to file containing the initial prompt
- `label` — human-friendly label (defaults to `run_id`)
- `timeout` — timeout as seconds or duration (e.g., `"90s"`, `"5m"`, `"1h"`)
- `model` — model to use
- `backend` — runtime backend (e.g., `"opencode-http"`, `"opencode-acp"`, `"codex-app-server"`, `"agy"`)
- `server_url` — server URL for opencode-http backend
- `supervisor_id` — supervisor socket to use instead of the default

**Returns:**
```json
{
  "run_id": "...",
  "label": "...",
  "supervisor_id": "..."
}
```

The returned `run_id` is generated and unique. Pass it to other tools to query or control this run.

### `avenor_status`

Queries the status of runs. Use this for lifecycle and permission checks, not final output retrieval.

**Optional:**
- `run_id` — specific run ID or label to query; omit to list all runs
- `view` — `lifecycle` for a compact response or `full` for compatibility (default: `full`)
- `supervisor_id` — supervisor socket to query (default: the autostarted supervisor)

**Returns:** One status object if `run_id` is given, or an array of status objects if omitted.

Status values:
- `running` — still executing
- `done` — completed successfully
- `failed` — ended with an error
- `timeout` — exceeded timeout
- `killed` — forcefully terminated

When blocked on a permission request, the result includes `pending_permission` with `request_id` and options.

### `avenor_result`

Waits for one run and returns its complete final output without transcript or raw event details.

**Required:**
- `run_id` — run ID or label to await

**Optional:**
- `wait` — wait for a terminal result (default: `true`)
- `timeout` — maximum time to wait, such as `30s` or `5m`
- `supervisor_id` — supervisor socket to query

A terminal response has `ready: true` and includes the complete `output` when the backend exposed final assistant text. A blocked run returns its `pending_permission` immediately. If an older or unavailable control endpoint prevents lossless retrieval and a presentation fallback is returned, `output_truncated: true` and `output_event_path` make its possible truncation explicit; retry `avenor_result` or read the durable event path. If the result tool's own timeout expires, it returns the latest state with `ready: false` and `timed_out: true`; the underlying run keeps going.

### `avenor_answer_permission`

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

### `avenor_events`

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

### `avenor_follow_up`

Spawns a new run continuing a completed run's session.

This is not a live prompt into the old runtime; it spawns a new runtime that resumes from the prior session's state.

**Required:**
- `run_id` — completed run ID or label
- `message` — follow-up prompt text

**Optional:**
- `label` — label for the new run (defaults to `<prior-label>-followup`)
- `supervisor_id` — supervisor socket to use

**Returns:**
```json
{
  "run_id": "...",
  "label": "..."
}
```

### `avenor_shutdown`

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

## Registry scope: important limitation

The Go MCP server keeps an in-memory run registry scoped to the MCP process. This registry maps `run_id` (generated by this MCP server) to stable runtime IDs, sentinel files, event logs, and other metadata.

**This means:**

- `avenor_events` requires a run spawned by this MCP process (must be in the registry).
- `avenor_follow_up` requires the prior run to be in this MCP process's registry.
- If you spawn a run with one MCP server and then try to query it with another MCP server, it will fail.

Durable cross-process recovery is intentionally out of scope. If you need to connect to an existing supervisor's runs from a different MCP process, use `supervisor_id` to point at that supervisor's control socket and query by stable runtime ID—but you won't be able to read events or spawn follow-ups for those runs.
