# MCP server

`avenor mcp` is the canonical MCP server for Avenor. It is built into the Go
binary, so MCP clients do not need Node, Bun, npm, or the
`@dougbots/avenor-mcp` package.

The Node MCP package still exists for npm-first setups and legacy OpenCode
workflows, but new MCP integrations should prefer:

```bash
avenor mcp
```

## Transports

### stdio

stdio is the default transport and is the best choice for most MCP clients:

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

HTTP uses MCP Streamable HTTP and is intended for local, long-lived clients:

```bash
MCP_AUTH_TOKEN="my-secret" avenor mcp --transport http
```

By default it binds to `127.0.0.1:3748`. HTTP requires a bearer token, supplied
with either `MCP_AUTH_TOKEN` or `--auth-token`:

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

The HTTP server rejects non-loopback hosts and non-loopback browser origins.
Keep it on loopback unless you are deliberately putting another authenticated
local proxy in front of it.

## Supervisor lifecycle

By default, `avenor mcp` starts a private child supervisor:

```bash
avenor stable --control-socket <socket> --idle-timeout 30m
```

The socket defaults to `~/.avenor/sockets/avenor-mcp-<pid>.sock`. You can
override it:

```bash
avenor mcp --control-socket ~/.avenor/sockets/avenor-mcp.sock
```

To connect to an existing supervisor instead of starting one:

```bash
avenor stable --control-socket /tmp/avenor-stable.sock
avenor mcp --supervisor-socket /tmp/avenor-stable.sock --no-autostart
```

`--supervisor-socket` disables autostart. `--no-autostart` requires
`--supervisor-socket`.

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

All tools use the `avenor_` prefix.

| Tool | Required inputs | Optional inputs | Output |
|---|---|---|---|
| `avenor_spawn` | `agent`, `repo_dir` | `prompt`, `prompt_file`, `label`, `timeout`, `model`, `supervisor_id` | `{ "run_id": string, "label": string, "supervisor_id": string }` |
| `avenor_status` | none | `run_id`, `supervisor_id` | one status object, or an array when `run_id` is omitted |
| `avenor_answer_permission` | `run_id`, `option_id` | `request_id`, `supervisor_id` | `{ "ok": true }` |
| `avenor_follow_up` | `run_id`, `message` | `label`, `supervisor_id` | `{ "run_id": string, "label": string }` |
| `avenor_events` | `run_id` | `types`, `limit`, `supervisor_id` | event object array |
| `avenor_shutdown` | none | `supervisor_id`, `force` | `{ "ok": true, "cleaned_up": string[] }` |

### `avenor_spawn`

Starts a new run. `agent` names the backend agent, and `repo_dir` is the working
directory for the run. `timeout` accepts seconds or duration suffixes such as
`90s`, `5m`, and `1h`.

Returns a generated MCP `run_id`. Pass that ID to the other tools.

### `avenor_status`

With no `run_id`, returns all supervisor runs. With `run_id`, returns one run.
Registry-backed runs include the MCP `run_id` and label. Completed runs derive
their terminal status from the sentinel file when available.

Status values are:

- `running`
- `done`
- `failed`
- `timeout`
- `killed`

When a run is blocked on a permission request, the result includes
`pending_permission`.

### `avenor_answer_permission`

Answers a pending permission request. If `request_id` is omitted, Avenor reads
the current pending request from status.

### `avenor_events`

Reads historical events from the run's event log. It skips malformed lines,
filters by `types` when provided, and returns the last `limit` matching events.
The default limit is `50`.

### `avenor_follow_up`

Starts a new run using the completed prior run's stored `SESSION=...` metadata.
This is not a live prompt into the old runtime; it spawns a new runtime that
continues from the prior session.

### `avenor_shutdown`

Shuts down the target supervisor and cleans up MCP-owned sentinel and event log
files. Use `force: true` to request a kill instead of graceful shutdown.

## Registry scope

The Go MCP server keeps an in-memory run registry scoped to the MCP process.
That registry maps MCP `run_id` values to stable runtime IDs, sentinel files,
event logs, agent names, and working directories.

This means:

- `events` and `follow_up` require a run that was spawned by this MCP process.
- Durable cross-process recovery is intentionally out of scope for now.
- If `supervisor_id` is provided, `avenor_status` can query a stable runtime ID
  even when the run is not in the local registry.

## Typical workflow

1. Call `avenor_spawn`.
2. Poll `avenor_status` until the run is terminal or asks for permission.
3. If permission is pending, call `avenor_answer_permission`.
4. Call `avenor_events` to inspect what happened.
5. Optionally call `avenor_follow_up` after a completed run.
6. Call `avenor_shutdown` when the MCP session is done.
