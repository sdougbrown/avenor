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
| `done` (no output) | Agent finished without writing anything. It likely asked a clarifying question. | Inspect then call `avenor_follow_up` with your answer. |
| `failed` | Agent hit an error mid-run. | Report the failure. |
| `timeout` | Run exceeded its timeout. | Report the timeout. |
| `killed` | Run was forcefully terminated. | Report the kill. |
| `waiting` (`pending_permission`) | Agent hit a tool approval gate. It is blocked mid-task. | Answer via `avenor_answer_permission`, then resume waiting. |

### Supervision approaches

**Option A: Blocking** — let `avenor_result` handle the loop:

```
avenor_spawn(agent="jockey", repo_dir="/path/to/repo", prompt="fix the tests")
→ { "run_id": "...", "label": "...", "supervisor_id": "..." }

avenor_result(run_id="...")
→ { "status": "done", "ready": true, "output": "..." }

avenor_events(run_id="...")
→ { "events": [ { "type": "...", ... }, ... ] }

avenor_shutdown()
→ { "ok": true, "cleaned_up": [...] }
```

1. Call `avenor_spawn` with an agent name and repository path.
2. Call `avenor_result` to wait. It returns on terminal completion or a
   current pending permission. Answer the permission with
   `avenor_answer_permission`, then call `avenor_result` again.
3. Call `avenor_events` only when you need raw recent history.
4. Call `avenor_shutdown` when the session is done.

**Option B: Targeted waits** — use `avenor_status` when you need lifecycle
control without implementing a caller-side poll loop:

```
avenor_spawn(agent="jockey", repo_dir="/path/to/repo", prompt="fix the tests")
→ { "run_id": "...", "label": "...", "supervisor_id": "..." }

avenor_status(run_id="...", wait_for="turn_complete", timeout="5m", view="lifecycle")
→ { "status": "done", "phase": "done" }
```

Each wait requires one `run_id`. The server polls internally at a fixed cadence.
A caller does not provide a poll interval. A wait returns when its condition is
met, when a current permission request appears, or when its timeout expires.

### What not to watch for

- **File changes alone.** An agent that ends its turn without writing anything
  has still ended its turn. Do not treat an empty working tree as "still
  running." Check `avenor_status` instead.
- **`session.end` alone.** A permission-blocked run never reaches
  `session.end`. Check `pending_permission` separately.

### Follow up

Call `avenor_follow_up` when a `done` run needs more direction. It spawns a
new session continuing from the prior one. Treat the follow-up as a new run
through the same lifecycle. The existing `auto_approve` policy is inherited by
follow-ups, so approved runs remain unattended across continuation turns.

### Progress notifications

This issue uses long-poll responses instead of MCP progress notifications. The
Go SDK exposes notification capability, but Claude Code host rendering and
useful progress semantics are not established. Wait correctness is independent
of notifications.

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
- `wait_for` — wait for `terminal`, `phase_change`, `turn_complete`, or `permission`
- `timeout` — maximum wait such as `30s`, `5m`, or `1h`; only valid with `wait_for`
- `supervisor_id` — supervisor socket to query (default: the autostarted supervisor)

A wait requires one `run_id`. Without `wait_for`, `avenor_status` performs its
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
Inspect `pending_permission`, answer the request with `avenor_answer_permission`,
and then issue another wait. The legacy `waiting` status remains supported.

**Returns:** One status object if `run_id` is given, or an array of status objects if omitted.
A timed-out wait returns the latest status with `timed_out: true`; it does not
cancel the underlying run. Lifecycle view retains `timed_out` but omits
`final_output` and usage. Use `avenor_result` to harvest complete output.

### `avenor_result`

Waits for one run and returns its complete final output without transcript or raw event details.

**Required:**
- `run_id` — run ID or label to await

**Optional:**
- `wait` — wait for a terminal result (default: `true`)
- `timeout` — maximum time to wait, such as `30s` or `5m`
- `supervisor_id` — supervisor socket to query

A terminal response has `ready: true` and includes the complete `output` when the backend exposed final assistant text. A blocked run returns its `pending_permission` immediately, even when its public status is still `running`. Answer the request before waiting again. If an older or unavailable control endpoint prevents lossless retrieval and a presentation fallback is returned, `output_truncated: true` and `output_event_path` make its possible truncation explicit; retry `avenor_result` or read the durable event path. If the result tool's own timeout expires, it returns the latest state with `ready: false` and `timed_out: true`; the underlying run keeps going.

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
