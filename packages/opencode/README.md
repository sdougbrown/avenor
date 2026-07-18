# @dougbots/avenor-opencode

OpenCode plugin that registers avenor agent runs as tools in your OpenCode session. When you need a sub-agent to handle a well-defined task — write code, run tests, search a codebase — avenor operates in the background while you keep your session focused.

## Setup

Install the package alongside its peer dependency:

```bash
bun add @dougbots/avenor-opencode @opencode-ai/plugin@1.15.3
```

You also need the `avenor` binary. Either add it to your `PATH` or set the `AVENOR_BIN` environment variable to its location.

```bash
# Option 1 — add to PATH (e.g. via mise, brew, or manual install)
which avenor

# Option 2 — explicit path
export AVENOR_BIN=~/.botfiles/bin/avenor
```

## Register the plugin

Add `@dougbots/avenor-opencode` to your OpenCode configuration. If you use a global config:

```json
// ~/.opencode.json
{
  "plugins": ["@dougbots/avenor-opencode"]
}
```

For a project-level config, add the same entry to `.opencode.json` in your repo root.

Once registered, the plugin exposes eight tools that your OpenCode agent can call directly — no extra configuration required.

## Behaviour

When a run is dispatched with `avenor_spawn`, the plugin:

1. **Blocks by default** — the tool call stays open, updating its title and metadata as the sub-agent runs, exactly like OpenCode's own sub-agent tool calls. The tool returns when the run reaches a terminal state.
2. **Re-prompts on completion** — when a `wait=false` (fire-and-forget) run finishes, the plugin automatically injects a completion message into the orchestrating session so the LLM picks up without manual polling.
3. **Routes permissions** — when a sub-agent running under the `opencode-http` backend requests a permission, the plugin injects a re-prompt into the orchestrating session. The LLM can call `avenor_answer_permission` to respond; the normal permission dialog is also shown as a fallback.

## Tools

### `avenor_spawn`

Dispatch an agent run in the current working directory.

**Blocking mode (default, `wait=true`):** the tool call shows live progress updates and returns a structured result when the run finishes.

**Fire-and-forget (`wait=false`):** returns immediately with `run_id`. The orchestrating session is re-prompted automatically on completion.

| Argument | Required | Description |
|---|---|---|
| `agent` | yes | Agent name (no default — must be specified) |
| `prompt` | no | Prompt text |
| `prompt_file` | no | Path to a prompt file |
| `label` | no | Human-readable label for the run |
| `timeout` | no | Timeout duration (e.g. `3600s`, `30m`) |
| `model` | no | Model override |
| `wait` | no | Block until complete (default `true`) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

The working directory (`dir`) is injected automatically from your OpenCode session context — it is not a user-facing argument.

### `avenor_status`

Get status of a specific run or all active runs. Use the compact `lifecycle` view for progress and pending permission checks; `full` remains the compatibility default.

| Argument | Required | Description |
|---|---|---|
| `run_id` | no | Specific run ID to query (omit for all runs) |
| `view` | no | Response detail: `lifecycle` or `full` (default) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_result`

Wait for a run to finish and return its bounded final output without transcript or event details. This is the output retrieval tool; `avenor_status` remains a lifecycle check.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID to await |
| `wait` | no | Wait for a terminal result (default `true`) |
| `timeout` | no | Maximum time to wait (for example `30s` or `5m`) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

A blocked run returns its pending permission instead of waiting forever. If this tool's own timeout expires, it returns the latest status with `ready: false` and `timed_out: true`.

### `avenor_answer_permission`

Answer a pending permission request. The `option_id` comes from the `pending_permission.options` array returned by `avenor_status`.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID with the pending permission |
| `option_id` | yes | Option ID from `pending_permission.options` |
| `request_id` | no | Request ID (auto-discovered if omitted) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_follow_up`

Resume a completed run with a follow-up message. Useful for iterating on results or asking the sub-agent to refine its output.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Completed run ID to resume |
| `message` | yes | Follow-up message text |
| `label` | no | Override label (defaults to `<original>-followup`) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_events`

Read events from a run, optionally filtered by type. Returns the last N events.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID to read events from |
| `types` | no | Filter by event type (array of strings) |
| `limit` | no | Max events to return (default 50) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_shutdown`

Shut down the avenor supervisor process and clean up temp files. Call this when you are done delegating work or if the supervisor has crashed.

| Argument | Required | Description |
|---|---|---|
| `supervisor_id` | no | Supervisor to shut down (defaults to the singleton) |
| `force` | no | Force shutdown instead of graceful |

## Typical workflows

**Blocking (default):**
```
1. avenor_spawn            →  tool call shows live progress, blocks until done
2. (avenor_answer_permission  →  if a permission is routed to this session mid-run)
3. tool call returns       →  completion preview with status + session_id
4. avenor_result           →  retrieve the bounded final output when needed
5. avenor_follow_up        →  optionally iterate
6. avenor_shutdown         →  clean up when finished
```

**Parallel / fire-and-forget (`wait=false`):**
```
1. avenor_spawn × N        →  each returns run_id immediately
2. plugin starts monitoring each run immediately
3. (sub-agent finishes)    →  plugin re-prompts this session automatically
4. avenor_answer_permission  →  if a permission was routed here mid-run
5. avenor_result           →  wait for and retrieve the final output
6. avenor_events           →  inspect raw event history when needed
```

## Dependencies

- **Peer:** `@opencode-ai/plugin` v1.15.3 (the runtime that host agents use to mount plugins)
- **Binary:** `avenor` must be available on `PATH` or at `AVENOR_BIN` (checked at first tool invocation, not at plugin load time)
