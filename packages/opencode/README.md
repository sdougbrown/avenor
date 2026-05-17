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

Once registered, the plugin exposes six tools that your OpenCode agent can call directly — no extra configuration required.

## Tools

### `avenor_spawn`

Dispatch an agent run in the current working directory. Returns a `run_id` you can poll with `avenor_status`.

| Argument | Required | Description |
|---|---|---|
| `agent` | yes | Agent name (no default — must be specified) |
| `prompt` | no | Prompt text |
| `prompt_file` | no | Path to a prompt file |
| `label` | no | Human-readable label for the run |
| `timeout` | no | Timeout duration (e.g. `3600s`, `30m`) |
| `model` | no | Model override |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

The working directory (`dir`) is injected automatically from your OpenCode session context — it is not a user-facing argument.

### `avenor_status`

Get status of a specific run or all active runs. Surfaces any pending permission requests the sub-agent is blocked on.

| Argument | Required | Description |
|---|---|---|
| `run_id` | no | Specific run ID to query (omit for all runs) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

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

## Typical workflow

```
1. avenor_spawn  →  receive run_id
2. avenor_status  →  check if still running, spot pending permissions
3. avenor_answer_permission  →  unblock if a permission is requested
4. avenor_events  →  inspect output once complete
5. avenor_follow_up  →  optionally send a follow-up
6. avenor_shutdown  →  clean up when finished
```

## Dependencies

- **Peer:** `@opencode-ai/plugin` v1.15.3 (the runtime that host agents use to mount plugins)
- **Binary:** `avenor` must be available on `PATH` or at `AVENOR_BIN` (checked at first tool invocation, not at plugin load time)
