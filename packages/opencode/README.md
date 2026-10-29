# @dougbots/avenor-opencode

OpenCode plugin that registers avenor agent runs as tools in your OpenCode session. When you need a sub-agent to handle a well-defined task — write code, run tests, search a codebase — avenor operates in the background while you keep your session focused.

## Setup

Install the package alongside its peer dependencies:

```bash
bun add @dougbots/avenor-opencode @opencode-ai/plugin@1.15.3
```

OpenCode 2 additionally loads the plugin definition shape directly from the default export, so no OpenCode 2 package is needed at runtime; OpenCode 1 hosts (1.18.29+) call the same entrypoint's `server()`.

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

Once registered, the plugin exposes these tools that your OpenCode agent can call directly — no extra configuration required: `avenor_spawn`, `avenor_status`, `avenor_result`, `avenor_inspect`, `avenor_answer_permission`, `avenor_message`, `avenor_peers`, `avenor_events`, `avenor_kill`, `avenor_shutdown`, `avenor_workflow_cancel`, `avenor_workflow_status`, `avenor_workflow_wait`, `avenor_workflow_inspect`, `avenor_workflow_events`, `avenor_workflow_complete`, `avenor_workflow_gate`, and `avenor_workflow_controller_status`.

## Behaviour

When a run is dispatched with `avenor_spawn`, the plugin:

1. **Blocks by default** — the tool call stays open, updating its title and metadata as the sub-agent runs, exactly like OpenCode's own sub-agent tool calls. The tool returns when the run reaches a terminal state.
2. **Re-prompts on completion** — when a `wait=false` (fire-and-forget) run finishes, the plugin automatically injects a completion message into the orchestrating session so the LLM picks up without manual polling.
3. **Routes permissions** — when a sub-agent running under the `opencode-http` backend requests a permission, the plugin injects a re-prompt into the orchestrating session. The LLM can call `avenor_answer_permission` to respond; the normal permission dialog is also shown as a fallback.
4. **Formats results** — OpenCode returns concise, bounded prose in its shared `output` for `avenor_status`, `avenor_result`, `avenor_answer_permission`, `avenor_message`, `avenor_peers`, `avenor_events`, `avenor_inspect`, and `avenor_shutdown`. Structured metadata stays in host/session state.
5. **Publishes its own finished turn** — a settled turn from the session that first ran a host messaging tool is recorded through `session_last_set`, so a peer's `avenor_peers` `action: "last"` can read it.

MCP clients render their own results.

## Tools

### `avenor_spawn`

Dispatch an agent run in the current working directory.

**Blocking mode (default, `wait=true`):** the tool call shows live progress updates and returns a structured result when the run finishes.

**Fire-and-forget (`wait=false`):** returns immediately with `run_id`. The orchestrating session is re-prompted automatically on completion.

| Argument | Required | Description |
|---|---|---|
| `agent` | no | Agent name; omission uses the supplied model or runtime defaults |
| `prompt` | no | Prompt text |
| `prompt_file` | no | Path to a prompt file |
| `label` | no | Human-readable label for the run |
| `timeout` | no | Timeout duration (e.g. `3600s`, `30m`) |
| `model` | no | Model override |
| `thinking` | no | Run-level reasoning control; unsupported effective backends reject explicit values |
| `backend` | no | Backend override; direct mode defaults to Avenor's `opencode-acp` backend when omitted |
| `roster_file` | no | Path to a top-level roster map |
| `roster_entry` | no | Key within `roster_file` |
| `wait` | no | Block until complete (default `true`) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

The working directory (`dir`) is injected automatically from your OpenCode session context — it is not a user-facing argument.

#### Direct and roster modes

Use `roster_file` plus `roster_entry` to select a complete identity. Every roster entry requires `backend` and at least one of `agent` or `model`; roster mode rejects direct `agent`, `model`, and `backend` overrides. Direct mode allows all three identity fields to be omitted independently, including backend-only selection. Roster mode does not apply the direct `opencode-acp` default.

```json
{
  "prompt": "Review the repository",
  "roster_file": "/repo/roster.json",
  "roster_entry": "reviewer",
  "wait": true
}
```

Roster entries currently support only `backend`, `agent`, and `model`. `system` and `thinking` are rejected; `thinking` remains a run-level value and is validated against the effective backend. A resumed session cannot change backend. The plugin passes `roster_file` as supplied, so an absolute path avoids ambiguity when the OpenCode process directory differs from the project directory.

### `avenor_status`

Get status of a specific run or all active runs. Use the compact `lifecycle` view for progress and pending permission checks; `full` remains the compatibility default.

| Argument | Required | Description |
|---|---|---|
| `run_id` | no | Specific run ID to query (omit for all runs) |
| `view` | no | Response detail: `lifecycle` or `full` (default) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_result`

Wait for a run to finish and return its complete final output without transcript or event details. This is the output retrieval tool; `avenor_status` remains a lifecycle check.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID to await |
| `wait` | no | Wait for a terminal result (default `true`) |
| `timeout` | no | Maximum time to wait (for example `30s` or `5m`) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

A blocked run returns its pending permission instead of waiting forever. If this tool's own timeout expires, it returns the latest status with `ready: false` and `timed_out: true`.

### `avenor_inspect`

Use `avenor_inspect` to review a bounded snapshot. The snapshot includes transcript, completed and live tools, permissions, and final output. Use `avenor_events` when raw event records are required.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID to inspect |
| `limit` | no | Max events to reduce into the snapshot |
| `after_seq` | no | Only include events after this sequence number |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_answer_permission`

Answer a pending permission request. The `option_id` comes from the `pending_permission.options` array returned by `avenor_status`.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID with the pending permission |
| `option_id` | yes | Option ID from `pending_permission.options` |
| `request_id` | no | Request ID (auto-discovered if omitted) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_message`

Send a message to another run, withdraw a pending ask, resume a completed run, or deliver a control reply. Every action is explicit and every result states whether a failed ask can still be withdrawn.

| `action` | Required fields | What it does |
|---|---|---|
| `tell` | `target`, `message` | Deliver a message without creating an ask edge |
| `ask` | `target`, `message`, optional `timeout_ms` | Deliver a message and wait for a reply |
| `reply` | `reply_to`, `message` | Answer an inbound ask. This session receives no inbound delivery, so the result is always `edge-absent` and nothing is sent |
| `resume` | `target`, `message`, optional `label`, `idempotency_key` | Resume a completed run from its persisted session |
| `withdraw` | `message_id` | Withdraw a pending ask |
| `control_reply` | `target`, optional `payload` | Deliver a control reply where the provider supports it |

`target` is a run id, a peer label, or `"self"`. `timeout_ms` must be an integer no greater than the shared ask cap. This tool does not take `supervisor_id`; it is bound to the default supervisor.

The tool's structured metadata is the shared contract object in snake_case (`action`, `outcome`, `text`, and `message_id`, `run_id`, `label`, `reply`, `sentinel_file`, `on_event` when the outcome carries them), the same fields the MCP hosts return.

### `avenor_peers`

List the visible agent sessions, rename a run, or read a run's last agent message.

| `action` | Fields | What it does |
|---|---|---|
| `list` (default) | — | List the visible peers |
| `rename` | `name`, optional `session_id` | Change a display name, self by default |
| `last` | `target` | Read the target's last agent message with its stop reason |

`last` is read-only and returns the shared empty result when the target has no completed turn yet. `target` accepts a run id, a label, or `"self"`. The structured metadata is `{peers}`, `{renamed}` or `{last}` in snake_case, the same object the MCP hosts return.

### `avenor_events`

Read events from a run, optionally filtered by type. Returns the last N events.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID to read events from |
| `types` | no | Filter by event type (array of strings) |
| `limit` | no | Max events to return (default 50) |
| `supervisor_id` | no | Reuse an existing supervisor by socket path |

### `avenor_kill`

Terminate a run without shutting down the supervisor. Other runs and the supervisor itself keep running.

| Argument | Required | Description |
|---|---|---|
| `run_id` | yes | Run ID or label of the run to terminate |
| `supervisor_id` | no | Supervisor ID for multi-supervisor mode |

### `avenor_workflow_cancel`

Cancel a workflow instance. Both the actor and the reason are required; the manager rejects a missing actor before it looks at the reason.

| Argument | Required | Description |
|---|---|---|
| `workflow_id` | yes | Workflow ID |
| `actor` | yes | Who is cancelling |
| `reason` | yes | Why the workflow is being cancelled |
| `supervisor_id` | no | Supervisor ID for multi-supervisor mode |

### Workflow tools

The remaining workflow tools read or drive workflow instances. Their argument lists are in [`docs/control-protocol.md`](../../docs/control-protocol.md).

| Tool | Purpose |
|---|---|
| `avenor_workflow_status` | Lightweight instance status |
| `avenor_workflow_wait` | Wait for a terminal state or a timeout |
| `avenor_workflow_inspect` | Full instance detail |
| `avenor_workflow_events` | The instance's log events |
| `avenor_workflow_complete` | Complete a machine/external handoff activation |
| `avenor_workflow_gate` | Record a gate decision on a parked activation |
| `avenor_workflow_controller_status` | Controller status, with an advisory candidate count |

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
4. avenor_result           →  retrieve the complete final output when needed
5. avenor_message          →  optionally iterate with action "resume"
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
- **Peer:** `@opencode/plugin` ^2.2.0 (type definitions for the OpenCode 2 loader; not required at runtime)
- **Binary:** `avenor` must be available on `PATH` or at `AVENOR_BIN` (checked at first tool invocation, not at plugin load time)
