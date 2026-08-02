# CLI Reference

The `avenor` command-line tool runs agents, supervises them over time, inspects their output, and answers their questions. This page catalogs every subcommand, its flags, and how they fit together.

## Overview

Avenor operates in three modes:

1. **Single run** — `avenor run` (or just `avenor` with no subcommand). Start an agent, wait for it to finish.
2. **Long-lived supervisor** — `avenor stable`. A process that lives for hours or days, spawning child agents on demand via the control plane.
3. **Orchestration** — Combine `stable` + `control spawn/cancel/prompt` + `watch` to coordinate multi-phase workflows, permission handling, and real-time visibility.

All modes produce NDJSON event streams on disk, which you query with `watch`. Permissions are answered via `answer` or the control plane.

## avenor run

Default subcommand. Starts a single agent and blocks until it finishes or times out.

```
avenor run [flags]
avenor [flags]  # equivalent; explicit "run" is optional
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--agent` | (none) | Agent name; used to resolve configured model from opencode config if `--model` is not set |
| `--prompt` | (none) | Inline prompt text; mutually exclusive with `--prompt-file` |
| `--prompt-file` | (none) | Path to prompt file; mutually exclusive with `--prompt` |
| `--loop-file` | (none) | Path to multi-phase loop config JSON; enables looping mode (incompatible with `--resume`) |
| `--roster-file` | (none) | Path to a roster map for direct selection, or the root workflow fallback |
| `--roster-entry` | (none) | Entry key within `--roster-file` for a direct spawn; invalid with a workflow file |
| `--label` | (none) | Free-form label for log correlation; appears in events and sentinel |
| `--dir` | `.` | Working directory for the agent |
| `--resume` | (none) | Resume an existing session by ID; incompatible with `--loop-file` |
| `--server-url` | (none) | Long-lived ACP server endpoint; required for `--backend opencode-http` |
| `--backend` | `opencode-acp` | Runtime backend: `opencode-acp`, `agy`, `gemini-acp`, `cursor-acp`, `codex-app-server`, `opencode-http`, `claude-channel` |
| `--model` | (none) | Backend-specific model ID; if not set, resolved from opencode config via `--agent` |
| `--thinking` | (none) | Backend-native reasoning level; validated against the effective backend |
| `--on-event` | (none) | Path to write NDJSON event stream; events are discarded if unset |
| `--sentinel-file` | (none) | Path to write a completion sentinel (exit code, session ID, stop reason); also derives permission handler unless `--permission-handler` is set |
| `--permission-handler` | (derived) | Permission resolver: `file:<path>` for file-based answers, or omitted for socket/auto-approve only. If `--sentinel-file` is set and `--permission-handler` is not, defaults to `file:<sentinel-base>` |
| `--auto-approve` | `false` | Automatically approve ordinary permission requests. Questions that require user input still wait for an answer |
| `--control-socket` | (none) | Unix socket path for control plane; enables remote commands while session is running |
| `--http-debug` | (none) | HTTP debug adapter bind address (e.g., `127.0.0.1:8080`); enables `/debug/status` and `/debug/events` endpoints |
| `--timeout` | `0` (disabled) | Overall session timeout (Go duration: `30s`, `5m`, etc.); fires after duration regardless of progress |
| `--progress-timeout` | `0` (disabled) | Session progress timeout; fires if no event received for this duration |
| `--max-retries` | `0` | Maximum retry attempts on transient failure (exit code 1); emits `avenor.retry` and `avenor.error` events (see [events.md](events.md)) |
| `--run-id` | (generated) | Correlation ID for this run; auto-generated if not set |
| `--permission-claim-timeout` | `0` (disabled) | Optional deadline for a connected control client to answer a permission request. With 0, fallback occurs only after all control clients disconnect |

### Examples

Run a single agent with a prompt file:

```sh
avenor run --prompt-file my-prompt.txt --label "data-prep" \
  --on-event /tmp/events.jsonl --sentinel-file /tmp/run.sentinel
```

Run with auto-retry on transient failure (up to 3 attempts):

```sh
avenor run --prompt "find bugs" --max-retries 3 \
  --backend opencode-http --server-url http://localhost:8080 \
  --on-event /tmp/events.jsonl
```

See [loop.md](loop.md) for multi-phase loop config.

### Roster selection

A roster is a JSON map from a name to a complete backend/agent/model loadout. `roster_file` names the map; `roster_entry` names one key inside it. Every entry must contain `backend` and at least one of `agent` or `model`:

```json
{
  "planner": {
    "backend": "opencode-acp",
    "model": "provider/model"
  },
  "executor": {
    "backend": "agy",
    "agent": "windsurf-swe"
  }
}
```

Direct mode selects an entry with both flags and does not accept direct identity overrides:

```sh
avenor run --prompt "Analyze the repository" \
  --roster-file /repo/roster.json --roster-entry planner
```

The direct selector is intentionally permissive when no roster is selected: `--agent`, `--model`, and `--backend` are independently optional. Any combination of the three may be supplied — including none at all — and omitted identity values use the selected runtime's defaults, and the last command uses the explicit backend with no agent or model:

```sh
avenor run --prompt "Use the runtime defaults"
avenor run --prompt "Use this agent" --agent reviewer
avenor run --prompt "Use this model" --model provider/model
avenor run --prompt "Use backend defaults" --backend opencode-acp
```

`--roster-file` and `--roster-entry` must be supplied together for a direct request. A roster request cannot also supply `--agent`, `--model`, or `--backend`; the roster entry supplies all three identity fields. A roster file is strict: `system`, `thinking`, and unknown entry fields are rejected. Run-level `--thinking` remains separate and is checked against the effective backend selected by the direct entry or by each workflow phase.

For a workflow, `--roster-file` is only a root fallback and `--roster-entry` is invalid. The workflow config selects entries per direct phase:

```sh
mkdir -p /tmp/avenor-roster-demo
cat >/tmp/avenor-roster-demo/roster.json <<'JSON'
{
  "plan": {"backend": "opencode-acp", "model": "provider/planner"},
  "test": {"backend": "agy", "model": "provider/tester"}
}
JSON
cat >/tmp/avenor-roster-demo/loop.json <<'JSON'
{
  "roster_file": "roster.json",
  "pre": [{
    "name": "plan",
    "roster_entry": "plan",
    "prompt": "Produce the implementation plan."
  }],
  "loop": [{
    "name": "test",
    "roster_entry": "test",
    "prompt": "Run the tests and report the result.",
    "on_incomplete": {"nudge": "Finish the test report.", "max_nudges": 1}
  }]
}
JSON
avenor run --dir /tmp/avenor-roster-demo --loop-file /tmp/avenor-roster-demo/loop.json
```

A phase roster entry overrides the run-level backend, agent, and model for that phase. Phases without an entry retain the existing run-level selection rules. `roster_entry` is valid only for a direct prompt or prompt-file phase, not for a phase dispatching `loop_file` or `team_file`.

For CLI workflows, a declared `roster_file` is resolved relative to its loop/team config. If the root config has no declaration, the command-level `--roster-file` fallback is resolved relative to `--dir`; nested CLI loads do not reuse that fallback. A child with no declaration inherits the already loaded parent entries, recursively; a child declaration replaces them. Stable-mode nesting keeps its existing behavior and does not gain this CLI-only inheritance rule. For direct selection, pass a path that is valid from the invoking process (an absolute path avoids ambiguity).

A phase with `resume_from_previous: true` must match four values stored for the session: effective backend, agent, model, and agent profile. If its roster selection changes any value, Avenor fails the phase before creating a provider. Matching only the backend is insufficient.

## avenor stable

Start a long-lived supervisor process that accepts control commands on a Unix socket. The supervisor manages a pool of child runtimes, spawning them on demand and reusing them across commands.

```
avenor stable --control-socket /tmp/avenor.sock
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--control-socket` | (required) | Unix socket path for control plane commands |
| `--http-debug` | (none) | HTTP debug adapter bind address (e.g., `127.0.0.1:8080`) |
| `--max-runtimes` | `16` | Maximum concurrent child runtimes |
| `--idle-timeout` | `0` (disabled) | Exit after this duration with no child runtimes and no control connections |
| `--shutdown-timeout` | `10s` | Graceful shutdown timeout before killing children |
| `--permission-claim-timeout` | `0` (disabled) | Optional permission claim deadline; with 0, fallback occurs only after all control clients disconnect |

### Example

Start a long-lived supervisor:

```sh
avenor stable --control-socket /tmp/avenor.sock --max-runtimes 4 \
  --idle-timeout 2h &
```

Then spawn runtimes via `avenor control spawn`. See [stable.md](stable.md) and [control-protocol.md](control-protocol.md) for full details.

## avenor control

Send commands to a running supervisor or standalone run with a control socket. The supervisor must be listening on `--control-socket`.

```
avenor control --socket /tmp/avenor.sock <command> [args...]
```

### Global flags

| Flag | Required | Description |
|------|----------|-------------|
| `--socket` | yes | Control socket path |

### Commands

#### status [runtime-id]

Fetch the status snapshot of a runtime, or the supervisor if no runtime-id given. Outputs JSON.

```sh
avenor control --socket /tmp/avenor.sock status
avenor control --socket /tmp/avenor.sock status rt-12345
```

#### list

List all running child runtimes. Outputs JSON array.

```sh
avenor control --socket /tmp/avenor.sock list
```

#### tail

Subscribe to and tail all events from all runtimes. Outputs NDJSON.

```sh
avenor control --socket /tmp/avenor.sock tail
```

#### spawn [flags]

Spawn a new runtime under the supervisor with the given parameters. Outputs JSON result with assigned runtime-id.

Spawn flags (all optional):

| Flag | Description |
|------|-------------|
| `--prompt` | Inline prompt text (mutually exclusive with `--prompt-file`) |
| `--prompt-file` | Path to prompt file (mutually exclusive with `--prompt`) |
| `--dir` | Working directory |
| `--agent` | Agent name |
| `--label` | Free-form label for log correlation |
| `--model` | Backend-specific model ID |
| `--backend` | Runtime backend: `opencode-acp`, `agy`, `gemini-acp`, `cursor-acp`, `codex-app-server`, `opencode-http`, `claude-channel` |
| `--server-url` | Long-lived ACP server endpoint |
| `--on-event` | Path to write NDJSON events |
| `--sentinel-file` | Path to write completion sentinel |
| `--permission-handler` | Permission resolver (e.g., `file:<path>`) |
| `--auto-approve` | Automatically approve all permission requests |
| `--timeout` | Overall session timeout (Go duration: `30s`, `5m`, etc.) |
| `--max-retries` | Maximum retry attempts on transient failure |

```sh
avenor control --socket /tmp/avenor.sock spawn \
  --prompt "find bugs" --label "phase-1" --on-event /tmp/events.jsonl
```

#### cancel [runtime-id]

Cancel a running runtime. If no runtime-id given, cancels the only child or errors.

```sh
avenor control --socket /tmp/avenor.sock cancel rt-12345
```

#### prompt `<text>` [runtime-id]

Queue a follow-up prompt for the runtime. If no runtime-id given, targets the only child.

```sh
avenor control --socket /tmp/avenor.sock prompt "continue with part 2" rt-12345
```

#### interrupt-and-prompt `<text>` [runtime-id]

Interrupt the current message generation and queue a new prompt.

```sh
avenor control --socket /tmp/avenor.sock interrupt-and-prompt "stop and refocus" rt-12345
```

#### answer-permission `<request-id>` `<option-id>` [runtime-id]

Answer a pending permission request by request-id and option-id. If no runtime-id given, targets the only child.

```sh
avenor control --socket /tmp/avenor.sock answer-permission \
  req-abc123 opt-allow
```

See [permission-handler.md](permission-handler.md) for how to integrate with your permission UI.

#### shutdown [mode]

Shut down the supervisor. Mode is `graceful` (default, waits for children) or `immediate`.

```sh
avenor control --socket /tmp/avenor.sock shutdown graceful
```

## avenor watch

Read or tail NDJSON event logs, with optional digestion into human-readable format.

```
avenor watch [flags] <log>
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--follow` | `false` | Poll and tail the log instead of reading to EOF |
| `--format` | `plain` | Output format: `plain` (EVENT lines) or `json` (pass-through NDJSON) |
| `--classify` | `false` | Prefix each plain-format line with MILESTONE \| FINDING \| ACTIVITY; in json format, adds a top-level `classify` field |
| `--poll-interval` | `250ms` | Follow-mode sleep interval |
| `--since-cursor` | (none) | Cursor file path: seek to saved offset before reading; rewrite offset on exit |

### Examples

Read a log to completion and output human-readable digest:

```sh
avenor watch --format plain /tmp/events.jsonl
```

Tail a log as it grows, updating a cursor file so you can resume from the same point:

```sh
avenor watch --follow --format plain --since-cursor /tmp/cursor /tmp/events.jsonl
```

Tail with classification (milestones, findings, etc.):

```sh
avenor watch --follow --format plain --classify /tmp/events.jsonl
```

See [watch.md](watch.md) and [events.md](events.md) for full details.

## avenor answer

Write a permission response file to answer a pending permission request. Specific to the file-based permission handler (`--permission-handler file:<path>`); not applicable when using `--auto-approve` or a control socket.

```
avenor answer [flags] <perm-base>
```

The permission handler expects files at `<perm-base>.req` (request) and `<perm-base>.req.response` (response).

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--option` | (required) | Option ID to select (must match an optionId from the .req file) |
| `--message` | (none) | Free-text message to include in the response |
| `--outcome` | `selected` | Outcome: `selected` (approve) or `cancelled` (deny) |
| `--force` | `false` | Overwrite an existing response file; by default, command errors if response already exists |

### Example

Read the request file, find an allowed option ID, and answer:

```sh
cat /tmp/run.perm.req | jq '.options[] | select(.kind == "allow") | .optionId'
# Output: opt-12345

avenor answer --option opt-12345 --message "approved by ops" /tmp/run.perm
```

See [permission-handler.md](permission-handler.md) for full integration guide.

## avenor mcp

Run an MCP (Model Context Protocol) server that orchestrates avenor runtimes. Used by Claude and other compatible clients to invoke agents remotely.

```
avenor mcp [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--transport` | `stdio` | Transport: `stdio` (default, single client) or `http` (multi-client) |
| `--control-socket` | (none) | Unix socket path for the supervisor (created if not present and `--no-autostart` is not set) |
| `--supervisor-socket` | (none) | Unix socket path for an existing supervisor; requires `--no-autostart` |
| `--no-autostart` | `false` | Disable automatic supervisor startup; requires `--supervisor-socket` |
| `--idle-timeout` | `30m` | Idle timeout before server exits |
| `--addr` | `127.0.0.1:3748` | Address to listen on for HTTP transport |
| `--auth-token` | (none) | Bearer token for HTTP transport; defaults to `MCP_AUTH_TOKEN` env var |

### Examples

Run stdio MCP server (single client):

```sh
avenor mcp
```

Run HTTP MCP server on a custom port:

```sh
avenor mcp --transport http --addr 0.0.0.0:9000 --auth-token secret123
```

Connect to an existing supervisor:

```sh
avenor mcp --transport stdio --supervisor-socket /tmp/avenor.sock --no-autostart
```

See [mcp.md](mcp.md) for protocol details and client configuration.

## avenor probe

Run a diagnostic probe against an OpenCode ACP backend to discover its capabilities and configuration.

```
avenor probe --out <transcript>
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--dir` | `.` | Working directory for the probe |
| `--out` | (required) | Path to write the probe transcript (NDJSON events) |
| `--prompt` | (default) | Override the default probe prompt; used only in Stage 1 discovery |
| `--timeout` | `5m` | Probe timeout |

### Example

Probe an OpenCode installation and save the transcript:

```sh
avenor probe --dir /path/to/project --out /tmp/probe.jsonl
```

Then read the transcript:

```sh
avenor watch /tmp/probe.jsonl
```

See [backends.md](backends.md) for more on backend discovery.

## avenor claude-channel

Internal MCP sidecar invoked automatically by Claude Code when using the `claude-channel` backend. You don't run this directly — Avenor writes a per-run entry to the project's `.mcp.json` that tells Claude Code how to invoke it, then removes the entry when the session ends. The backend requires `tmux` on `PATH` because Avenor boots Claude Code inside a detached interactive tmux session.

```
avenor claude-channel --run-id <id> --token <token> --broker-url <url>
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `--run-id` | yes | Avenor run ID issued by the broker |
| `--token` | yes | Per-run bearer token for broker authentication |
| `--broker-url` | yes | HTTP base URL of the in-process broker |

### Bootstrap and cleanup

When starting a `claude-channel` backend session, Avenor:
1. Generates a unique 8-character run ID (e.g., `47212ef3`)
2. Writes an MCP server entry to `<project-dir>/.mcp.json` with name `avenor-channel-47212ef3`
3. The entry uses the avenor binary itself: `avenor claude-channel --run-id <id> --token <token> --broker-url <url>`
4. On session exit, removes the entry from `.mcp.json`

If the session crashes or is forcibly killed, the entry may remain. Use `avenor claude-channel-cleanup --dir <project-dir>` as a recovery command to remove stale entries manually after crash cleanup did not run.

The sidecar speaks JSON-RPC 2.0 over stdio, declares `claude/channel` and `claude/channel/permission` capabilities, and exposes the three tools Claude uses to communicate back:

- `avenor_report` — progress update
- `avenor_finish` — session completion with status and summary
- `avenor_reply` — directed reply to a named recipient

See [backends.md](backends.md#claude-channel-experimental) for architecture and security notes.

## avenor claude-channel-cleanup

Remove all `avenor-channel-*` MCP server entries from a project's `.mcp.json`. Use this to clean up stale entries if an avenor session crashes or is forcibly killed before it can remove its bootstrap config.

```
avenor claude-channel-cleanup --dir <project-dir>
```

### Flags

| Flag | Default | Description |
|------|----------|-------------|
| `--dir` | `.` | Project directory containing `.mcp.json` |

### Example

Clean up stale avenor-channel entries from a project:

```sh
avenor claude-channel-cleanup --dir /path/to/project
# Output: removed 2 avenor-channel entries from /path/to/project/.mcp.json
```

The cleanup command is safe to run at any time — it only removes entries with names starting with `avenor-channel-` and leaves all other MCP servers intact. If no avenor-channel entries exist, it reports `removed 0` and exits successfully.

## Common Patterns

### Long-lived supervisor with on-demand agents

Start a supervisor in the background, then spawn agents as needed:

```sh
# Terminal 1: Start supervisor
avenor stable --control-socket /tmp/avenor.sock &
SUPERVISOR_PID=$!

# Terminal 2: Spawn a phase-1 agent
avenor control --socket /tmp/avenor.sock spawn \
  --prompt "analyze the codebase" \
  --label phase-1 \
  --on-event /tmp/phase1.jsonl

# Terminal 2: Tail its output in real-time
avenor watch --follow --format plain /tmp/phase1.jsonl

# When phase-1 finishes, spawn phase-2 on the same supervisor
avenor control --socket /tmp/avenor.sock spawn \
  --prompt "generate tests for the issues found" \
  --label phase-2 \
  --on-event /tmp/phase2.jsonl

# Shutdown supervisor when done
avenor control --socket /tmp/avenor.sock shutdown graceful
kill $SUPERVISOR_PID 2>/dev/null
```

### Permission handling via file

Run an agent with file-based permission handling:

```sh
# Start agent with permission handler
avenor run --prompt-file prompt.txt \
  --sentinel-file /tmp/run.sentinel \
  --on-event /tmp/events.jsonl &
RUN_PID=$!

# Monitor for permission requests
avenor watch --follow /tmp/events.jsonl | grep -i "permission.request" &

# When you see a request, answer it
avenor answer --option opt-allow /tmp/run.perm

# Wait for completion
wait $RUN_PID
```

### Multi-phase workflow with control socket

Orchestrate a multi-phase workflow using control commands:

```sh
# Start first phase
avenor run --prompt "phase 1 prompt" \
  --control-socket /tmp/rt.sock \
  --on-event /tmp/events.jsonl &

# While it's running, monitor status
sleep 2
avenor control --socket /tmp/rt.sock status

# Interrupt and send follow-up prompt
avenor control --socket /tmp/rt.sock interrupt-and-prompt "phase 2 prompt"

# Tail events
avenor watch --follow /tmp/events.jsonl
```

### Event log inspection with cursor tracking

Process event logs incrementally, remembering position across runs:

```sh
# Initial read and cursor save
avenor watch --format plain --since-cursor /tmp/cursor /tmp/events.jsonl

# Later, resume from saved position
avenor watch --follow --format plain --since-cursor /tmp/cursor /tmp/events.jsonl
```

## Exit Codes

- `0` — Success (session ended with stop_reason "success" or no explicit reason)
- `1` — General error or session ended with stop_reason "error"; may trigger retry if `--max-retries` > 0
- `2` — Usage/validation error (avenor control subcommands only)
- `124` — Overall session timeout (`--timeout` reached)
- `130` — Cancelled (SIGINT, context done)

See [loop.md](loop.md) for how exit codes interact with multi-phase loops.

## Environment Variables

- `AVENOR_OPENCODE_URL` — Default server URL if `--server-url` not set (only consulted for subprocess discovery)
- `MCP_AUTH_TOKEN` — Bearer token for HTTP MCP server if `--auth-token` not set
- `OPENCODE_CONFIG_DIR` — Directory to search for opencode config; defaults to `~/.config/opencode/`

## Related Documentation

- [stable.md](stable.md) — Supervisor architecture and lifecycle
- [control-protocol.md](control-protocol.md) — Control plane protocol and command semantics
- [watch.md](watch.md) — Event log digestion and filtering
- [permission-handler.md](permission-handler.md) — Permission request/response flow
- [mcp.md](mcp.md) — MCP server configuration and protocol
- [backends.md](backends.md) — Supported runtime backends
- [loop.md](loop.md) — Multi-phase loop configuration
- [events.md](events.md) — Event schema and types
