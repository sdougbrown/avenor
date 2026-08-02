# Avenor: Stable Mode

Stable mode runs Avenor as a long-lived supervisor process that manages many child runtimes over its lifetime. Instead of `avenor run` (single prompt, runs once), you start a supervisor with `avenor stable` that accepts commands to spawn new runtimes, check status, send follow-up prompts, cancel runtimes, or shut everything down — all over a control socket.

## When You Need Stable Mode

A plain `avenor run` is fire-and-forget: you give it a prompt, it runs to completion, you check results later. That's fine for one-shot tasks.

Stable mode is for orchestrators. You need it when:

- **Many sequential or concurrent runs** — a CI/CD pipeline spawning 5 test suites in parallel, waiting for results, then triggering a merge review
- **Long-lived supervision** — an agent that kicks off a task every hour and manages its lifecycle over days
- **External orchestration** — a third-party scheduler (Kubernetes, Nomad, etc.) that needs to dynamically spawn runs and query their state without maintaining a 1:1 parent process
- **Interactive workflows** — a human operator who wants to interrupt a stuck run, inject a new prompt mid-session, or answer permission requests in real time

The control protocol sits on top of the supervisor, so you interact with it the same way whether you're controlling one runtime directly or 100 child runtimes spawned by a stable supervisor.

## Starting the Supervisor

```bash
avenor stable --control-socket /tmp/avenor-stable.sock
```

Flags:

| Flag | Default | Description |
|---|---|---|
| `--control-socket` `<path>` | (required) | Unix socket path for the control plane. Avenor writes a tombstone file at `<path>.dead` to signal abnormal shutdown |
| `--max-runtimes` | 16 | Maximum concurrent child runtimes. Spawn requests are rejected once this limit is hit |
| `--idle-timeout` | 0 | Exit cleanly after this duration with no child runtimes running and no control connections active. 0 disables (supervisor runs until signaled) |
| `--shutdown-timeout` | 10s | How long to wait for child runtimes to finish gracefully before killing them |
| `--http-debug` | (empty) | If set, bind an HTTP debug adapter to this address (e.g. `:8080`). Useful for rapid inspection and testing |
| `--permission-claim-timeout` | 0 | Optional deadline for a connected control client to answer a permission request. With 0, control retains the request until it is answered or all clients disconnect |

The supervisor does not exit until you signal it (SIGINT/SIGTERM), all child runtimes have finished and the idle timeout expires, or a fatal error occurs.

### Tombstone File

When the supervisor exits (cleanly or not), it writes a tombstone file at `<control-socket>.dead` with the format:

```
STOPPED reason=<reason> pid=<pid> at=<timestamp>
```

Possible reasons:

- `signal` — caught SIGINT or SIGTERM
- `shutdown` — shut down via `avenor control shutdown` command
- `idle` — exited due to idle timeout
- `start_failed` — failed to start the control socket or HTTP debug server
- `crashed` — panic in supervisor goroutine

The tombstone exists so that external tooling (e.g. a process monitor checking for stale sockets) can detect that the supervisor is gone and clean up accordingly. Avenor removes any existing tombstone file before starting, so its mere presence indicates a previous instance.

## Spawning Child Runtimes

Use `avenor control spawn` to start a new child runtime. The supervisor accepts one prompt source (either inline or from file) and optional loop configuration.

```bash
avenor control --socket /tmp/avenor-stable.sock spawn \
  --prompt "Review this pull request and suggest fixes" \
  --dir /repo/branch-a \
  --agent reviewer \
  --model claude-sonnet \
  --label "pr-review-#42" \
  --on-event /tmp/pr42-events.ndjson \
  --sentinel-file /tmp/pr42-done.env
```

Spawn parameters (all optional except one prompt source):

| Parameter | Type | Description |
|---|---|---|
| `--prompt` | string | Inline prompt text. Mutually exclusive with `--prompt-file` |
| `--prompt-file` | string | Path to a file containing the prompt. Mutually exclusive with `--prompt` |
| `--dir` | string | Working directory for the runtime. Defaults to `.` |
| `--agent` | string | Agent name (e.g. `jockey`, `butler`). Backend-specific |
| `--label` | string | Free-form label for log correlation and list output |
| `--model` | string | Backend-specific model ID (e.g. `claude-sonnet-4-5`) |
| `--backend` | string | Runtime backend (opencode-acp, opencode-http, codex-app-server, agy, gemini-acp, cursor-acp, pi). Defaults to opencode-acp |
| `--server-url` | string | External ACP server endpoint for opencode-http backend |
| `--on-event` | string | Path to write NDJSON events. Auto-created under `$TMPDIR/avenor-stable/<supervisor_run_id>/<runtime_id>/events.ndjson` if not set |
| `--sentinel-file` | string | Path to write completion sentinel (exit code, session ID, stop reason). Auto-created under `$TMPDIR/avenor-stable/<supervisor_run_id>/<runtime_id>/sentinel.env` if not set |
| `--permission-handler` | string | Permission handler (supports `file:<path>`). Auto-derived from sentinel-file if unset. Use `--auto-approve` to skip file-based handlers |
| `--auto-approve` | bool | Automatically approve ordinary permission requests without file-based handlers. Questions that require user input still wait for an answer |
| `--timeout` | int | Overall session timeout in seconds |
| `--max-retries` | int | Maximum retry attempts on transient failure |

Spawn returns immediately with the runtime ID and paths to the event log and sentinel file:

```json
{
  "runtime_id": "rt_1",
  "session_id": "ses_abc123",
  "on_event": "/tmp/pr42-events.ndjson",
  "sentinel_file": "/tmp/pr42-done.env"
}
```

### Prompt Sources

At least one of `--prompt`, `--prompt-file`, or `--loop-file` is required (see below for loop spawns). If both `--prompt` and `--prompt-file` are set, spawn fails.

### Auto-Created Artifact Directories

If you don't specify `--on-event` or `--sentinel-file`, the supervisor creates them automatically under:

```
$TMPDIR/avenor-stable/<supervisor_run_id>/<runtime_id>/
```

For example, with a supervisor run ID of `run_abc123` and a spawned runtime `rt_2`:

```
/var/tmp/avenor-stable/run_abc123/rt_2/events.ndjson
/var/tmp/avenor-stable/run_abc123/rt_2/sentinel.env
```

This keeps artifacts isolated and lets you inspect multiple runtime outputs without manual path management.

### Loop File Spawns

If you pass `--loop-file` alongside spawn, the supervisor routes the spawn through the loop runner instead of a single provider/session. This is identical to running `avenor run --loop-file`, except the runtime is managed by the supervisor.

Loop spawns work with the same parameters as single-prompt spawns. You may optionally provide `--prompt` or `--prompt-file` — it becomes an implicit pre-phase before the loop phases. See [loop.md](loop.md) for details on loop configuration.

Loop spawns do not return a `session_id` in the spawn result (individual phase session IDs appear in the events).

## Managing Runtimes

### Status and List

Query the supervisor's state at any time:

```bash
# Status of all runtimes
avenor control --socket /tmp/avenor-stable.sock list

# Status of a specific runtime
avenor control --socket /tmp/avenor-stable.sock status rt_1
```

Both return:

```json
{
  "runtime_id": "rt_1",
  "session_id": "ses_abc123",
  "label": "pr-review-#42",
  "dir": "/repo/branch-a",
  "status": "running",
  "exit_code": 0,
  "on_event": "/tmp/pr42-events.ndjson",
  "sentinel_file": "/tmp/pr42-done.env"
}
```

Status values:

| Status | Meaning |
|---|---|
| `idle` | Runtime exists but no session is currently active (waiting for next prompt after previous one completed) |
| `running` | Session is active (provider processing) |
| `ended` | Runtime has finished and will not accept new prompts |

Exit codes appear once the runtime has ended.

### Prompts and Interrupts

Send a follow-up prompt to a running runtime:

```bash
avenor control --socket /tmp/avenor-stable.sock prompt \
  "Check if the fix worked by running the tests again" \
  rt_1
```

The prompt is queued and executed after the current session finishes.

Interrupt the current session and inject a prompt at the front of the queue:

```bash
avenor control --socket /tmp/avenor-stable.sock interrupt-and-prompt \
  "Never mind the previous approach. Try a different strategy." \
  rt_1
```

This cancels the in-flight session and prepends the new prompt to the queue.

### Cancellation

Cancel a runtime completely:

```bash
avenor control --socket /tmp/avenor-stable.sock cancel rt_1
```

This terminates the runtime's context, finishing any in-flight session. If prompts are queued, they are discarded. The runtime ends with a stop reason of `cancelled`.

### Permission Requests

If a backend supports permission relay (most do; `opencode-http` does not), the runtime emits `permission.request` events and the supervisor caches the request options. Answer a permission request by runtime, request ID, and option ID:

```bash
avenor control --socket /tmp/avenor-stable.sock answer-permission \
  req_xyz \
  allow \
  rt_1
```

This passes an option-only answer back to the active session. If the runtime has no active session, the command fails.

Write-ins use the JSON-RPC, Core, Pi, or MCP answer API's optional `message` field. The positional `avenor control answer-permission` command does not accept a message; use one of those APIs when the pending option has `requiresMessage: true`. See [Control Protocol](control-protocol.md#answer_permission) for both request shapes.

### Event Subscription

Stream events from all child runtimes in real time:

```bash
avenor control --socket /tmp/avenor-stable.sock tail
```

Each line is a JSON object with a `runtime_id` field added by the supervisor. See [events.md](events.md) for the event schema.

## Shutdown

Gracefully shut down the supervisor and all child runtimes:

```bash
avenor control --socket /tmp/avenor-stable.sock shutdown graceful
```

Graceful shutdown cancels all child runtimes' contexts and waits up to `--shutdown-timeout` (default 10s) for them to finish. If any runtime doesn't finish in time, stderr reports how many are still running; the supervisor then exits anyway.

Immediate shutdown (kill mode):

```bash
avenor control --socket /tmp/avenor-stable.sock shutdown kill
```

This cancels all runtimes without waiting. Any in-flight sessions are terminated immediately.

## Max Runtimes Limit

The supervisor enforces a maximum number of concurrent child runtimes set by `--max-runtimes` (default 16). When this limit is reached, spawn requests fail with an error:

```
max runtimes (16) reached
```

To spawn more runtimes, you must wait for existing ones to finish. The limit prevents resource exhaustion and gives you a predictable constraint for scheduling.

## Idle Timeout

If you set `--idle-timeout` (e.g. `--idle-timeout 5m`), the supervisor exits cleanly when:

1. No child runtimes are running (all have finished or been cancelled), AND
2. The supervisor has been idle for the full duration, AND
3. No control connections are active

This is useful for dynamic environments where you spawn supervisors on-demand and want them to clean up when they're no longer needed. Without an idle timeout, the supervisor runs indefinitely until signaled.

The idle timeout resets whenever:
- A new runtime finishes spawning
- A running runtime finishes a session
- A control connection arrives

## HTTP Debug Adapter

Pass `--http-debug :8080` to bind an HTTP debug server to port 8080. This exposes:

- `GET /status` — supervisor state snapshot (mirrors `avenor control status`)
- `GET /runtimes/{runtime_id}` — per-runtime status
- `DELETE /runtimes/{runtime_id}` — cancel a runtime
- `GET /events` — subscribe to events (Server-Sent Events, streaming)
- `POST /shutdown` — trigger graceful shutdown

The HTTP adapter is useful for rapid testing, integration with HTTP-based orchestrators, or debugging in environments where Unix sockets are inconvenient.

## OpenCode HTTP Subprocess Discovery

If you spawn a runtime with `--backend opencode-http` and no `--server-url`, the supervisor automatically spawns an `opencode serve` subprocess in the target directory. This subprocess:

- Runs `opencode serve --port <free_port>` in the spawn's working directory
- Is shared across all runtimes spawned in that directory
- Stays alive as long as at least one runtime for that directory is active
- Is cleaned up during supervisor shutdown

This is convenient for interactive development — you don't have to manually start an OpenCode server, and the supervisor manages its lifecycle.

## Cross-References

- [Control Protocol](control-protocol.md) — JSON-RPC 2.0 protocol reference for all commands
- [Phase Loop](loop.md) — loop config file format and phase lifecycle
- [Backends](backends.md) — backend selection, capabilities, and configuration
- [Events](events.md) — event schema and stop reasons
- [Permission Handler](permission-handler.md) — file-based permission handler format
