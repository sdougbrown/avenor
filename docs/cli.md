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
| `--loop-file` | (none) | Path to multi-phase loop config (JSON, YAML, or TOML); enables looping mode (incompatible with `--resume`) |
| `--roster-file` | (none) | Path to a roster map (JSON, YAML, or TOML) for direct selection, or the root workflow fallback |
| `--roster-entry` | (none) | Entry key within `--roster-file` for a direct spawn; invalid with a workflow file |
| `--label` | (none) | Free-form label for log correlation; appears in events and sentinel |
| `--dir` | `.` | Working directory for the agent |
| `--resume` | (none) | Resume an existing session by ID; incompatible with `--loop-file` |
| `--server-url` | (none) | Long-lived ACP server endpoint; required for `--backend opencode-http` |
| `--backend` | `opencode-acp` | Runtime backend: `opencode-acp`, `agy`, `gemini-acp`, `cursor-acp`, `codex-app-server`, `opencode-http`, `claude-channel` |
| `--model` | (none) | Backend-specific model ID; if not set, resolved from opencode config via `--agent` |
| `--thinking` | (none) | Backend-native reasoning level; validated against the effective backend |
| `--claude-mcp-wait` | `0` (off) | Hold the first prompt of a `claude` or `claude-channel` run until this long after launch (Go duration, at most `60s`), so MCP servers Claude Code connects in the background are available to the first turn. Direct runs only; rejected with other backends, with `--loop-file`/`--team-file`, and when not shorter than `--progress-timeout`. See [backends.md](backends.md#claude) |
| `--on-event` | (none) | Path to write NDJSON event stream; events are discarded if unset. A final symlink is refused, so the event log cannot be redirected into another artifact's file |
| `--sentinel-file` | (none) | Path to write a completion sentinel (exit code, session ID, stop reason); also derives permission handler unless `--permission-handler` is set |
| `--result-file` | (none) | Path to write the run's final assistant message as plain text. Direct runs only (not with `--loop-file`/`--team-file`). Written atomically before the sentinel; the sentinel then carries a `RESULT=<path>` line |
| `--pid-file` | (none) | Path to write the run's own pid at startup, removed when it exits. Track a backgrounded run by this pid instead of a launcher's `$!` |
| `--permission-handler` | (derived) | Permission resolver: `file:<path>` for file-based answers, or omitted for socket/auto-approve only. If `--sentinel-file` is set and `--permission-handler` is not, defaults to `file:<sentinel-base>` |
| `--auto-approve` | `false` | Automatically approve ordinary permission requests. Questions that require user input still wait for an answer |
| `--control-socket` | (none) | Unix socket path for control plane; enables remote commands while session is running |
| `--http-debug` | (none) | HTTP debug adapter bind address (e.g., `127.0.0.1:8080`); enables `/debug/status` and `/debug/events` endpoints |
| `--timeout` | `0` (disabled) | Overall session timeout (Go duration: `30s`, `5m`, etc.); fires after duration regardless of progress |
| `--progress-timeout` | `0` (disabled) | Session progress timeout; fires if no event received for this duration |
| `--max-retries` | `0` | Maximum retry attempts on transient failure (exit code 1, or a rate-limited turn); emits `avenor.retry` and `avenor.error` events (see [events.md](events.md)) |
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

### Result and pid files

A caller that runs `avenor run` unattended usually needs two things the event
log makes awkward: the run's final answer and its pid.

`--result-file <path>` writes the final assistant message as plain text — the
same text the terminal `session.end` carries as `final_output` (the backend's
own final output when it supplies one, otherwise the text of the run's last
turn). The file is written atomically before the sentinel, so a reader that
waits for the sentinel never sees a partial answer, and the sentinel then ends
with `RESULT=<path>`. A run that ends with no final message writes no file and
no `RESULT=` line.

The file is written only when the run's last turn settled on the backend's
terminal event. A turn that settled with a failure still has a final message, so
a failed run writes the text it settled with; a cancelled run, a timed-out run,
and a run whose attempt or resume failed before the backend settled write no
file and no `RESULT=` line, because the reply collected so far is a prefix of an
answer, not the run's final message. SIGINT and SIGTERM both end the run at exit
code 130 with a `KILLED` sentinel carrying `STOP_REASON=cancelled`. An answer
left by an earlier run at the same path is removed just before the first prompt,
once the provider and session are up, so a run that fails before then leaves
that answer alone.

`--pid-file <path>` writes the run's own pid once the setup that mode can fail
has passed, and removes the file when the run exits, including on SIGINT and
SIGTERM. Every run mode publishes it: a direct run publishes before its first
attempt, and a `--loop-file`/`--team-file` run publishes after its workflow
config loads and validates, so a rejected config still leaves no pid file
behind. A run rejected before the pid is written, such as one whose prompt file
is unreadable or whose workflow config is invalid, never publishes a pid and
leaves another run's pid file untouched.
The signal trap is installed before the pid file is published, so a first signal
landing during setup still has a cleanup path and does not leave the pid file
behind; a second signal restores the default disposition and can end the process
before that cleanup runs, leaving the file behind. A SIGKILL or a crash skips
cleanup and leaves a stale pid file behind, and the kernel recycles pids, so
`kill -0 $(cat run.pid)` alone can report an unrelated process as live. Check
the process identity before signalling — `ps -o command= -p <pid>` works on both
Linux and macOS — or wait for the sentinel instead. Launch wrappers cannot
supply a trustworthy pid: `setsid` forks when the caller leads a process group,
so a recorded `$!` can name a shell that already exited while the run continues.
Letting the run publish its pid avoids that. The pid file is only removed by the
run that wrote it.

`--result-file`, `--pid-file`, `--sentinel-file` and `--on-event` must name
distinct paths — compared as absolute paths with existing parent directories
resolved, and by file identity when the paths already exist — and none may claim
the permission request or response file derived from the sentinel while the file
handler is active, and the result and pid paths must be single-line. An invocation rejected by flag validation writes
no result or pid file and leaves an earlier answer untouched. A rejection after
the flags are parsed still writes its sentinel when `--sentinel-file` is set, so
a supervisor can wait on it; a command line that does not parse exits 1 before
the sentinel path is known, so nothing is written. Treat the sentinel path as
the run's own output rather than a place to keep an answer, since a rejection
rewrites it like any other exit.

```sh
avenor run --prompt-file my-prompt.txt --label "data-prep" \
  --on-event /tmp/events.jsonl --sentinel-file /tmp/run.sentinel \
  --result-file /tmp/result.txt --pid-file /tmp/run.pid &

until [ -f /tmp/run.sentinel ]; do sleep 1; done
cat /tmp/result.txt
```

See [loop.md](loop.md) for multi-phase loop config.

### Roster selection

A roster is a map from a name to a complete backend/agent/model loadout. `roster_file` names the map; `roster_entry` names one key inside it. Every entry must contain `backend` and at least one of `agent` or `model`. An entry may also carry `thinking`. Roster files can be authored in JSON, YAML, or TOML — the format is detected from the file extension:

```json
{
  "planner": {
    "backend": "opencode-acp",
    "model": "provider/model"
  },
  "executor": {
    "backend": "agy",
    "agent": "windsurf-swe"
  },
  "reviewer": {
    "backend": "codex-app-server",
    "model": "provider/reviewer",
    "thinking": "high"
  }
}
```

`thinking` is a **default, not part of the identity**: an explicit `--thinking` wins over the entry's value, so a roster only supplies the level when the caller did not. An unknown level is rejected when the roster loads, with the offending entry named. Whether a level is usable at all still depends on the backend, and that check happens when the run starts. In loop and team configs, a per-phase roster entry supplies the same default for that phase: the run-level `--thinking` still wins when set, and the merged level is validated against the phase's resolved backend before the phase starts.

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

`--roster-file` and `--roster-entry` must be supplied together for a direct request. A roster request cannot also supply `--agent`, `--model`, or `--backend`; the roster entry supplies all three identity fields. A roster file is strict: `system`, `thinking`, and unknown entry fields are rejected in all formats. Run-level `--thinking` remains separate and is checked against the effective backend selected by the direct entry or by each workflow phase.

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

## avenor verify

Validate config files without starting a run. Loads and checks loop, team, and roster configs for structural correctness — format decoding, unknown-field rejection, mutual exclusions, prompt presence, roster entry references, and prompt\_file resolution. Recursively validates nested `loop_file`/`team_file` references that are only loaded on demand during a real run.

```
avenor verify [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--dir` | `.` | Working directory (relative config paths are resolved from here) |
| `--loop-file` | (none) | Path to loop config to validate |
| `--team-file` | (none) | Path to team config to validate |
| `--roster-file` | (none) | Path to roster config to validate (standalone, or as fallback for loop/team) |
| `--roster-entry` | (none) | Roster entry name to look up (requires `--roster-file`) |

At least one of `--loop-file`, `--team-file`, or `--roster-file` is required.

### Examples

Validate a loop config and its nested references:

```sh
avenor verify --loop-file loop.json
# ok: loop loop.json (3 phases, max_iterations=5)
# ok: loop inner.json (2 phases, max_iterations=2)
```

Validate a team config with a roster fallback and check a specific entry:

```sh
avenor verify --team-file team.json --roster-file roster.json --roster-entry reviewer
# ok: roster roster.json (3 entries)
# ok: roster roster.json: entry "reviewer" found
# ok: team team.json (4 phases)
```

Catch an invalid config before a run:

```sh
avenor verify --loop-file loop.yaml
# error: loop loop.yaml: decode config loop.yaml: unknown field "bogus"
```

Works with JSON, YAML, and TOML configs. Exit code 0 with `ok:` messages on success; exit code 1 with `error:` messages on failure.

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
| `--max-runtimes` | `16` | Maximum concurrent child runtimes for this supervisor (parked finished runtimes do not count) |
| `--max-tree-budget` | `64` | Maximum concurrent executing runtimes across the whole supervisor tree including nested supervisors |
| `--idle-timeout` | `0` (disabled) | Exit after this duration with no live child runtimes (parked runtimes count as live); control connections do not delay it |
| `--parked-timeout` | `30m` | Reap a finished runtime parked for follow-up after this duration (0 = park until shutdown) |
| `--lease-sweep-interval` | `10s` | Expire workflow leases whose heartbeat stopped at this cadence (0 = disable; leases expire only on restart recovery) |
| `--shutdown-timeout` | `10s` | Graceful shutdown timeout before killing children |
| `--permission-claim-timeout` | `0` (disabled) | Optional permission claim deadline; with 0, fallback occurs only after all control clients disconnect |
| `--broker-parent-url` | (none) | `http://` address of the parent broker to join. The credential comes from the `AVENOR_PARENT_BROKER_TOKEN` environment variable (required when this flag is set); a credential without a URL runs this broker as the tree's parent broker |
| `--broker-parent-token` | (none) | Explicit parent-link credential overriding `AVENOR_PARENT_BROKER_TOKEN`; prefer the env var, since argv is world-readable via `/proc/<pid>/cmdline` |

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

The stream ends when the control server shuts down, including when the server refuses the subscription because shutdown has already begun. A connection that drops before the subscription is answered exits 1.

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
| `--launch-wrapper` | Opaque argv prefix placed in front of the backend process command; repeatable, each use adds one element (e.g. `--launch-wrapper nice --launch-wrapper -n --launch-wrapper 5`). Avenor does not interpret the wrapper — see [backends.md](backends.md#launch-wrapper) |

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

## avenor await

Wait on a run until it reaches attention or done. Resolves a run by ID or label over a control socket, or by scanning `~/.avenor/sockets` when `--socket` is omitted.

```
avenor await [flags] <run-id|label>
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--socket` | (none) | Control socket path; when omitted, `avenor await` scans `~/.avenor/sockets` |
| `--until` | `attention` | Wait target: `attention` (stop at `pending_permission`) or `done` (continue to terminal state) |
| `--timeout` | `0` (disabled) | Wall-clock backstop duration; exits `124` when reached |
| `--print-output` | `false` | Print the result payload after `---` |
| `--format` | `plain` | Output format: `plain` or `json` |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Reached `done` |
| `2` | Usage, not found, or dead socket |
| `10` | Reached `failed` |
| `11` | Reached phase timeout |
| `12` | Reached `killed` |
| `20` | Reached `attention` (`pending_permission`) |
| `124` | Wall-clock timeout |

### Output

Plain format emits line-buffered transition records:

- `ATTENTION permission <runtime_id> <summary>`
- `TURN-DONE <runtime_id>`
- `END <failed|timeout|killed> <runtime_id> [reason]` (completion emits `TURN-DONE`)

With `--print-output`, the final payload is printed after `---`.
With `--format json`, the same transitions are emitted as structured records.

### Example

```sh
avenor await --socket /tmp/avenor.sock run-123 --until done --print-output
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

The permission handler expects files at `<perm-base>.req` (request) and `<perm-base>.req.response` (response). Flags may appear before or after `<perm-base>`; the examples below show them before the path. Repeating a flag on the same side of the path keeps the last value. Giving the same flag on both sides is an error, so a later flag cannot silently override an earlier one across the path.

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

## avenor workflow

Drive a [durable workflow](workflow.md) through a stable supervisor's control socket. All subcommands take `--socket` (before or after the subcommand). `claim`, `start`, and `heartbeat` are control-plane commands sent through the `workflow.command` control method, not CLI subcommands.

```
avenor workflow <command> [args] [flags]
```

### Commands

| Command | Purpose |
|------|---------|
| `create --request-file FILE` | Register a versioned template. |
| `instantiate --template-id ID --template-version V --request-file FILE` | Create an instance of a template version. |
| `status <workflow-id>` | Current workflow snapshot summary. |
| `wait <workflow-id> [--timeout 30s]` | Block until terminal or timeout. |
| `inspect <workflow-id>` | Full instance detail. |
| `events <workflow-id> [--after-seq N] [--limit N]` | Event log. |
| `complete <workflow-id> <node-id> --activation-id ID --attempt-id ID --lease-id ID --request-file FILE` | Complete a machine/external handoff activation. |
| `gate <workflow-id> <node-id> <gate-id> --activation-id ID --operation OP --request-file FILE` | Record a gate decision (`satisfy`, `reject`, `waive`, `external_result`). |
| `skip <workflow-id> <node-id> --request-file FILE` | Waive every unsatisfied required gate on a parked activation. |
| `unblock <workflow-id> <node-id> --request-file FILE` | Return a blocked activation to ready. |
| `cancel <workflow-id> --reason TEXT [--actor ID]` | Cancel the whole instance: terminal `canceled`, in-flight attempts terminated. |

### Examples

Register a template and instantiate a work unit:

```sh
avenor workflow create --socket /tmp/avenor.sock \
  --request-file templates/software-factory/work.json

echo '{"metadata":{"issue":"115"}}' > /tmp/instance.json
avenor workflow instantiate --socket /tmp/avenor.sock \
  --template-id software-factory-work --template-version 1.0.0 \
  --request-file /tmp/instance.json
```

Observe the workflow:

```sh
avenor workflow status --socket /tmp/avenor.sock wf_...
avenor workflow inspect --socket /tmp/avenor.sock wf_...
avenor workflow events --socket /tmp/avenor.sock wf_... --after-seq 0 --limit 50
```

Complete a run node with evidence and a declared outcome:

```sh
avenor workflow complete --socket /tmp/avenor.sock \
  wf_... assessment \
  --activation-id act_... --attempt-id att_... --lease-id lease_... \
  --request-file /tmp/complete.json
```

Record an external gate result bound to an exact PR head:

```sh
avenor workflow gate --socket /tmp/avenor.sock \
  wf_... review ci \
  --activation-id act_... --operation external_result \
  --request-file /tmp/ci-result.json
```

See [workflow.md](workflow.md) for the full durable-workflow reference and the `templates/software-factory/README.md` guide for a complete example.

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

The sidecar speaks JSON-RPC 2.0 over stdio, declares `claude/channel` and `claude/channel/permission` capabilities, and exposes the four tools Claude uses to communicate back:

- `avenor_report` — progress update
- `avenor_finish` — session completion with status and summary
- `avenor_message` — reply to a control message with `action="control_reply"`, plus the shared tell/ask/reply/resume/withdraw actions
- `avenor_peers` — list sessions, rename a session, or read a peer's last message

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

## avenor parent-token

Print a fresh 256-bit random parent-link credential as 64 lowercase hex characters. Use it to mint the shared credential that links a child branch supervisor's broker to its parent's broker. See [messaging.md](messaging.md#linking-a-branch) for how the token is carried and consumed.

```
avenor parent-token
```

### Flags

None.

### Example

Mint a token and hand it to both sides of a parent link:

```sh
avenor parent-token
# Output: 3f9c1a… (64 hex characters, one line)
```

Each invocation prints a different token; the value is never persisted by this command.

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
- `5` — Blocked (a phase aborted with an abort marker)
- `6` — Provider quota exhausted (`quota_exhausted`); fall back to another model or retry later
- `7` — Provider rate limit (`rate_limited`); retried when `--max-retries` > 0
- `124` — Overall session timeout (`--timeout` reached)
- `130` — Cancelled (`SIGINT`, `SIGTERM`, or context cancellation)

A provider limit failure is reported as exit code 6 or 7 instead of 1, so an
orchestrator can distinguish "out of quota, switch or retry later" from "the
agent failed" without reading the event log.

### Completion sentinel

With `--sentinel-file`, the file contains the status, session ID, and stop
reason, plus any applicable extra lines:

```
FAILED
SESSION=ses_abc123
STOP_REASON=rate_limited
EXIT_CODE=7
RETRY_AFTER=20
RUN=a3f9...
```

`RETRY_AFTER` (whole seconds) appears only when the provider stated how long to
wait. Without a hint, the line is omitted; it is never emitted empty.

See [loop.md](loop.md) for how exit codes interact with multi-phase loops.

## Environment Variables

- `AVENOR_OPENCODE_URL` — Default server URL if `--server-url` not set (only consulted for subprocess discovery)
- `AVENOR_PARENT_BROKER_TOKEN` — Parent-link credential consumed by `avenor stable` on both sides of a parent link; required with `--broker-parent-url`, and a credential without a URL runs the broker as the tree's parent broker. Prefer this over `--broker-parent-token`
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
- [workflow.md](workflow.md) — Durable workflows: templates, gates, composition, recovery
- [events.md](events.md) — Event schema and types
