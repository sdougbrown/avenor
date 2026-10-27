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
| `--control-socket` `<path>` | (required) | Unix socket path for the control plane. Avenor writes a tombstone file at `<path>.dead` to signal abnormal shutdown, an owner record at `<path>.owner.json`, and a persistent lock at `<path>.control.lock` |
| `--broker-addr` | (empty) | Concrete broker bind address; empty uses ephemeral loopback. The supervisor publishes its URL in `<control-socket>.broker.url`. See [hub setup](messaging.md#running-a-hub) for network trust requirements |
| `--broker-parent-url` | `$AVENOR_PARENT_BROKER_URL` | Parent hub URL; a non-empty flag overrides the environment. Requires `AVENOR_PARENT_BROKER_TOKEN` or `--broker-parent-token` |
| `--broker-parent-token` | `$AVENOR_PARENT_BROKER_TOKEN` | Parent-link credential; prefer the environment to keep it out of process arguments |
| `--max-runtimes` | 16 | Maximum concurrent child runtimes for this supervisor. Spawn requests are rejected once this limit is hit. Runtimes parked after a successful turn do not count against this limit |
| `--max-tree-budget` | 64 | Maximum concurrent executing runtimes across the whole supervisor tree including nested supervisors. Bounds recursive fan-out |
| `--idle-timeout` | 0 | Exit cleanly after this duration with no live child runtimes (parked runtimes count as live). 0 disables (supervisor runs until signaled) |
| `--parked-timeout` | 30m | How long a finished runtime stays parked awaiting a follow-up prompt before it is reaped. Parked runtimes hold a live backend process but do not count against `--max-runtimes`. 0 parks until supervisor shutdown or cancellation |
| `--lease-sweep-interval` | 10s | How often the supervisor expires workflow leases whose heartbeat stopped (the live stall detector). An expired lease re-arms its node for a replacement claim without a restart. 0 disables the sweep: leases then expire only on supervisor restart |
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

A tombstone corroborates a past instance; it authorizes nothing. Nothing requires it before a replacement starts, and its absence is not liveness — a `SIGKILL` or OOM kill leaves none. `avenor` itself replaces a dead supervisor from the socket state alone: a credential-free dial that is refused or finds no path proves the owner is gone, and the replacement then binds under the control lock.

### Owner Record and Lock

A supervisor that binds a socket publishes its owner capability at `<control-socket>.owner.json` (mode `0600`, owned by the supervisor's euid) before it accepts a connection. Attaching clients load that record, verify the live socket's device and inode against it, bind the connection, and then authenticate; see [Control Protocol → Owner Semantics](control-protocol.md#owner-semantics).

A persistent `<control-socket>.control.lock` file serializes every change to the socket path — stale-path removal, bind, publication and cleanup. It is created or opened and never unlinked, so it survives a restart.

On shutdown the supervisor removes its own socket and record while its listener is still live and the lock is held, and leaves the lock file and the tombstone in place.

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
| `--thinking` | string | Run-level backend-native reasoning level; validated against the effective backend |
| `--backend` | string | Runtime backend (opencode-acp, opencode-http, codex-app-server, agy, gemini-acp, cursor-acp, pi). Defaults to opencode-acp in direct mode |
| `--roster-file` | string | Roster map for direct selection, or root workflow fallback |
| `--roster-entry` | string | Entry key for direct selection; invalid with `--loop-file` or `--team-file` |
| `--server-url` | string | External ACP server endpoint for opencode-http backend |
| `--on-event` | string | Path to write NDJSON events. Auto-created under `$TMPDIR/avenor-stable-<euid>/run_<supervisor_run_id>/<runtime_id>/events.ndjson` if not set |
| `--sentinel-file` | string | Path to write completion sentinel (exit code, session ID, stop reason). Auto-created under `$TMPDIR/avenor-stable-<euid>/run_<supervisor_run_id>/<runtime_id>/sentinel.env` if not set |
| `--permission-handler` | string | Permission handler (supports `file:<path>`). Auto-derived from sentinel-file if unset. Use `--auto-approve` to skip file-based handlers |
| `--auto-approve` | bool | Automatically approve ordinary permission requests without file-based handlers. Questions that require user input still wait for an answer |
| `--timeout` | int | Overall session timeout in seconds. Bounds direct, loop, and team spawns alike: every attempt, retry, and nudge stops at the shared wall-clock deadline and the runtime records `timeout` |
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

### Roster selection

A roster file is a top-level JSON map. `roster_file` is its path and `roster_entry` is the key to select. Each entry requires `backend` plus at least one of `agent` or `model`; an entry may provide both. An entry may also set `thinking`, a default reasoning level rather than part of its identity: an explicit `--thinking` or spawn-supplied level wins. Direct spawns (#208), loop and team phases (#248), and workflow node assignments (#292) apply it. Other fields, such as `system`, are not supported and strict decoding rejects them.

```sh
cat >/tmp/avenor-roster.json <<'JSON'
{
  "planner": {
    "backend": "opencode-acp",
    "model": "provider/planner"
  }
}
JSON
avenor control --socket /tmp/avenor-stable.sock spawn \
  --prompt "Analyze this repository" \
  --dir "$PWD" \
  --roster-file /tmp/avenor-roster.json \
  --roster-entry planner
```

Direct selection requires the `roster_file`/`roster_entry` pair and excludes direct `agent`, `model`, and `backend` overrides. Without roster selection, direct `agent`, `model`, and `backend` are independently optional: omitting agent and model uses the selected backend's defaults, and `--backend opencode-acp` alone is valid. In roster mode the entry's backend wins; stable does not apply its direct default backend over the entry.

A roster phase in a loop or team config is a per-phase override of the run-level backend, agent, and model. Phases without `roster_entry` retain the existing run-level selection. The command-level `--roster-file` is only a root workflow fallback; `--roster-entry` is never a workflow selector. A declared workflow `roster_file` is resolved relative to that loop/team config, while a relative command fallback is resolved relative to the selected `--dir`. Nested stable workflow behavior is unchanged: the CLI-only roster inheritance rule does not apply to stable nesting. Use a roster declaration in the nested config where stable's existing workflow contract requires one.

Run-level `--thinking` overrides a roster entry's `thinking` default and is validated after each phase's effective backend is known. A roster entry selecting `agy` rejects an explicit `--thinking low` setting because Agy does not support it.

For `resume_from_previous` or a spawn that resumes `session_id`, Avenor compares four stored session values: effective backend, agent, model, and agent profile. Any mismatch fails the resume. Matching only the backend is insufficient.

### Prompt Sources

At least one of `--prompt`, `--prompt-file`, or `--loop-file` is required (see below for loop spawns). If both `--prompt` and `--prompt-file` are set, spawn fails.

### Auto-Created Artifact Directories

If you don't specify `--on-event` or `--sentinel-file`, the supervisor creates them automatically under its per-user artifact root:

```
$TMPDIR/avenor-stable-<euid>/run_<supervisor_run_id>/<runtime_id>/
```

For example, for uid `1000`, supervisor run `692b628b253261f0` and spawned runtime `rt_2`:

```
/var/tmp/avenor-stable-1000/run_692b628b253261f0/rt_2/events.ndjson
/var/tmp/avenor-stable-1000/run_692b628b253261f0/rt_2/sentinel.env
```

The euid in the root keeps two users of a shared temporary directory in separate trees, and the root is created mode `0700`. Before writing into it or pruning it, a supervisor checks that the root is a real directory — not a symlink — owned by its own euid with mode exactly `0700`, so another user can neither read a run's artifacts nor add entries to the tree. A root that fails the check is never used: a spawn that needs a default path fails with an error naming the root and saying to remove it, or to chown and chmod it to `0700`, and pruning is skipped with one diagnostic.

A supervisor prunes only entries named `run_*`. When it starts, and then hourly while it runs, it removes a `run_<id>` directory whose newest entry is older than 24 hours and keeps a newer one. Each supervisor refreshes its own run directory before every prune, so a live supervisor's directory stays out of the candidate set even when its runtimes wrote nothing for a day, and anything under the root that is not named `run_*` is left alone. A finished runtime's sentinel and event log therefore stay readable for a status, result, or follow-up read after the turn ends. Pruning is best-effort, never follows a symlink out of the tree, and never touches a path you passed with `--on-event` or `--sentinel-file`: paths outside the artifact root are never touched.

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
| `idle` | No attempt is live: a direct run settled a turn and is waiting for the next prompt, or is in a pending retry probe. A workflow runtime reads `idle` only briefly, between its run ending and its completion being published |
| `running` | Runtime is live: an active attempt, a spawned child that has not produced a turn yet, or a workflow (loop/team) child before its first attempt and between attempts |
| `ended` | Runtime has finished and will not accept new prompts |

Exit codes appear once the runtime has ended.

Consumers such as `status` (MCP) and `avenor await` translate these raw
statuses into their own lifecycle vocabulary. A raw status a consumer does not
recognize reads as `unknown`, which is not a finished state: waiters keep
waiting until an existing completion, permission, or timeout rule applies. An
unrecognized raw status reads as `unknown` unless a completion sentinel
resolves it.

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

The supervisor enforces two independent capacity bounds:

### Local fan-out limit

`--max-runtimes` (default 16) bounds the number of concurrent child runtimes managed by a **single** supervisor. When this limit is reached, spawn requests fail with a typed, retryable capacity error:

```
max runtimes (16) reached
```

The limit prevents resource exhaustion and gives you a predictable constraint for scheduling.

A runtime that finishes its turn successfully does not keep its slot: it parks (holding its backend process open for a potential follow-up prompt) and stops counting against the limit. A parked runtime is reaped after `--parked-timeout` (default 30m). Whether a reaped runtime's session can still be continued depends on the backend: codex persists threads and the message tool's `resume` action continues them by ID, while pi and claude sessions live only in their host process, so a resume after the reap fails with an explicit error instead of silently resuming a fresh session.

### Tree descendant budget

`--max-tree-budget` (default 64) bounds the total number of concurrently **executing** runtimes across the **whole supervisor tree**, including runtimes started by nested supervisors. A child that starts its own supervisor inherits its parent's tree budget rather than receiving an unrelated fresh quota, so recursive spawning cannot multiply capacity at every level.

The tree budget is a cross-process, flock-protected admission controller backed by a root-owned file in Avenor runtime state (`~/.avenor/sockets/tree-budget-<pid>-<random>.tree-budget`). It is deliberately separate from the control socket, which callers may place in a project or worktree. The root supervisor propagates the budget path to descendants via the `AVENOR_TREE_BUDGET` environment variable so nested supervisors join the same tree; shutdown removes the root-owned file.

When the tree budget is exhausted, spawn requests fail with a typed, retryable error distinct from the local limit:

```json
{"source": "tree", "retryable": true, "limit": 64, "active": 64}
```

The `source` field distinguishes `"local"` (the per-supervisor fan-out limit) from `"tree"` (the inherited descendant budget). Callers that can wait may poll `wait_for_capacity` to be notified on a capacity change; callers that cannot wait receive the typed error and may retry.

#### Optional coordination and degraded mode

The budget is a same-user coordination primitive, not a security boundary: descendants that can access the inherited path are trusted to share it. If a root cannot create its file, or a descendant cannot join the inherited file, the supervisor continues with its local `--max-runtimes` limit. This preserves availability but no longer bounds the full supervisor tree, so it is a weaker guarantee. A warning is written to stderr and `tree_budget` reports `"mode": "degraded"` with a `reason`; an active budget reports `"mode": "active"`.

The budget counts supervisor-managed runtimes. A runtime that internally fans out to provider sessions, such as a team workflow, holds one supervisor runtime slot rather than one slot per provider process.

#### Parked runtimes and re-admission

A runtime that finishes a turn (for example, `end_turn`) parks and **releases** its tree-budget slot — a parked (resident) runtime does not consume descendant budget while idle. When a parked runtime receives a follow-up prompt, it **re-acquires** tree admission before executing; if the tree budget is full, it waits for capacity rather than bypassing the budget. The local fan-out limit still counts a parked runtime as a resident child.

#### Stale descendant recovery

Each reservation records the PID of the supervisor process that holds it. The root supervisor runs a background reaper that periodically reclaims capacity held by dead (crashed or orphaned) descendant processes and notifies capacity waiters.

#### Independent roots

Two independently launched `avenor stable` supervisors do not share a tree budget. Each root creates its own budget file and propagates only to its own descendants.

### Idempotency capacity

`--idempotency-capacity` (default 1024) bounds the supervisor's idempotency store for keyed spawns. Unexpired entries are never evicted; a caller that exhausts the store gets a typed, retryable error. Raise it for busy remote hosts. See [docs/mcp.md](mcp.md#remote-access-over-a-tailnet) for the client-side contract and `templates/remote-mcp/` in the repository for service-managed deployment.

### Inspecting tree budget status

The `tree_budget` control method reports the current tree admission state:

```bash
avenor control --socket /tmp/avenor.sock tree_budget
```

```json
{"active": 3, "capacity": 64, "root_id": "a1b2c3...", "mode": "active"}
```

A root without an active budget reports degraded mode and its cause:

```json
{"active": 0, "capacity": 0, "root_id": "", "mode": "degraded", "reason": "create tree budget: ..."}
```


## Idle Timeout

If you set `--idle-timeout` (e.g. `--idle-timeout 5m`), the supervisor exits cleanly when:

1. No child runtimes are live (none running, parked awaiting a follow-up, or waiting on a slot), AND
2. A full idle window has passed since the last runtime activity — the last spawn, or the last runtime finishing a session or being reaped

Control connections do not hold the supervisor open: an idle supervisor exits even while a client is connected.

This is useful for dynamic environments where you spawn supervisors on-demand and want them to clean up when they're no longer needed. Without an idle timeout, the supervisor runs indefinitely until signaled.

A parked runtime — a finished turn waiting for a follow-up prompt — still owns a backend process, so it counts as live and keeps the supervisor up until it is prompted or reaped.

The idle timeout resets whenever:
- A new runtime finishes spawning
- A running runtime finishes a session or is reaped

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

## Workflow Controllers

A workflow controller is an optional in-process reconciler that keeps opted-in workflow nodes moving without an agent or human claiming each one. A node opts in by declaring a `dispatch` policy (`mode: auto`, `controller_id`) in its workflow template; every node without that declaration stays manual, and ordinary spawn behavior is unaffected. See [Workflows](workflow.md) for the dispatch policy schema. The controller's desired state, leader lease, and external-gate poll cursors persist under the workflow root, so a replacement supervisor resumes without coordinator memory.

Controllers are created disabled and must be enabled before they dispatch or poll:

```bash
# Register a controller (starts disabled)
avenor workflow controller create --request-file controller.json
avenor workflow controller enable software-factory
avenor workflow ready software-factory --limit 10
avenor workflow controller status software-factory
avenor workflow controller list
avenor workflow controller disable software-factory --reason "maintenance"
```

Status and list are readable through the MCP `workflow_controller_status` tool (pass `controller_id` for a single controller, omit it to list all); create, enable, and disable are CLI and control-protocol only, via the commands above and the control protocol's `workflow.controller.*` methods (see [Control Protocol](control-protocol.md)).

Pass `--workflow-adapter-dir` to the stable command to point the supervisor at a directory of external-gate adapter manifests (default: `$XDG_CONFIG_HOME/avenor/workflow-adapters`, falling back to `~/.config/avenor/workflow-adapters`).

`avenor workflow controller status` reports:

- `desired_state` — `enabled` or `disabled`; disabled controllers stop future dispatch and polling but never cancel running attempts.
- `leader` — lease health: `is_this_process` tells you whether this supervisor's runner leads; `expires_at` is when the leader lease lapses if not renewed. A controller with no leader is idle until a process acquires the lease.
- `inflight` — the controller's non-terminal dispatch attempts, capped by `max_inflight`.
- `last_reconcile` — the latest reconcile pass of the leading runner.
- `capacity_blocked` — present when dispatch is suppressed by admission capacity (`local` runtime slots or `tree` budget), with a detail string.
- `next_poll_at` — the next scheduled external-gate poll time.

The host tools render these fields in a compact controller view (Pi render hooks and OpenCode prose) that appears only when controllers exist on the supervisor; with no controller support the tools report `method not found` and nothing else changes.

See [the workflow controller example](workflow-controller.md) for the full walkthrough: registering adapters, inspecting decisions, the human checkpoints the controller never touches, and recovery on a fresh supervisor.

## Cross-References

- [Workflow Controller Example](workflow-controller.md) — end-to-end optional-controller walkthrough
- [Control Protocol](control-protocol.md) — JSON-RPC 2.0 protocol reference for all commands
- [Phase Loop](loop.md) — loop config file format and phase lifecycle
- [Backends](backends.md) — backend selection, capabilities, and configuration
- [Events](events.md) — event schema and stop reasons
- [Permission Handler](permission-handler.md) — file-based permission handler format
