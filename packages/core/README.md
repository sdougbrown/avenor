# @dougbots/avenor-core

Control socket client, supervisor lifecycle, and tool implementations for the avenor agent runner.

Avenor is a CLI daemon that runs AI agent sessions. This package manages that daemon's process and provides typed wrappers around its JSON-RPC control socket — the interface used to spawn runs, check status, follow up on sessions, and shut everything down cleanly.

It is a **private** workspace package consumed by `@dougbots/avenor-mcp` and `@dougbots/avenor-opencode`. It is not published to npm.

## Binary discovery

The avenor binary is located through a three-step fallback:

1. `AVENOR_BIN` environment variable
2. `avenor` on `PATH` (via `which avenor`)
3. `~/.botfiles/bin/avenor`

The exported helper `findAvenorBinary()` runs this discovery. The `Supervisor` calls it automatically if no explicit `binaryPath` is given.

## Core concepts

### Client

`dial(socketPath)` connects to an avenor control socket and returns a `Client`. The `Client` speaks JSON-RPC over a Unix socket and exposes methods like `spawn()`, `status()`, `list()`, `shutdown()`, `answerPermission()`, `prompt()`, and `cancel()`. It also provides an async iterable `events()` for streaming runtime events.

### Supervisor

`Supervisor.get()` is a singleton that ensures one avenor `stable` process is running, bound to a per-PID control socket. It starts the binary with `avenor stable --control-socket <path> --idle-timeout 30m`, retries the socket dial until it's ready, and registers signal handlers to clean up on exit.

Spawned runs are tracked in-memory (label → runtime ID, sentinel path, event log path) so follow-ups and status checks can resolve the right runtime.

## Tools

The package exports six self-contained tool functions. Each is designed for embedding in MCP servers or CLI handlers.

### `spawnTool`

Starts a new agent run.

```ts
import { spawnTool } from '@dougbots/avenor-core'

const result = await spawnTool({
  agent: 'codex',
  prompt: 'Write a function to reverse a string',
  label: 'reverse-string',
  model: 'claude-sonnet-4',
})
// { run_id: '...', label: 'reverse-string', supervisor_id: '/path/to/socket' }
```

Accepts `agent`, `prompt`, `promptFile`, `label`, `dir`, `timeout`, `model`, `sessionId`, and `supervisorId` (to target a specific socket).

### `statusTool`

Reports status for one or all runs. When `runId` is omitted it lists every run across the supervisor.

```ts
import { statusTool } from '@dougbots/avenor-core'

const status = await statusTool({ runId: 'reverse-string' })
// { run_id: 'reverse-string', label: 'reverse-string', status: 'running', ... }
```

Status values: `running`, `done`, `failed`, `timeout`, `killed`. If a permission request is pending, the result includes `pending_permission` with available options.

### `answerPermissionTool`

Answers a pending permission request for a run. If `requestId` is omitted, it looks up the currently pending request automatically.

```ts
import { answerPermissionTool } from '@dougbots/avenor-core'

await answerPermissionTool({ runId: 'my-run', optionId: 'allow-once' })
// { ok: true }
```

### `followUpTool`

Resumes a completed run by spawning a follow-up run on the same session, preserving conversation context.

```ts
import { followUpTool } from '@dougbots/avenor-core'

await followUpTool({ runId: 'reverse-string', message: 'Now optimize it for speed' })
// { run_id: '...', label: 'reverse-string-followup' }
```

### `eventsTool`

Reads recent events from a run's on-disk event log. Optionally filters by event type and limits the count (default 50, max 1000).

```ts
import { eventsTool } from '@dougbots/avenor-core'

const events = await eventsTool({ runId: 'my-run', types: ['tool_use', 'error'], limit: 20 })
```

⚠️ Only supported through the singleton `Supervisor` — passing an explicit `supervisorId` will throw.

### `shutdownTool`

Shuts down the avenor daemon. Pass `force: true` for an immediate kill instead of graceful termination. Cleans up sentinel files and event logs for tracked runs.

```ts
import { shutdownTool } from '@dougbots/avenor-core'

const result = await shutdownTool({ force: false })
// { ok: true, cleaned_up: ['/path/to/sentinel.done', ...] }
```

## Quick start

```ts
import { Supervisor, spawnTool, statusTool, shutdownTool } from '@dougbots/avenor-core'

// 1. The supervisor auto-starts avenor. Call get() early — it's a singleton.
const sup = await Supervisor.get()

// 2. Spawn a run
const { run_id } = await spawnTool({
  agent: 'codex',
  prompt: 'Review src/app.ts for security issues',
  label: 'security-review',
})

// 3. Poll until complete
let state = await statusTool({ runId: run_id })
while (state.status === 'running' || state.status === undefined) {
  await new Promise(r => setTimeout(r, 2000))
  state = await statusTool({ runId: run_id })
}
console.log(state) // { run_id, label, status: 'done', ... }

// 4. Clean up
await shutdownTool()
```

## Exports

| Export | Kind | Description |
|---|---|---|
| `dial` | function | Connect to a control socket, returning a `Client` |
| `Client` | class | JSON-RPC client for the avenor daemon |
| `Supervisor` | class | Singleton avenor process lifecycle manager |
| `findAvenorBinary` | function | Discover the avenor binary path |
| `spawnTool` | function | Start a new agent run |
| `statusTool` | function | Query run status (single or all) |
| `answerPermissionTool` | function | Answer a pending permission request |
| `followUpTool` | function | Resume a session with a follow-up prompt |
| `eventsTool` | function | Read recent events from a run's event log |
| `shutdownTool` | function | Stop the avenor daemon and clean up |
