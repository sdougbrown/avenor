# @dougbots/avenor-pi

Pi extension that registers avenor agent runs as tools in your Pi session. When you need a sub-agent to handle a well-defined task — write code, run tests, search a codebase — avenor operates in the background while you keep your session focused.

## Prerequisites

You need the `avenor` binary available. It is installed to `~/.botfiles/bin/avenor` by default.

```bash
# Check it's on PATH
which avenor

# Or set explicitly
export AVENOR_BIN=~/.botfiles/bin/avenor
```

You also need Pi 1.4.0 or newer (`pi --version`). Busy-session delivery depends on
the `agent_before_settle` hook, so a notice raised while you were busy needs that
version to reach you. A missing notification is not a lost run.

## Installation

### From npm (once published)

```bash
pi install npm:@dougbots/avenor-pi
```

This writes to your global settings (`~/.pi/agent/settings.json`). Use `-l` for project-local installation (`.pi/settings.json`).

### From a git repo

```bash
pi install git:github.com/sdougbrown/avenor@v0.1.0
```

Pi will clone the repo and load the extension from `packages/pi`.

### Local development

**Quick test** — run pi with the extension directly:

```bash
cd packages/pi
pi -e ./src/index.ts
```

**Symlink for auto-discovery** — link into your global extensions directory:

```bash
mkdir -p ~/.pi/agent/extensions/avenor
ln -sf $(pwd)/packages/pi/src/index.ts ~/.pi/agent/extensions/avenor/index.ts
```

Pi auto-discovers `~/.pi/agent/extensions/*/index.ts`. Changes are hot-reloadable with `/reload`.

**Local path in settings** — add the built dist directly:

```bash
# Build first
cd packages/pi && bun run build

# Add to settings
pi install /path/to/avenor/packages/pi
```

### Manual settings.json

If you prefer to edit settings directly:

```json
// ~/.pi/agent/settings.json
{
  "packages": ["npm:@dougbots/avenor-pi"]
}
```

Or for a local path:

```json
{
  "packages": ["/path/to/avenor/packages/pi"]
}
```

## Package Structure

```
packages/pi/
├── package.json
├── tsdown.config.ts
├── src/
│   ├── index.ts          # main extension, tool registration, commands, and hooks
│   ├── render.ts         # host-only tool-call and result presentation
│   ├── types.ts          # shared types and status emoji mapping
│   └── watch.ts          # EventFeedOverlay TUI component
└── dist/
    └── index.js          # built output (loaded by pi)
```

The `pi` key in `package.json` declares the extension entry point:

```json
{
  "pi": {
    "extensions": ["./dist/index.js"]
  }
}
```

## Features

### Tools

Available tools for LLM sub-agent management:

| Tool | Description |
|---|---|
| `avenor_spawn` | Dispatch an agent run (blocking or fire-and-forget). Uses the `pi` backend by default in direct mode; accepts optional `agent`, `model`, `prompt`, `dir`, `roster_file`, and `roster_entry`. In host mode, spawns to this process's singleton supervisor (the default) receive a parent broker note for `avenor_message`. Non-local and nested spawns need an address supplied in the brief; their asks are not forwarded to the parent. |
| `avenor_status` | Get status of a run or all runs; use `view="lifecycle"` for compact progress and permission checks |
| `avenor_result` | Wait for a run and return its complete final output without transcript details |
| `avenor_inspect` | Review a bounded transcript, tool activity, permissions, and final output |
| `avenor_answer_permission` | Answer a pending permission request |
| `avenor_message` | One messaging tool with an explicit `action`: `tell`, `ask`, `reply`, `resume`, `withdraw` or `control_reply`. Every result states the outcome and, for a failed ask, whether it can still be withdrawn. |
| `avenor_peers` | One peers tool with an explicit `action`: `list` (the default), `rename` or `last`. `last` returns the target's last agent message with its stop reason, or the empty result when the target has no completed turn yet. |
| `avenor_events` | Read events from a run |
| `avenor_kill` | Kill a run |
| `avenor_workflow_status`, `avenor_workflow_wait`, `avenor_workflow_inspect`, `avenor_workflow_events`, `avenor_workflow_complete`, `avenor_workflow_gate`, `avenor_workflow_controller_status` | Inspect and drive workflow instances |
| `avenor_workflow_cancel` | Cancel a workflow instance (`workflow_id`, `actor`, `reason`) |
| `avenor_shutdown` | Shut down the avenor supervisor |

### Direct and roster spawns

Direct mode permits `agent`, `model`, and `backend` to be omitted independently, including a backend-only request; omitted values use runtime defaults. Pi supplies `backend: "pi"` for direct tool calls when no backend is given. Roster mode uses both `roster_file` and `roster_entry`, and leaves backend selection to the roster entry instead of applying Pi's direct default:

```json
{
  "prompt": "Review the repository",
  "roster_file": "/repo/roster.json",
  "roster_entry": "reviewer"
}
```

Each roster entry must contain `backend` and at least one of `agent` or `model`. Do not combine roster mode with direct `agent`, `model`, or `backend` overrides. Roster entries currently do not support `system` or `thinking`; `thinking` remains a run-level option and is validated against the effective backend. A resumed session must stay on its original backend. Use an absolute `roster_file` path when the Pi process and target project have different working directories.

### Pi-specific features

Beyond the tools, the extension integrates with Pi's TUI and event system:

- **Status widget** — persistent widget showing all active runs with status, phase, and permission state
- **Footer status** — the `avenor` status key carries active runs, the live agent-tree total, the unread notice count, and the current poll-failure count (see [Completion notices](#completion-notices))
- **Completion notices** — one notice per finished run, delivered immediately when your session is idle and batched into a single continuation when it was busy
- **Polling diagnostics** — bounded polling errors are emitted on `avenor:poll:error` for companion extensions; a completion-notice delivery that exhausts its retries is emitted separately on `avenor:delivery:error`
- **Live progress** — blocking `avenor_spawn` calls stream progress updates via `onUpdate`
- **Context enrichment** — active sub-agents are automatically surfaced in the system prompt via `before_agent_start`
- **Custom rendering** — The Pi extension renders all Avenor tool calls and results as themed, bounded summaries. Results have collapsed and expanded views.
- **Commands** — interactive commands for run management:
  - `/avenor-status` — show status of all runs
  - `/avenor-errors` — show and clear recent polling errors
  - `/avenor-watch <run_id>` — open a live event feed overlay
  - `/avenor-cancel <run_id>` — cancel a running sub-agent

### Completion notices

A fire-and-forget spawn (`wait: false`) promises one thing: you hear about the
run when it ends. Each finished run produces one notice — label, terminal
status, bounded output preview — and where it lands depends on your session at
that moment.

- **Idle.** The notice is sent as a user message and starts its own turn. It
  counts as unread until Pi confirms the message landed, and while compaction is
  running the send waits for it to finish. A send that is accepted but never
  confirmed is sent at most three times; once that budget is spent the notice
  stays unread for the next settlement and the failure is emitted on
  `avenor:delivery:error`.
- **Busy.** The notice is queued and the footer shows `unread:N`. The next
  settlement that completes normally appends each queued notice as an
  `avenor-completion` custom message under a single continuation, so three runs
  finishing while you work mean three messages and one wake-up. A settlement
  that ends in abort or error delivers nothing; the notices wait.

Every terminal status queues a notice — `done`, `failed`, `timeout`, `killed`.

`avenor_result` and a blocking `avenor_spawn` (`wait: true`) own delivery while
they run; `avenor_result` takes the label you spawned with, so you need not keep
the id. A `ready` result consumes the run: the queued notice is dropped,
`unread:N` falls by one, and nothing arrives for it later. A result that is not
`ready` — interrupted by a sibling's stopping point, timed out, or reporting a
pending permission — consumes nothing, and its notice arrives on the normal
schedule. A blocking spawn that reaches completion returns the result inline and
removes the notice; the footer can show a transient `unread:N` for that run while
the wait is in flight.

#### Footer status

One string under the status key `avenor`, in this order:

| Token | Meaning |
|---|---|
| `🟢 label — running [phase] 🔒 +2` | one per non-terminal run: status emoji, label, status, phase label, `🔒` for a pending permission, `+N` for that run's active descendants |
| `total:N` | direct active runs plus discovered active descendants; present only when active descendants exist |
| `unread:N` | notices not yet delivered: queued for a settlement, or in flight for an idle session |
| `errors:N` | recent poll failures; cleared by a cycle that adds no new error |

Missing `unread:N` or `errors:N` means zero. Without `total:N`, count the active run lines. `unread:N` counts notices,
not runs, and can stay set after the last run exits — an otherwise idle session
can still advertise `unread:1` until you consume or receive that notice.
`total:N` tracks live runs and disappears with them. `errors:N` outlives them
too: it clears when a later poll cycle adds no new error, so it counts recent
failures rather than a consecutive streak. The bounded history behind
`/avenor-errors` is longer than `errors:N`; running that command clears both.

### Tool-result channels

Pi returns JSON model content in `content[0].text` and structured `details` for
all tools except `avenor_spawn`. The renderer creates themed, bounded summaries
for display. It does not modify the underlying model content or the `details`
object.

Pi's rendered summaries sanitize and bound the output, events, and snapshot rows
they display. Use `avenor_result`, `avenor_inspect`, or `avenor_events` for the
corresponding data.

MCP clients render their own results.

### Agent profiles

The `avenor_spawn` tool accepts an optional `agent` parameter that maps to a named profile in pi's `agents.json`:

```json
{
  "jockey": {
    "model": "anthropic/claude-sonnet-4",
    "systemPrompt": "You are a PR reviewer..."
  }
}
```

When `agent` is set, avenor passes `PI_AGENT=<name>` to the pi subprocess. The `@dougbots/pi-agents` extension reads this to load the corresponding profile (model, system prompt, tools, permissions) from `agents.json` (located at `PI_CODING_AGENT_DIR` or `~/.pi/agent/`).

If you don't need a named profile, `model` alone is sufficient — no agent config or extension setup required.

When `pi-agents` has selected a session-scoped `/agent-profile`, this extension
also forwards that optional session metadata to Pi automatically. Avenor does
not depend on `pi-agents`; absent or malformed metadata is ignored. The setting
is not an `avenor_spawn` parameter and applies only to Pi-backed child runs.

## Typical workflows

**Blocking (default):**
```
1. avenor_spawn            →  tool call shows live progress, blocks until done
2. tool call returns       →  completion preview with status + session_id
3. avenor_result           →  retrieve the complete final output when needed
4. avenor_inspect          →  inspect transcript and tool details when needed
5. avenor_message           →  action "resume" to optionally iterate
6. avenor_shutdown         →  clean up when finished
```

**Parallel / fire-and-forget (`wait=false`):**
```
1. avenor_spawn × N        →  each returns run_id immediately
2. (status widget updates) →  persistent widget shows all active runs
3. avenor_status           →  optional compact lifecycle/permission check
4. avenor_result           →  wait for and retrieve each final output
5. /avenor-watch <id>      →  open live diagnostics for a specific run
```

## Dependencies

- **Peer:** `@earendil-works/pi-coding-agent` (Pi runtime, provided by pi)
- **Peer:** `@earendil-works/pi-tui` (TUI components, provided by pi)
- **Dependency:** `@dougbots/avenor-core` (supervisor, client, tool primitives)
- **Binary:** `avenor` must be available on `PATH` or at `AVENOR_BIN`
