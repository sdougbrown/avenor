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
│   ├── index.ts          # main extension (tools, commands, hooks, rendering)
│   ├── types.ts          # shared types, status emoji mapping
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
| `avenor_spawn` | Dispatch an agent run (blocking or fire-and-forget). Accepts `agent` (optional, maps to a named profile in `agents.json`), `model`, `prompt`, and `dir`. |
| `avenor_status` | Get status of a run or all runs; use `view="lifecycle"` for compact progress and permission checks |
| `avenor_result` | Wait for a run and return its complete final output without transcript details |
| `avenor_inspect` | Review a bounded transcript, tool activity, permissions, and final output |
| `avenor_answer_permission` | Answer a pending permission request |
| `avenor_follow_up` | Resume a completed run with a follow-up message |
| `avenor_events` | Read events from a run |
| `avenor_shutdown` | Shut down the avenor supervisor |

### Pi-specific features

Beyond the tools, the extension integrates with Pi's TUI and event system:

- **Status widget** — persistent widget showing all active runs with status, phase, and permission state
- **Footer status** — active runs shown in the Pi footer bar
- **Live progress** — blocking `avenor_spawn` calls stream progress updates via `onUpdate`
- **Context enrichment** — active sub-agents are automatically surfaced in the system prompt via `before_agent_start`
- **Custom rendering** — tool calls and results are rendered with status emojis and color-coded output
- **Commands** — interactive commands for run management:
  - `/avenor-status` — show status of all runs
  - `/avenor-watch <run_id>` — open a live event feed overlay
  - `/avenor-cancel <run_id>` — cancel a running sub-agent

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

## Typical workflows

**Blocking (default):**
```
1. avenor_spawn            →  tool call shows live progress, blocks until done
2. tool call returns       →  completion preview with status + session_id
3. avenor_result           →  retrieve the complete final output when needed
4. avenor_inspect          →  inspect transcript and tool details when needed
5. avenor_follow_up        →  optionally iterate
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
