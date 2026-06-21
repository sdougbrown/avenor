# Backends

Avenor talks to agents through backends. Each backend speaks a different protocol to a different runtime — OpenCode, Claude, Codex, Gemini, Cursor, or PI. Pick the one that matches what's running locally and what you need from the orchestration layer.

## Backend selection

Pass `--backend` to choose which runtime protocol to use. The default is `opencode-acp`:

```sh
avenor --backend opencode-http --server-url http://127.0.0.1:4096 --prompt "say hi"
avenor --backend opencode-acp --prompt "say hi"
avenor --backend codex-app-server --prompt "say hi"
avenor --backend gemini-acp --prompt "say hi"
avenor --backend cursor-acp --prompt "say hi"
avenor --backend pi --model anthropic/claude-sonnet-4-5 --prompt "say hi"
avenor --backend claude --prompt "say hi"
avenor --backend claude-channel --prompt "say hi"
```

Both `claude` and `claude-channel` require `claude` to be on `PATH`. `claude-channel` additionally requires `tmux` on `PATH` and uses `--dangerously-load-development-channels` to open a broker channel; `claude` requires neither.

## Capability matrix

| Capability | opencode-acp | opencode-http | codex-app-server | gemini-acp | cursor-acp | pi | claude | claude-channel |
|---|---|---|---|---|---|---|---|---|
| New sessions | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Session resume | ✓ | ✓ | ✓ | — | — | ✓ | ⚠ | ⚠ |
| Prompt execution | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓  |
| Cancel | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓  |
| Event streaming | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Permission relay | ✓ | ✗ | ✓ | ✓ | ✓ | ✓ | ⚠ | ⚠  |
| Model selection | ✓ | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ | ✓  |
| External server URL | ✗ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Subprocess discovery | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✓ |

`—` means not verified; `✗` means not supported, `⚠ ` means experimental.

Session resume for `claude` and `claude-channel` is in-memory only: a long-lived avenor process (stable mode) keeps sessions in its map and lets clients reattach by `session_id` while the session is alive. Cross-restart resume (avenor process exits, then a new one tries to pick up the same session) is not yet supported by either backend.

---

## claude

Channel-less provider for Claude Code. Starts Claude in a terminal session (PTY by default), injects prompts via PTY paste, and reads progress from the JSONL transcript file that Claude writes to `~/.claude/projects/`. No broker, no sidecar, no `.mcp.json` injection, no `--dangerously-load-development-channels`.

### Launcher

Default launcher is PTY. Set `AVENOR_CLAUDE_TERMINAL=tmux` to use tmux instead.

```sh
# PTY (default)
avenor --backend claude --prompt "say hi"

# tmux
AVENOR_CLAUDE_TERMINAL=tmux avenor --backend claude --prompt "say hi"
```

`AVENOR_CLAUDE_TERMINAL` is shared with `claude-channel`, but the defaults differ: `claude` defaults to PTY, `claude-channel` defaults to tmux.

### Requirements

- Claude Code installed and on `PATH`.
- `tmux` on `PATH` only when `AVENOR_CLAUDE_TERMINAL=tmux` is set.

### Capabilities

| Capability | Supported |
|---|---|
| New sessions | ✓ |
| Session resume | ⚠ (in-memory, while avenor is alive) |
| Prompt execution | ✓ (PTY paste) |
| Cancel | ✓ (Esc-Esc → pane-scrape idle → forced kill) |
| Event streaming | ✓ |
| Permission relay | ⚠ (pane-scrape only, experimental) |
| Model selection | ✓ (via `--model`) |
| External server URL | ✗ |
| Subprocess discovery | ✓ |

### Cancel semantics

`Cancel()` sends Esc-Esc and polls the pane for an idle prompt (up to ~3s). If the pane goes idle, `session.end stop_reason=cancelled` is emitted and the terminal is killed. If the poll times out, the terminal is killed immediately and `session.end stop_reason=cancelled_forced` is emitted.

---

## Choosing between `claude` and `claude-channel`

Use `claude` if:
- Your org policy blocks research-preview channel flags (`--dangerously-load-development-channels`).
- You don't need broker-mediated permission/control flow.
- You want PTY as the default launcher (no tmux required).

Use `claude-channel` if:
- You need structured, broker-mediated permission and control flow via `claude/channel` MCP push events.
- You want the richer event surface (`agent.channel_ready`, `agent.prompt_queued`, `agent.report`, `agent.finish`) for orchestration.

Both backends share the same terminal session model, pane-scrape idle detection, and JSONL transcript reader. The difference is whether a broker channel is opened between Avenor and the running Claude process.

---

## claude-channel (experimental)

Starts a full interactive Claude Code session in the background, controlled via
`claude/channel` MCP push events with tmux PTY lifecycle fallback. This backend requires `tmux`; it will fail at startup if `tmux` is not installed or not on `PATH`.

### How it works

1. Avenor generates a unique run ID (e.g., `47212ef3`) and writes an MCP server entry to the project's `.mcp.json` with a name like `avenor-channel-47212ef3`.
2. The MCP entry uses the avenor binary itself as the command: `avenor claude-channel --run-id <id> --token <token> --broker-url <url>`.
3. Avenor spawns Claude Code in a detached tmux session with `--dangerously-load-development-channels server:avenor-channel-47212ef3`.
4. Claude discovers the MCP server from `.mcp.json`, loads it, and the bidirectional channel opens.
5. Avenor pushes control messages (continue, cancel, permission decisions) to Claude via `claude/channel` events.
6. Claude calls `avenor_report`, `avenor_finish`, and `avenor_reply` tools to communicate back.
7. If the channel is ignored or not yet active, Avenor falls back to tmux PTY prompt injection.
8. On session exit, Avenor removes the `avenor-channel-*` entry from `.mcp.json`.

The `.mcp.json` is written to the project directory so Claude Code can discover it. Manual cleanup is available via `avenor claude-channel-cleanup --dir <project-dir>` if a session crashes without removing its entry.

### Requirements

- Claude Code v2.1.80 or later installed and on `PATH`.
- `tmux` installed and on `PATH`.
- No separate sidecar runtime needed — the avenor binary implements both the orchestrator and the MCP server.
- Research-preview channels may be blocked by org policy.
- This backend does **not** use `claude -p`; it launches a real interactive session in tmux.

### Security

- Only load trusted local channel servers.
- The broker binds to `127.0.0.1` and requires a per-run random bearer token.
- Do not use `--dangerously-skip-permissions` in environments you do not fully trust.

### Capabilities

| Capability | Supported |
|---|---|
| New sessions | ✓ |
| Session resume | ⚠ (in-memory, while avenor is alive) |
| Prompt execution | ✓ (via channel push) |
| Cancel | ✓ (channel cancel → signal → kill) |
| Event streaming | ✓ |
| Permission relay | ⚠ (experimental, not yet verified) |
| Model selection | ✓ (via `--model`) |
| External server URL | ✗ |
| Subprocess discovery | ✓ |

## opencode-acp

Avenor spawns `opencode acp --pure --log-level WARN` and owns its lifecycle for the duration of the run.

**When to use this:** You want to avoid running a long-lived OpenCode server. Avenor starts one, uses it for one orchestration job, then tears it down. Simple and isolated.

**When not to:** You're running the same orchestration multiple times and want to reuse an OpenCode session across invocations. For that, use `opencode-http` and a separate server.

```sh
avenor \
  --backend opencode-acp \
  --agent jockey \
  --model deepseek/deepseek-v4-pro \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --control-socket /tmp/avenor.sock \
  --sentinel-file /tmp/done.env
```

### URL behavior

The ACP backend does not support network transport. If you pass `--server-url` or set `AVENOR_OPENCODE_URL`, the backend will fail cleanly. Either remove the URL or switch to `opencode-http`.

---

## opencode-http

The default. Avenor talks to a running `opencode serve` instance over HTTP, with SSE for the event stream.

**When to use this:** You want to point multiple Avenor invocations at a single server. Useful for `avenor stable` setups where many sequential runs share the same OpenCode instance.

**When not to:** You're running a single orchestration job and don't want to manage a separate server process. For that, use `opencode-acp`.

### Starting the server

```sh
opencode serve --port 4096 --pure
```

### Running against it

```sh
avenor \
  --backend opencode-http \
  --server-url http://127.0.0.1:4096 \
  --agent jockey \
  --model deepseek/deepseek-v4-pro \
  --prompt "Review the changes in the current branch" \
  --on-event /tmp/avenor-http.ndjson \
  --sentinel-file /tmp/avenor-http.env
```

### Auth

If the OpenCode server requires basic auth:

```sh
--server-url http://user:pass@127.0.0.1:4096
```

### Model format

The `--model` flag uses `providerID/modelID`:

```sh
--model deepseek/deepseek-v4-pro
```

### Known differences from ACP

- **Event stream is global.** The SSE `/event` endpoint streams events for all sessions on the server. Avenor filters by session ID locally.
- **Session end detection uses HTTP semantics.** Avenor watches the message response finish status. SSE idle transitions arrive late and are not treated as terminal.
- **Working directory is per-server, not per-session.** Start `opencode serve` in the target directory. The `--dir` flag on Avenor does not work with HTTP mode — the server's working directory is the only one in play.
- **Permissions may auto-approve.** Permission request/response behavior depends on the server's configuration. If the server is running with `--pure`, permissions are relayed correctly. Otherwise the server may auto-approve based on its own allow list.

---

## codex-app-server

Avenor spawns the Codex app-server subprocess and talks to it via JSON-RPC over stdio. Manages the lifecycle directly.

**When to use this:** You're running Codex and want Avenor to orchestrate a single run. Similar to `opencode-acp` — you get subprocess management for free.

```sh
avenor \
  --backend codex-app-server \
  --prompt "Implement the changes described in the plan" \
  --dir /repo \
  --sentinel-file /tmp/done.env
```

---

## gemini-acp

Avenor spawns `gemini --acp --approval-mode default` and talks via ACP over stdio.

**Model selection caveat:** Gemini reads its model from its own CLI config at spawn time. Passing `--model` to Avenor has no effect on `gemini-acp`. Set the model in your Gemini CLI config before invoking Avenor.

```sh
avenor \
  --backend gemini-acp \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --sentinel-file /tmp/done.env
```

---

## cursor-acp

Avenor spawns `agent acp` and authenticates via `cursor_login`. Talks via ACP over stdio.

**Model selection caveat:** Like Gemini, model and configuration are managed through the `agent` binary's own config — not via session methods. Update your Cursor agent config before running Avenor if you need a different model.

```sh
avenor \
  --backend cursor-acp \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --sentinel-file /tmp/done.env
```

---

## pi

Avenor spawns `pi --mode rpc --no-session` and talks via its own JSON-RPC protocol over stdio.

**One session per instance.** This is a PI protocol constraint, not something Avenor can route around. If you try to start a second session before the first ends, the backend will error. If you need multiple concurrent sessions, run multiple Avenor invocations.

Model selection works at spawn time via `--model`:

```sh
avenor \
  --backend pi \
  --model anthropic/claude-sonnet-4-5 \
  --prompt "Implement the changes described in the plan" \
  --dir /repo \
  --control-socket /tmp/avenor.sock \
  --sentinel-file /tmp/done.env
```

### Agent profiles

When using pi with the `@dougbots/pi-agents` extension, you can select a configured agent profile with `--agent` instead of (or in addition to) `--model`:

```sh
avenor --backend pi --agent jockey --prompt "review this PR"
```

The `--agent` flag resolves the model from pi's `agents.json` (looked up via `PI_CODING_AGENT_DIR` or `~/.pi/agent/`). It also sets the `PI_AGENT` environment variable in the pi subprocess, which the `@dougbots/pi-agents` extension reads to apply the full agent profile (model, system prompt, tool restrictions, permission gates).

`--model` works fine on its own — no agent config or extension required. Use `--agent` when you want pi to load a named profile with its own configuration.

---

## Diagnosing backend connectivity

If you're not sure whether `opencode-acp` is wired up correctly — wrong binary on `PATH`, ACP not supported in your OpenCode version, subprocess dies immediately — `avenor probe` runs a minimal diagnostic session and writes the full event transcript to a file.

```sh
avenor probe --out /tmp/probe-transcript.ndjson --dir /repo --timeout 2m
```

The transcript is NDJSON in the same format as `--on-event`. Read it with `avenor watch` or inspect it directly to see what events the backend emitted before failing.

`avenor probe` only works with `opencode-acp` — it spawns `opencode acp --pure` directly and is not intended for production use.

| Flag | Default | Description |
|---|---|---|
| `--out` | required | Path to write the probe transcript |
| `--dir` | `.` | Working directory for the probe session |
| `--timeout` | `5m` | Probe timeout |
| `--prompt` | built-in | Override the default probe prompt |

---

## See also

- [events.md](events.md) — event stream format and lifecycle
- [permission-handler.md](permission-handler.md) — permission request/response JSON shapes
- [control-protocol.md](control-protocol.md) — control socket methods and ownership rules
