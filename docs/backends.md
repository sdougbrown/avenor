# Backends

Avenor talks to agents through backends. Each backend speaks a different protocol to a different runtime — OpenCode, Claude, Codex, Gemini, Cursor, Pi, agy, or the in-process Pony test backend. Pick the one that matches what's running locally and what you need from the orchestration layer.

## Backend selection

Pass `--backend` to choose which runtime protocol to use. The default is `opencode-acp`:

```sh
avenor --backend opencode-http --server-url http://127.0.0.1:4096 --prompt "say hi"
avenor --backend opencode-acp --prompt "say hi"
avenor --backend codex-app-server --prompt "say hi"
avenor --backend agy --model gemini-2.5-flash --prompt "say hi"
avenor --backend gemini-acp --prompt "say hi"
avenor --backend cursor-acp --prompt "say hi"
avenor --backend pi --model anthropic/claude-sonnet-4-5 --prompt "say hi"
avenor --backend claude --prompt "say hi"
```

`claude` requires `claude` on `PATH`, and `tmux` on `PATH` only when `AVENOR_CLAUDE_TERMINAL=tmux`.

## Launch wrapper

Spawns accept an optional **launch wrapper** — an opaque argv prefix placed in front of the backend process command at launch:

```sh
avenor control --socket /tmp/avenor.sock spawn \
  --prompt "find bugs" \
  --launch-wrapper nice --launch-wrapper -n --launch-wrapper 5
```

The wrapper applies to the backend process only. Version probes (`claude --version`, `pi --help`, `agy --version`) and helper processes (tmux, the shell tool, lsof) are never wrapped. Roster entries do not carry wrappers — they select backend/agent/model identity only, so the wrapper is a per-spawn parameter. Inside a loop or team, every phase inherits the parent run's wrapper; phases cannot override it.

The recorded PID for a wrapped backend is the wrapper's PID: the wrapper is the direct child of the launcher and owns the backend process tree.

Ownership is recorded per session: resuming with a different wrapper is rejected exactly like a backend, agent, or model mismatch. An omitted wrapper on resume inherits the session's authoritative wrapper.

The launch wrapper does not change the control socket's trust boundary: a caller that can present the owner capability could already choose backend commands. The wrapper is an additional capability for trusted callers, not a security feature.

### MCP surfaces: operator allowlist

The launch wrapper on MCP-driven spawns (`spawn` tool) is **disabled by default** and gated by an operator allowlist. Start the supervisor with:

```sh
avenor stable --control-socket /tmp/avenor.sock --mcp-launch-wrapper-policy /etc/avenor/launch-wrapper-policy.json
```

The policy file is strict JSON:

```json
{"allowed_argv": [["/usr/bin/nice", "-n", "5"]]}
```

Unknown fields, trailing data, an empty allowlist, empty entries, empty elements, NUL bytes, and non-absolute executables are rejected at supervisor startup (`mcp launch wrapper policy: <reason>`). An MCP spawn carrying a wrapper must **exactly match** one `allowed_argv` entry — same executable (absolute path), same flags, same order, no extra or missing arguments. A wrapper that is not allowed is rejected with `launch_wrapper: argv is not allowed by this supervisor` before any process is created; a supervisor with no policy rejects every wrapped MCP spawn with `launch_wrapper: disabled for MCP on this supervisor`. MCP servers also fail closed against older supervisors that predate the gated `spawn_mcp` verb.

The policy path is picked up in this order, for both `avenor stable` and `avenor mcp`:

1. `--mcp-launch-wrapper-policy <path>` (wins),
2. `$AVENOR_MCP_LAUNCH_WRAPPER_POLICY`,
3. `<config home>/launch-wrapper-policy.json` — the shared avenor config home (`$XDG_CONFIG_HOME/avenor`, else `~/.config/avenor`), the same root the host-owned trusted allowlists use,
4. none of the above: the policy is disabled.

A path named by the flag or the environment must exist and load; a missing or malformed file is an error. The well-known file is optional — absent means disabled — but a malformed one is an error too, never a silent fallback to disabled. Creating that file is the durable, machine-local way to enable the allowlist, so a plugin's versioned `.mcp.json` never has to carry a machine-specific argv.

Approved wrappers are **trusted code with supervisor privileges**: a wrapper process receives the backend environment, including provider API keys and per-run broker credentials. The allowlist is an exposure control on which wrappers MCP clients may invoke — it is **not** sandboxing, and approving a wrapper accepts that residual risk.

The `avenor mcp` command accepts `--mcp-launch-wrapper-policy` only to forward the policy to a supervisor it autostarts; it resolves the same flag → environment → `<config home>/launch-wrapper-policy.json` chain and forwards the path it picked. Attaching to an already-running supervisor never changes that supervisor's policy — that supervisor's own pickup chain decided it.

The CLI (`avenor control spawn`) and ordinary control-socket `spawn` calls are operator-trusted surfaces: they are **not** allowlist-gated and keep the pre-existing behavior above.

### agy RPC host

The launch wrapper is not supported for the agy RPC transport (PTY RPC host). A spawn that combines a wrapper with the RPC host path fails explicitly with `launch_wrapper: not supported for agy rpc host`.

The wrapper is documented, not enforced. Whatever argv you supply must satisfy these requirements — avenor does not check them:

- **exec semantics** — stdio passes through (Avenor speaks the backend protocol over pipes), the backend's exit code must propagate as the wrapper's exit code, signals sent to the wrapper must reach the backend, and child processes must be torn down when the backend dies.
- **Same-path mounts** — `--dir` and the paths Avenor injects (config files, event/sentinel files, permission-handler directories) must resolve identically inside the wrapper.
- **MCP-server reachability** — stdio MCP servers launched by the backend inherit the wrapper's environment automatically; socket or HTTP MCP servers must be exposed by the wrapper (port forwarding, network namespace sharing, or equivalent).

Workflow templates declare their own named `launch_wrappers` registry — a trusted, author-reviewed set of executable configurations that `run`, `loop`, and `team` nodes select by ID. That is distinct from the per-spawn backend wrapper above and from the ad-hoc MCP allowlist. See [Launch wrappers](workflow.md#launch-wrappers).

## Launch identity

A backend that starts a harness process exports Avenor's launch identity in that process's environment. A local MCP server started by the backend can read it. The variable table, omission rules, attempt numbering and join recipe are in [Event stream → Launch identity](events.md#launch-identity).

| Backend | Harness child receives identity |
|---|---|
| `pi` | ✓ RPC subprocess |
| `opencode-acp`, `gemini-acp`, `cursor-acp` | ✓ ACP subprocess |
| `codex-app-server` | ✓ app-server subprocess |
| `agy` | ✓ headless print process, and RPC host when the RPC transport is active |
| `claude` | ✓ PTY or tmux pane |
| `opencode-http` | ✗ no child process |
| `pony` | ✗ in-process |

- `opencode-http` attaches to an external `opencode serve` instance. Avenor starts no backend process, so it cannot inject a per-session environment. MCP servers loaded by that server do not see the identity. A successful attach also leaves the Avenor process environment unchanged.
- `pony` runs in-process. It has no harness child.
- With the tmux launcher, Avenor removes inherited identity and exports the launch values inside the pane command, except the values a launch provisions for a broker peer: those go into a mode-0600 file in a mode-0700 directory the launch owns, which the pane command sources (`unset …; . <file>; exec claude`) and which the launcher removes when the session is torn down. A `tmux new-session` command string is argv, readable by any local process through `ps` or `/proc/<pid>/cmdline`, so no provisioned value may appear in it; the file's path may. The tmux server environment keeps its values.

Each spawning backend removes inherited identity before it adds the current launch values, and the same scrub removes the broker peer credentials (`AVENOR_BROKER_URL`, `AVENOR_RUN_ID`, `AVENOR_BROKER_TOKEN`) and the retired `AVENOR_SESSION_NAME`. A backend therefore cannot pass a launcher's peer credential down by forgetting a call: the strip list is `runtime.ScrubbedEnvNames`, applied by `runtime.LaunchEnv`, and the terminal launchers unset the same list in the pane command. Other variables keep their values, including `AVENOR_TREE_BUDGET` and backend selectors such as `AVENOR_CLAUDE_TERMINAL` and `AVENOR_AGY_TRANSPORT`.

`pi` also removes inherited `PI_AGENT` and `PI_AGENT_PROFILE`. It then adds the agent, the agent profile and the broker credentials of the current launch only. The `pi` agent process receives a broker value only when its own launch supplies all three, so an inherited or half-supplied credential never reaches it. The short `pi --help` capability probe runs with the inherited environment.

A `claude` child gets a one-shot attach credential in `AVENOR_BROKER_TOKEN` instead of the run's own: its plugin mods layer trades it at the broker's `POST /attach` for the run credential at session start, and the broker spends it there, so the run has one inbound consumer. A `pi` child gets the run's credential, because its extension authenticates each request itself. `runtime.ProvisionPeer` is the one place that decides whether a launch provisions a peer at all, and it appends the provisioned values after the shared scrub; no backend keeps an inherited copy.

A prompt to an already-running process cannot change that process's environment. A fresh start or resume that starts a process receives the identity of that request. A backend that reuses a live process keeps the identity of the original launch. The `agy` print continuation inside one attempt keeps that attempt's identity.

A launch wrapper receives the environment and passes it to the backend process. A wrapper that drops, filters or rewrites variables hides the identity from the backend's children. Avenor does not verify wrapper behavior.

Avenor's identity variables are informational and forgeable. They are never credentials and never authorization inputs. The broker variables are separate: `AVENOR_RUN_ID` is broker run identity, not the containing execution, and the credential a launch provisions (`AVENOR_BROKER_TOKEN`) is a capability — the run's token for `pi`, a one-shot attach credential for `claude` — which is never rendered into an argv.

A nested loop or team inside one CLI run shares the CLI run ID. `AVENOR_KIND` names the inner runner. Repeated nested phase names with the same iteration produce identical tuples, so the join is ambiguous. Avenor does not generate a scope ID.

## Capability matrix

| Capability | opencode-acp | opencode-http | codex-app-server | agy | gemini-acp | cursor-acp | pi | claude | pony |
|---|---|---|---|---|---|---|---|---|---|
| New sessions | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Session resume | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | ⚠ | ✗ |
| Prompt execution | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Cancel | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Event streaming | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Permission relay | ✓ | ✗ | ✓ | ⚠ | ✓ | ✓ | ✓ | ⚠ | ⚠ |
| Model selection | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ | ✓ |
| Thinking control | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ | ✓ | ⚠ | ✗ |
| External server URL | ✗ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Subprocess discovery | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✗ |

`—` means not verified; `✗` means not supported, `⚠ ` means experimental. `agy` permission relay requires the explicit RPC transport.

Thinking control is native for Codex and Pi. Claude supports five startup values but no explicit resume value. Every other backend rejects explicit thinking. See [Thinking level](thinking-level.md) for values, version scope, validation order, and resume behavior.

Session resume for `claude` is in-memory only: a long-lived avenor process (stable mode) keeps sessions in its map and lets clients reattach by `session_id` while the session is alive. Cross-restart resume (avenor process exits, then a new one tries to pick up the same session) is not yet supported by this backend.

---

## claude

Channel-less provider for Claude Code. Starts Claude in a terminal session (PTY by default), injects prompts via PTY paste, and reads progress from the JSONL transcript file that Claude writes to `~/.claude/projects/`. No broker, no sidecar, no `.mcp.json` injection.

### Launcher

Default launcher is PTY. Set `AVENOR_CLAUDE_TERMINAL=tmux` to use tmux instead.

```sh
# PTY (default)
avenor --backend claude --prompt "say hi"

# tmux
AVENOR_CLAUDE_TERMINAL=tmux avenor --backend claude --prompt "say hi"
```

`AVENOR_CLAUDE_TERMINAL` selects the launcher; `claude` defaults to PTY.

### Status reports (OSC 7501)

On the PTY launcher Avenor answers Claude Code's OSC 7501 (Program Status Protocol) detection query, so Claude reports its own state: `idle`, `working`, `blocked` (on a permission, question, or authentication), and `done`. Without the answer Claude reports nothing, and Avenor ignores any report that arrives before it answered. After the first report Avenor uses the reports alongside the pane and transcript:

- **Readiness.** The prompt waits for the input box and for Claude's root `idle` (startup) or `done` (after a completed turn) report. If no detection query arrives within about 2s of the input box appearing, or neither report within about 3s, the input box alone decides. `agent.prompt_submitted` carries `ready_signal` (`osc7501` or `pane`).
- **MCP startup wait.** Claude Code connects MCP servers, including claude.ai connectors, in the background after the input box is ready, so a first prompt sent on readiness can start its turn without their tools. `--claude-mcp-wait <duration>` (at most `60s`) holds the first prompt until that long after launch; time spent reaching readiness counts toward it, later prompts never wait, and the wait runs before the prompt is armed so startup reports are not attributed to the turn. It is off by default because Claude Code reports no MCP-settled signal and claude.ai connectors come from an account API fetch, so Avenor cannot tell locally that no server is configured and a default wait would delay every run. Direct runs only: roster entries do not carry it yet.
- **Status.** Each transition emits one `agent.status` with `signal=osc7501` (see [Events](events.md#claude-backend-events)); the 500ms pane heartbeat and the transcript `working` events stop. A `working` report also ends the Enter resend.
- **Permissions.** A blocked-on-permission report brokers the dialog as soon as two consecutive screen reads show it fully painted, without waiting for the next pane scan. Each dialog is brokered once: after it is answered, the screen still shows it until Claude moves on, and it is not brokered again while Claude reports the same blocked state. A blocked report with a new message is a new dialog. The pending request survives a pane scan that shows the input box while Claude still reports blocked.
- **Completion.** The transcript `end_turn` still ends the turn. While Claude reports the turn working, an `end_turn` (from a background subagent) is held until Claude reports `done`, `idle`, `clear` or `error`, the terminal resets, or reports are lost. A later `working` report before any `done` holds it again. A hold lasts at most 10 minutes.
- **Cancel.** Claude reporting `idle` after the turn started working counts as a completed cancel, alongside the pane going idle.

Reports are untrusted: they never approve anything, and report text is display-only. Set `AVENOR_CLAUDE_OSC7501=off` (or `0`, `false`) to leave the query unanswered; the session then behaves exactly as with scraping alone (`ready_signal=pane`, no `signal=osc7501` events). The tmux launcher never answers the query and keeps scraping. The MCP startup wait applies to both launchers.

### Requirements

- Claude Code installed and on `PATH`.
- `tmux` on `PATH` only when `AVENOR_CLAUDE_TERMINAL=tmux` is set.

### Capabilities

| Capability | Supported |
|---|---|
| New sessions | ✓ |
| Session resume | ⚠ (in-memory, while avenor is alive) |
| Prompt execution | ✓ (PTY paste) |
| Cancel | ✓ (Esc-Esc → reported or pane-scrape idle → forced kill) |
| Event streaming | ✓ |
| Permission relay | ⚠ (pane-scrape only, experimental) |
| Model selection | ✓ (via `--model`) |
| External server URL | ✗ |
| Subprocess discovery | ✓ |

### Cancel semantics

`Cancel()` sends Esc-Esc and polls the pane for an idle prompt (up to ~3s). If the pane goes idle, or Claude reports `idle` after the turn started working, `session.end stop_reason=cancelled` is emitted and the terminal is killed. If the poll times out, the terminal is killed immediately and `session.end stop_reason=cancelled_forced` is emitted.

---

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

### OpenCode version support

OpenCode 1.15.x–1.17.x report a turn aborted by a configured permission deny (for example `permission.doom_loop: deny`) as a clean `end_turn` — the ACP layer drops the assistant error. Avenor works around this by classifying denial-shaped turns (a tool call aborted with the `interrupted` footprint, or a turn ending `end_turn` right after an explicit `reject`) as the `tool_denied` stop reason instead of success. OpenCode 1.18.0+ fixed the upstream bug and reports the denial as a failed prompt; Avenor's classification stays on as defense in depth for those versions.

The pre-1.18 workaround is a deprecated compatibility shim, expected to be removed on **2027-07-14** (one year after the 1.18.0 fix). Runs on OpenCode 1.15.x–1.17.x that rely on a configured deny will silently report success again after removal — upgrade to 1.18.0+.

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

## agy

A headless `agy` run works until the agent needs an answer from outside the process. Tool approval, user questions, and mid-turn cancellation all need a control path that stdout JSONL cannot provide.

Avenor gives the dedicated `agy` backend three transport policies. None of them treats `agy` as ACP. The interactive path also does not scrape the terminal or send TUI keystrokes.

### Headless is the default

With `AVENOR_AGY_TRANSPORT` unset or set to `headless`, Avenor runs `agy` in stream-JSON print mode and translates its stdout into canonical events. This is the least surprising choice for unattended text work.

```sh
avenor \
  --backend agy \
  --model gemini-2.5-flash \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --sentinel-file /tmp/agy-headless.env
```

Headless prompts use `--print-timeout 24h`, which keeps `agy`'s shorter print timeout from ending a run before Avenor does. The ceiling is still a ceiling: a single headless prompt cannot run longer than 24 hours.

Headless mode cannot relay interactive permission decisions. `agy` denies tools that require confirmation, although its persisted allow rules still apply. Use explicit RPC mode when the run must stop and ask.

### RPC carries the conversation

Set `AVENOR_AGY_TRANSPORT=rpc` in the environment of the Avenor process. For a stable, that means the environment of `avenor stable`, not an individual spawn request.

```sh
AVENOR_AGY_TRANSPORT=rpc avenor \
  --backend agy \
  --model gemini-2.5-flash \
  --prompt "Reply with a one-line status update" \
  --dir /repo \
  --control-socket /tmp/avenor-agy.sock \
  --sentinel-file /tmp/agy-rpc.env
```

RPC mode hosts interactive `agy` in a PTY, but the PTY owns process lifetime only. Turns, events, permissions, questions, cancellation, and recovery use the loopback RPC connection. Model selection is exact: `--model` must contain a nonempty slug advertised by `agy`, and Avenor will not substitute another model.

RPC accepts stable semantic versions in major version 1, including newer minor and patch releases. Malformed versions, prereleases, and different major versions are rejected.

Permission choices and questions arrive as `permission.request` events. Questions that require user input do not go through generic auto-approval. An option marked `requiresMessage` needs a nonempty write-in message; see [Control Protocol](control-protocol.md#answer_permission) and [File Permission Handler](permission-handler.md).

A stable can run multiple RPC sessions concurrently. Each session owns its workspace, conversation identity, PTY, and RPC host. A follow-up prompt sent to the same runtime reuses that host; resume starts from the requested conversation identity. After a normal turn, the stable keeps the host while that runtime waits for another prompt. Cancelling or shutting down the runtime cleans it up.

Cancellation does not treat a successful cancel request as proof that the agent stopped. Avenor reuses the host only after the remote session reaches a terminal state. If it does not converge, Avenor interrupts the exact hosted process and escalates to kill and reap when necessary.

For a long-lived stable, start the transport policy with the supervisor:

```sh
AVENOR_AGY_TRANSPORT=rpc avenor stable \
  --control-socket /tmp/avenor-agy-stable.sock \
  --idle-timeout 30m
```

Then, from another shell, spawn an `agy` runtime normally:

```sh
avenor control --socket /tmp/avenor-agy-stable.sock spawn \
  --backend agy \
  --model gemini-2.5-flash \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --label agy-review
```

### Auto probes before committing

`AVENOR_AGY_TRANSPORT=auto` tries RPC for each new session. It falls back to headless only before the session is registered and only after the failed RPC host has been cleaned up.

```sh
AVENOR_AGY_TRANSPORT=auto avenor \
  --backend agy \
  --model gemini-2.5-flash \
  --prompt "Summarize the current branch" \
  --dir /repo \
  --sentinel-file /tmp/agy-auto.env
```

Because the selected transport is not known in advance, `auto` does not advertise interactive permission capability conservatively. If permission relay is required, choose `rpc` explicitly rather than hoping the probe wins.

### Security boundaries

RPC discovery accepts only loopback listeners owned by the exact hosted `agy` process. Its HTTP client does not use ambient proxies, prefers TLS, and does not enable plain-HTTP fallback in normal backend configuration. Private endpoints, certificates, and wire identities do not become public session identifiers or diagnostics.

Avenor never passes `--dangerously-skip-permissions` by default. RPC adds a control path; it does not remove `agy`'s permission boundary.

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

Avenor spawns `pi --mode rpc --no-session` by default and talks via its own JSON-RPC protocol over stdio. Sessions are ephemeral: the transcript is discarded when the process exits. Set `AVENOR_PI_SESSIONS=1` in the environment to opt in to persisted sessions (see below).

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

### Persisted sessions and resume

By default a pi run's transcript is discarded with its process: the only records are avenor's event log and sentinel. Setting `AVENOR_PI_SESSIONS=1` in the environment (of the process that spawns the runs — the supervisor, or the shell running `avenor run`) launches each pi child with a session directory instead of `--no-session`:

- the full transcript, prompt included, survives the process as a pi session file you can read or resume later with pi itself;
- the message tool's `resume` action can continue a finished pi run across processes, the way it continues codex runs: the supervisor reads the prior run's session ID from its sentinel, and the resumed child launches bound to that session.

Session files go to `$XDG_STATE_HOME/avenor/pi-sessions` (default `~/.avenor/pi-sessions`), or to the directory named by `AVENOR_PI_SESSION_DIR`. The state home is outside run artifacts on purpose: artifact retention prunes aggressively, and a transcript someone wants later must not be pruned with them. The knob is opt-in rather than default because session files cost disk on every fan-out, and the shared session store is one of the suspects in the same-model startup race.

A resume still refuses a run whose sentinel says `TIMEOUT` or `KILLED`, and a run that launched without persisted sessions cannot be resumed — the refusal names `AVENOR_PI_SESSIONS` so the fix is discoverable.

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
