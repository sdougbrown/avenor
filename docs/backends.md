# Backends

Avenor talks to agents through backends. Each backend speaks a different protocol to a different runtime — OpenCode, Claude, Codex, Gemini, Cursor, DeepSeek Harness, fx, Pi, agy, or poo. Pick the one that matches what's running locally and what you need from the orchestration layer.

## Backend selection

Pass `--backend` to choose which runtime protocol to use. The default is `opencode-acp`:

```sh
avenor --backend opencode-http --server-url http://127.0.0.1:4096 --prompt "say hi"
avenor --backend opencode-acp --prompt "say hi"
avenor --backend codex-app-server --prompt "say hi"
avenor --backend agy --model gemini-4-argon-high --prompt "say hi"
avenor --backend gemini-acp --prompt "say hi"
avenor --backend cursor-acp --prompt "say hi"
avenor --backend dsh --model deepseek-official/deepseek-v4-flash --prompt "say hi"
avenor --backend fx --prompt "say hi"
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

The wrapper applies to the backend process only. Version probes (`claude --version`, `pi --help`) and helper processes (tmux, the shell tool) are never wrapped. Roster entries do not carry wrappers — they select backend/agent/model identity only, so the wrapper is a per-spawn parameter. Inside a loop or team, every phase inherits the parent run's wrapper; phases cannot override it.

The recorded PID for a wrapped backend is the wrapper's PID: the wrapper is the direct child of the launcher and owns the backend process tree.

When a `claude` run on the default PTY terminal ends or is reaped (a parked runtime's `--parked-timeout`, a cancel, a shutdown), the terminal hosting the wrapper sends `SIGHUP` to its whole process group once. If any member of that group is still alive 5 s later — including a helper left behind by a wrapper that already exited — the terminal sends `SIGKILL` to the group. A wrapper that owns a VM or container should stop it on `SIGHUP`.

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

Unknown fields, trailing data, an empty allowlist, empty entries, empty elements, NUL bytes, and non-absolute executables are rejected at supervisor startup (`mcp launch wrapper policy: <reason>`). An MCP spawn carrying a wrapper must **exactly match** one `allowed_argv` entry — same executable (absolute path), same flags, same order, no extra or missing arguments. A wrapper that is not allowed is rejected with `launch_wrapper: argv is not allowed by this supervisor` before any process is created; a supervisor with no policy rejects every wrapped MCP spawn with `launch_wrapper: disabled for MCP on this supervisor`. Every MCP spawn goes through the gated `spawn_mcp` verb; the spawn tool has no route to the plain `spawn` verb, so omitting a field cannot skip the gate. MCP servers fail closed against older supervisors that predate `spawn_mcp`, wrapped or not.

What `spawn_mcp` gates:

- **A caller-chosen launch.** `spawn_mcp` refuses a spawn whose effective launch the caller chose — a selected roster entry that declares `command`, `args` or `settings`, or a resume whose recorded session launch is not the one its backend resolves for itself — with `roster entry "<name>": launch fields are disabled for MCP spawns (<field>)`, before any reservation, idempotency record or process. An entry that declares no launch field still spawns over MCP with the identity its backend resolves, so a roster of plain backend/agent/model entries keeps working. The gate resolves the roster once and the spawn it hands the request to starts with that same identity, so rewriting the roster between the check and the exec cannot change the launch.
- **Workflow mode.** `loop_file` or `team_file` set is refused with `spawn_mcp: loop_file and team_file are not accepted; workflow spawns use the spawn verb`. A workflow template names the roster its phases select, and on this verb both paths come from the caller. Workflow spawns keep running through the trusted `spawn` verb.
- **The launch wrapper**, as described above.

The message tool's `resume` is the other MCP path into a spawn, and its cold arm is gated the same way: continuing a run whose launch is not the backend's own default is refused, because the resume re-execs the run's inherited launch. A resume on the default launch continues.

The launch fields are not allowlist-gated: there is no MCP equivalent of `allowed_argv` for them, so the refusal is unconditional.

The policy path is picked up in this order, for both `avenor stable` and `avenor mcp`:

1. `--mcp-launch-wrapper-policy <path>` (wins),
2. `$AVENOR_MCP_LAUNCH_WRAPPER_POLICY`,
3. `<config home>/launch-wrapper-policy.json` — the shared avenor config home (`$XDG_CONFIG_HOME/avenor`, else `~/.config/avenor`), the same root the host-owned trusted allowlists use,
4. none of the above: the policy is disabled.

A path named by the flag or the environment must exist and load; a missing or malformed file is an error. The well-known file is optional — absent means disabled — but a malformed one is an error too, never a silent fallback to disabled. Creating that file is the durable, machine-local way to enable the allowlist, so a plugin's versioned `.mcp.json` never has to carry a machine-specific argv.

Approved wrappers are **trusted code with supervisor privileges**: a wrapper process receives the backend environment, including provider API keys and per-run broker credentials. The allowlist is an exposure control on which wrappers MCP clients may invoke — it is **not** sandboxing, and approving a wrapper accepts that residual risk.

The `avenor mcp` command accepts `--mcp-launch-wrapper-policy` only to forward the policy to a supervisor it autostarts; it resolves the same flag → environment → `<config home>/launch-wrapper-policy.json` chain and forwards the path it picked. Attaching to an already-running supervisor never changes that supervisor's policy — that supervisor's own pickup chain decided it.

The CLI (`avenor control spawn`) and ordinary control-socket `spawn` calls are operator-trusted surfaces: they are **not** allowlist-gated, they keep accepting a roster entry's launch fields, and they keep the pre-existing behavior above.

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
| `opencode-acp`, `agy`, `gemini-acp`, `cursor-acp`, `fx` | ✓ ACP subprocess |
| `codex-app-server` | ✓ app-server subprocess |
| `poo` | ✓ stdio subprocess |
| `claude` | ✓ PTY or tmux pane |
| `opencode-http` | ✗ no child process |

- `opencode-http` attaches to an external `opencode serve` instance. Avenor starts no backend process, so it cannot inject a per-session environment. MCP servers loaded by that server do not see the identity. A successful attach also leaves the Avenor process environment unchanged.
- With the tmux launcher, Avenor removes inherited identity and exports the launch values inside the pane command, except the values a launch provisions for a broker peer: those go into a mode-0600 file in a mode-0700 directory the launch owns, which the pane command sources (`unset …; . <file>; exec claude`) and which the launcher removes when the session is torn down. A `tmux new-session` command string is argv, readable by any local process through `ps` or `/proc/<pid>/cmdline`, so no provisioned value may appear in it; the file's path may. The tmux server environment keeps its values.

Each spawning backend removes inherited identity before it adds the current launch values, and the same scrub removes the broker peer credentials (`AVENOR_BROKER_URL`, `AVENOR_RUN_ID`, `AVENOR_BROKER_TOKEN`) and the retired `AVENOR_SESSION_NAME`. A backend therefore cannot pass a launcher's peer credential down by forgetting a call: the strip list is `runtime.ScrubbedEnvNames`, applied by `runtime.LaunchEnv`, and the terminal launchers unset the same list in the pane command. Other variables keep their values, including `AVENOR_TREE_BUDGET` and backend selectors such as `AVENOR_CLAUDE_TERMINAL`.

`pi` also removes inherited `PI_AGENT` and `PI_AGENT_PROFILE`. It then adds the agent, the agent profile and the broker credentials of the current launch only. The `pi` agent process receives a broker value only when its own launch supplies all three, so an inherited or half-supplied credential never reaches it. The short `pi --help` capability probe runs with the inherited environment.

A `claude` child gets a one-shot attach credential in `AVENOR_BROKER_TOKEN` instead of the run's own: its plugin mods layer trades it at the broker's `POST /attach` for the run credential at session start, and the broker spends it there, so the run has one inbound consumer. A `pi` child gets the run's credential, because its extension authenticates each request itself. `runtime.ProvisionPeer` is the one place that decides whether a launch provisions a peer at all, and it appends the provisioned values after the shared scrub; no backend keeps an inherited copy.

A prompt to an already-running process cannot change that process's environment. A fresh start or resume that starts a process receives the identity of that request. A backend that reuses a live process keeps the identity of the original launch.

A launch wrapper receives the environment and passes it to the backend process. A wrapper that drops, filters or rewrites variables hides the identity from the backend's children. Avenor does not verify wrapper behavior.

Avenor's identity variables are informational and forgeable. They are never credentials and never authorization inputs. The broker variables are separate: `AVENOR_RUN_ID` is broker run identity, not the containing execution, and the credential a launch provisions (`AVENOR_BROKER_TOKEN`) is a capability — the run's token for `pi`, a one-shot attach credential for `claude` — which is never rendered into an argv.

A nested loop or team inside one CLI run shares the CLI run ID. `AVENOR_KIND` names the inner runner. Repeated nested phase names with the same iteration produce identical tuples, so the join is ambiguous. Avenor does not generate a scope ID.

## Capability matrix

| Capability | opencode-acp | opencode-http | codex-app-server | agy | gemini-acp | cursor-acp | dsh | fx | pi | claude | poo |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| New sessions | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Session resume | ✓ | ✓ | ✓ | ✓ | — | — | — | — | ✓ | ⚠ | ✗ | ✓ |
| Prompt execution | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Cancel | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Event streaming | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Permission relay | ✓ | ✗ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ⚠ | ⚠ | ✓ |
| Model selection | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Thinking control | ✗ | ✗ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ | ✓ | ⚠ | ✗ | ✗ |
| External server URL | ✗ | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✗ |
| Subprocess discovery | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ |

`—` means not verified; `✗` means not supported, `⚠ ` means experimental.

Thinking control is native for Codex, Pi, and Agy. Agy accepts `low` through `xhigh` on both a fresh start and an explicit resume, and rejects `off`, `minimal`, and `max` before spawning. Claude supports five startup values but no explicit resume value. Every other backend rejects explicit thinking. See [Thinking level](thinking-level.md) for values, version scope, validation order, and resume behavior.

Session resume for `claude` is in-memory only: a long-lived avenor process (stable mode) keeps sessions in its map and lets clients reattach by `session_id` while the session is alive. Cross-restart resume (avenor process exits, then a new one tries to pick up the same session) is not yet supported by this backend.

## poo

`poo` drives a poo binary over its public stdio protocol. Each avenor session owns one poo process: the provider spawns the resolved binary, performs `poo/initialize` with a protocol-version check, opens the session with `session/create` or continues one with `session/load`, routes its events and tears the process down.

A poo roster entry is the only place `command`, `args` and `settings` are accepted:

```json
{
  "umpire-reviewer": {
    "backend": "poo",
    "command": "/opt/umpire-bot/bin/umpire-poo",
    "args": ["serve"],
    "settings": "/etc/umpire-bot/poo-settings.yaml",
    "agent": "search"
  }
}
```

`command` is used as given and recorded as an absolute path; a relative `command` resolves against Avenor's working directory, not the roster file's directory. Without `command` Avenor runs `poo` from `PATH`. `args` defaults to `["serve"]`. The launch argv is `<bin> <args...> --settings <path>`, with `--settings` only when the entry names a settings file. The resolved binary, args and settings path are part of the session's ownership record, so a resume that resolves differently is rejected before a provider exists. A backend that does not own these fields rejects them. A poo entry cannot carry `thinking`; the shared thinking policy refuses it like any backend without thinking support.

### Messaging

Poo messaging requires poo v0.12.0 or later: the reply note an ask carries names the `avenor_message` tool and `call_cli`'s `command` argument, which earlier builds do not offer. The `messaging` spawn option enables the inbox feed, the `avenor_message` CLI entry and the child's broker credentials together. It defaults on for a delegation spawn and off for a workflow node, loop or team phase, and permission-decider run. An explicit value overrides the default, except that loop and team phases reject `true`.

The manifest binds the entry's `message` argument to stdin with `--message-stdin` (poo [#112](https://github.com/sdougbrown/poo/pull/112)): a present message — even an explicitly empty one — rides stdin and never appears in argv, while an absent message emits no flag, so message-less actions such as `withdraw` are unaffected. This keeps the text out of process arguments and listings, not out of the tool call, the transcript or any other surface that shows the arguments. A poo binary without stdin delivery cannot load the binding.

Avenor checks that the session offers `avenor_message` (poo elevates a small registry catalog to tools of their own) or `call_cli`, and waits up to 10 seconds for an authenticated `/poll-control` from the child's run ID after the spawn began. That poll proves the feed started, reached the broker and authenticated; it does not prove delivery into the session. An end-to-end test checks delivery. If messaging cannot be enabled, a default-on spawn is relaunched once with messaging off and a warning; an explicit `messaging: true` fails instead. The warning is an `avenor.error` event with `source: "warning"`, so callers can see it on the event channel they already watch without a new event kind. A read-only profile cannot offer `call_cli` and follows the same policy. A resume keeps the effective choice; a session created with messaging on may not resume through `avenor run --resume`, where the default is off, because its recorded tools no longer match.

An inbox message can wake a parked poo session. While the supervisor is parked it retains the prompted turn's status, result and sentinel; events from the unattended run are not replayed into a later resumed turn, except permission requests and their answers. The session transcript still contains that run. An ask from a run Avenor did not prompt reaches the child's event stream while the session is parked, and the launch's own policy resolves it with no turn: auto-approve, a connected control client, or the file handler `--sentinel-file` derives. `avenor answer` and `RuntimeAnswerPermission` reach it the way they reach an ask raised during a turn, so the ask no longer waits out poo's chain timeout for an answer nothing would send.

At launch Avenor publishes rendered manifests under `<state home>/poo/manifests/<digest>/`. It prunes sibling digest directories older than seven days, never the directory being published. Each launch, including resume, publishes its own manifests again.

The broker token grants messaging as the child run, not supervisor authority. Avenor declares the broker URL, run ID and token through `POO_CREDENTIAL_ENV`. A poo binary that includes [poo#103](https://github.com/sdougbrown/poo/pull/103) keeps them for the inbox feed and `avenor_message` bindings, but removes them from a model-issued `bash` command's environment. Every poo version that can run messaging (v0.12.0 and later) includes it.

This is environment scrubbing, not process isolation. A registered CLI explicitly bound to a credential can receive it; with poo's `local` executor, a command can also read poo's environment through procfs. Adding the token to `tools.env.scrub` removes it from the feed as well, so the feed fails to start. Claude children instead get a one-shot attach credential.

A poo run is a **blocking prompt** over a run that starts in the background. `Prompt` sends `run/prompt`, then waits on `run/inspect{wait: {until: "settled"}}` and re-asks when a wait runs out. A prompt that arrives while a run is active is sent as `run/message{mode: "steer"}` for that run instead of starting a second one, and a `busy` refusal from `run/prompt` falls back the same way. While the session is alive, `Cancel` sends `run/cancel` with the run id the prompt started or the session reports. Incoming events are routed by the notification's own `session_id`; events that arrive before the `session/create` response are buffered until the caller subscribes, so nothing is dropped.

poo's settle reason is classified into avenor's closed stop-reason set, and the exit code follows from the avenor reason:

| poo settle reason | avenor `stop_reason` | exit |
|---|---|---|
| `end_turn`, `finished`, `stop_requested` | `end_turn` | 0 |
| `output_truncated` | `max_tokens` | 3 |
| `max_turns` | `max_turn_requests` | 4 |
| `cancelled` | `cancelled` | 130 |
| `provider_limit` with `detail.provider.kind: rate_limited` | `rate_limited` (plus `retry_after`) | 7 |
| `provider_limit` with `usage_limit` or `plan_inactive` | `quota_exhausted` | 6 |
| `provider_limit` with any other kind | generic failure, the kind in `error_message` | 1 |
| any other reason, including `provider_error`, `context_exhausted` and `invalid_turns` | generic failure, the reason in `classified_from` | 1 |

A denial is not a failed run on this backend: poo hands the denied call back to the model as a tool result and the run continues, so the backend declares through `runtime.DenialRecovery` that a clean `end_turn` after a denied tool call is a completion. Backends that do not declare it keep the `tool_denied` reclassification.

A permission ask is relayed as avenor's `permission.request`. poo asks only a client that declared `permission_answers`, so the handshake declares it; each `permission/ask` request becomes one event whose `request_id` is poo's ask id, whose `tool`, `question` and `description` come from the ask's tool and summary, and whose `options` offer `allow` and `reject`. `AnswerPermission` replies to the pending request with poo's own `{decision: allow|deny, message}` result, poo hands the decision to the model as the call's outcome, and the run continues either way: a refused call reaches the model as a failed tool result carrying the resolver's message, not a failed run. The message is sent as the resolver wrote it; poo bounds what it shows the model.

An answer for an ask that is not pending returns an error and writes nothing: never asked, already answered, or already resolved by poo's own chain timeout. poo's ask budget and the backend's own view of pending asks are separate, so an answer that lands exactly as poo times the ask out is written and then ignored by poo. The backend does not predict that boundary. A run never waits on an unanswered ask forever: when poo's chain budget (`permission.chain_timeout_s`) runs out, the configured fallback decides and the run continues. poo's own `permission.asked` and `permission.answered` records are delivered as `avenor.permission.asked` and `avenor.permission.answered`, the prefix every backend-specific record passes through.

`Resume` continues a recorded session in a fresh process. The session's recorded launch starts that process, and the session id alone finds the records: poo's `session/load` reads them from the settings file's session directory, so nothing per-session is stored for a resume. The load resolves the run the dead process was in as interrupted, and the restored transcript is what the next prompt sends the model.

A resume of a session with no records fails as `session not found: <id>`: an ephemeral session keeps none, and an id this poo never wrote to its session directory is unknown. A resume whose settings no longer fit the session fails as an unfitting session, carrying poo's own code in the message — `unknown_model` when the recorded model is no longer named in `models`, `unknown_profile`, or `session_incompatible` when the recorded tools changed. The session is never continued under another model or profile. Either failure reaps the process the attempt started, so a refused resume leaves nothing running.

`Prompt` returns `nil` for every settle that is not a failure, like `pi`: `end_turn`, `max_tokens`, `max_turn_requests`, `rate_limited` and `quota_exhausted` all confirm the terminal event rather than reporting an error. Only `cancelled` and a generic failure (`error`) return one; the terminal event carries the reason and the exit code either way.

Teardown sends `session/close`, closes stdin, and gives the process 30 s to exit before `SIGTERM` and another 5 s before `SIGKILL`.

---

## claude

Channel-less provider for Claude Code. Starts Claude in a terminal session (PTY by default), injects prompts via PTY paste, and reads progress from the JSONL transcript file that Claude writes under `$CLAUDE_CONFIG_DIR/projects/` when that variable names an absolute path, or `~/.claude/projects/` otherwise. No broker, no sidecar, no `.mcp.json` injection.

### Launcher

Default launcher is PTY. Set `AVENOR_CLAUDE_TERMINAL=tmux` to use tmux instead.

```sh
# PTY (default)
avenor --backend claude --prompt "say hi"

# tmux
AVENOR_CLAUDE_TERMINAL=tmux avenor --backend claude --prompt "say hi"
```

`AVENOR_CLAUDE_TERMINAL` selects the launcher; `claude` defaults to PTY.

### Workspace trust

Before launching, Avenor marks the run directory trusted in Claude Code's global config (`$CLAUDE_CONFIG_DIR/.claude.json`, otherwise `~/.claude.json`) by setting `projects[<resolved run directory>].hasTrustDialogAccepted` to `true`, so the first-launch trust dialog does not appear. It never creates the file: a missing config means Claude Code's onboarding has not happened and is left alone. It holds Claude Code's own `<config path>.lock` directory from the read through the rename, waits about a second for a held lock, removes only a lock it still owns, gives up instead of renaming once it has held that lock for 5s, and re-reads the file when a writer changed it in between; a config owned by another user is left alone. The edit is skipped when the run's Claude Code would read a different file: with `CLAUDE_CODE_CUSTOM_OAUTH_URL` set it names its config `.claude-custom-oauth.json`, and it prefers a non-empty legacy `<config home>/.config.json` when one exists. Wrapped and tmux runs are skipped: a wrapper (container, VM) may not read the host config, and a tmux pane inherits the tmux server's environment rather than Avenor's, so the edit could write a file the child never reads; the key-driven dialog handler answers there instead. When the edit fails, the dialog appears and the session's key-driven handler answers it.

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

Avenor spawns `agy acp --stdio --add-dir <dir>` (with `--agent <name>` when an agent persona is requested) and drives it over standard ACP JSON-RPC on stdio.

```sh
avenor \
  --backend agy \
  --model gemini-4-argon-high \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --sentinel-file /tmp/agy.env
```

- **Working directory & wrapper**: Avenor sets the child process working directory to `--dir` and passes `--add-dir <abs-dir>` plus ACP `cwd`. Launch wrappers (`--launch-wrapper`) are supported on both fresh starts and cold resumes.
- **Model selection**: `--model` is applied per session via ACP `session/set_config_option` (`configId: "model"`). Both base model names and effort-suffixed slugs (for example `gemini-4-argon-high`) are accepted; unknown model IDs fail cleanly with `agy acp`'s `-32602` error without installing a session. The slug is passed through unchanged: Avenor never strips an effort suffix or infers a thinking level from it.
- **Thinking level**: `--thinking` is applied per session via ACP `session/set_config_option` (`configId: "thought_level"`), after the model, on both fresh starts and explicit resumes. `low`, `medium`, `high`, and `xhigh` are admitted; `off`, `minimal`, and `max` fail the static policy before `agy` spawns. An omitted level sends no `thought_level` request, and a resume without one clears the constructor default instead of reapplying it. `agy acp` validates the level against the selected model after the session opens, so a model that does not offer a statically allowed level (for example `xhigh`) fails the launch with `agy`'s own error rather than the static allowed-values error; a fresh start or cold resume then closes the session, while a warm resume keeps its owned session.
- **Agent selection**: `--agent` is passed on the `agy acp` command line (ACP session modes select permission modes such as `default`, `accept-edits`, `auto`, and `plan`, not agent personas).
- **Permissions, tools, and cancellation**: Avenor never passes `--dangerously-skip-permissions`. Tool approval requests arrive via ACP `session/request_permission` and are relayed as canonical `permission.request` events; tool calls stream as `tool.call` and `tool.call_update`; `session/cancel` cancels active turns.
- **Multi-turn & cross-process resume**: A live `agy acp` child handles follow-up turns in place, and cold resumes reattach across process restarts via ACP `session/resume`. Legacy pre-ACP conversation IDs may not resume across the protocol boundary unless recognized by `agy acp`.
- **Broker messaging**: When a supervisor broker is active, Avenor injects `avenor channel-tools` through the standard ACP `mcpServers` list on `session/new` and `session/resume`, and polls inbound broker messages for turn-boundary delivery.
- **`AVENOR_AGY_TRANSPORT` retired**: The legacy `headless`, `rpc`, and `auto` transports have been removed. Setting `AVENOR_AGY_TRANSPORT` to any non-empty value fails launch with a migration error telling you to unset it.

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

## dsh

Avenor spawns `dsh --profile acp` and drives DeepSeek Harness over ACP. DSH's
ACP server advertises session new/list/resume/close, a model config option, and
`session/request_permission`, so Avenor's permission relay and model selection
both work per session.

```sh
avenor \
  --backend dsh \
  --dir /repo \
  --model deepseek-official/deepseek-v4-flash \
  --prompt "Review the changes in the current branch"
```

**Model selection:** DSH's ACP server takes a model as a provider/model pair, so
pass `provider/model`; Avenor encodes it into the form the server expects. A
bare model id is forwarded unchanged and DSH rejects it with an unknown-model
error rather than guessing a provider. Omitting `--model` keeps the model the
`acp` profile pins.

The accepted values are the provider's live catalog, which the ACP session
reports as its `model` config options; that list changes with provider
releases, so read it rather than assuming an id. Reasoning effort is exposed the
same way (`reasoning_effort`), but Avenor's thinking policy does not admit
`dsh` yet, so an explicit thinking value is rejected before launch.

**Custom providers:** the `acp` profile is a normal DSH profile, so a provider
route added to its patch layer (`~/.dsh/profiles/acp/cordis.patch.yml`) becomes
a model choice like any other and is selected as `provider/model`. An
OpenAI-compatible gateway needs `api`, `baseURL`, and a `models` list; when the
route declares `apiKeyEnv`, that variable must be in the environment of the
`dsh` process Avenor launches, because the harness resolves the credential per
request.

**Requirements and caveats:**

- `dsh` must resolve on `PATH`. A `npx`/`pnpm dlx` install exposes a shim there;
  an ephemeral install path is not discovered by Avenor.
- DSH resolves its own provider credentials. A launch with none succeeds and
  fails at the first prompt, so the run reports a prompt failure rather than a
  spawn error — check the run's error output before suspecting the backend.
- The `acp` profile enables the tools it needs itself; Avenor's agent name is
  not forwarded, because DSH's ACP modes are permission modes, not agent names.

---

## fx

Avenor spawns `fx acp` and talks via ACP over stdio.

**Model selection:** fx takes its model at process launch, so a run's `--model` rides the launch argv (`fx acp --model <id>`) and the model is part of the launch identity: a warm client started for one model is never reused for another, and changing the model starts a new process. Without `--model`, fx uses the model configured in its own CLI config (`fx provider`, `fx login`); selecting fx's provider is still done through fx's own config.

**Reliability caveat:** fx's ACP server can end a turn by rejecting `session/prompt` with its `InconsistentFinishReason` error instead of a clean stop, especially against local models, even though the reply already streamed. Avenor tolerates that error when the turn streamed message content: it logs a warning and completes the turn, so the session stays usable for a follow-up prompt.

```sh
avenor \
  --backend fx \
  --prompt "Review the changes in the current branch" \
  --dir /repo \
  --sentinel-file /tmp/done.env
```

---

## pi

Avenor spawns `pi --mode rpc --no-session` by default and talks via its own JSON-RPC protocol over stdio. Sessions are ephemeral: the transcript is discarded when the process exits. Set `AVENOR_PI_SESSIONS=1` in the environment, or pass a per-run choice (see below), to opt in to persisted sessions.

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

The per-run choice overrides the environment in both directions: a spawn with `persist_sessions: true` (MCP `spawn` argument, `persist_sessions` spawn parameter, `--persist-sessions` on `avenor run` and `avenor control spawn`) persists without the environment; `persist_sessions: false` stays ephemeral even when the environment opts in. Roster entries carry the same field, so loop, team, and workflow phases resolve it per entry (see the loop, team, and workflow roster docs). A run that inherited or forced persistence keeps it on resume: the resumed run carries the prior run's choice (a persisted run stays resumable even when the environment has since dropped the opt-in), and resuming a run recorded as ephemeral is refused at the resume gate rather than dying later on a missing session file.

Session files go to `$XDG_STATE_HOME/avenor/pi-sessions` (default `~/.avenor/pi-sessions`), or to the directory named by `AVENOR_PI_SESSION_DIR`. The state home is outside run artifacts on purpose: artifact retention prunes aggressively, and a transcript someone wants later must not be pruned with them. The knob is opt-in rather than default because session files cost disk on every fan-out, and the shared session store is one of the suspects in the same-model startup race.

A resume still refuses a run whose sentinel says `TIMEOUT` or `KILLED`, and a run that launched without persisted sessions cannot be resumed — the refusal names `AVENOR_PI_SESSIONS` so the fix is discoverable. A roster entry without the field keeps the environment's choice (see the loop, team, and workflow roster docs for the entry field).

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
