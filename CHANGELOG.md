# Changelog

## Unreleased

### Added

- `avenor run --claude-mcp-wait <duration>`: hold the first prompt of a `claude` or `claude-channel` session until that long after launch (at most `60s`), so MCP servers Claude Code connects in the background, including claude.ai connectors, are available to the first turn. The wait is anchored to launch, so time already spent reaching readiness counts toward it; only the first prompt waits, and it runs before the prompt is armed. `agent.prompt_submitted` carries `startup_wait_ms`, the extra time waited, when the flag is set. It is opt-in because Claude Code gives no MCP-settled signal and claude.ai connectors come from an account API fetch, so a run with no MCP configured cannot be detected locally. Direct runs only: other backends and `--loop-file`/`--team-file` reject it, and roster entries do not carry it yet.
- `avenor run --result-file <path>`: write the run's final assistant message as plain text so a caller no longer rebuilds it from `--on-event` chunks. The content is the same text the terminal `session.end` records as `final_output` (the backend's own final output when supplied, otherwise the last turn's text), written atomically before the sentinel, which then carries `RESULT=<path>`. The file is written only when the run's last turn settled on the backend's terminal event: a run that settled with a failure still writes the text it settled with, while a cancelled run, a timed-out run, and an attempt or resume that failed before the backend settled write no file and no `RESULT=` line. A stale answer at the same path is removed just before the first prompt, once the provider and session are up. Direct runs only — `--loop-file`/`--team-file` reject the flag, since a workflow has no single final message.
- `avenor run --pid-file <path>`: publish the run's own pid once every setup step that can fail has passed, and remove it on exit, so background callers stop inferring a pid from a launcher shell. Every run mode publishes it, including `--loop-file`/`--team-file`, which publish once their workflow config has loaded and validated. A `setsid` wrapper that forks leaves `$!` pointing at a shell that already exited, which makes `kill -0`-based waiters report a live run as finished. A run rejected during setup never publishes a pid, so it cannot clobber another run's pid file. The file is only removed by the run that wrote it.
- `session_id` on `avenor.phase.end` events (team and loop runs): the phase's final session — the last attempt's, so after retries and requirement nudges it names the session whose `session.end` carries the phase's `final_output`. Consumers that need to join a finding to its completion can now map each team member's phase to that member's final message in one event read; `session.end` itself carries no phase. Omitted when no session started (e.g. the phase failed before any attempt).
- `permission_policy.decider_model`: a direct-model permission resolver for auto-dispatched nodes. An assignment can name a provider from the user-level provider registry (`$XDG_CONFIG_HOME/avenor/providers.{json,yaml,toml}`, default `~/.config/avenor/`) plus a model on it, and avenor itself resolves each pending permission request with one bounded OpenAI-compatible chat-completions call — no decider runtime, no tools. The decision (allow/reject/defer, strict JSON) is judged against the request's structured mirror (tool, command, cwd), the node's task, and the pending options, all framed as untrusted data; allow/reject map onto the matching pending option through the ordinary control plane, defer (and every failed or unparseable call) leaves the request pending for a human gate with the same bounded-retry-then-exhaustion semantics as a failed harness decider spawn (`permission_decider_exhausted`). The pinned selection carries the provider, model, endpoint, and key variable name — never a credential; the API key resolves at spawn time from `api_key_env` (preferred) or a literal `api_key`. New `internal/modelapi` package (generic OpenAI-compatible client, the foundation for future harness-free judgment steps) and `internal/providerconfig` (user-level provider registry, strict decode). Runtime status and `avenor_status` surface the resolved decider as `permission_model_decider`.
- `workflow cancel` (CLI), `workflow.cancel` (control method), and `avenor_workflow_cancel` (MCP tool): cancel a whole workflow instance with one attributed decision (`actor` + required `reason`). The kernel records a `workflow.event.canceled` event, sets the instance terminal `canceled` (`canceled_by`/`cancel_reason` on the snapshot), releases every live lease, and parks non-terminal activations `canceled`; the supervisor then terminates the instance's in-flight attempt runtimes. Late `attempt_terminated` facts from still-running runtimes land harmlessly (attempt fact recorded; leases, retries, and statuses never regress), the controller stops scheduling the instance's nodes, and re-canceling is an idempotent no-op while a `completed`/`failed` instance refuses with a clear error.
- `avenor_kill` tool: terminates a single run without shutting down the supervisor — the supervisor and all sibling runs keep running. Implemented across every tool surface: the Go MCP server (`internal/mcpserver`), the TypeScript reference (`packages/core` `killTool`, `packages/mcp`), the pi extension (`avenor_kill`), and the OpenCode plugin. Killed runs settle as `killed` (exit code 130, stop reason `cancelled`) and remain resolvable for `avenor_status`/`avenor_result`; a killed run is still not resumable, and the `message` tool's `resume` action refuses it the same way it refuses a run that timed out.
- `Supervisor` (packages/core) exposes `getInstance(supervisorId)` — the single source of truth for singleton ownership, with `isCurrentInstance` now derived from it — and `forEachRun(cb)` for iterating stored runs without reaching into the private map. The last `(sup as any).runs` casts (in `avenor_shutdown`) are gone.
- Per-phase roster entries in loop and team configs now apply their `thinking` level: the entry's default merges with the run-level value (an explicit `--thinking`/spawn level wins) and is validated against the phase's resolved backend before the phase starts. `ResolvedSelection` now carries the entry's level for phase consumers; the spawn selector deliberately still excludes `thinking`, which remains a default rather than a selection field.
- `avenor await`: new wait-only subcommand from this feature work. Waits on a run until attention or done, with plain transition lines or JSON output.
- Control ownership is fixed at construction instead of granted to the first caller: `avenor stable` generates an owner capability before it builds the control server, and a connection receives authority only by presenting it in the new `authenticate_owner` method. Fourteen methods are gated (`cancel`, `prompt`, `interrupt_and_prompt`, `answer_permission`, `spawn`, `spawn_mcp`, `shutdown`, `message_send`, `message_wait`, `message_reply`, `message_withdraw`, `resume`, `session_rename`, `session_last_set`); the other twenty-eight — observation, runner plumbing and the whole `workflow.*` family — stay unauthenticated. **Migration:** a client that calls a gated method without attaching now gets `-32010` (`permission_denied`), where the first connection to call one used to become the owner. Attach with the published capability to keep calling those methods.
- The supervisor publishes its ownership state in files beside the control socket: `<socket>.owner.json` (mode `0600`, owned by the server's euid) holds the capability plus the socket's device and inode, and `<socket>.control.lock` is a persistent file that serializes every mutation of the path — stale-path removal, bind, publication and cleanup — created or opened and never unlinked. The `<socket>.dead` tombstone keeps its existing role as corroboration of a past instance: it authorizes nothing, and its absence is never liveness.
- Both clients attach as owner, binding the credential recipient with what their runtime offers, and both launchers attach a winner that already holds the path instead of unlinking anything. The Go client asks the kernel for the connected peer's uid (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED` on macOS) and requires its own euid; the TypeScript client has no peer-credential API, so it binds by filesystem instead — a socket owned by its euid, in a directory that euid or root owns and that is not group- or other-writable unless sticky — then re-checks the socket's device and inode after connecting and before writing the capability. An owned start proves the child with its per-attempt identity token on the connection it authenticates.
- The broker ask path is now one transport contract in a new `internal/messaging` package: request/result types, the `Transport` and `Messenger` interfaces, the outcome vocabulary, the v1 wire error classes and the message-ID minter, with `askstore` (admission, fences, pruning, generation rotation), `askclient` (send→wait→withdraw ordering and tool rendering), and the `inprocess`, `localhttp` and `hosthttp` transports over shared HTTP plumbing. The leaf package imports no sibling, and the transport, store and failure suites are shared across the implementations. Documented in [docs/messaging.md](docs/messaging.md).
- Provider quota and rate-limit failures now get their own stop reasons and exit codes: `quota_exhausted` (exit `6`) and `rate_limited` (exit `7`), classified once the turn settles from the provider-owned failure text each backend forwards (`pi` provider message or stderr, the billing `402` body, codex `turn/completed` error, ACP prompt error, `opencode-http` error name). `session.end` and the completion sentinel carry the provider's retry-after hint as `retry_after` seconds / `RETRY_AFTER=`, a rate-limited turn stays eligible for `--max-retries` and waits at least the hinted delay — clamped to the run deadline and to five minutes, with an unhonorable hint ending the run instead of parking it — and a replaced backend stop reason stays visible as `classified_from`. Only explicit throttle signals are retryable, so quota wording with a reset hint exits `6`. Model-authored prose such as a claude-channel finish summary never classifies a run. Documented in [docs/events.md](docs/events.md).

### Changed

- **A spawned `claude` child joins the broker as its own run.** The default `claude` backend now hands every child the broker URL, its run id, its token and its label — the run the supervisor already provisions for it — so the Avenor Claude Code plugin's mods layer (0.7.0) makes the child an addressable peer: `SendMessage` to `avenor:<label>` reaches it, and its `message` tool's `ask` action blocks on the broker's reply edge. Previously a spawned child had no broker identity at all, and claude-channel was the only Claude backend whose children could be addressed. With the run id and token in the environment the layer joins that run directly instead of registering through the broker's open `/register` route, so one label never names two records; without them a hand-started session registers exactly as before.
- A hosted `claude` child no longer inherits a launching session's avenor peer credentials. The terminal launchers strip `AVENOR_BROKER_URL`, `AVENOR_RUN_ID`, `AVENOR_BROKER_TOKEN` and `AVENOR_SESSION_NAME` from the environment they hand the child and export only what the backend provisions — a pane inherits the tmux server's environment, which can carry a peer session's live credentials, and a child that kept them would join the broker as that run. `terminal.StartOptions.Env` is the seam that carries the provisioned values.

- **A spawned `pi` child now knows where to send status updates and questions.** The pi extension supplies `parent_run_id: supervisor` for host-mode spawns to its singleton supervisor, including an explicit `supervisor_id` naming that singleton. The supervisor's existing parent note reaches the child's prompt, naming the eagerly registered broker run. Non-local and nested spawns omit the note because their supervisor's ask queue has no reader in the parent session. Those children still need an address supplied in the brief; the extension does not forward their asks to the parent.

- **Breaking: one messaging tool, one peers tool, and unprefixed MCP names.** Every host surface now exposes one message tool — `avenor_message` on the Pi extension and the OpenCode plugin, `message` on the two MCP servers — whose explicit `action` replaces the separate messaging tools, `avenor_peers` grows `rename` and `last`, and both MCP servers register every tool without the `avenor_` prefix.
  - `message` covers `tell`, `ask`, `reply`, `resume`, `withdraw` and `control_reply`. `resume` replaces `avenor_follow_up` and keeps its semantics: a new run continuing the prior session, `<prior-label>-followup` by default, an inherited `auto_approve`, and a `follow_up:`-namespaced idempotency key. The retired names are deleted with no alias window — Pi's `avenor_follow_up`, `avenor_ask`, `avenor_reply` and `avenor_cancel`, the OpenCode plugin's and both MCP servers' `avenor_follow_up`, and the child servers' (`avenor-channel-tools`, the claude-channel sidecar) `avenor_send`, `avenor_ask`, `avenor_reply` and `avenor_cancel` — so a caller still using one gets an unknown-tool error. `control_reply` is the claude-channel backend's reply path; every other surface answers `unsupported_action` because only the sidecar's provider can deliver one.
  - All seven surfaces return the same snake_case result object (`action`, `outcome`, `text`, and `message_id`/`run_id`/`label`/`reply`/`sentinel_file`/`on_event` when they apply) and the same outcome vocabulary and sentences, from one schema, one route table and one outcome table in `schemas/`. `reply` on a surface that never receives inbound asks is `edge-absent` and sends nothing, and a reply whose receipt did not survive is reported as possibly landed rather than as never sent. Every failure still says whether the ask can be withdrawn.
  - MCP surfaces publish a flat input schema (`schemas/message.input.flat.json`, `schemas/peers.input.flat.json`) compiled from the per-action union, so a client keeps `timeout_ms` an integer and `payload` an object instead of losing both to a `oneOf`; the per-action rules — required fields, unknown properties, types and limits — stay with the router, which refuses a mismatch with the same `invalid_request` and repair text as before.
  - `avenor_peers` (`peers` on MCP) lists sessions with their `kind`, renames a display name, and reads a peer's last agent message. Rename has two levels: the control owner may rename only the supervisor session, and a broker-authenticated run may rename only itself. A launch label is now the rename target, so `/sessions` and the peers list report the current name, the outbound sender name follows it, and `session.renamed` is published on the control stream. `last` is a bounded read of the latest settled turn (empty turn and no turn stay distinguishable, and truncation is reported) and a Pi or OpenCode host publishes its own turn; full text stays with `avenor_result`.
  - The control protocol's six `broker_*` verbs (`broker_send`, `broker_ask`, `broker_reply`, `broker_cancel`, `broker_peers`, `broker_receive`) and their client methods are deleted. Their replacements are `message_send`, `message_wait`, `message_reply`, `message_withdraw`, `message_peers` and `message_receive`, plus `resume`, `session_rename`, `session_last` and `session_last_set`. The broker also gains `POST /rename` and `GET /last_message`, the one broker read endpoint whose target need not be the caller. `broker_*` callers must move to the `message_*` names; [docs/control-protocol.md](docs/control-protocol.md) lists which methods are owner-gated.

### Removed

- The `followUpTool`, `askTool`, `cancelTool` and `brokerReplyTool` exports (packages/core) and the `Client.brokerSend`, `Client.brokerAsk`, `Client.brokerCancel` and `Client.brokerReply` wrappers are deleted with no alias window. `messageTool` and `Client.messageSend`/`messageWait`/`messageReply`/`messageWithdraw` replace them.

### Fixed

- Supervisor-created artifact directories now live under the per-user root `$TMPDIR/avenor-stable-<euid>/run_<supervisor_run_id>/<runtime_id>` and have an age-based retention instead of dying with their supervisor: a supervisor prunes run directories whose newest entry is older than 24 hours at startup and then hourly, so a finished run's default sentinel and event log stay readable for the status, result, and follow-up reads that classify it. Nothing removed them before, so every test run and every long-lived host accumulated them until `/tmp` ran out of inodes. The root is created `0700` and refused — a spawn that needs it fails, pruning is skipped — unless it is a real directory owned by this euid with mode exactly `0700`, so a shared `/tmp` cannot redirect retention through a pre-created symlink and cannot read the artifacts of a group- or other-readable root. A refusal names the root and says to remove it, or to chown and chmod it to `0700`; there is no fallback root. Only `run_*` entries are candidates, each supervisor refreshes its own run directory before every prune so a live run is never removed, and a run directory that is itself a symlink is never followed. Pruning is best-effort, stays inside the artifact tree, and never touches paths passed through `on_event` or `sentinel_file`: paths outside the artifact root are never touched. Run directories under the previous `$TMPDIR/avenor-stable/` root are no longer pruned by anything; delete that directory by hand once.
- `avenor answer` no longer reports a published response as a failed write. A commit that hard-links the response into place but cannot remove its staged temp file now returns `durablefile.ErrTempNotRemoved`, so the command exits `0` and warns on stderr with the leftover temp path instead of exiting `1` after the response had already landed.
- Workflow snapshots, templates and the workflow and controller event logs are created `0600` instead of `0644`. A snapshot and a template carry caller-supplied parameters, and an event log carries prompts and request tokens, so group and other read access is gone; every reader opens these files as the same uid that wrote them, and an event log created by an earlier version keeps its existing mode.
- `avenor watch --since-cursor` stages each cursor rewrite in a uniquely named temp file, so two watchers sharing one cursor path no longer lose writes to each other: both used `<cursor>.tmp`, and the writer that renamed second failed on the temp file the first had already moved away. The cursor file is now created `0600` instead of `0644`, since it is per-user state.
- `client.SubscribeRuntime` no longer races an event send against closing its channel. The runtime fanout sends under the lock that removes and closes a cancelled subscription, and every send stays non-blocking, so cancelling a subscription while events are arriving can no longer panic with `send on closed channel`.
- A control-stream subscriber no longer loses events that were still queued for it when a run's control server stops. `Stop` drains each subscriber's queue, the final `session.end` included, before closing its connection; a short overall deadline and per-connection write deadline keep a stuck reader from holding up shutdown. A `subscribe` that arrives once shutdown has begun is refused with `-32014` (`control server stopped`), and every connection still registered when shutdown begins is closed, so a late subscriber and a late client both fail fast instead of waiting on a stopped server. `client.Subscribe` reports that refusal as `client.ErrControlStopped`, which wraps `client.ErrConnectionClosed`, so `avenor control tail` ends quietly on the refusal while a connection that merely drops is still reported.
- Pi completion notices no longer arrive after you have already read the result. While Pi is busy, the extension holds each finished run's notice and shows `unread:N` in the footer. A ready `avenor_result` or `wait: true` spawn removes its notice, by label or canonical run id. Unread notices are delivered together in one continuation when Pi settles normally, while an idle completion still starts its own turn immediately; an idle send that is never confirmed stops after three attempts and then reports on the new `avenor:delivery:error` channel, keeping `avenor:poll:error` a pure polling signal. This requires Pi 1.4.0 or newer.
- Cancelling a control server watch subscription while events are being published no longer risks a panic. The event fanout that feeds `ControlServer.SubscribeEvents` — one subscription per `--http-debug` event stream — now sends to each watch channel under the lock that removes and closes it.
- A spawning runtime is no longer published to `List` and `RuntimeStatus` half-initialized. The supervisor registered the child and then filled in its identity, paths, event sink and direct attempt without `child.mu`, so a status reader racing a spawn (direct, loop or team) could read partially written fields. Everything the spawn parameters determine — identity, effective selection, roster metadata, directory, and the event and sentinel paths — is now set before registration, and the remainder (event sink, file handler, resolved roster, direct attempt) is installed under the lock those readers hold. The per-runtime artifact directory is created after registration and outside the control lock, only when a path actually defaults into it, so a spawn that supplies both paths creates nothing; a failed `mkdir` still rolls the runtime back. A row seen mid-spawn therefore carries its paths from the start instead of empty ones.
- The local broker no longer accepts run credentials in a URL. GET requests such as `/sessions` authenticate with the `X-Avenor-Run-Id` and `X-Avenor-Run-Token` headers, so a run token no longer lands in access logs or error reports that print the request URL.
- The ACP and codex app-server providers' agent-message poll no longer discards the run's other control messages. It drained the whole control queue, so a status request or shutdown queued for the same run was lost. Non-agent messages now stay queued, the way the agent-only control poll already keeps them.
- The ACP provider no longer leaks a backend launched concurrently with `Close`. A launch still starting when close runs used to install its client and config cleanup after `Close` had returned, orphaning the child until a second close; it now leaves nothing installed, tears down the child and generated config it started, and fails with `provider is closed`. `Close` runs each launch under a provider lifetime, so a handshake still in flight is cancelled with the provider instead of surviving shutdown, and a wedged start is abandoned after a two-second bound rather than holding shutdown open or delaying a later close. Only a launch or session that `Close` itself ended reports `provider is closed`; one that fails on its own while shutdown is in progress keeps its own error. Every backend built on the ACP provider, including `opencode-acp`, inherits the fix.
- A permission request answered directly, for example by a model decider, no longer reads as unanswered. The run's resolver goroutine could stamp its fallback state onto the claim after the answer had landed, which erased the resolution. A resolved claim, or one a direct answer has taken, now keeps its state.
- `avenor run` now traps SIGTERM alongside SIGINT, so a backgrounded run records its outcome instead of dying with the default disposition and leaving a pid file behind. Both signals end the run at exit code 130 with a `KILLED` / `STOP_REASON=cancelled` sentinel, including a signal that lands before the first session starts; a cancelled run writes no result file and no `RESULT=` line, since the reply collected so far is a prefix rather than the run's final message. The trap is installed before the pid file is published, so a first signal landing during startup still has a cleanup path, and once the first signal has cancelled the run the default disposition is restored so a second signal still ends a teardown that will not finish. A cancel during retry backoff now reports `cancelled` rather than the previous attempt's stop reason.
- `avenor run` now rejects artifact paths that resolve to one file: `--result-file`, `--pid-file`, `--sentinel-file` and `--on-event` must name distinct paths (compared as absolute paths with existing parent directories resolved, and by file identity when the paths already exist), and the result and pid paths must be single-line. Previously two spellings of one path — a relative and an absolute form, a symlinked directory, or a link to an existing file — passed validation, and `--on-event` could truncate a path another flag owned; the event log is now opened without following a final symlink for the same reason, and it is created `0o600` (an existing file is tightened to `0o600`) because it carries prompt and turn content.
- A direct run's `session.end` now always carries the `exit_code` its settled stop reason maps to, so the published record, the sentinel and the reported status agree. A backend-supplied `exit_code` never wins.
- The dispatch permission-decider routing test no longer races the spawn's identity writes: it waits on the decider's published session identity and bounds every spawn-identity read, and it asserts the routed request stays pending through the permission claim state with a held-open worker turn instead of polling a flag on an ending runtime.
- The idle-time `session.end` now flows through the runtime's event fanout like any other end, so the broker recorder and the durable event stream see it, not just control subscribers. It is tagged `end_kind: "run"` — a tag the ordinary write path strips from a backend's own turn end, so only the run-level writer can stamp it — is published at most once per session, and is never published with an empty `session_id`. Every `session.end` the fanout publishes carries the `exit_code` its stop reason maps to; a backend-supplied number never overrides it.
- The stable supervisor's idle timer now measures its window from the last runtime activity — the most recent spawn or completion — instead of a deadline armed at startup. A runtime that outlives `--idle-timeout` no longer lets the supervisor exit the moment it ends, and one that ends does re-arm the timer (previously nothing woke the loop after a spawn, so the supervisor never expired). Parked runtimes count as live, so a parked runtime keeps the supervisor up until it is prompted or reaped, and only then does the idle window start.
- `pending_permission` is cleared when a runtime ends, together with the `permission` record, so a runtime cancelled while a request was pending reports the killed state instead of waiting for permission no live turn can answer.
- A managed `opencode serve` whose start was still in flight when the supervisor shut down no longer outlives the supervisor: shutdown previously deleted the in-flight marker, and the completing start re-inserted its server into the map, orphaning the process. The completing start now checks the shutdown flag and tears its server down instead.
- The stable supervisor's HTTP-server registry no longer stores sentinel values in a `map[string]any`: ready servers and in-flight starts live in separate typed maps, eliminating the "Shouldn't happen" type assertion flagged in #16.
- A prompt error no longer outruns the authoritative session end when classifying a run. When a provider settles the session right around a prompt failure (pi retry exhaustion is the real case), `WaitForSession` now drains under a short grace window so the terminal `session.end` wins, and the authoritative stop reason is surfaced on the result. Previously the run could settle as `stop_reason: exit_1` even though `session.end` named the real reason (e.g. `error`), which mislabeled retry-exhausted pi runs.
- The pi client no longer maintains an unread global event buffer. `fanout` wrote every event into a 4096-slot channel that nothing in production consumed, so long runs logged a misleading `dropped event ...: global event buffer full` line for every event past the cap — including the terminal `session.end` that was still delivered through the subscriber channel. The per-session subscriber path is now the only event path.
- The `message` tool's `resume` action (formerly `avenor_follow_up`) now resumes `failed` and permission-`blocked` runs whose backend session persisted, instead of refusing with "run is not resumable (status: failed)". A pi run that lost its provider stream after thousands of events kept its session and working tree; the resume now continues the recorded session rather than forcing a fresh one. `timeout` and `killed` runs remain non-resumable.
- `avenor_status` MCP tool: the list form (no `run_id`) now returns `{ "runs": [...], "count": N }` instead of a bare array. The MCP spec requires `structuredContent` to be an object, so the array form previously failed client-side schema validation in Claude Code. The tool also now declares its output schema, derived from the typed Go result, and the TypeScript reference implementation (`packages/core`) returns the same wrapped shape.
- `avenor_status` now carries the `permission` request record (`request_id`, `description`, `options`) alongside the `pending_permission` flag, in both full and lifecycle views. Previously the Go MCP tool dropped the record, forcing agents to answer permission requests blind or re-query; the TypeScript reference already surfaced it.
- The MCP server no longer becomes unusable after `avenor_shutdown`: the server previously marked itself closed, so every later call — including a fresh `avenor_spawn` that should autostart a replacement supervisor — failed with "control client not available" until the host session restarted. Shutdown now retires only the supervisor connection; the next default-supervisor tool call autostarts a new supervisor, and tools with autostart disabled report "no supervisor running" instead of a dead-client error.
- `pi` turn failures no longer report `unknown error` when the child printed the real cause: the captured stderr is the failure text, and a child that exits before settling the turn now fails the prompt with that text instead of leaving the run waiting for its own timeout. Each turn reads the buffer from its own attribution mark, so an earlier turn's cause cannot describe a later failure.
- `pi` sub-agent turns now honor pi's auto-retry: when pi's `agent_end` carries `willRetry: true`, avenor no longer treats the turn as terminally failed and tears the session down before pi's retry backoff fires. Transient model/gateway errors (e.g. a LiteLLM 500 connection error) are retried by pi itself instead of killing the run. The settled `session.end` now also carries the provider's real `error_message` instead of `unknown error`.
- The `message` tool's `ask` action (formerly `avenor_ask`) now resolves its run reference (public run id, label, or broker runtime id) the way its sibling actions do. Addressing a live run by its canonical spawn id previously failed with "to run not found" because the broker registers runs only under their runtime id.
- `pi` sub-agents now receive inbound asks and can reply: the supervisor provisions a broker run + token for each pi child, the pi provider passes them to the sub-process, and the `packages/pi` extension polls `/poll-control` to surface asks into the session and answers through the `avenor_message` tool's `reply` action (formerly `avenor_reply`) as its own run.

## v0.7.1 — 2026-05-11

### Fixed

- `exitWithSentinel` closure is now defined immediately after `fs.Parse` succeeds,
  covering all post-parse return paths. Previously, the three early-exit paths for
  unreadable prompt file, event-log creation failure, and bad permission-handler
  returned bare `1` without writing a sentinel — stableboy would hang waiting for a
  signal that never arrived.
- `cleanupSentinelFiles` now accepts an `io.Writer` for stderr and logs any
  `os.Remove` error that is not `ErrNotExist`. Previously all removal errors were
  silently discarded, leaving stale files that could cause subsequent runs to fail
  silently.
- `TestSentinelContent`: switched assertions from `strings.Contains` line checks to
  exact byte equality. Line order in the sentinel is load-bearing (stableboy reads
  line 1 as the status header); the old checks did not catch reordering.
- `TestWriteSentinel`: same exact-equality fix for the content assertion.
- `TestSessionEndFields`: replaced hardcoded `/tmp/...` path for the missing-file
  case with `filepath.Join(t.TempDir(), "no-such-file.ndjson")` for proper test
  isolation; replaced brittle name-string dispatch with a `missingFile bool` field.
- `TestWriteSentinel`: added subtest asserting that a missing parent directory logs
  an error to stderr and leaves no sentinel file behind.

## v0.7.0 — 2026-05-11

### Added

- `avenor run --sentinel-file <path>`: write a completion sentinel file after every run
  (clean, timeout, signal, or error), matching the output format of `dispatch-avenor.sh`.
  - Sentinel format by exit code:
    - `0` → `DONE\nSESSION=...\nSTOP_REASON=...\n` (default stop_reason: `end_turn`)
    - `124` → `TIMEOUT\nSESSION=...\nSTOP_REASON=...\n` (default: `timeout`)
    - `130` → `KILLED\nSESSION=...\nSTOP_REASON=...\nEXIT_CODE=130\n` (default: `cancelled`)
    - other → `FAILED\nSESSION=...\nSTOP_REASON=...\nEXIT_CODE=N\n` (default: `exit_N`)
  - Session metadata is extracted from the last `session.end` event in the event log.
  - Sentinel is written via atomic tmp+rename to avoid partial-file races on the watcher side.
- When `--sentinel-file` is set and `--permission-handler` is not explicitly provided,
  `avenor run` auto-derives the permission handler base from the sentinel path:
  `<base>.done` → `<base>.perm`; any other suffix → `<sentinel>.perm`.
  This mirrors the `derive_permission_base` function in `dispatch-avenor.sh`.
  An explicit `--permission-handler` always wins — derivation is skipped.
- Pre-run cleanup when `--sentinel-file` is active: removes the existing sentinel and the
  permission request files (`<perm-base>.req`, `<perm-base>.req.response`) before each run,
  matching the shim's pre-run setup. Callers without `--sentinel-file` see no change.

### Notes

- This release deprecates `dispatch-avenor.sh` consumer-side. Callers can now pass
  `--sentinel-file <path>` directly to `avenor run` instead of routing through the shim.
  Consumer cutover (deleting `dispatch-avenor.sh` from `.botfiles`) is a follow-on step.
- Callers that do not pass `--sentinel-file` see zero behavior change.

## v0.6.0 — 2026-05-11

### Added

- `avenor watch --classify`: prefix each plain-format digest line with `MILESTONE`,
  `FINDING`, or `ACTIVITY` based on event type and content heuristics.
  - **MILESTONE**: `session.end`, `permission.request`, `tool.call`/`tool.call_update`
    where `kind==commit`, `kind==task` with `status==completed`, or title/command
    contains `git commit`.
  - **FINDING**: message/thought/user chunks containing `[finding]` marker,
    `reviewer flagged`, `correction needed`, `failed test`, or `confidence: N%`
    where N ≥ 60.
  - **ACTIVITY**: everything else (default).
- `--format json --classify`: injects a top-level `"classify"` field into each
  emitted JSON object without altering other fields.
- Classification rules live in `internal/digest/classify.go` as the single source
  of truth; encoding what was previously prose in `agents/groom.md`.

## v0.5.0 — 2026-05-11

### Added

- `avenor answer <perm-base>` subcommand: reads `<perm-base>.req`, validates
  `--option` against the offered set, and atomically writes
  `<perm-base>.req.response`. Flags: `--option <id>` (required),
  `--message <text>` (optional), `--outcome selected|cancelled` (default
  `selected`), `--force` (overwrite existing response).
- Replaces the printf/jq response-write block in `opencode/skills/answer-jockey`
  and `agents/groom.md` (Stage 3 of the avenor-subsume-consumer-prose refactor).

## v0.4.1 — 2026-05-11

### Fixed

- `avenor watch` plain mode now emits nothing for JSON lines lacking an `event` field. Previously it emitted `EVENT   ` noise for legacy text-protocol input that had no event key.

## v0.4.0 — 2026-05-11

### Added

- `avenor watch --since-cursor <path>`: persist byte offset to a cursor file, atomically rewrite the cursor on EOF, rewrite every 10 events in follow mode.

## v0.3.0 — 2026-05-11

### Added

- `avenor watch <log>` subcommand: plain digest format (`EVENT name session_id excerpt`), per-event excerpt mapping (`.content.text` for chunks, `kind:title [status]` for tools, etc.), `--follow` and `--format json` flags.

## v0.2.0 — 2026-05-11

### Added

- Documented the `--permission-handler file:<path>` flow end to end (`docs/permission-handler.md` was added in v0.1.0; v0.2.0 promotes it to README + consumer integration story).
- `templates/opencode/skills/answer-jockey/SKILL.md` is now the reference consumer skill for responding to permission requests (installed downstream in `.botfiles`).

### Notes

- Version tag is now injected via ldflags (`-X main.Version={{ .Version }}`), so `avenor --version` reports the release tag instead of a hardcoded constant.

## v0.1.1 — 2026-05-11

### Changed

- `cmd/avenor/main.go`: replaced `const Version` with `var Version` so goreleaser's `-ldflags -X main.Version=...` injects the real tag. Previously `avenor --version` reported `v0.0.1` regardless of the release.

## v0.1.0 — 2026-05-11

### Added

- Initial MVP CLI: `avenor`, `avenor probe`, `--prompt-file`, `--on-event`, `--dir`, `--model`, `--timeout`, `--agent`.
- `opencode-acp` runtime adapter (`internal/runtime/opencodeacp/`).
- File permission handler (`--permission-handler file:<path>`, `internal/permission/`).
- NDJSON event vocabulary (`agent.message_chunk`, `agent.thought_chunk`, `tool.call`, `tool.call_update`, `user.message_chunk`, `session.plan`, `permission.request`, `session.end`); `docs/events.md`.
- Stop reasons: `end_turn` / `cancelled` / `max_tokens` / `max_turn_requests` / `refusal` / `timeout`. Exit map: `end_turn=0`, `refusal=2`, `max_tokens=3`, `max_turn_requests=4`, `cancelled=130`, `timeout=124`.

## v0.0.1 — 2026-05-11

Initial scaffolding.
