# Messaging — the message tool and the peers tool

The merged surface that replaced the separate send/ask/reply/cancel/follow-up
tools. One tool routes six actions, one tool lists/renames/reads sessions, and
every surface returns the same snake_case result object
(`schemas/message_route.result.json`).

## Sub-features

`avenor_message` (MCP surfaces register the same tool as `message`):

- `tell` — deliver text, no reply edge. Result: `sent` with `message_id`.
- `ask` — deliver text and wait for one reply. `timeout_ms` is a whole number of
  milliseconds, `0`–`600000`; absent or zero uses the shared 600 s cap. Results:
  `replied` (with `reply`), `timed-out`, `withdrawn`, `edge-absent`,
  `possibly-pending`, `lost`, `not-sent`, `auth-rejected`.
- `reply` — answer an inbound ask: `reply_to` is the ask's message id, `message`
  is the answer. `reply` reports `edge-absent` on any surface that never
  receives asks (the OpenCode plugin, the TypeScript MCP host).
- `withdraw` — retire a pending ask by `message_id`.
- `resume` — continue a finished run as a new run: a new public run id, the
  prior label plus `-followup` unless `label` overrides it, and the
  `sentinel_file`/`on_event` paths the new run minted.
- `control_reply` — claude-channel only; every other surface reports
  `unsupported_action`.

`avenor_peers` (MCP: `peers`):

- `list` (the default) — one record per visible session: `run_id`, `label`,
  `kind`, `status`, `backend`, `model`, `dir`.
- `rename` — `name` (and optional `session_id`): the caller renames itself, the
  owner renames the supervisor. Result: `renamed {run_id, label,
  previous_label}`; the persisted label shows in the next `list` and a
  `session.renamed` line appears on the **control stream**, not in the target's
  event log.
- `last` — read one peer's recorded last agent message (`target`: run id, label
  or `self`). Result: `last {run_id, runtime_id, label, final_output,
  stop_reason, final_output_truncated}` plus rendered text.

## How to get to it (user POV)

A host agent session owns the supervisor and its broker: a `pi` session carrying
the `@dougbots/avenor-pi` extension (host mode) or an MCP host attached to
`avenor mcp`. It spawns children; children talk to it and to each other. A child
gets the same two tools over its inherited broker (the extension in sub-agent
mode, or the Go `avenor channel-tools` MCP server that the `opencode-acp`
backend injects).

## Driving it with the avenor binary

Two drives matter. Work inside one scratch dir under the checkout's `.cache`
(never `/tmp`) and one evidence dir that outlives cleanup.

```sh
REPO=$PWD
SCRATCH=$REPO/.cache/verify/v8/messaging      # keep this short: see Gotchas
EVIDENCE=$REPO/.cache/verify/evidence/$(date +%Y%m%d-%H%M%S)
mkdir -p "$SCRATCH/work" "$SCRATCH/avenor-home" "$EVIDENCE"
export AVENOR_HOME=$SCRATCH/avenor-home
export AVENOR_BIN=$REPO/.cache/avenor-bin
go build -o "$AVENOR_BIN" ./cmd/avenor
mise run js-pi-build
```

Spawned `pi` children load the installed `@dougbots/avenor-pi`, so a drive must
point them at this checkout's build. Children are launched as plain `pi`, so put
a shim first on `PATH` (the supervisor and its children inherit it); the host
itself is launched with the extension named explicitly:

```sh
mkdir -p "$REPO/.cache/verify/bin"
cat > "$REPO/.cache/verify/bin/pi" <<EOF
#!/bin/sh
exec $(command -v pi) --no-extensions --extension "$REPO/packages/pi/dist/index.js" "\$@"
EOF
chmod +x "$REPO/.cache/verify/bin/pi"
export PATH=$REPO/.cache/verify/bin:$PATH
```

Launch the host as a long-lived RPC session (no `AVENOR_BROKER_*` in its
environment, so it is a host, not a child). RPC is what makes every tool call a
structured record:

```sh
cd "$SCRATCH/work"
pi --mode rpc --no-session --no-extensions \
  -e "$REPO/packages/pi/dist/index.js" \
  --no-skills --no-context-files --model deepseek/deepseek-flash \
  > "$EVIDENCE/host-rpc.ndjson" 2> "$EVIDENCE/host-rpc.stderr"
```

`avenor_spawn` takes `model` too; pass `model "deepseek/deepseek-flash"` to keep
spawned pi children on the same model as the host (without it a pi child uses
the default model from the user's pi settings).

Drive it with a strict-JSONL client (split stdout on `\n` only) that sends one
`prompt` and waits for `agent_settled` before the next. Prompt sequence that
proves the host legs:

1. `Call avenor_spawn with dir "$SCRATCH/work", prompt "Reply with the single word OK and nothing else.", label "host-child-c", backend "pi", wait false.` then `Call avenor_result with run_id "host-child-c" wait true.` — the spawn result carries `run_id`, `label`, `effective_backend`; the result carries `runtime_id`, `session_id`, `stop_reason`, `output`.
2. Spawn a second child that stays busy: `prompt "Use the bash tool to run: sleep 900. Then reply with the single word OK.", label "sleeper-pi", backend "pi", wait false.` — take its `runtime_id` from `avenor_status run_id "sleeper-pi" view "full"`.
3. `Call avenor_message with action "resume", target "host-child-c", message "Reply with the single word AGAIN and nothing else."` — this is a **pi** child, and pi runs `--no-session`, so the answer is the documented refusal (`session not found ... cross-process resume is not supported`). Resuming an `opencode-acp` child with the same prompt returns `outcome: resumed`, a new `run_id`, the `-followup` label, and the new run's `sentinel_file`/`on_event`; the resumed sentinel keeps the same `SESSION=` as the original.
4. `Call avenor_message with action "tell", target "host-child-c", message "ping"` after that run ends — a tool error whose repair sentence says `run "rt_1" is done; use action "resume" to continue it`. (A tool error carries no `details` on this surface; the sentence is the observable.)
5. `Call avenor_peers with action "rename", name "host-renamed-supervisor"` then `Call avenor_peers` — the rename returns `renamed {run_id: "supervisor", label, previous_label: "supervisor"}` and the next list shows the new label with `kind: "supervisor"`.
6. `Call avenor_workflow_cancel with workflow_id "wf-does-not-exist", actor "verify", reason "verify"` — the tool is registered on the host surface and fails with `rpc error [-32000]: workflow not found: wf-does-not-exist`.

Start a control-stream tail before the rename (`./avenor control --socket
"$AVENOR_HOME"/sockets/*.sock tail > "$EVIDENCE/control-tail.ndjson"`); the
`session.renamed` line lives only there.

The same host session drives an `opencode-acp` child over the Go child surface
(`avenor channel-tools`, injected as MCP). Spawn it with `backend
"opencode-acp"` and a prompt that runs: `peers`, `peers rename`, `tell
supervisor`, `ask` the busy sleeper with `timeout_ms` 1500, `withdraw` that
`message_id`, `resume target "self"`, `control_reply`, `peers last supervisor`,
then write its results to `$SCRATCH/child-d.json`.

**Observable end state (the proof).** All of it is structured, never transcript
prose:

- `host-rpc.ndjson`: one `tool_execution_end` per `avenor_message`/`avenor_peers`
  call whose `result.details` is the snake_case object (`outcome`, `run_id`,
  `sentinel_file`, `on_event`, `renamed`, `last`, …). Tool denials are
  `isError: true` with `details: {}` and the shared sentence in
  `result.content[0].text`, so a denial is proven by that sentence.
- The child's own event log (`$AVENOR_HOME/runs/<public run id>/events.log`):
  `tool.call`/`tool.call_update` carrying `rawInput` and the tool result. A pi
  child records the structured result as `result.details` on the completed
  `tool.call_update`; the `opencode-acp` child records `rawOutput.output` (the
  render text) plus `rawInput`. The public run id is the spawn result's
  `run_id`, never the label.
- Sentinels at the paths the resume result returned: `DONE`, `SESSION=`,
  `STOP_REASON=`; the original and the resumed run share `SESSION=` and differ
  in `RUN=`.
- `control-tail.ndjson`: `session.renamed` with `runtime_id`, `label`,
  `previous_label` (`supervisor` for a supervisor rename, the child's `rt_N` for
  a self rename).
- `$AVENOR_HOME/sockets/*.sock`: shut the supervisor down through the control
  plane (`avenor control --socket <sock> shutdown graceful`) and close the
  host's stdin.

### What `last` proves, and its three shapes

`last` is a **record read, not a transcript read and not a live poll**. The
record is written when a run's turn ends: `broker.Recorder.Feed` on the child's
`session.end`, and the host's own `turn_end` handler (`session_last_set`) on Pi
or per completed assistant `message.updated` in the OpenCode plugin.

- `header` + `final_output` when the record carries text: the rendered text is
  `**Last message from "<label>"** (run "<run_id>", stop reason: <reason>)`
  followed by the text. This is how a peer reads another agent's own words.
- `no_text` when the record exists with empty text: `run <id> finished (stop
  reason: <reason>) with no final text`.
- `empty` when no record exists yet: `run <id> has no completed turn yet`, with
  empty `final_output` and `stop_reason` in the structured object. A child that
  is blocked in its first `ask` is the deterministic way to see it.
- `stop_reason` comes from the backend's own terminal vocabulary (`end_turn`,
  `tool_use`, `aborted`, `error`, …), so a record's presence also proves the
  turn producer ran.

### The ask-serialisation rule

An `ask` blocks its caller, and an inbound ask reaches the target as a steer
(`pi.sendUserMessage(..., {deliverAs: 'steer'})`), which the model can only
answer between its own tool calls. Two agents that ask each other at the same
moment deadlock until a timeout. Any multi-agent drive must therefore give each
asker a window in which the peer is idle: a barrier ask, or a `bash sleep` in
the responder's own turn, and a generous `timeout_ms`. A `timed-out` mesh ask
means the ordering rule was broken, not that the tool misbehaved.

### A mesh's expected evidence

For three agents (host + two children) each must see the other two: every
`peers list` names the other two (a child sees the supervisor as
`run_id: "supervisor"`, `kind: "supervisor"`), all six ordered pairs exchange a
`sent` tell and a `replied` ask whose `reply` names the responder, every agent
reads the other two with `last`, and no result reports `withdrawn` or
`timed-out` on its own.

## Known limits today

Read these against the binary before promising the drive above works end to end.

- **A pi child's `ask` is refused.** The child mints a versioned message id and
  posts `/send` without the broker generation, so the broker answers
  `409 generation-mismatch` and the tool reports
  `<id> was not sent to "<target>"; nothing is pending`. `tell` still works (it
  takes the fire-and-forget path, which does not check the generation), and the
  Go child (`opencode-acp`) pins the generation, so its asks work.
- **`resume` of a `pi` run is refused.** pi sessions run `--no-session`, so a
  finished pi child cannot be continued from another process; the tool says so
  explicitly. Resume of an `opencode-acp` child works and keeps the session id.
- **`timeout_ms` is unusable through an MCP client that consumes
  `schemas/message.input.json`.** The schema is a `oneOf` union, and the
  installed OpenCode 1.15.5 sends `"timeout_ms": "1500"` (a string) for it while
  it sends `7` as a number for a flat `{"type":"integer"}` tool schema. The
  router correctly rejects the string (`property "timeout_ms" must be an
  integer`), so an `opencode-acp` child cannot pass a timeout; the Pi tools use
  TypeBox and are unaffected.
- **A `tell` to the host arrives as an ask-shaped steer.** The host's ask poll
  drains the supervisor's control queue, which also carries fire-and-forget
  messages; such a message has no `message_id`, so the delivered steer instructs
  `reply_to: ""` and an answering model gets `reply ... edge-absent`.

## Gotchas

- **Keep `$AVENOR_HOME` short.** The supervisor binds
  `$AVENOR_HOME/sockets/avenor-mcp-<pid>.sock`; a path over roughly 107 bytes
  fails to bind and the extension reports `avenor exited with code 1 during
  startup` (a `.sock.dead` tombstone with `reason=start_failed` is the tell).
  `.cache/verify/v8/<drive>/avenor-home` is short enough; a deeper name is not.
- **Never install the extension into `~/.pi`.** Pass it with `-e` for the host and
  through the `pi` shim for children.
- **`opencode` needs a config it accepts.** The installed
  `~/.config/opencode/opencode.json` is rejected by opencode 1.15.5
  (`Unrecognized key: subagent_depth`, then missing `modalities.output`), and
  `opencode acp` exits before the ACP handshake. Point `XDG_CONFIG_HOME` at a
  scratch config (model + provider only, no plugins) for the child; the avenor
  backend merges its own MCP server into that.
- A child's ask of a *finished* run is queued with no reader and the asker waits
  out `timeout_ms`; `target-unavailable` means the run is not registered, not
  that the broker is down.
- A rename replaces the label (it is an alias, not an extra name): after a
  child renames itself, `target "<old label>"` answers
  `no run matches "<old label>"`; address it by its new label or its run id.
- The spawn argument is `dir`, not `repo_dir`; a `tell`/`ask` target is a run id, a
  label, `self`, or `supervisor` — a public run id is a host-surface concept, not
  a broker address.
