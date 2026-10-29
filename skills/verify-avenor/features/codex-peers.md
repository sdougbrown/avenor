# Codex peer layer — plugin command hooks (issue #486)

`marketplace/plugins/avenor/hooks/codex.json` makes an interactive Codex
session itself a broker peer. Command hooks cannot share module memory the way
Claude's mods layer does, so the session's identity lives in a mode-0600 record
under `$AVENOR_HOME/peers/codex/<session id>/identity.json`, and every hook reads
it from there:

- **Register:** `SessionStart` trades a provisioned attach credential
  (`AVENOR_RUN_ID` + `AVENOR_BROKER_TOKEN`) once, or self-registers through
  `/register` and names itself through `/rename`. A live record is reused on
  resume, clear and compaction; a fork has a new thread id and mints its own
  peer.
- **Serve tools:** `PreToolUse` on `mcp__avenor__(message|peers)` runs the
  actions this session owns — `message` `tell`/`ask`/`reply`/`withdraw` and
  `peers` `rename` — over the session's own broker credentials. No tool argument
  selects an identity: the hook reads the record its own session wrote and
  answers the call with the routed result as the denial reason. The hook also
  serves `peers list` and `peers last`, because they must read the session's
  broker rather than the MCP server's default supervisor. `message resume`
  passes through to the owner path.
- **Deliver inbound:** a background `SessionStart` hook long-polls
  `/poll-control` and queues each framed message into the thread with
  `codex queue --thread <session id> --message <frame>`. Codex holds the
  message while busy and starts a turn when idle. Each ask's id is recorded so
  the MCP server can claim it for `reply`.
- **End:** `SessionEnd` posts `/finish` with status `ended`.

## How a user reaches it

Install the plugin in Codex, start a session in an environment carrying
`AVENOR_BROKER_URL` (plus `AVENOR_LABEL` for a display name), and run an
`avenor stable` supervisor whose broker that URL names. The session prints
`Avenor peer layer on:` as developer context and logs
`peer layer on (self-registered run …)` under the peer record. A session the
supervisor spawned carries the provisioned pair instead; a launcher-owned Codex
child (`AVENOR_RUNTIME_ID` with no `AVENOR_RUN_ID`) stays off until step 5 of
the issue aligns its provider identity.

## Driving it

Scratch layout under `.cache/verify/codex-peers/`, a short `AVENOR_HOME`, and
`go build -o $AVENOR_BIN ./cmd/avenor`. Start the supervisor in a persistent
pane (a background process from a script dies with its launcher):

```sh
tmux new-window -d -n peersup "cd $REPO && env -i PATH=$PWD:$HOME/.local/bin:/usr/bin:/bin \
  HOME=$HOME TMPDIR=/tmp AVENOR_HOME=$SCRATCH/home \
  $AVENOR_BIN stable --control-socket $SCRATCH/live.sock --idle-timeout 0; exec bash"
BROKER=$(cat $SCRATCH/live.sock.broker.url)
```

Install the plugin into a scratch `CODEX_HOME` and launch two sessions with
distinct labels. An initial prompt is what makes this build emit
`SessionStart`; later turns are driven with `codex queue`:

```sh
CODEX_HOME=$SCRATCH/codex codex plugin marketplace add "$REPO/marketplace"
CODEX_HOME=$SCRATCH/codex codex plugin add avenor@avenor
# config.toml: model, [features] hooks = true, [mcp_servers.avenor] pointing at
# $AVENOR_BIN with default_tools_approval_mode = "approve"
AVENOR_HOME=$SCRATCH/home AVENOR_BROKER_URL=$BROKER AVENOR_LABEL=codex-a \
  CODEX_HOME=$SCRATCH/codex codex --dangerously-bypass-hook-trust 'Say ready'
```

1. **Two distinct peers.** `GET /sessions` authenticated as either peer lists
   `codex-<hex>` records carrying `codex-a` and `codex-b`. `peers list` in one
   session shows the other, itself and `supervisor`.
2. **Rename is self-only.** `peers rename` with no `session_id` moves that
   peer's label and leaves the other peer and `supervisor` untouched.
3. **Tell.** `message tell` sends as the session's own run; the recipient's
   frame names the broker-stamped sender, and a plain tell carries no reply
   instruction.
4. **Idle delivery and ask/reply.** With the recipient idle, `message ask`
   queues a turn in it; the recipient answers with `action: "reply"` and
   `reply_to: "<id>"`, and the asker's result is
   `{"action":"ask","outcome":"replied","reply":"…"}`.
5. **Resume.** `codex resume <thread>` reuses the same run id (the log records
   `reused run …`), adds no second record, and re-arms the poller on the next
   prompt; a post-resume tell lands.
6. **End.** Closing the session records `session ended` and posts `/finish`.

## Proof state

Driven live on 2026-10-28 (Codex 0.165.0, local `deepseek-v4.1-flash:cloud`,
scratch broker): two peers listed with their labels; rename moved only the
renaming peer; tell rendered the sender and produced no ack loop; ask returned
`outcome: replied` with `4`; a resumed peer reused `codex-746377`, re-armed its
poller, and received `post-resume ping`; closing a session logged
`session ended`. Captures live in `.cache/spike-486/codex-peers/`. Offline:
`node --test marketplace/plugins/avenor/test/codex-hooks.test.mjs` and
`go test ./internal/mcpserver -run 'Codex|PeerSurface|AskClaim|SplitCodex'`.

## Known limits

- **Spawned Codex children are deferred** (issue #486 step 5). A launcher-owned
  child stays off rather than minting a duplicate identity beside the provider's
  own broker run.
- **`codex resume` emits no `SessionStart`** in this build; the same
  registration runs on `UserPromptSubmit`, so a resumed thread re-arms on its
  next prompt but cannot receive a peer message before that prompt.
- **`--dangerously-bypass-hook-trust` forces embedded mode** in the TUI. Plugin
  hooks still run; the shared background app-server does not.
- A session with no `AVENOR_LABEL` is displayed by its run id until renamed.
- Hook-served peer actions arrive as a tool denial whose reason is the result
  (`message "av1…" delivered to "codex-b"`). The reason ends with a line telling
  the model not to retry; there is no success-shaped result on this host.
- The run credential lives in the mode-0600 session record because the poller
  and each hook invocation are separate processes. A shell-capable injection in
  the same user account can read it; the tool-argument identity primitive is
  gone, but the same-user boundary is not a security boundary.
- The poller is a background hook with a seven-day timeout; a longer session
  re-arms on its next prompt.
- `AVENOR_CODEX_BIN` overrides the `codex` used for delivery when the binary is
  not on the hook's `PATH`.
