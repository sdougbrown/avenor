# Claude peer layer — the plugin mods layer (issue #391, PR 1)

`marketplace/plugins/avenor/hooks/` makes a Claude Code session itself a
native avenor broker peer: outbound `SendMessage({to: "avenor:<run or label>"})`
is delivered through the broker (and answered `{isDelivered:true}` by the
`session.send` hook), inbound asks are long-polled off `POST /poll-control`
and submitted as prompts when the session is idle, and the avenor message
tool's `ask` action is served against the session's own broker credentials,
blocking on `/wait_reply`. Trust comes from the broker's authenticated run
identity, never from message text.

## How a user reaches it

Load the plugin in a Claude session whose environment carries
`AVENOR_BROKER_URL` (plus optional `AVENOR_RUN_ID`, `AVENOR_BROKER_TOKEN`,
`AVENOR_SESSION_NAME` — the repo's own peer-credential vocabulary), against an
`avenor stable` supervisor whose broker that URL names. The layer logs
"peer layer on" at `session.start`, self-registers the session as a broker
run when no token is provisioned, and disables itself below the tested Claude
Code floor (2.1.311) with one log line instead of erroring.

## Driving it

Two Claude sessions on one machine, both with the plugin loaded and the same
broker URL in their environment; the supervisor is the harness's own
`avenor stable` (record its PID at launch, as `stable-control.md` says):

1. In the parent: ask for `SendMessage` to `avenor:<run label>`. Proof: the
   tool result `{"success":true,"message":"Delivered by a plugin hook;
   nothing else was sent."}` and the recipient session's transcript showing
   the broker-framed peer message (sender = the broker's display name for the
   run, never message text).
2. Let the recipient reply with `SendMessage` to `avenor:claude-parent`: the
   reply lands on the parent as a prompt the plugin submits between turns
   ("The avenor plugin sent a message: ... Address the message above.").
3. In one session: call the avenor message tool with `action: "ask"` against
   the other's `avenor:` address while that session idles. Proof: the idle
   session starts a plugin-submitted turn on the ask; the asker's tool result
   is the router shape `{"action":"ask","outcome":"replied","message_id":
   "av1...","text":"pong","reply":"pong"}` once the peer answers — the reply
   edge consumed, nothing left pending.

The session transcript JSONL under `~/.claude/projects/<cwd slug>/` is the
verbatim record (the tmux pane renderer can lag behind a completed turn; read
the transcript, not the pane). `claude plugin validate <plugin dir>` covers
the manifest and hooks shape; the module scan's verdict is whatever the
session prints on `/reload-plugins` ("reloaded (5 hooks: ...)").

## Proof state

`feature-map acceptance` = all three drives above with both transcripts
captured, plus `claude plugin validate` exit 0 and the offline suites: pure
modules under
`node --test marketplace/plugins/avenor/test/addresses.test.mjs marketplace/plugins/avenor/test/wire.test.mjs`
(address ladder, wire helpers — name files explicitly; node 24 treats a bare
directory as a module path) and the committed live-broker hook-contract drive
`node marketplace/plugins/avenor/test/preflight.mjs http://127.0.0.1:<port>`
against a quiet `avenor stable` (self-registration/naming, idle tell
delivery, tell sends ride no edge, ask correlation, reply-edge unblock,
busy-path append, ask-tool service, version gate, disabled pass-through).
The PR 1 run's captured evidence (transcripts, validation report, review
grounding drives for turn axes and reload behavior) lives in the branch's
`.cache/runs/evidence/` (uncommitted by repo policy).

## Known limits

- Provisioning (broker URL + run id + token into a spawned child) is PR 2;
  until then children join as self-minted peers or via exported env.
- The reply correlation is a heuristic: a send to a peer with a pending
  inbound ask answers the most recent one.
- Pass-through turn/session hooks log a benign "hook skipped: returned the
  wrong shape" notice per turn; the events run anyway. Aborted turns still
  emit `turn.complete` (`reason: "aborted"`, verified live), so the busy/idle
  counter drains.
- A hot `/reload-plugins` re-runs no `session.start`; the armed layer keeps
  serving the session unchanged across the reload (verified both directions
  live). Restart the session to run new module code.