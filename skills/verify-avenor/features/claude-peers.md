# Claude peer layer — the plugin mods layer (issue #391, PR 1)

`marketplace/plugins/avenor/hooks/` makes a Claude Code session itself a
native avenor broker peer: outbound `SendMessage({to: "avenor:<run or label>"})`
is delivered through the broker (and answered `{isDelivered:true}` by the
`session.send` hook), inbound asks are long-polled off `POST /poll-control`
and submitted as prompts when the session is idle, and the avenor message
tool's `ask` action is served against the session's own broker credentials,
blocking on `/wait_reply` and retracting the edge when that wait fails, while
its `reply` action answers an ask the session itself received. Trust comes
from the broker's authenticated run identity, never from message text.

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
   edge consumed, nothing left pending. Leave it unanswered and the result is
   `{"action":"ask","outcome":"timed-out","message_id":"av1...","text":"no
   reply to \"av1...\" from \"<peer>\" before the timeout; withdraw it with
   avenor_message action=\"withdraw\" message_id=\"av1...\""}` — the sentence
   every surface renders, not raw HTTP text.
4. In the session that received an ask, answer it with the message tool's
   `reply` action (`{action: "reply", reply_to: "<message id from the
   frame>", message: "…"}`). Proof: the tool result is the router's reply
   shape `{"action":"reply","outcome":"replied","message_id":"av1...",
   "text":"reply to \"av1...\" delivered"}` and the asker's `ask` unblocks;
   answering the same id again passes through to the MCP server instead, whose
   `edge-absent` result is the honest answer for an ask the hook no longer
   holds.

The session transcript JSONL under `~/.claude/projects/<cwd slug>/` is the
verbatim record (the tmux pane renderer can lag behind a completed turn; read
the transcript, not the pane). `claude plugin validate <plugin dir>` covers
the manifest and hooks shape; the module scan's verdict is whatever the
session prints on `/reload-plugins` ("reloaded (5 hooks: ...)").

## Proof state

`feature-map acceptance` = all four drives above with both transcripts
captured, plus `claude plugin validate` exit 0 and the offline suites: pure
modules under
`node --test marketplace/plugins/avenor/test/addresses.test.mjs marketplace/plugins/avenor/test/wire.test.mjs`
(address ladder incl. empty refs and cross-row run-id wins, wire helpers, the
mirrored ask and reply sentences, the escaping a peer-controlled fact cannot
slip through — backslash, bidi, astral format characters — and the failure
classification — name files explicitly; node 24 treats a bare directory as a
module path) and the committed live-broker hook-contract drive
`node marketplace/plugins/avenor/test/preflight.mjs http://127.0.0.1:<port>`
against a quiet `avenor stable` (self-registration/naming, idle and
subagent-turn idle delivery, tell sends ride no edge, ask correlation,
reply-edge unblock, the `reply` action answering the ask the hook polled and
passing an unknown `reply_to` through, busy-path append, the ask tool's reply
and timed-out shapes, a cut `/wait_reply` retracting its edge, a refused
`prompt.submit` falling back to a row while the rest of the batch still lands,
a `/poll-control` failure pausing the loop without killing it, ask-input
refusals that never reach the peer, a partial listing refusing resolution,
one-listing ask pinning, the version-shape matrix, a broker-less session
staying off with one reason line, a stale self-minted name reminted once, and
the old-release disabled pass-through). Every wait in that drive is a
deadline-bounded poll, and it exits with a status: a hang is a failure, not a
slow pass. The PR 1 run's captured evidence (transcripts, validation report,
review grounding drives for turn axes and reload behavior) lives in the
branch's `.cache/runs/evidence/` (uncommitted by repo policy).

## Known limits

- Provisioning (broker URL + run id + token into a spawned child) is PR 2;
  until then children join as self-minted peers or via exported env.
- The reply correlation is a heuristic: a plain `SendMessage` to a peer with
  a pending inbound ask answers the most recent one. The message tool's
  explicit `reply` action names the ask instead, so concurrent asks from one
  peer can each be answered (the table holds the newest 128 unanswered asks).
- A peer listing the broker itself reports as partial refuses every ref, so a
  `SendMessage`/`ask` during a broken peer listing fails with that reason
  rather than resolving against the subset.
- Pass-through turn/session hooks log a benign "hook skipped: returned the
  wrong shape" notice per turn; the events run anyway. Aborted turns still
  emit `turn.complete` (`reason: "aborted"`, verified live), so the busy/idle
  counter drains.
- A hot `/reload-plugins` re-runs no `session.start`; the armed layer keeps
  serving the session unchanged across the reload (verified both directions
  live). Restart the session to run new module code.