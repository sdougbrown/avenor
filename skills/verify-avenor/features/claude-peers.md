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
`AVENOR_SESSION_NAME` — the repo's own peer-credential vocabulary — or the
`AVENOR_LABEL` every avenor launch exports, which is what the layer falls back
to for a display name), against an `avenor stable` supervisor whose broker that
URL names. The layer logs
"peer layer on" at `session.start`, self-registers the session as a broker
run when no token is provisioned, and disables itself below the tested Claude
Code floor (2.1.311) with one log line instead of erroring.

There are two ways to arrive, and the environment picks between them:

- **A session you start by hand** exports the broker URL (and optionally a run
  id) and self-registers through the broker's open `/register` route. That is
  the two-session drive below.
- **A run avenor spawned** with `--backend claude` is provisioned: the
  `claude` provider hands the child the run id, a one-shot attach credential
  (`AVENOR_BROKER_TOKEN`), its label, and the broker URL through the launch
  environment, and the layer trades the credential at `POST /attach` for the
  run's own — joining as that run, the same `rt_N` record `avenor control
  list` shows, under the label it was spawned with, with no `/register` call.
  The credential is spent by the first session that presents it, so a session
  spawned from the child's own shell is refused and the layer logs the
  broker's reason. The plugin has to be installed (user or project scope) for
  the child to load it at all; `--plugin-dir` is not passed to a spawned
  child.

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

## Driving a provisioned child run (PR 2)

One `avenor stable` supervisor, one spawned `claude` backend run, and a parent
session on the same broker:

```sh
# supervisor: private tmux server, the worktree's avenor first on PATH, and
# the tmux launcher for the claude backend
nohup env -i PATH="$PWD:$HOME/.local/bin:/usr/bin:/bin" HOME="$HOME" TMPDIR=/tmp \
  AVENOR_CLAUDE_TERMINAL=tmux TMUX_TMPDIR="$SCRATCH/tmux" \
  ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" ANTHROPIC_BASE_URL="$ANTHROPIC_BASE_URL" \
  ./avenor stable --control-socket "$SCRATCH/stable.sock" --idle-timeout 0 &

# the child; wrap.sh adds --plugin-dir to a claude that cannot be given flags
./avenor control --socket "$SCRATCH/stable.sock" spawn \
  --backend claude --label child-a --dir "$SCRATCH/dir" \
  --launch-wrapper "$SCRATCH/wrap.sh" --auto-approve \
  --prompt-file "$SCRATCH/child-prompt.txt"
```

1. **The child carries the provisioned identity.** `ps eww -p <pane pid>` of
the child's `claude` shows `AVENOR_BROKER_URL`, `AVENOR_RUN_ID=rt_1`,
   `AVENOR_BROKER_TOKEN` (this launch's one-shot attach credential, not the
   run's own), and the launch label once, as `AVENOR_LABEL=child-a`; the
   `tmux new-session` command string carries none of those values, only the
   path of the mode-0600 launch env file it sources; its transcript's first log
   line is `peer layer on (provisioned run rt_1, broker http://…)` right after
   one `POST /attach`, and `GET /sessions` (authenticated with the run token
   the attach answered) lists
   one record per label — `rt_1`/`child-a` — with no `claude-<hex>`
   self-registration beside it. The child writes nothing about its own
   identity: no `/register`, and no `/session_name` either, so `child-a` stays
   the record's own label and a later authorized rename through `/rename` is
   what moves the name peers see. Starting a second `claude` from the child's
   own shell (its inherited environment) is refused: one more `POST /attach`,
   a `409 attach-consumed` log line, and no second poller.
2. **Parent → child.** In the parent: `SendMessage({to: "avenor:child-a"})` →
   `{"success":true,… "Delivered by a plugin hook; nothing else was sent."}`,
   and the child's transcript gains the broker-framed peer row (sender = the
   broker's display name for `claude-parent`).
3. **Child → idle parent.** The child calls the avenor message tool with
   `action:"ask"` against `avenor:claude-parent`; the parent, idle, starts a
   plugin-submitted turn ("Prompt from the avenor plugin … Reply with
   SendMessage to `avenor:rt_1`").
4. **Reply unblocks the wait.** The parent's `SendMessage` to `avenor:rt_1`
   rides that ask's reply edge, and the child's tool result is the router shape
   with `"outcome":"replied"` and the parent's text, carrying the same
   `message_id` the parent's inbound row showed.

Accepting the "trust this folder" dialog once per scratch dir is a one-time
manual keystroke; the panes are interactive sessions, not `-p` runs.

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
passing an unknown `reply_to` through, busy-path append, an unpaired
`turn.complete` clamping at zero with the next live turn still appending, two
outstanding asks from one peer (a plain send riding the newest, the `reply`
action still reaching the older by id), the ask tool's reply
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

The identity drive covers the identities a session can be handed: a launch
carrying broker URL, run id and a credential together selects the provisioned
shape, whose only entry is `POST /attach` — no `/register`, no `/session_name`
write (asserted by request path, with the launch label set as a supervisor
spawn sets it), no run minted, and a refused credential disables the layer with
the broker's reason instead of leaving a second reader on the queue; a run id
without a credential self-registers and posts its name; a bare session mints its
own run and names it; and `/register` answers the run credential with no attach
grant. The drive's provisioned case carries a credential the broker never minted
on purpose: a launch grant is minted in process, by the broker that provisions
the launch, so a broker-only script cannot hold a spendable one. The handshake
itself — grant spent once, the run's own credential returned, a nested session
refused `409 attach-consumed`, the label surviving an authorized rename — is
proven by the spawn drive below and by Go (`internal/runtime/broker/attach_test.go`,
`internal/runtime/claude/peer_test.go`).

PR 2's live proof (2026-10-27, 2.1.311): `.cache/runs/pr2/evidence/proof.md`
— the provisioned-child drive end to end, with `child-run-transcript.md`,
`parent-session-transcript.md`, the `/sessions` listings, and the offline
identity-selection preflight (`preflight.log`, `PREFLIGHT OK`). The same
directory's `scripts/drive.sh` reruns it; the identity preflight's committed
successor is `test/preflight-identity.mjs`, and `evidence/review-fix.md`
records its run on this head (`preflight-identity-review.log`), including the
reverted-fix run that fails it.

## Known limits

- The provisioned child's broker record belongs to the supervisor: it is
  retired with the supervisor, not when the child's session ends (after a kill
  or `cancel` it is listed with `status: ended`). The provider does not delete
  a run it did not mint — a second record under one label is what makes
  `avenor:<label>` ambiguous, and a resume would need a record that still
  authenticates.
- A hosted child never inherits a launching session's peer credentials:
  `runtime.ScrubbedEnvNames` includes the three peer names (and the retired
  `AVENOR_SESSION_NAME` a launcher may still hold), and every spawning backend
  strips that list through `runtime.LaunchEnv` — the terminal launchers unset it
  in the pane command — so a backend that provisions nothing cannot pass a
  launcher's credential down either. A launch then provisions only its own, so
  the plugin in one session cannot make its children join as itself — and a
  session that inherits a provisioned environment anyway (a `claude` started
  from inside the child) is refused at `/attach`, its credential already spent.
  A supervisor started inside a provisioned session therefore inherits no run to
  hand its own children: it is a root broker unless the parent-link pair names
  another one.
  A backend that authenticates each request itself rather than trading a
  credential (pi's extension) still receives the run's own token, so a nested
  pi process could poll with it; the claude layer's single-consumer guarantee
  is the attach handshake, not the environment.
- The plugin must be installed for a spawned child to load it (`--plugin-dir`
  is the parent's way in, and a launch wrapper is the e2e's stand-in).
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