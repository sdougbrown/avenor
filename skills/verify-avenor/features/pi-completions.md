# Pi completion notices — footer status and `avenor-completion` delivery

The Pi-side half of a run's life: how a finished sub-agent reaches the model that
spawned it. The binary and its supervisor still run the child; what is under test
here is the notification path owned by the Pi extension, not CLI output.

## Sub-features

- Footer status key `avenor`: one line per active run, then `total:N` (live tree
  size, only once discovered active descendants exist), `unread:N` (notices
  waiting for a settlement to carry them), `errors:N` (poll failures recorded so
  far in the current poll cycle, cleared by a cycle that adds none). An absent
  token is zero.
- `unread:N` covers every terminal status — `done`, `failed`, `timeout`, `killed`.
- Consuming a result: `avenor_result` (or a `wait: true` spawn) that returns
  `ready` drops that run's queued notice, by label or by canonical run id, and
  the count falls by one. Nothing for that run is delivered afterwards.
- Automatic delivery: at `agent_before_settle` with outcome `completed`, each
  queued notice is appended as its own `custom_message` with customType
  `avenor-completion` (carrying that run's `run_id` and `status`), and one
  continuation covers the batch — one wake-up, one message per run.
- Idle delivery: a run that finishes while the session is idle is sent with
  `sendUserMessage`, so it arrives as an ordinary user-role message and starts
  its own turn. Nothing is queued, and no `avenor-completion` message exists.
- Retention: a settlement that ends in abort or error delivers nothing; the
  notices stay queued. Session shutdown clears the queue.

## How to get to it (user POV)

A host `pi` session with `@dougbots/avenor-pi` loaded spawns a child with
`wait: false` and keeps working. When the child finishes, the user either sees
the result arrive as a prompt they did not type (session idle) or sees `unread:1`
in the footer and gets the notice batched in once the current turn ends normally.
Asking for the result first — `avenor_result` with the label — consumes the run
and nothing arrives.

## Driving it with the avenor binary

Setup is the [messaging.md](messaging.md#driving-it-with-the-avenor-binary)
recipe and is not repeated here: scratch layout under `.cache/verify/`, a short
`$AVENOR_HOME`, `go build -o $AVENOR_BIN ./cmd/avenor`, `mise run js-build` for
`packages/pi/dist`, the `pi` PATH shim that loads this checkout's
`dist/index.js` for children, and the host launched as
`pi --mode rpc --no-session --no-extensions -e $REPO/packages/pi/dist/index.js`
with a strict-JSONL client. Read its Gotchas before choosing a scratch path.

Three deltas for this surface:

- The host environment must carry no `AVENOR_BROKER_*` / `AVENOR_RUN_ID` entries,
  or it runs as a child and never owns the supervisor.
- Pass `model` to the host **and** to every `avenor_spawn` call, or the children
  drift to the user's default model and the drive gets slower than the gate.
- `pi --version` must be 1.4.0 or newer: busy-session delivery needs the
  `agent_before_settle` hook, so without it the busy legs cannot be driven.

### The gate

Nothing here waits on a fixed sleep. Each drive makes the host provably busy by
running a bash command that polls for a marker file, and the observer (whatever
reads the RPC stream) writes that marker only after it has seen the footer
record it is waiting for. A `setStatus` call surfaces on the RPC stream as an
`extension_ui_request` record with `method: "setStatus"`, `statusKey: "avenor"`
and `statusText` holding the whole footer string.

```sh
GATE=$SCRATCH/markers
mkdir -p "$GATE"   # only the observer writes here; host and child just poll
# one bash tool call inside the host: waits up to 10 minutes for the observer
for i in $(seq 1 2400); do [ -f "$GATE/release-consumed" ] && break; sleep 0.25; done; echo gate-ok
```

Give every prompt the same standing rules, so an injected notice is answered
rather than triggering a shopping trip:

```
Standing rules: follow the numbered steps exactly, in order, and call only the
tools each step names. If an automatic sub-agent completion notice appears in
your context, reply with exactly ACK and call no tools. Never call
avenor_shutdown, avenor_message, avenor_peers, avenor_inspect or avenor_events
unless a step tells you to.
```

### Drive 1 — consumed while busy

Child prompt (`consumed-child.txt`): `Reply with the single word OK and nothing
else.`

Host prompt:

```
<standing rules>
Working directory for every call: $SCRATCH/work

Steps, in order:
1. Call avenor_spawn with prompt_file "$SCRATCH/prompts/consumed-child.txt",
   label "consumed-child", backend "pi", model "<model>", wait false.
2. Call the bash tool with timeout 600 and command:
   for i in $(seq 1 2400); do [ -f $GATE/release-consumed ] && break; sleep 0.25; done; echo gate-ok
   Do nothing else until that command returns.
3. Call avenor_result with run_id "consumed-child" and wait false.
4. Reply with exactly DONE and call no more tools.
```

Observer: wait for an `avenor` status record whose text carries `unread:1`, then
write `$GATE/release-consumed`. Release only after that record — releasing
earlier lets the host settle while the child is still running, and the delivery
under test never happens.

**Observable end state (the proof).** The footer showed `unread:1` while the host
was inside its bash call. The `avenor_result` record is `ready`, resolved from the
label, with `output: "OK"`. A later `avenor` status record carries no `unread`
token. The final transcript has no message with role `custom` and customType
`avenor-completion`, and the turn ends with one `agent_start` and one
`agent_settled` — no continuation. The consumed notice is gone, not deferred.

### Drive 2 — mixed batch

Three children, so the batch is countable after one consumption. Spawn
`batch-a`, `batch-b`, `batch-c` (`wait false`, same model), gate, then consume
only `batch-a`:

```
1. Call avenor_spawn with prompt_file "$SCRATCH/prompts/batch-a.txt",
   label "batch-a", backend "pi", model "<model>", wait false.
2. Call avenor_spawn with prompt_file "$SCRATCH/prompts/batch-b.txt",
   label "batch-b", backend "pi", model "<model>", wait false.
3. Call avenor_spawn with prompt_file "$SCRATCH/prompts/batch-c.txt",
   label "batch-c", backend "pi", model "<model>", wait false.
4. Call the bash tool with timeout 600 and command:
   for i in $(seq 1 2400); do [ -f $GATE/release-batch ] && break; sleep 0.25; done; echo gate-ok
   Do nothing else until that command returns.
5. Call avenor_result with run_id "batch-a" and wait false.
   Do not call avenor_result for batch-b or batch-c.
6. Reply with exactly DONE and call no more tools.
```

Every `batch-*` child prompt is the same one-word task.

**Observable end state (the proof).** Footer `unread:3` before the release, then
`unread:2` after the result call — with no active runs left, so the token is
notices, not processes. Exactly two `avenor-completion` messages reach the
transcript, both with role `custom`, delivered together by a single
continuation: the drive's next `agent_settled` follows one extra `agent_start`
that ran no tools. Their content names `batch-b` and `batch-c` and never
`batch-a`.

Three children rather than two is deliberate. With one unread notice left,
batched delivery and one-wake-up-per-notice delivery look the same. With two,
they come apart: two notices, one continuation.

### Drive 3 — idle

The child gates itself, so the host can finish its turn first. Child prompt
(`idle-child.txt`), written with the marker directory substituted so the child
receives a literal path:

```
Call the bash tool with timeout 600 with exactly this command, wait for it to
finish, then reply with exactly OK and call no more tools:
for i in $(seq 1 2400); do [ -f <GATE>/release-idle-child ] && break; sleep 0.25; done; echo child-released
```

Host prompt:

```
<standing rules>
Working directory for every call: $SCRATCH/work

Steps, in order:
1. Call avenor_spawn with prompt_file "$SCRATCH/prompts/idle-child.txt",
   label "idle-child", backend "pi", model "<model>", wait false.
2. Reply with exactly DONE and call no more tools. Do not call avenor_result.
```

Observer: wait for that prompt's `agent_settled`, then write
`<GATE>/release-idle-child`.

**Observable end state (the proof).** One message arrives, naming `idle-child`
and carrying its `OK`, with role `user` and no customType — the idle path, not a
batched `avenor-completion`. It starts its own run: a second
`agent_start`/`agent_settled` pair with no tool call in between and no second
prompt from the driver. The child finished while the session was idle, so nothing
was queued and the footer never showed an `unread` token for it.

## Expected evidence

Per drive, into an evidence dir that outlives cleanup:

1. `argv.json` — the host argv, the shim's body, the `avenor` binary and model
   strings, and the environment variables the drive set (never a token).
2. `host-rpc.ndjson` — the raw host stdout, split on LF only: `tool_execution_end`
   records with `result.details` for `avenor_spawn` / `avenor_result`,
   `agent_start` / `agent_settled`, and `message_end` for assistant text.
3. Status history — every `extension_ui_request` / `setStatus` record in order,
   filtered to `statusKey: "avenor"`. The `unread:1` → gone and `unread:3` →
   `unread:2` transitions are read from here, not from the driver's beliefs.
4. `messages-final.json` — the `get_messages` transcript. A batched notice appears
   there as a message with role `custom`, `customType: "avenor-completion"` and a
   `details` object; an idle notice appears as role `user` with no customType.
   `custom_message` is the session-entry kind the extension appends — read the
   resulting role and customType, never the entry name.
5. Per-run child evidence from `$AVENOR_HOME/runs/<run_id>/`: `events.log` and
   the sentinel (`DONE`, `SESSION=`, `STOP_REASON=`) proving the child really
   ended where the notice says it did. Address runs by public run id, never by
   label.
6. Control plane while the supervisor is up: `avenor control --socket <sock>
   list` and `status <runtime_id>` per label, captured before teardown.
7. Cleanup: `avenor control --socket <sock> shutdown graceful` first, then the
   recorded host PID, then the scratch dir — after closing the host's stdin as
   the RPC client does. Keep the evidence dir.

Assert on structured records only. Notice prose is bounded and sanitized; the
label, `run_id`, `status` and the child's sentinel are the evidence.

## Gotchas

- **Release the gate after `unread:N`, never before.** The gate's whole job is to
  keep the host busy at the instant the child ends. Release early and the drive
  proves the idle leg while claiming the busy one.
- **A missing `unread` token proves less than it looks.** The count also falls
  when an idle session takes the notice directly, and the footer only repolls on
  the next tick, so a recent finish may simply not be counted yet. Wait for one
  more `avenor` status record before concluding anything, and never read absence
  as "the run finished without a notice".
- **Never call `avenor_result` before the notice is queued.** The result consumes
  the run; the notice then never exists and the footer stays bare.
- **Do not observe this through a `wait: true` spawn.** The inline result consumes
  the notice, so the wake-up you mean to watch for cannot happen. A blocking wait
  can still briefly queue a notice while a poll sees the run go terminal, and the
  inline completion removes it again — that transient is exactly the ambiguity
  this drive exists to avoid. Use `wait: false` for every observed child.
- **Do not let the host abort its turn.** An `agent_before_settle` delivery needs
  a settlement with outcome `completed`; if the model aborts or errors, the
  notices stay queued and the drive reads as a failure of the feature rather
  than of the prompt.
- **The model is the flakiest part.** Short numbered steps, one child per step,
  and the standing rules above. Non-compliance (wrong tool, `wait true`, extra
  answer text) is a failed drive, not a soft pass.
- **Keep `$AVENOR_HOME` short** for the socket-bind reason in
  [messaging.md](messaging.md#gotchas).

## Known limits today

- The unread queue is in-memory session state. `session_start` clears it but
  restarts polling, so a still-tracked run's notice is rebuilt; `session_shutdown`
  stops polling too, so an undelivered notice does not survive shutdown.
- A notice exists only for a run this session tracks — one it spawned or
  resumed. A run another Pi session owns appears in the status widget but never
  produces a notice here.
- Resumed runs (`avenor_message` action `resume`) are tracked as new public
  runs. A parked Pi runtime can resume warm; after it is reaped its in-memory
  session cannot be resumed, per [messaging.md](messaging.md#known-limits-today).
