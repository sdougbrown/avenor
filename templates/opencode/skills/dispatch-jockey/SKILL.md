# dispatch-jockey

Use this skill to start a jockey run through Avenor, monitor it for permission requests, and wait for completion.

## Inputs

You need:
- A prompt or prompt file for jockey
- A working directory
- A base path for the permission handler (used by `answer-jockey` if permission requests arrive)

## Invocation

```sh
avenor run \
  --backend opencode-acp \
  --agent jockey \
  --prompt "Your task here" \
  --dir /repo \
  --permission-handler file:/tmp/avenor-perm \
  --on-event /tmp/avenor-events.ndjson \
  --sentinel-file /tmp/avenor-done.env
```

Run this in the background. The process writes events to `/tmp/avenor-events.ndjson` as it runs and writes a completion sentinel to `/tmp/avenor-done.env` when it finishes.

To use a prompt file instead:

```sh
avenor run \
  --backend opencode-acp \
  --agent jockey \
  --prompt-file /repo/task.md \
  --dir /repo \
  --permission-handler file:/tmp/avenor-perm \
  --on-event /tmp/avenor-events.ndjson \
  --sentinel-file /tmp/avenor-done.env
```

## Monitoring

Tail the event log to see what jockey is doing:

```sh
avenor watch --follow /tmp/avenor-events.ndjson
```

Watch for `permission.request` events — those mean jockey has a question. When one appears, use the `answer-jockey` skill with `/tmp/avenor-perm` as the handler path.

To filter to milestones and findings only:

```sh
avenor watch --follow --classify /tmp/avenor-events.ndjson
```

## Completion

When the run finishes, `/tmp/avenor-done.env` contains the outcome:

```
DONE
SESSION=ses_abc123
STOP_REASON=end_turn
RUN=a3f9...
```

Or on failure:

```
FAILED
SESSION=ses_abc123
STOP_REASON=error
RUN=a3f9...
```

Read `STATUS=` to branch on the outcome. `DONE` is clean completion. `FAILED`, `TIMEOUT`, and `KILLED` indicate problems. `BLOCKED` means jockey emitted `[loop: abort]` and needs escalation.

## Using a stable supervisor

If you're running multiple jockey runs or want to keep a supervisor alive across invocations, use `avenor stable` instead:

```sh
# Start the supervisor once
avenor stable --control-socket /tmp/avenor-stable.sock

# Spawn a jockey run
avenor control --socket /tmp/avenor-stable.sock spawn \
  --prompt "Your task here" \
  --dir /repo \
  --agent jockey \
  --permission-handler file:/tmp/avenor-perm \
  --label my-task

# List running runtimes
avenor control --socket /tmp/avenor-stable.sock list
```

The spawn result includes `on_event` and `sentinel_file` paths — use those with `avenor watch` and `answer-jockey` the same way as above.

## See also

- `answer-jockey` — respond to permission requests that arrive during the run
- [docs/permission-handler.md](../../docs/permission-handler.md) — request/response file format
- [docs/stable.md](../../docs/stable.md) — long-lived supervisor mode
- [docs/watch.md](../../docs/watch.md) — tailing and reading event logs
- [docs/cli.md](../../docs/cli.md) — full CLI reference
