# dispatch-jockey

Use this skill to start a jockey run through Avenor, monitor it for permission requests, and wait for completion.

## Inputs

You need:
- A prompt or prompt file for jockey
- A working directory
- A base path for the permission handler (used by `answer-jockey` if permission requests arrive)

## Invocation

Create a fresh directory for each run so old artifacts cannot be mistaken for
this run's output:

```sh
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/avenor-jockey.XXXXXX")
```

```sh
avenor run \
  --backend opencode-acp \
  --agent jockey \
  --prompt "Your task here" \
  --dir /repo \
  --permission-handler "file:$RUN_DIR/perm" \
  --on-event "$RUN_DIR/events.ndjson" \
  --sentinel-file "$RUN_DIR/done.env"
```

Run the selected invocation as a direct background child and record its process
handle. Bound the whole command with your execution tool's timeout. Do not rely
on a launcher that forks and leaves its child untracked.

To use a prompt file instead:

```sh
avenor run \
  --backend opencode-acp \
  --agent jockey \
  --prompt-file /repo/task.md \
  --dir /repo \
  --permission-handler "file:$RUN_DIR/perm" \
  --on-event "$RUN_DIR/events.ndjson" \
  --sentinel-file "$RUN_DIR/done.env"
```

## Monitoring

Tail the event log to see what jockey is doing:

```sh
avenor watch --follow "$RUN_DIR/events.ndjson"
```

Watch for `permission.request` events — those mean jockey has a question. When one appears, use the `answer-jockey` skill with `$RUN_DIR/perm` as the handler path.

To filter to milestones and findings only:

```sh
avenor watch --follow --classify "$RUN_DIR/events.ndjson"
```

## Completion

Wait for the child to exit, then inspect `$RUN_DIR/done.env` if present. A parse
error, crash, forced termination, or sentinel write error can leave no sentinel;
report that outcome rather than polling forever. A sentinel contains:

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

The first line is the outcome. `DONE` is clean completion. `FAILED`, `TIMEOUT`,
and `KILLED` indicate problems. `BLOCKED` means jockey emitted
`<|workflow: abort | reason|>` and needs escalation. Read a result file only if
this run's sentinel names it in `RESULT=`.

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
