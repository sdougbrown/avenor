---
name: dispatch-jockey
description: "Dispatch a task to an OpenCode jockey agent via Avenor. Use when delegating a multi-step implementation task that needs jockey's plan-delegate-verify loop. Jockey runs in OpenCode; this skill wires up the invocation, monitors events, and handles permission requests."
argument-hint: '[task description or @prompt-file]'
---

$ARGUMENTS: the task to dispatch to jockey, or a path to a prompt file prefixed with @

## Steps

### 1. Set up paths

Choose consistent paths for this run:

```bash
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/avenor-jockey.XXXXXX")
PERM_BASE=$RUN_DIR/perm
EVENTS=$RUN_DIR/events.ndjson
SENTINEL=$RUN_DIR/done.env
```

### 2. Dispatch

```bash
avenor run \
  --backend opencode-acp \
  --agent jockey \
  --prompt "$ARGUMENTS" \
  --dir "$(pwd)" \
  --permission-handler "file:$PERM_BASE" \
  --on-event "$EVENTS" \
  --sentinel-file "$SENTINEL"
```

If `$ARGUMENTS` is a `@path` reference, use `--prompt-file` instead:

```bash
avenor run \
  --backend opencode-acp \
  --agent jockey \
  --prompt-file "${ARGUMENTS#@}" \
  --dir "$(pwd)" \
  --permission-handler "file:$PERM_BASE" \
  --on-event "$EVENTS" \
  --sentinel-file "$SENTINEL"
```

Run the selected command as a direct child in the background and record its
process handle. Bound the whole command with your execution tool's timeout.
Do not rely on a launcher that forks and leaves its child untracked.

### 3. Monitor

Tail the event log while the run proceeds:

```bash
avenor watch --follow --classify "$EVENTS"
```

Watch for `permission.request` events — jockey is asking a question. When one appears, read `$PERM_BASE.req` and respond:

```bash
avenor answer "$PERM_BASE" --option <option-id>
```

### 4. Read the result

Wait for the child to exit, then inspect `$SENTINEL` if it exists. A parse
error, crash, forced termination, or sentinel write error can leave no sentinel;
report that outcome rather than polling forever. The file contains:

```
DONE
SESSION=ses_abc123
STOP_REASON=end_turn
RUN=a3f9...
```

The first line is `DONE` (clean), `FAILED`, `TIMEOUT`, `KILLED`, or `BLOCKED`
(jockey escalated — check `REASON=`). Do not infer success from a result file
unless this run's sentinel names it in `RESULT=`.

## See also

- [docs/permission-handler.md](../../../docs/permission-handler.md) — request/response format
- [docs/watch.md](../../../docs/watch.md) — event log tailing
- [docs/backends.md](../../../docs/backends.md) — opencode-acp details
