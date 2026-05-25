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
PERM_BASE=/tmp/avenor-perm-$$
EVENTS=/tmp/avenor-events-$$.ndjson
SENTINEL=/tmp/avenor-done-$$.env
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

Run in the background. Avenor writes events to `$EVENTS` and the completion sentinel to `$SENTINEL`.

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

Poll for `$SENTINEL` to know when the run is done. The file contains:

```
DONE
SESSION=ses_abc123
STOP_REASON=end_turn
RUN=a3f9...
```

`STATUS` values: `DONE` (clean), `FAILED`, `TIMEOUT`, `KILLED`, `BLOCKED` (jockey escalated — check `REASON=`).

## See also

- [docs/permission-handler.md](../../../docs/permission-handler.md) — request/response format
- [docs/watch.md](../../../docs/watch.md) — event log tailing
- [docs/backends.md](../../../docs/backends.md) — opencode-acp details
