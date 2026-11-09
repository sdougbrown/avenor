---
name: dispatch-jockey
description: "Dispatch a task to an OpenCode jockey agent via Avenor. Use when delegating work that needs jockey's plan-delegate-verify loop. Jockey runs in OpenCode via the ACP backend."
argument-hint: '[task description or @prompt-file]'
---

$ARGUMENTS: the task to dispatch to jockey, or a path to a prompt file prefixed with @

## Steps

### 1. Set up paths

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

If `$ARGUMENTS` is a `@path` reference, use `--prompt-file`:

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

### 3. Monitor

Run the selected invocation as a direct background child and record its process
handle. Bound the whole command with your execution tool's timeout. Do not rely
on a launcher that forks and leaves its child untracked.

```bash
avenor watch --follow --classify "$EVENTS"
```

If a `permission.request` event appears, jockey has a question. Read `$PERM_BASE.req` and respond:

```bash
avenor answer "$PERM_BASE" --option <option-id>
```

### 4. Read the result

Wait for the child to exit, then inspect `$SENTINEL` if present. A parse error,
crash, forced termination, or sentinel write error can leave no sentinel; report
that outcome rather than polling forever. `DONE` is clean; `FAILED`, `TIMEOUT`,
and `KILLED` indicate problems. `BLOCKED` means jockey escalated — read
`REASON=` for context. Read a result file only if this run's sentinel names it
in `RESULT=`.

## Note on dispatch depth

Codex can dispatch to OpenCode jockey (this skill). Jockey can in turn delegate to horse or mule sub-agents within OpenCode. Dispatching from Codex back to Claude Code is not yet supported.

## See also

- [docs/permission-handler.md](../../../docs/permission-handler.md) — request/response format
- [docs/watch.md](../../../docs/watch.md) — event log tailing
- [docs/backends.md](../../../docs/backends.md) — opencode-acp details
