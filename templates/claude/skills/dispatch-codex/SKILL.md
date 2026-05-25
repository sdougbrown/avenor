---
name: dispatch-codex
description: "Dispatch a mechanical implementation task to Codex via Avenor. Use when the work is well-scoped and doesn't need jockey's full plan-delegate-verify loop. Codex runs via the app-server backend; this skill wires up the invocation and monitors completion."
argument-hint: '[task description or @prompt-file]'
---

$ARGUMENTS: the task to dispatch to Codex, or a path to a prompt file prefixed with @

## Steps

### 1. Set up paths

```bash
PERM_BASE=/tmp/avenor-perm-$$
EVENTS=/tmp/avenor-events-$$.ndjson
SENTINEL=/tmp/avenor-done-$$.env
```

### 2. Dispatch

```bash
avenor run \
  --backend codex-app-server \
  --prompt "$ARGUMENTS" \
  --dir "$(pwd)" \
  --permission-handler "file:$PERM_BASE" \
  --on-event "$EVENTS" \
  --sentinel-file "$SENTINEL"
```

If `$ARGUMENTS` is a `@path` reference, use `--prompt-file` instead:

```bash
avenor run \
  --backend codex-app-server \
  --prompt-file "${ARGUMENTS#@}" \
  --dir "$(pwd)" \
  --permission-handler "file:$PERM_BASE" \
  --on-event "$EVENTS" \
  --sentinel-file "$SENTINEL"
```

### 3. Monitor

```bash
avenor watch --follow --classify "$EVENTS"
```

Permission requests during a Codex run are handled the same way as jockey:

```bash
avenor answer "$PERM_BASE" --option <option-id>
```

### 4. Read the result

Poll for `$SENTINEL`. `DONE` is clean completion. `FAILED`, `TIMEOUT`, `KILLED` indicate problems.

## When to use jockey instead

Use `dispatch-jockey` when the task needs planning, sub-delegation to horse or mule, or iterative verification. Codex is better for tightly-scoped mechanical work where the full implementation path is already clear.

## See also

- [docs/permission-handler.md](../../../docs/permission-handler.md) — request/response format
- [docs/watch.md](../../../docs/watch.md) — event log tailing
- [docs/backends.md](../../../docs/backends.md) — codex-app-server details
