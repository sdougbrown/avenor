# Single run — `avenor run`

## Sub-features

- Inline prompt vs `--prompt-file`
- Event stream emission (`--on-event`, NDJSON)
- Completion sentinel (`--sentinel-file`: `DONE`, `SESSION=`, `STOP_REASON=`, `RUN=`)
- Timeout guards (`--timeout`, `--progress-timeout`)
- Label correlation (`--label` appears in events)

## How to get to it (user POV)

`avenor run` (or bare `avenor`) is the front door: start an agent with a
prompt, block until it finishes. Users pick a backend (`--backend`), aim it at
a working directory (`--dir`), and optionally capture events and the sentinel.

## Driving it with the avenor binary

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
./avenor run --backend pi \
  --prompt "Reply with the single word OK and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/events.ndjson" \
  --sentinel-file "$SCRATCH/done.env" \
  --timeout 90s --label verify-smoke
echo "exit=$?"

cat "$SCRATCH/done.env"
grep -c '"event"' "$SCRATCH/events.ndjson"
```

**Observable end state (the proof):** exit code 0; sentinel contains `DONE`,
a `SESSION=` id, and `STOP_REASON=end_turn`; `events.ndjson` contains a
`session.start` and a terminal `session.end` with `stop_reason: end_turn`,
all carrying the `verify-smoke` label.

## Gotchas

- Runs are real model calls — keep prompts to one-word replies.
- `--dir` is where the agent works; always point it at a scratch dir, never
  the repo.
- A bare `avenor run` (no `--backend`) defaults to `opencode-acp`, which needs
  `opencode` on PATH — absent here. Always pass `--backend pi`.
- `--timeout` matters: without it a wedged backend hangs the drive forever.
- The permission handler defaults to `file:<sentinel-base>` when
  `--sentinel-file` is set; if you see the run stall, check for a permission
  request file next to the sentinel (see [permissions.md](permissions.md)).
