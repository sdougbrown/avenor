# Permission answering — `avenor answer`

## Sub-features

- File-based permission requests written by a running session
- Atomic answer via `avenor answer <path> --option <id>`
- Free-text message attachment (`--message`)
- Cancellation outcome (`--outcome cancelled`)
- Force-overwrite of an existing response (`--force`)
- Request shapes in `docs/permission-handler.md`

## How to get to it (user POV)

When a backend asks for tool approval mid-run, avenor writes the request to the
permission-handler file (`file:<path>`, derived from the sentinel path when
`--sentinel-file` is set). The user (or their agent) reads the request JSON and
writes back an answer with `avenor answer <path> --option <id>`.

## Driving it with the avenor binary

Use a prompt that forces a real permission request — a task the agent cannot
complete without a tool approval. From the `--dir` scratch dir, the agent must
write outside its sandbox or run a command that triggers approval:

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
./avenor run --backend pi \
  --prompt "Write the file $SCRATCH/proof.txt containing OK, then exit." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/events.ndjson" \
  --sentinel-file "$SCRATCH/done.env" \
  --timeout 120s --label verify-perms &

# Poll for the request file (same base as the sentinel, .request suffix or as
# documented in docs/permission-handler.md), then inspect it:
cat "$SCRATCH"/*request*.json        # note the option ids
./avenor answer "$SCRATCH"/<request-file> --option <allow-option-id> \
  --message "approved by verify harness"
wait
```

**Observable end state (the proof):** the run completes (sentinel `DONE`) only
after the answer was written; the response file exists with the chosen option;
the agent's side effect (`proof.txt`) exists in the scratch dir; events show
the permission exchange. A denial path (`--outcome cancelled` or a deny option)
should produce a sentinel with a non-`end_turn` stop reason — capture both
branches when the feature under test is the answerer itself.

## Gotchas

- Without `--auto-approve` or an answered file, the run blocks indefinitely —
  always bound it with `--timeout` and run it in the background.
- The request file may be rewritten between polls; read it once, answer once.
- `--option` is required; `--outcome` defaults to `selected`.
- If no request file ever appears, the pi backend may not be surfacing the
  approval for this action — try a different tool trigger before concluding
  the answer path is broken.
