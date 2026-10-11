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
writes back an answer with `avenor answer <perm-base> --option <id>`.

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

# With --sentinel-file "$SCRATCH/done.env" and no explicit
# --permission-handler, the file handler base is "$SCRATCH/done.env.perm"
# (DerivePermBase appends .perm). The request lands at "$SCRATCH/done.env.perm.req"
# and the answer goes to "$SCRATCH/done.env.perm.req.response".
cat "$SCRATCH/done.env.perm.req"     # note the option ids
./avenor answer "$SCRATCH/done.env.perm" --option <allow-option-id> \
  --message "approved by verify harness"
wait
```

**Observable end state (the proof):** the run completes (sentinel `DONE`) only
after the answer was written; the response file exists with the chosen option;
the agent's side effect (`proof.txt`) exists in the scratch dir; events show
the permission exchange. The two denial paths end differently: a deny option
(`kind: reject`, outcome `selected`) relays the rejection and the session
still ends normally — sentinel `DONE` with `STOP_REASON=end_turn`, rejection
visible in events — while `--outcome cancelled` terminates the run with
sentinel `KILLED`, `STOP_REASON=cancelled`, `EXIT_CODE=130`. Capture both
branches when the feature under test is the answerer itself.

## Gotchas

- Without `--auto-approve` or an answered file, the run blocks until the file
  handler's default 10-minute timeout (`DefaultTimeout` in
  `internal/permission/file.go`) expires and the run terminates with a
  permission error — always answer promptly and bound the run with `--timeout`.
- The request file may be rewritten between polls; read it once, answer once.
- `--option` is required; `--outcome` defaults to `selected`.
- If no request file ever appears, the pi backend may not be surfacing the
  approval for this action — try a different tool trigger before concluding
  the answer path is broken.
