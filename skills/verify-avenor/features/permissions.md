# Permission answering — `avenor answer`

## Sub-features

- File-based permission requests written by a running session
- Atomic answer via `avenor answer --option <id> <path>`
- Free-text message attachment (`--message`)
- Cancellation outcome (`--outcome cancelled`)
- Force-overwrite of an existing response (`--force`)
- Request shapes in `docs/permission-handler.md`

## How to get to it (user POV)

When a backend asks for tool approval mid-run, avenor writes the request to the
permission-handler file (`file:<path>`, derived from the sentinel path when
`--sentinel-file` is set). The user (or their agent) reads the request JSON and
writes back an answer with `avenor answer --option <id> <perm-base>`.

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
./avenor answer --option <allow-option-id> \
  --message "approved by verify harness" "$SCRATCH/done.env.perm"
wait
```

**Observable end state (the proof):** the run completes (sentinel `DONE`) only
after the answer was written; the response file exists with the chosen option;
the agent's side effect (`proof.txt`) exists in the scratch dir; events show
the permission exchange (`permission.request`, then `permission.response` with
`kind: allow`). The two refusal paths end differently. A deny option
(`kind: reject`, outcome `selected`) relays the rejection and the pi backend
ends the run as a tool denial: sentinel `FAILED`, `STOP_REASON=tool_denied`,
`EXIT_CODE=2`, no side effect, and the rejection visible in events. An
`--outcome cancelled` answer terminates the run with sentinel `KILLED`,
`STOP_REASON=cancelled`, `EXIT_CODE=130`. Capture both branches when the feature
under test is the answerer itself, and answer every request the run makes — a
backend that retries the tool after a denial asks again.

## Gotchas

- Without `--auto-approve` or an answered file, the run blocks until the file
  handler's default 10-minute timeout (`DefaultTimeout` in
  `internal/permission/file.go`) expires and the run terminates with a
  permission error — always answer promptly and bound the run with `--timeout`.
- The request file may be rewritten between polls; read it once, answer once.
- `--option` is required, including with `--outcome cancelled`; `--outcome`
  defaults to `selected`.
- The pi backend does not ask before writing inside the run's directory or
  running a command. A drive must load an extension that calls
  `ctx.ui.confirm` (for example a `tool_call` gate) before a request file
  appears at all.
- If no request file ever appears, check the extension gate before concluding
  the answer path is broken.
