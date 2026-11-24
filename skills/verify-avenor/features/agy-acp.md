# Agy ACP backend — `--backend agy`

## Sub-features

- Single run over `agy acp --stdio` (`session.start`, streamed `agent.message_chunk`, terminal `session.end` with `stop_reason: end_turn` and normalized snake_case `usage`)
- Strict model validation via ACP `session/set_config_option` (`configId: "model"`; rejects unknown model slugs with `-32602` without writing a `DONE` sentinel)
- Retired `AVENOR_AGY_TRANSPORT` migration refusal (any non-empty value fails before spawning `agy`)
- Interactive tool permission relay (`permission.request` → `avenor control answer_permission`)
- Mid-turn cancellation (`avenor control cancel`)
- Cold cross-process resume (`avenor run --resume <session_id>` against a fresh `agy acp` process with a different backend PID)
- Native broker messaging via ACP `mcpServers` (`avenor channel-tools` send + inbound receipt)

## How to get to it (user POV)

Pass `--backend agy` to `avenor run` or `avenor control spawn`. Avenor spawns `agy acp --stdio --add-dir <dir>` (never `--dangerously-skip-permissions`), sets the model over ACP, relays permissions, and injects `avenor channel-tools` when a supervisor broker is active.

## Driving it with the avenor binary

Build first (`mise run go-build` or `go build -o ./avenor ./cmd/avenor`), then use a fresh scratch directory:

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify-agy.XXXXXX)

# 1. Retired AVENOR_AGY_TRANSPORT fails before spawn
AVENOR_AGY_TRANSPORT=rpc ./avenor run --backend agy \
  --prompt "Reply OK" --dir "$SCRATCH" \
  --sentinel-file "$SCRATCH/retired.env" 2>"$SCRATCH/retired.err"
echo "retired_exit=$?"
grep "AVENOR_AGY_TRANSPORT is no longer supported" "$SCRATCH/retired.err"
! grep -q '^DONE$' "$SCRATCH/retired.env"

# 2. Unknown model rejected by agy acp (-32602)
./avenor run --backend agy --model definitely-not-a-real-model-xyz \
  --prompt "Reply OK" --dir "$SCRATCH" \
  --sentinel-file "$SCRATCH/badmodel.env" 2>"$SCRATCH/badmodel.err"
echo "badmodel_exit=$?"
grep "Unknown model ID" "$SCRATCH/badmodel.err"

# 3. Single run with valid model slug
./avenor run --backend agy --model gemini-3.8-flash-low \
  --prompt "Reply with the single word OK and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/events.ndjson" \
  --sentinel-file "$SCRATCH/done.env" \
  --result-file "$SCRATCH/result.txt" \
  --timeout 90s --label verify-agy
echo "run_exit=$?"
cat "$SCRATCH/done.env"
cat "$SCRATCH/result.txt"

# 4. Cold cross-process resume (fresh agy acp process, same session ID)
SID=$(awk -F= '/^SESSION=/{print $2}' "$SCRATCH/done.env")
./avenor run --backend agy --model gemini-3.8-flash-low \
  --resume "$SID" \
  --prompt "Reply with the single word RESUMED and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/resume-events.ndjson" \
  --sentinel-file "$SCRATCH/resume-done.env" \
  --result-file "$SCRATCH/resume-result.txt" \
  --timeout 90s --label verify-agy-resume
echo "resume_exit=$?"
cat "$SCRATCH/resume-done.env"
cat "$SCRATCH/resume-result.txt"
```

For supervisor-mode permission relay and `avenor-channel-tools` messaging, start `avenor stable --control-socket "$SCRATCH/stable.sock" --idle-timeout 5m`, spawn `--backend agy --model gemini-3.8-flash-low --no-park --permission-handler "file:$SCRATCH/perm" --timeout 90` with a shell-tool prompt, wait for `$SCRATCH/perm.req`, approve via `./avenor answer --option allow "$SCRATCH/perm"`, verify the file side effect, verify a broker `tell` via `avenor-channel-tools` (`--auto-approve`), and shut down the stable cleanly.

## Observable end state (the proof)

- Step 1 exits non-zero with `"AVENOR_AGY_TRANSPORT is no longer supported"` and writes no `DONE` sentinel.
- Step 2 exits non-zero with `-32602` / `"Unknown model ID"`.
- Step 3 exits `0`; `done.env` carries `DONE`, `SESSION=<uuid>`, `STOP_REASON=end_turn`, `RESULT=`; `events.ndjson` contains `session.start` (`backend: "agy"`) and `session.end` with normalized `usage`; `result.txt` holds `OK`.
- Step 4 exits `0` against a fresh `agy acp` process with the same `SESSION=<uuid>` and `RESUMED` in `resume-result.txt`.
- Supervisor drive proves `permission.request` → `answer_permission` execution, mid-turn `cancel`, and `avenor-channel-tools` broker send + receipt, with zero credentials in captured logs/evidence.
