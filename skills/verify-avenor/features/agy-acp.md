# Agy ACP backend — `--backend agy`

## Sub-features

- Single run over `agy acp --stdio` (`session.start`, streamed `agent.message_chunk`, terminal `session.end` with `stop_reason: end_turn` and normalized snake_case `usage`)
- Strict model validation via ACP `session/set_config_option` (`configId: "model"`; rejects unknown model slugs with `-32602` without writing a `DONE` sentinel)
- Thinking control via ACP `session/set_config_option` (`configId: "thought_level"`): `low`, `medium`, `high`, and `xhigh` on a fresh start and an explicit resume, an omitted level on resume applying no setter, and `off` rejected by the static policy before `agy` spawns
- Retired `AVENOR_AGY_TRANSPORT` migration refusal (any non-empty value fails before spawning `agy`)
- Interactive tool permission relay (`permission.request` → `avenor answer` / `avenor control answer-permission`)
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

# 5. Thinking: start with a level, resume with a changed level, then resume with none
./avenor run --backend agy --model gemini-3.8-flash-low --thinking high \
  --prompt "Reply with the single word THINK and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/think-events.ndjson" \
  --sentinel-file "$SCRATCH/think-done.env" \
  --result-file "$SCRATCH/think-result.txt" \
  --timeout 90s --label verify-agy-thinking
echo "thinking_exit=$?"
cat "$SCRATCH/think-done.env"
cat "$SCRATCH/think-result.txt"

TSID=$(awk -F= '/^SESSION=/{print $2}' "$SCRATCH/think-done.env")

./avenor run --backend agy --resume "$TSID" --thinking low \
  --prompt "Reply with the single word LOWER and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/think-resume-events.ndjson" \
  --sentinel-file "$SCRATCH/think-resume-done.env" \
  --result-file "$SCRATCH/think-resume-result.txt" \
  --timeout 90s --label verify-agy-thinking-resume
echo "thinking_resume_exit=$?"
cat "$SCRATCH/think-resume-done.env"
cat "$SCRATCH/think-resume-result.txt"

./avenor run --backend agy --resume "$TSID" \
  --prompt "Reply with the single word DEFAULT and nothing else." \
  --dir "$SCRATCH" \
  --sentinel-file "$SCRATCH/think-resume-default-done.env" \
  --result-file "$SCRATCH/think-resume-default-result.txt" \
  --timeout 90s --label verify-agy-thinking-resume-default
echo "thinking_resume_default_exit=$?"
cat "$SCRATCH/think-resume-default-result.txt"

# 6. A rejected thinking value fails before agy spawns
./avenor run --backend agy --thinking off \
  --prompt "Reply OK" --dir "$SCRATCH" \
  --sentinel-file "$SCRATCH/think-off.env" 2>"$SCRATCH/think-off.err"
echo "thinking_off_exit=$?"
grep 'does not support thinking value "off"' "$SCRATCH/think-off.err"
! grep -q '^DONE$' "$SCRATCH/think-off.env"
```

For supervisor-mode permission relay and `avenor-channel-tools` messaging, start `avenor stable --control-socket "$SCRATCH/stable.sock" --idle-timeout 5m`, spawn `--backend agy --model gemini-3.8-flash-low --no-park --permission-handler "file:$SCRATCH/perm" --timeout 90` with a shell-tool prompt, wait for `$SCRATCH/perm.req`, read the offered allow `optionId` (for example `allow` or `allow-once`) from `$SCRATCH/perm.req`, approve via `./avenor answer --option <allow-option-id> "$SCRATCH/perm"`, verify the file side effect, verify a broker `tell` via `avenor-channel-tools` (`--auto-approve`), and shut down the stable cleanly.

## Observable end state (the proof)

- Step 1 exits non-zero with `"AVENOR_AGY_TRANSPORT is no longer supported"` and writes no `DONE` sentinel.
- Step 2 exits non-zero with `-32602` / `"Unknown model ID"`.
- Step 3 exits `0`; `done.env` carries `DONE`, `SESSION=<uuid>`, `STOP_REASON=end_turn`, `RESULT=`; `events.ndjson` contains `session.start` (`backend: "agy"`) and `session.end` with normalized `usage`; `result.txt` holds `OK`.
- Step 4 exits `0` against a fresh `agy acp` process with the same `SESSION=<uuid>` and `RESUMED` in `resume-result.txt`.
- Step 5 exits `0` three times on the same session: with `--thinking high` (`THINK` in `think-result.txt`, `thought_level` in `think-events.ndjson`), with `--thinking low` on a cold resume (`LOWER`), and with no `--thinking` on the next resume (`DEFAULT`, no `thought_level` request). If the chosen model does not offer `high`, Avenor fails with `agy thinking "high"` and the `agy acp` rejection rather than the allowed-values error; record that outcome and pick a model that offers the level.
- Step 6 exits non-zero with `backend "agy" does not support thinking value "off" (allowed: low, medium, high, xhigh)` and writes no `DONE` sentinel.
- Supervisor drive proves `permission.request` → `answer_permission` execution, mid-turn `cancel`, and `avenor-channel-tools` broker send + receipt, with zero credentials in captured logs/evidence.
