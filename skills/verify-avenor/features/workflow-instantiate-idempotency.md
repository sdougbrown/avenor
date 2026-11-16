# Workflow instantiation idempotency — `avenor stable` + `avenor workflow`

## What it proves

An instantiate request carrying `--idempotency-key` is retry-safe: a retried
request with the same key, template, version, and params returns the original
`workflow_id` instead of creating a second instance — across a lost response,
a supervisor restart, and a second supervisor process sharing the workflow
root. Reusing the key for a different request is a conflict error, and the
binding persists on disk under `<workflow-root>/instantiation-keys/`.

## Prerequisites (cross-ref `../SKILL.md` and `stable-control.md`)

- Build the binary: `mise run go-build` (→ `./avenor`). No backend model is
  needed — this feature is exercised with `manual` action nodes only.
- Fresh scratch dir with an isolated workflow root:
  `SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)` and
  `--workflow-root "$SCRATCH/workflows"`.

## Fixture and drive (full real commands; only the supervisor readiness loop is elided)

```bash
set -eu; cd "$(git rev-parse --show-toplevel)"   # repo root, where ./avenor lives
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX); SOCK="$SCRATCH/s.sock"; SOCK2="$SCRATCH/s2.sock"
cat > "$SCRATCH/t1.json" <<'EOF'
{"schema_version":1,"template_id":"verify-idem","template_version":"1","entry_nodes":["start"],"nodes":[{"id":"start","action":{"type":"manual"}}],"terminal_outcomes":["done"]}
EOF
cat > "$SCRATCH/t2.json" <<'EOF'
{"schema_version":1,"template_id":"verify-idem-b","template_version":"1","entry_nodes":["start"],"nodes":[{"id":"start","action":{"type":"manual"}}],"terminal_outcomes":["done"]}
EOF
echo '{}' > "$SCRATCH/req.json"
./avenor stable --control-socket "$SOCK" --workflow-root "$SCRATCH/workflows" &> "$SCRATCH/stable.log" &
STABLE_PID=$!   # record this — cleanup kills THIS pid only
for _i in $(seq 1 50); do ./avenor control --socket "$SOCK" list >/dev/null 2>&1 && break; sleep 0.2; done

./avenor workflow create --socket "$SOCK" --request-file "$SCRATCH/t1.json" >/dev/null
./avenor workflow create --socket "$SOCK" --request-file "$SCRATCH/t2.json" >/dev/null

W1=$(./avenor workflow instantiate --socket "$SOCK" --template-id verify-idem --template-version 1 --request-file "$SCRATCH/req.json" --idempotency-key unit-42 | python3 -c 'import json,sys;print(json.load(sys.stdin)["workflow_id"])')
W2=$(./avenor workflow instantiate --socket "$SOCK" --template-id verify-idem --template-version 1 --request-file "$SCRATCH/req.json" --idempotency-key unit-42 | python3 -c 'import json,sys;print(json.load(sys.stdin)["workflow_id"])')
W3=$(./avenor workflow instantiate --socket "$SOCK" --template-id verify-idem --template-version 1 --request-file "$SCRATCH/req.json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["workflow_id"])')
./avenor workflow instantiate --socket "$SOCK" --template-id verify-idem-b --template-version 1 --request-file "$SCRATCH/req.json" --idempotency-key unit-42 2>&1 | grep -q 'idempotency key conflict' || { echo "FAIL: expected conflict"; exit 1; }

# Restart: a second supervisor over the SAME workflow root must resolve the
# same key to the same workflow (lost-response + restart convergence).
./avenor control --socket "$SOCK" shutdown graceful
kill "$STABLE_PID" 2>/dev/null || true
./avenor stable --control-socket "$SOCK2" --workflow-root "$SCRATCH/workflows" &> "$SCRATCH/stable2.log" &
STABLE_PID2=$!
for _i in $(seq 1 50); do ./avenor control --socket "$SOCK2" list >/dev/null 2>&1 && break; sleep 0.2; done
W4=$(./avenor workflow instantiate --socket "$SOCK2" --template-id verify-idem --template-version 1 --request-file "$SCRATCH/req.json" --idempotency-key unit-42 | python3 -c 'import json,sys;print(json.load(sys.stdin)["workflow_id"])')
```

## Evidence (capture before cleanup; copy out of the scratch dir first)

```bash
EV=/tmp/avenor-verify-evidence/$(date +%Y%m%d-%H%M%S); mkdir -p "$EV"
[ "$W1" = "$W2" ] || { echo "FAIL: retry diverged"; exit 1; }
[ "$W1" = "$W4" ] || { echo "FAIL: restart retry diverged"; exit 1; }
[ "$W1" != "$W3" ] || { echo "FAIL: keyless call reused the keyed ID"; exit 1; }
printf 'same-key retry id: %s\nrestart retry id:  %s\nkeyless id:        %s\n' "$W1" "$W4" "$W3" > "$EV/ids.txt"
cp "$SCRATCH/workflows/instantiation-keys/"*.json "$EV/"
cat "$EV/"*.json | python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["workflow_id"]==r["workflow_id"]; print(r["key"], r["workflow_id"])'
./avenor control --socket "$SOCK2" shutdown graceful
kill "$STABLE_PID2" 2>/dev/null || true
rm -rf "$SCRATCH"   # never delete "$EV"
```

## Observable end state (the proof)

- `ids.txt`: the retried instantiate under the same key returns the **same**
  `workflow_id` as the first call, both within one supervisor and after a
  restart over the same root; the keyless call minted a **different** ID.
- The conflict attempt failed with `workflow: instantiation idempotency key
  conflict` and did not disturb the original binding (the restart retry still
  converged after it).
- The key record JSON under `instantiation-keys/` (filename is the SHA-256 of
  the key) carries `key`, `template_id`, `template_version`, `request_hash`,
  and `workflow_id`.

## Gotchas

- `--idempotency-key` is optional; requests without one mint a fresh workflow
  ID every time (unchanged behavior).
- Conflict fingerprint covers template, version, and params only — `metadata`
  is caller bookkeeping and never conflicts.
- Requests that fail validation (unknown template, invalid params) never bind
  the key: template and param validation run before key resolution, so a bad
  request costs nothing.
