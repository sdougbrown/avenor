#!/usr/bin/env bash
# Reproduction probe for the current broker HTTP messaging behavior.
#
# Starts one isolated hub supervisor, two sibling branches (A and B) joined to
# the hub, and a sub-branch C joined to branch A, registers plain runs on each
# broker, and captures — verbatim, as reproduction output, not desired
# expectations:
#
#   1. a versioned ordinary tell posted without a generation
#   2. the same tell posted with the generation /sessions advertises, plus the
#      queued row observed through the target's /poll-control
#   3. a branch-A run's peer listing (the hub and sibling B are expected to be
#      hidden) and a send addressed to the sibling's qualified ID as the hub's
#      own listing names it
#   4. a branch-C run's peer listing (a hub peer visible through the relay) and
#      a send from C addressed to that hub peer
#
# The script reports; it does not assert desired behavior. The only hard
# failures are setup and cleanup failures. Every supervisor started here is
# tracked by PID and control socket and shut down through its control plane.
#
# Usage: scripts/broker-messaging-probe.sh [EVIDENCE_DIR]
# Evidence defaults to .cache/verify/561/probe/<timestamp>/ under the repo.
# Scratch state lives under /tmp/opencode (short socket paths). Tokens are
# never written to evidence.

set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
EVIDENCE=${1:-$REPO/.cache/verify/561/probe/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$EVIDENCE"
SCRATCH=$(mktemp -d /tmp/opencode/561-probe.XXXXXX)
mkdir -p "$SCRATCH/tmp"

BIN="$REPO/avenor"
if [ ! -x "$BIN" ]; then
  echo "building the checkout binary"
  (cd "$REPO" && GOCACHE=$REPO/.cache/go-build GOMODCACHE=$REPO/.cache/go-mod \
    go build -o ./avenor ./cmd/avenor)
fi

TOKEN=$("$BIN" parent-token)
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')

HUB_SOCK="$SCRATCH/hub.sock"
BRANCH_A_SOCK="$SCRATCH/branch-a.sock"
BRANCH_B_SOCK="$SCRATCH/branch-b.sock"
BRANCH_C_SOCK="$SCRATCH/branch-c.sock"
HUB_PID= BRANCH_A_PID= BRANCH_B_PID= BRANCH_C_PID=

await_path() { # await_path <path> <seconds> <what>
  local n=0
  until [ -e "$1" ]; do
    n=$((n + 1))
    [ "$n" -le $(($2 * 10)) ] || { echo "FAIL: $3 did not appear within ${2}s: $1" >&2; return 1; }
    sleep 0.1
  done
}

cleanup() {
  for sock in "$BRANCH_C_SOCK" "$BRANCH_B_SOCK" "$BRANCH_A_SOCK" "$HUB_SOCK"; do
    if [ -S "$sock" ]; then
      "$BIN" control --socket "$sock" shutdown graceful >/dev/null 2>&1 || true
    fi
  done
  for pid in "$BRANCH_C_PID" "$BRANCH_B_PID" "$BRANCH_A_PID" "$HUB_PID"; do
    case "$pid" in ''|*[!0-9]*) continue ;; esac
    kill "$pid" 2>/dev/null || true
  done
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

# --- start the isolated supervisors -----------------------------------------
# Each supervisor runs in a scrubbed environment: an ambient AVENOR_BROKER_URL
# or peer credential would leak the operator's peer rows into the listings.

launch_stable() { # launch_stable <home> <control_socket> <parent_url_or_''> [extra args...]
  local home=$1 sock=$2 parent=$3
  shift 3
  if [ -n "$parent" ]; then
    env -i PATH="$PATH" HOME="$HOME" TMPDIR="$SCRATCH/tmp" \
      AVENOR_HOME="$home" AVENOR_PARENT_BROKER_URL="$parent" AVENOR_PARENT_BROKER_TOKEN="$TOKEN" \
      "$BIN" stable --control-socket "$sock" "$@" >"$EVIDENCE/$(basename "$sock").log" 2>&1 &
  else
    env -i PATH="$PATH" HOME="$HOME" TMPDIR="$SCRATCH/tmp" \
      AVENOR_HOME="$home" AVENOR_PARENT_BROKER_TOKEN="$TOKEN" \
      "$BIN" stable --control-socket "$sock" "$@" >"$EVIDENCE/$(basename "$sock").log" 2>&1 &
  fi
}

launch_stable "$SCRATCH/hub-home" "$HUB_SOCK" '' \
  --broker-addr "127.0.0.1:$PORT" --idle-timeout 0
HUB_PID=$!
await_path "$HUB_SOCK.broker.url" 15 "hub broker URL record"
HUB_URL=$(cat "$HUB_SOCK.broker.url")

launch_stable "$SCRATCH/a-home" "$BRANCH_A_SOCK" "$HUB_URL" --idle-timeout 0
BRANCH_A_PID=$!
await_path "$BRANCH_A_SOCK.broker.url" 15 "branch A broker URL record"
BRANCH_A_URL=$(cat "$BRANCH_A_SOCK.broker.url")

launch_stable "$SCRATCH/b-home" "$BRANCH_B_SOCK" "$HUB_URL" --idle-timeout 0
BRANCH_B_PID=$!
await_path "$BRANCH_B_SOCK.broker.url" 15 "branch B broker URL record"
BRANCH_B_URL=$(cat "$BRANCH_B_SOCK.broker.url")

launch_stable "$SCRATCH/c-home" "$BRANCH_C_SOCK" "$BRANCH_A_URL" --idle-timeout 0
BRANCH_C_PID=$!
await_path "$BRANCH_C_SOCK.broker.url" 15 "branch C broker URL record"
BRANCH_C_URL=$(cat "$BRANCH_C_SOCK.broker.url")

# A failed join poisons every later probe, so it is a setup failure.
for log in "$EVIDENCE/branch-a.sock.log" "$EVIDENCE/branch-b.sock.log" "$EVIDENCE/branch-c.sock.log"; do
  if grep -q "parent broker registration failed" "$log"; then
    echo "FAIL: $(basename "$log") did not join its parent:" >&2
    grep "parent broker registration failed" "$log" >&2
    exit 1
  fi
done

# --- helpers -----------------------------------------------------------------

register_run() { # register_run <broker_url> <run_id>; prints the token
  curl -sS -X POST "$1/register" -H 'Content-Type: application/json' \
    -d "{\"run_id\":\"$2\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])'
}

sessions_get() { # sessions_get <broker_url> <run_id> <token>
  curl -sS "$1/sessions" -H "X-Avenor-Run-Id: $2" -H "X-Avenor-Run-Token: $3"
}

mint_msg_id() { # a versioned id: av1.<deadline-ms base36>.<16-byte hex>
  python3 - <<'PY'
import secrets, time
deadline = int((time.time() + 600) * 1000)
def b36(n):
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while n:
        out = digits[n % 36] + out
        n //= 36
    return out or "0"
print(f"av1.{b36(deadline)}.{secrets.token_hex(16)}")
PY
}

# send_msg <broker_url> <from_run> <token> <to_run> <generation-or-''> <message>
send_msg() {
  URL="$1" FROM="$2" TOKEN="$3" TO="$4" GENERATION="$5" MESSAGE="$6" MSG_ID="$(mint_msg_id)" \
  python3 - <<'PY'
import json, os, urllib.error, urllib.request
body = {
    "run_id": os.environ["FROM"], "token": os.environ["TOKEN"],
    "from_run_id": os.environ["FROM"], "to_run_id": os.environ["TO"],
    "type": "agent_message",
    "payload": {"id": os.environ["MSG_ID"], "from": os.environ["FROM"],
                "from_run_id": os.environ["FROM"], "to_run_id": os.environ["TO"],
                "message": os.environ["MESSAGE"], "role": "agent"},
}
if os.environ["GENERATION"]:
    body["generation"] = os.environ["GENERATION"]
req = urllib.request.Request(os.environ["URL"] + "/send",
    data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        print(json.dumps({"status": resp.status, "body": json.loads(resp.read().decode() or "{}")}))
except urllib.error.HTTPError as err:
    raw = err.read().decode()
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = raw
    print(json.dumps({"status": err.code, "body": parsed}))
PY
}

poll() { # poll <broker_url> <run_id> <token>
  curl -sS -X POST "$1/poll-control" -H 'Content-Type: application/json' \
    -d "{\"run_id\":\"$2\",\"token\":\"$3\"}"
}

record() { # record <name> <json...>
  printf '%s\n' "$2" >"$EVIDENCE/$1.json"
  printf '%s: %s\n' "$1" "$2"
}

# --- registered runs ---------------------------------------------------------

HUB_TOKEN=$(register_run "$HUB_URL" rt_hub)
A_TOKEN=$(register_run "$BRANCH_A_URL" rt_a)
A2_TOKEN=$(register_run "$BRANCH_A_URL" rt_a2)
B_TOKEN=$(register_run "$BRANCH_B_URL" rt_b)
C_TOKEN=$(register_run "$BRANCH_C_URL" rt_c)

# --- probe 1: versioned tell without a generation ---------------------------

record 01-generation-mismatch "$(send_msg "$BRANCH_A_URL" rt_a "$A_TOKEN" rt_a2 '' 'ping without generation')"

# --- probe 2: the same tell with the generation /sessions advertises --------

GENERATION=$(sessions_get "$BRANCH_A_URL" rt_a "$A_TOKEN" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("generation",""))')
record 02-generation-send "$(send_msg "$BRANCH_A_URL" rt_a "$A_TOKEN" rt_a2 "$GENERATION" 'ping with generation')"
sleep 0.3
record 02-generation-poll "$(poll "$BRANCH_A_URL" rt_a2 "$A2_TOKEN")"

# --- probe 3: siblings hidden in a branch listing, sendable by qualified ID --

record 03-branch-a-listing "$(sessions_get "$BRANCH_A_URL" rt_a "$A_TOKEN")"
HUB_LISTING=$(sessions_get "$HUB_URL" rt_hub "$HUB_TOKEN")
record 03-hub-listing "$HUB_LISTING"
SIBLING_ID=$(printf '%s' "$HUB_LISTING" | python3 -c '
import json, sys
rows = json.load(sys.stdin).get("sessions", [])
match = next((r["run_id"] for r in rows if r.get("run_id", "").startswith("rt_b@")), "")
print(match)')
if [ -n "$SIBLING_ID" ]; then
  record 03-sibling-send "$(send_msg "$BRANCH_A_URL" rt_a "$A_TOKEN" "$SIBLING_ID" "$GENERATION" 'ping to qualified sibling')"
else
  record 03-sibling-send '{"error": "no rt_b@ qualified row in the hub listing"}'
fi

# --- probe 4: hub peer listed to C, send from C to that hub peer -------------

record 04-branch-c-listing "$(sessions_get "$BRANCH_C_URL" rt_c "$C_TOKEN")"
C_GENERATION=$(sessions_get "$BRANCH_C_URL" rt_c "$C_TOKEN" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("generation",""))')
# The hub peer a sub-branch sees is the hub's own relayed row (rt_hub); a
# qualified hub row (run@branch-…) is absent from the sub-branch listing.
record 04-hub-peer-send "$(send_msg "$BRANCH_C_URL" rt_c "$C_TOKEN" rt_hub "$C_GENERATION" 'ping to hub peer')"

echo
echo "evidence written under $EVIDENCE"
