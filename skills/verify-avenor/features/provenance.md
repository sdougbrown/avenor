# Launch identity — `avenor run --team-file` + a local MCP server

## Sub-features

- Launch identity exported to a local stdio MCP server loaded from opencode config
- Three-member `opencode-acp` team attribution, one tool call per member
- CLI and supervisor ID mapping (`AVENOR_EXECUTION_ID` → event `runtime_id`)
- Removal of inherited identity, including an absent loop iteration
- Join from an observation to member `stop_reason` and team `exit_reason`

## How to get to it (user POV)

A team of reviewers runs under Avenor. Each reviewer is an agent runtime that
launches a local MCP server from opencode config. When the model calls the
server's tool, the server records the launch identity from its own environment.
The user reads those records and the NDJSON event log, then joins each record to
the member's `avenor.phase.end` and the team's `avenor.team.end`.

The variable table and trust rules are in
[docs/events.md](../../../docs/events.md#launch-identity). Per-backend delivery
and the `opencode-http`/`pony` limits are in
[docs/backends.md](../../../docs/backends.md#launch-identity).

## Driving it with the avenor binary

### 1. Doctor

Run from the repo root. Missing `opencode` or model auth is a blocker: do not
substitute another backend.

```sh
mise run go-build
./avenor --help 2>&1 | grep -q backend && echo "binary OK"   # usage exits 1 by design
command -v opencode >/dev/null || { echo "doctor: opencode missing, stop"; exit 1; }
```

### 2. Scratch, evidence, fixture

```sh
SCRATCH=$(mktemp -d /tmp/avenor-provenance.XXXXXX)
EVIDENCE=/tmp/avenor-verify-evidence/$(date +%Y%m%d-%H%M%S)
mkdir -p "$SCRATCH/work" "$SCRATCH/fixture" "$SCRATCH/xdg/opencode" "$EVIDENCE"
export XDG_CONFIG_HOME="$SCRATCH/xdg"
opencode --version | tee "$EVIDENCE/opencode-version.txt"
```

Build the observation server. It records only the seven identity variables from
its own environment and never reads `os.Environ()` or writes credentials.

```sh
cat > "$SCRATCH/fixture/main.go" <<'GO'
// Records Avenor's launch-identity variables for each observe_identity call.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var keys = []string{
	"AVENOR_RUNTIME_ID", "AVENOR_EXECUTION_ID", "AVENOR_KIND", "AVENOR_PHASE",
	"AVENOR_ITERATION", "AVENOR_LABEL", "AVENOR_ATTEMPT",
}

type request struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "identity-observer: observation file path required")
		os.Exit(2)
	}
	out := os.Args[1]
	enc := json.NewEncoder(os.Stdout)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)

	for scanner.Scan() {
		var req request
		if len(scanner.Bytes()) == 0 || json.Unmarshal(scanner.Bytes(), &req) != nil {
			continue
		}
		if len(req.ID) == 0 || string(req.ID) == "null" {
			continue
		}
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "identity-observer", "version": "1"},
			}
		case "ping":
			result = map[string]any{}
		case "tools/list":
			result = map[string]any{"tools": []map[string]any{{
				"name":        "observe_identity",
				"description": "Record the Avenor launch identity visible to this process. Call once with no arguments.",
				"inputSchema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}},
			}}}
		case "tools/call":
			record(out)
			result = map[string]any{"content": []map[string]any{{"type": "text", "text": "recorded"}}}
		default:
			result = map[string]any{}
		}
		_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}
}

func record(out string) {
	vars := map[string]string{}
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			vars[key] = value
		}
	}
	rec := map[string]any{
		"ts":   time.Now().UTC().Format(time.RFC3339Nano),
		"pid":  os.Getpid(),
		"ppid": os.Getppid(),
		"vars": vars,
	}
	line, _ := json.Marshal(rec)
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "identity-observer: %v\n", err)
		return
	}
	_, _ = f.Write(append(line, '\n'))
	_ = f.Close()
}
GO
GOCACHE="$PWD/.cache/go-build" go build -o "$SCRATCH/identity-observer" "$SCRATCH/fixture/main.go"
```

### 3. opencode config with the MCP entry

Point `OPENCODE_BASE_CONFIG` at an opencode config that already runs a model.
The default is the user's global config. Add the local server with no
`environment` block, so the server sees only what the harness passes.

```sh
: "${OPENCODE_BASE_CONFIG:=$HOME/.config/opencode/opencode.json}"
cp "$OPENCODE_BASE_CONFIG" "$SCRATCH/xdg/opencode/opencode.json"
python3 - "$SCRATCH" <<'PY'
import json, os, sys
scratch = sys.argv[1]
path = os.path.join(scratch, "xdg", "opencode", "opencode.json")
config = json.load(open(path))
config.setdefault("mcp", {})["identity-observer"] = {
    "type": "local",
    "enabled": True,
    "command": [os.path.join(scratch, "identity-observer"),
                os.path.join(scratch, "observations.ndjson")],
}
json.dump(config, open(path, "w"), indent=2)
PY
XDG_CONFIG_HOME="$SCRATCH/xdg" opencode mcp list   # must list identity-observer
```

`XDG_CONFIG_HOME` isolates opencode from the user's global config. If
`opencode mcp list` fails, the base config is invalid: fix it or point
`OPENCODE_BASE_CONFIG` at a working file. That is a doctor failure, not a
provenance result.

### 4. Team file and join checker

```sh
cat > "$SCRATCH/team.json" <<'JSON'
{"team": [
  {"name": "reviewer", "prompt": "Call the observe_identity tool exactly once, then reply OK."},
  {"name": "test-reviewer", "prompt": "Call the observe_identity tool exactly once, then reply OK."},
  {"name": "security-reviewer", "prompt": "Call the observe_identity tool exactly once, then reply OK."}
]}
JSON

cat > "$SCRATCH/join.py" <<'PY'
"""Join recorded launch identity to phase and team completion events."""
import json
import sys

obs_path, events_path, surface = sys.argv[1], sys.argv[2], sys.argv[3]
members = ["reviewer", "test-reviewer", "security-reviewer"]
observations = [json.loads(line) for line in open(obs_path) if line.strip()]
events = [json.loads(line) for line in open(events_path) if line.strip()]

assert len(observations) == 3, f"observations = {len(observations)}, want one per member"
seen = set()
execution_id = None
for obs in observations:
    env = obs["vars"]
    assert env["AVENOR_KIND"] == "team", env
    assert env["AVENOR_ATTEMPT"] == "1", env
    assert env["AVENOR_LABEL"] == "verify-provenance", env
    assert "AVENOR_ITERATION" not in env, f"inherited iteration survived: {env}"
    execution_id = env["AVENOR_EXECUTION_ID"]
    assert env["AVENOR_RUNTIME_ID"] == execution_id, env
    seen.add(env["AVENOR_PHASE"])
    ends = [e for e in events if e.get("event") == "avenor.phase.end"
            and e.get("runtime_id") == execution_id
            and e.get("run_kind") == env["AVENOR_KIND"]
            and e.get("phase") == env["AVENOR_PHASE"]
            and e.get("iteration") == 0]
    assert len(ends) == 1, f"{env['AVENOR_PHASE']} matched {len(ends)} phase ends"
    assert ends[0]["stop_reason"] == "end_turn", ends[0]
    if surface == "cli":
        assert ends[0]["run_id"] == execution_id, ends[0]
    else:
        assert ends[0]["run_id"] != execution_id, ends[0]
    print(f"{env['AVENOR_PHASE']}: phase.end stop_reason={ends[0]['stop_reason']}")

assert seen == set(members), f"phases = {sorted(seen)}"
team = [e for e in events if e.get("event") == "avenor.team.end"
        and e.get("runtime_id") == execution_id]
assert len(team) == 1, f"team ends = {len(team)}"
assert team[0]["exit_reason"] == "end_turn", team[0]
print(f"team.end exit_reason={team[0]['exit_reason']} execution_id={execution_id}")
PY
```

### 5. CLI team drive

Poison the harness environment with stale identity. Each observation must show
the launch values and no inherited `AVENOR_ITERATION`.

```sh
XDG_CONFIG_HOME="$SCRATCH/xdg" \
AVENOR_ITERATION=99 AVENOR_PHASE=poisoned AVENOR_LABEL=poisoned-label \
./avenor run --backend opencode-acp --team-file "$SCRATCH/team.json" \
  --dir "$SCRATCH/work" \
  --on-event "$SCRATCH/events.ndjson" \
  --sentinel-file "$SCRATCH/done.env" \
  --timeout 180s --label verify-provenance
echo "exit=$?"
cat "$SCRATCH/done.env"
python3 "$SCRATCH/join.py" "$SCRATCH/observations.ndjson" "$SCRATCH/events.ndjson" cli
```

### 6. Supervisor team drive

Run the same team through `avenor stable`. The spawn request carries
`team_file`, and `--backend opencode-acp` keeps every member a child process.

```sh
cp "$SCRATCH/observations.ndjson" "$EVIDENCE/observations-cli.ndjson"
: > "$SCRATCH/observations.ndjson"
SOCK="$SCRATCH/stable.sock"
XDG_CONFIG_HOME="$SCRATCH/xdg" \
AVENOR_ITERATION=99 AVENOR_PHASE=poisoned AVENOR_LABEL=poisoned-label \
  ./avenor stable --control-socket "$SOCK" &> "$SCRATCH/stable.log" &
STABLE_PID=$!   # record this; cleanup kills THIS pid only
until ./avenor control --socket "$SOCK" list >/dev/null 2>&1; do sleep 0.2; done

SPAWN=$(python3 - "$SOCK" "$SCRATCH/team.json" "$SCRATCH/work" "$SCRATCH" <<'PY'
import json, socket, sys
sock, team, work, scratch = sys.argv[1:5]
request = {"jsonrpc": "2.0", "id": 1, "method": "spawn", "params": {
    "team_file": team, "dir": work, "label": "verify-provenance",
    "backend": "opencode-acp", "on_event": scratch + "/sup-events.ndjson",
    "sentinel_file": scratch + "/sup-done.env", "timeout": 240}}
conn = socket.socket(socket.AF_UNIX)
conn.connect(sock)
conn.sendall((json.dumps(request) + "\n").encode())
print(conn.makefile().readline().strip())
PY
)
printf '%s\n' "$SPAWN"
RT=$(printf '%s' "$SPAWN" | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["runtime_id"])')
./avenor await --socket "$SOCK" --until done --timeout 240s "$RT"
python3 "$SCRATCH/join.py" "$SCRATCH/observations.ndjson" "$SCRATCH/sup-events.ndjson" supervisor
```

### 7. Evidence and cleanup

```sh
cp "$SCRATCH/observations.ndjson" "$EVIDENCE/observations-supervisor.ndjson"
cp "$SCRATCH/events.ndjson" "$SCRATCH/sup-events.ndjson" "$SCRATCH/done.env" "$EVIDENCE/"
./avenor control --socket "$SOCK" shutdown graceful
kill "$STABLE_PID" 2>/dev/null
rm -rf "$SCRATCH"
```

Keep `$EVIDENCE`. Do not delete it.

## Observable end state (the proof)

The drive passes when all of these are captured:

1. `opencode --version` is recorded in the evidence dir, and `opencode mcp list`
   lists `identity-observer`.
2. The CLI run exits 0. `done.env` contains `DONE`, a `SESSION=`, `RUN=` equal
   to the CLI run ID, and `STOP_REASON=end_turn`.
3. `join.py ... cli` prints one line per member with `stop_reason=end_turn` and
   a final line `team.end exit_reason=end_turn`. It also enforces exactly three
   observations, `AVENOR_KIND=team`, `AVENOR_ATTEMPT=1`,
   `AVENOR_LABEL=verify-provenance`, no `AVENOR_ITERATION`, and
   `AVENOR_EXECUTION_ID` equal to both `runtime_id` and `run_id`.
4. The supervisor drive prints the same three member joins. Its final line shows
   `execution_id=rt_N`, and the checker requires that `run_id` on those events
   is the supervisor run, not the execution ID.
5. `avenor await` exits 0 for the supervisor run. Exit 10 or 11 is a failed run
   and therefore a finding, not a pass.

## Gotchas

- Every drive is a real model call. Keep each prompt to one tool call and a
  one-word reply.
- A missing `opencode` binary or model auth is a blocker. Do not substitute
  `pi`: only a backend that starts a harness process delivers the environment.
- A broken global opencode config is a doctor failure. Isolate with
  `XDG_CONFIG_HOME` and point `OPENCODE_BASE_CONFIG` at a config that already
  passes `opencode mcp list`.
- Do not add identity to the MCP config or seed expected values anywhere except
  the poisoned harness environment. The fixture ignores tool arguments, so a
  model-authored identity cannot satisfy the join.
- A member that calls the tool twice makes its attribution ambiguous. Rerun the
  drive; do not edit the event log.
- `--dir` is where the agents work. Point it at the scratch dir, never the repo.
- `--timeout 180s` on the CLI run and `--timeout 240s` on `avenor await` bound
  a wedged backend. The supervisor PID must come from `$!`; never kill by
  process name.
- The supervisor writes child state under `/tmp/avenor-stable/`. Copy evidence
  out before cleanup.
- `opencode-http` attaches to an external server and `pony` runs in-process, so
  neither can produce this proof. See
  [backends.md](../../../docs/backends.md#launch-identity).
- Repeated nested phase names share one execution ID and can produce identical
  tuples. This recipe drives one non-nested team and does not claim uniqueness
  for nested runs.
