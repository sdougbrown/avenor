# Config validation — `avenor verify`

## Sub-features

- Roster file validation (`--roster-file`, optional `--roster-entry`)
- Loop config validation (`--loop-file`)
- Team config validation (`--team-file`)
- Cross-file recursion (loop/team configs referencing other files)
- Error surfacing on stderr, `ok:` lines on stdout, exit code 1 on any error

## How to get to it (user POV)

Before spending tokens on a run, a user validates their roster/loop/team
configs with `avenor verify` — it loads and checks structure, unknown fields,
mutual exclusions, and roster entry references without starting an agent.

## Driving it with the avenor binary

No backend or model needed — this is the fully offline drive:

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
cat > "$SCRATCH/roster.json" <<'JSON'
{
  "planner": {"backend": "opencode-acp", "model": "provider/planner"},
  "worker":  {"backend": "opencode-acp", "agent": "mule"}
}
JSON

./avenor verify --roster-file "$SCRATCH/roster.json" --roster-entry planner
echo "exit=$?"    # expect 0, stdout has "ok: ..." lines

# Negative case: unknown entry
./avenor verify --roster-file "$SCRATCH/roster.json" --roster-entry nonexistent
echo "exit=$?"    # expect 1, stderr names the error
```

**Observable end state (the proof):** valid config → exit 0 with `ok:` lines
naming each checked file; invalid config → exit 1 with `error: ...` on stderr;
no agent process is ever started.

## Gotchas

- At least one of `--loop-file`, `--team-file`, `--roster-file` is required;
  bare `avenor verify` is a usage error (exit 1).
- `--roster-entry` requires `--roster-file`.
- Rosters are strict: unknown entry fields are rejected, not ignored — a
  "valid" roster with a stray field is a valid negative test.
- This is the only mapped feature that never touches a model; use it as the
  fallback proof when the backend is down.
