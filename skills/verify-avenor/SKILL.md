---
name: verify-avenor
description: Drive the real avenor CLI end to end — launch a run, exercise a feature through the actual binary, and capture sentinel/event evidence. Use before claiming user-facing avenor work is done, or whenever a change touches run, stable/control, watch, answer, or verify behavior.
---

# Verify avenor (CLI)

Avenor is a Go CLI that orchestrates agent runs. Its primary surface is the
`avenor` binary; there is no server to keep alive for the core features. The
secondary surfaces are the embedded board web UI (needs `-tags board`) and the
MCP server (`avenor mcp`) — see `features/README.md` for coverage.

All commands below are run from the repo root (the checkout containing this skill).

## Launch

Build the binary (the mise task sets isolated caches; use it):

```sh
mise run go-build        # produces ./avenor
```

If `mise` is unavailable, the equivalent is:

```sh
mkdir -p .cache/go-build .cache/go-mod
GOCACHE=$PWD/.cache/go-build GOMODCACHE=$PWD/.cache/go-mod \
  go build -o avenor ./cmd/avenor
```

There is no daemon for single runs — "launch" means: build once, then start
each drive as its own short-lived `avenor run` invocation in a fresh scratch
dir. For supervisor features, `avenor stable` is itself the process under
test; record its PID at launch (see `features/stable-control.md`).

Every drive gets its own scratch directory (event log, sentinel, socket live
there). Never point a verification run's `--dir` at the repo root — the agent
under test gets write access to that directory.

## Doctor

Run this first whenever anything looks off. It answers "is this instance worth driving?":

```sh
./avenor --help 2>&1 | grep -q backend && echo "binary OK"  # usage output means build is runnable
# (note: --help exits 1 by design; only the usage output is the signal)
command -v pi >/dev/null && echo "pi backend OK"  # the drive backend exists
```

If the binary is stale relative to your changes, rebuild — `go build` is
incremental and fast. The pi backend needs `pi` on `PATH` and a working
default model; the cheapest connectivity test is the smoke drive in
`features/single-run.md` with the one-word prompt.

## Drive

The driver is the real binary with the real `pi` backend. One session per
invocation; prompts must be cheap ("Reply with the single word OK and nothing
else.") because every drive is a real model call.

Single run (the core drive — full recipe in `features/single-run.md`):

```sh
SCRATCH=$(mktemp -d /tmp/avenor-verify.XXXXXX)
./avenor run --backend pi \
  --prompt "Reply with the single word OK and nothing else." \
  --dir "$SCRATCH" \
  --on-event "$SCRATCH/events.ndjson" \
  --sentinel-file "$SCRATCH/done.env" \
  --timeout 90s --label verify-smoke
```

Supervisor drives (spawn/list/status/prompt/cancel via control socket):
`features/stable-control.md`.

Event stream reads: `features/watch.md`. Permission answers:
`features/permissions.md`. Config validation without a live agent:
`features/verify-command.md`.

Stable handles over coordinates: sentinel file keys (`DONE`, `SESSION=`,
`STOP_REASON=end_turn`), NDJSON event names (`session.start`,
`session.end`, `avenor.turn.start`), control-socket JSON fields (`status`,
`exit_code`, `final_output`, `pending_permission`). Do not parse human-formatted
CLI output when a sentinel or event field exists.

## Evidence

Capture into a per-run evidence dir that outlives cleanup:

```sh
EVIDENCE=/tmp/avenor-verify-evidence/$(date +%Y%m%d-%H%M%S)
mkdir -p "$EVIDENCE"
```

For a feature proof, capture **all** of:

1. The command invocation (the full argv you ran).
2. The sentinel file contents (`cat done.env`) — proves the run reached
   `end_turn` with the session/run IDs.
3. The event log (`events.ndjson`) — proves the event pipeline produced
   `session.start` and a terminal `session.end` with `stop_reason`.
4. For supervisor features: the `avenor control ... status` JSON showing
   `status`, `exit_code`, and `final_output`.
5. Side effects the run created (files the agent wrote, permission request/
   response files) checked after the run exits.

Proof standard: a feature is proven when the observable end state named in its
`features/<feature>.md` file is present in the captured evidence — not when
the command merely exits 0. Exercise the real user path (a real `avenor run`
through a real backend); there are no test-only endpoints to shortcut through.

Mock nothing. If a backend is unavailable, the doctor fails and that is the
finding — do not fake a drive. Keep prompts to single-word replies to bound
cost; that is a budget choice, not a mock.

## Cleanup

- Single runs: nothing to kill — the process exits when the run ends. Remove
  scratch dirs (`rm -rf /tmp/avenor-verify.XXXXXX`) after copying evidence out.
- `stable` supervisors: shut down via the control plane first, then kill only
  the PID you recorded at launch:
  `avenor control --socket <sock> shutdown graceful; kill <recorded-pid>`.
  Never kill by process name.
- Remove socket files and `/tmp/avenor-stable/` child dirs created by your
  runs. Never delete the evidence dir.

## Maintaining this skill

The `features/` map is this repo's verification source of truth. When a change
alters how a feature is driven or what its proof state is, update that
feature's map file in the same change. When a new user-facing feature ships
(new subcommand, new event type used as a proof point, new sentinel key), add
its map file. A map that drifts from the binary teaches the next agent wrong.
