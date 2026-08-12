# Changelog

## Unreleased

### Added

- `avenor await`: new wait-only subcommand from this feature work. Waits on a run until attention or done, with plain transition lines or JSON output.

## v0.7.1 — 2026-05-11

### Fixed

- `exitWithSentinel` closure is now defined immediately after `fs.Parse` succeeds,
  covering all post-parse return paths. Previously, the three early-exit paths for
  unreadable prompt file, event-log creation failure, and bad permission-handler
  returned bare `1` without writing a sentinel — stableboy would hang waiting for a
  signal that never arrived.
- `cleanupSentinelFiles` now accepts an `io.Writer` for stderr and logs any
  `os.Remove` error that is not `ErrNotExist`. Previously all removal errors were
  silently discarded, leaving stale files that could cause subsequent runs to fail
  silently.
- `TestSentinelContent`: switched assertions from `strings.Contains` line checks to
  exact byte equality. Line order in the sentinel is load-bearing (stableboy reads
  line 1 as the status header); the old checks did not catch reordering.
- `TestWriteSentinel`: same exact-equality fix for the content assertion.
- `TestSessionEndFields`: replaced hardcoded `/tmp/...` path for the missing-file
  case with `filepath.Join(t.TempDir(), "no-such-file.ndjson")` for proper test
  isolation; replaced brittle name-string dispatch with a `missingFile bool` field.
- `TestWriteSentinel`: added subtest asserting that a missing parent directory logs
  an error to stderr and leaves no sentinel file behind.

## v0.7.0 — 2026-05-11

### Added

- `avenor run --sentinel-file <path>`: write a completion sentinel file after every run
  (clean, timeout, signal, or error), matching the output format of `dispatch-avenor.sh`.
  - Sentinel format by exit code:
    - `0` → `DONE\nSESSION=...\nSTOP_REASON=...\n` (default stop_reason: `end_turn`)
    - `124` → `TIMEOUT\nSESSION=...\nSTOP_REASON=...\n` (default: `timeout`)
    - `130` → `KILLED\nSESSION=...\nSTOP_REASON=...\nEXIT_CODE=130\n` (default: `cancelled`)
    - other → `FAILED\nSESSION=...\nSTOP_REASON=...\nEXIT_CODE=N\n` (default: `exit_N`)
  - Session metadata is extracted from the last `session.end` event in the event log.
  - Sentinel is written via atomic tmp+rename to avoid partial-file races on the watcher side.
- When `--sentinel-file` is set and `--permission-handler` is not explicitly provided,
  `avenor run` auto-derives the permission handler base from the sentinel path:
  `<base>.done` → `<base>.perm`; any other suffix → `<sentinel>.perm`.
  This mirrors the `derive_permission_base` function in `dispatch-avenor.sh`.
  An explicit `--permission-handler` always wins — derivation is skipped.
- Pre-run cleanup when `--sentinel-file` is active: removes the existing sentinel and the
  permission request files (`<perm-base>.req`, `<perm-base>.req.response`) before each run,
  matching the shim's pre-run setup. Callers without `--sentinel-file` see no change.

### Notes

- This release deprecates `dispatch-avenor.sh` consumer-side. Callers can now pass
  `--sentinel-file <path>` directly to `avenor run` instead of routing through the shim.
  Consumer cutover (deleting `dispatch-avenor.sh` from `.botfiles`) is a follow-on step.
- Callers that do not pass `--sentinel-file` see zero behavior change.

## v0.6.0 — 2026-05-11

### Added

- `avenor watch --classify`: prefix each plain-format digest line with `MILESTONE`,
  `FINDING`, or `ACTIVITY` based on event type and content heuristics.
  - **MILESTONE**: `session.end`, `permission.request`, `tool.call`/`tool.call_update`
    where `kind==commit`, `kind==task` with `status==completed`, or title/command
    contains `git commit`.
  - **FINDING**: message/thought/user chunks containing `[finding]` marker,
    `reviewer flagged`, `correction needed`, `failed test`, or `confidence: N%`
    where N ≥ 60.
  - **ACTIVITY**: everything else (default).
- `--format json --classify`: injects a top-level `"classify"` field into each
  emitted JSON object without altering other fields.
- Classification rules live in `internal/digest/classify.go` as the single source
  of truth; encoding what was previously prose in `agents/groom.md`.

## v0.5.0 — 2026-05-11

### Added

- `avenor answer <perm-base>` subcommand: reads `<perm-base>.req`, validates
  `--option` against the offered set, and atomically writes
  `<perm-base>.req.response`. Flags: `--option <id>` (required),
  `--message <text>` (optional), `--outcome selected|cancelled` (default
  `selected`), `--force` (overwrite existing response).
- Replaces the printf/jq response-write block in `opencode/skills/answer-jockey`
  and `agents/groom.md` (Stage 3 of the avenor-subsume-consumer-prose refactor).

## v0.4.1 — 2026-05-11

### Fixed

- `avenor watch` plain mode now emits nothing for JSON lines lacking an `event` field. Previously it emitted `EVENT   ` noise for legacy text-protocol input that had no event key.

## v0.4.0 — 2026-05-11

### Added

- `avenor watch --since-cursor <path>`: persist byte offset to a cursor file, atomically rewrite the cursor on EOF, rewrite every 10 events in follow mode.

## v0.3.0 — 2026-05-11

### Added

- `avenor watch <log>` subcommand: plain digest format (`EVENT name session_id excerpt`), per-event excerpt mapping (`.content.text` for chunks, `kind:title [status]` for tools, etc.), `--follow` and `--format json` flags.

## v0.2.0 — 2026-05-11

### Added

- Documented the `--permission-handler file:<path>` flow end to end (`docs/permission-handler.md` was added in v0.1.0; v0.2.0 promotes it to README + consumer integration story).
- `templates/opencode/skills/answer-jockey/SKILL.md` is now the reference consumer skill for responding to permission requests (installed downstream in `.botfiles`).

### Notes

- Version tag is now injected via ldflags (`-X main.Version={{ .Version }}`), so `avenor --version` reports the release tag instead of a hardcoded constant.

## v0.1.1 — 2026-05-11

### Changed

- `cmd/avenor/main.go`: replaced `const Version` with `var Version` so goreleaser's `-ldflags -X main.Version=...` injects the real tag. Previously `avenor --version` reported `v0.0.1` regardless of the release.

## v0.1.0 — 2026-05-11

### Added

- Initial MVP CLI: `avenor`, `avenor probe`, `--prompt-file`, `--on-event`, `--dir`, `--model`, `--timeout`, `--agent`.
- `opencode-acp` runtime adapter (`internal/runtime/opencodeacp/`).
- File permission handler (`--permission-handler file:<path>`, `internal/permission/`).
- NDJSON event vocabulary (`agent.message_chunk`, `agent.thought_chunk`, `tool.call`, `tool.call_update`, `user.message_chunk`, `session.plan`, `permission.request`, `session.end`); `docs/events.md`.
- Stop reasons: `end_turn` / `cancelled` / `max_tokens` / `max_turn_requests` / `refusal` / `timeout`. Exit map: `end_turn=0`, `refusal=2`, `max_tokens=3`, `max_turn_requests=4`, `cancelled=130`, `timeout=124`.

## v0.0.1 — 2026-05-11

Initial scaffolding.
