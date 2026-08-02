# Issue 132 correction execution record

## Session

- Branch: `issue/132-pretty-tool-calls`
- Session: `019fc055-8912-7df1-901e-8bf4acc38578`
- Agent: `horse`
- Model: `gpt-5.6-terra` (`openai-codex`)
- Scope: tests only; no implementation defect surfaced.

## Corrections

- Added exact Pi execute-contract assertions for status, result, inspect, permission, follow-up, events, and shutdown. Each verifies the pretty-printed JSON wire content and expected details; inspect uses `buildInspectPayload` as its expected payload.
- Added hostile, over-limit renderer coverage for Pi and OpenCode: 12 status/event rows, 8 permission options/event types, 5 rows per inspect group, and 8 cleanup paths. The tests assert sanitization, exact omission counts, and preservation of Pi details/OpenCode metadata.

## Verification

- `env -u PI_AGENT_PROFILE bun run --cwd packages/pi test` — passed (67 tests).
- `env -u PI_AGENT_PROFILE bun run --cwd packages/opencode test` — passed (19 tests).
- `bun run --cwd packages/pi build && bun run --cwd packages/opencode build` — passed.
- `env -u PI_AGENT_PROFILE bun run test` — passed (all workspace package suites).
- `go test ./...` — passed.
- `bun run build` — passed.
- `go vet ./...` — passed.

`PI_AGENT_PROFILE` is unset for Bun verification because this harness supplies `hybrid`, while the Pi profile test intentionally exercises the no-environment session-profile path.

## Initial delivery

The correction commit is authored and committed as `avenor horse <019fc055-8912-7df1-901e-8bf4acc38578>`. No push, PR, or merge was performed.

## Tail-preview follow-up

- Session: `019fc061-e369-7125-9fba-007a26e1d0f1`
- Agent: `horse`
- Model: `gpt-5.6-terra` (`openai-codex`)
- Corrected the OpenCode renderer's tail preview to retain the final 12 lines after character clipping, so a final conclusion is not discarded.
- Added a multiline result regression that asserts the character marker (`923`), line marker (`19`), ordered tail contents, and unchanged raw metadata.

### Verification

- `env -u PI_AGENT_PROFILE bun run --cwd packages/opencode test` — passed (20 tests).
- `env -u PI_AGENT_PROFILE bun run --cwd packages/opencode build` — passed.
- `env -u PI_AGENT_PROFILE bun run test` — passed (all workspace JavaScript package suites).
- `go test ./...` — passed.

### Delivery

This follow-up is committed as `avenor horse <019fc061-e369-7125-9fba-007a26e1d0f1>`. No push, PR, or merge was performed.

## Documentation follow-up

- Branch: `issue/132-pretty-tool-calls`
- Session: `019fc0bb-43d0-7900-8ce8-aecea2efe79b`
- Agent: `horse`
- Model: `gpt-5.6-terra` (`openai-codex`)
- Restored the three documentation files to `4fd4e01` before applying the narrow correction.
- Pi docs now state that non-spawn tools return JSON model content and `details`, while rendering changes only Pi's display.
- OpenCode docs now state that seven non-spawn tools return bounded prose in shared `output`, metadata stays in host/session state, and MCP clients render results.
- Preserved the Pi renderer/channel documentation and the OpenCode `avenor_inspect` reference.

### Verification

- `writetighter lint` and `writetighter revise` completed for the PR-added documentation prose.
- `bun run --cwd docs build` — passed.
- `env -u PI_AGENT_PROFILE bun run --cwd packages/pi test` — passed (67 tests).
- `env -u PI_AGENT_PROFILE bun run --cwd packages/opencode test` — passed (20 tests).

### Delivery

This follow-up is committed locally as `avenor horse <019fc0bb-43d0-7900-8ce8-aecea2efe79b>`. The branch remains unpushed and unmerged.
