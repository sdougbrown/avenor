# Avenor: Phase Loop

When you need to run a sequence of agent sessions — some once, some
repeatedly — and have Avenor manage the iteration, exit conditions, and
handoff between them.

## Problem

A single `avenor run` handles one task well, but real workflows alternate
between phases: build once, then test → review → fix until clean. Doing
this today means an external shell script calling `avenor run` in a loop,
gluing results together with bespoke logic. Avenor already holds the run
ID, event log, sentinel, and retry machinery — it's the right place to own
the loop.

## Quick start

Create a loop config file:

```json
{
  "max_iterations": 5,
  "pre": [
    {
      "name": "build",
      "prompt": "Build the project. Fix any compilation errors until the build succeeds."
    }
  ],
  "loop": [
    {
      "name": "test",
      "prompt": "Run the test suite. If all tests pass, emit [loop: exit | tests green]. Otherwise report the failures."
    },
    {
      "name": "fix",
      "prompt": "Fix the test failures reported in the previous phase."
    }
  ]
}
```

Run it:

```bash
avenor run --loop-file loop.json --auto-approve --sentinel-file run.done
```

That's it. Avenor runs `build` once, then repeats `test` → `fix` until the
test phase emits `[loop: exit]` or five iterations pass.

`--prompt` and `--prompt-file` are optional when `--loop-file` is set. If
you provide one, it runs as an implicit pre-phase (named `(initial)`)
before any phases in the config.

## Loop config file

A JSON file with three top-level keys:

| Key | Type | Required | Description |
|---|---|---|---|
| `pre` | `Phase[]` | At least one of `pre` or `loop` must be non-empty | Phases that run once, in order, before the loop |
| `loop` | `Phase[]` | At least one of `pre` or `loop` must be non-empty | Phases that repeat until an exit condition fires |
| `max_iterations` | `int` | No — defaults to `10` | Maximum loop iterations. Must be ≥ 1 when `loop` is non-empty |

### Phase fields

Each phase object requires:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | Yes | Unique within the config. Reserved: `(initial)` |
| `prompt` | `string` | One of `prompt` or `prompt_file` | Inline prompt text sent to the agent. Supports template variables |
| `prompt_file` | `string` | One of `prompt` or `prompt_file` | Path to a file containing the prompt. Relative paths are resolved from the config file's directory. Supports template variables |
| `resume_from_previous` | `boolean` | No — defaults to `false` | Resume the immediately preceding phase's session instead of starting fresh |

Phase names must be unique across `pre` and `loop`. The name `(initial)` is
reserved for the implicit pre-phase created when you pass `--prompt` or
`--prompt-file` alongside `--loop-file`.

## Pre vs loop phases

**Pre phases** run once, before the loop starts. If any pre phase exits
with a stop reason other than `end_turn`, the entire run fails immediately
— the loop never begins.

**Loop phases** run in sequence from top to bottom, then repeat from the
top. The loop stops when:

- A phase agent emits `[loop: exit]` — clean completion
- A phase agent emits `[loop: abort]` — blocked, needs escalation
- The iteration count reaches `max_iterations`
- Any phase exits with a non-clean stop reason
- The run is cancelled or times out

Phases always run to the natural end of their session before Avenor acts on
a marker. This gives the agent time to write findings or clean up before
control returns to the loop runner.

## Loop markers

Agents signal loop control by emitting specially formatted lines in their
output:

```
[loop: exit]                    clean completion, stop iterating
[loop: exit | tests green]      with label
[loop: continue]                explicit no-op (for readability in prompts)
[loop: abort]                   blocked, needs escalation
[loop: abort | architectural issue: layering violation in pkg/db]
```

### Format rules

- Case-insensitive directive word (`exit`, `abort`, `continue`)
- Optional pipe-separated label after the directive
- The line must match the whole-line pattern — markers embedded in prose are ignored
- Markers inside fenced code blocks (` ``` ` or `~~~`) are ignored
- Unknown directive words are silently ignored (not treated as markers)

### Severity and priority

When multiple markers appear in the same phase session, the most severe
wins: **abort > exit > continue**. If a phase emits `[loop: exit]` and
later `[loop: abort]`, the phase is treated as aborted.

Avenor extracts loop markers from the same chunk events already scanned for
`[status: ...]` markers. The marker text is **not** stripped from the
forwarded event — raw text consumers still see it.

## Abort and escalation

An agent emits `[loop: abort | reason]` when it discovers something it
cannot resolve on its own — an architectural constraint, a decision
requiring human judgement, or a dependency on another agent's unavailable
output.

When a phase aborts:

1. **The loop stops.** No further iterations or retries.

2. **A `BLOCKED` sentinel is written.** Exit code `5`, stop reason
   `"blocked"`. The abort label is preserved as a `REASON=` line:

   ```
   BLOCKED
   SESSION=ses_abc123
   STOP_REASON=blocked
   REASON=architectural issue: layering violation in pkg/db
   RUN=a3f9...
   ```

   The `REASON=` line is omitted when the marker has no label (`[loop: abort]`
   with no pipe).

3. **The `avenor.loop.end` event** carries `exit_reason: "abort"` and
   `exit_label` set to the abort reason.

### Inter-agent escalation pattern

An orchestrating agent (jockey) watching a worker's event log via
`--on-event` sees the `avenor.loop.end` event with `exit_reason: "abort"`.
The jockey can:

- Surface the reason to a human
- Invoke a specialist agent with the abort label as prompt context
- Re-invoke the original worker with an amended prompt that addresses the blocker

No new Avenor mechanism is needed on the jockey side — reading
`exit_reason` and `exit_label` from the event stream is enough.

## Prompt templates

Phase prompts support Go `text/template` syntax. Avenor provides the
values; you decide how to use them. Template rendering happens per phase,
per iteration — variables reflect the current state at the moment the
phase begins.

### Run context variables

| Variable | Value |
|---|---|
| `{{.RunID}}` | The run's correlation ID |
| `{{.Phase}}` | Current phase name |
| `{{.Iteration}}` | Current loop iteration (1-indexed; `0` for pre-phases) |
| `{{.MaxIterations}}` | Value of `max_iterations` |
| `{{.WorkDir}}` | Working directory |

```json
{
  "name": "status",
  "prompt": "Report progress. (Phase {{.Phase}}, iteration {{.Iteration}} of {{.MaxIterations}})"
}
```

### Git delta variables

Populated only when running inside a git repository and only when a
previous phase commit exists (empty string otherwise):

| Variable | Value |
|---|---|
| `{{.PrevPhaseCommit}}` | Git commit SHA at the end of the previous phase |
| `{{.DiffStat}}` | Output of `git diff --stat <prev-commit>..HEAD` |
| `{{.ChangedFiles}}` | Newline-separated list of files changed since previous phase |

Avenor snapshots `git rev-parse HEAD` after each phase and uses that
snapshot as the reference point for the next phase. The reference moves
forward — it reflects what the immediately preceding phase left behind, not
the start of the loop.

Delta variables are informational context. Avenor does not use them to
restrict what the agent can see or do. The scoping strategy belongs to your
prompt:

```json
{
  "name": "review",
  "prompt": "Review the branch for issues.\n\n{{if .ChangedFiles}}Since the last iteration the following files changed:\n{{.ChangedFiles}}\nReview these changes carefully, and also check whether they introduce knock-on effects elsewhere.{{else}}This is the first review pass. Cover the entire branch.{{end}}"
}
```

## resume_from_previous

By default each phase starts a fresh session — the phase prompt is the sole
input, and context flows through agent-managed files that one agent writes
and the next reads.

`resume_from_previous` opts a phase into resuming the immediately preceding
phase's session. The phase agent starts with full visibility into that
session's message history — its reasoning, tool calls, and output — without
needing to reconstruct context from handoff artefacts.

```json
{ "name": "verify", "prompt": "...", "resume_from_previous": true }
```

**Default is `false` (fresh session).** Fresh is the right default because
context accumulates. `resume_from_previous` is for tightly coupled adjacent
phases where agent-managed files are a lossy handoff — for example, a
`review → verify` pair where the verify agent benefits from the full
reasoning behind each finding, not just the summary written to a file.

Accumulation is bounded: each phase that sets the flag extends only its
immediate predecessor's session, not a chain running back to the start of
the loop. Phase N resumes phase N-1; if phase N+1 also sets the flag it
resumes phase N (which already incorporated N-1). The context window grows,
but only one phase at a time, and you control which phases participate.

### When not to use it

- Phases with naturally self-contained prompts (build, test, fix) rarely need it.
- When the flag is set for the first phase of the loop (index 0), it is
  silently ignored — there is no preceding phase to resume from.
- When set on a pre phase, it is silently ignored — pre phases always start
  fresh.
- On the first loop iteration, only the first phase is affected; subsequent
  loop phases in the same iteration *can* resume from their predecessor
  within that iteration.

## Lifecycle events

Avenor emits four synthetic events during a loop run. All carry `run_id`,
`ts`, and phase-related fields. Subscribe with `--on-event` to receive
them as NDJSON.

### avenor.loop.start

Emitted once before the first pre-phase (or the first loop phase if there
are no pre phases).

```json
{
  "event": "avenor.loop.start",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "max_iterations": 5,
  "pre_phase_count": 1,
  "loop_phase_count": 4
}
```

### avenor.phase.start

Emitted immediately before each phase's session begins.

```json
{
  "event": "avenor.phase.start",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "phase": "test",
  "iteration": 2,
  "kind": "loop"
}
```

`kind` is `"pre"` or `"loop"`. `iteration` is 0 for pre-phases, 1-indexed
for loop phases.

### avenor.phase.end

Emitted after each phase's session ends (before any backoff or retry).

```json
{
  "event": "avenor.phase.end",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "phase": "test",
  "iteration": 2,
  "stop_reason": "end_turn",
  "exit_marker": true,
  "exit_marker_label": "tests green"
}
```

Marker fields appear only when a marker fired during the phase:

| Field | Present when |
|---|---|
| `exit_marker: true` | `[loop: exit]` was seen |
| `exit_marker_label` | exit marker had a label |
| `abort_marker: true` | `[loop: abort]` was seen |
| `abort_marker_label` | abort marker had a label |

If both markers appear in the same phase, only the abort fields are set
(abort takes priority).

### avenor.loop.end

Emitted once after the loop finishes, regardless of how it ended.

```json
{
  "event": "avenor.loop.end",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "iterations_completed": 2,
  "exit_reason": "abort",
  "exit_label": "architectural issue: layering violation in pkg/db"
}
```

`exit_reason` is one of:

| Value | Meaning |
|---|---|
| `marker` | A phase emitted `[loop: exit]` |
| `abort` | A phase emitted `[loop: abort]` |
| `max_iterations` | Loop reached `max_iterations` with no exit marker |
| `phase_failure` | A phase exited with a non-clean stop reason |
| `cancelled` | Run was cancelled (SIGINT) |
| `timeout` | `--timeout` was reached |
| `end_turn` | Pre phases completed but no loop phases were defined |

`exit_label` carries the label from the winning marker when `exit_reason`
is `marker` or `abort`; absent otherwise.

## Sentinel outcomes

A single sentinel is written after the entire loop finishes (not after each
phase). The exit code reflects the loop's overall outcome:

| Outcome | Status | Exit code |
|---|---|---|
| Clean exit (marker or max_iterations) | `DONE` | `0` |
| Abort marker | `BLOCKED` | `5` |
| Phase non-clean stop | `FAILED` | Phase exit code |
| Timeout | `TIMEOUT` | `124` |
| Cancellation | `KILLED` | `130` |

For the `BLOCKED` sentinel, a `REASON=` line is included when the abort
marker carried a label:

```
BLOCKED
SESSION=ses_abc123
STOP_REASON=blocked
REASON=architectural issue: layering violation in pkg/db
RUN=a3f9...
```

If you need per-phase sentinels (for external monitoring), subscribe to
`avenor.phase.end` events via `--on-event` and write your own markers.
Avenor does not provide per-phase sentinels.

## CLI invocation

```
avenor run --loop-file <path> [other flags...]
```

### What's required

`--loop-file` is the path to your loop config JSON. When set, `--prompt`
and `--prompt-file` become optional. If you provide one, it runs as an
implicit pre-phase named `(initial)` before any config-defined phases.

### Mutual exclusions

- `--loop-file` and `--resume` are mutually exclusive. The loop runner
  manages session lifecycle internally; it rejects external resume requests.

### Shared flags

All other `avenor run` flags apply uniformly across every phase:
`--agent`, `--dir`, `--model`, `--timeout`, `--max-retries`,
`--auto-approve`, `--permission-handler`, `--sentinel-file`,
`--on-event`, `--run-id`, `--control-socket`.

Retries (`--max-retries`) apply per phase: if a phase exits with code 1
(transient failure), Avenor retries that phase with exponential backoff
(2 to 30 seconds) before advancing or failing the loop.

## Stable spawn

When using `avenor stable`, spawn a loop run by setting `loop_file` in the
spawn parameters:

```json
{
  "prompt": "Initial instructions",
  "loop_file": "loop.json",
  "dir": "/repo/A",
  "label": "phase-loop-example"
}
```

The supervisor detects the `loop_file` field and routes the spawn through
the loop runner instead of the normal single-session path.

### What's different from a normal spawn

The `SpawnResult` returned by a loop spawn has an empty `SessionID`:
```json
{
  "runtime_id": "rt_1",
  "session_id": "",
  "on_event": "/tmp/avenor-stable/.../events.ndjson",
  "sentinel_file": "/tmp/avenor-stable/.../sentinel.env"
}
```

Loop phases each get their own session; those session IDs appear in the
event stream (`avenor.phase.start`, `avenor.phase.end` events) rather than
in the spawn result. Other stable operations (cancel, prompt, list) work
on the runtime as a whole — you cancel the entire loop run, not an
individual phase.

## Config validation

On load, before any phase runs, Avenor validates:

- At least one of `pre` or `loop` must be non-empty. An entirely empty
  config is rejected.
- `max_iterations` must be ≥ 1 when `loop` is non-empty.
- Every phase must have a non-empty `name` and exactly one of `prompt` or `prompt_file`. Setting both is an error.
- Phase names must be unique across `pre` and `loop`.
- The name `(initial)` is reserved and cannot be used explicitly.
- `--prompt` / `--prompt-file` with `--loop-file` inserts an unnamed
  pre-phase at index 0 (emitted in events as `phase: "(initial)"`).

If validation fails, Avenor logs the error to stderr, writes a `FAILED`
sentinel (if `--sentinel-file` is set), and exits with code 1.

## Out of scope

Loop is intentionally single-level and serial. Things not supported:

- **Parallel phases** — serial execution avoids coordination problems
  (shared file writes, event ordering) with unclear benefit for the primary
  use case.
- **Conditional phase skipping** — phases always run in order; skip logic
  belongs in the phase prompt ("if X is already clean, emit [loop: exit]").
- **Per-phase `--max-retries`** — the existing retry flag applies to each
  phase individually; that's enough.
- **Cross-session context injection** — agent-managed files are the handoff
  mechanism; Avenor does not summarise or inject prior session output.
- **Loop nesting** — one level only.
- **Non-JSON config formats** — YAML would require a new dependency.
