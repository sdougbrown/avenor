# Avenor: Phase Loop

You need a loop when you've got a workflow with distinct phases: build once, then test → fix → verify until clean. That's the loop's job — it runs your phases in sequence, stops when an agent says it's ready, or keeps spinning until a limit.

## The problem phase loops solve

A single `avenor run` handles one task well, but real workflows are cyclical. Build once, then repeat test → review → fix until the test phase says go. Today you might try to script this externally, gluing results together with bespoke logic. Or you would just tell your agent "loop until..." and hope for the best. Avenor can do this for you *deterministically*, not according to your agents whims. (Well... some whims. You might want to let your agent leave the loop early if it encounters certain conditions.) Avenor already holds the run ID, event log, sentinel, and retry machinery. It's the right place to own the loop.

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
      "prompt": "Run the test suite. If all tests pass, emit <|workflow: exit | tests green|>. Otherwise report the failures."
    },
    {
      "name": "fix",
      "resume_from_previous": true,
      "prompt": "Fix the test failures reported in the previous phase."
    }
  ]
}
```

Run it:

```bash
avenor run --loop-file loop.json --auto-approve --sentinel-file run.done
```

Avenor runs `build` once, then repeats `test` → `fix` until the test phase emits `<|workflow: exit|>` or five iterations pass. You get a single sentinel file afterward.

`--prompt` and `--prompt-file` are optional when `--loop-file` is set. If you provide one, it runs as an implicit pre-phase (named `(initial)`) before any config phases.

## How a loop run works

**Pre phases** run once, before the loop starts. If any pre phase exits with a stop reason other than `end_turn`, the entire run fails immediately — the loop never begins.

**Loop phases** run in sequence from top to bottom, then repeat from the top. The loop stops when:

- A phase agent emits `<|workflow: exit|>` — clean completion
- A phase agent emits `<|workflow: abort | reason|>` — stuck, needs escalation
- The iteration count reaches `max_iterations`
- Any phase exits with a non-clean stop reason
- The run is cancelled or times out

Phases always run to the natural end of their session before Avenor acts on a marker. This gives the agent time to write findings or clean up before control returns to the loop runner.

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

Phase names must be unique across `pre` and `loop`. The name `(initial)` is reserved for the implicit pre-phase created when you pass `--prompt` or `--prompt-file` alongside `--loop-file`.

## Loop markers

Agents signal loop control by emitting specially formatted lines:

```
<|workflow: continue|>                explicit no-op (for readability in prompts)
<|workflow: exit | tests green|>      with optional label
<|workflow: abort | architectural issue: layering violation in pkg/db|>
```

### Format rules

- Directive word (`continue`, `exit`, `abort`) enclosed in `<|workflow: ...|>`
- Optional pipe-separated label after the directive
- The line must match the whole-line pattern — markers embedded in prose are ignored
- Markers inside fenced code blocks (` ``` ` or `~~~`) are ignored
- Unknown directive words are silently ignored (not treated as markers)

### Severity and priority

When multiple markers appear in the same phase, the most severe wins: **abort > exit > continue**. If a phase emits `<|workflow: exit|>` and later `<|workflow: abort|>`, the phase is treated as aborted.

Avenor extracts loop markers from the same chunk events already scanned for `<|status: ...|>` markers. The marker text is not stripped from the forwarded event — raw text consumers still see it.

## Abort and escalation

An agent emits `<|workflow: abort | reason|>` when it hits a wall — an architectural constraint, a decision requiring human judgement, or a dependency on another agent's unavailable output.

When a phase aborts:

1. **The loop stops.** No further iterations.

2. **A `BLOCKED` sentinel is written** with exit code `5` and stop reason `"blocked"`. The abort label (if present) is preserved as a `REASON=` line:

   ```
   BLOCKED
   SESSION=ses_abc123
   STOP_REASON=blocked
   REASON=architectural issue: layering violation in pkg/db
   RUN=a3f9...
   ```

   The `REASON=` line is omitted when the marker has no label.

3. **The `avenor.loop.end` event** carries `exit_reason: "abort"` and `exit_label` set to the abort reason.

### Using abort for orchestration

An orchestrating agent (e.g., jockey) watching a worker's event log via `--on-event` sees the `avenor.loop.end` event with `exit_reason: "abort"`. The jockey can:

- Surface the reason to a human
- Invoke a specialist agent with the abort label as prompt context
- Re-invoke the original worker with an amended prompt that addresses the blocker

No new Avenor mechanism is needed — reading `exit_reason` and `exit_label` from the event stream is enough.

## Prompt templates

Phase prompts support Go `text/template` syntax. Variables reflect the current state at the moment each phase begins.

### Available variables

::: v-pre
| Variable | Value |
|---|---|
| `{{.RunID}}` | The run's correlation ID |
| `{{.Phase}}` | Current phase name |
| `{{.Iteration}}` | Current loop iteration (1-indexed; `0` for pre-phases) |
| `{{.MaxIterations}}` | Value of `max_iterations` |
| `{{.WorkDir}}` | Working directory |
:::

Example:

::: v-pre
```json
{
  "name": "status",
  "prompt": "Report progress. (Phase {{.Phase}}, iteration {{.Iteration}} of {{.MaxIterations}})"
}
```
:::

### Git delta variables

Populated only when running inside a git repository and only after a previous phase has completed:

::: v-pre
| Variable | Value |
|---|---|
| `{{.PrevPhaseCommit}}` | Git commit SHA at the end of the previous phase |
| `{{.DiffStat}}` | Output of `git diff --stat <prev-commit>..HEAD` |
| `{{.ChangedFiles}}` | Newline-separated list of files changed since previous phase |
:::

Avenor snapshots `git rev-parse HEAD` after each phase and uses that as the reference point for the next phase. The reference moves forward — it reflects what the immediately preceding phase left behind, not the start of the loop.

Delta variables are informational. Avenor does not enforce scoping; that belongs in your prompt:

::: v-pre
```json
{
  "name": "review",
  "prompt": "Review the branch for issues.\n\n{{if .ChangedFiles}}Since the last iteration the following files changed:\n{{.ChangedFiles}}\nReview these changes carefully, and also check whether they introduce knock-on effects elsewhere.{{else}}This is the first review pass. Cover the entire branch.{{end}}"
}
```
:::

## resume_from_previous

By default each phase starts fresh — the prompt is the sole input, and context flows through files that one agent writes and the next reads.

Setting `resume_from_previous: true` opts a phase into resuming the immediately preceding phase's session:

```json
{
  "name": "verify",
  "prompt": "Verify the fix. Check for regressions.",
  "resume_from_previous": true
}
```

The phase agent starts with full visibility into the previous phase's message history — its reasoning, tool calls, and output — without needing to reconstruct context from files. This is most useful for tightly coupled adjacent phases where file-based handoff would lose reasoning detail.

Accumulation is bounded: phase N resumes phase N-1; if phase N+1 also sets the flag it resumes phase N (which already incorporated N-1). The context window grows one phase at a time, and you control which phases participate.

### When not to use it

- **Self-contained phases** (build, test, fix) rarely need it. The phase name and prompt are enough context.
- **First loop phase** — when set on index 0 of the loop array, the flag is silently ignored (no preceding loop phase to resume from).
- **Pre phases** — when set on a pre phase, the flag is silently ignored (pre phases always start fresh).
- **First iteration** — on iteration 1, only the first loop phase is affected; subsequent phases can resume their predecessor within that iteration.

Use this when reasoning detail from the previous phase is load-bearing — e.g., a `review → verify` pair where the verify agent benefits from understanding *why* each finding was made, not just the list.

## Lifecycle events

Avenor emits four synthetic events during a loop run. All carry `run_id`, `ts`, and phase-related fields. Subscribe with `--on-event` to receive them as NDJSON.

### avenor.loop.start

Emitted once before the first pre-phase (or the first loop phase if there are no pre phases).

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

`kind` is `"pre"` or `"loop"`. `iteration` is 0 for pre-phases, 1-indexed for loop phases.

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
| `exit_marker: true` | `<|workflow: exit|>` was seen |
| `exit_marker_label` | exit marker had a label |
| `abort_marker: true` | `<|workflow: abort | reason|>` was seen |
| `abort_marker_label` | abort marker had a label |

If both markers appear in the same phase, only the abort fields are set (abort takes priority).

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
| `marker` | A phase emitted `<|workflow: exit|>` |
| `abort` | A phase emitted `<|workflow: abort | reason|>` |
| `max_iterations` | Loop reached `max_iterations` with no exit marker |
| `phase_failure` | A phase exited with a non-clean stop reason |
| `cancelled` | Run was cancelled (SIGINT) |
| `timeout` | `--timeout` was reached |
| `end_turn` | Pre phases completed but no loop phases were defined |

`exit_label` carries the label from the winning marker when `exit_reason` is `marker` or `abort`; absent otherwise.

## Sentinel outcomes

A single sentinel is written after the entire loop finishes. The exit code reflects the loop's overall outcome:

| Outcome | Status | Exit code |
|---|---|---|
| Clean exit (marker or max_iterations) | `DONE` | `0` |
| Abort marker | `BLOCKED` | `5` |
| Phase non-clean stop | `FAILED` | Phase exit code |
| Timeout | `TIMEOUT` | `124` |
| Cancellation | `KILLED` | `130` |

For the `BLOCKED` sentinel (exit code 5), a `REASON=` line is included when the abort marker carried a label:

```
BLOCKED
SESSION=ses_abc123
STOP_REASON=blocked
REASON=architectural issue: layering violation in pkg/db
RUN=a3f9...
```

If you need per-phase sentinels for external monitoring, subscribe to `avenor.phase.end` events via `--on-event` and write your own. Avenor does not provide per-phase sentinels.

## CLI invocation

```bash
avenor run --loop-file <path> [other flags...]
```

### What's required

`--loop-file` is the path to your loop config JSON. When set, `--prompt` and `--prompt-file` become optional. If you provide one, it runs as an implicit pre-phase named `(initial)` before any config phases.

### Mutual exclusions

- `--loop-file` and `--resume` are mutually exclusive. The loop runner manages session lifecycle internally; it rejects external resume requests.

### Shared flags

All other `avenor run` flags apply uniformly across every phase: `--agent`, `--dir`, `--model`, `--timeout`, `--max-retries`, `--auto-approve`, `--permission-handler`, `--sentinel-file`, `--on-event`, `--run-id`, `--control-socket`.

Retries (`--max-retries`) apply per phase: if a phase exits with code 1 (transient failure), Avenor retries that phase with exponential backoff (2 to 30 seconds) before advancing or failing the loop.

## Stable spawn

When using `avenor stable`, spawn a loop run by setting `loop_file` in the spawn parameters:

```json
{
  "prompt": "Initial instructions",
  "loop_file": "loop.json",
  "dir": "/repo/A",
  "label": "phase-loop-example"
}
```

The supervisor detects the `loop_file` field and routes the spawn through the loop runner.

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

Loop phases each get their own session; those session IDs appear in the event stream (`avenor.phase.start`, `avenor.phase.end` events) rather than in the spawn result. Other stable operations (cancel, prompt, list) work on the runtime as a whole — you cancel the entire loop run, not an individual phase.

## Config validation

On load, before any phase runs, Avenor validates:

- At least one of `pre` or `loop` must be non-empty
- `max_iterations` must be ≥ 1 when `loop` is non-empty
- Every phase must have a non-empty `name` and exactly one of `prompt` or `prompt_file`
- Phase names must be unique across `pre` and `loop`
- The name `(initial)` is reserved and cannot be used explicitly
- `--prompt` / `--prompt-file` with `--loop-file` inserts an unnamed pre-phase at index 0 (emitted in events as `phase: "(initial)"`)

If validation fails, Avenor logs the error to stderr, writes a `FAILED` sentinel (if `--sentinel-file` is set), and exits with code 1.

## Out of scope

Loop is intentionally single-level and serial:

- **Parallel phases** — serial execution avoids coordination problems (shared file writes, event ordering) with unclear benefit for the primary use case
- **Conditional phase skipping** — phases always run in order; skip logic belongs in the phase prompt
- **Per-phase `--max-retries`** — the existing retry flag applies to each phase individually
- **Cross-session context injection** — agent-managed files are the handoff mechanism
- **Loop nesting** — one level only
- **Non-JSON config formats** — no YAML, no extra dependencies
