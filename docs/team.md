# Avenor: Team Run

You need a team run when the work fans out — multiple agents that can run independently, then a single phase to collect and reconcile their output. Not a loop, not a single agent grinding through a list: parallel agents, each owning their own task, all racing to completion.

## The problem team runs solve

A loop is serial by design: test, fix, repeat. That's the right shape for iterative refinement. But some work is naturally parallel — five agents reviewing five components, three specialists investigating three failure modes, a handful of translators working on a handful of locales. Running those serially wastes time without any benefit. The coordination problem isn't sequencing; it's fan-out and fan-in.

Without a team run, you'd either wire this up externally — launching subprocesses, polling sentinel files, stitching output back together — or you'd hand-wave it to a single agent and trust it not to serialize everything anyway. Neither is satisfying. Avenor already holds the run ID, event log, and sentinel machinery. Team run uses those to own the fan-out for you.

## Quick start

Create a team config file:

```json
{
  "pre": [
    {
      "name": "plan",
      "prompt": "Read the codebase and identify the three areas most in need of test coverage. Write a brief summary to plan.md."
    }
  ],
  "team": [
    {
      "name": "auth-tests",
      "prompt": "Write tests for the auth package. Focus on edge cases."
    },
    {
      "name": "api-tests",
      "prompt": "Write tests for the API layer. Aim for at least 80% coverage."
    },
    {
      "name": "db-tests",
      "prompt": "Write tests for the database layer. Ensure all error paths are covered."
    }
  ],
  "post": [
    {
      "name": "review",
      "prompt": "Review the tests written by all three agents. Check for gaps and inconsistencies. Write a summary to review.md."
    }
  ]
}
```

Run it:

```bash
avenor run --team-file team.json --auto-approve --sentinel-file run.done
```

Avenor runs `plan` once, then launches `auth-tests`, `api-tests`, and `db-tests` in parallel. When all three complete, it runs `review`. A single sentinel is written after the whole run finishes.

`--prompt` and `--prompt-file` are optional when `--team-file` is set. If you provide one, it runs as an implicit pre-phase (named `(initial)`) before any config phases.

## How a team run works

**Pre phases** run once, in sequence, before the team starts. If any pre phase exits with a stop reason other than `end_turn`, the entire run fails immediately — the team never starts.

**Team phases** run in parallel. All team members start at the same time; each runs in its own session. The team phase ends when all members have finished — or when any member aborts (see Abort and escalation below).

**Post phases** run once, in sequence, after all team members complete successfully. Post phases are skipped if the team phase ends in an abort or failure.

If the team member list resolves to zero members after conditional filtering, Avenor skips directly to post phases and exits cleanly.

Phases run to the natural end of their session before Avenor acts on a marker. A member can emit `[team: skip | name]` during the pre phase to remove another member from the team; it cannot interrupt a member that has already started.

## Team config file

A JSON file with three top-level keys:

| Key | Type | Required | Description |
|---|---|---|---|
| `pre` | `Phase[]` | At least one of `pre` or `team` must be non-empty | Phases that run once, in order, before the parallel team |
| `team` | `Phase[]` | At least one of `pre` or `team` must be non-empty | Phases that run in parallel as team members |
| `post` | `Phase[]` | No | Phases that run once, in order, after all team members complete |

### Phase fields

Each phase object:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | Yes | Unique within the config. Reserved: `(initial)` |
| `prompt` | `string` | One of `prompt`, `prompt_file`, `loop_file`, or `team_file` | Inline prompt text sent to the agent. Supports template variables |
| `prompt_file` | `string` | One of `prompt`, `prompt_file`, `loop_file`, or `team_file` | Path to a file containing the prompt. Relative paths resolve from the config file's directory. Supports template variables |
| `loop_file` | `string` | One of `prompt`, `prompt_file`, `loop_file`, or `team_file` | Path to a loop config JSON. This phase dispatches a nested loop run instead of a direct agent session |
| `team_file` | `string` | One of `prompt`, `prompt_file`, `loop_file`, or `team_file` | Path to a team config JSON. This phase dispatches a nested team run instead of a direct agent session |
| `resume_from_previous` | `boolean` | No — defaults to `false` | Resume the immediately preceding phase's session instead of starting fresh. Only meaningful in pre and post phases |
| `conditional` | `boolean` | No — defaults to `false` | Whether the pre phase can instruct Avenor to skip this team member |
| `agent` | `string` | No | Agent override for this team member. Ignored for pre and post phases |
| `model` | `string` | No | Model override for this team member. Ignored for pre and post phases |

Phase names must be unique across `pre`, `team`, and `post`. The name `(initial)` is reserved for the implicit pre-phase created when you pass `--prompt` or `--prompt-file` alongside `--team-file`.

`loop_file` and `team_file` are mutually exclusive. `prompt` and `prompt_file` are mutually exclusive. A phase that sets `loop_file` or `team_file` cannot also set `prompt` or `prompt_file`.

## Conditional members

Sometimes you don't know until runtime whether a team member should run. Mark a team phase `conditional: true`, and the pre phase can opt it out before the parallel work starts.

```json
{
  "pre": [
    {
      "name": "triage",
      "prompt": "Check which components have changed since the last release. For any component that has NOT changed, skip its review."
    }
  ],
  "team": [
    {
      "name": "auth-review",
      "prompt": "Review the auth component.",
      "conditional": true
    },
    {
      "name": "api-review",
      "prompt": "Review the API component.",
      "conditional": true
    },
    {
      "name": "summary",
      "prompt": "Summarize all review findings."
    }
  ]
}
```

To skip a conditional member, the pre phase emits a skip marker:

```
[team: skip | auth-review]
```

### Skip marker format

```
[team: skip | <name>]          skip a conditional team member by name
[team: skip | auth-review]     case-insensitive match against the member's name
```

- Case-insensitive matching against the member's `name` field
- The line must match the whole-line pattern — markers embedded in prose are ignored
- Only members with `conditional: true` can be skipped; non-conditional members always run
- If the named member does not exist or is not conditional, the marker is silently ignored

### The pre phase requirement

Conditional members require at least one pre phase. Without a pre phase, there is no agent to evaluate conditions — Avenor rejects the config at load time.

Avenor automatically appends instructions to the last pre phase's prompt, listing the conditional members and explaining the skip marker format. You don't need to include this in your own prompt; it's injected before the pre phase runs.

## Abort and escalation

A team member emits `[loop: abort | reason]` when it hits a wall — a missing dependency, a decision requiring human judgement, or a constraint it cannot satisfy alone. The marker uses the `loop:` prefix even in a team run; there is no `[team: abort]`.

When any member aborts:

1. **Avenor cancels all remaining members.** Members already in flight are interrupted via context cancellation; members that haven't started don't start. This is a best-effort cancellation — a member that has already written significant output may finish naturally before the signal arrives.

2. **First-abort-wins.** When multiple members abort concurrently, the first one to register its abort label wins. The others are counted in the `members_aborted` field of `avenor.team.end`, but their labels are not preserved in the sentinel.

3. **Post phases do not run** after an abort. The run terminates with a `BLOCKED` sentinel.

4. **A `BLOCKED` sentinel is written** with exit code `5` and stop reason `"blocked"`:

   ```
   BLOCKED
   SESSION=ses_abc123
   STOP_REASON=blocked
   REASON=auth service unavailable: cannot proceed with integration tests
   RUN=a3f9...
   ```

   The `REASON=` line is omitted when the abort marker has no label.

### Using abort for orchestration

A jockey watching the event stream via `--on-event` sees `avenor.team.end` with `exit_reason: "abort"` and `exit_label` set to the first member's abort label. The jockey can surface the reason to a human, reassign the work, or re-invoke with an amended configuration. No new Avenor mechanism is needed.

## Prompt templates

Phase prompts support Go `text/template` syntax. Variables reflect the current state at the moment each phase begins.

### Available variables

::: v-pre
| Variable | Value |
|---|---|
| `{{.RunID}}` | The run's correlation ID |
| `{{.Phase}}` | Current phase name |
| `{{.WorkDir}}` | Working directory |
:::

`Iteration` and `MaxIterations` do not exist in team runs. There is no loop; phases run exactly once.

Example:

::: v-pre
```json
{
  "name": "api-review",
  "prompt": "Review the API layer. Run ID: {{.RunID}}"
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

Avenor snapshots `git rev-parse HEAD` after each phase and uses that as the reference point for the next phase. For team members running in parallel, all members receive the same `PrevPhaseCommit` — the commit at the end of the last pre phase. Post phases receive the commit at the end of the team phase (whichever commit was HEAD when the last member finished).

Delta variables are informational. Avenor does not enforce scoping; that belongs in your prompt.

## resume_from_previous

By default each phase starts fresh — the prompt is the sole input, and context flows through files that one phase writes and the next reads.

Setting `resume_from_previous: true` opts a phase into resuming the immediately preceding phase's session:

```json
{
  "name": "finalize",
  "prompt": "Review the draft output and finalize it for delivery.",
  "resume_from_previous": true
}
```

The phase agent starts with full visibility into the previous phase's message history — its reasoning, tool calls, and output — without needing to reconstruct context from files.

### Constraints

`resume_from_previous` is only meaningful in pre and post phases. Team members always start fresh — each runs in its own session with no predecessor to resume from. Setting this flag on a team phase is silently ignored.

For pre and post phases, the flag works the same way it does in loop phases: phase N resumes phase N-1's session. The first pre phase (or the first post phase) cannot resume anything; the flag is silently ignored there too.

Use this when reasoning detail from the previous phase is load-bearing — e.g., a `plan → execute` pre-phase pair where `execute` benefits from understanding the full reasoning behind the plan.

## Lifecycle events

Avenor emits synthetic events during a team run. All carry `run_id`, `ts`, and relevant fields. Subscribe with `--on-event` to receive them as NDJSON.

### avenor.team.start

Emitted once before the first pre phase (or before the team phase if there are no pre phases).

```json
{
  "event": "avenor.team.start",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "pre_phase_count": 1,
  "team_member_count": 3,
  "post_phase_count": 1
}
```

### avenor.phase.start

Emitted immediately before each phase's session begins. For team members, this fires in parallel — you may see multiple `avenor.phase.start` events with `kind: "team"` at nearly the same timestamp.

```json
{
  "event": "avenor.phase.start",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "phase": "auth-tests",
  "iteration": 0,
  "kind": "team"
}
```

`kind` is `"pre"`, `"team"`, or `"post"`. `iteration` is always `0` in team runs.

### avenor.phase.end

Emitted after each phase's session ends (before any retry).

```json
{
  "event": "avenor.phase.end",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "phase": "auth-tests",
  "iteration": 0,
  "stop_reason": "end_turn"
}
```

Abort marker fields appear when a member emitted an abort:

| Field | Present when |
|---|---|
| `abort_marker: true` | `[loop: abort]` was seen during this phase |
| `abort_marker_label` | abort marker had a label |

### avenor.team.end

Emitted once after the team run finishes, regardless of how it ended.

```json
{
  "event": "avenor.team.end",
  "run_id": "a3f9...",
  "ts": 1715000000000,
  "exit_reason": "end_turn",
  "members_completed": 3,
  "members_aborted": 0
}
```

`exit_reason` is one of:

| Value | Meaning |
|---|---|
| `end_turn` | All members completed successfully and post phases (if any) finished cleanly |
| `abort` | At least one member emitted `[loop: abort]` |
| `phase_failure` | A phase exited with a non-clean stop reason |
| `pre_failure` | A pre phase exited with a non-clean stop reason |
| `post_failure` | A post phase exited with a non-clean stop reason |
| `cancelled` | Run was cancelled (SIGINT) |
| `timeout` | `--timeout` was reached |

`exit_label` carries the label from the first abort marker when `exit_reason` is `"abort"`; absent otherwise.

`members_completed` and `members_aborted` count how many team members finished (completed) and how many were counted as aborted. Pre and post phase counts are not reflected here.

## Sentinel outcomes

A single sentinel is written after the entire team run finishes:

| Outcome | Status | Exit code |
|---|---|---|
| All members clean, post phases clean | `DONE` | `0` |
| Any member aborted | `BLOCKED` | `5` |
| Phase non-clean stop | `FAILED` | Phase exit code |
| Timeout | `TIMEOUT` | `124` |
| Cancellation | `KILLED` | `130` |

For the `BLOCKED` sentinel (exit code 5), a `REASON=` line is included when the first abort marker carried a label:

```
BLOCKED
SESSION=ses_abc123
STOP_REASON=blocked
REASON=auth service unavailable: cannot proceed with integration tests
RUN=a3f9...
```

If you need per-member sentinels for external monitoring, subscribe to `avenor.phase.end` events via `--on-event` and write your own. Avenor does not provide per-phase sentinels.

## CLI invocation

```bash
avenor run --team-file <path> [other flags...]
```

### What's required

`--team-file` is the path to your team config JSON. When set, `--prompt` and `--prompt-file` become optional. If you provide one, it runs as an implicit pre-phase named `(initial)` before any config phases.

### Mutual exclusions

- `--team-file` and `--loop-file` are mutually exclusive.
- `--team-file` and `--resume` are mutually exclusive. The team runner manages session lifecycle internally; it rejects external resume requests.

### Shared flags

All other `avenor run` flags apply uniformly across every phase: `--agent`, `--dir`, `--model`, `--timeout`, `--max-retries`, `--auto-approve`, `--permission-handler`, `--sentinel-file`, `--on-event`, `--run-id`, `--control-socket`.

`--agent` and `--model` set defaults for the run. Individual team members can override both via their `agent` and `model` fields in the config. Pre and post phases always use the run-level `--agent` and `--model`; per-phase `agent` and `model` fields are ignored outside the `team` array.

Retries (`--max-retries`) apply per phase: if a phase exits with code 1 (transient failure), Avenor retries that phase with exponential backoff (2 to 30 seconds) before declaring failure.

## Stable spawn

When using `avenor stable`, spawn a team run by setting `team_file` in the spawn parameters:

```json
{
  "prompt": "Optional initial instructions",
  "team_file": "team.json",
  "dir": "/repo/A",
  "label": "review-team"
}
```

The supervisor detects the `team_file` field and routes the spawn through the team runner.

### What's different from a normal spawn

The `SpawnResult` returned by a team spawn has an empty `SessionID`:

```json
{
  "runtime_id": "rt_1",
  "session_id": "",
  "on_event": "/tmp/avenor-stable/.../events.ndjson",
  "sentinel_file": "/tmp/avenor-stable/.../sentinel.env"
}
```

Team members each get their own session; those session IDs appear in the event stream (`avenor.phase.start`, `avenor.phase.end` events) rather than in the spawn result. Other stable operations (cancel, prompt, list) work on the runtime as a whole — you cancel the entire team run, not an individual member.

## Config validation

On load, before any phase runs, Avenor validates:

- At least one of `pre` or `team` must be non-empty
- Conditional team members require at least one pre phase
- Every phase must have a non-empty `name`
- Every phase must have exactly one of `prompt`, `prompt_file`, `loop_file`, or `team_file`
- `prompt` and `prompt_file` are mutually exclusive
- `prompt` (or `prompt_file`) and `loop_file` (or `team_file`) are mutually exclusive
- `loop_file` and `team_file` are mutually exclusive within a single phase
- Phase names must be unique across `pre`, `team`, and `post`
- The name `(initial)` is reserved and cannot appear explicitly

If validation fails, Avenor logs the error to stderr, writes a `FAILED` sentinel (if `--sentinel-file` is set), and exits with code 1.

## Out of scope

Team run is intentionally flat and parallel:

- **Sequential team members** — use `pre` and `post` for work that must serialize; the `team` array runs everything at once
- **Per-member iteration** — a team member that needs looping should set `loop_file` and dispatch to a nested loop run
- **Member-to-member communication** — members do not share sessions or message history; file-based handoff is the mechanism
- **Dynamic member counts** — the team list is fixed at config load time; conditional filtering can shrink it, but new members cannot be added at runtime
- **resume_from_previous on team phases** — team members always start fresh
- **Non-JSON config formats** — no YAML, no extra dependencies
