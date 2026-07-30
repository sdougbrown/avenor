---
name: avenor-orchestrate
description: Select an available backend, then delegate and supervise independent agent runs through the Avenor MCP tools. Use when the user explicitly asks to use Avenor, dispatch work to jockey or another Avenor agent, run nested agent delegation, inspect an Avenor run, answer a run's permission request, or continue a completed Avenor session.
---

# Orchestrate with Avenor

Use the Avenor MCP tools for the full run lifecycle. Do not replace them with shell commands while the tools are available.

## Start a run

1. Confirm that delegation is explicit in the request. Do not dispatch ordinary tasks merely because they could be delegated.
2. Resolve `repo_dir` to the absolute path of the repository the user placed in scope.
3. Select the backend using the workflow below.
4. Preserve any requested agent, model, label, and timeout. Omit optional overrides rather than guessing them.
5. Call `avenor_spawn` with a complete, self-contained prompt and the selected `backend`. Retain the returned `run_id`, `label`, and `supervisor_id`.

## Select a backend

1. Run `python3 <skill-dir>/scripts/detect_backends.py --json`, resolving `<skill-dir>` to this skill's directory. Pass `--server-url` for an OpenCode HTTP endpoint and `--prefer <backend>` only when the user explicitly requested or previously stated a backend preference.
2. Treat an explicitly requested backend as authoritative. If detection says it is unavailable, explain the missing executable or endpoint and ask before substituting another backend.
3. Without an explicit preference, use `recommended` only when `selection_reason` is `single_candidate`. When multiple candidates are detected, ask the user to choose; backend choice affects runtime configuration, capabilities, billing, and identity. Do not encode a personal preference as a public default.
4. Treat configuration results as a preflight heuristic, not an authentication guarantee. If startup fails, report the error. Try another backend only when no task work began and doing so cannot duplicate side effects.
5. Always pass the chosen backend explicitly so Avenor does not fall back to its `opencode-acp` default. For `opencode-http`, also pass the same `server_url` used during detection.

## Select an agent and model

- Never invent a default `agent` or `model`.
- For Pi, pass an explicitly requested named agent only when `pi_agents_extension` and `named_agents_ready` are true and the name appears in `agent_profiles`. A profile file without the extension is not usable.
- For OpenCode ACP, pass an explicitly requested agent only when it appears in `agent_profiles`.
- Omit `agent` for other backends; Avenor does not configure named profiles for them.
- Pass an explicit model only when `model_selection_supported` is true. `configured_models` is intentionally unknown because provider catalogs, aliases, authentication, and account access are runtime-owned. Explain that the model is validated at startup.
- When the user specifies neither agent nor model, omit both and let the selected runtime use its own configured defaults.

## Supervise the run

A run can settle into one of several states while you wait. Your monitoring
must distinguish them because each requires a different action.

### Run lifecycle states

| Status | Means | Next action |
|---|---|---|
| `running` | Agent is actively working (reading, thinking, running tools). | Keep waiting. Do not treat unchanged `running` as a failure. |
| `done` (with output) | Agent finished its turn and produced a result (`final_output` present, files changed). | Report the outcome. Verified changed files against the workspace. |
| `done` (no output) | Agent ended its turn without writing anything. It is likely asking a clarifying question or needs direction. | Inspect via `avenor_events` or `avenor_inspect`. If it asked a question, call `avenor_follow_up` with your answer. |
| `failed` | Agent encountered an error mid-run. | Report the failure and any partial artifacts. |
| `timeout` | Run exceeded its configured timeout. | Report the timeout and any partial artifacts. |
| `killed` | Run was forcefully terminated. | Report the kill. |
| `waiting` (`pending_permission`) | Agent hit a tool approval gate. It is blocked mid-task, not finished. | Inspect the offered options. Call `avenor_answer_permission` with an offered `option_id`, then resume monitoring. |

### Two supervision patterns

**Blocking pattern** — call `avenor_result` and let it handle the lifecycle:

- `avenor_result` waits until the run reaches a terminal state (`done`, `failed`,
  `timeout`, `killed`) or a waiting state (`pending_permission`). It handles the
  polling internally and returns the first meaningful state change.
- This is the simplest pattern: one call, no loop. Use this when you can afford
  to wait rather than parallelize other work.
- After `pending_permission`, answer it with `avenor_answer_permission` and call
  `avenor_result` again.

**Polling pattern** — call `avenor_status` periodically and dispatch on the
returned state:

```
loop:
  status = avenor_status(run_id, view="lifecycle")

  if status.status == "running":
    wait a few seconds, repeat the loop

  if status.status == "waiting" and status.pending_permission:
    answer permission, then repeat the loop

  if status.status == "done":
    check for final_output or changed files
    if output is a question or empty:
      call avenor_follow_up with your answer
    else:
      report results, exit loop

  if status.status in ("failed", "timeout", "killed"):
    report failure, exit loop
```

### Common monitoring pitfalls

- **Watching only for file changes.** An agent that finishes its turn without
  writing anything has still finished its turn. Do not treat an empty working
  tree as "still running." Check `avenor_status` to see if the run is done and
  inspect its output.
- **Watching only for `session.end`.** A permission-blocked run never reaches
  `session.end`. Use the polling pattern above, which checks both terminal
  and waiting states.
- **Confusing "done but empty" with "still running."** A file-watch cannot
  distinguish them. One needs a follow-up; the other needs patience.

### When to inspect or follow up

- Call `avenor_inspect` after a `done` run to see a bounded transcript, tool
  calls, and final output without reading raw events.
- Call `avenor_events` only when you need the raw event log, such as
  permission history or detailed timing.
- Call `avenor_follow_up` when a `done` run needs more direction. It spawns a
  new session continuing from the prior one. Supervise the follow-up run
  through the same lifecycle: watch for permission gates, terminal states,
  and clarifying questions.

## Clean up

Call `avenor_shutdown` only when the user asks to stop Avenor or when all relevant runs are terminal and no follow-up is expected. Do not shut down a supervisor that may own unrelated active runs.
