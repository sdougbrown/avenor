---
name: avenor-orchestrate
description: Delegate and supervise independent agent runs through the Avenor MCP tools. Use when the user explicitly asks to use Avenor, dispatch work to jockey or another Avenor agent, run nested agent delegation, inspect an Avenor run, answer a run's permission request, or continue a completed Avenor session.
---

# Orchestrate with Avenor

Use the Avenor MCP tools for the full run lifecycle. Do not replace them with shell commands while the tools are available.

## Start a run

1. Confirm that delegation is explicit in the request. Do not dispatch ordinary tasks merely because they could be delegated.
2. Resolve `repo_dir` to the absolute path of the repository the user placed in scope.
3. Preserve any requested agent, backend, model, label, and timeout. Default `agent` to `jockey` when the user asks for Avenor delegation without naming an agent. Omit optional overrides rather than guessing them.
4. Call `avenor_spawn` with a complete, self-contained prompt and retain the returned `run_id`, `label`, and `supervisor_id`.

## Supervise the run

1. Call `avenor_status` with the run identifier until the run reaches `done`, `failed`, `timeout`, or `killed`, or reports `pending_permission`.
2. Keep the user informed during long runs. Do not treat an unchanged `running` result as a failure.
3. For `pending_permission`, inspect the offered options. Apply the same authorization and safety boundaries as the parent task. Call `avenor_answer_permission` only with an offered `option_id`; ask the user when the choice needs new authority or materially changes scope.
4. After a terminal result, call `avenor_events` for the relevant recent events and report the outcome, important findings, and changed files. Distinguish the worker's claims from changes verified in the local workspace.

## Continue a run

Call `avenor_follow_up` only for a completed run. Pass the prior run identifier and a self-contained follow-up message, then supervise the returned run through the same lifecycle.

The Avenor MCP registry is scoped to the current MCP server process. Events and follow-ups may be unavailable after the plugin, MCP server, or Codex task restarts.

## Clean up

Call `avenor_shutdown` only when the user asks to stop Avenor or when all relevant runs are terminal and no follow-up is expected. Do not shut down a supervisor that may own unrelated active runs.
