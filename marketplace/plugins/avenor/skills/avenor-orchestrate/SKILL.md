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

1. Run `python3 <skill-dir>/scripts/detect_backends.py --json`, resolving `<skill-dir>` to this skill's directory. Pass `--server-url` when the user supplied an OpenCode HTTP endpoint.
2. Treat an explicitly requested backend as authoritative. If detection says it is unavailable, explain the missing executable or endpoint and ask before substituting another backend.
3. Without an explicit choice, use the script's recommendation. It prefers Pi, then a configured OpenCode ACP runtime, then Codex app-server, followed by the other detected runtimes. Always pass the chosen backend explicitly so Avenor does not fall back to its `opencode-acp` default.
4. Treat configuration results as a preflight heuristic, not an authentication guarantee. If startup fails, report the error. Try another backend only when no task work began and doing so cannot duplicate side effects.
5. For `opencode-http`, pass the same `server_url` used during detection.

Named agents are backend-specific. For Pi or OpenCode, pass an explicitly requested `agent` only when it appears in that backend's `agent_profiles`; otherwise explain that its profile is not detected. When no agent is requested, use `jockey` only when that profile is detected. Omit `agent` for other backends unless the user specifically supplied one and Avenor supports it there. Pi can run with a `model` and no named profile; its `pi_agents_extension` signal indicates whether a profile can apply the full model, prompt, tools, and permission configuration.

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
