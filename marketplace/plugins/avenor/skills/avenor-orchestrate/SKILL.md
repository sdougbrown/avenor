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

1. Call `avenor_result` with the run identifier to wait for the bounded final output. A blocked run returns its `pending_permission` instead of waiting indefinitely.
2. Use `avenor_status` with `view: "lifecycle"` only when you need a non-blocking progress check. Do not poll status to retrieve output.
3. Keep the user informed during long runs. Do not treat an unchanged `running` result as a failure.
4. For `pending_permission`, inspect the offered options. Apply the same authorization and safety boundaries as the parent task. Call `avenor_answer_permission` only with an offered `option_id`; ask the user when the choice needs new authority or materially changes scope. Then call `avenor_result` again.
5. Call `avenor_events` only when relevant raw history is needed. Report the outcome, important findings, and changed files, and distinguish the worker's claims from changes verified in the local workspace.

## Continue a run

Call `avenor_follow_up` only for a completed run. Pass the prior run identifier and a self-contained follow-up message, then supervise the returned run through the same lifecycle.

The Avenor MCP registry is scoped to the current MCP server process. Events and follow-ups may be unavailable after the plugin, MCP server, or Codex task restarts.

## Clean up

Call `avenor_shutdown` only when the user asks to stop Avenor or when all relevant runs are terminal and no follow-up is expected. Do not shut down a supervisor that may own unrelated active runs.
