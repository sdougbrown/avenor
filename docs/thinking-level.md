# Thinking level

Avenor exposes one optional `thinking` setting for backend-native reasoning controls. Use it when a run needs a specific reasoning effort and the selected backend can apply that setting without changing the prompt.

```sh
avenor --backend pi --thinking high --prompt "Review this change"
avenor --backend codex-app-server --thinking xhigh --prompt "Implement the plan"
avenor --backend claude --thinking medium --prompt "Summarize the design"
```

The control is also available in stable spawn JSON, `avenor control spawn`, the Go and TypeScript MCP tools, and the core, Pi, and OpenCode TypeScript spawn tools.

## Values

`thinking` accepts these exact lowercase values:

- `off`
- `minimal`
- `low`
- `medium`
- `high`
- `xhigh`
- `max`

Omit the setting to use the backend default. Avenor passes the same explicit spawn-level value to retries, loop and team phases, queued prompts, and follow-up runs. Avenor does not normalize case, clamp values, or add thinking instructions to prompts. An invalid value returns an error that lists the accepted values.

## Backend support

The native mechanisms below were verified with Codex CLI 0.144.6, Pi 0.83.0, and Claude Code 2.1.220. A required feature check that does not match returns the same unsupported-parameter error instead of guessing.

| Backend | New session | Explicit resume | Native control |
|---|---|---|---|
| `codex-app-server` | all seven values | all seven values | `turn/start.effort` |
| `pi` | all seven values | all seven values | fresh client: `--thinking`; reused client: `set_thinking_level` |
| `claude` | `low`, `medium`, `high`, `xhigh`, `max` | unsupported | `--effort` at startup |
| `opencode-acp` | unsupported | unsupported | none verified |
| `opencode-http` | unsupported | unsupported | none verified |
| `gemini-acp` | unsupported | unsupported | none verified |
| `cursor-acp` | unsupported | unsupported | none verified |
| `dsh` | unsupported | unsupported | none verified |
| `fx` | unsupported | unsupported | none verified |
| `agy` | `low`, `medium`, `high`, `xhigh` | `low`, `medium`, `high`, `xhigh` | ACP `session/set_config_option` (`thought_level`) |
| `pony` | unsupported | unsupported | none |
| `poo` | unsupported | unsupported | none: the binary's settings choose the model |

Every explicit value on an unsupported path fails with an error such as:

```text
backend "gemini-acp" does not support parameter "thinking"
```

A backend that supports thinking in general but not one canonical value rejects that value on its own:

```text
backend "agy" does not support thinking value "off" (allowed: low, medium, high, xhigh)
```

The rejection occurs before Avenor starts a provider, reserves a stable runtime, creates session artifacts, or sends a prompt. The MCP and control clients validate the enum for early feedback. The stable supervisor and direct CLI apply the authoritative backend policy.

A run in direct mode that omits `backend` is judged against the runtime default, `opencode-acp`, which supports no thinking level. Name a backend that supports the level, such as `pi` or `codex-app-server`. The rejection names the omission as well as the default, so it is not mistaken for a backend the caller chose. A roster entry, a workflow phase's own roster entry, or a session the stable supervisor can restore supplies the backend instead, and those report its own verdict. A CLI resume validates the `--backend` it was given, because the session's identity lives in the process that started it.

## Native behavior

### Codex app-server

Avenor stores an explicit effort as thread-scoped outbound state and sends the exact value on the first and each later `turn/start` request. It does not add `reasoning_effort` to `thread/start`, query `model/list` as a gate, require a model, or clamp the value.

The app-server validates effort against the active thread model on the actual turn. If Codex rejects it, Avenor adds the backend and requested value while retaining the app-server error text.

An explicit resume replaces Avenor's outbound override. A resume with omitted thinking clears only that override and omits `effort`; it does not reset the effort retained by Codex.

### Pi

For each fresh explicit start or resume, Avenor first checks that `pi --help` advertises `--thinking <level>`. It then starts Pi with `--thinking <level>`. It does not send a setter to a fresh client.

For a reused client, Avenor sends `set_thinking_level` on that client and requires a successful response before state lookup, session registration, or the next prompt. Avenor does not start an extra Pi process to probe the setter. Omitted thinking sends neither the flag nor the setter.

Pi owns model-specific mapping and clamping. The seven Avenor controls do not promise identical effective behavior for every Pi model.

### Claude

Claude checks that `claude --help` advertises `--effort <level>` before an explicit start. It passes `--effort` for `low`, `medium`, `high`, `xhigh`, and `max`. Claude has no native startup value for `off` or `minimal`, so Avenor rejects those values.

The effort flag is startup-only. Claude rejects every explicit resume value, even if the live in-process session appears to use the same value. Omitted thinking keeps the existing in-memory resume behavior.

### Agy

`agy` applies thinking per session over ACP `session/set_config_option` with `configId: "thought_level"`, on a fresh start and on an explicit resume. The level is set after the model and only when the caller supplied one: a resume without a level sends no `thought_level` request, even when the provider was constructed with a default. A model slug is passed to `agy acp` unchanged, so an effort-suffixed slug and an explicit level are independent choices.

The static policy admits `low`, `medium`, `high`, and `xhigh` before Avenor starts a provider. `agy acp` validates the level against the selected model when it applies it, after the session is open. If that model does not offer the requested level, Avenor reports the backend, the requested value, and the `agy acp` rejection instead of presenting the static allowed-values error. The failed launch sends no prompt: a fresh start or cold resume closes the session it opened, while a warm resume leaves its owned session and event poller in place. Avenor does not roll back a model change `agy acp` already accepted.

## Unsupported backends

OpenCode ACP 1.18.3 has no verified thinking config option. OpenCode HTTP SDK 1.15.3 exposes model variants, but a variant name is not a canonical thinking level. Avenor does not infer a mapping or add an unverified payload field.

Gemini ACP and Cursor ACP have no verified session control for this setting. Pony's model request has no thinking field. A poo binary takes its model and its options from its own settings file, and its wire protocol has no reasoning field, so the backend rejects an explicit level rather than ignoring it. Agy no longer appears here: it uses ACP (`agy acp`) and applies `thought_level`; see its section above for values and model-specific validation.

None of these backends receives a prompt prefix, system instruction, warning-only fallback, or silent no-op.
