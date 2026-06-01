# Avenor Event Stream

Avenor writes newline-delimited JSON to `--on-event`. Each line is one event. Consumers should switch on the `event` field and read domain-specific fields directly from the same object — there is no nested wrapper.

The event structure is flat: all fields at the top level, including a required `event` string that names the event type.

```json
{"event":"tool.call","session_id":"ses_123","toolCallId":"call_1","status":"pending"}
```

## Why events matter

Without events, you're polling HTTP endpoints or reading a finished log file. With events, you know the *moment* something happens: when the agent transitions between thinking and working, when a permission gate is hit, when a critical tool runs. That timing is load-bearing for orchestration, reporting, and permission handling.

Avenor classifies each event as MILESTONE, FINDING, or ACTIVITY. LLMs that consume this stream use those tags to decide whether to wake up, log, or escalate.

## Event types

### Protocol events (passed from backend)

These come from the Claude API backend. Avenor parses them, validates required fields, and passes them downstream.

**`agent.message_chunk`** — Streamed response text visible to the agent. Contains `content` (object with `text` field), `messageId`, and optional `session_update` metadata.

**`agent.thought_chunk`** — Intermediate reasoning or progress text exposed by the model (when enabled in the backend configuration). Same structure as `agent.message_chunk`.

**`tool.call`** — A tool invocation began. Fields: `toolCallId` (unique identifier), `title` (human-readable name), `kind` (tool category like `bash`, `read`, `write`), `status` (`"pending"` at emission), `rawInput` (the serialized argument JSON), and optional `session_update`.

**`tool.call_update`** — Tool status changed. Emitted when a tool completes or transitions states. Fields: `toolCallId`, `kind`, `status` (`"completed"` or `"failed"`), and optional `session_update`.

**`user.message_chunk`** — User text reflected by the backend. Rarely emitted; documented for protocol completeness.

**`session.plan`** — Backend plan update (experimental). Rarely emitted; documented for protocol completeness.

### Permission events

**`permission.request`** — Backend is asking the client to choose a permission option. For example: "Allow bash command?" with options `["allow", "reject"]`.

Avenor populates `request_id` (unique for this session), `tool` (best-effort), `question` (best-effort), and `options` (array of choice objects) when available. If you're using `--permission-handler file:<path>`, Avenor writes the `.req` file and emits this event immediately after, without waiting for the response.

Classifies as MILESTONE because permission gates are decision points.

Example:

```json
{"event":"permission.request","session_id":"ses_xyz","request_id":"17","tool":"bash","question":"Run this command?","options":[{"optionId":"allow","kind":"allow"},{"optionId":"reject","kind":"reject"}]}
```

**`permission.response`** — Synthesized by Avenor after resolving a permission decision. Emitted for all resolution paths: `--auto-approve`, control socket, or file handler. Fields: `request_id` (string), `option_id` (the chosen `optionId` from the request's option list), `kind` (`"allow"` or `"reject"`), `source` (`"avenor"` for auto-approve, `"control"` for control socket, `"file"` for file handler), `ts` (Unix milliseconds).

Also includes `run_id` and `run_label` when present.

Classifies as ACTIVITY because the permission is already decided by the time this event fires.

Example:

```json
{"event":"permission.response","session_id":"ses_xyz","request_id":"17","option_id":"allow","kind":"allow","source":"file","ts":1234567890}
```

### Phase and iteration events (Avenor synthesized)

When running with `--config`, Avenor executes a loop: optional pre-phases followed by a configurable number of iterations through phases. Each phase is a separate Claude prompt invocation.

**`avenor.loop.start`** — Loop is starting. Emitted before the first pre-phase. Fields: `max_iterations` (from config), `pre_phase_count`, `loop_phase_count`, `ts`.

Classifies as MILESTONE.

Example:

```json
{"event":"avenor.loop.start","run_id":"run_1","max_iterations":5,"pre_phase_count":1,"loop_phase_count":2,"ts":1234567890}
```

**`avenor.phase.start`** — One phase is starting. Emitted before invoking the Claude backend for this phase. Fields: `phase` (phase name from config), `iteration` (0 for pre-phases, 1+ for loop iterations), `kind` (`"pre"` or `"loop"`), `ts`.

Classifies as ACTIVITY.

Example:

```json
{"event":"avenor.phase.start","run_id":"run_1","phase":"code_review","iteration":1,"kind":"loop","ts":1234567890}
```

**`avenor.phase.end`** — Phase completed (or failed). Emitted after the Claude invocation returns. Fields: `phase`, `iteration`, `stop_reason` (why the phase ended: `"end_turn"` for normal completion, `"max_tokens"`, `"tool_use"`, etc.), `ts`.

If the phase exit text contained a loop directive marker (e.g., `[abort]` or `[exit]`), additional fields capture it: `abort_marker` (boolean) with optional `abort_marker_label`, or `exit_marker` (boolean) with optional `exit_marker_label`.

Classifies as ACTIVITY.

Example:

```json
{"event":"avenor.phase.end","run_id":"run_1","phase":"code_review","iteration":1,"stop_reason":"end_turn","ts":1234567890}
```

**`avenor.loop.end`** — Loop finished (or stopped early). Emitted after the final phase completes or when a phase directive halts the loop. Fields: `exit_reason` (one of `"end_turn"`, `"abort"`, `"exit"`, `"max_iterations"`, `"phase_failure"`, `"timeout"`, `"cancelled"`), `iterations_completed` (count of fully-executed iterations), `ts`.

Also includes `exit_label` when a directive marker had a label.

Classifies as MILESTONE because loop completion is a significant boundary.

Example:

```json
{"event":"avenor.loop.end","run_id":"run_1","exit_reason":"end_turn","iterations_completed":3,"ts":1234567890}
```

### Retry and error events

**`avenor.retry`** — A phase is being retried (exit code 1, non-fatal failure). Emitted before each retry attempt after the first. Fields: `attempt` (which retry this is: 1 for the first retry, 2 for the second, etc.), `max_retries`, `ts`.

Classifies as MILESTONE.

Example:

```json
{"event":"avenor.retry","run_id":"run_1","attempt":1,"max_retries":3,"ts":1234567890}
```

**`avenor.error`** — Avenor encountered a runtime error (malformed request, permission handler timeout, backend unavailable, etc.). Fields: `source` (subsystem: `"permission"`, `"cancel"`, `"backend"`, `"loop"`, etc.), `message` (human-readable error text), `ts`.

Also includes `run_id` and `run_label` when present. Loop-level degenerate
reasoning aborts include additional fields: `kind` (`"degenerate_reasoning_stream"`),
`stop_reason` (the session-end stop reason that follows), `model` (the
provider profile that produced the bad stream), and `diagnostics` (an
object with `abort_signal`, `observed_total`, `hex_total`, `hex_in_window`,
`window_size`, `consecutive_hex`, and `progress_in_window`).

Classifies as MILESTONE because errors usually require operator intervention.

Example (permission timeout):

```json
{"event":"avenor.error","session_id":"ses_xyz","run_id":"run_1","source":"permission","message":"handler timed out after 10m","ts":1234567890}
```

Example (degenerate reasoning stream):

```json
{"event":"avenor.error","session_id":"ses_xyz","source":"loop","kind":"degenerate_reasoning_stream","stop_reason":"degenerate_reasoning_stream","model":"gpt-oss-120b","message":"degenerate reasoning stream: provider control token leakage (<|channel>)","diagnostics":{"abort_signal":"control_token","observed_total":1,"hex_total":0,"hex_in_window":0,"window_size":2048,"consecutive_hex":0,"progress_in_window":0}}
```

### Status and session events

**`agent.status`** — Synthesized by Avenor to signal agent phase transitions. Emitted before the protocol event that triggered it.

Fields: `phase` (one of `thinking`, `working`, `waiting`, `done`), `source` (`"avenor"` for synthesized transitions, `"agent"` for explicit markers in output text), optional `label` (human-readable activity description), `ts` (Unix milliseconds).

Also includes `run_id` and `run_label` when present.

The `phase` values mean:
- `thinking`: Agent is reasoning or planning (triggered by `agent.thought_chunk`).
- `working`: Agent is actively executing tools (triggered by `tool.call`).
- `waiting`: Agent is blocked on a permission decision (triggered by `permission.request`).
- `done`: Session ended (triggered by `session.end`).

Classifies as MILESTONE when phase is `done` or `waiting`; ACTIVITY otherwise.

Example:

```json
{"event":"agent.status","session_id":"ses_xyz","phase":"working","label":"reading files","source":"avenor","ts":1234567890}
```

**`session.end`** — Terminal record for the session. Always the last event in any run. Fields: `stop_reason`, optional `usage` (object with `input_tokens`, `output_tokens`, `total_tokens`, `cached_read_tokens` in snake_case).

Classifies as MILESTONE.

The `stop_reason` indicates *why* the session ended:

- `"end_turn"` — Normal completion. Backend finished responding.
- `"max_tokens"` — Hit the token limit during generation.
- `"stop_sequence"` — Backend stopped on a configured stop sequence.
- `"tool_use"` — Session ended while waiting for tool results (unexpected in normal flows).
- `"timeout"` — Avenor's client-side timeout fired. Usage is best-effort from the buffer at timeout.
- `"cancelled"` — Avenor or the operator cancelled the session. Usage may be incomplete.
- `"degenerate_reasoning_stream"` — The reasoning/thought stream became
  clearly non-productive (provider leakage of internal control tokens, or
  a long run of single-character hex deltas). Avenor aborted the session
  early to prevent the run from consuming the entire output budget. The
  matching `avenor.error` event contains the abort signal and diagnostics;
  the `model` and `session_id` fields on the error are enough to identify
  the provider profile that produced the bad stream.

Example (normal completion):

```json
{"event":"session.end","session_id":"ses_123","stop_reason":"end_turn","usage":{"input_tokens":1000,"output_tokens":500,"total_tokens":1500,"cached_read_tokens":100}}
```

Example (client-side timeout):

```json
{"event":"session.end","session_id":"ses_123","stop_reason":"timeout","usage":{"input_tokens":1000,"output_tokens":200,"total_tokens":1200}}
```

Example (cancelled):

```json
{"event":"session.end","session_id":"ses_123","stop_reason":"cancelled"}
```

## Classification system

Avenor classifies events into three buckets: MILESTONE, FINDING, ACTIVITY. This classification drives alerting and reporting.

**MILESTONE** — Structural decisions or boundaries: loop start/end, phase end, permission gates, session end, retries, errors, `agent.status` transitions to `waiting` or `done`.

**FINDING** — Content that explicitly signals a problem: confidence scores ≥60% in message text, explicit `[finding]` markers, phrases like "reviewer flagged", "correction needed", "failed test".

**ACTIVITY** — Everything else: routine tool calls, message chunks, phase transitions to `thinking` or `working`.

When using `avenor watch --classify`, MILESTONE and FINDING events are tagged so they stand out; ACTIVITY events are not.

## Emitting your own events

You can emit custom events for Avenor to pass downstream. Avenor treats any unknown event type as ACTIVITY. If you want a custom event classified as MILESTONE or FINDING, post-process it with `avenor digest --classify` before shipping downstream.

Custom events must have an `event` field (string) and may include any other fields. Avenor does not validate or transform them.

## Design notes

- **Flattened structure.** No nested `fields` object. All domain fields live at the top level alongside `event` and `session_id`. This simplifies both human reading and LLM parsing.

- **Timestamps.** Most events include `ts` (Unix milliseconds) so consumers can correlate timing without relying on file-write order.

- **Session and run tracking.** `session_id` identifies a single Claude conversation. `run_id` identifies a loop execution (only present in config-driven runs); `run_label` is an optional human label for the run.

- **Markers.** Loop directives (`[abort]`, `[exit]`) are extracted from phase exit text and encoded in `avenor.phase.end` fields so LLMs can react without parsing free text.

- **Permission flow.** `permission.request` fires when the backend asks the client to decide. `permission.response` fires after Avenor resolves the decision, marking the moment the backend is answered.
