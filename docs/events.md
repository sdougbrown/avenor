# Avenor Event Stream

Avenor writes newline-delimited JSON to `--on-event`. Each line is one event.

`internal/events.Event` flattens event fields into top-level JSON keys. There is no nested `fields` object:

```json
{"event":"tool.call","session_id":"ses_123","toolCallId":"call_1","status":"pending"}
```

Consumers should switch on `event` and read domain fields directly from the same object.

## Event Kinds

`agent.message_chunk`: Assistant-visible response text. Common fields include `content`, `messageId`, and `session_update`.

`agent.thought_chunk`: Assistant reasoning or progress text exposed by the backend. Common fields match `agent.message_chunk`.

`tool.call`: A tool invocation began. Common fields include `toolCallId`, `title`, `kind`, `status`, `rawInput`, and `session_update`.

`tool.call_update`: A tool invocation changed status or produced more metadata. Common fields include `toolCallId`, `kind`, `status`, and `session_update`.

`user.message_chunk`: User text reflected by the backend. This is accepted by the parser even when trivial probes do not emit it.

`session.plan`: Backend plan update. This is accepted by the parser even when trivial probes do not emit it.

`agent.status`: Synthesized by Avenor (not passed through from the backend) to signal agent phase transitions. Fields: `phase` (one of `thinking`, `working`, `waiting`, `done`), `label` (optional human-readable description of current activity), `source` (`"avenor"`), `ts` (Unix milliseconds), `run_id` (present when `--run-id` was set or auto-generated). Emitted before the ACP event that triggered the transition. `waiting` and `done` phases classify as MILESTONE; `thinking` and `working` classify as ACTIVITY.

`permission.request`: Backend asks the client to choose a permission option. Avenor emits `request_id`, plus best-effort `tool`, `question`, and `options` fields when available. With `--permission-handler file:<path>`, this event is emitted after `<path>.req` is written.

`permission.response`: Synthesized by Avenor after it resolves a permission decision. Fields: `request_id` (string), `option_id` (string — the chosen optionId), `kind` (`"allow"` or `"reject"`), `source` (`"avenor"` for `--auto-approve`, `"control"` for control-socket answer, `"file"` for file-handler resolution), `run_id` (when present), `run_label` (when present), `ts` (Unix milliseconds). Classifies as ACTIVITY. Emitted for all permission resolution paths.

`session.end`: Terminal record for the prompt. The final event stream line for a completed Avenor run is always `session.end` and includes `stop_reason`. When the backend provides it, `usage` is present with snake_case keys: `input_tokens`, `output_tokens`, `total_tokens`, and `cached_read_tokens`.

## Terminal Record

Normal completion:

```json
{"event":"session.end","session_id":"ses_123","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}
```

Client-side timeout (includes best-effort buffered usage):

```json
{"event":"session.end","session_id":"ses_123","stop_reason":"timeout","usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}
```

Client-side interrupt or cancel:

```json
{"event":"session.end","session_id":"ses_123","stop_reason":"cancelled"}
```

OpenCode ACP 1.14.41 returns `end_turn` after a client-sent cancel, so Avenor overrides the terminal stop reason to `cancelled` or `timeout` when it initiated cancellation.

Stage 1 trivial probes observed `agent.message_chunk`, `agent.thought_chunk`, `tool.call`, `tool.call_update`, and `session.end`. `user.message_chunk`, `session.plan`, and `permission.request` are documented protocol events accepted by Avenor's parser. See `internal/runtime/opencodeacp/testdata/probe-transcript.ndjson` for the canonical sample.
