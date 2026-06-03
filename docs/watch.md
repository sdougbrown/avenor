# avenor watch

Read and digest event logs produced by `--on-event`. The raw log is newline-delimited JSON (NDJSON); `watch` parses it and outputs human-readable digest lines, optional classification tags, or pass-through JSON.

`watch` is the tool to inspect what happened in a completed run, or to tail a live run from a separate process. An agent or operator uses it to detect milestones, read findings, and react to structural events without parsing raw JSON.

## Basic usage

Read a completed event log and print digest lines to stdout:

```sh
avenor watch /tmp/run.ndjson
```

Output looks like:

```
EVENT agent.status ses_123 thinking
EVENT agent.thought_chunk ses_123 confidence: 85%, need to read the code
EVENT avenor.phase.start ses_123 phase: code_review (iter 1)
EVENT tool.call ses_123 read:/repo/main.go [pending]
EVENT tool.call_update ses_123 read [completed]
EVENT agent.message_chunk ses_123 the bug is in the loop condition
EVENT avenor.phase.end ses_123 phase: code_review → end_turn
EVENT session.end ses_123 stop_reason=end_turn
```

Each digest line is: `EVENT <event-name> <session-id> <excerpt>`

## Follow mode

Tail a live event log as it grows:

```sh
avenor watch --follow /tmp/run.ndjson
```

`watch` polls the file every 250ms (configurable with `--poll-interval`) and outputs new lines as they arrive. It terminates in two ways:

- **Gracefully:** When the underlying session ends, `watch` reads the `session.end` event and exits with status 0.
- **Signal:** Press Ctrl+C (SIGINT) or send SIGTERM. `watch` flushes output and exits cleanly.

If the log is rotated or truncated while tailing, `watch` will read whatever remains and exit cleanly on the next poll cycle when the file does not reopen.

## Output formats

### Plain format (default)

`--format plain` outputs digest lines. Each line contains:

- `EVENT` prefix
- Event name (`agent.status`, `tool.call`, etc.)
- Session ID
- Excerpt (context-specific summary of the event)

Excerpts are trimmed to 120 characters and have line breaks replaced with spaces:

```
EVENT agent.message_chunk ses_123 I found three issues with the review logic
EVENT avenor.phase.end ses_123 phase: analyze → end_turn
```

### JSON format

`--format json` passes through the original NDJSON with no digestion:

```sh
avenor watch --format json /tmp/run.ndjson
```

Output is NDJSON (one JSON object per line), identical to the input:

```json
{"event":"agent.thought_chunk","session_id":"ses_123","content":{"text":"confidence: 92%"},"ts":1234567890}
{"event":"tool.call","session_id":"ses_123","toolCallId":"call_1","kind":"read","title":"/repo/main.go"}
```

## Classification

The `--classify` flag adds a three-level classification tag to each line:

- **MILESTONE** — Structural events: loop start/end, permission gates, session end, retries, phase end, errors, agent status transitions to `waiting` or `done`.
- **FINDING** — Content signals: message or thought text with confidence ≥60%, explicit `[finding]` marker, phrases like "reviewer flagged", "correction needed", "failed test".
- **ACTIVITY** — Everything else: routine tool calls, message chunks, phase transitions to `thinking` or `working`.

### Plain format with classification

```sh
avenor watch --classify /tmp/run.ndjson
```

Output:

```
MILESTONE EVENT agent.status ses_123 waiting
FINDING EVENT agent.thought_chunk ses_123 confidence: 75% that the fix is correct
ACTIVITY EVENT tool.call ses_123 read:/repo/file.go [pending]
MILESTONE EVENT session.end ses_123 stop_reason=end_turn
```

### JSON format with classification

```sh
avenor watch --classify --format json /tmp/run.ndjson
```

Each JSON object gets an injected top-level `classify` field:

```json
{"event":"agent.thought_chunk","session_id":"ses_123","content":{"text":"I found a bug"},"classify":"FINDING","ts":1234567890}
{"event":"session.end","session_id":"ses_123","stop_reason":"end_turn","classify":"MILESTONE","ts":1234567890}
```

Use classification to wake up a consumer that only cares about milestones or findings. For example, a supervisor agent might tail a run and only log lines where `classify` is `FINDING` or `MILESTONE`.

## Cursor-based reading

For incremental polling (reading new events since last check), use the cursor feature:

```sh
avenor watch --since-cursor /tmp/watch.cursor /tmp/run.ndjson
```

**On first run:** The cursor file does not exist. `watch` reads from byte offset 0 (beginning of the log).

**On subsequent runs:** `watch` reads the saved offset from the cursor file, seeks to that position in the log, and processes only new lines.

**On exit:** `watch` writes the current byte offset to the cursor file atomically (write to `.tmp`, then rename). If the log file was rotated (cursor offset exceeds file size), `watch` will fail at startup with an error message.

The cursor file is plain text, containing a single decimal byte offset and a newline:

```
12345
```

### Rotation handling

If the log is rotated while `watch` is running with a cursor, the next invocation will detect that the cursor offset is beyond the rotated log's size and exit with an error:

```
avenor watch: log /tmp/run.ndjson shorter than cursor (offset 50000 > size 5000) (cursor /tmp/watch.cursor)
```

In that case, delete the cursor file and restart — `watch` will read the new log from the beginning.

### Pattern: polling a live run

Use the cursor to implement incremental polling from a separate script:

```sh
#!/bin/bash
LOG=/tmp/run.ndjson
CURSOR=/tmp/watch.cursor

while true; do
  avenor watch --classify --since-cursor "$CURSOR" "$LOG" | while read line; do
    if [[ "$line" =~ MILESTONE|FINDING ]]; then
      notify-operator "$line"
    fi
  done
  sleep 5
done
```

Each call to `avenor watch` reads only the delta since the last call, processes it, and updates the cursor. The script can react to milestones and findings without re-reading the entire log.

## Flags reference

All flags (with defaults):

| Flag | Default | Type | Meaning |
|---|---|---|---|
| `--follow` | false | bool | Poll and tail the log (exit on session.end or SIGINT) |
| `--poll-interval` | 250ms | duration | Sleep between polls in follow mode |
| `--format` | `plain` | string | Output format: `plain` or `json` |
| `--classify` | false | bool | Prefix each line with MILESTONE/FINDING/ACTIVITY tag |
| `--since-cursor` | (empty) | string | Cursor file path: seek and save offset |

## Practical patterns

### Tail a live run with milestones only

```sh
avenor watch --classify --follow /tmp/run.ndjson | grep MILESTONE
```

Exits when the session ends.

### Read a completed run and export findings

```sh
avenor watch --classify --format json /tmp/run.ndjson | \
  jq 'select(.classify == "FINDING")'
```

Output each event classified as FINDING as a JSON object.

### Incremental polling with classification

```sh
avenor watch --classify --since-cursor /tmp/watcher.cursor /tmp/run.ndjson
```

On each invocation, processes only new events since the last run, and updates the cursor file.

## Relationship to tracked state

`avenor watch` is useful for operators and shell scripts, but orchestrating code usually wants a stable in-memory view instead of individual lines. Inside the Avenor codebase, use `internal/events.SessionTracker` to reduce the raw `--on-event` stream into a single `SessionState` snapshot.

That gives controllers one canonical interpretation of channel lifecycle fields such as `channel_ready`, prompt queueing/submission, permission waits, report state, and terminal finish/session-end state.

## Relationship to raw events

See [events.md](events.md) for the full specification of event types, fields, and semantics. `watch` digests the raw NDJSON by:

1. Parsing each line as JSON.
2. Extracting the `event` field and domain-specific fields (like `title`, `kind`, `phase`).
3. Formatting a human-readable excerpt (120 chars max, whitespace normalized).
4. Optionally prepending a classification tag.

The digest is lossy — it discards most fields — but captures the narrative of the run. For detailed inspection of a particular event, use `--format json` and pipe to `jq`.

## See also

- [events.md](events.md) — full event type reference and classification rules
- [stable.md](stable.md) — batching multiple runs and exporting results
