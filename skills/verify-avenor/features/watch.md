# Event stream reader — `avenor watch`

## Sub-features

- Plain digest output (`EVENT <name> <session>` lines)
- JSON pass-through (`--format json`)
- Follow mode (`--follow`, `--poll-interval`)
- Classification prefixes (`--classify`: MILESTONE / FINDING / ACTIVITY)
- Accumulation of chunked messages (`--accumulate`)
- Cursor resume (`--since-cursor <file>`)

## How to get to it (user POV)

Runs write NDJSON event streams (via `--on-event` or the supervisor's per-child
`events.ndjson`). A user points `avenor watch <log>` at that file to see what
an agent did, optionally tailing it live while the run is in flight.

## Driving it with the avenor binary

Offline read of a completed run's log (the cheap path — reuse an events file
from a [single-run.md](single-run.md) drive):

```sh
./avenor watch /path/to/events.ndjson | head -20
./avenor watch --format json /path/to/events.ndjson | head -5
./avenor watch --classify /path/to/events.ndjson | head -10
```

Live follow (start before a single-run drive, watch it land):

```sh
./avenor watch --follow "$SCRATCH/events.ndjson" > "$SCRATCH/watch.out" &
WATCH_PID=$!
# ... run the drive ...
kill "$WATCH_PID"; cat "$SCRATCH/watch.out"
```

**Observable end state (the proof):** plain output lists `session.start` and
`session.end` lines for the driven session; `--format json` echoes raw NDJSON
lines; `--classify` prefixes lines with one of the three labels.

## Gotchas

- The log path is a positional argument (no flag).
- `--follow` polls until killed — always kill the recorded PID; never leave a
  watcher behind.
- Cursor files (`--since-cursor`) are rewritten on exit; use a scratch path.
