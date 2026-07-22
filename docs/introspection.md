# Run introspection

A status such as `working` tells you that a delegated run is alive. It does not tell you whether the agent is reading the right file, repeating itself, blocked on a tool, or already holding the answer. Run introspection combines Avenor's event stream with a bounded transcript view so hosts can show that difference without loading an entire event log into model context.

## Data flow

Avenor keeps the layers separate:

1. Runtime adapters translate backend activity into the canonical event vocabulary.
2. The control protocol stamps runtime events with `ts` and runtime-local `seq`, retains a bounded replay window, and streams live notifications.
3. `@dougbots/avenor-core` normalizes canonical and compatibility events into a bounded run snapshot.
4. Pi and OpenCode choose how to present that snapshot.

The NDJSON file configured by `on_event` remains the durable source of truth. Control-socket history and host snapshots are intentionally bounded attachment and display surfaces.

## Pi

Use `/avenor-watch` to select a tracked run or pass a run ID directly:

```text
/avenor-watch <run-id>
```

The inspector shows assistant and reasoning text exposed by the backend, live and completed tools, permissions, status, and final output. It supports scrolling, cancellation, and follow-up input. Raw ANSI and terminal control characters are removed before rendering.

The model-facing `avenor_inspect` tool returns the same kind of bounded snapshot as JSON. Use it for transcript and tool diagnostics without consuming raw `avenor_events` records. When the parent only needs the sub-agent's conclusion, `avenor_result` waits for the run and returns its complete final output without the snapshot.

## OpenCode

The OpenCode plugin uses the shared observer for live tool metadata, waiting/permission transitions, and final output. It also exposes `avenor_inspect`. OpenCode does not currently reproduce Pi's full-screen overlay; presentation remains host-specific.

## Fidelity boundaries

Backends expose different native detail. Avenor normalizes what is available but does not invent hidden reasoning or missing tool output. Compatibility events such as Pi's `avenor.message.*` and `avenor.tool.*` remain in the stream for existing consumers, while the shared reducer avoids showing their canonical equivalents twice.

Replay is limited to the control server's recent in-memory history. A subscriber that cannot keep up receives `subscriber.lagged`; consult the NDJSON log when complete history matters.

See also: [Control protocol](control-protocol.md) and [Event stream](events.md).
