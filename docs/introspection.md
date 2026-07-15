# Run introspection

A status such as `working` tells you that a delegated run is alive. It does not tell you whether the agent is reading the right file, repeating itself, blocked on a tool, or already holding the answer. Run introspection combines Avenor's event stream with a bounded transcript view so hosts can show that difference without loading an entire event log into model context.

## Data flow

Avenor keeps the layers separate:

1. Runtime adapters translate backend activity into the canonical event vocabulary.
2. The control protocol stamps runtime events with `ts` and runtime-local `seq`, retains a bounded replay window, and streams live notifications.
3. `@dougbots/avenor-core` normalizes canonical and compatibility events into a bounded run snapshot.
4. Hosts choose how to present that snapshot.

The NDJSON file configured by `on_event` remains the durable source of truth. Control-socket history and host snapshots are intentionally bounded attachment and display surfaces.

## Fidelity boundaries

Backends expose different native detail. Avenor normalizes what is available but does not invent hidden reasoning or missing tool output. Compatibility events such as Pi's `avenor.message.*` and `avenor.tool.*` remain in the stream for existing consumers, while the shared reducer avoids showing their canonical equivalents twice.

Replay is limited to the control server's recent in-memory history. A subscriber that cannot keep up receives `subscriber.lagged`; consult the NDJSON log when complete history matters.

See also: [Control protocol](control-protocol.md) and [Event stream](events.md).
