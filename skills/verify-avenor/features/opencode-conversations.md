# OpenCode conversations — native host messaging integration

A bound OpenCode conversation has its own broker run and feed. This map verifies
messaging between native OpenCode sessions and routing to a spawned child's
parent conversation.

This map covers the OpenCode v2 messaging slice of #561. It does not establish identity for non-bound sessions or verify the `avenor-inbox` poo feed surface.

## Setup

Follow [verify-avenor](../SKILL.md) to build the binary and create an evidence directory. Use the absolute path to the OpenCode v2 binary, as the default `opencode` on the path may be v1.15.5.

Build the checkout's Go binary and OpenCode plugin. Use a scratch working
folder, `HOME`, `XDG_CONFIG_HOME`, `XDG_STATE_HOME`, and `AVENOR_HOME`.
Configure the checkout's `packages/opencode` **directory** as the plugin;
OpenCode v2 rejects a path to its `dist` file. Configure a model that the
scratch host can use without copying credentials. The plugin starts its own
supervisor when a session first uses an Avenor tool.

Start the native host without inherited `AVENOR_*` variables, using a free
loopback port. For example, after preparing the scratch environment and plugin
config:

```bash
/home/douglasbrown/.opencode/bin/opencode serve --hostname 127.0.0.1 --port 18437
```

Do not spawn the host through `avenor_spawn`. Record its PID and use the
control plane to shut down its supervisor. Kill only PIDs from the drive.
Keep credentials out of arguments, URLs, logs, and evidence.

## Drive

1. **Identity & Renaming**: Start two bound OpenCode sessions (A and B). Verify they have distinct run IDs and distinct session IDs. Use the native OpenCode session API to rename both sessions. Capture the peer listings to confirm the names updated while the run IDs remained unchanged.

2. **Bidirectional tells**: Send a `tell` from A to B and another from B to A.
   Capture each target session's transcript and the peer listings.

3. **Cross-Session Ask/Reply**: Session A issues an `ask` to Session B with a specific `timeout_ms`. Session B answers using `avenor_message` with action `reply` and the exact `reply_to` message ID. Verify that Session A receives the reply in well under the timeout. Capture the matching message IDs and the `replied` tool result.

4. **Child-Parent Routing**: Spawn a local Pi child from the OpenCode host. Instruct the child to target the host's bound run ID (the `parent_run_id` provided in the supervisor's parent note) rather than the `supervisor` label. 
   - Verify the child's `tell` reaches the host's inbox.
   - Verify the child's `ask` reaches the host and the host's response reaches the child.
   - Capture the child's sentinel and event logs.

5. **Argument Security**: Verify that all `avenor_message` calls with action `reply` use `--message-stdin` rather than passing the reply text as a command-line argument. Capture the exact `argv` for all invocations to prove the text never appears in the process list.

6. **Lifecycle & Errors**:
   - Exercise a `last` call to confirm the last settled turn of a session is readable.
   - Attempt a reply to an unknown, well-formed message ID and capture the refusal.
   - Verify that the host process correctly handles the shutdown of the supervisor and its own cleanup.

## Proof state

Retain all of the following in the evidence directory:

- Full launch arguments for the host, supervisor, and all spawned children.
- The host's environment variables (scrubbed of sensitive credentials) and the host's absolute binary path.
- Complete session transcripts for A and B, showing inbound messages and tool results.
- The `argv` logs for all `avenor_message` invocations, proving text is passed via stdin.
- The child's sentinel (`.done`) and event logs.
- Peer listings showing the lifecycle of session names and run IDs.
- Structured tool results for all `ask`, `reply`, and `tell` actions, including matching message IDs.

A model transcription error does not prove successful ask/reply. Repeat that
leg with the exact message ID before claiming it passed. This drive does not
prove cross-broker routing, strict consumer fencing, or exactly-once delivery.
