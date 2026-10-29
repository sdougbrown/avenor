# Top-level peers — Pi and Claude on one central broker

Independently started Pi and Claude sessions register distinct inboxes on the
same central broker. Pi retains its local supervisor for children. Neither
client needs an inbound listener for central messages.

This map covers the Pi–Claude slice of #434. It does not establish host-session
identity for every harness or plain Model Context Protocol (MCP) client.

## Setup

Follow [verify-avenor](../SKILL.md) to build the binary and create an evidence
directory. Build the Pi extension too:

```bash
mise run go-build js-pi-build
```

Load `packages/pi/dist` explicitly in the top-level Pi session and its children,
as [messaging.md](messaging.md#driving-it-with-the-avenor-binary) describes.
Use the checkout's Claude plugin and a supported Claude release, as
[claude-peers.md](claude-peers.md) describes. Follow the verification skill's
cleanup procedure when the drive ends.

Start the central broker independently of either client. For a local drive:

```bash
./avenor stable --control-socket /tmp/avenor-434.sock \
  --broker-addr 127.0.0.1:18787 --idle-timeout 0
```

Start Pi and Claude directly. Set `AVENOR_BROKER_URL` to the broker address;
leave `AVENOR_RUN_ID` and `AVENOR_BROKER_TOKEN` unset. Name the Pi session
`pi-scout` and set `AVENOR_SESSION_NAME=claude-scout` for Claude.

Do not use `avenor_spawn` for either top-level session. Keep both sessions alive
between prompts so the recipient can receive while idle.

For a container drive, place the broker and clients in separate network
namespaces and use a broker address reachable from the client network. Do not
publish client ports. Record the container and network inspection output, each
process's full launch arguments, and each client's listening sockets.

A Podman pod shares a network namespace among its members. Use separate client
containers or pods to prove delivery across a network boundary.

Keep credentials out of captured arguments, URLs, logs, and evidence. Retain
session transcripts and structured tool results, not credential-bearing HTTP
request bodies.

## Drive

1. In Pi, call `avenor_peers`. Capture a complete listing containing
   `pi-scout` and `claude-scout` with distinct run IDs. Before any spawn,
   confirm that using central peer tools did not start a local supervisor.
2. In Pi, ask `claude-scout` to reply with `CLAUDE-PEER-OK`. Let idle Claude
   receive the injected prompt and answer through its peer layer. Capture Pi's
   `replied` result and Claude's matching inbound message and reply result.
3. In Claude, ask `pi-scout` to reply with `PI-PEER-OK`. Let idle Pi receive
   the ask and use `avenor_message` action `reply` with the instructed
   `reply_to`. Capture both tool results and the matching message ID.
4. In Pi, spawn a local Pi child with a standing rule to answer inbound asks.
   Capture spawn, status, result, and an ask by the child's unique label through
   the real tools. Preserve its sentinel and event log as the
   [messaging map](messaging.md) requires.

   Capture a combined peer list that includes the child and both top-level peers.
5. Give two different peers the same label. A message to that label must return
   `ambiguous_name` with candidates and reach neither inbox. Address a unique
   ID successfully. Exercise distinct brokers with matching IDs too: matching
   IDs must not be treated as evidence of one identity.
6. Interrupt one peer-list transport. Resolve a name still present in the other
   view, capture the refusal, and confirm no message reached that destination.

   The deterministic incomplete-listing cases in
   `packages/pi/src/peer-provider.test.ts` cover a listing marked partial.
   Keep their test result separate from the live transport-interruption proof.
7. Exercise an inbound reply claim and an outbound withdrawal on each provider.
   Capture that the owning provider handles each action. Unknown message IDs
   and colliding ownership must be refused without trying the other provider.
8. Interrupt Pi's broker connection for less than 60 seconds, then restore it.
   Capture recovery with the same run ID and no new registration.

   Repeat past the lease deadline. The stale peer must disappear, receive no
   new messages, and refuse participation until a new session starts.
9. Shut down Pi normally. Capture its retirement and absence from a later peer
   listing.

   Capture the delayed-registration and non-peer-retirement refusals from
   `internal/runtime/broker/peer_lifecycle_test.go`. Keep credentials out of
   captured output.

Use deadline-bounded observations of tool results or registry state. A fixed
sleep does not prove that a session received or answered a message.

## Proof state

Retain all of the following in the evidence directory:

- Full launch arguments, broker process ID or container ID, network topology,
  client port inspection, and cleanup commands.
- Pi RPC output in JSON Lines format with tool arguments and `result.details`,
  plus the independent Claude transcript with inbound prompts and reply results.
- Complete peer listings with names and IDs before spawn, after spawn, after
  reconnect, and after teardown.
- The local child's sentinel, event log, and terminal status/result.
- Refusal and ownership results from the duplicate-label, duplicate-ID,
  incomplete-listing, reply, withdrawal, and lifecycle drives.

Unit tests cover deterministic ownership and lifecycle cases. They do not
replace the independent-session exchange or local-child drive.

A container network drive proves outbound delivery across namespaces with no
published client listener. It replaces the separate-host setup for that network
proof, but does not prove the deployed Tailscale path. Record a cross-host
exchange over Tailscale before claiming that deployment is verified.

## Known limits

The broker's open registration route relies on network access control. Leases
are opt-in; existing Claude self-registrations are not leased. A retired peer's
record is reaped 30 minutes after its identity stops answering
(`peerTombstoneTTL`), so its last turn stops being readable and its run id
becomes registrable again after that window.

A bearer credential can be copied. Overlapping-poll exclusion does not prove
one physical consumer.

Polling dequeues before session injection. Pi retains an injection failure in
memory for retry, but a process crash or interrupted response can lose delivery.
Neither the local nor container drive establishes exactly-once delivery.
