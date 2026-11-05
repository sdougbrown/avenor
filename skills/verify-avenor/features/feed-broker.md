# Broker feed — `avenor feed broker`

## Sub-features

- Broker-stamped tell and ask messages become one NDJSON line each on stdout.
- Asks name the message ID and the `call_cli` reply instruction.
- One feed consumes a run's inbox per broker; a competing feed exits without polling.
- A revoked credential parks the feed after one refusal; shutdown drains an in-flight poll.

## How to get to it (user POV)

A poo session starts `avenor feed broker` as a feed with its broker URL, run ID,
and token in `AVENOR_BROKER_URL`, `AVENOR_RUN_ID`, and `AVENOR_BROKER_TOKEN`.
The feed is the sole consumer of that run's broker inbox. Its stdout belongs to
poo; diagnostics go to stderr.

## Driving it with the avenor binary

Use a live supervisor and a registered run provisioned for this feed. Do not
use a run whose inbox another consumer is polling. Send the run a tell and an
ask from another registered run, using the broker messaging drive in
[broker-messaging.md](broker-messaging.md). Keep the supervisor and the feed's
parent alive until both messages arrive. Do not print or save the token.

```sh
mkdir -p .cache/feed-verify
# Set the three broker environment variables from the provisioned run.
timeout -k 5s 60s ./avenor feed broker \
  --lock-dir "$PWD/.cache/feed-verify/locks" \
  > .cache/feed-verify/feed.ndjson 2> .cache/feed-verify/feed.log
```

While the feed is running, start the same command with the same credentials and
lock directory; it exits 0 without taking messages. Send a tell and an ask to
the run, then stop the first feed with SIGTERM. Capture the command arguments,
stdout, stderr, and the sender's message IDs in the evidence directory, without
recording credentials or message content that should remain private.

**Observable end state (the proof):** the tell and ask each have one JSON line
whose `from` is the broker-stamped sender. The tell's `text` is the message; the
ask's `text` includes the ask ID and the `call_cli` instruction for `avenor_message` with `action: "reply"`.
The second feed exits 0, and the first exits 0 on SIGTERM. A separately started
feed with a revoked token logs one refusal and remains alive until stopped.

## Gotchas

- A broker run ID alone is not globally unique; lock identity includes the URL.
- The feed drains the inbox. Never run the drive against a session whose normal
  consumer needs those messages.
- A live supervisor and independently registered sender are required; the
  missing-credential usage path alone does not prove message delivery.
