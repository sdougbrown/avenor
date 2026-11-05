# Child message CLI — `avenor message`

## How to reach it

A spawned child inherits `AVENOR_BROKER_URL`, `AVENOR_BROKER_TOKEN` and
`AVENOR_RUN_ID`. `avenor message --action tell`, `ask` and `reply` route as
the child, not as `supervisor`. Tell and ask require `--target`. A reply to an
ask needs the claim record the feed wrote; use `--asks-dir` if the feed uses a
non-default directory. A second reply is `edge-absent`. A reply to a tell has
no claim record and the broker resolves its sender.

## Drive and proof

Use the real host and spawned Pi children described in
[messaging.md](messaging.md#driving-it-with-the-avenor-binary), with this
checkout's `avenor` binary on the children's PATH. Capture host RPC output,
child event logs and sentinels in an evidence directory outside the child work
directory. Ensure the broker feed records delivered asks. Ask the host to spawn
children and coordinate these commands through their shell tools, recording JSON:

1. Child A runs `avenor message --action tell --target supervisor --message CLI-TELL`
   and `avenor message --action ask --target supervisor --message CLI-ASK --timeout-ms 120000`.
   The host receives both from A and replies `CLI-ANSWER`; A reports `replied`.
2. The host tells A `CLI-NOTICE`; A runs `avenor message --action reply --reply-to ID --message CLI-TELL-REPLY`.
   The host receives the reply and A reports `sent`.
3. The host asks A `CLI-QUESTION`; A runs `avenor message --action reply --reply-to ID --message CLI-REPLY`.
   A reports `replied`, and the host receives `CLI-REPLY`. Repeating the reply
   reports `edge-absent`.
4. A asks an idle child with `--timeout-ms 1000`. Its command exits 0 with
   outcome `timed-out`. Missing action or credentials exits 2; an unknown
   target exits 1 with `unknown_target` in stdout JSON.

Proof is the host's structured tool results and delivered messages, the CLI
JSON and exit statuses captured by the children, and each child's `session.end`
and `DONE` sentinel. Save the exact invocations and do not include broker tokens
in evidence. Shut down the supervisor through its control socket and clean up
only the scratch files and processes started by the drive.
