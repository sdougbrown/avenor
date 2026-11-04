# Child message CLI — `avenor message`

## How to reach it

A spawned child inherits `AVENOR_BROKER_URL`, `AVENOR_BROKER_TOKEN` and
`AVENOR_RUN_ID`. From that child, `avenor message tell`, `ask` and `reply`
route as the child, not as `supervisor`. `tell` and `ask` default their target
to `supervisor`; `reply --to` names the asker's broker run ID. A reply to a
tell omits `--to`. A wrong `--to` falls back to a plain message to that run,
leaving the ask unanswered.

## Drive and proof

Use the real host and spawned Pi children described in
[messaging.md](messaging.md#driving-it-with-the-avenor-binary), with this
checkout's `avenor` binary on the children's PATH. Capture host RPC output,
child event logs and sentinels in an evidence directory outside the child work
directory. Ask the host to spawn two Pi children and coordinate the following
commands through their shell tools, recording each command's JSON output:

1. Child A runs `avenor message tell --text CLI-TELL` and
   `avenor message ask --text CLI-ASK --timeout-ms 120000`. The host receives
   both messages from A's runtime ID and replies to the ask with `CLI-ANSWER`.
   A's ask JSON reports `replied` and contains `CLI-ANSWER`.
2. The host tells child A `CLI-NOTICE`; A uses the delivered message ID in
   `avenor message reply --reply-to ID --text CLI-TELL-REPLY`, omitting `--to`.
   The host receives `CLI-TELL-REPLY` from A and the CLI reports `sent`.
3. The host asks child A `CLI-QUESTION`; A uses the delivered ID in
   `avenor message reply --reply-to ID --to supervisor --text CLI-REPLY`.
   The CLI reports `replied` and the host's ask returns `CLI-REPLY`.
4. The host asks A another question. A replies with that ID but `--to` set to
   child B's broker run ID. The command reports `sent`, B receives the plain
   message, and the host's ask remains pending until A replies using
   `--to supervisor`.
5. A asks an idle child with `--timeout-ms 1000`. Its command exits 0 with
   outcome `timed-out`. Running a message command without the three credentials
   exits 2.

Proof is the host's structured tool results and delivered messages, the CLI
JSON and exit statuses captured by the children, and each child's `session.end`
and `DONE` sentinel. Save the exact invocations and do not include broker tokens
in evidence. Shut down the supervisor through its control socket and clean up
only the scratch files and processes started by the drive.
