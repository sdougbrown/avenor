# @dougbots/avenor-escalation-bridge

TypeScript implementation of Avenor's [remote-human gate escalation
bridge](../../docs/escalation.md). Watches a durable workflow for parked
`awaiting_gate` activations, carries each pending human gate to a transport
webhook, and records the human's answer through the ordinary gate command
path. Avenor ships no notification code; this package is the transport half,
kept deliberately generic:

- **ask**: POSTs a JSON question payload to `--webhook-url` (a Slack bot, a
  pager script — anything that can receive a webhook)
- **answer**: polls `--decision-dir` for `decision-<activation>-<gate>.json`
  files written by the transport

This is a port of the stdlib-only Python reference in
[`templates/escalation-bridge/bridge.py`](../../templates/escalation-bridge/bridge.py)
and produces identical `response_hash` values for the same decision (parity
holds for the string/integer/bool/null payloads the protocol calls for;\nfloat-valued fields may hash differently across the two implementations). Use
this package from TypeScript applications that already depend on
[`@dougbots/avenor-core`](../core/); pull the Python reference into Python
environments instead. The protocol contract itself is documented in
[docs/escalation.md](../../docs/escalation.md).

## CLI

```sh
avenor-escalation-bridge --socket /tmp/avenor-stable.sock --workflow-id wf_... \
  --template templates/escalation-bridge/demo.json \
  --webhook-url http://127.0.0.1:8765/ask --decision-dir /tmp/escalation \
  [--wait 30s] [--gate-timeout 15m] [--poll 2s]
```

The bridge runs until the workflow reaches a terminal state, then exits 0.
Transient RPC/webhook failures back off and retry; `SIGINT` exits 130.

## Library

Every layer is exported so hosts can compose their own loop — drive the
control client yourself, swap the transport, or reuse just the decision
plumbing:

```ts
import {
  runBridge,           // full watch/ask/record loop
  buildGateCommand,    // decision file -> workflow.command gate payload
  waitForDecision,     // poll a decision file with state re-checks
  pendingHumanGates,   // parked activations x human gate definitions
  latestOutputs,       // inspect outputs -> decoded decision context
  loadHumanGates,      // template JSON -> node_id -> human gates
  parseDuration,
} from '@dougbots/avenor-escalation-bridge'

await runBridge({
  socketPath: '/tmp/avenor-stable.sock',
  workflowId: 'wf_...',
  webhookUrl: 'http://127.0.0.1:8765/ask',
  decisionDir: '/tmp/escalation',
  templatePath: 'templates/escalation-bridge/demo.json',
})
```

`runBridge` accepts injectable `connect`, `ask`, `sleep`, `now`, and `log`
functions for embedding in a host's own runtime; the control connection only
requires the structural `ControlClient` interface (`call`/`isClosed`/`close`),
which `dial()` from `@dougbots/avenor-core` satisfies.

## Decision file schema

Same contract as the Python reference — see the
[decision file schema](../../templates/escalation-bridge/README.md#decision-file-schema)
in the template README. Answered files are archived under `recorded/`; invalid
answers (bad operation, missing actor/reason, subject-type mismatch,
malformed JSON) are parked under `rejected/` and never recorded, with the
rejection reason carried to the next ask as `previous_decision_error`.

Transport-side, write decision files atomically (write a temp file in the
directory, then rename it into `decision-<activation>-<gate>.json`):
the bridge treats a file that fails to parse once as possibly still
in-flight, but one whose content fails two consecutive polls as invalid
and moves it aside before it is complete.

Unlike the Python reference (whose `urllib` client follows redirects), the
TypeScript bridge refuses 3xx webhook responses: a 307/308 redirect
preserves method and body, so the question payload would be forwarded to
the target. Address the transport at its final URL (e.g. `https://` directly).

## Trust model

The decision directory is created `0700`, the bridge refuses to start on a
pre-existing dir that is group- or other-writable, and both must stay
local-user scoped:
a decision file's `actor` is self-asserted, so any local user who can
write there can forge an attributed gate decision. The ask payload also
POSTs the workflow's full `latestOutputs` map to the webhook; if those
outputs can carry secrets, point the bridge at a transport you trust with
them (or filter on the transport side).
