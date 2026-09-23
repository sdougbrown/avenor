# Escalation bridge template

A reference integration for Avenor's [remote-human gate escalation surface](../../docs/escalation.md).
It demonstrates the full protocol — wake, observe, decide — against a generic
webhook transport, so any notification service (Challah, a Slack bot, a pagerduty
script) can adopt the same contract without Avenor knowing it exists.

## What's here

```text
templates/escalation-bridge/
  demo.json    minimal workflow template with a human merge-authorization gate
  bridge.py    reference bridge: watches for parked gates, asks, records decisions
```

A TypeScript port of the bridge ships as
[`@dougbots/avenor-escalation-bridge`](../../packages/escalation-bridge/) for
consumption from TypeScript applications; it derives identical `response_hash`
values for the same decision (for the string/integer/bool/null payloads the
protocol calls for), so decision files and the audit trail are
interchangeable between the two implementations.

## The protocol in one pass

Avenor side (all existing control-socket commands — no Avenor changes):

1. `workflow.wait` ticks: it returns on a terminal status or on timeout. A
   parked gate never wakes it early — parking lives on activations, not on the
   workflow status.
2. `workflow.inspect` exposes flat `activations`/`gates`/`outputs` lists; a
   parked activation reads `status: "awaiting_gate"`. Pending human gates are
   the human gate definitions of the parked activation's node, loaded from the
   template out-of-band (inspect carries no template definitions).
3. `workflow.command {op: "gate"}` records the attributed decision — `actor`,
   `reason`, at least one `evidence_id`, a `subject` matching the gate's
   `subject_type`, and a `response_hash` — as a durable gate instance; the
   activation follows its declared branch.

Bridge side (this template):

1. Tick `workflow.wait`; on every non-terminal return, inspect for parked
   activations and derive the pending human gates from the `--template` file
2. POST the question (gate, declared subject type, allowed outcomes, the
   workflow's latest outputs — and, for a bound gate, the exact pinned
   subject) to the transport webhook
3. Poll for the transport's answer file — bounded by `--gate-timeout`, with
   the workflow state re-checked on every tick — and submit the gate command,
   deriving a stable `evidence_id` from the decision and carrying the
   transport's pointer in the `reason`. For a bound gate the bridge submits
   the pinned subject (never the transport's) and re-inspects before
   submitting to confirm the activation is still parked on the same pinned
   subject; a changed subject drops the decision.

The transport owns all human-facing semantics: deadlines, one-use claims,
button expiry. The kernel records only the durable, append-only decision.

## Try it

Start a stable supervisor, register the demo template, and drive the two
manual nodes to the gate. Node driving uses the claim → start → complete cycle
([workflow.md](../../docs/workflow.md#the-claim--start--complete-cycle)):

```sh
# 1. Stable supervisor with a control socket
avenor stable --control-socket /tmp/avenor-stable.sock &

# 2. Register and instantiate the demo workflow
avenor workflow create --socket /tmp/avenor-stable.sock \
  --request-file templates/escalation-bridge/demo.json
echo '{"metadata":{}}' > /tmp/instance.json
avenor workflow instantiate --socket /tmp/avenor-stable.sock \
  --template-id escalation-demo --template-version 1.0.0 \
  --request-file /tmp/instance.json    # prints the workflow id wf_...

# 3. Claim + start publish, then complete it with the exact head under review:
#    outcome "ready", outputs summary/repository/pull_number/head_sha.
#    merge-auth activates.

# 4. Claim + start merge-auth, then complete it:
#    outcome "authorized", output authorized_head.
#    The activation parks awaiting_gate — the merge-authorization gate is
#    still unsatisfied, so no branch is followed yet.

# 5. Run the bridge (with a webhook receiver of your choice)
mkdir -p /tmp/escalation
python3 templates/escalation-bridge/bridge.py \
  --socket /tmp/avenor-stable.sock --workflow-id wf_... \
  --template templates/escalation-bridge/demo.json \
  --webhook-url http://127.0.0.1:8765/ask --decision-dir /tmp/escalation

# 6. Decide. The transport writes the decision file the question named. This
#    gate is bound (subject_binding), so the bridge submits the subject the
#    kernel pinned on the activation — the transport's subject is ignored and
#    the question already carried the pinned subject for the human to review:
cat > /tmp/escalation/decision-act_...-merge-authorization.json <<'EOF'
{"decision": "satisfy", "actor": "austin",
 "reason": "Diff reviewed in Slack; approved",
 "evidence": "https://slack.com/archives/C123/p1650000000"}
EOF
#    The bridge records the decision (submitting the pinned subject);
#    merge-auth follows its "authorized" branch and reconcile activates.

# 7. Claim + start reconcile, then complete it with outcome "merged" (or
#    "abandoned"). The workflow reaches its terminal outcome.
```

The recorded decision is part of the append-only gate history — inspect it any
time:

```sh
avenor workflow inspect --socket /tmp/avenor-stable.sock wf_...
```

### Decision file schema

| Field | Required | Meaning |
|---|---|---|
| `decision` | yes | `satisfy` or `reject` |
| `actor` | yes | The deciding human, in the transport's namespace |
| `reason` | yes | Free-text rationale; the bridge appends the evidence pointer |
| `evidence` | recommended | The transport's immutable pointer (permalink, timestamp) |
| `subject` | when the gate declares `subject_type` (and is not bound) | Must carry a matching non-empty `type` — e.g. the exact PR head from the publish outputs. For a bound gate the bridge submits the kernel-pinned subject and ignores this field. |
| `outcome` | no | For `satisfy`, overrides the branch outcome; defaults to the activation's selected outcome. For `reject`, an outcome is needed to follow a declared failure/correction branch — without one the activation settles rejected with no branch. |

The bridge derives the `response_hash` and the `evidence_id` from the decision
payload and archives the answered file under `recorded/`, so the local audit
trail survives recording. An invalid answer — a subject whose `type` doesn't
match the gate's declaration, a missing actor/reason, or a malformed file — is
parked under `rejected/` and never recorded; the next ask carries the
rejection reason as `previous_decision_error` so the human can correct it.

## Adapting a real transport

Replace steps 2–3 on the bridge side with the transport's native flow. For a
Slack-based service like Challah: the webhook POST becomes
`challah_request_self` (approval mode, exact proposed action, deadline), the
decision file becomes the Slack button/reply callback, and the recorded
`actor`/`evidence` come from the Slack user and message timestamp. Nothing on
the Avenor side changes.
