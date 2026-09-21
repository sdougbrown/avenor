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

## The protocol in one pass

Avenor side (all existing control-socket commands — no Avenor changes):

1. `workflow.wait` long-polls until the workflow parks on `awaiting_gate`
2. `workflow.inspect` returns the pending `human` gate and its exact subject
3. `workflow.command {op: "gate"}` records the attributed decision as a durable
   gate instance; the activation follows its declared branch

Bridge side (this template):

1. Long-poll `workflow.wait` on the opted-in workflow
2. On a parked human gate, POST the question to the transport webhook
3. Poll for the transport's answer file and submit the gate command with
   `actor`, `reason`, and a `response_hash` derived from the answer

The transport owns all human-facing semantics: deadlines, one-use claims,
button expiry. The kernel records only the durable, append-only decision.

## Try it

Start a stable supervisor with the demo template registered, then run the
bridge against a toy transport:

```sh
# 1. Stable supervisor with a control socket
avenor stable --control-socket /tmp/avenor-stable.sock &

# 2. Register and instantiate the demo workflow
avenor workflow create --socket /tmp/avenor-stable.sock \
  --request-file templates/escalation-bridge/demo.json
echo '{"metadata":{}}' > /tmp/instance.json
avenor workflow instantiate --socket /tmp/avenor-stable.sock \
  --template-id escalation-demo --template-version 1.0.0 \
  --request-file /tmp/instance.json

# 3. Run the bridge (with a webhook receiver of your choice)
mkdir -p /tmp/escalation
python3 templates/escalation-bridge/bridge.py \
  --socket /tmp/avenor-stable.sock --workflow-id wf_... \
  --webhook-url http://127.0.0.1:8765/ask --decision-dir /tmp/escalation

# 4. Complete the publish node, let it park on merge-auth, then decide:
cat > /tmp/escalation/decision-merge-authorization.json <<'EOF'
{"decision": "satisfy", "actor": "austin", "reason": "Diff reviewed; approved"}
EOF
```

The activation resumes and the workflow walks to its terminal outcome. Inspect
the decision any time — it is part of the append-only gate history:

```sh
avenor workflow inspect --socket /tmp/avenor-stable.sock wf_...
```

## Adapting a real transport

Replace steps 2–3 on the bridge side with the transport's native flow. For a
Slack-based service like Challah: the webhook POST becomes
`challah_request_self` (approval mode, exact proposed action, deadline), the
decision file becomes the Slack button/reply callback, and the recorded
`actor`/`evidence` come from the Slack user and message timestamp. Nothing on
the Avenor side changes.
