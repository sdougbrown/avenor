# Human gate escalation

A workflow's `human` gate parks an activation in `awaiting_gate` until an explicit,
attributed decision is recorded. The kernel does not care *where* the human is when
they decide. The decision surface is the same whether the human types
`avenor workflow gate` at the terminal or answers through a remote transport — a
Slack bot, a pager script, a web dashboard.

This page documents that remote-human recipe. Avenor ships no notification code
and no transport integration; a bridge process (external to Avenor) watches for
parked gates, carries the question to a human, and records the answer through the
ordinary gate command path. [Challah](https://github.com/afresh-technologies/challah)
is one such transport; the reference bridge in
[`templates/escalation-bridge/`](../templates/escalation-bridge/) demonstrates the
protocol against a generic webhook.

## The recipe

A bridge needs three control-socket methods, all documented in
[control-protocol.md](control-protocol.md) and [workflow.md](workflow.md):

```text
1. wake      workflow.wait    — long-poll until the workflow leaves its prior status
2. observe   workflow.status / workflow.inspect
             → activation parked awaiting_gate, pending gate definitions
3. decide    workflow.command { op: "gate" }
             → durable GateInstance with actor, reason, evidence
```

### 1. Wake

Call `workflow.wait` with a timeout. It blocks until the workflow status changes
or the timeout expires. On `awaiting_gate`, inspect.

```json
{"jsonrpc":"2.0","id":1,"method":"workflow.wait","params":{"workflow_id":"wf_...","timeout_ms":30000}}
```

### 2. Observe

`workflow.status` returns the workflow snapshot summary; `workflow.inspect` returns
full instance detail, including each parked activation and its unsatisfied gates.
The bridge reads the pending gate's `id`, `name`, `type` (`human`), `subject_type`,
and `allowed_outcomes`, plus the subject bound to the activation (for example, the
exact PR head that merge authorization applies to). This is the content the bridge
renders for the human — see the "composing the question" guidance in
[workflow.md](workflow.md#gates): the human must be able to decide from the
message alone.

### 3. Decide

Record the decision through `workflow.command` with the `gate` op
([request shape](https://github.com/sdougbrown/avenor/blob/main/internal/workflow/gate_handlers.go)):

```json
{"op":"gate","node_id":"merge-auth","gate_id":"merge-authorization",
 "activation_id":"act_...","operation":"satisfy","actor":"austin@slack",
 "reason":"Reviewed the diff in Slack; approved",
 "subject":{"type":"pull_request","repository":"org/repo","pull_request":123,"revision":"<head-sha>"},
 "response_hash":"<opaque idempotency hash of the transport response>",
 "source":"slack"}
```

Required context differs per operation — `satisfy`, `reject`, and `waive` need
`actor`, `reason`, and `subject` (when the gate declares a `subject_type`);
`external_result` needs `poll_id`, `source`, `result`, and `observed_at`. The
kernel validates and records the decision as a durable, append-only
`GateInstance`; the activation follows its declared branch.

## Conventions for remote decisions

- **Actor identity.** `actor` should name the deciding human in the transport's
  namespace (e.g. a Slack member ID or email). The kernel does not resolve it;
  it records it for audit.
- **Evidence.** Put the transport's immutable pointer in `reason` or attach
  evidence — a Slack message timestamp and thread ID, a ticket link. Gate history
  is append-only and is the audit record; the transport must not be the only
  place the decision lives.
- **Idempotency.** Use `response_hash` to carry the transport's response identity
  so a retried delivery cannot double-apply. The kernel's own command
  idempotency additionally protects against duplicate commands.
- **Expiry.** The kernel never satisfies a gate by silence. A transport that
  renders deadlines (buttons expiring, threads closing) maps naturally: expiry
  is the transport declining to decide, and the activation stays parked.
- **Exact-head binding.** When the gate declares `subject_type`, the decision
  binds to the immutable subject recorded on the activation. A new head creates
  a new activation and a new gate instance; a prior decision never carries over.
  A bridge must read the subject from `workflow.inspect` per activation, never
  cache it across activations.

## Trust boundary

The bridge speaks the same control-socket commands a local human would. All of
the kernel's existing properties hold unchanged:

- A `human` gate cannot be satisfied by silence, a successful run, a green PR,
  or an agent-authored marker.
- `waive` is authority-gated; a bridge should never waive on a human's behalf.
- The bridge holds whatever credential reaches the stable control socket —
  protect it like the terminal it stands in for.

## What this is not

- **Not a machine-observation path.** External machine facts (CI, review bots)
  belong to external gates and, later, the workflow controller's trusted CLI
  adapters (see the controller plan). Adapters cannot satisfy human gates; a
  bridge never needs to, because it speaks the human command path with a human
  actor.
- **Not a notifier.** Avenor emits workflow events (`workflow.event.gate`,
  `workflow.event.transition`) on its existing event stream; a bridge may tail
  those instead of long-polling `workflow.wait`. The kernel pushes nothing.
