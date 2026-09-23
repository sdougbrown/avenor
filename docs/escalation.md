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
1. wake      workflow.wait    — tick: returns on a terminal status or timeout
2. observe   workflow.inspect → activations parked awaiting_gate
3. decide    workflow.command { op: "gate" }
             → durable GateInstance with actor, reason, evidence, subject
```

### 1. Wake

Call `workflow.wait` with a timeout. It returns when the workflow reaches a
terminal status (`completed`, `failed`, or `canceled`) or when the timeout
expires, and its result carries top-level `terminal` and `timed_out` flags
beside the full inspect map.

```json
{"jsonrpc":"2.0","id":1,"method":"workflow.wait","params":{"workflow_id":"wf_...","timeout_ms":30000}}
```

Parking is an activation-level state: a gate parking an activation does **not**
move the workflow status off `active`, so a parked gate never wakes `wait`
early. A bridge therefore treats every wait return as a tick: on `terminal`,
exit; otherwise inspect for parked activations and repeat.

### 2. Observe

`workflow.status` returns the compact summary; `workflow.inspect` returns full
instance detail with flat `activations`, `gates`, `evidence`, and `outputs`
lists. An activation parked on its gates has `status: "awaiting_gate"`.

Inspect carries no template definitions and a `GateInstance` is recorded only
when a decision lands, so the bridge derives the pending human gates from the
parked activations plus the gate definitions loaded out-of-band from the
template the workflow was instantiated from (`id`, `name`, `subject_type`,
`allowed_outcomes`), skipping gates with a `passed`/`waived` instance for that
activation. The activation's `selected_outcome` and the instance's latest
`outputs` are the decision context — this is the content the bridge renders
for the human, see the "composing the question" guidance in
[workflow.md](workflow.md#gates): the human must be able to decide from the
message alone.

### 3. Decide

Record the decision through `workflow.command` with the `gate` op
([request shape](https://github.com/sdougbrown/avenor/blob/main/internal/workflow/gate_handlers.go)):

```json
{"op":"gate","node_id":"merge-auth","gate_id":"merge-authorization",
 "activation_id":"act_...","operation":"satisfy","actor":"austin@slack",
 "reason":"Reviewed the diff in Slack; approved [evidence: <thread permalink>]",
 "evidence_ids":["ev_bridge_<hash of the transport response>"],
 "subject":{"type":"pull_request","repository":"org/repo","pull_request":123,"revision":"<head-sha>"},
 "response_hash":"<opaque idempotency hash of the transport response>",
 "source":"slack"}
```

Required context differs per operation — `satisfy`, `reject`, and `waive` need
`actor`, `reason`, at least one `evidence_id`, and `subject` (when the gate
declares a `subject_type`); `external_result` needs `poll_id`, `source`,
`result`, `observed_at`, `subject`, `response_hash`, and at least one
`evidence_id`. The kernel rejects any decision missing these before it mutates
state. The kernel validates and records the decision as a durable, append-only
`GateInstance`; the activation follows its declared branch.

## Conventions for remote decisions

- **Actor identity.** `actor` should name the deciding human in the transport's
  namespace (e.g. a Slack member ID or email). The kernel does not resolve it;
  it records it for audit.
- **Evidence.** The kernel requires at least one `evidence_id` on every human
  decision but does not resolve the id against the instance's evidence store —
  a bridge records a stable, derived id (the reference bridge hashes the
  decision payload) and puts the transport's immutable pointer in `reason` — a
  Slack message timestamp and thread ID, a ticket link. Gate history is
  append-only and is the audit record; the transport must not be the only
  place the decision lives.
- **Idempotency.** The kernel's dedup key for a human decision is
  `gate-<operation>-<gate_id>-<activation_id>`, so a re-issued decision for the
  same operation on the same gate is a safe no-op — that, not `response_hash`,
  is what protects against double-apply. Record `response_hash` anyway: it is
  stored on the gate instance for audit and lets the transport correlate a
  retried delivery with the decision already recorded.
- **Expiry.** The kernel never satisfies a gate by silence. A transport that
  renders deadlines (buttons expiring, threads closing) maps naturally: expiry
  is the transport declining to decide, and the activation stays parked.
- **Exact-head binding.** When the gate declares `subject_type`, the
  decision-maker supplies the subject; the kernel only requires that it be
  present with a non-empty `type` — it does not compare the type against the
  declaration, so a careful bridge enforces that match itself (the reference
  bridge rejects a decision whose subject type differs from the gate's
  declaration). Ground the subject in the workflow's own recorded outputs (for
  example, the repository, pull request, and exact head SHA a publish node
  emitted), never in transport-side state. A new head creates a new activation
  and a new gate instance; a prior decision never carries over, and a bridge
  must never cache a subject across activations.

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
