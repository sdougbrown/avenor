# Software factory work template

A durable [Avenor workflow](../../docs/workflow.md) template for one review
unit of software-factory work: intake, assessment, plan drafting, hardening,
execution, verification, publication, and exact-head CI + external review with
declared review branches.

This is **one workflow for one review unit**. Campaign coordination —
scheduling, indexing, and grouping many issues — is a separate concern and is
**out of scope** for the workflow kernel. For a stack of review units, the
planning workflow emits a typed immutable topology and an explicit caller
instantiates a bounded parent with declared review-unit child workflows before
execution starts.

## The flow

```text
intake                         [manual]
  → assessment                 [run]
  → draft-plan                 [run]
  → hardening                  [run]
  → execution                  [loop]
  → verification               [team]   tests + independent review
  → publication                [run]    outputs the exact PR head
  → review                     [external]  CI + external review, exact-head gates
      clean                    → merge-auth [manual, exact-head human gate]
                                 → reconciliation [run] → merged
      changes_requested        → correction [run]
      action_required          → correction [run]
                                 → reverify [team] → publication → review
      replan                   → assessment (re-assess, re-plan, re-harden)
      checkpoint               → advisor [manual checkpoint gate]
```

The dependency edges declare the primary path; the declared branches declare
the review outcomes. Branches may point back to earlier nodes (correction,
replan) — only the ordinary dependency graph must be acyclic.

## Layout

```text
templates/software-factory/
  work.json              the workflow template (validates against the profile schema)
  prompts/               prompt fixtures for the run nodes
    assessment.md
    draft-plan.md
    hardening.md
    correction.md
    publication.md
    reconciliation.md
  loops/
    execution.json       loop config for the execution node
  teams/
    verification.json    team config for the verification node
    reverify.json        team config for the reverify node
```

The template's `prompt_file`, `loop_file`, and `team_file` references are
resolved relative to the stable supervisor's working directory at dispatch
time. Run the supervisor with this directory as its working directory (or copy
the fixtures and adjust the paths).

## Using the template

All commands go through the stable supervisor's control socket. Start the
supervisor first:

```sh
avenor stable --workflow-root /path/to/workflows
```

### 1. Create and instantiate

```sh
# Register the versioned template.
avenor workflow create --socket /path/to/socket \
  --request-file templates/software-factory/work.json

# Instantiate one work unit. The instance metadata is free-form.
echo '{"metadata":{"issue":"115","base_sha":"6e77a0d"}}' > /tmp/instance.json
avenor workflow instantiate --socket /path/to/socket \
  --template-id software-factory-work --template-version 1.0.0 \
  --request-file /tmp/instance.json
```

Instantiation returns the `workflow_id`. Every later command takes it.

### 2. Drive a node: claim → start → complete

There is no automatic scheduler. The controller explicitly claims a ready node
and starts its declared action.

```sh
# Claim the intake node (returns lease_id, owner_token, expiry).
# The activation id comes from `avenor workflow status` or `inspect`.
```

`claim` and `start` are control-plane commands (they are not CLI subcommands;
send them through the `workflow.command` control method or the MCP tools).
`complete`, `gate`, `skip`, and `unblock` are CLI subcommands:

```sh
# Complete a run node with evidence and a declared outcome.
avenor workflow complete --socket /path/to/socket \
  <workflow-id> assessment \
  --activation-id <act> --attempt-id <attempt> --lease-id <lease> \
  --request-file /tmp/complete-assessment.json
```

A complete request file carries `owner_token`, `outcome`, `outputs`, and
`artifacts`. The `outcome` must be a declared branch key or a template
terminal outcome — undeclared outcomes are rejected.

### 3. Resolve the review gates

The `review` node is an `external` action with two required external gates
(`ci` and `review-verdict`), both bound to an exact pull-request subject.
After the node completes with a declared outcome, it parks `awaiting_gate`.
Record each observed external result with `external_result`:

```sh
avenor workflow gate --socket /path/to/socket \
  <workflow-id> review ci \
  --activation-id <act> --operation external_result \
  --request-file /tmp/ci-result.json
```

An `external_result` request file carries `poll_id`, `source`, `observed_at`,
`result` (`pending`, `passed`, `failed`, `action_required`, or
`changes_requested`), `subject` (the exact PR + head SHA), `response_hash`, and
`evidence_ids`. When every required gate resolves, the activation follows the
declared branch for the completed outcome.

The `merge-auth` node's human gate requires an explicit actor, reason,
evidence, and the exact subject:

```sh
avenor workflow gate --socket /path/to/socket \
  <workflow-id> merge-auth merge-authorization \
  --activation-id <act> --operation satisfy \
  --request-file /tmp/merge-auth.json
```

### 4. Observe

```sh
avenor workflow status  --socket /path/to/socket <workflow-id>
avenor workflow inspect --socket /path/to/socket <workflow-id>
avenor workflow events  --socket /path/to/socket <workflow-id> --after-seq 0 --limit 50
avenor workflow wait    --socket /path/to/socket <workflow-id> --timeout 5m
```

## Resume, inspect, and reconcile without coordinator memory

The workflow is durable: its authoritative state is the file-backed store
(`workflow.json` snapshot + `events.ndjson` log) under the workflow root, not
coordinator memory. A work unit can be:

- **Resumed** after a supervisor restart. On startup the catalog replays events
  after the snapshot revision, expires only leases whose persisted expiry has
  passed, and re-arms `awaiting_child` compositions. A stalled attempt expires
  and a replacement worker can claim the node.
- **Inspected** at any time with `status`, `inspect`, and `events`. The
  generated `workflow.md`, `execution.md`, and gate projections are
  deterministic reads of the store.
- **Reviewed** through the exact-head gates. CI, review, and merge
  authorization bind to an immutable revision (repository, pull request, head
  SHA). A new head invalidates the prior gate instance; history is immutable.
- **Reconciled** through explicit commands and evidence. Human-supplied plans
  and waived gates are represented as explicit `workflow.complete` /
  `workflow.gate` commands with evidence — never inferred from files.

## Compatibility discipline

- Human-supplied plans and waived gates are **explicit workflow commands +
  evidence**, not files the kernel infers.
- Campaign coordination (scheduling, indexing, merge trains) stays **outside
  the kernel**. This template is one review unit.
- The template is additive: it exercises the existing kernel (schema, reducer,
  store, manager, composition, evidence, gates, heartbeat/recovery) and adds no
  kernel functionality.
