# Software factory work templates

Durable [Avenor workflow](../../docs/workflow.md) templates for
software-factory work:

- `work.json` (`software-factory-work@1.2.0`) — one review unit: intake,
  assessment, plan drafting, hardening, execution, verification, publication,
  and exact-head CI + external review with declared review branches. It
  declares one required instance param, `worktree`, and every keyed node
  resolves its concurrency key from that param at instantiation.
- `stack.json` (`software-factory-stack@1.1.0`) — a bounded parent whose
  planning node records a typed, immutable stack topology and whose
  kernel-local `workflow` actions compose review-unit children of the work
  template, passing each child its worktree param explicitly. The parent
  owns only declared composition and typed handoff.

Campaign coordination — scheduling, indexing, and grouping many issues
beyond the declared stack composition — is **out of scope** for the workflow
kernel and for the workflow controller. For anything richer, an explicit
caller instantiates additional bounded parents.

## The flow

```text
intake                         [manual]
  → assessment                 [run, auto]
  → draft-plan                 [run, auto]
  → hardening                  [run, manual — the plan checkpoint]
  → execution                  [loop, auto]
  → verification               [team, auto]   tests + independent review
  → publication                [run, auto]    outputs repository + PR head
  → review                     [external, auto-park]  CI + external review,
                               exact-head gates via trusted adapters
      clean                    → merge-auth [manual, exact-head human gate]
                                 → reconciliation [run, auto] → merged
      changes_requested        → correction [run, auto]
      action_required          → correction [run, auto]
                                 → reverify [team, auto] → publication → review
      replan                   → assessment (re-assess, re-plan, re-harden)
      checkpoint               → advisor [manual checkpoint gate]
```

Provider-backed nodes dispatch automatically under the `software-factory`
workflow controller with explicit priorities that rank later-pipeline work
(correction 70, publication/review/reverify 60) above intake (assessment and
draft-plan 30). Every node that writes the review unit's worktree carries
the templated concurrency key `{"prefix": "worktree:",
"from_instance_param": "worktree"}`, resolved from the instance's recorded
`worktree` param at instantiation: instances pinned to different worktrees
run concurrently under one controller, while two instances sharing a
worktree param serialize — one work item at a time per worktree across the
whole workflow root. Hardening is a provider-backed run with **manual**
dispatch — the plan checkpoint a human or advisor releases deliberately.
Merge authorization remains a manual human gate bound to the exact published
subject; the controller never parks it, satisfies it, or merges.

The dependency edges declare the primary path; the declared branches declare
the review outcomes. Branches may point back to earlier nodes (correction,
replan) — only the ordinary dependency graph must be acyclic.

## Layout

```text
templates/software-factory/
  work.json              the review-unit template (validates against the profile schema)
  stack.json             the bounded stack parent composing review-unit children
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
  roster.json            roster entries the node assignments resolve through
  fixtures/              example request files (controller, work items, stack)
  adapters/              example trusted-adapter manifests (placeholder executables)
```

The template's `prompt_file`, `loop_file`, `team_file`, and `roster_file`
references are resolved relative to the stable supervisor's working directory
at dispatch time. Run the supervisor with this directory as its working
directory (or copy the fixtures and adjust the paths).

## Using the template

All commands go through the stable supervisor's control socket. Start the
supervisor first:

```sh
avenor stable --workflow-root /path/to/workflows
```

### 1. Register the controller, adapters, and template

```sh
# Register the versioned template.
avenor workflow create --socket /path/to/socket \
  --request-file templates/software-factory/work.json

# Create the disabled software-factory controller (max_inflight 2), then
# enable it once at least one work item exists.
avenor workflow controller create --socket /path/to/socket \
  --request-file templates/software-factory/fixtures/controller.json
avenor workflow controller enable --socket /path/to/socket software-factory
```

Install trusted adapter manifests for the review node's bound gates first —
see `adapters/README.md` — and pass `--workflow-adapter-dir` when starting
the supervisor. See [the workflow controller example](../../docs/workflow-controller.md)
for the full walkthrough.

### 2. Instantiate one work unit per review unit

```sh
# Instantiate one work unit. The worktree param is required by the template
# and immutable once recorded; the fixtures/ directory ships examples for two
# independent work items and for the stack parent.
echo '{"params":{"worktree":"avenor-issue-115"},"metadata":{"issue":"115","base_sha":"6e77a0d"}}' > /tmp/instance.json
avenor workflow instantiate --socket /path/to/socket \
  --template-id software-factory-work --template-version 1.2.0 \
  --request-file /tmp/instance.json
```

Instantiation returns the `workflow_id`. Every later command takes it.

### 3. Drive the human nodes

The controller dispatches every auto node itself — claiming ready nodes,
starting their actions, and parking the review node — while a manual claim on
an auto node remains valid (the kernel claim is the race arbiter). Only the
manual nodes need a human:

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

### 4. Resolve the review gates

The `review` node is an `external` action with two required external gates
(`ci` and `review-verdict`), both bound to an exact pull-request subject
pinned from publication's outputs and polled through the trusted adapters.
The controller parks the node `awaiting_gate` automatically and records each
observed external result:

```sh
avenor workflow gate --socket /path/to/socket \
  <workflow-id> review ci \
  --activation-id <act> --operation external_result \
  --request-file /tmp/ci-result.json
```

An adapter result of `passed` on every required gate follows the node's
declared `success_outcome` (`clean`). Advisory and failed results route
through the gates' declared `result_outcomes`:
`changes_requested`/`action_required` to the correction branches, `failed`
to replan. A manual `external_result` command remains available for a
declared reporter.

The `merge-auth` node's human gate requires an explicit actor, reason,
evidence, and the exact subject:

```sh
avenor workflow gate --socket /path/to/socket \
  <workflow-id> merge-auth merge-authorization \
  --activation-id <act> --operation satisfy \
  --request-file /tmp/merge-auth.json
```

### 5. Observe

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
  passed, and re-arms `awaiting_child` compositions. The controller record,
  its leader lease, and its poll cursors recover from the same root; a fresh
  supervisor resumes parked reviews and provider work without coordinator
  memory. A stalled attempt expires
  and a replacement worker can claim the node.
- **Inspected** at any time with `status`, `inspect`, and `events`. The
  generated `workflow.md`, `execution.md`, and gate projections are
  deterministic reads of the store.
- **Reviewed** through the exact-head gates. CI, review, and merge
  authorization bind to an immutable revision (repository, pull request, head
  SHA). Gate instances are scoped to their activation and the gate history is
  append-only, so a new head's gate decision is distinct from a prior head's.
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
