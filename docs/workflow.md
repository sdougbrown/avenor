# Avenor: Durable Workflows

A **workflow** gives Avenor durable control-plane ownership of one composable
piece of work. A workflow records declared flow, activations, execution
attempts, evidence, gates, typed outputs, and completion. It survives agent,
runtime, and supervisor replacement without reconstructing progress from agent
memory or agent-edited files.

A workflow is **one review unit of work**. Campaign coordination — scheduling,
indexing, and grouping many issues — is a separate concern and is **not part of
the kernel** (see [Boundaries](#boundaries)).

A workflow is a *declared flow*, not only a DAG retry machine. Templates
declare completion and gate outcomes plus explicit branch targets. A retry
creates another attempt for the same activation and consumes only that
activation's retry budget; it cannot select an outcome. A semantic transition
follows a declared outcome and creates a new activation of its target.

## When you need a workflow

You need a workflow when a piece of work has distinct, reviewable stages that
must survive interruption and be reconciled by evidence rather than by a
coordinator's memory:

- A multi-stage pipeline (assess → plan → execute → review → publish) where a
  crash or supervisor restart must not lose progress.
- Work with **gates** — CI, external review, human merge authorization — that
  must bind to an exact, immutable revision.
- Work with **declared review branches** (clean, changes requested, replan,
  checkpoint) that a controller drives explicitly.

A single `avenor run`, [loop](loop.md), or [team](team.md) is enough for one
task without durable cross-stage state. A workflow adds the durable store,
composition, and acceptance semantics on top.

## Quick start

The shipped software-factory work template at
`templates/software-factory/work.json` is a complete, validated
example. The minimal path:

```sh
# 1. Start a stable supervisor with a workflow root.
avenor stable --workflow-root /path/to/workflows

# 2. Register the versioned template.
avenor workflow create --socket /path/to/socket \
  --request-file templates/software-factory/work.json

# 3. Instantiate one work unit.
echo '{"metadata":{"issue":"115"}}' > /tmp/instance.json
avenor workflow instantiate --socket /path/to/socket \
  --template-id software-factory-work --template-version 1.0.0 \
  --request-file /tmp/instance.json
# → { "workflow_id": "wf_...", "revision": 1 }

# 4. Observe.
avenor workflow status --socket /path/to/socket wf_...
```

Then drive nodes with `claim` → `start` → `complete` (or `gate`), as described
in [Driving a workflow](#driving-a-workflow).

## The durable layout

The workflow root separates reusable templates from instantiated workflows:

```text
<workflow-root>/
  templates/<template-id>/<version>.json
  instances/<workflow-id>/
    workflow.json             # authoritative materialized snapshot
    events.ndjson             # append-only workflow history and recovery source
    workflow.md               # generated human-readable projection
    evidence/<evidence-id>/   # immutable copies of accepted evidence artifacts
    nodes/<node-id>/
      execution.md            # generated attempt/evidence projection
      review-1.md             # generated gate projection when applicable
```

`workflow.json` is the authoritative snapshot. `events.ndjson` is the
append-only history and recovery source. The `.md` files are **generated
projections**, not agent-authored inputs. The store applies each command under
one POSIX file lock: load the snapshot, validate the command, append the event
batch (fsync), atomically replace the snapshot, and regenerate projections. A
crash after the event append but before the snapshot replace is recovered by
replay.

The default workflow root is `$XDG_STATE_HOME/avenor/workflows`, falling back
to `$HOME/.avenor/workflows`. Override it with `--workflow-root` on `avenor
stable`.

## Templates

A template is a versioned, reusable definition. It validates against the
Umpire JSON Schema Composition Profile (`schemas/workflow.profile.json`,
`schema_version: 1`). The top-level
fields:

| Field | Required | Description |
|---|---|---|
| `schema_version` | yes | Must be `1`. |
| `template_id` | yes | Stable identifier. |
| `template_version` | yes | Version string. |
| `metadata` | no | Free-form object. |
| `entry_nodes` | yes | Node IDs that activate at instantiation. |
| `nodes` | yes | Node definitions. |
| `terminal_outcomes` | yes | Template-global terminal outcome vocabulary. |
| `params` | no | Declared instance parameters: `[{id, type, required?}]`. Only `type: "string"` exists; ids are path-safe identifiers. |
| `bounded_loops` | no | Explicit bounded loop constructs. |
| `default_lease_policy` | no | `{ttl_seconds, heartbeat_interval_seconds}`. |
| `default_retry_policy` | no | `{max_attempts, exhaustion, outcome}`. |
| `composition_limits` | no | `{max_depth, max_children}` for child workflows. |

### Node definition

| Field | Description |
|---|---|
| `id` | Stable node ID (a safe path component). |
| `name` | Display name. |
| `dependencies` | Node IDs that must be satisfied first (the ordinary, acyclic flow). |
| `outcomes` | Declared outcomes: `{name, target_node_id?, terminal?}`. |
| `branches` | Map of outcome name → target node ID. The branch map is itself the declaration of the node's branch outcomes. |
| `action` | The execution action (see [Action types](#action-types)). |
| `assignment` | Role / roster / backend / agent / model / thinking selection. Automatic dispatch resolves it through the roster path and pins the effective selection on the activation. |
| `completion` | Completion contract (see [Completion](#completion)). |
| `outputs` | Declared typed outputs: `{id, name, type, required?}`. |
| `gates` | Declared gates: `{id, name?, type, required?, allowed_outcomes?, subject_type?}`. |
| `dispatch` | Dispatch policy (see [Dispatch policy](#dispatch-policy)). |
| `retry_policy` | Per-node retry override. |
| `loop_id` | Bounded-loop membership. |
| `checkpoint` | Checkpoint definition for a bounded loop. |
| `lease_policy` | Per-node lease override. |
| `skip_rule` | Authority rule for `workflow.skip`. |
| `waive_rules` | Authority rules for `workflow.gate` `waive`. |

Ordinary `dependencies` must be acyclic. A cycle is valid only through an
explicit bounded loop or through declared **branches**, which create new
activations and may point back to earlier nodes.

### Action types

A node's `action` is a tagged union. Exactly one variant:

| Type | Fields | Behavior |
|---|---|---|
| `run` | `prompt` **or** `prompt_file` | Dispatches a single direct run. |
| `loop` | `loop_file` | Dispatches an existing [loop](loop.md) config. |
| `team` | `team_file` | Dispatches an existing [team](team.md) config. |
| `manual` | `instructions?` | Parks awaiting an explicit `workflow.complete`. No runtime. |
| `external` | `source`, `subject_type?` | Parks awaiting an explicit `workflow.complete` + external gate results. No runtime. |
| `workflow` | `template_id`, `template_version`, `child_key`, `input_bindings?`, `output_bindings?`, `outcome_map` | Invokes a declared child workflow (see [Composition](#composition)). |

`run`, `loop`, and `team` are provider-backed: `workflow.start` dispatches them
through a registered executor and records the attempt's terminal facts.
`manual` and `external` create an attempt with no runtime and require an
explicit `workflow.complete`. File references (`prompt_file`, `loop_file`,
`team_file`) resolve relative to the stable supervisor's working directory at
dispatch time.

### Completion

A node's `completion` contract selects a safe machine evaluator:

| Kind | Waits for terminal output | Description |
|---|---|---|
| `explicit` | no | An explicit worker handoff; may precede the attempt's termination. |
| `files` | yes | Declares artifact requirements; completion waits for the attempt's terminal fact. |
| `git` | yes | Declares a git requirement (`clean`, `head`, `changed_from_base`); waits for the terminal fact. |

The contract's `artifacts` and `git` fields are **declarations** of what the
controller should supply and verify; the `workflow.complete` command stages and
hashes the supplied artifacts and validates the declared outputs and outcome.
A `files` or `git` contract cannot complete before the attempt reaches a
terminal status.

### Outputs

Each node declares typed outputs. The `workflow.complete` command supplies
values for the declared output definitions. Output types: `string`, `number`,
`boolean`, `json`, `file`. A `file` output references the staged artifact
evidence. Output values are append-only revisions: a later authorized
activation can produce a new revision without mutating prior facts.

### Dispatch policy

A node opts into automatic dispatch with a `dispatch` object in its node
definition. A node without a `dispatch` object is manual: it is claimed and
started explicitly, and ordinary `avenor_spawn` behavior is unaffected.

| Field | Description |
|---|---|
| `mode` | `manual` (default) or `auto`. |
| `controller_id` | The controller that dispatches the node. Required when `mode` is `auto`; forbidden on manual nodes. |
| `priority` | 0–100, default 50. The controller's dispatcher orders ready candidates by priority, aging a candidate by the whole minutes it has waited. |
| `concurrency_key` | Optional; must be non-blank. While a live attempt holds the key, the node is blocked. |
| `success_outcome` | External nodes only. Names the declared branch an auto external node follows once every required external gate has passed; required exactly when an external node is `auto`. |

`mode: auto` is limited to `run`, `loop`, and `team` nodes, and to `external`
nodes that declare a `success_outcome` with at least one required external
gate.

## Instantiation

`avenor workflow instantiate` creates an instance of a registered template
version. It returns the `workflow_id` and initial revision. Instantiation
creates activations for the declared entry nodes and records
`workflow.instantiated`.

An instantiate request may carry a `params` object satisfying the template's
declared `params`: unknown names are rejected, required names must be
present, and values must be non-empty strings of at most 256 characters
without control characters. Params are recorded on the instance and are
immutable facts of it.

A node's `dispatch.concurrency_key` is either a non-empty string or
`{"prefix": "...", "from_instance_param": "..."}` naming a declared param.
The templated form resolves at activation creation — the optional prefix
followed by the instance's recorded param value — and freezes on the
activation as a plain string, so candidate views, held-key serialization,
and manual starts all read the same resolved key. Replay reproduces it from
the recorded params. Templates whose keys are plain strings and that declare
no params behave exactly as before.

For a template with a `workflow` (child) action, instantiation idempotently
creates each pinned child and freezes the composition manifest before the
parent can execute. The parent's workflow action may pass instance params to
the child explicitly with `params` bindings — each binding supplies either a
literal `value` or the parent's `from_instance_param`; nothing is inherited
implicitly.

## Driving a workflow

By default there is no automatic scheduler: a controller or agent explicitly
claims a ready node and starts its declared action. Nodes that opt in with a
declared `dispatch` policy are instead dispatched automatically by the
optional [workflow controller](workflow-controller.md) — see the [workflow
controller example](workflow-controller.md). The activation lifecycle:

```text
pending → ready → leased → running
       ├→ skipped       ├→ attempt_failed
       │                ├→ awaiting_completion
       │                ├→ awaiting_gate
       │                ├→ blocked
       │                └→ lease_expired
awaiting_gate → satisfied | rejected | declared branch
blocked ──workflow.unblock──> ready
```

Workflow status is one of `active`, `blocked`, `awaiting_gate`, `completed`,
`failed`, or `canceled`.

### Commands

The CLI exposes these subcommands (all take `--socket`):

| Command | Purpose |
|---|---|
| `create` | Register a versioned template. |
| `instantiate` | Create an instance of a template version. |
| `status` | Current workflow snapshot summary. |
| `wait` | Block until terminal or `--timeout`. |
| `inspect` | Full instance detail. |
| `events` | Event log, `--after-seq` / `--limit`. |
| `complete` | Complete a machine/external handoff activation. |
| `gate` | Record a gate decision. |
| `skip` | Waive every unsatisfied required gate on a parked activation. |
| `unblock` | Return a blocked activation to ready. |

`claim`, `start`, and `heartbeat` are control-plane commands, not CLI
subcommands. Send them through the `workflow.command` control method (see
[Control protocol](control-protocol.md#workflow-methods)) with an `op`
discriminator:

```json
{"jsonrpc":"2.0","id":1,"method":"workflow.command",
 "params":{"workflow_id":"wf_...","command":{"op":"claim","node_id":"intake","activation_id":"act_...","actor":"controller"}}}
```

`claim` returns `lease_id`, `owner_token`, and `expiry`. `start` validates the
lease, allocates an `attempt_id`, and dispatches the declared action.
`heartbeat` renews the lease with the claim's `(lease_id, owner_token)` pair.

### The claim → start → complete cycle

For a provider-backed node (`run`/`loop`/`team`):

```sh
# 1. Claim (control method) → lease_id, owner_token.
# 2. Start (control method) → attempt_id. The executor dispatches the run.
# 3. Wait for the attempt's terminal fact (the run ends).
# 4. Complete (CLI) with evidence and a declared outcome.
avenor workflow complete --socket /path/to/socket \
  wf_... assessment \
  --activation-id act_... --attempt-id att_... --lease-id lease_... \
  --request-file /tmp/complete.json
```

A complete request file carries `owner_token`, `outcome`, `outputs`, and
`artifacts`:

```json
{
  "owner_token": "<from claim>",
  "outcome": "ready",
  "outputs": [
    { "definition_id": "assessment", "value": null }
  ],
  "artifacts": [
    { "src_path": "/abs/path/assessment.md", "stored_path": "assessment.md",
      "non_empty": true, "sha256": "<sha256>" }
  ]
}
```

The `outcome` must be a declared branch key or a template terminal outcome —
undeclared outcomes are rejected. If the node declares required gates that are
unsatisfied, the activation parks `awaiting_gate` instead of following the
branch.

For a `manual` or `external` node, `start` returns `requires_complete: true`
and the activation is `running` with no runtime. Complete it directly with the
evidence and outcome.

## Gates

A gate is a named, typed decision point. Gate types: `machine`, `external`,
`human`. A gate cannot be satisfied by silence, a successful run, a green PR,
or an agent-authored marker.

For human gates, the deciding human does not have to be at the terminal: an
external bridge can carry the question to Slack or any other transport and
record the attributed answer through the ordinary gate command path. See
[escalation.md](escalation.md) and the reference bridge in
[`templates/escalation-bridge/`](https://github.com/sdougbrown/avenor/tree/main/templates/escalation-bridge).

The `workflow.gate` command records a decision with a closed operation enum:

| Operation | Requires | Effect |
|---|---|---|
| `satisfy` | actor, reason, evidence, subject (when declared) | Passes the gate. |
| `reject` | actor, reason, evidence, subject (when declared) | Rejects; may follow a declared failure/correction branch. |
| `waive` | actor, reason, evidence, subject (when declared) | Waives the gate (authority-gated). |
| `external_result` | poll_id, source, observed_at, result, subject, response_hash, evidence | Records an observed external result. |

An `external_result` `result` is one of `pending`, `passed`, `failed`,
`action_required`, or `changes_requested`. `pending`, `action_required`, and
`changes_requested` keep the activation parked; `passed` resolves it (following
the declared branch); `failed` rejects it.

### Exact-head binding

An external subject is typed and includes its immutable revision:

```json
{ "type": "pull_request", "repository": "sdougbrown/avenor",
  "pull_request": 123, "revision": "<head-sha>" }
```

CI, reviewer evidence, and merge authorization bind to the subject. Gate
instances are scoped to their activation and the gate history is append-only:
a new head creates a new activation with its own gate instances, so a prior
head's gate decision no longer applies, and the prior instances are retained
unchanged. A human merge-authorization gate requires an explicit actor, reason,
evidence, and the exact subject — it cannot be satisfied by PR state alone.

### Bound gate subjects and inputs

A gate can bind its decision to a subject and to late-bound adapter inputs,
both resolved from the workflow's own recorded outputs rather than supplied at
decision time.

- **`subject_binding`.** Declared on `external` and `human` gates (forbidden on
  `machine` gates). Its `type` is `pull_request`, and its `repository`,
  `pull_request`, and `revision` fields each name a `from_node_output`
  reference to a transitive dependency's declared output (string, number, and
  string respectively); all three must resolve from the same source node. When
  the activation is created, the kernel resolves the references along the
  causal provenance chain and pins the resulting `subject` onto the
  activation; a decision on a bound gate must carry a subject equal to the
  pinned subject on all four fields.
- **`inputs`.** Declared only on `external` gates. Each entry is a strict
  primitive literal (string, number, or boolean) or a `from_node_output`
  reference to a transitive dependency's declared primitive output. The kernel
  pins each reference to the exact causal source activation, output, and
  revision.
- **`adapter_id`.** Names the trusted external adapter that polls the gate.
  Declared only on `external` gates.
- **`result_outcomes`.** Routes advisory external results to the node's
  declared branch outcomes. Declared only on bound `external` gates; `passed`
  is excluded (a pass always follows the node's dispatch `success_outcome`).
  The routable keys are `failed`, `action_required`, and `changes_requested`.
- **`dispatch.success_outcome`.** The declared branch an auto external node
  follows on a pass. An external node may dispatch automatically only when it
  declares no outputs, names a declared `success_outcome` branch, carries at
  least one required external gate, has no human or machine gates, and every
  required gate declares a `subject_binding`.
- **`resolved_gates` in inspect.** Each activation carries `resolved_gates`,
  keyed by gate id, recording the pinned `inputs` and the resolved `subject`
  (or the `unresolved` fields when a reference could not resolve). A bound
  gate with an unresolved pin cannot be decided yet.
- **`poll_id` idempotency.** A bound `external_result` is keyed by `poll_id`:
  a re-reported poll with the same `poll_id` and `response_hash` is an
  idempotent replay, while the same `poll_id` with a different hash is a
  replay conflict. Unbound gates keep the legacy result-discriminated key.

## Composition

A `workflow` action names a pinned child template version and a typed
input/output contract. The parent instantiation idempotently creates each
declared child and freezes an immutable composition manifest. When the node
activates, the local action attaches or resumes the child and enters durable
`awaiting_child` state. The parent records the child workflow ID, pinned
template/version, action activation, and selected output references. Child
terminal outcomes map only through the parent's declared `outcome_map`.

Composition is kernel-owned and bounded by `composition_limits`
(`max_depth`, `max_children`). It supports logical PR stacks and work
decomposition **without** adding campaign grouping, campaign scheduling, or
cross-workflow inference to the workflow resource. A planning workflow can emit
a typed immutable topology; an explicit caller then instantiates the bounded
parent/child composition before execution starts.

## Evidence

Evidence is an immutable record: an ID, kind, source (`machine`, `agent`,
`human`, or `external`), authority/actor, the original artifact reference, a
copied artifact reference inside the workflow store, path, size, SHA-256, a
structured result, a timestamp, and the activation identity plus exact external
subject when applicable.

The store copies an evidenced artifact into an instance-owned evidence
directory before recording the evidence event (a same-filesystem hard link is
an optional optimization; the copied artifact is the portability fallback).
Completion fails if the artifact cannot be copied and hashed. Agents can
submit evidence; they cannot edit the workflow snapshot, event log, or
generated projections.

## Heartbeat, recovery, and retry

A lease is live only while it is heartbeated. `workflow.heartbeat` renews the
activation's active lease for the claim's `(lease_id, owner_token)` pair.
Runtime activity can extend an observed activity timestamp but cannot replace
the required explicit heartbeat for lease liveness.

On supervisor restart, the catalog replays events after the snapshot revision,
expires **only** leases whose persisted expiry has passed (appending
`workflow.lease.expired` with reason `recovery`), and re-arms `awaiting_child`
compositions. A valid lease is not expired merely because the supervisor
restarted. A stalled attempt expires and a replacement worker can claim the
node.

A retry retains the activation and loop iteration, consumes only the retry
budget, and cannot follow a semantic branch or checkpoint. Exhaustion applies
only the configured `block`, operational `fail`, or named `outcome`.

## Resume, inspect, review, and reconcile without coordinator memory

The workflow is durable: its authoritative state is the file-backed store, not
coordinator memory. One work workflow can be:

- **Resumed** after a supervisor restart. Replay reconstructs state from
  `events.ndjson`; expired leases are swept; `awaiting_child` compositions are
  re-attached. A stalled attempt expires and a replacement worker claims the
  node. No coordinator global is consulted.
- **Inspected** at any time with `status`, `inspect`, and `events`. The
  generated `workflow.md`, `execution.md`, and gate projections are
  deterministic reads of the store.
- **Reviewed** through the exact-head gates. CI, review, and merge
  authorization bind to an immutable revision. Gate instances are scoped to
  their activation and the gate history is append-only, so a new head's gate
  decision is distinct from a prior head's.
- **Reconciled** through explicit commands and evidence. Human-supplied plans
  and waived gates are represented as explicit `workflow.complete` /
  `workflow.gate` commands with evidence — never inferred from files.

The coordinator's context is working memory, not workflow state. The store is
the source of truth.

## Boundaries

The workflow kernel owns durable state, composition, and acceptance semantics
for **one** work unit. It deliberately does **not** own:

- **Campaign coordination** — grouping, indexing, scheduling, or merge trains
  across many issues. That is a separate concern (the software-factory
  campaign record) and stays outside the kernel.
- Automatic scheduling beyond the optional workflow controller's dispatch of
  explicitly `auto`-declared nodes (see [Workflow controller
  example](workflow-controller.md)).
- Arbitrary dynamic graph mutation, unbounded loops, or unbounded child
  recursion.
- Automatic merge or merge authorization based on PR state alone.
- Agent-authored state files as an acceptance mechanism.

For a stack of review units, the planning workflow ends with a typed immutable
topology; an explicit caller instantiates a bounded parent with declared
review-unit child workflows before execution. Each child owns its state, review
loop, evidence, and gates. The parent owns composition and typed handoff only.
The shipped software-factory templates demonstrate the whole shape — dispatch
policy, worktree concurrency keys, trusted-adapter gates, and the stack
parent — in [the workflow controller example](workflow-controller.md).

## Kitchen-sink example

The software-factory work template at `templates/software-factory/work.json`
is a complete, validated kitchen-sink example: a manual intake, three `run`
planning nodes, a `loop` execution, two `team` verification nodes, a `run`
publication that outputs the exact PR head, an `external` review node with two
exact-head gates, a manual merge-authorization gate, a manual advisor
checkpoint, and a `run` reconciliation — with the declared review branches
(clean, changes_requested/action_required, replan, checkpoint). Its
`templates/software-factory/README.md` walks through creating, instantiating,
driving, and reconciling it.

## Related documentation

- [CLI reference](cli.md) — `avenor workflow` subcommands
- [Control protocol](control-protocol.md) — `workflow.*` control methods
- [MCP](mcp.md) — `avenor_workflow_*` tools
- [Loop](loop.md) — `loop` action configs
- [Team](team.md) — `team` action configs
- [Stable](stable.md) — the supervisor that hosts the workflow manager
