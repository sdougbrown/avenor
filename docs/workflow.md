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

Registration is idempotent: re-creating a template with identical content
under the same `template_id` + `template_version` succeeds as a no-op. Different
content under an already-registered id+version is rejected with an idempotency
conflict error (code `-32031`), preserving the exact revision instances bind
to.

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
| `launch_wrappers` | no | Registry of trusted, author-reviewed executable configurations: `[{id, argv}]` (see [Launch wrappers](#launch-wrappers)). |
| `bounded_loops` | no | Explicit bounded loop constructs. |
| `default_lease_policy` | no | `{ttl_seconds, heartbeat_interval_seconds}`. |
| `default_retry_policy` | no | `{max_attempts, exhaustion, outcome}`. |
| `working_directory` | no | Default attempt working directory: `{"from_instance_param": "<param id>"}`. |
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
| `assignment` | Role / roster / backend / agent / model / thinking selection, plus the optional permission policy (`auto_approve`, `permission_policy.decider`, or `permission_policy.decider_model`). Automatic dispatch resolves it through the roster path and pins the effective selection on the activation. |
| `completion` | Completion contract (see [Completion](#completion)). |
| `outputs` | Declared typed outputs: `{id, name, type, required?}`. |
| `gates` | Declared gates: `{id, name?, type, required?, allowed_outcomes?, subject_type?}`. |
| `dispatch` | Dispatch policy (see [Dispatch policy](#dispatch-policy)). |
| `retry_policy` | Per-node retry override. |
| `working_directory` | Per-node attempt working directory override: `{"from_instance_param": "<param id>"}`. Takes precedence over the template default. |
| `loop_id` | Bounded-loop membership. |
| `checkpoint` | Checkpoint definition for a bounded loop. |
| `lease_policy` | Per-node lease override. |
| `skip_rule` | Authority rule for `workflow.skip`. |
| `waive_rules` | Authority rules for `workflow.gate` `waive`. |
| `launch_wrapper` | Selects an ID from the template's `launch_wrappers` registry (see [Launch wrappers](#launch-wrappers)). |

Ordinary `dependencies` must be acyclic. A cycle is valid only through an
explicit bounded loop or through declared **branches**, which create new
activations and may point back to earlier nodes.

### Action types

A node's `action` is a tagged union. Exactly one variant:

| Type | Fields | Behavior |
|---|---|---|
| `run` | `prompt` **or** `prompt_file`, `timeout?` | Dispatches a single direct run. |
| `loop` | `loop_file`, `timeout?` | Dispatches an existing [loop](loop.md) config. |
| `team` | `team_file`, `timeout?` | Dispatches an existing [team](team.md) config. |
| `manual` | `instructions?` | Parks awaiting an explicit `workflow.complete`. No runtime. |
| `external` | `source`, `subject_type?` | Parks awaiting an explicit `workflow.complete` + external gate results. No runtime. |
| `workflow` | `template_id`, `template_version`, `child_key`, `input_bindings?`, `output_bindings?`, `outcome_map` | Invokes a declared child workflow (see [Composition](#composition)). |

`run`, `loop`, and `team` are provider-backed: `workflow.start` dispatches them
through a registered executor and records the attempt's terminal facts.
`timeout` (seconds, optional) is an enforceable execution deadline for the
dispatched child: every phase attempt, retry, and nudge stops at the shared
wall-clock limit and the attempt records `timed_out`. `manual` and `external`
create an attempt with no runtime and require an
explicit `workflow.complete`. File references (`prompt_file`, `loop_file`,
`team_file`) resolve relative to the stable supervisor's working directory at
dispatch time. Renewing the attempt's lease keeps the attempt alive; it never
extends the execution deadline.

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

An auto-dispatched `run`, `loop`, or `team` node is completed by the
supervisor itself when its attempt exits successfully — the worker never
sees the owner token and files no completion. The supervisor evaluates the
declared contract against the attempt's working directory: the outcome comes
from the node's declared outcome vocabulary when it has exactly one
candidate, otherwise from the loop/team terminal marker label; the files/git
requirements and the declared output sources are checked against the work
product. A contract that is not met is recorded as a failed attempt with the
`contract_unmet` marker, so the node's retry policy and exhaustion apply.
For that reason an auto provider node must not declare `completion.kind:
explicit`, and must not declare `completion.git.changed_from_base` (no base
commit is recorded for the supervisor to compare against) — such templates
are rejected. Manual nodes and external nodes keep explicit worker/operator
handoff.

### Outputs

Each node declares typed outputs. The `workflow.complete` command supplies
values for the declared output definitions. Output types: `string`, `number`,
`boolean`, `json`, `file`. A `file` output references the staged artifact
evidence. Output values are append-only revisions: a later authorized
activation can produce a new revision without mutating prior facts.

An output may declare a `source`, which the supervisor's auto-completion
resolves against the attempt's working directory:

| Source | Resolves to |
|---|---|
| `{"artifact": "path"}` | The whole declared artifact (file outputs): the output value is the artifact path, bound to the staged evidence. |
| `{"artifact": "path", "pointer": "/json/pointer"}` | One RFC 6901 pointer location inside a JSON artifact; the resolved value must match the declared output type. |
| `{"git": "head"}` | `git rev-parse HEAD` in the attempt's working directory (string outputs). |

An `artifact` source path must be one of the node's `files` contract
artifacts, a `pointer` requires an `artifact`, and exactly one of `artifact`
or `git` may be declared. The source must fit the output type: a whole
`artifact` source requires a `file` output, a `pointer` source requires a
non-`file` output, and a `git` source requires a `string` output. Artifact
and source paths must stay inside the attempt's working directory. Without a `source`, a `file` output on a node
whose files contract declares exactly one artifact binds to that artifact.
Required outputs that do not resolve leave the contract unmet; optional
ones are omitted.

### Dispatch policy

A node opts into automatic dispatch with a `dispatch` object in its node
definition. A node without a `dispatch` object is manual: it is claimed and
started explicitly, and ordinary spawn behavior is unaffected.

| Field | Description |
|---|---|
| `mode` | `manual` (default) or `auto`. |
| `controller_id` | The controller that dispatches the node. Required when `mode` is `auto`; forbidden on manual nodes. |
| `priority` | 0–100, default 50. The controller's dispatcher orders ready candidates by priority, aging a candidate by the whole minutes it has waited. |
| `concurrency_key` | Optional; must be non-blank. While a live attempt holds the key, the node is blocked. |
| `success_outcome` | External nodes only. Names the declared branch an auto external node follows once every required external gate has passed; required exactly when an external node is `auto`. |

`mode: auto` is limited to `run`, `loop`, and `team` nodes, and to `external`
nodes that declare a `success_outcome` with at least one required external
gate. An auto `run`, `loop`, or `team` node must not declare
`completion.kind: explicit`: the supervisor completes it from the declared
contract, and no worker handoff exists.

### Permission policy

An `assignment` declares how a dispatched node's permission requests are
adjudicated. For unattended runs the judgment-gate policy is the headline:

```json
"assignment": {
  "roster_file": "roster.json",
  "roster_entry": "worker",
  "permission_policy": {"decider": "rm-guard"}
}
```

`permission_policy.decider` names a roster entry in the assignment's roster
file — the deciding agent, resolved through the same roster path as the
node's own identity and pinned on the execution selection, so retries keep
the policy. The kernel routes, it does not judge: when a dispatched node
with a declared decider hits a permission request, no file permission
handler is derived (the request cannot stall into the file handler or be
approved blindly), and the supervisor spawns the decider run for the
pending request. The decider agent — for example a static-plus-model gate
like [pi-rm-guard](https://github.com/sdougbrown/pi-rm-guard) — decides
allow or reject against the node's declared task and answers through the
ordinary control plane (the status and answer_permission tools);
anything it cannot clearly allow or deny it defers, and a deferral is a
visible pending state: the request stays pending in runtime status for a
human to answer, never a silent allow. A decider that fails to spawn also
leaves the request pending.

> **Operator caveat:** the decider run is spawned with auto-approve in the
> requesting runtime's working directory, so a compromised decider roster
> entry is an unsandboxed execution path on the node host. Scope the
> decider's agent profile at the roster/agent level — restrict its tools to
> the minimum the judgment task needs — rather than relying on the workflow
> to contain it.

`auto_approve: true` remains available as one explicit, narrowly scoped
option: every permission request is resolved as allow, and no file
permission handler is derived for the runtime. Use it only for nodes whose
requests are known-safe; the decider policy is the default answer for
unattended runs. An assignment must not declare both. Neither policy makes
a per-tool allow/deny expressible at the template level: judgment lives in
the decider agent, and `auto_approve` is all-or-nothing per node.

### Direct-model decider

`permission_policy.decider_model` is the lightweight alternative to the
harness decider: no agent run is spawned at all, and avenor itself performs
one model call per pending request and applies the answer.

```json
"assignment": {
  "permission_policy": {"decider_model": {"provider": "groq", "model": "llama-3.3-70b"}}
}
```

The policy names a provider from the user-level provider registry plus a
model on it. The registry lives at
`$XDG_CONFIG_HOME/avenor/providers.json` (default
`~/.config/avenor/providers.json`; `.yaml`/`.yml`/`.toml` variants are read
in that fallback order), and each provider declares an
OpenAI-compatible chat-completions endpoint:

```json
{
  "providers": {
    "groq": {
      "base_url": "https://api.groq.com/openai/v1",
      "api_key_env": "GROQ_API_KEY"
    },
    "local": {
      "base_url": "http://localhost:8080/v1",
      "api_key": "sk-..."
    }
  }
}
```

`api_key_env` names the environment variable holding the bearer token and
wins over a literal `api_key`; prefer it so the secret stays out of config
files. The resolved selection pins the provider, model, endpoint, and key
variable name on the activation (retries keep the policy), but never a
credential: the API key is resolved at spawn time and held only in
supervisor memory. A provider that cannot be resolved at dispatch — unknown
name, no key, invalid endpoint — fails the dispatch with a clear error
instead of stalling the first permission request.

When a pending request arrives, the supervisor builds the decision prompt
from the request's structured mirror (tool, command, working directory), the
node's task, and the pending options — all framed to the model as untrusted
worker-supplied data — and makes one bounded, non-streaming call (concurrent
calls are capped; a request that hits the cap stays pending until capacity
frees). The model answers with a strict-JSON decision:

- **allow** / **reject**: mapped onto the pending option of the matching
  kind — a once-style variant when the backend offers both (least
  privilege) — and applied through the ordinary control plane, with the
  model's reason as the answer message.
- **defer** (or anything the model cannot clearly decide): the request
  stays pending for a human gate — the same visible deferral as a harness
  decider, never a silent allow.

A failed or unparseable call shares the harness decider's bounded retry
semantics: after three failed attempts no further calls fire, the request
stays pending for a human, and runtime status surfaces
`permission_decider_exhausted`. The runtime status also surfaces the
resolved decider as `permission_model_decider` (`"provider/model"`).

Trade-offs between the three policies:

| Policy | Judgment | Cost | Best for |
|---|---|---|---|
| `permission_policy.decider` | Full agent with tools, verifies via the status tool | One runtime spawn per pending request | Requests that need inspection, multi-step reasoning, or tools |
| `permission_policy.decider_model` | One direct model call against the request mirror | One HTTP call per pending request | Cheap, bounded per-request classification on chatty backends |
| `auto_approve` | None — everything is allowed | None | Nodes whose requests are known-safe |

Resource bounds differ: harness decider runs consume MaxRuntimes admission
slots and can exhaust their retry bound under pressure
(`permission_decider_exhausted`); model calls bypass admission, are capped
only by their own concurrency cap and a per-runtime call budget, and a
blocked or budget-exhausted request simply stays pending.

The direct-model call is made by `internal/modelapi`, a small
OpenAI-compatible client shared with future harness-free judgment steps
(gate answers, loop exit markers), so the resolver is not
permission-specific infrastructure.

> **Trust assumption:** the model's allow decision is applied without a
> second confirmation — that is the point of the variant. The worker's
> request data (command, options) reaches the model as untrusted input, but
> a compromised or fooled model still decides. Choose this variant for
> nodes whose request surface is well-understood and bounded; when the
> worker is untrusted or the request surface is not, use the harness
> decider, whose agent can inspect, or keep requests on a human gate by
> not declaring an unattended policy.

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

## Launch wrappers

A template may declare a `launch_wrappers` registry: a list of trusted,
author-reviewed executable configurations that a node may launch under. The
helper executable is operator-provided — Avenor does not ship or instantiate
one.

```json
{
  "params": [{ "id": "run_dir", "type": "string", "required": true }],
  "launch_wrappers": [
    {
      "id": "sandbox",
      "argv": [
        { "type": "literal", "value": "/opt/bin/agent-sandbox" },
        { "type": "instance_param", "from_instance_param": "run_dir" }
      ]
    }
  ]
}
```

A node selects a wrapper by ID with `launch_wrapper`, a sibling of `action`
— never inside `action`:

```json
{ "id": "fix", "launch_wrapper": "sandbox",
  "action": { "type": "run", "prompt": "Reply OK" } }
```

- **Resolution.** Each `instance_param` reference substitutes the param's value
  as one complete argv element — whole-argv substitution, with no interpolation
  or expansion. `argv[0]` must be a literal absolute path. References must name
  declared, required `string` params. Unknown or duplicate wrapper IDs and
  invalid argv are rejected at validation.
- **Selection.** Only `run`, `loop`, and `team` nodes select a wrapper. Phases
  inherit the parent's resolved argv; child workflows do not. There is no
  template default. An existing session's ownership persists the resolved argv;
  resuming with a different wrapper is rejected.
- **Trust.** Wrappers are trusted, author-reviewed executable config — there is
  no per-workflow allowlist, and the ad-hoc MCP policy is unchanged.
  Permission-decider spawns are not wrapped by a worker's setting.
- **Semantics.** The wrapper must preserve stdio, exit-code, and signal
  semantics and keep same-path mounts and reachability per
  [backends](backends.md#launch-wrapper). The operator's wrapper (not the
  supervisor) must expose Claude's transcript
  (`~/.claude/projects/<encoded cwd>/`) at host-visible matching paths. Avenor
  does not implement isolation.

## Working directories

A template may declare where its attempts run: an optional template-level
`working_directory` default plus an optional per-node `working_directory`
override (the node wins), mirroring `default_lease_policy` and node lease
policy. The only declared value form is
`{"from_instance_param": "<param id>"}` naming a declared, required `string`
param —
templates are write-once, run-N, so a literal, machine-specific path is
rejected in validation. The machine-specific directory is supplied at
instantiation: a params value feeding a working-directory declaration must
be an absolute, clean path (relative paths and `..` segments are rejected),
while existence is checked at dispatch, not instantiation, so a worktree may
be created after the instance exists.

At dispatch the directory resolves once per attempt — for controller
dispatch and for manual starts alike — and the attempt's provider runs
there; on termination the attempt records that directory as
`working_directory`. A declared directory that does not exist or is not a
directory fails the attempt before any runtime starts, so the node's retry
policy applies. When no working directory is declared, the attempt runs in
the supervisor's working directory, exactly as templates without the
declaration always have. Avenor never creates worktrees or directories.

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
canceled is terminal: no command can advance a canceled instance.
```

Workflow status is one of `active`, `blocked`, `awaiting_gate`, `completed`,
`failed`, or `canceled`.

### Canceling a workflow

`workflow cancel` (CLI), the `workflow.cancel` control method, and the
`workflow_cancel` MCP tool cancel a whole instance with one attributed
decision. `actor` and `reason` are required — the reason is the durable audit
trail recorded on the `workflow.canceled` event. The kernel transition is
deterministic: the instance becomes terminal `canceled`, every live lease is
released, and every non-terminal activation parks in the `canceled` status so
no command can ever advance it again. The supervisor surface then terminates
the instance's in-flight attempt runtimes; their late `attempt_terminated`
facts land harmlessly on the already-canceled instance (the attempt fact is
recorded; leases, retries, and statuses never regress). Canceling an already
terminal instance (`completed`, `failed`) is a clear error; re-canceling the
same instance is an idempotent no-op. A cancel covers only the targeted
instance: attached child instances keep running.

```sh
avenor workflow cancel --socket /tmp/avenor.sock wf_... \
  --actor review-pr-auto --reason "PR head moved; run superseded"
```

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
| `cancel` | Cancel the whole instance: status `canceled`, every live lease released, non-terminal activations parked `canceled`, in-flight attempt runtimes terminated. Requires `actor` and `reason`. |

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

For a manual provider-backed node (`run`/`loop`/`team` without `mode:
auto`); an auto node is completed by the supervisor instead (see
[Completion](#completion)):

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
[`templates/escalation-bridge/`](../templates/escalation-bridge).

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
directory before recording the evidence event. It always copies and never
hard-links, so a worker that later rewrites the source in place cannot change
evidence already recorded.
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

While the supervisor is running, the same expiry runs live: a sweep (default
every 10s, `--lease-sweep-interval`) expires leases whose persisted expiry has
passed (reason `stale`) and wakes the controller through the same change
notification as an ordinary command, so a dead lease is reclaimed and its node
re-dispatched without a restart. 0 disables the live sweep.

A terminal attempt status of `failed`, `timed_out`, or `panicked` consumes
the retry budget: the activation re-arms while attempts remain and applies
the configured exhaustion when the budget is gone. A `canceled` attempt — a
deliberate operator kill or supervisor teardown — never re-arms and never
retries: the activation parks in `blocked`, the canceled attempt still
occupies one retry-budget slot, and `workflow.unblock` re-arms it.

A retry retains the activation and loop iteration, consumes only the retry
budget, and cannot follow a semantic branch or checkpoint. Exhaustion applies
only the configured `block`, operational `fail`, or named `outcome`. When an
activation blocks — retry exhaustion with `block`, or a canceled attempt —
the instance reports `blocked`; `workflow.unblock` restores `active` once no
activation remains blocked.

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
- [MCP](mcp.md) — `workflow_*` tools
- [Loop](loop.md) — `loop` action configs
- [Team](team.md) — `team` action configs
- [Stable](stable.md) — the supervisor that hosts the workflow manager
