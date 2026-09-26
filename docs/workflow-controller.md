# Avenor: Workflow Controller Example

A worked example of the optional [workflow controller](control-protocol.md)
driving the [software-factory templates](../templates/software-factory/README.md):
instantiate work items ad hoc, create and enable one controller, watch it
dispatch ready nodes and park external reviews, and recover a new supervisor
from the same root without coordinator memory.

The controller is a reconciler over explicitly declared dispatch policy. It
owns no workflow state, no ready queue, and no merge authority — the kernel
claims are the race arbiter, and every human gate stays human.

## The pieces

| Piece | Where |
|---|---|
| Review-unit template with dispatch policy and bound gates | `templates/software-factory/work.json` |
| Bounded stack parent composing review-unit children | `templates/software-factory/stack.json` |
| Request fixtures (controller, two work items, stack) | `templates/software-factory/fixtures/` |
| Example trusted-adapter manifests | `templates/software-factory/adapters/` |

## 1. Start a supervisor with adapters

```sh
# Install the example adapter manifests into a private directory and point
# the executable fields at your own operator-provided adapter binaries.
mkdir -p ~/.avenor/adapters && chmod 700 ~/.avenor/adapters
cp templates/software-factory/adapters/*.json ~/.avenor/adapters/
$EDITOR ~/.avenor/adapters/*.json

avenor stable --workflow-root /path/to/workflows \
  --workflow-adapter-dir ~/.avenor/adapters
```

The registry load applies trust checks (ownership, modes, strict manifest
shape) at startup and before every adapter execution. A failed load disables
controller activity for that process; workflow RPCs keep serving.

## 2. Register the templates and instantiate issues ad hoc

```sh
avenor workflow create --socket /path/to/socket \
  --request-file templates/software-factory/work.json
# For a stack of review units, also register the parent:
avenor workflow create --socket /path/to/socket \
  --request-file templates/software-factory/stack.json

# One workflow per issue, whenever it arrives:
avenor workflow instantiate --socket /path/to/socket \
  --request-file templates/software-factory/fixtures/work-item-a.json
avenor workflow instantiate --socket /path/to/socket \
  --request-file templates/software-factory/fixtures/work-item-b.json
```

There is no campaign resource and no cross-workflow inference: each
instantiation is one independent review unit. Every provider-backed node in
the work template declares `dispatch: {mode: auto, controller_id:
software-factory, priority: N, concurrency_key: {prefix: "worktree:",
from_instance_param: "worktree"}}`, so the controller — and only that
controller — will claim them. The concurrency key resolves from the
instance's recorded `worktree` param at instantiation: two work items pinned
to different worktrees run concurrently under one controller, while two
items sharing a worktree param serialize — one work item at a time per
worktree, no per-worktree template versions required.

The controller also resolves each node's declared assignment through the
supervisor's roster path before dispatching: the roster entry supplies the
pinned backend/agent/model, the roster file's digest and the declared
thinking level ride on the pinned selection, and retries inherit that pin.

## 3. Create and enable the controller

```sh
avenor workflow controller create --socket /path/to/socket \
  --request-file templates/software-factory/fixtures/controller.json
avenor workflow controller enable --socket /path/to/socket software-factory
```

`create` registers a disabled controller; `enable` persists the desired state
and the live supervisor's leader loop acquires the leadership lease. The
controller then repeatedly:

1. queries ready auto-dispatch nodes (`workflow ready` is the same advisory
   view); order candidates by priority, then ready time;
2. dispatches provider candidates through stable admission under its
   `max_inflight` limit and the worktree concurrency keys; and
3. parks auto external nodes whose bound gates carry resolved exact-head
   subjects and polls those gates through the registered adapters.

Manual nodes — intake, hardening (the plan checkpoint), merge-auth, advisor —
are never eligible. A manual claim on an auto node remains valid; whichever
actor takes the kernel claim first proceeds.

## 4. Inspect its decisions

```sh
# The advisory ready view: what the controller sees right now.
avenor workflow ready --socket /path/to/socket software-factory --limit 10

# Leadership, desired state, in-flight count, capacity blocks, next poll.
avenor workflow controller status --socket /path/to/socket software-factory
avenor workflow controller list --socket /path/to/socket

# The workflow-level view of what the controller did.
avenor workflow status --socket /path/to/socket <workflow-id>
avenor workflow events --socket /path/to/socket <workflow-id> --after-seq 0 --limit 50
```

Each candidate in `workflow ready` carries its supervisor, workflow, node and
activation identity, revision, priority, and concurrency key; it is advisory
and grants no lease. When the controller dispatches a provider node it pins
the resolved execution selection — effective backend, agent, model, thinking,
and roster digest — on the activation; retries inherit that pin, and only a
declared reroute (a new activation) can carry a different selection.

A parked review's gates are polled on the cursor cadence visible in
`controller status`. An adapter `passed` on every required gate follows the
node's declared success outcome; advisory results route through the gates'
declared `result_outcomes` (correction branches, replan). Nothing else lands:
a result whose subject does not match the pinned exact head is rejected, and
a superseded head's cursors are dropped.

## 5. The human checkpoints

- **Hardening** (`manual` dispatch): a provider run the controller will not
  touch until a human or the declared release path claims and completes it.
- **merge-auth**: after a clean review verdict the workflow stops at the
  manual human gate bound to the exact published repository, pull request,
  and head SHA. Satisfy it with `avenor workflow gate ... --operation
  satisfy` and explicit evidence. The controller can never park it, satisfy
  it, or merge.
- **advisor**: the manual checkpoint for a `checkpoint` review outcome.

## 6. Recover

```sh
# Stop the supervisor (or it crashes), then start a fresh one on the same root:
avenor stable --workflow-root /path/to/workflows \
  --workflow-adapter-dir ~/.avenor/adapters
```

The startup barrier is exactly-once and ordered: recover the workflow
catalog, resume awaiting child compositions, rebuild the candidate index,
recover controller records and poll cursors, then restart a leader loop for
every controller whose desired state is enabled. Dispatched work continues
from the durable snapshot — a parked review re-parks on its exact subject, an
expired attempt lease re-arms for a replacement claim — with no coordinator
memory involved. `disable` is the deliberate stop: it halts future dispatch,
cancels poll contexts, freezes cursors, and releases the lease, leaving
running attempts alone.

## Boundaries

- The controller acts only on nodes declaring its controller ID. Ordinary
  manual workflow commands, non-workflow spawns, and other supervisors are
  unaffected.
- No campaign coordination, automatic merge, PR-state completion inference,
  arbitrary graph mutation, or external credentials in workflow data.
- The internal `workflow.dispatch` method is never exposed on the control
  socket, CLI, MCP, or client hosts; only the in-process leader calls it.
