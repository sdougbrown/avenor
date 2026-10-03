# Horse loop work template

A lean [Avenor workflow](../../docs/workflow.md) template for the
issue → horse → PR → review → address-pr loop:

- `work.json` (`horse-loop-work@1.0.0`) — one review unit per issue: intake,
  horse implementation in an isolated worktree, publication, exact-head
  external review, and an address-pr correction loop that repeats until the
  reviewer's verdict is clean. Declares the same two required instance params
  as the software-factory work template: `worktree` (the concurrency-key name
  resolved at instantiation) and `worktree_path` (the absolute worktree
  directory, which must exist when a node dispatches).
- `roster.json` — `implementer` pins the horse profile (pi backend, high
  thinking); `publisher` and `reconciler` are lightweight pi runs.
- `fixtures/` — a controller request (`horse-loop`, `max_inflight` 3) and a
  work-item instantiation fixture.

## The flow

```text
intake            [manual]   verbatim issue body + base SHA
  → implement     [run, auto]   horse implements in the worktree, commits, no push
  → publication   [run, auto]   push branch, open/update PR, pr-info.json
  → review        [external, auto-park]   exact-head gate via github-pr-review
      clean                    → merge-auth [manual, exact-head human gate]
                                 → reconciliation [run, auto] → merged
      changes_requested        → correction [run, auto: address-pr loop]
      action_required          → correction → publication → review (repeat)
      failed                   → correction
      replan                   → implement (re-implement from the issue)
      checkpoint               → advisor [manual checkpoint gate]
```

The correction prompt carries the full address-pr discipline: paginated
comment fetch, fix/accept/dismiss triage with justifications, per-thread
replies with the `👾 AI Agent` prefix, resolve-only-fixed-threads, one
top-level summary, re-request with the reviewer's bare trigger text, and a
3-iteration cap per run. The review gate re-parks on each new exact head, so
the loop repeats until the verdict is clean; the human merge gate always
stays human.

## Usage

```sh
# Register the template once per supervisor (workflow root = this directory's parent):
avenor workflow create --socket /path/to/socket \
  --request-file templates/horse-loop/work.json

# One workflow per issue, whenever it arrives:
avenor workflow instantiate --socket /path/to/socket \
  --request-file templates/horse-loop/fixtures/work-item.json

# Create and enable the controller:
avenor workflow controller create --socket /path/to/socket \
  --request-file templates/horse-loop/fixtures/controller.json
avenor workflow controller enable --socket /path/to/socket horse-loop
```

The supervisor must run with `--workflow-root` covering this directory and
`--workflow-adapter-dir` containing a trusted `github-pr-review` adapter
manifest (see `../software-factory/adapters/` for the shape; point its
`executable` at your own operator-provided adapter binary). The adapter maps
reviewer verdicts to the gate's declared outcomes.

## Differences from the software-factory template

No assessment, plan drafting, hardening, verification-team, or CI gates —
this is the light loop for issues whose contract fits in the issue body. The
hardening checkpoint is the place to intercept when a work unit needs a plan;
use `../software-factory/` for those. The `replan` branch re-runs
implementation from the issue rather than re-assessing.
