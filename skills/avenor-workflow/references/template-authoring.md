# Template authoring for unattended pipelines

This file covers the authoring choices that matter when no human claims or
completes a node. Field definitions live in `docs/workflow.md`. The schema is
`schemas/workflow.profile.json`.

## Auto dispatch

```json
"dispatch": {
  "mode": "auto",
  "controller_id": "my-automation",
  "priority": 50,
  "concurrency_key": { "prefix": "repo:", "from_instance_param": "repo" }
}
```

- `mode: auto` is allowed on `run`, `loop`, and `team` nodes. An `external`
  node can be `auto` only with a `success_outcome` and required external
  gates. See `docs/workflow.md#dispatch-policy`.
- Give later stages a higher `priority` than entry nodes so that units already
  in flight finish before new ones start. The controller also ages waiting
  candidates by the minute.
- Key `concurrency_key` on the resource that must not be shared, such as a
  repository or a worktree. The key resolves from the instance param when the
  activation is created and then stays fixed. See
  `docs/workflow.md#instantiation`.
- An auto `run`, `loop`, or `team` node must not declare `completion.kind:
  explicit` or `completion.git.changed_from_base`. Validation rejects both.

## Assignment

```json
"assignment": {
  "role": "primary",
  "roster_file": "roster.json",
  "roster_entry": "automations",
  "thinking": "medium"
}
```

- The controller resolves `roster_entry` at dispatch. It pins the backend,
  agent, model, thinking level, and roster digest on the activation, and
  retries reuse that pin. A roster edit therefore affects only new
  activations. See `docs/workflow-controller.md` sections 2 and 4.
- Set `thinking` on the node's assignment. Workflow assignments do not yet
  apply a roster entry's `thinking` default (#287 in `known-gaps.md`).
- A `team` node needs no assignment. Each team member names its own
  `roster_entry` in the team config.
- Choose roster entries whose backend does not ask for permission. Nobody
  answers a prompt from an auto-dispatched node (#285 in `known-gaps.md`).

### Path resolution

- `prompt_file`, `loop_file`, and `team_file` in the template resolve against
  the supervisor's working directory at dispatch time. See
  `docs/workflow.md#action-types`. An assignment's `roster_file` does too,
  because `resolveAssignmentSelection` in `internal/stable/dispatch.go` loads
  the path as given. Start the supervisor
  from the directory that holds the templates so that these relative paths
  stay portable.
- Paths inside a loop or team config (`prompt_file`, `roster_file`) resolve
  against that config file's directory. See `docs/team.md` and
  `docs/loop.md`. A team file one directory deeper than the template needs
  one more `../` to reach the same roster.

## Completion contract and outputs

```json
"completion": {
  "kind": "files",
  "artifacts": [{ "path": "result.json", "non_empty": true }]
},
"outputs": [{
  "id": "head_sha", "name": "Head", "type": "string", "required": true,
  "source": { "artifact": "result.json", "pointer": "/head_sha" }
}]
```

When an auto attempt exits successfully, the supervisor evaluates the contract
against the attempt's working directory. See `docs/workflow.md#completion` and
`docs/workflow.md#outputs`.

- Each artifact must exist as a regular file inside the working directory,
  and must be non-empty when `non_empty` is set. The store copies each one
  into `instances/<id>/evidence/` and records its SHA-256.
- A `files` contract does not parse file contents. A **required**
  pointer-sourced output does: the file must be valid JSON, the pointer must
  resolve, and the value must match the output type. The kernel omits an
  optional output that does not resolve, so mark an output `required` when
  you need the parse to fail the attempt.
- An unmet contract records the attempt as failed with marker
  `contract_unmet`. The node's `retry_policy` then applies.

## How an auto node picks its outcome

The supervisor builds the node's outcome vocabulary from its `branches` keys,
its `outcomes` names, and its checkpoint exits. When the node declares none of
these, the vocabulary is the template's `terminal_outcomes`. Then:

- With exactly one name in the vocabulary, the supervisor uses that name.
- Otherwise, only a loop or team terminal marker with the `exit` directive can
  select one. A `run` node never carries a marker, so a run node with two
  outcomes always fails with `contract_unmet`.

Give every run node exactly one outcome. Use a single `branches` entry to the
next node, or a single terminal entry in `outcomes`. See
`internal/workflow/autocompletion.go` (`autoCompletionOutcome`).

## Mode variants: separate templates instead of branches

A deterministic choice between paths, such as a cheap mode and a thorough
mode, should not be a branch inside one template:

- An auto run node cannot choose between outcomes, and a loop or team marker
  is chosen by a model.
- For the runner to choose, it must complete a `manual` node. That requires
  `claim` and `start` first, which exist only as `workflow.command` ops on the
  control socket. They have no CLI subcommand and no MCP tool. See
  `docs/workflow.md#commands`.

Register one template per mode instead, and let the runner pick the template
at instantiation. The stored `template_id` then records which path ran, and
the runner can derive a label from it. To keep the variants from drifting,
derive one from the other with a `jq` filter in the setup script, and bump
both versions together.

## Working directory

```json
"params": [{ "id": "run_dir", "type": "string", "required": true }],
"working_directory": { "from_instance_param": "run_dir" }
```

- The param must be a required string. The value must be an absolute, clean
  path. Avenor checks that the directory exists at dispatch and never creates
  it, so the runner creates it before instantiating. See
  `docs/workflow.md#working-directories`.
- Without a declared working directory, every attempt runs in the
  supervisor's working directory. Concurrent units then share artifact paths.
- Agent harnesses load instruction files such as `AGENTS.md` and `CLAUDE.md`
  based on their working directory. A target can be untrusted, such as a
  pull request that edits its own instructions. Run attempts for it in a
  per-unit directory outside the target. Pass the target's
  path in an input file, and copy trusted instructions from a trusted revision
  into the inputs.
- Completion artifacts must sit inside the working directory. A per-unit run
  directory therefore also keeps agent output out of the target checkout.

## Retry and leases

- `default_retry_policy: {"max_attempts": 2, "exhaustion": "fail"}` retries a
  failed attempt once. On exhaustion it fails the workflow. The runner then
  sees a terminal status. `exhaustion: "block"` leaves the
  activation `blocked`, and the runner must detect that state. See
  `docs/workflow.md#heartbeat-recovery-and-retry`.
- Retry covers only attempts that end `failed`, which includes
  `contract_unmet`. Canceled, timed-out, and panicked attempts do not retry
  (#284 in `known-gaps.md`).
- The supervisor heartbeats the lease of an auto attempt. Set
  `default_lease_policy` so that a dead attempt expires within the time you
  are willing to wait.

## Agent prompts

- Keep one shared contract file that lists the hard rules, the inputs, and
  the output format. The runner copies it into each unit's input directory,
  and every node prompt points to it.
- Name the exact files each node writes. Tell the agent to write the file even
  when it has nothing to report, so that an empty result does not fail the
  contract.
- Tell the agent that the kernel parses the file, so invalid JSON fails the
  node.
