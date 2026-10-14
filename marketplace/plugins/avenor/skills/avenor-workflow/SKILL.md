---
name: avenor-workflow
description: Build unattended automations on Avenor durable workflows and the in-process workflow controller. Use when writing a workflow template, wiring a controller-driven pipeline around a dedicated `avenor stable`, or debugging one that stalls, skips a stage, or never reaches a terminal state.
---

# Avenor workflow automations

This skill covers the shape of an unattended pipeline built on the workflow
kernel and the workflow controller. The docs own the reference material. Read
them for field definitions instead of guessing:

- `docs/workflow.md`: templates, nodes, completion, outputs, dispatch policy,
  working directories, the activation lifecycle, and retry.
- `docs/workflow-controller.md`: a controller walkthrough with the
  software-factory templates.
- `docs/stable.md#workflow-controllers`: controller CLI and status fields.
- `docs/control-protocol.md#workflow-methods`: the `workflow.*` methods.
- `schemas/workflow.profile.json`: the template schema the kernel validates.
- `templates/software-factory/`: a complete, validated template set.

All paths are relative to the repo root.

## When to use a workflow

Use a single `avenor run`, a loop, or a team for one task whose result you
check yourself. Use a workflow when a model could skip a stage and still claim
that it ran. Each stage becomes a separate attempt. The kernel completes an
attempt only when the stage's declared evidence exists. A skipped stage
therefore fails instead of passing on the worker's report.

`docs/workflow.md#when-you-need-a-workflow` lists the durable-state reasons
(crash recovery, exact-head gates, declared review branches).

## Architecture

```text
process supervisor ─ keeps alive ─► avenor stable (own socket, own workflow root)
                                     └─ controller dispatches auto nodes ─► agents write files
scheduler ─ runs periodically ─► runner (deterministic glue)
                                     ├─ discover work, prepare inputs, instantiate
                                     └─ read stored evidence, perform side effects
setup script (idempotent) ─► register templates, create + enable controller
```

- **Dedicated supervisor.** Run one `avenor stable` per automation with its own
  `--control-socket`, `--workflow-root`, and `--workflow-adapter-dir`. Pass
  the adapter directory even when the pipeline has no external gates. The
  default directory (`docs/stable.md#workflow-controllers`) may hold adapters
  installed for other supervisors. Keep it alive with a process supervisor.
- **Idempotent setup script.** It registers every template, creates the
  controller only when `controller status` fails, and always runs `enable`.
  Re-running it after a template change is the deploy step.
- **Deterministic runner.** It discovers units of work and writes each unit's
  input files. It instantiates one workflow per unit-of-work key and passes
  the key as an instance param. On a later tick it inspects the workflow,
  reads evidence from the workflow store, and performs every external side
  effect itself (post, push, notify, label).
- **Agents only write files.** Strip credentials from the supervisor's
  environment so agents cannot perform side effects. Agents inherit that
  environment.
- **Derive claims from evidence.** Base each claim about a run, such as a
  label, a mode, or "stage X ran", on stored facts. Those facts are the
  template ID, activation status, and evidence files. Never derive a claim
  from what an agent wrote about itself. Read the evidence copy under
  `<workflow-root>/instances/<id>/` and check its SHA-256 against `inspect`.
  Never read evidence from the agent-writable working directory.

## Author the template

Read `references/template-authoring.md` before writing a template. It covers:

- auto dispatch with a concurrency key templated from an instance param
- roster-resolved assignments and pinned `thinking`
- `files` completion contracts with JSON-pointer outputs
- how an auto-completed node selects its outcome (run nodes declare exactly
  one outcome)
- mode variants as separate templates instead of branches
- where attempts run, and why that can be outside the target repo
- path resolution for `prompt_file`, `team_file`, and `roster_file`

## Lifecycle

All commands take `--socket <path>`. Each command and its flags are in
`docs/workflow.md#commands` and `docs/stable.md#workflow-controllers`.

1. **Register.** `avenor workflow create --request-file <template.json>`.
   Bump `template_version` on every template change. Running instances load
   their template by ID and version (see #283 in `references/known-gaps.md`).
2. **Controller.** `avenor workflow controller create --request-file
   <controller.json>` registers it disabled. `controller.json` holds
   `controller_id` and `max_inflight`. Then run `controller enable <id>`.
   `controller disable <id> --reason <text>` stops future dispatch but leaves
   running attempts alone.
3. **Instantiate.** `avenor workflow instantiate --template-id <id>
   --template-version <v> --request-file <params.json>` with
   `{"params": {...}}`. It prints `workflow_id`.
4. **Observe.**
   - `avenor workflow ready <controller-id> --limit N` shows what the
     controller can dispatch now.
   - `controller status <id>` shows `desired_state`, `leader`, `inflight`,
     and `capacity_blocked`.
   - `workflow inspect <wf>` shows activations, attempts, evidence, and
     outputs.
   - `workflow events <wf> --after-seq 0` shows the history.
5. **Retention.** The kernel keeps instances and evidence. Your runner owns
   its own working directories and must prune them. Never prune a directory
   that a non-terminal workflow still uses as its working directory.

## Verify

Drive the CLI as `skills/verify-avenor/SKILL.md` describes (build, doctor,
evidence capture). For a workflow pipeline:

1. Start an isolated `avenor stable` with its own workflow root and a short
   socket path such as `/tmp/wf-<name>.sock`. A long socket path fails with
   `bind: invalid argument` because Unix socket paths are limited to about 100
   bytes on macOS. Keep the full socket path under that.
2. Register the templates and create the controller, but do not enable it
   yet. Instantiate a fixture unit and confirm that `workflow ready` lists
   the entry node. Then enable the controller and confirm that `controller
   status` shows `desired_state: enabled` with a leader. An enabled
   controller dispatches the entry node within one pass, so `ready` is
   usually empty after you enable it.
3. Run one real end-to-end unit with external side effects disabled, for
   example with a no-post flag on the runner.
4. In `workflow inspect`, confirm one attempt per stage and evidence for every
   declared artifact. In `workflow events`, confirm separate attempts. A stage
   that should not have run must have no activation.

## Known gaps

`references/known-gaps.md` lists open kernel issues, how to check whether each
is fixed on your installed version, and the workaround for each. Read it
before you debug a stuck workflow.

## Worked example: unattended PR review

The first real controller consumer reviews pull requests unattended.

- A runner screens each PR, sizes it, and picks one of two templates by mode
  (`deep`, `light`). The setup script derives the light template from
  the deep one with a jq filter.
- Graph: `primary` (run) → `fanout` (team of three reviewers, deep only) →
  `synthesize` (run, terminal `ready_to_post`). Each node is `auto` under one
  controller with the concurrency key `repo:<owner/repo>`.
- Evidence: `primary.json`; one `reviewer-<role>.json` per reviewer; then
  `review.json` and `triage.json`. JSON-pointer outputs (`/head_sha`, `/node`,
  `/commit_id`) make the kernel parse each file.
- Attempts run in a per-PR run directory outside the PR checkout, so agents
  load no instructions from the PR under review.
- The runner labels a review `deep` only when the stored template is the deep
  one and all three reviewer files are in satisfied-`fanout` evidence. It
  validates `review.json`, rechecks the PR head, and posts once.
