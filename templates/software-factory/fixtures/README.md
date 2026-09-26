# Software-factory request fixtures

Strict-JSON request files for driving the software-factory templates through
the stable supervisor's control socket. Each file is an example: copy it,
fill in the real issue bodies and SHAs, and pass it with `--request-file`.

## Files

| File | Purpose |
|---|---|
| `controller.json` | `avenor workflow controller create` request: registers the disabled `software-factory` controller with `max_inflight: 2`. |
| `work-item-a.json` | Instantiates one independent `software-factory-work@1.2.0` review unit (issue 115) pinned to the `avenor-issue-115` worktree param. |
| `work-item-b.json` | Instantiates a second, independent review unit (issue 130) pinned to a different `worktree` param. Two instances of the same template version with different worktree params resolve different concurrency keys, so their provider-backed nodes run concurrently; instances sharing a worktree param still serialize — one work item at a time per worktree. |
| `stack-instance.json` | Instantiates the `software-factory-stack@1.1.0` parent, which composes two review-unit child workflows and passes each child its worktree param explicitly. |

The instance `params` object must satisfy the template's declared `params`
contract (`worktree` is required; values are non-empty strings of at most 256
characters). The `metadata` object is free-form; only `template_id` and
`template_version` are required by `workflow.instantiate`.

## Usage

```sh
avenor workflow controller create --socket /path/to/socket \
  --request-file templates/software-factory/fixtures/controller.json
avenor workflow controller enable --socket /path/to/socket software-factory

avenor workflow instantiate --socket /path/to/socket \
  --request-file templates/software-factory/fixtures/work-item-a.json
```

See `docs/workflow-controller.md` for the full walkthrough, including
inspecting controller decisions and recovery.
