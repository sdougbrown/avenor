# Software-factory request fixtures

Strict-JSON request files for driving the software-factory templates through
the stable supervisor's control socket. Each file is an example: copy it,
fill in the real issue bodies and SHAs, and pass it with `--request-file`.

## Files

| File | Purpose |
|---|---|
| `controller.json` | `avenor workflow controller create` request: registers the disabled `software-factory` controller with `max_inflight: 2`. |
| `work-item-a.json` | Instantiates one independent `software-factory-work@1.3.0` review unit (issue 115) pinned to the `avenor-issue-115` worktree key and the placeholder `worktree_path` directory. |
| `work-item-b.json` | Instantiates a second, independent review unit (issue 130) pinned to a different `worktree` key and its own `worktree_path` directory. Two instances of the same template version with different worktree keys resolve different concurrency keys, so their provider-backed nodes run concurrently — each in its own working directory; instances sharing a worktree key still serialize — one work item at a time per worktree. |
| `stack-instance.json` | Instantiates the `software-factory-stack@1.2.0` parent, which composes two review-unit child workflows and passes each child its worktree key name and worktree directory explicitly. |

The instance `params` object must satisfy the template's declared `params`
contract (`worktree` and `worktree_path` are required; values are non-empty
strings of at most 256 characters, and `worktree_path` must be a clean
absolute path). Replace the placeholder `/absolute/path/to/...` values with
real worktree directories that already exist on the machine — Avenor never
creates worktrees, and a node whose directory does not exist at dispatch
fails pre-start and consumes its retry policy. The `metadata` object is
free-form; only `template_id` and `template_version` are required by
`workflow.instantiate`.

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
