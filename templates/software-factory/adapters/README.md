# Software-factory adapter manifests (examples)

Trusted external-gate adapter manifests for the software-factory work
template's `review` node. The node's `ci` gate polls `circleci-pipeline` and
its `review-verdict` gate polls `github-pr-review`.

These files are **examples**, not working registrations:

- `executable` values (`/opt/avenor/adapters/...`) are placeholders. Replace
  them with absolute paths to operator-provided adapter executables that you
  own and control. Avenor never ships an adapter binary and never invokes
  `gh` or any external service itself.
- The adapter receives one JSON poll request on stdin (subject, inputs,
  cursor) and writes exactly one JSON result object to stdout
  (`{"version":1,"result":"passed|failed|changes_requested|action_required",
  ...}`). See `docs/workflow-controller.md` and the manifest contract in
  `internal/workflowcontroller/adapter_manifest.go` for the full wire shape.

## Installing

Copy the manifests into a private, owner-only directory and point the
supervisor at it:

```sh
mkdir -p ~/.avenor/adapters && chmod 700 ~/.avenor/adapters
cp templates/software-factory/adapters/*.json ~/.avenor/adapters/
$EDITOR ~/.avenor/adapters/*.json   # replace the placeholder executables
avenor stable --workflow-root /path/to/workflows \
  --workflow-adapter-dir ~/.avenor/adapters
```

The registry applies trust checks at load time and before every execution:
the manifest and executable must be owned by the supervisor user, the
executable must not be group/world-writable, and unknown manifest fields are
rejected. No credentials belong in workflow data or manifests; adapters read
their own credentials from their own environment.
