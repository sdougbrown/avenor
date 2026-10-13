# Known gaps

Each gap below is an open issue that affects unattended pipelines. Delete an
entry when its fix ships.

Before you apply a workaround, check whether the gap is fixed:

```sh
gh issue view <n> -R sdougbrown/avenor --json state
git tag --contains <fix-sha>   # compare with: avenor --version
```

A closed issue helps only when the installed `avenor --version` is at or after
a tag that contains the fix commit.

## #283: `workflow create` overwrites the same ID and version

Registering a template whose `template_id` and `template_version` already
exist replaces the stored file without an error. Running instances then load
the new content.

**Workaround:** Bump `template_version` on every template change. Setup scripts
may re-register unchanged templates.

## #284: canceled, timed-out, or panicked attempts strand the workflow

An attempt that ends `canceled`, `timed_out`, or `panicked` leaves its
activation `attempt_failed`. `retry_policy` is skipped, and the instance stays
`active`. The controller does not dispatch it again. Plain failures, including
`contract_unmet`, do retry.

**Workaround:** On every tick, the runner inspects each pending workflow.

- It treats any activation in `attempt_failed` or `blocked` as stuck.
- It enforces its own wall-clock timeout per workflow.
- For a stuck or timed-out workflow, it cancels each live runtime (an attempt
  with no `ended_at`) with `avenor control --socket <s> cancel <runtime_id>`.
- It counts that run as a failed try against its own retry cap.

No CLI command cancels a workflow instance, so the instance stays `active` in
the store. The runner's own state records the outcome.

## #285: auto-dispatched nodes cannot set a permission policy

Dispatch never sets `auto_approve` or a permission handler. A backend that
asks for approval falls through to the default file handler
(`docs/control-protocol.md#permission-resolution-precedence`). That handler
waits about 10 minutes and then returns a timeout error
(`docs/permission-handler.md#failures-and-timeouts`).

**Workaround:** Assign roster entries whose backend and agent do not prompt
for the tools a node uses. Start the supervisor with a short
`--permission-claim-timeout`, such as `30s`. A connected control client, such
as the runner during a tick, then cannot hold a request indefinitely. The flag bounds only the wait on a connected client. It does not
answer the request.

## #287: roster-entry `thinking` is not applied to workflow assignments

Roster entries may declare a default `thinking` level. For workflow nodes,
`resolveAssignmentSelection` in `internal/stable/dispatch.go` copies the
backend, agent, and model from the roster entry, but takes `thinking` only
from the node's `assignment`.

**Workaround:** Set `thinking` on each node's `assignment`. Team and loop
phases that name a roster entry still use that entry's default.

## #289: no command cancels a workflow instance

The `canceled` workflow status exists and counts as terminal, but no CLI
subcommand, control method, or MCP tool sets it. `avenor control cancel`
ends a runtime, and the instance stays `active`.

**Workaround:** Cancel the runtime, then record the workflow as abandoned in
the consumer's own state so later ticks skip it. Expect `workflow status` and
`controller status` to keep listing it as active.
