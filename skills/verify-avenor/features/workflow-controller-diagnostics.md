# Workflow controller diagnostics — `avenor stable` + `avenor workflow`

## What it proves

A dispatch selection that cannot resolve (a roster the assignment
references is missing or invalid) is contained and visible: the controller
records an `unresolved_selection` diagnostic naming the blocked
`workflow/node/activation` with the resolution error, holds that candidate
in the dispatch backoff instead of re-dispatching it every pass, and logs
the error once. The diagnostic shows in `controller status` (`diagnostics`)
and in the blocked workflow's `workflow inspect`
(`controller_diagnostics`). Fixing the cause lets the candidate dispatch on
the first pass after the backoff expires and clears the diagnostic — no
supervisor restart.

## Prerequisites (cross-ref `../SKILL.md` and `stable-control.md`)

- Build the binary: `mise run go-build` (→ `./avenor`).
- Export `MODEL` to an installed Pi model; the roster entry references it.
- Fresh scratch with `--workflow-root "$SCRATCH/workflows"` and
  `--workflow-adapter-dir "$SCRATCH/adapters"` (`0700`).

## Fixture

The retry-backoff fixture's shape, with one defect: the roster entry sets
`"thinking": "none"` (an invalid thinking value), which
`rosterconfig.Load` rejects at dispatch selection time. Template: single
`run` node, `assignment` → `role`/`roster_file`/`roster_entry`,
`dispatch` → `{"mode": "auto", "controller_id": "verify-diag"}`, no
completion contract.

## Drive

1. Launch `avenor stable` as in `retry-backoff.md`; create the template,
   controller, and instance; enable the controller.
2. Wait ~12 s (several reconcile passes at the default 30 s backoff base).
3. `avenor workflow controller status --socket "$SOCK" verify-diag` —
   `diagnostics` holds `unresolved_selection/<wf>/<node>/<activation>` whose
   detail ends with `thinking: invalid thinking value "none"`;
   `last_outcome` reads `dispatch_backoff` and `dispatch_backoff_until` is
   set (~30 s out).
4. `avenor workflow inspect --socket "$SOCK" "$WF"` —
   `controller_diagnostics` carries the same kind/name/detail; the
   activation stays `pending` with no attempts.
5. `grep -c "unresolved selection" "$SCRATCH/stable.log"` — exactly 1.
6. Rewrite the roster entry with a valid `thinking` value. Poll
   `workflow status` until `completed` (one attempt, no restart). The
   diagnostic is gone from `controller status`.

## Observable end state (the proof)

- `controller status`: `diagnostics["unresolved_selection/…"]` carries the
  roster validation error while the cause stands; absent after recovery.
- `workflow inspect`: `controller_diagnostics` matches the status entry;
  `attempts` stays empty until the roster is fixed, then holds exactly one
  successful attempt.
- Supervisor log: one `unresolved selection <identity>: <cause>` line per
  candidate and distinct detail, never one per pass; a candidate that
  recovers and later fails the same way logs again.

## Gotchas

- The diagnostic is keyed per activation identity: a new instance re-records
  its own; a fixed roster clears only the identity that dispatches.
- Executor start failures keep `start_failed` and kernel retry semantics —
  do not expect a diagnostic for those; the selection-failure path records
  no attempt at all.
- Cleanup follows `stable-control.md`: `control shutdown graceful` plus the
  recorded PID, never a pattern kill.
