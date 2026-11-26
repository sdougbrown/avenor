# `@dougbots/dsh-avenor` (spike)

A DeepSeek Harness bundle that registers an `avenor` subagent provider, so DSH's
own `subagent` tool can delegate to Avenor instead of a local child Agent.

**Status: spike.** One provider, no tools, no client UI, not published. It exists
to answer one question — whether Avenor's run lifecycle maps cleanly onto DSH's
`SubagentProvider` contract — before any packaging or UI work.

## What it does

`apply()` registers a provider named `avenor` with DSH's subagent registry. The
patch then mounts DSH's own `@deepseek-ai/dsh-tool-subagent` against it under the
tool name `subagent_avenor`, next to the shipped `subagent` tool.

Each `start()` spawns one Avenor run through `@dougbots/avenor-core`, waits for
its terminal state with `resultTool`, and projects that state onto
`SubagentResult`:

| Avenor status | DSH stop reason | Notes |
|---|---|---|
| `done` | `completed` | output is the run's final output |
| `killed` | `aborted` | partial output is kept |
| `failed`, `timeout`, `unknown` | `error` | with a diagnostic naming the status |
| wait expired (not settled) | `error` | diagnostic says the wait expired |

Abort and `dispose()` both cancel the run. `result` never rejects on a child
failure — only a broker/infrastructure fault rejects, which is what the seam
requires. Disposal after settlement is a no-op: killing a finished run makes
Avenor append a second `session.end` (`cancelled`, exit 130) after the real
`end_turn`, so a successful run looks stopped in the event log.

The provider declares no `depthLimit` capability, so DSH refuses to enforce a
numeric recursion budget itself. The tool row must pass
`maxDepth: provider-managed`, which makes the provider — Avenor's own nesting
rules — own that budget. Without it the entry fails to activate with
`provider "avenor" cannot enforce maxDepth`.

## Install (local bundle)

From a DSH session:

```
plugin_manager: action=install_bundle, target=<absolute path to this directory>
```

or **Plugins → Add plugin** and point at this directory. Installing mutates the
profile for every session, so it needs approval.

The `avenor` binary is an external prerequisite, resolved by
`findAvenorBinary()`: `AVENOR_BIN`, then `avenor` on `PATH`, then an existing
install. This bundle declares no `postinstall` and makes no install-time network
request.

## Open questions this spike does not answer

1. **Child runtime selection.** `avenor` runs its default agent unless the plugin
   passes `agent`/`model`/`backend`/roster fields. `createCoreSeam(selection)`
   accepts them; the bundle does not wire them to config yet. A delegated child
   inheriting nothing from the parent is the honest default, but it is a product
   decision, not a bug.
2. **`toolName` collision.** The spike adds `subagent_avenor` and leaves the
   shipped `subagent` tool on the local provider. Making Avenor the only backend
   means overriding that row (`provider: avenor`, `toolName: subagent`), which
   reroutes every agent in the profile.
3. **No streaming.** `SubagentProvider.start()` returns a terminal promise only,
   so the run panel cannot show live progress from here. Live output needs a
   `ctx.jobs`-backed tool path, which is the next piece, not this one.
4. **Permissions belong to the orchestrator, not the human.** Avenor's design is
   that the parent agent answers a child's permission request, the way
   `packages/pi/src/index.ts` does: the pending request is surfaced into the
   parent's context and the parent calls `avenor_answer_permission`. DSH has no
   seam that carries an external child's request into that child's own session,
   but the provider does get `request.parent: Agent`, so the plugin can
   `parent.inject()` the pending request (and `followup()` when the parent's turn
   has already ended) and register the answer tool. That is a Host-side
   capability, not a client card — `registerPendingInteraction` would be the
   wrong mechanism, since it asks the person looking at the harness.
   Unanswered requests still stall the run, so the plugin also needs a default
   policy for the unattended case.
5. **Types are mirrored.** `src/types.ts` restates the subagent contract instead
   of importing `@deepseek-ai/dsh-subagent`, because a bundle must not depend on
   packages the installed Harness provides. Promoting this means switching to the
   real types and deleting the mirror.

## Layout

- `src/index.ts` — the Host plugin entry (`inject`, `apply`).
- `src/provider.ts` — the provider, the core-backed seam, prompt flattening.
- `src/stop-reason.ts` — the terminal-state projection, tested in isolation.
- `cordis.patch.yml` — inserts the provider row and the tool row.

## Checks

```
bun run build      # tsdown, ESM + d.ts
bun run typecheck
bun test
```
