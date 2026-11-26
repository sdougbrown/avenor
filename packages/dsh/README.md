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

### Permissions go to the orchestrator

A blocked run is its own outcome, not a failure: core's `result` returns early
while the run is `waiting`, so the seam reports a pending request through
`onPending`, polls until the decision clears it, and keeps waiting. The provider
then hands the request to the agent that spawned the child:

- parent `running` → `parent.inject(...)`, so the decision lands in the current
  turn;
- parent `idle` → `parent.followup(...)`, which wakes it with a new turn. An
  unanswered request would otherwise stall the run until Avenor's timeout.

The notice carries the run id, Avenor's question, and the option ids, and the
plugin registers `avenor_answer_permission` (core's `answerPermissionTool`) so
the orchestrator can decide. The option ids are Avenor's, not this plugin's
(`once`, `always`, `reject` today, with `kind` values `allow_once` /
`allow_always` / `reject_once`), which is why they travel in the notice. Nothing here asks the person looking at the
harness: `registerPendingInteraction` and a composer card would be the wrong
mechanism for a delegation decision.

At most one notice is sent per request id. If the run replaces its pending
request, the replacement gets its own notice.

The provider declares no `depthLimit` capability, so DSH refuses to enforce a
numeric recursion budget itself. The tool row must pass
`maxDepth: provider-managed`, which makes the provider — Avenor's own nesting
rules — own that budget. Without it the entry fails to activate with
`provider "avenor" cannot enforce maxDepth`.

## Choosing the child's runtime

The bundle row's `config` decides how every delegated child runs. All fields are
optional; omitted, Avenor's own defaults decide.

| Field | Meaning |
|---|---|
| `backend` | Avenor backend for the child, such as `dsh`, `pi`, or `claude` |
| `model` | Model id the backend understands (`dsh` takes `provider/model`) |
| `agent` | Avenor agent profile |
| `rosterFile` / `rosterEntry` | A roster map and the entry that supplies backend, agent, and model together |

```yaml
- id: avenor-delegation
  name: '@dougbots/dsh-avenor'
  config:
    model: grace/qwen
```

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

1. **Child runtime selection is per bundle, not per call.** The bundle row's
   `config` chooses the child's route (`agent`, `model`, `backend`, or a roster
   entry), and every delegated run in the profile uses it. A tool argument that
   selects a model per delegation would need a second tool; the provider seam
   itself carries no per-call selection.
2. **`toolName` collision.** The spike adds `subagent_avenor` and leaves the
   shipped `subagent` tool on the local provider. Making Avenor the only backend
   means overriding that row (`provider: avenor`, `toolName: subagent`), which
   reroutes every agent in the profile.
3. **No streaming.** `SubagentProvider.start()` returns a terminal promise only,
   so the run panel cannot show live progress from here. Live output needs a
   `ctx.jobs`-backed tool path, which is the next piece, not this one.
4. **What if the orchestrator never answers?** Permissions now reach the parent
   (see above), but a wake-up turn is not a guarantee: the parent may be gone,
   may decline to answer, or may itself be waiting on something. Avenor's
   `--permission-handler` is the unattended answer; the plugin has no config for
   it yet, so today a still-unanswered request stalls the run until Avenor's
   timeout. A deny-by-default deadline is the missing piece.
5. **Contracts are mirrored.** `src/types.ts` restates the subagent contract
   instead of importing `@deepseek-ai/dsh-subagent`, and
   `src/answer-tool.ts` hand-builds the tool definition that `defineTool` would
   normally compile. Both mirror the running Harness because a bundle cannot
   import DSH's own packages: the loader resolves shipped packages for *rows*,
   not for a plugin's imports. A live run has not yet exercised the hand-built
   definition, so treat `avenor_answer_permission` appearing in a session's tool
   list as the check that it is accepted.

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
