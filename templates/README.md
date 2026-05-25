# Avenor Template Pack

Starting points for wiring Avenor into multi-agent workflows. Each host folder contains skills and prompts adapted for that runtime.

## Dispatch matrix

Avenor runs agents through backends. Each row is the dispatching host; each column is the agent runtime being dispatched to.

| From \ To | OpenCode (jockey) | Codex | Claude Code |
|---|---|---|---|
| **Claude Code** | `claude/skills/dispatch-jockey` | `claude/skills/dispatch-codex` | — (same process) |
| **OpenCode** | `opencode/skills/dispatch-jockey` | — | — |
| **Codex** | `codex/skills/dispatch-jockey` | — (same process) | not yet supported |

"Same process" means the host can delegate directly without Avenor. Avenor's value is crossing runtime boundaries.

## Template folders

### `opencode/`

Agent prompts and skills for OpenCode-hosted workflows.

- `prompts/jockey.md` — lead agent: plans, delegates to horse/mule, integrates, verifies
- `prompts/horse.md` — bounded implementation executor
- `prompts/mule.md` — literal executor for mechanical tasks
- `skills/dispatch-jockey/` — start a jockey run and monitor it
- `skills/answer-jockey/` — respond to permission requests mid-run

### `claude/`

Skills for Claude Code. Claude can dispatch outward to OpenCode jockey or Codex.

- `skills/dispatch-jockey/` — CC dispatches to OC jockey via `opencode-acp`
- `skills/dispatch-codex/` — CC dispatches to Codex via `codex-app-server`

### `codex/`

Skills for Codex. Codex can dispatch outward to OpenCode jockey.

- `skills/dispatch-jockey/` — Codex dispatches to OC jockey via `opencode-acp`

## How it fits together

The intended pattern: a top-level Claude Code or Codex session receives a complex task, dispatches to OpenCode jockey via Avenor, and jockey in turn delegates bounded work to horse or mule sub-agents within OpenCode. Avenor brokers the runtime boundary; the agent hierarchy handles the work.

Permission requests flow back up through the file handler. The dispatching host reads `<perm-base>.req` and responds with `avenor answer`.

For the full CLI reference and orchestration patterns, see [docs/cli.md](../docs/cli.md).
