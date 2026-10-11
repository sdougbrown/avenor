# Feature map — avenor CLI

Each file describes one user-facing feature: what it is, how a user reaches it,
and how to drive it with the avenor binary itself (the harness). The proof
state named in each file is the acceptance bar for that feature.

| Feature | File | Surface |
|---|---|---|
| Single run | [single-run.md](single-run.md) | `avenor run` → events + sentinel |
| Supervisor + control plane | [stable-control.md](stable-control.md) | `avenor stable` + `avenor control` |
| Event stream reader | [watch.md](watch.md) | `avenor watch` |
| Permission answering | [permissions.md](permissions.md) | `avenor answer` + permission handler files |
| Config validation | [verify-command.md](verify-command.md) | `avenor verify` |

Known secondary surfaces not yet mapped: embedded board web UI (built with
`-tags board`, see `mise.toml:go-build-full`), MCP server (`avenor mcp`,
`docs/mcp.md`), multi-phase loops/teams/workflows (`docs/loop.md`,
`docs/team.md`, `docs/workflow.md`), and `avenor await`. Add a map file here
when one of these needs verification coverage.

The backend for all drives is `pi` (the only agent runtime guaranteed on this
machine); the default `opencode-acp` backend requires `opencode`, which is not
installed here.
