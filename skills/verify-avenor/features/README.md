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
| Broker messages | [broker-messaging.md](broker-messaging.md) | spawned child ↔ supervisor ask/reply over the broker |
| Messaging tools | [messaging.md](messaging.md) | `avenor_message` (six actions) + `avenor_peers` (list/rename/last) from a host session and its children |
| Config validation | [verify-command.md](verify-command.md) | `avenor verify` |
| Launch identity | [provenance.md](provenance.md) | `avenor run --team-file` → MCP env + phase/team events |

Known secondary surfaces not yet mapped: embedded board web UI (built with
`-tags board`, see `mise.toml:go-build-full`), MCP server (`avenor mcp`,
`docs/mcp.md`), multi-phase loops/teams/workflows (`docs/loop.md`,
`docs/team.md`, `docs/workflow.md`), and `avenor await`. Add a map file here
when one of these needs verification coverage.

The backend for all drives is `pi` (the only agent runtime guaranteed on this
machine). `opencode` is installed at `/usr/local/bin/opencode`, so the
`opencode-acp` backend is driveable too — but the installed
`~/.config/opencode/opencode.json` is rejected by opencode 1.15.5, so a drive
must point `XDG_CONFIG_HOME` at a scratch config (see
[messaging.md](messaging.md#gotchas)).
