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
| Broker messages | [broker-messaging.md](broker-messaging.md) | spawned child ↔ supervisor ask/reply over the broker; the child-ask leg does not pass with the installed extension — [messaging.md](messaging.md#the-bound-generation) step 3 is the recipe that does |
| Broker feed | [feed-broker.md](feed-broker.md) | `avenor feed broker` → poo feed NDJSON |
| Messaging tools | [messaging.md](messaging.md) | `avenor_message` (six actions) + `avenor_peers` (list/rename/last) from a host session and its children |
| Child message CLI | [message-cli.md](message-cli.md) | `avenor message --action tell|ask|reply|withdraw` as a broker child |
| Owner message bind | [message-bind.md](message-bind.md) | owner-only `message_bind` over the stable control socket → credentials + typed refusals |
| Pi completion notices | [pi-completions.md](pi-completions.md) | real `pi --mode rpc` host + `avenor` supervisor + `pi` children → footer `avenor` status and `avenor-completion` messages |
| Config validation | [verify-command.md](verify-command.md) | `avenor verify` |
| Launch identity | [provenance.md](provenance.md) | `avenor run --team-file` → MCP env + phase/team events |
| Workflow launch wrappers | [workflow-launch-wrappers.md](workflow-launch-wrappers.md) | `avenor stable` + `avenor workflow` → wrapper argv + marker completion |
| Workflow retry backoff | [retry-backoff.md](retry-backoff.md) | `avenor stable` + `avenor workflow` → held activation + timed second attempt |
| Claude peer layer | [claude-peers.md](claude-peers.md) | plugin `hooks/` mods layer → `SendMessage({to:"avenor:*"})`, inbound prompt turn, blocking `ask` |
| Top-level Pi–Claude peers | [top-level-peers.md](top-level-peers.md) | independent sessions → central broker over outbound polling, combined local-child peers, identity lifecycle |
| Codex peer layer | [codex-peers.md](codex-peers.md) | plugin `hooks/codex.json` command hooks → identity-bound `message`/`peers`, `codex queue` idle delivery |
| OpenCode conversations | [opencode-conversations.md](opencode-conversations.md) | native OpenCode host per-conversation messaging (tell, ask, reply, withdraw, child-parent routing) |
