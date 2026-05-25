# Avenor

Most agent harnesses will only let you call a single layer of sub-agents. That's normally all you need. For large projects, you may want to reach for more advanced orchestration models. (I did.)

Avenor allows any given top-level agent (the one you're chatting with) to kick off an entirely separate process that is no longer bound by the single-level sub-agent restriction. Now your sub-agents can call sub-agents. Let your horses run wild!

The way I have this organized personally is to have a "jockey" agent that is write-restricted, forcing it to spawn "horse" or "mule" sub-agents to do write-oriented work for it. An amusing pattern, and keeps implementing agents from getting confused and doing the wrong thing. Each agent above keeps focusing the prompts so that the implementors don't get distracted and start doing things they aren't supposed to.

Avenor supports six backends: `opencode-acp` (default), `opencode-http`, `codex-app-server`, `gemini-acp`, `cursor-acp`, and `pi`. For MCP-compatible clients, `avenor mcp` is the canonical Go-native MCP server — no Node/Bun required. The Node.js packages (`@dougbots/avenor-mcp`, `@dougbots/avenor-opencode`) remain for OpenCode integration. See [docs/mcp.md](docs/mcp.md) for MCP setup and tool details. Get your agent to check the docs to ensure it uses the CLI correctly. (Hilariously, this CLI is intended for your agent to use, not you as a human.)

A template pack in [`templates/`](templates/) covers the full dispatch matrix: Claude Code dispatching to OpenCode jockey, Claude Code dispatching to Codex, and Codex dispatching to OpenCode jockey. Each folder has agent prompts, permission boundaries, and dispatch skills ready to adapt to your own setup. For the full CLI reference and common orchestration patterns, start at [docs/cli.md](docs/cli.md).

## Installation

Grab the release asset for your platform from [GitHub Releases](https://github.com/sdougbrown/avenor/releases), make it executable, and check that it runs. On macOS arm64:

```bash
curl -fsSL https://github.com/sdougbrown/avenor/releases/latest/download/avenor_darwin_arm64 -o avenor
chmod +x avenor
./avenor --version
```

## Development

This repo is Go-first, with a small JS workspace for package integrations. `mise` is the convenience layer for common local tasks; it wraps the underlying `go` and `bun` CLI commands rather than replacing them.

```bash
mise run build      # Go binary + JS packages
mise run test       # Go tests + JS package tests

mise run go-build   # Go binary only
mise run go-test    # Go tests only
mise run js-build   # JS packages only
mise run js-test    # JS tests only
```

The direct equivalents are still ordinary commands such as `go build -o avenor ./cmd/avenor`, `go test ./...`, `bun run build`, and `bun run test`.

## Permission handling

Permission handling matters because a backend can ask for approval mid-run, and Avenor's job is to broker that request without turning the harness into a blocking human-in-the-loop primitive. When your backend forwards tool approval through ACP `session/request_permission`, point `--permission-handler` at a file path:

```bash
--permission-handler file:<path>
```

Avenor writes the request there; `avenor answer <path> --option <id>` writes the response back atomically. See [docs/permission-handler.md](docs/permission-handler.md) for the request and response JSON shapes.

## Control sockets

Avenor can expose a Unix-domain control socket so another process can inspect status, tail live events, answer permissions, cancel work, and send follow-up prompts while a run is active:

```bash
avenor run \
  --control-socket /tmp/avenor.sock \
  --prompt "List the files in this directory and exit." \
  --on-event /tmp/events.ndjson \
  --sentinel-file /tmp/done.env

avenor control --socket /tmp/avenor.sock status
avenor control --socket /tmp/avenor.sock tail
avenor control --socket /tmp/avenor.sock prompt "Continue with the next step"
avenor control --socket /tmp/avenor.sock cancel
```

For long-lived orchestration, `avenor stable` starts a supervisor that can spawn and manage multiple child runtimes:

```bash
avenor stable --control-socket /tmp/avenor-stable.sock

avenor control --socket /tmp/avenor-stable.sock spawn \
  --prompt "Review PR #42" \
  --dir /repo/A \
  --label review-42

avenor control --socket /tmp/avenor-stable.sock list
avenor control --socket /tmp/avenor-stable.sock prompt "Continue" rt_1
avenor control --socket /tmp/avenor-stable.sock cancel rt_1
avenor control --socket /tmp/avenor-stable.sock shutdown graceful
```

The socket also speaks newline-delimited JSON-RPC 2.0 directly, and `--http-debug` can expose loopback-only HTTP/SSE endpoints for debugging. See [docs/control-protocol.md](docs/control-protocol.md) and [docs/stable.md](docs/stable.md) for the full method list, event stream, ownership rules, and stable supervisor reference.

## Phase loops

When a single prompt isn't enough — build once, then test → review → fix until clean — define a loop config and let Avenor run the phases:

```bash
avenor run --loop-file loop.json --auto-approve --sentinel-file run.done
```

Phases emit `[loop: exit]` to finish clean or `[loop: abort | reason]` to escalate. Pre phases run once. Loop phases repeat until exit, abort, or `max_iterations`. See [docs/loop.md](docs/loop.md) for the full config reference, prompt templates, lifecycle events, and abort mechanics.

## Event monitoring

Every run writes a structured NDJSON event log. Tail and classify it while a run is active:

```bash
avenor watch --follow --classify /tmp/events.ndjson
```

Events are typed (`agent.message_chunk`, `tool.call`, `permission.request`, `session.end`, etc.) and carry enough context to drive automated handling — permission responses, downstream triggers, or log aggregation. See [docs/events.md](docs/events.md) and [docs/watch.md](docs/watch.md).

## Name

*Avenor* is the chief stable officer of a king, a nod to the horse/mule/groom/jockey vocabulary already used in agent orchestration frameworks.

Someone still has to clean out the stables, but at least the naming keeps the chore list from bolting.
