# Claude Code plugin

Manually registering Avenor's MCP server gives Claude Code the tools, but it leaves Claude to work out how to use them. The Avenor plugin packages the existing Go MCP server with orchestration instructions.

This is not another Avenor implementation. The plugin starts `avenor mcp`, so the same binary and MCP lifecycle described in the [MCP guide](mcp.md) remain underneath it. It shares its skill and MCP config with the [Codex plugin](codex-plugin.md): the plugin itself still lives at `marketplace/plugins/avenor`, carrying both a `.claude-plugin/` and a `.codex-plugin/` manifest side by side, pointing at the same `skills/` and `.mcp.json`. Unlike the Codex plugin, the marketplace manifest for Claude Code lives at the repository root (`.claude-plugin/marketplace.json`), because Claude Code's `owner/repo` installer only looks there — it has no option to point at a nested manifest.

## Prerequisites

- Install Avenor and make sure `avenor` is on your `PATH`.
- Install a Claude Code CLI version that supports plugins.

You can confirm that Claude Code will be able to find the binary before installing the plugin:

```bash
avenor --help
```

## Setup

1. Register the marketplace from GitHub:

```bash
claude plugin marketplace add sdougbrown/avenor
```

No local checkout is required for this form; Claude Code fetches the manifest and plugin files directly from the repository's default branch.

2. Install Avenor from the registered marketplace:

```bash
claude plugin install avenor@avenor
```

3. Start a new Claude Code session. Plugin MCP servers and skills are loaded when the session starts, so an already-open session will not pick up the integration.

### Installing from a local checkout or branch

If you're working against a fork or an unmerged branch, register the marketplace from an absolute path instead:

```bash
claude plugin marketplace add /absolute/path/to/avenor
```

Or pin a branch on GitHub directly, without cloning:

```bash
claude plugin marketplace add sdougbrown/avenor@branch-name
```

Either way, pass the repository root, not the `marketplace/` subdirectory — `.claude-plugin/marketplace.json` lives at the root and its `source` for the `avenor` plugin resolves relative to that.

## Verification

List the installed plugins:

```bash
claude plugin list
```

Inspect the plugin's component inventory:

```bash
claude plugin details avenor@avenor
```

Confirm that Avenor appears as an installed plugin from the `avenor` marketplace, with one skill (`avenor-orchestrate`) and one MCP server (`avenor`). In a new Claude Code session, asking Claude to delegate explicitly through Avenor should make the `avenor_` MCP tools available. Avenor delegation remains opt-in; the bundled skill does not dispatch ordinary work without an explicit request.

## Troubleshooting

### Claude Code cannot start the MCP server

The plugin runs `avenor mcp` by command name. Make sure the directory containing the Avenor binary is on the `PATH` inherited by Claude Code, then start a new session.

### Claude Code cannot find the marketplace

For a local checkout, pass the absolute path to the repository root, not the `marketplace/` subdirectory — `.claude-plugin/marketplace.json` lives at the repository root. For the GitHub form, confirm the manifest has landed on the branch you're targeting; `owner/repo` resolves against the default branch only.

### The plugin is installed but its tools are missing

Start a new Claude Code session after installation. MCP tools and the orchestration skill are not added retroactively to an existing session.

## See also

- [MCP server](mcp.md) covers the underlying server, transports, tools, and registry scope.
- [Codex plugin](codex-plugin.md) covers the equivalent integration for OpenAI Codex.
- [CLI reference](cli.md) covers Avenor commands outside the Claude Code integration.
