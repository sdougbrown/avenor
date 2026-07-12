# Codex plugin

Manually registering Avenor's MCP server gives Codex the tools, but it leaves Codex to work out how to use them. The Avenor plugin packages the existing Go MCP server with orchestration instructions and Codex-facing metadata.

This is not another Avenor implementation. The plugin starts `avenor mcp`, so the same binary and MCP lifecycle described in the [MCP guide](mcp.md) remain underneath it.

## Prerequisites

- Install Avenor and make sure `avenor` is on your `PATH`.
- Check out the Avenor repository locally.
- Install a Codex CLI version that supports plugins.

You can confirm that Codex will be able to find the binary before installing the plugin:

```bash
avenor --help
```

## Setup

1. Register the marketplace from your checkout. Replace `/absolute/path/to/avenor` with the absolute path to the repository:

```bash
codex plugin marketplace add /absolute/path/to/avenor/marketplace
```

2. Install Avenor from the registered marketplace:

```bash
codex plugin add avenor@avenor
```

3. Start a new Codex task. Plugin MCP servers and skills are loaded when the task starts, so an already-open task will not pick up the integration.

The local checkout flow above is the currently verified installation path. Direct installation from a GitHub location is not documented yet; clone or update the repository, then register its `marketplace` directory.

## Verification

List the installed plugins:

```bash
codex plugin list
```

Confirm that Avenor appears as an installed plugin from the `avenor` marketplace. In a new Codex task, asking Codex to delegate explicitly through Avenor should make the `avenor_` MCP tools available. Avenor delegation remains opt-in; the bundled skill does not dispatch ordinary work without an explicit request.

## Troubleshooting

### Codex cannot start the MCP server

The plugin runs `avenor mcp` by command name. Make sure the directory containing the Avenor binary is on the `PATH` inherited by Codex, then start a new task.

### Codex cannot find the marketplace

Pass the absolute path to the repository's `marketplace` directory. That directory contains `.agents/plugins/marketplace.json`; passing the repository root will not register it.

### The plugin is installed but its tools are missing

Start a new Codex task after installation. MCP tools and the orchestration skill are not added retroactively to an existing task.

## See also

- [MCP server](mcp.md) covers the underlying server, transports, tools, and registry scope.
- [Claude Code plugin](claude-plugin.md) covers the equivalent integration for Claude Code.
- [CLI reference](cli.md) covers Avenor commands outside the Codex integration.
