# Codex plugin

Manually registering Avenor's MCP server gives Codex the tools, but it leaves Codex to work out how to use them. The Avenor plugin packages the existing Go MCP server with orchestration instructions and Codex-facing metadata.

This is not another Avenor implementation. The plugin starts `avenor mcp`, so the same binary and MCP lifecycle described in the [MCP guide](mcp.md) remain underneath it.

## Prerequisites

- Install Avenor and make sure `avenor` is on your `PATH`.
- Install a Codex CLI version that supports plugins.

You can confirm that Codex will be able to find the binary before installing the plugin:

```bash
avenor --help
```

## Setup

1. Register Avenor's marketplace from GitHub:

```bash
codex plugin marketplace add sdougbrown/avenor
```

2. Install Avenor from the registered marketplace:

```bash
codex plugin add avenor@avenor
```

3. Start a new Codex task. Plugin MCP servers and skills are loaded when the task starts, so an already-open task will not pick up the integration.

For GitHub and repository-root installs, Codex and Claude Code both consume `.claude-plugin/marketplace.json`. That manifest points to `marketplace/plugins/avenor`, where Codex loads `.codex-plugin/plugin.json`. The separate `marketplace/.agents/plugins/marketplace.json` supports registering only the `marketplace/` subdirectory.

### Installing from a local checkout or branch

The GitHub source above is the normal installation path. To test local plugin changes, register the repository root instead. Replace `/absolute/path/to/avenor` with the checkout's absolute path:

```bash
codex plugin marketplace add /absolute/path/to/avenor
```

To test a Git branch without cloning it, pin that ref when registering the GitHub source:

```bash
codex plugin marketplace add sdougbrown/avenor --ref feature/my-plugin-change
```

These are alternative marketplace sources; use one registration method, then run `codex plugin add avenor@avenor` as usual.

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

Check whether the marketplace was registered:

```bash
codex plugin marketplace list
```

If `avenor` is missing, retry with the exact owner and repository name:

```bash
codex plugin marketplace add sdougbrown/avenor
```

This step needs network access to GitHub. If GitHub registration still fails and you have a local checkout, register its absolute repository-root path instead:

```bash
codex plugin marketplace add /absolute/path/to/avenor
```

### The plugin is installed but its tools are missing

Start a new Codex task after installation. MCP tools and the orchestration skill are not added retroactively to an existing task.

## See also

- [MCP server](mcp.md) covers the underlying server, transports, tools, and registry scope.
- [Claude Code plugin](claude-plugin.md) covers the equivalent integration for Claude Code.
- [CLI reference](cli.md) covers Avenor commands outside the Codex integration.
