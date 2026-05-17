# Backends

Avenor supports three backends: `opencode-http` (remote server), `opencode-acp` (subprocess), and `codex-app-server`.

## Backend selection

Pass `--backend` to choose the runtime:

```sh
avenor --server-url http://127.0.0.1:4096 --prompt "say hi"
avenor --backend opencode-acp --prompt "say hi"
avenor --backend codex-app-server --prompt "say hi"
```

The default backend is `opencode-http`, which requires an already-running `opencode serve` endpoint. Provide it with `--server-url` or `AVENOR_OPENCODE_URL`.

## opencode-acp

Uses OpenCode's ACP JSON-RPC protocol over stdio. Spawns `opencode acp --pure` as a subprocess.

### Discovery

1. `--server-url <url>`
2. `AVENOR_OPENCODE_URL`
3. Spawn `opencode acp --pure` for this Avenor invocation

If a URL is provided, the ACP backend fails cleanly — it does not support network transport.

### Capabilities

| Capability | Supported |
|---|---|
| New sessions | ✓ |
| Session resume | ✓ |
| Prompt execution | ✓ |
| Cancel | ✓ |
| Event streaming | ✓ |
| Permission relay | ✓ |
| Model selection | ✓ (via `SetSessionMode`/`SetSessionModel`) |
| External server URL | ✗ |
| Subprocess discovery | ✓ (subprocess fallback) |

## opencode-http

The default backend. Talks to `opencode serve` over its HTTP API. Requires `--server-url` or `AVENOR_OPENCODE_URL` pointing at a running `opencode serve` instance.

### Starting the server

```sh
opencode serve --port 4096 --pure
```

### Basic usage

```sh
avenor \
  --backend opencode-http \
  --server-url http://127.0.0.1:4096 \
  --agent jockey \
  --model deepseek/deepseek-v4-pro \
  --prompt "say ok" \
  --on-event /tmp/avenor-http.ndjson \
  --sentinel-file /tmp/avenor-http.env
```

### Auth

The server supports HTTP basic auth. Pass credentials via the URL:

```sh
--server-url http://user:pass@127.0.0.1:4096
```

Or set `OPENCODE_SERVER_USERNAME` and `OPENCODE_SERVER_PASSWORD` environment variables (future support).

### Agent and model

Agent and model are forwarded on every prompt. The `--model` string uses `providerID/modelID` format:

```sh
--model deepseek/deepseek-v4-pro
```

This is split and sent as `{"providerID":"deepseek","modelID":"deepseek-v4-pro"}` to the server.

### Capabilities

| Capability | Supported |
|---|---|
| New sessions | ✓ |
| Session resume | ✓ (via `GET /session/:id`) |
| Prompt execution | ✓ |
| Cancel | ✓ (via `POST /session/:id/abort`) |
| Event streaming | ✓ (SSE over `GET /event`) |
| Permission relay | ✗ (not yet verified) |
| Model selection | ✓ (set per prompt) |
| External server URL | ✓ (required) |
| Subprocess discovery | ✗ |

### Known differences from ACP

- **Event stream is global.** The SSE `/event` endpoint delivers events for all sessions on the server. The provider filters by session ID locally.
- **Session end detection.** Uses the message response finish status; SSE idle transitions are ignored as terminal signals because they can arrive late for prior turns.
- **Working directory.** HTTP mode does not support per-session `--dir`; start `opencode serve` in the target directory instead.
- **Resume.** Checks `GET /session/:id` for existence. No dedicated resume endpoint.
- **Permissions.** Permission request/response behavior has not been verified for HTTP mode yet. The server may auto-approve tools depending on configuration.
