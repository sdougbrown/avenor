# Remote MCP deployment

## What this deploys

Two service-managed processes on a tailnet host:

- a durable `avenor stable` supervisor, restarted on failure by the service manager
- an HTTP MCP server (`avenor mcp --transport http`) that dials the supervisor over its Unix control socket

The MCP endpoint is what remote clients talk to. The control socket stays Unix-only — it is never exposed over the network.

## Prerequisites

- `avenor` on `PATH` (or edit the binary path in the unit files)
- a Tailscale tailnet
- a hostname like `<host>.<tailnet>.ts.net` for the host

## Token

Generate a token file the MCP server reads. The `umask 077` subshell makes the file 0600 from creation, so it is never world-readable before `chmod` runs:

```bash
mkdir -p "$HOME/Library/Application Support/avenor/remote"
( umask 077; openssl rand -hex 32 > "$HOME/Library/Application Support/avenor/remote/token" ) && chmod 600 "$HOME/Library/Application Support/avenor/remote/token"
```

On Linux, use your config directory instead (the templates use `/etc/avenor/remote`):

```bash
install -d -m 700 /etc/avenor/remote
( umask 077; openssl rand -hex 32 > /etc/avenor/remote/token ) && chmod 600 /etc/avenor/remote/token
```

## Install

### macOS (launchd)

```bash
mkdir -p "$HOME/Library/Application Support/avenor/remote/logs"
cp templates/remote-mcp/avenor-stable.plist templates/remote-mcp/avenor-mcp.plist ~/Library/LaunchAgents/
```

Adjust the placeholders in both plists (socket path, token path, allowed host, binary path, capacity numbers), then load:

```bash
launchctl load ~/Library/LaunchAgents/dev.avenor.stable.plist
launchctl load ~/Library/LaunchAgents/dev.avenor.mcp.plist
```

LaunchAgents only run while you have an active login session. After a reboot, a headless Mac has no MCP server until someone logs in. For durability, use the LaunchDaemon variant instead: install the plists in `/Library/LaunchDaemons/` (the system domain) and add a `UserName=` key so they run as your user.

### Linux (systemd)

Create a dedicated non-root user and the state/config directories. BOTH units run as this user — the control socket is created 0600, so the MCP server can only dial it when the two processes share the UID:

```bash
sudo useradd -r -s /usr/sbin/nologin avenor
install -d -m 700 /var/lib/avenor/remote /etc/avenor/remote
cp templates/remote-mcp/avenor-stable.service templates/remote-mcp/avenor-mcp.service /etc/systemd/system/
```

Adjust the placeholders in both units, then:

```bash
systemctl daemon-reload
systemctl enable --now avenor-stable.service
systemctl enable --now avenor-mcp.service
```

## Expose via Tailscale

```bash
tailscale serve --bg --https=443 http://127.0.0.1:3748
```

## Smoke test

Allowed Host, no token → 401:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: <host>.<tailnet>.ts.net' http://127.0.0.1:3748/mcp
```

Allowed Host + token → not 401/403. Write the token to a header file first so it never appears in `curl`'s argv (visible via `ps` for the duration of the request):

```bash
# macOS:
hdr="$(mktemp "${TMPDIR:-/tmp}/avenor-auth-header.XXXXXX")"
printf 'Authorization: Bearer %s\n' "$(cat "$HOME/Library/Application Support/avenor/remote/token")" > "$hdr"
# Linux:
hdr="$(mktemp "${TMPDIR:-/tmp}/avenor-auth-header.XXXXXX")"
printf 'Authorization: Bearer %s\n' "$(cat /etc/avenor/remote/token)" > "$hdr"

curl -s -o /dev/null -w '%{http_code}\n' -H @"$hdr" -H 'Host: <host>.<tailnet>.ts.net' http://127.0.0.1:3748/mcp
rm -f "$hdr"
```

`mktemp` creates the file 0600 atomically, so a stale or planted `/tmp` path cannot leak the token.

Unlisted Host → 403:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: evil.example' http://127.0.0.1:3748/mcp
```

## Capacity

`--max-runtimes` (default 16) and `--max-tree-budget` (default 64) are tuned for a single local session. A remote host serving several clients should raise both. The tree budget is an independent aggregate bound including nested supervisors; it may be lower than the local limit and can constrain top-level concurrency. The templates set them explicitly as placeholders — raise them deliberately. See [docs/stable.md](../../docs/stable.md#max-runtimes-limit) for what each bound means.

## Client contract

What a remote MCP caller should do:

- send `idempotency_key` on `avenor_spawn` and `avenor_follow_up` so retries never double-spawn
- send `request_id` on `avenor_answer_permission` (required to retry a resolved answer)
- poll `avenor_events` with `after_seq` starting at `0`, feeding back `latest_seq`
- treat `wait_clamped: true` as "poll again"
- on `supervisor unavailable`, back off until the supervisor is ready, then retry with the same key

## Known limits

- LaunchAgents only run while the user has an active login session; a headless Mac has no MCP server after a reboot until someone logs in (use the LaunchDaemon variant for durability)
- idempotency records and the supervisor's runtimes are lost when the supervisor process restarts
- parked runtimes are reaped after `--parked-timeout`
- the wait budget is approximate: an in-flight RPC may add up to its own timeout
