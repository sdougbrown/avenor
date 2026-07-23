# agy RPC evidence (1.1.5)

Evidence-only reproduction notes for the private loopback service. The implementation boundary is recorded at [`internal/runtime/agy/testdata/rpc/v1.1.5`](../../../internal/runtime/agy/testdata/rpc/v1.1.5/).

## Local capture procedure

Run an interactive `agy 1.1.5` in a PTY host that executes `agy` directly. Discover only listeners owned by that exact child PID (macOS: `lsof -nP -a -p <pid> -iTCP -sTCP:LISTEN`; Linux: PID fd/socket inode correlation). Do not scan global listeners.

For each parsed loopback candidate, independently identify TLS or plain HTTP, use a proxy-disabled per-endpoint client, and send a framed protobuf `Heartbeat` request to:

```
POST /exa.language_server_pb.LanguageServerService/Heartbeat
Content-Type: application/grpc-web+proto
body: 00 00 00 00 00
```

The validated service uses gRPC-Web protobuf (data `0x00`; trailer `0x80`), not JSON or Connect JSON. Probe `GetConversationMetadata` after Heartbeat and compare its nested metadata identity with the cascade/session being hosted. Prefer TLS; do not silently downgrade a selected TLS endpoint to plain HTTP.

## Safety and retention

Use disposable workspaces and capture only redacted structural records. Never retain prompts, replies, opaque IDs, ports, certificates, home paths, PIDs, or cache files. Run the corpus redaction scan before adding fixtures.

The retained extracted descriptors are an incomplete import closure. Do not generate bindings, write manual protobuf structs, or implement RPC from them. Obtain a complete versioned closure and review its descriptor diff first.
