# agy 1.1.5 gRPC-Web evidence corpus

This is a minimal, redacted, fixture-ready record of the 2026-07-23 local `agy 1.1.5` protocol capture. It intentionally excludes raw prompts/replies, opaque IDs, ports, certificates, home paths, process IDs, caches, and the original multi-hundred-KB captures.

## Validated protocol

- Service: `exa.language_server_pb.LanguageServerService`.
- Unary request body: gRPC-Web binary envelope: flag `0x00`, four-byte big-endian payload length, protobuf payload; `Content-Type: application/grpc-web+proto`.
- Unary response: zero or more `0x00` protobuf data envelopes followed by a `0x80` ASCII trailer envelope. The validated success trailer is `grpc-status: 0\r\n`.
- gRPC method failures may be HTTP 200 with `Grpc-Status` / `Grpc-Message`; incompatible content types returned HTTP 400/415. This is neither JSON nor Connect JSON.
- The launched exact PID owned two loopback listeners: one TLS 1.3, self-signed localhost listener and one plain HTTP listener. Both exposed the service over the correct respective transport. Discovery must enumerate exact-PID listeners, reject non-loopback/unowned candidates, probe framed Heartbeat, then compare metadata identity. Prefer TLS; a plain fallback must be explicit, never a silent TLS downgrade.

`heartbeat-grpcweb.json` is the only retained byte-level capture. `unary-shapes.json` preserves every validated unary method's typed request/response boundary without retaining opaque values. `listener-roles.json` preserves transport selection evidence without addresses. Tests consume all JSON fixtures.

## Schema provenance and boundary

`schema-provenance.json` pins SHA-256s for the descriptors extracted from the installed 1.1.5 binary and for source captures. A complete 102-descriptor closure was subsequently recovered locally and validated by its recorded SHA-256 with zero missing imports or unresolved type references. It is intentionally **not retained** because it includes unrelated private schemas. The checked-in `internal/runtime/agy/interop/v115` schema is instead a small Avenor-owned wire-compatible view, with every retained symbol mapped back to the complete closure by a parity manifest and caller-supplied parity check.

`stream-agent-state-updates-structural.json` is a clearly labeled redacted structural fixture generated from the parity-checked minimal schema; it is not a raw live capture. Stage 11 consumes the stream framing and typed boundary facts for decoding only. `FIELD_MAP.md` and `RECONNECT_BRIEF.md` remain evidence-only inputs for deferred Stage 12 mapping, reconciliation, and reconnect work; this corpus does not add PTY hosting, transport selection, or provider integration.

## Redaction

`rpc_fixtures_test.go` parses the fixtures, validates the protocol facts, and rejects common home-path, loopback-port, certificate, PID, and opaque-ID patterns. The explicit Stage 9 scan is recorded in `REDACTION.md`.
