# agy 1.1.5 / 1.1.7 interop schema

`agy.proto` is Avenor-owned and intentionally minimal. It retains only the
Stage 9 validated unary boundaries plus the shared state-update, step,
interaction, tool, text, status, and usage fields needed by later agy stages.
It is not a copy of agy's private schema closure. Fields not declared here are
protobuf unknown fields and therefore survive decode/re-marshal.

`parity-manifest.json` maps every retained descriptor symbol to its original
1.1.5 symbol and accepts the independently validated, declaration-identical
1.1.7 closure. The full private closure is caller-supplied only for validation:

```sh
AGY_DESCRIPTOR_SET=/path/to/descriptor-set.pb \
  GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache \
  go test ./internal/runtime/agy/interop/v115 -run TestParityAgainstSuppliedClosure
```

The expected closure SHA-256 is embedded in the manifest. The normal test
suite never reads that path.

Regenerate bindings with the pinned generator:

```sh
GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache \
  scripts/generate-agy-interop-proto.sh
AGY_VERIFY_GENERATION=1 GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache \
  go test ./internal/runtime/agy/interop/v115 -run TestDeterministicGenerationClean
```

The script accepts the trusted local `protoc-gen-go` 1.36.11 binary only when
its supplied SHA-256 matches; otherwise it builds exactly v1.36.11 in a
temporary tool directory using the Go module proxy. Tool binaries and complete
private descriptor sets are not committed.

This package declares service descriptors only. It implements no RPC framing,
discovery, HTTP/TLS, unary or streaming behavior, event mapping, PTY hosting,
or provider integration.
