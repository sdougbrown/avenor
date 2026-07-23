#!/usr/bin/env bash
# Regenerate the checked-in agy interop Go bindings. No private descriptor input
# is used or retained: agy.proto is the reviewed, minimal Avenor-owned schema.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="v1.36.11"
KNOWN_BIN="/tmp/avenor-protobuf-tools/protoc-gen-go"
KNOWN_SHA256="3f8357089f815565cebfe24f570660fe2b1cdd493f5d50877d681727752b5cac"
TOOLS="${TMPDIR:-/tmp}/avenor-protobuf-tools-${VERSION}"
BIN="$TOOLS/protoc-gen-go"

if [[ -x "$KNOWN_BIN" ]] && [[ "$(sha256sum "$KNOWN_BIN" | awk '{print $1}')" == "$KNOWN_SHA256" ]]; then
  BIN="$KNOWN_BIN"
elif [[ ! -x "$BIN" ]] || [[ "$("$BIN" --version 2>/dev/null || true)" != "protoc-gen-go $VERSION" ]]; then
  mkdir -p "$TOOLS"
  GOBIN="$TOOLS" \
    GOCACHE="${GOCACHE:-$ROOT/.gocache}" \
    GOMODCACHE="${GOMODCACHE:-$ROOT/.gomodcache}" \
    GOPROXY="${GOPROXY:-https://proxy.golang.org}" \
    GOSUMDB="${GOSUMDB:-sum.golang.org}" \
    go install "google.golang.org/protobuf/cmd/protoc-gen-go@$VERSION"
fi

if [[ "$("$BIN" --version)" != "protoc-gen-go $VERSION" ]]; then
  echo "protoc-gen-go must be $VERSION" >&2
  exit 1
fi

cd "$ROOT"
protoc --plugin="protoc-gen-go=$BIN" --go_out=paths=source_relative:. \
  internal/runtime/agy/interop/v115/agy.proto \
  internal/runtime/agy/interop/v115/snapshot.proto
