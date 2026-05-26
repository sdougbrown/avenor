#!/usr/bin/env bash
set -euo pipefail

# Install avenor — the agent orchestration harness.
#
# Usage:
#   curl -fsSL https://avenor.douggo.com/install.sh | sh
#   curl -fsSL https://avenor.douggo.com/install.sh | sh -s -- --global
#   AVENOR_VERSION=v0.3.1 curl -fsSL https://avenor.douggo.com/install.sh | sh

REPO="sdougbrown/avenor"
GLOBAL=0
VERSION="${AVENOR_VERSION:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --global)          GLOBAL=1; shift ;;
    --version)         VERSION="$2"; shift 2 ;;
    --version=*)       VERSION="${1#*=}"; shift ;;
    -h|--help)
      echo "Usage: install.sh [--global] [--version <tag>]"
      echo ""
      echo "  --global          Install to /usr/local/bin (requires sudo)."
      echo "                    Default: installs to ~/.local/bin."
      echo "  --version <tag>   Install a specific release tag (e.g. v0.3.1)."
      echo "                    Default: latest release."
      echo ""
      echo "  AVENOR_VERSION=<tag>  Environment variable alternative to --version."
      exit 0 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ── Install dir ───────────────────────────────────────────────────────────
if [ "$GLOBAL" = "1" ]; then
  BIN_DIR="/usr/local/bin"
else
  BIN_DIR="${HOME}/.local/bin"
  mkdir -p "$BIN_DIR"
fi
TARGET="${BIN_DIR}/avenor"

# ── Platform detection ────────────────────────────────────────────────────
case "$(uname -s)" in
  Darwin) OS=darwin ;;
  Linux)  OS=linux ;;
  *) echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=amd64 ;;
  *) echo "Unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac

# ── Resolve version ───────────────────────────────────────────────────────
if [ -z "$VERSION" ]; then
  echo "Fetching latest release…"
  VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name":' \
    | sed 's/.*"tag_name": "\(.*\)".*/\1/')
  if [ -z "$VERSION" ]; then
    echo "Could not determine latest version. Set AVENOR_VERSION explicitly." >&2
    exit 1
  fi
fi

# ── Skip if already up to date ────────────────────────────────────────────
if [ -x "$TARGET" ] && [ ! -L "$TARGET" ]; then
  INSTALLED=$("$TARGET" --version 2>/dev/null | awk '{print $NF}' || true)
  if [ "$INSTALLED" = "$VERSION" ] || [ "v$INSTALLED" = "$VERSION" ]; then
    echo "avenor $VERSION is already installed at $TARGET."
    exit 0
  fi
fi

ASSET="avenor_${OS}_${ARCH}"
BASE="https://github.com/${REPO}/releases/download/${VERSION}"

echo "Installing avenor ${VERSION} (${OS}/${ARCH})…"

TMP=$(mktemp -d)
trap "rm -rf '$TMP'" EXIT

# ── Download ──────────────────────────────────────────────────────────────
if ! curl -fsSL -o "${TMP}/${ASSET}" "${BASE}/${ASSET}"; then
  echo "Download failed: ${BASE}/${ASSET}" >&2
  exit 1
fi
if ! curl -fsSL -o "${TMP}/checksums.txt" "${BASE}/checksums.txt"; then
  echo "Download failed: ${BASE}/checksums.txt" >&2
  exit 1
fi

# ── Verify checksum ───────────────────────────────────────────────────────
(
  cd "$TMP"
  if ! grep -E "  ${ASSET}$" checksums.txt | shasum -a 256 -c - >/dev/null 2>&1; then
    echo "Checksum verification failed for ${ASSET}" >&2
    exit 1
  fi
)

# ── Install ───────────────────────────────────────────────────────────────
if [ "$GLOBAL" = "1" ]; then
  sudo install -m 0755 "${TMP}/${ASSET}" "$TARGET"
else
  install -m 0755 "${TMP}/${ASSET}" "$TARGET"
fi

echo "Installed avenor ${VERSION} → ${TARGET}"

# ── PATH reminder ─────────────────────────────────────────────────────────
if ! command -v avenor &>/dev/null 2>&1; then
  echo ""
  echo "Note: ${BIN_DIR} is not in your PATH. Add to your shell profile:"
  echo "  export PATH=\"${BIN_DIR}:\$PATH\""
fi
