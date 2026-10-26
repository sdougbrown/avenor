// Package token provides the shared random credential generator for avenor
// subcommands and internals.
package token

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// EnvBrokerParentToken carries the parent-link credential to `avenor stable`.
// It is defined here so both the CLI that reads it and the MCP spawn path that
// must keep it out of nested environments agree on the name.
const EnvBrokerParentToken = "AVENOR_PARENT_BROKER_TOKEN"

// EnvControlIdentityToken carries the supervisor ownership credential through
// the nested supervisor's inherited readiness pipe. It is defined here so the
// MCP spawn path and the control server that reads it back agree on the name.
const EnvControlIdentityToken = "AVENOR_CONTROL_IDENTITY_TOKEN"

// NewHexToken returns a 256-bit random token encoded as 64 lowercase hex
// characters. Subcommands that mint credentials (parent-link tokens, control
// identity tokens) share this generator so the token format lives in one place.
func NewHexToken() (string, error) {
	return NewHex(32)
}

// NewHex returns n random bytes encoded as lowercase hex. Callers with an
// existing token size contract pass their size explicitly so consolidating the
// generator does not silently change token widths.
func NewHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
