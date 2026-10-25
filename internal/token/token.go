// Package token provides the shared random credential generator for avenor
// subcommands and internals.
package token

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewHexToken returns a 256-bit random token encoded as 64 lowercase hex
// characters. Subcommands that mint credentials (parent-link tokens, control
// identity tokens) share this generator so the token format lives in one place.
func NewHexToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
