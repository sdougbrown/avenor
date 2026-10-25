package parentlink

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

// BranchID derives a branch's durable identity at its parent broker from the
// supervisor identity that survives a restart — the control socket path — and
// the host that path lives on. A branch that comes back after an unclean stop
// on the host it ran on registers under the same identity, so it reclaims the
// runs it held instead of leaving them behind. The host keeps two branches on
// different hosts that mount the same socket path distinct: the registry keys
// a claim by this identity alone, so identical paths under one parent would
// otherwise adopt and retire each other's runs every refresh tick.
//
// The readable prefix is the sanitized file stem, and the 64-bit digest keeps
// two branches whose stems collide distinct — and one branch's identity not
// guessable from another's — without depending on process state.
func BranchID(identity, host string) string {
	if identity == "" {
		return ""
	}
	stem := strings.TrimSuffix(filepath.Base(identity), filepath.Ext(identity))
	sum := sha256.Sum256([]byte(host + "\x00" + identity))
	return sanitizeBranchToken(stem) + "-" + hex.EncodeToString(sum[:8])
}

// sanitizeBranchToken reduces stem to the characters a run ID and a peer
// listing carry cleanly: letters, digits, '-' and '_'.
func sanitizeBranchToken(stem string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range stem {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
			lastDash = false
		case !lastDash:
			b.WriteRune('-')
			lastDash = true
		}
	}
	token := strings.Trim(b.String(), "-")
	if token == "" {
		return "branch"
	}
	return token
}
