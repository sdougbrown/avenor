package workflow

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// DefaultLeaseTTL is the lease TTL applied when neither a node's lease policy
// nor the template default declares one. It must be strictly positive and in
// the future so a claimed lease (including one reconstructed from replay) is
// never swept as zero-expiry on recovery.
const DefaultLeaseTTL = 30 * time.Second

// DefaultHeartbeatInterval is the advisory heartbeat cadence used when no
// policy declares one.
const DefaultHeartbeatInterval = 10 * time.Second

// leaseTTL resolves the effective, strictly positive lease duration for a
// node, honoring the node's lease policy first, then the template default,
// then DefaultLeaseTTL.
func leaseTTL(node *NodeDefinition, templateDefault *LeasePolicy) time.Duration {
	if node != nil && node.LeasePolicy != nil && node.LeasePolicy.TTLSeconds > 0 {
		return time.Duration(node.LeasePolicy.TTLSeconds) * time.Second
	}
	if templateDefault != nil && templateDefault.TTLSeconds > 0 {
		return time.Duration(templateDefault.TTLSeconds) * time.Second
	}
	return DefaultLeaseTTL
}

// newOwnerToken returns a random opaque owner token. The raw token is given
// to the claimer only; only a digest of it is recorded on the lease.
func newOwnerToken() string {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand never fails on the platforms we support; degrade to a
		// time-seeded value so a claim can still proceed.
		sum := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
		return hex.EncodeToString(sum[:])
	}
	return hex.EncodeToString(buf)
}

// ownerTokenDigest hashes an owner token so the raw token is never persisted.
func ownerTokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}
