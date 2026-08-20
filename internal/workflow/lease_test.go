package workflow

import (
	"testing"
	"time"
)

func TestLeaseTTLResolution(t *testing.T) {
	tests := []struct {
		name            string
		node            *NodeDefinition
		templateDefault *LeasePolicy
		expectedSeconds int64
	}{
		{"nil node + nil template default returns default", nil, nil, 30},
		{"template default overrides default", nil, &LeasePolicy{TTLSeconds: 5}, 5},
		{"node policy overrides template default", &NodeDefinition{LeasePolicy: &LeasePolicy{TTLSeconds: 9}}, &LeasePolicy{TTLSeconds: 5}, 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := leaseTTL(tt.node, tt.templateDefault)
			expected := time.Duration(tt.expectedSeconds) * time.Second
			if result != expected {
				t.Fatalf("leaseTTL: got %v want %v", result, expected)
			}
		})
	}
}

func TestOwnerTokenDigest(t *testing.T) {
	token1 := "token-12345"
	digest := ownerTokenDigest(token1)
	if got := digest[:7]; got != "sha256:" {
		t.Fatalf("ownerTokenDigest prefix: got %q want sha256:", got)
	}
	hexLen := len(digest) - len("sha256:")
	if hexLen != 64 {
		t.Fatalf("ownerTokenDigest length: got %d want 64", hexLen)
	}

	token2 := "different-token"
	digest2 := ownerTokenDigest(token2)
	if digest == digest2 {
		t.Fatalf("ownerTokenDigest equality: same digest for different tokens")
	}
	token3 := "token-12345"
	digest3 := ownerTokenDigest(token3)
	if digest != digest3 {
		t.Fatalf("ownerTokenDigest inequality: equal tokens produce different digests")
	}

	// Ensure raw token is never persisted: digest is not the raw token.
	if digest == token1 {
		t.Fatalf("ownerTokenDigest leaked raw token")
	}
}

func TestNewOwnerTokenUnique(t *testing.T) {
	token1 := newOwnerToken()
	if token1 == "" {
		t.Fatalf("newOwnerToken returned empty string")
	}
	token2 := newOwnerToken()
	if token1 == token2 {
		t.Fatalf("newOwnerToken returned duplicate tokens (unlikely)")
	}
}

func TestApplyLeasedStampsExpiryOnReduction(t *testing.T) {
	state := newInstance(t)
	actID := state.Instance.Activations[0].ID
	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-expiry",
		Identity: ExecutionIdentity{
			WorkflowID:   "wf1",
			NodeID:       "start",
			ActivationID: actID,
		},
		LeaseID: "lease-e",
		Actor:   "alice",
		Lease: &Lease{
			ID:           "lease-e",
			ActivationID: actID,
			Owner:        "alice",
			TokenDigest:  "sha256:abc",
			AcquiredAt:   time.Unix(1000, 0).UTC(),
			ExpiresAt:    time.Unix(2000, 0).UTC(),
		},
	}, "claim with expiry")
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("activations count: got %d want 1", len(state.Instance.Activations))
	}
	act := &state.Instance.Activations[0]
	if act.Status != ActivationLeased {
		t.Fatalf("status: got %q want %q", act.Status, ActivationLeased)
	}
	if act.ActiveLease == nil {
		t.Fatalf("ActiveLease is nil")
	}
	if act.ActiveLease.TokenDigest != "sha256:abc" {
		t.Fatalf("TokenDigest: got %q want sha256:abc", act.ActiveLease.TokenDigest)
	}
	if !act.ActiveLease.AcquiredAt.Equal(time.Unix(1000, 0).UTC()) {
		t.Fatalf("AcquiredAt: got %v want %v", act.ActiveLease.AcquiredAt, time.Unix(1000, 0).UTC())
	}
	if !act.ActiveLease.ExpiresAt.Equal(time.Unix(2000, 0).UTC()) {
		t.Fatalf("ExpiresAt: got %v want %v", act.ActiveLease.ExpiresAt, time.Unix(2000, 0).UTC())
	}
}

func TestApplyLeasedNoMetadataKeepsZeroExpiry(t *testing.T) {
	state := newInstance(t)
	actID := state.Instance.Activations[0].ID
	state = mustApply(t, state, Command{
		Kind:             CommandClaim,
		ExpectedRevision: state.Instance.Revision,
		IdempotencyKey:   "claim-bare",
		Identity: ExecutionIdentity{
			WorkflowID:   "wf1",
			NodeID:       "start",
			ActivationID: actID,
		},
		LeaseID: "lease-bare",
		Actor:   "alice",
		// Lease field is omitted -> legacy/bare claim with zero expiry
	}, "claim without lease")
	if len(state.Instance.Activations) != 1 {
		t.Fatalf("activations count: got %d want 1", len(state.Instance.Activations))
	}
	act := &state.Instance.Activations[0]
	if act.Status != ActivationLeased {
		t.Fatalf("status: got %q want %q", act.Status, ActivationLeased)
	}
	if act.ActiveLease == nil {
		t.Fatalf("ActiveLease is nil")
	}
	if !act.ActiveLease.ExpiresAt.IsZero() {
		t.Fatalf("ExpiresAt is not zero (got %v)", act.ActiveLease.ExpiresAt)
	}
	// When event.Lease is nil, the reducer does not carry AcquiredAt/ExpiresAt; both remain zero.
	// The spec only requires ExpiresAt zero for legacy bare claims.
	// No assertion on AcquiredAt needed.
}
