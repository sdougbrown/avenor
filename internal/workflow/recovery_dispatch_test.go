package workflow

// Internal tests for the recovery sweep's ReadyAt contract (recovery.go).
// These own the re-claimability guarantee: an expired auto-dispatch lease
// swept on restart recovery stamps the activation's ReadyAt from the
// recovery event, so the re-expired activation re-surfaces as a dispatch
// candidate after a candidate-index rebuild. A missing stamp would leave the
// activation with a nil ReadyAt and make it permanently unclaimable.

import (
	"testing"
	"time"
)

// TestRecoveryLeaseExpiryStampsReadyAt proves the recovery sweep stamps
// ReadyAt on the expired activation (equal to the recovery event's ReadyAt)
// and that the activation re-surfaces as a dispatch candidate after a
// candidate-index rebuild.
func TestRecoveryLeaseExpiryStampsReadyAt(t *testing.T) {
	_, s, wf := newAutoDispatchFixture(t, "dispatch-recovery", "ctl-a", -1)
	node := NodeID("start")

	snap, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found")
	}
	act := activationByNode(&snap.Instance, node)
	if act == nil {
		t.Fatal("start activation not found")
	}

	// Claim an already-expired lease, then simulate a restart: a fresh store
	// over the same root runs the recovery sweep.
	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, node, Lease{
		ID:           "lease-recovery",
		ActivationID: act.ID,
		Owner:        "bob",
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "bob")

	s2 := New(s.Root())
	if _, err := s2.Catalog(); err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	// The swept activation is lease_expired with the lease released and
	// ReadyAt stamped.
	snap2, exists, err := s2.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent after recovery: exists=%v err=%v", exists, err)
	}
	act2 := activationByNode(&snap2.Instance, node)
	if act2 == nil {
		t.Fatal("start activation not found after recovery")
	}
	if act2.Status != ActivationLeaseExpired {
		t.Fatalf("status after recovery = %s, want lease_expired (swept)", act2.Status)
	}
	if act2.ActiveLease != nil {
		t.Fatalf("lease after recovery = %+v, want released", act2.ActiveLease)
	}
	if act2.ReadyAt == nil {
		t.Fatal("ready_at missing on re-expired activation; it would never be re-claimable")
	}

	// The activation's ReadyAt equals the recovery event's stamp.
	var sawEvent bool
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventLeaseExpired && e.Identity.NodeID == node {
			if e.ReadyAt == nil {
				t.Fatalf("lease_expired event ready_at = nil, want the recovery stamp")
			}
			if !act2.ReadyAt.Equal(*e.ReadyAt) {
				t.Fatalf("activation ready_at = %v, want the recovery event stamp %v", *act2.ReadyAt, *e.ReadyAt)
			}
			sawEvent = true
		}
	}
	if !sawEvent {
		t.Fatal("event log has no lease_expired event for the swept node")
	}

	// The re-expired activation re-surfaces as a dispatch candidate after a
	// candidate-index rebuild.
	m2 := NewManager(s2)
	if err := m2.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	candidates, err := m2.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	found := false
	for _, c := range candidates {
		if c.Identity.NodeID == node && c.Identity.ActivationID == act2.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("re-expired activation not in candidates: %v", candidates)
	}
}
