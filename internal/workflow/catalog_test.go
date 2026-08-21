package workflow

import (
	"testing"
	"time"
)

// attachWithLeaseExpiry drives the parent's spawn activation to awaiting_child
// directly through store commands, carrying a lease whose expiry is in the
// past (the claim command records the supplied lease metadata verbatim).
func attachWithLeaseExpiry(t *testing.T, s *Store, wf WorkflowID, leaseID LeaseID, attempt AttemptID, expiresAt time.Time) ActivationID {
	t.Helper()
	snap, exists, err := s.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("loadCurrent: exists=%v err=%v", exists, err)
	}
	actID := activationByNode(&snap.Instance, "spawn").ID
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandClaim,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "claim-" + string(leaseID),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID},
		LeaseID:          leaseID,
		Actor:            "alice",
		Lease: &Lease{
			ID:           leaseID,
			ActivationID: actID,
			Owner:        "alice",
			AcquiredAt:   expiresAt.Add(-time.Minute),
			ExpiresAt:    expiresAt,
		},
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after claim: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandStart,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "start-" + string(attempt),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID, AttemptID: attempt},
		LeaseID:          leaseID,
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after start: %v", err)
	}
	if _, err := s.ApplyCommand(wf, Command{
		Kind:             CommandChildAttach,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "child-attach-" + string(attempt),
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: "spawn", ActivationID: actID, AttemptID: attempt},
		LeaseID:          leaseID,
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	return actID
}

// TestRecoveryExemptsAwaitingChildFromLeaseSweep proves the recovery
// invariant that an awaiting_child activation's kernel-held claim survives a
// supervisor restart: even with an expired lease, the sweep must not move it
// to lease_expired (the child-outcome resolution requires awaiting_child),
// while a non-awaiting_child expired lease in the same catalog pass IS swept.
func TestRecoveryExemptsAwaitingChildFromLeaseSweep(t *testing.T) {
	_, s, wf, childID := compositionPhase2Fixture(t, map[OutcomeName]OutcomeName{"done": "done"})

	// Parent's spawn activation is durably awaiting_child on an expired lease.
	past := time.Now().Add(-time.Hour)
	_ = attachWithLeaseExpiry(t, s, wf, "expired-lease", "att-exp", past)

	// Control: the child's start activation is leased (not awaiting_child) on
	// an expired lease.
	csnap, exists, err := s.loadCurrent(childID)
	if err != nil || !exists || len(csnap.Instance.Activations) == 0 {
		t.Fatalf("child loadCurrent: exists=%v err=%v", exists, err)
	}
	cact := csnap.Instance.Activations[0]
	if _, err := s.ApplyCommand(childID, Command{
		Kind:             CommandClaim,
		ExpectedRevision: csnap.Instance.Revision,
		IdempotencyKey:   "ctl-claim",
		Identity:         ExecutionIdentity{WorkflowID: childID, NodeID: cact.NodeID, ActivationID: cact.ID},
		LeaseID:          "ctl-lease",
		Actor:            "bob",
		Lease: &Lease{
			ID:           "ctl-lease",
			ActivationID: cact.ID,
			Owner:        "bob",
			AcquiredAt:   past.Add(-time.Minute),
			ExpiresAt:    past,
		},
	}); err != nil {
		t.Fatalf("control claim: %v", err)
	}

	// Simulated restart: a fresh store over the same root runs the recovery.
	s2 := New(s.Root())
	if _, err := s2.Catalog(); err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	// The awaiting_child activation keeps its status and its (expired) lease.
	psnap, exists, err := s2.loadCurrent(wf)
	if err != nil || !exists {
		t.Fatalf("parent loadCurrent: exists=%v err=%v", exists, err)
	}
	pact := activationByNode(&psnap.Instance, "spawn")
	if pact.Status != ActivationAwaitingChild {
		t.Fatalf("spawn status after recovery = %q, want awaiting_child (not swept)", pact.Status)
	}
	if pact.ActiveLease == nil || pact.ActiveLease.ID != "expired-lease" {
		t.Fatalf("spawn lease after recovery = %+v, want the expired claim lease retained", pact.ActiveLease)
	}

	// The control (non-awaiting_child) expired lease IS still swept.
	csnap2, exists, err := s2.loadCurrent(childID)
	if err != nil || !exists {
		t.Fatalf("child loadCurrent: exists=%v err=%v", exists, err)
	}
	cact2 := activationByNode(&csnap2.Instance, cact.NodeID)
	if cact2.Status != ActivationLeaseExpired {
		t.Fatalf("control activation status after recovery = %q, want lease_expired", cact2.Status)
	}
	if cact2.ActiveLease != nil {
		t.Fatalf("control activation lease after recovery = %+v, want released", cact2.ActiveLease)
	}
}
