package workflow

import (
	"testing"
	"time"
)

// TestLeaseExpiryTimesOutNonTerminalAttempts proves that expiring a lease
// terminalizes the crashed holder's non-terminal attempts: the attempt is
// timed_out at the lease's expiry time, the activation is lease_expired and
// claimable again, no retry budget is consumed, and a late termination
// report for the already-timed-out attempt is an idempotent no-op.
func TestLeaseExpiryTimesOutNonTerminalAttempts(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID

	// Claim with an already-expired lease (a holder that crashed right
	// after claiming) and drive it to running.
	past := time.Now().UTC().Add(-time.Hour)
	expiry := past
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-ghost",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("ghost-token"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    expiry,
	}, "alice")
	started := startWithToken(t, m, wf, node, string(actID), "lease-ghost", "ghost-token")
	attemptID, ok := started["attempt_id"].(string)
	if !ok || attemptID == "" {
		t.Fatalf("start result missing attempt_id: %#v", started)
	}

	// The recovery sweep expires the stale lease.
	if _, err := m.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after sweep: %v", err)
	}
	act := activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("activation status = %s, want lease_expired", act.Status)
	}
	if act.ActiveLease != nil {
		t.Fatal("activation still carries a lease after expiry")
	}
	if len(act.AttemptIDs) != 1 {
		t.Fatalf("attempt count = %d, want 1 (expiry consumes no retry budget)", len(act.AttemptIDs))
	}
	attempt := findAttempt(&snap, act, AttemptID(attemptID))
	if attempt == nil {
		t.Fatal("crashed attempt not found after sweep")
	}
	if attempt.Status != AttemptTimedOut {
		t.Fatalf("crashed attempt status = %s, want timed_out", attempt.Status)
	}
	if attempt.EndedAt == nil || !attempt.EndedAt.Equal(expiry) {
		t.Fatalf("crashed attempt ended_at = %v, want the lease expiry %v", attempt.EndedAt, expiry)
	}
	// No concurrency key is held by the timed-out attempt.
	held := map[string]bool{}
	collectHeldKeys(held, snap, "", "")
	if len(held) != 0 {
		t.Fatalf("held concurrency keys after expiry = %v, want none", held)
	}

	// A late termination report for the already-timed-out attempt is an
	// idempotent no-op: the attempt stays timed_out and the activation stays
	// claimably lease_expired (no re-termination, no retry re-arm).
	if err := m.RecordAttemptTerminated(wf, nodeID, actID, AttemptID(attemptID), "lease-ghost", AttemptFailed); err != nil {
		t.Fatalf("late RecordAttemptTerminated: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after late termination: %v", err)
	}
	act = activationByNode(&snap.Instance, nodeID)
	attempt = findAttempt(&snap, act, AttemptID(attemptID))
	if attempt.Status != AttemptTimedOut {
		t.Fatalf("attempt status after late report = %s, want timed_out (idempotent no-op)", attempt.Status)
	}
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("activation status after late report = %s, want lease_expired", act.Status)
	}

	// The expired activation is claimable again: a replacement lease and
	// start append a fresh attempt to the same activation.
	now := time.Now().UTC()
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-replacement",
		ActivationID: actID,
		Owner:        "bob",
		TokenDigest:  ownerTokenDigest("replacement-token"),
		AcquiredAt:   now,
		ExpiresAt:    now.Add(time.Minute),
	}, "bob")
	startWithToken(t, m, wf, node, string(actID), "lease-replacement", "replacement-token")
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after replacement: %v", err)
	}
	act = activationByNode(&snap.Instance, nodeID)
	if act.Status != ActivationRunning {
		t.Fatalf("replacement activation status = %s, want running", act.Status)
	}
	if len(act.AttemptIDs) != 2 {
		t.Fatalf("attempt count after replacement = %d, want 2", len(act.AttemptIDs))
	}
}

// TestLeaseExpiryTimedOutAttemptsReplay proves replaying the event log
// reproduces the sweep's timed_out attempts deterministically.
func TestLeaseExpiryTimedOutAttemptsReplay(t *testing.T) {
	m, s, wf, node := newManagerFixture(t)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	nodeID := NodeID(node)
	actID := activationByNode(&snap.Instance, nodeID).ID
	base := resetToPostInstantiate(snap, nodeID)

	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, nodeID, Lease{
		ID:           "lease-replay",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("replay-token"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	started := startWithToken(t, m, wf, node, string(actID), "lease-replay", "replay-token")
	attemptID, ok := started["attempt_id"].(string)
	if !ok || attemptID == "" {
		t.Fatalf("start result missing attempt_id: %#v", started)
	}
	if _, err := m.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}

	replayed, _, _, err := replayEvents(base, s.eventsPath(wf))
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	act := activationByNode(&replayed.Instance, nodeID)
	if act == nil {
		t.Fatal("replayed activation not found")
	}
	if act.Status != ActivationLeaseExpired {
		t.Fatalf("replayed activation status = %s, want lease_expired", act.Status)
	}
	attempt := findAttempt(&replayed, act, AttemptID(attemptID))
	if attempt == nil {
		t.Fatal("replayed attempt not found")
	}
	if attempt.Status != AttemptTimedOut {
		t.Fatalf("replayed attempt status = %s, want timed_out", attempt.Status)
	}
	if attempt.EndedAt == nil || !attempt.EndedAt.Equal(past) {
		t.Fatalf("replayed attempt ended_at = %v, want the lease expiry %v", attempt.EndedAt, past)
	}
}
