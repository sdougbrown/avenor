package workflow

import (
	"testing"
)

// TestCommandUnblockRefreshesReadyAt asserts the unblock re-arm path
// (applyUnblocked -> copyReadyAt) refreshes the activation's ReadyAt from the
// unblock event's explicit timestamp and re-surfaces it as a dispatch
// candidate. Existing unblock tests only assert the status flip to ready; a
// missing ready_at refresh on unblock would go undetected.
func TestCommandUnblockRefreshesReadyAt(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "dispatch-unblock", "ctl-a", -1)
	node := NodeID("start")

	// Record the entry activation's initial ReadyAt (stamped on instantiate).
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
	if act.Dispatch == nil || !act.Dispatch.IsAuto() {
		t.Fatalf("activation dispatch = %+v, want auto", act.Dispatch)
	}
	if act.ReadyAt == nil {
		t.Fatal("ready_at missing on instantiated activation")
	}
	initial := *act.ReadyAt
	actID := act.ID

	// Drive the activation to blocked via retry exhaustion (max_attempts: 3,
	// exhaustion: block): claim -> start -> failed terminate, three times. The
	// first two re-arm to ready (budget remains); the third exhausts to
	// blocked.
	for attempt := 1; attempt <= 3; attempt++ {
		res := claimActivation(t, m, wf, "start", string(actID), "alice")
		leaseID := LeaseID(res["lease_id"].(string))
		out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "start", string(actID), res, nil))
		if err != nil {
			t.Fatalf("start (attempt %d): %v", attempt, err)
		}
		attemptID := AttemptID(out.(map[string]any)["attempt_id"].(string))
		if err := m.RecordAttemptTerminated(wf, node, actID, attemptID, leaseID, AttemptFailed); err != nil {
			t.Fatalf("RecordAttemptTerminated (attempt %d): %v", attempt, err)
		}
	}

	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after exhaustion: %v", err)
	}
	act = activationByNode(&snap.Instance, node)
	if act == nil {
		t.Fatal("start activation not found after exhaustion")
	}
	if act.Status != ActivationBlocked {
		t.Fatalf("activation status after exhaustion = %q, want blocked", act.Status)
	}
	// The pre-block ReadyAt is the value the activation carries while blocked
	// (the last retry re-arm's stamp; exhaustion-to-block does not touch it).
	// Comparing against this value — rather than the initial one — is what
	// makes a missing unblock refresh detectable: without the refresh the
	// value would be unchanged and not strictly after itself.
	if act.ReadyAt == nil {
		t.Fatal("ready_at missing while blocked")
	}
	preBlock := *act.ReadyAt

	// Unblock: the re-arm must refresh ReadyAt from the unblock event's
	// explicit timestamp.
	out, err := m.WorkflowCommand(string(wf), unblockCommandPayload(t, "alice", "root cause fixed"))
	if err != nil {
		t.Fatalf("unblock: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("unblock result = %#v, want map", out)
	}
	if mm["status"] != "unblocked" || mm["activation_status"] != string(ActivationReady) {
		t.Fatalf("unblock result = %#v", mm)
	}

	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after unblock: %v", err)
	}
	act = activationByNode(&snap.Instance, node)
	if act == nil {
		t.Fatal("start activation not found after unblock")
	}
	if act.Status != ActivationReady {
		t.Fatalf("activation status after unblock = %q, want ready", act.Status)
	}
	if act.ReadyAt == nil {
		t.Fatal("ready_at missing after unblock")
	}
	if !act.ReadyAt.After(initial) {
		t.Fatalf("ready_at after unblock = %v, want after initial %v", act.ReadyAt, initial)
	}
	if !act.ReadyAt.After(preBlock) {
		t.Fatalf("ready_at after unblock = %v, want strictly after pre-block %v", act.ReadyAt, preBlock)
	}

	// The re-armed activation must surface as a dispatch candidate after a
	// rebuild.
	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	candidates, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	found := false
	for _, c := range candidates {
		if c.Identity.NodeID == node && c.Identity.ActivationID == actID {
			found = true
		}
	}
	if !found {
		t.Fatalf("unblocked activation not in candidates: %v", candidates)
	}
}
