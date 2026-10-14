package workflow

// Internal tests proving the durable Stage 13 (phase 3) retry/replacement
// contract end to end through the store + manager — store.ApplyCommand,
// Manager.WorkflowCommand (claim/start), and Manager.RecordAttemptTerminated —
// not just the pure reducer. The retry policy lives in real template JSON and
// is resolved by the manager's store-backed resolver (matching production),
// so the reducer sees the durable policy rather than a test-installed stub.
// The contract: lease expiry -> replacement claim -> retry budget ->
// exhaustion (block/fail/outcome), all on the SAME activation with prior
// attempts preserved, and retry/exhaustion never follows a branch or
// checkpoint and never creates a new activation. They use only the standard
// library.

import (
	"fmt"
	"testing"
	"time"
)

// retryTemplateJSON builds a single-node template whose run node carries the
// given retry policy. The outcome kind declares its configured outcome as a
// terminal outcome so the template is realistic and valid.
func retryTemplateJSON(templateID string, maxAttempts int, exhaustion RetryExhaustionKind, outcome string) []byte {
	outcomeField := ""
	terminal := `"done"`
	if outcome != "" {
		outcomeField = fmt.Sprintf(`, "outcome": %q`, outcome)
		terminal = `"done", "eh_out"`
	}
	return []byte(fmt.Sprintf(`{
  "schema_version": 1,
  "template_id": %q,
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [{"id": "start", "action": {"type": "run", "prompt": "do the thing"}, "retry_policy": {"max_attempts": %d, "exhaustion": %q%s}}],
  "terminal_outcomes": [%s]
}`, templateID, maxAttempts, exhaustion, outcomeField, terminal))
}

// newRetryFixture builds an ephemeral store + manager with a stored run-node
// template carrying a retry policy, a fake run executor registered, and a
// fresh instance of the template.
func newRetryFixture(t *testing.T, templateJSON []byte, templateID string) (*Manager, *Store, WorkflowID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	fake := &fakeExecutor{}
	m.RegisterExecutor(ActionRun, fake)
	if _, err := m.WorkflowCreate(templateJSON); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, templateID, "1")
	return m, s, wf
}

// mustTerminate records a failed attempt termination through the manager.
func mustTerminate(t *testing.T, m *Manager, wf WorkflowID, nodeID NodeID, actID ActivationID, attemptID, leaseID string) {
	t.Helper()
	if err := m.RecordAttemptTerminated(wf, nodeID, actID, AttemptID(attemptID), LeaseID(leaseID), AttemptFailed); err != nil {
		t.Fatalf("RecordAttemptTerminated(%s): %v", attemptID, err)
	}
}

// retryScenario is one end-to-end exhaustion scenario.
type retryScenario struct {
	name           string
	templateID     string
	maxAttempts    int
	exhaustion     RetryExhaustionKind
	outcome        string
	wantActivation ActivationStatus
	wantWorkflow   WorkflowStatus
}

// TestDurableRetryExhaustion drives the FULL durable path for each exhaustion
// kind: expiry of the first attempt's lease, a replacement claim + start on
// the SAME activation (prior attempts preserved), then failed terminations
// until the retry budget is consumed, asserting the per-kind exhaustion
// outcome. Every step asserts the single-activation / constant-iteration
// invariant: retry never branches, checkpoints, or creates a new activation.
func TestDurableRetryExhaustion(t *testing.T) {
	scenarios := []retryScenario{
		{"block", "retry-block", 3, RetryExhaustionBlock, "", ActivationBlocked, WorkflowBlocked},
		{"fail", "retry-fail", 2, RetryExhaustionFail, "", ActivationAttemptFailed, WorkflowFailed},
		{"outcome", "retry-outcome", 2, RetryExhaustionOutcome, "eh_out", ActivationAttemptFailed, WorkflowActive},
	}
	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			m, s, wf := newRetryFixture(t, retryTemplateJSON(sc.templateID, sc.maxAttempts, sc.exhaustion, sc.outcome), sc.templateID)
			node := NodeID("start")

			snap, _, err := s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("loadCurrent: %v", err)
			}
			act := activationByNode(&snap.Instance, node)
			if act == nil {
				t.Fatal("start activation not found")
			}
			actID := act.ID
			iteration := act.Iteration
			wantActivations := len(snap.Instance.Activations)

			// 1. Attempt A1: a crafted lease whose expiry is already in the
			//    past, claimed through the store and started through the
			//    manager.
			past := time.Now().UTC().Add(-time.Hour)
			claimWithLease(t, s, wf, node, Lease{
				ID:           "lease-a1",
				ActivationID: actID,
				Owner:        "alice",
				TokenDigest:  ownerTokenDigest("tok-a1"),
				AcquiredAt:   past.Add(-time.Minute),
				ExpiresAt:    past,
			}, "alice")
			first := startWithToken(t, m, wf, "start", string(actID), "lease-a1", "tok-a1")
			attempt1, _ := first["attempt_id"].(string)
			if attempt1 == "" {
				t.Fatalf("first start result missing attempt_id: %#v", first)
			}
			attempts := []string{attempt1}

			// 2. The live detector expires A1's stale lease (durably, with the
			//    "stale" reason).
			if _, err := m.ExpireStaleLeases(); err != nil {
				t.Fatalf("ExpireStaleLeases: %v", err)
			}
			snap, _, err = s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("reload after expiry: %v", err)
			}
			act = activationByNode(&snap.Instance, node)
			if act.Status != ActivationLeaseExpired {
				t.Fatalf("status after expiry = %s, want lease_expired", act.Status)
			}
			if act.ActiveLease != nil {
				t.Fatalf("lease after expiry = %+v, want released", act.ActiveLease)
			}

			// 3. Replacement claim + start of A2 through the manager: SAME
			//    activation, prior attempt preserved (append-only).
			res := claimActivation(t, m, wf, "start", string(actID), "bob")
			lease2, _ := res["lease_id"].(string)
			token2, _ := res["owner_token"].(string)
			if lease2 == "" || token2 == "" || lease2 == "lease-a1" {
				t.Fatalf("replacement claim = %#v, want a fresh lease distinct from lease-a1", res)
			}
			second := startWithToken(t, m, wf, "start", string(actID), lease2, token2)
			attempt2, _ := second["attempt_id"].(string)
			if attempt2 == "" || attempt2 == attempt1 {
				t.Fatalf("second start attempt = %q (first %q), want a distinct attempt", attempt2, attempt1)
			}
			attempts = append(attempts, attempt2)

			snap, _, err = s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("reload after replacement start: %v", err)
			}
			act = activationByNode(&snap.Instance, node)
			if act.ID != actID {
				t.Fatalf("replacement start moved to activation %s, want the same %s", act.ID, actID)
			}
			if act.Status != ActivationRunning {
				t.Fatalf("status after replacement start = %s, want running", act.Status)
			}
			if act.Iteration != iteration {
				t.Fatalf("replacement start changed iteration: got %d want %d", act.Iteration, iteration)
			}
			if len(act.AttemptIDs) != 2 || act.AttemptIDs[0] != AttemptID(attempt1) || act.AttemptIDs[1] != AttemptID(attempt2) {
				t.Fatalf("attempt ids = %v, want [%s %s] on the same activation", act.AttemptIDs, attempt1, attempt2)
			}
			if len(snap.Instance.Attempts) != 2 {
				t.Fatalf("attempts = %d, want 2 (prior attempt preserved)", len(snap.Instance.Attempts))
			}
			if n := len(snap.Instance.Activations); n != wantActivations {
				t.Fatalf("activations = %d after replacement, want %d (no new activation)", n, wantActivations)
			}

			// 4. Terminate A2 as failed: with budget remaining the activation
			//    re-arms to ready on the SAME activation (no new activation,
			//    iteration unchanged).
			mustTerminate(t, m, wf, node, actID, attempt2, lease2)
			snap, _, err = s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("reload after A2 termination: %v", err)
			}
			act = activationByNode(&snap.Instance, node)
			if sc.maxAttempts > 2 {
				if act.Status != ActivationReady {
					t.Fatalf("status after A2 termination = %s, want ready (budget remains)", act.Status)
				}
			}

			// 5. Consume the remaining retry budget: claim -> start -> failed
			//    terminate, asserting the re-arm after each.
			for len(attempts) < sc.maxAttempts {
				res := claimActivation(t, m, wf, "start", string(actID), "bob")
				lease, _ := res["lease_id"].(string)
				token, _ := res["owner_token"].(string)
				out := startWithToken(t, m, wf, "start", string(actID), lease, token)
				attemptID, _ := out["attempt_id"].(string)
				if attemptID == "" {
					t.Fatalf("start result missing attempt_id: %#v", out)
				}
				attempts = append(attempts, attemptID)
				mustTerminate(t, m, wf, node, actID, attemptID, lease)

				if len(attempts) < sc.maxAttempts {
					snap, _, err = s.loadCurrent(wf)
					if err != nil {
						t.Fatalf("reload after attempt %d termination: %v", len(attempts), err)
					}
					act = activationByNode(&snap.Instance, node)
					if act.Status != ActivationReady {
						t.Fatalf("status after attempt %d termination = %s, want ready (budget remains)", len(attempts), act.Status)
					}
					if act.Iteration != iteration {
						t.Fatalf("retry changed iteration: got %d want %d", act.Iteration, iteration)
					}
					if n := len(snap.Instance.Activations); n != wantActivations {
						t.Fatalf("retry created a new activation: got %d want %d", n, wantActivations)
					}
				}
			}

			// 6. Exhaustion: the per-kind terminal state, durably.
			snap, _, err = s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("reload after exhaustion: %v", err)
			}
			if snap.Instance.Status != sc.wantWorkflow {
				t.Fatalf("workflow status after exhaustion = %s, want %s", snap.Instance.Status, sc.wantWorkflow)
			}
			act = activationByNode(&snap.Instance, node)
			if act.Status != sc.wantActivation {
				t.Fatalf("activation status after exhaustion = %s, want %s", act.Status, sc.wantActivation)
			}
			if sc.exhaustion == RetryExhaustionOutcome {
				if act.SelectedOutcome != "eh_out" {
					t.Fatalf("selected outcome after exhaustion = %q, want eh_out", act.SelectedOutcome)
				}
			} else if act.SelectedOutcome != "" {
				t.Fatalf("selected outcome after exhaustion = %q, want empty", act.SelectedOutcome)
			}
			if len(act.AttemptIDs) != sc.maxAttempts {
				t.Fatalf("attempt ids = %v, want all %d attempts on the one activation", act.AttemptIDs, sc.maxAttempts)
			}
			for i, want := range attempts {
				if act.AttemptIDs[i] != AttemptID(want) {
					t.Fatalf("attempt ids[%d] = %s, want %s", i, act.AttemptIDs[i], want)
				}
			}
			if len(snap.Instance.Attempts) != sc.maxAttempts {
				t.Fatalf("attempt records = %d, want %d (all preserved)", len(snap.Instance.Attempts), sc.maxAttempts)
			}
			if act.Iteration != iteration {
				t.Fatalf("iteration after exhaustion = %d, want constant %d", act.Iteration, iteration)
			}

			// 7. Durable event-log invariants: exactly one activation for the
			//    node, one stale lease_expired, and NO branch or reroute event
			//    for the node (retry/exhaustion never follows a semantic
			//    branch or checkpoint).
			activations := 0
			for i := range snap.Instance.Activations {
				if snap.Instance.Activations[i].NodeID == node {
					activations++
				}
			}
			if activations != 1 {
				t.Fatalf("activations for node = %d, want exactly 1", activations)
			}
			expiredStale, transitions, reroutes := 0, 0, 0
			for _, e := range readEvents(t, s, wf) {
				switch e.Kind {
				case EventLeaseExpired:
					if e.Identity.NodeID == node && e.Reason == "stale" {
						expiredStale++
					}
				case EventTransition:
					if e.Identity.NodeID == node {
						transitions++
					}
				case EventRerouted:
					if e.Identity.NodeID == node {
						reroutes++
					}
				}
			}
			if expiredStale != 1 {
				t.Fatalf("lease_expired(stale) events = %d, want exactly 1", expiredStale)
			}
			if transitions != 0 || reroutes != 0 {
				t.Fatalf("events for the node: transitions=%d reroutes=%d, want 0/0 (retry never branches)", transitions, reroutes)
			}
		})
	}
}

// TestRetryReplacementNeverBranches proves the replacement side of the
// contract in isolation: after expiry and replacement, the successor attempt
// stays on the same activation, no EventTransition/EventRerouted event exists
// for the node, no new activation is created, and exhaustion does not
// reroute either.
func TestRetryReplacementNeverBranches(t *testing.T) {
	m, s, wf := newRetryFixture(t, retryTemplateJSON("retry-nobranch", 2, RetryExhaustionBlock, ""), "retry-nobranch")
	node := NodeID("start")

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	act := activationByNode(&snap.Instance, node)
	actID := act.ID
	iteration := act.Iteration

	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, node, Lease{
		ID:           "lease-rb1",
		ActivationID: actID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-rb1"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	first := startWithToken(t, m, wf, "start", string(actID), "lease-rb1", "tok-rb1")
	attempt1, _ := first["attempt_id"].(string)

	if _, err := m.ExpireStaleLeases(); err != nil {
		t.Fatalf("ExpireStaleLeases: %v", err)
	}
	res := claimActivation(t, m, wf, "start", string(actID), "bob")
	lease2, _ := res["lease_id"].(string)
	token2, _ := res["owner_token"].(string)
	second := startWithToken(t, m, wf, "start", string(actID), lease2, token2)
	attempt2, _ := second["attempt_id"].(string)

	// After the replacement start: one activation, both attempts on it, no
	// branch/reroute in the log.
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after replacement: %v", err)
	}
	if n := len(snap.Instance.Activations); n != 1 {
		t.Fatalf("activations = %d after replacement, want exactly 1", n)
	}
	act = activationByNode(&snap.Instance, node)
	if act.Iteration != iteration {
		t.Fatalf("iteration = %d after replacement, want constant %d", act.Iteration, iteration)
	}
	if e := noBranchEvents(t, s, wf); e {
		t.Fatal("replacement produced a transition/reroute event")
	}

	// Exhaustion (block) still does not reroute.
	mustTerminate(t, m, wf, node, actID, attempt2, lease2)
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after exhaustion: %v", err)
	}
	if n := len(snap.Instance.Activations); n != 1 {
		t.Fatalf("activations = %d after exhaustion, want exactly 1", n)
	}
	act = activationByNode(&snap.Instance, node)
	if act.Status != ActivationBlocked || act.Iteration != iteration {
		t.Fatalf("after exhaustion = status %s iteration %d, want blocked / %d", act.Status, act.Iteration, iteration)
	}
	if e := noBranchEvents(t, s, wf); e {
		t.Fatal("exhaustion produced a transition/reroute event")
	}
	_ = attempt1
}

// noBranchEvents reports whether the event log carries any transition or
// reroute event for the node.
func noBranchEvents(t *testing.T, s *Store, wf WorkflowID) bool {
	t.Helper()
	for _, e := range readEvents(t, s, wf) {
		if e.Kind == EventTransition || e.Kind == EventRerouted {
			if e.Identity.NodeID == NodeID("start") {
				return true
			}
		}
	}
	return false
}

// TestRetryPolicyFallsBackToTemplateDefault proves the manager's resolver
// applies the template's default_retry_policy to a node that declares no
// retry_policy, while a node's own policy still overrides it.
func TestRetryPolicyFallsBackToTemplateDefault(t *testing.T) {
	const templateID = "retry-default"
	templateJSON := []byte(`{
  "schema_version": 1,
  "template_id": "retry-default",
  "template_version": "1",
  "entry_nodes": ["start"],
  "nodes": [
    {"id": "start", "action": {"type": "run", "prompt": "do the thing"}, "branches": {"done": "own"}},
    {"id": "own", "dependencies": ["start"], "action": {"type": "run", "prompt": "again"}, "retry_policy": {"max_attempts": 5, "exhaustion": "fail"}}
  ],
  "terminal_outcomes": ["done"],
  "default_retry_policy": {"max_attempts": 2, "exhaustion": "block"}
}`)
	_, s, wf := newRetryFixture(t, templateJSON, templateID)
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}

	inherited := retryPolicyFor(&snap, "start")
	if inherited == nil || inherited.MaximumAttempts != 2 || inherited.Exhaustion != RetryExhaustionBlock {
		t.Fatalf("start retry policy = %+v, want the template default {2 block}", inherited)
	}
	own := retryPolicyFor(&snap, "own")
	if own == nil || own.MaximumAttempts != 5 || own.Exhaustion != RetryExhaustionFail {
		t.Fatalf("own retry policy = %+v, want the node's own {5 fail}", own)
	}
}

// startRetryAttempt claims the activation and starts one attempt, returning
// the attempt and lease IDs.
func startRetryAttempt(t *testing.T, m *Manager, wf WorkflowID, nodeID NodeID, actID ActivationID) (attemptID, leaseID string) {
	t.Helper()
	res := claimActivation(t, m, wf, string(nodeID), string(actID), "bob")
	lease, _ := res["lease_id"].(string)
	token, _ := res["owner_token"].(string)
	if lease == "" || token == "" {
		t.Fatalf("claim result = %#v, want lease_id and owner_token", res)
	}
	out := startWithToken(t, m, wf, string(nodeID), string(actID), lease, token)
	attemptID, _ = out["attempt_id"].(string)
	if attemptID == "" {
		t.Fatalf("start result = %#v, want attempt_id", out)
	}
	return attemptID, lease
}

// mustTerminateAs records an attempt termination with an explicit terminal
// status through the manager.
func mustTerminateAs(t *testing.T, m *Manager, wf WorkflowID, nodeID NodeID, actID ActivationID, attemptID, leaseID string, status AttemptStatus) {
	t.Helper()
	if err := m.RecordAttemptTerminated(wf, nodeID, actID, AttemptID(attemptID), LeaseID(leaseID), status); err != nil {
		t.Fatalf("RecordAttemptTerminated(%s): %v", status, err)
	}
}

// TestTimedOutAndPanickedAttemptsConsumeRetryBudget proves that timed_out and
// panicked terminations take the same retry/exhaustion path as plain failed
// terminations: the activation re-arms while budget remains and exhausts per
// the node's policy when it is gone.
func TestTimedOutAndPanickedAttemptsConsumeRetryBudget(t *testing.T) {
	scenarios := []struct {
		name   string
		status AttemptStatus
	}{
		{"timed_out", AttemptTimedOut},
		{"panicked", AttemptPanicked},
	}
	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			// max_attempts 2: the first terminal attempt re-arms, the second
			// exhausts to the configured block.
			m, s, wf := newRetryFixture(t, retryTemplateJSON("retry-"+sc.name, 2, RetryExhaustionBlock, ""), "retry-"+sc.name)
			node := NodeID("start")
			snap, _, err := s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("loadCurrent: %v", err)
			}
			act := activationByNode(&snap.Instance, node)
			if act == nil {
				t.Fatal("start activation not found")
			}
			actID := act.ID

			attempt1, lease1 := startRetryAttempt(t, m, wf, node, actID)
			mustTerminateAs(t, m, wf, node, actID, attempt1, lease1, sc.status)
			snap, _, err = s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("reload after %s termination: %v", sc.status, err)
			}
			act = activationByNode(&snap.Instance, node)
			if act.Status != ActivationReady {
				t.Fatalf("status after %s termination = %s, want ready (retried)", sc.status, act.Status)
			}
			if snap.Instance.Status != WorkflowActive {
				t.Fatalf("workflow status after %s termination = %s, want active", sc.status, snap.Instance.Status)
			}

			attempt2, lease2 := startRetryAttempt(t, m, wf, node, actID)
			mustTerminateAs(t, m, wf, node, actID, attempt2, lease2, sc.status)
			snap, _, err = s.loadCurrent(wf)
			if err != nil {
				t.Fatalf("reload after exhaustion: %v", err)
			}
			act = activationByNode(&snap.Instance, node)
			if act.Status != ActivationBlocked {
				t.Fatalf("status after budget exhaustion = %s, want blocked", act.Status)
			}
			if snap.Instance.Status != WorkflowBlocked {
				t.Fatalf("workflow status after budget exhaustion = %s, want blocked", snap.Instance.Status)
			}
		})
	}
}

// TestCanceledAttemptBlocksWithoutRetrying proves the cancellation semantics:
// a canceled attempt is a deliberate stop that never re-arms the node. The
// activation parks blocked, the instance surfaces blocked until an authorized
// unblock restores it, and the canceled attempt still occupies a retry-budget
// slot on the activation.
func TestCanceledAttemptBlocksWithoutRetrying(t *testing.T) {
	m, s, wf := newRetryFixture(t, retryTemplateJSON("retry-cancel", 2, RetryExhaustionBlock, ""), "retry-cancel")
	node := NodeID("start")
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	act := activationByNode(&snap.Instance, node)
	if act == nil {
		t.Fatal("start activation not found")
	}
	actID := act.ID

	// A1 canceled: no re-arm, no retry — the activation parks blocked and the
	// instance surfaces blocked.
	attempt1, lease1 := startRetryAttempt(t, m, wf, node, actID)
	mustTerminateAs(t, m, wf, node, actID, attempt1, lease1, AttemptCanceled)
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after cancellation: %v", err)
	}
	act = activationByNode(&snap.Instance, node)
	if act.Status != ActivationBlocked {
		t.Fatalf("status after cancellation = %s, want blocked", act.Status)
	}
	if snap.Instance.Status != WorkflowBlocked {
		t.Fatalf("workflow status after cancellation = %s, want blocked", snap.Instance.Status)
	}

	// Unblock: the activation re-arms and the instance returns to active.
	if _, err := m.WorkflowCommand(string(wf), unblockCommandPayload(t, "alice", "operator resumed the node")); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after unblock: %v", err)
	}
	act = activationByNode(&snap.Instance, node)
	if act.Status != ActivationReady {
		t.Fatalf("status after unblock = %s, want ready", act.Status)
	}
	if snap.Instance.Status != WorkflowActive {
		t.Fatalf("workflow status after unblock = %s, want active", snap.Instance.Status)
	}

	// A2 fails: with max_attempts 2 the canceled A1 occupies one slot, so this
	// failure exhausts the budget to the configured block again.
	attempt2, lease2 := startRetryAttempt(t, m, wf, node, actID)
	mustTerminateAs(t, m, wf, node, actID, attempt2, lease2, AttemptFailed)
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("reload after exhaustion: %v", err)
	}
	act = activationByNode(&snap.Instance, node)
	if act.Status != ActivationBlocked {
		t.Fatalf("status after exhaustion = %s, want blocked", act.Status)
	}
	if snap.Instance.Status != WorkflowBlocked {
		t.Fatalf("workflow status after exhaustion = %s, want blocked", snap.Instance.Status)
	}
}

// TestFailExhaustionDoesNotHideBlockedInstance drives the two-branch edge: a
// parallel branch whose retry policy exhausts to block parks the instance in
// blocked (actionable), and a later fail exhaustion on the other branch must
// not overwrite that instance status with the terminal failed — the failure
// is recorded on the activation, while the blocked branch stays surfaced.
func TestFailExhaustionDoesNotHideBlockedInstance(t *testing.T) {
	templateJSON := []byte(`{
  "schema_version": 1,
  "template_id": "retry-parallel-fail",
  "template_version": "1",
  "entry_nodes": ["a", "b"],
  "nodes": [
    {"id": "a", "action": {"type": "run", "prompt": "branch a"}, "retry_policy": {"max_attempts": 1, "exhaustion": "block"}},
    {"id": "b", "action": {"type": "run", "prompt": "branch b"}, "retry_policy": {"max_attempts": 1, "exhaustion": "fail"}}
  ],
  "terminal_outcomes": ["done"]
}`)
	m, s, wf := newRetryFixture(t, templateJSON, "retry-parallel-fail")

	run := func(node NodeID, leaseID, token string) {
		snap, _, err := s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("loadCurrent: %v", err)
		}
		act := activationByNode(&snap.Instance, node)
		if act == nil {
			t.Fatalf("activation for %s not found", node)
		}
		past := time.Now().UTC().Add(-time.Hour)
		claimWithLease(t, s, wf, node, Lease{
			ID:           LeaseID(leaseID),
			ActivationID: act.ID,
			Owner:        "alice",
			TokenDigest:  ownerTokenDigest(token),
			AcquiredAt:   past.Add(-time.Minute),
			ExpiresAt:    past,
		}, "alice")
		started := startWithToken(t, m, wf, string(node), string(act.ID), leaseID, token)
		attemptID, _ := started["attempt_id"].(string)
		if attemptID == "" {
			t.Fatalf("start result missing attempt_id: %#v", started)
		}
		mustTerminate(t, m, wf, node, act.ID, attemptID, leaseID)
	}

	// Branch a exhausts to block: the instance reports blocked.
	run("a", "lease-a", "tok-a")
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if snap.Instance.Status != WorkflowBlocked {
		t.Fatalf("instance after block exhaustion = %s, want blocked", snap.Instance.Status)
	}

	// Branch b then exhausts to fail: the failure stays on the activation and
	// the blocked branch remains surfaced at the instance level.
	run("b", "lease-b", "tok-b")
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	actB := activationByNode(&snap.Instance, "b")
	if actB.Status != ActivationAttemptFailed {
		t.Fatalf("activation b after fail exhaustion = %s, want attempt_failed", actB.Status)
	}
	if snap.Instance.Status != WorkflowBlocked {
		t.Fatalf("instance after fail exhaustion = %s, want blocked (blocked branch stays surfaced)", snap.Instance.Status)
	}
}
