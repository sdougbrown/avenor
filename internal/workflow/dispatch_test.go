package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// dispatchNodeMutation returns a mutation that replaces a node's action and
// dispatch object in the shared valid template fixture.
func dispatchNodeMutation(nodeID string, action map[string]any, dispatch map[string]any) func(map[string]any) {
	return func(template map[string]any) {
		nodes, ok := template["nodes"].([]any)
		if !ok {
			return
		}
		for _, raw := range nodes {
			node, ok := raw.(map[string]any)
			if !ok || node["id"] != nodeID {
				continue
			}
			if action != nil {
				node["action"] = action
			}
			if dispatch != nil {
				node["dispatch"] = dispatch
			} else {
				delete(node, "dispatch")
			}
		}
	}
}

func TestDispatchTemplateValidation(t *testing.T) {
	runAction := map[string]any{"type": "run", "prompt": "go"}
	loopAction := map[string]any{"type": "loop", "loop_file": "loop.json"}
	teamAction := map[string]any{"type": "team", "team_file": "team.json"}
	externalAction := map[string]any{"type": "external", "source": "webhook"}
	workflowAction := map[string]any{
		"type": "workflow", "template_id": "child", "template_version": "1",
		"child_key": "c1", "outcome_map": map[string]any{"done": "done"},
	}

	valid := []struct {
		name     string
		nodeID   string
		action   map[string]any
		dispatch map[string]any
	}{
		{"auto run", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "ctl-a"}},
		{"auto loop", "execute", loopAction, map[string]any{"mode": "auto", "controller_id": "ctl-a", "priority": 70, "concurrency_key": "deploys"}},
		{"auto team priority bounds", "intake", teamAction, map[string]any{"mode": "auto", "controller_id": "ctl-a", "priority": 0}},
		{"auto priority upper bound", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "ctl-a", "priority": 100}},
		{"manual default", "intake", runAction, map[string]any{}},
		{"manual explicit", "intake", runAction, map[string]any{"mode": "manual"}},
		{"manual on external", "intake", externalAction, map[string]any{"mode": "manual"}},
		{"manual on workflow composition", "intake", workflowAction, map[string]any{"mode": "manual"}},
	}
	for _, tc := range valid {
		tc := tc
		t.Run("valid/"+tc.name, func(t *testing.T) {
			input := mutateTemplate(t, dispatchNodeMutation(tc.nodeID, tc.action, tc.dispatch))
			if err := ValidateTemplateJSON(input); err != nil {
				t.Fatalf("ValidateTemplateJSON() error = %v\ninput: %s", err, input)
			}
		})
	}

	invalid := []struct {
		name     string
		nodeID   string
		action   map[string]any
		dispatch map[string]any
		wantErr  string
	}{
		{"auto without controller_id", "intake", runAction, map[string]any{"mode": "auto"}, "requires controller_id"},
		{"manual with controller_id", "intake", runAction, map[string]any{"controller_id": "ctl-a"}, "forbidden"},
		{"priority below range", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "c", "priority": -1}, "dispatch"},
		{"priority above range", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "c", "priority": 101}, "dispatch"},
		{"priority non-integer", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "c", "priority": 1.5}, "dispatch"},
		{"empty concurrency_key", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "c", "concurrency_key": ""}, "dispatch"},
		{"blank concurrency_key", "intake", runAction, map[string]any{"mode": "auto", "controller_id": "c", "concurrency_key": "  "}, "cannot be blank"},
		{"auto on external", "intake", externalAction, map[string]any{"mode": "auto", "controller_id": "c"}, "run, loop, and team"},
		{"auto on workflow composition", "intake", workflowAction, map[string]any{"mode": "auto", "controller_id": "c"}, "run, loop, and team"},
		{"unknown mode", "intake", runAction, map[string]any{"mode": "teleport"}, "dispatch"},
	}
	for _, tc := range invalid {
		tc := tc
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			assertTemplateError(t, mutateTemplate(t, dispatchNodeMutation(tc.nodeID, tc.action, tc.dispatch)), tc.wantErr)
		})
	}
}

// TestDispatchSchemaGenerationStable regenerates the profile code and requires
// the generated file to be byte-identical afterwards.
func TestDispatchSchemaGenerationStable(t *testing.T) {
	genPath := "workflow_schema.gen.go"
	sum := func() string {
		data, err := os.ReadFile(genPath)
		if err != nil {
			t.Fatalf("read generated schema: %v", err)
		}
		digest := sha256.Sum256(data)
		return hex.EncodeToString(digest[:])
	}
	before := sum()
	cmd := exec.Command("go", "generate", ".")
	cmd.Dir = "."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go generate: %v\n%s", err, out)
	}
	if after := sum(); after != before {
		t.Fatal("go generate changed workflow_schema.gen.go; regenerate and commit the generated file")
	}
}

// TestDispatchLegacyReadsManual loads a template and instance without any
// dispatch metadata and asserts legacy activations read as manual, are never
// candidates, and the query guards on recovery.
func TestDispatchLegacyReadsManual(t *testing.T) {
	m, s, wf, _ := newManagerFixture(t)
	if _, err := m.CandidatesForController("ctl-a", 10); !errors.Is(err, ErrCandidatesNotRecovered) {
		t.Fatalf("CandidatesForController before rebuild error = %v, want ErrCandidatesNotRecovered", err)
	}
	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	candidates, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("legacy manual candidates = %v, want empty", candidates)
	}

	snap, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found")
	}
	act := activationByNode(&snap.Instance, "start")
	if act == nil {
		t.Fatal("start activation not found")
	}
	if act.Dispatch != nil {
		t.Fatalf("legacy activation dispatch = %+v, want nil", act.Dispatch)
	}

	// Simulate a legacy event log record: a transition event written before
	// the ready_at field existed. Its activation must never surface as a
	// candidate even after a rebuild.
	event := Event{
		ID:       NewEventID(),
		Kind:     EventTransition,
		Sequence: snap.Instance.Revision + 1,
		Identity: ExecutionIdentity{WorkflowID: wf, NodeID: "start", ActivationID: act.ID},
		Transition: &Transition{
			Outcome:      "done",
			TargetNodeID: NodeID("execute"),
		},
	}
	if err := s.appendEvents(wf, []Event{event}); err != nil {
		t.Fatalf("append legacy event: %v", err)
	}
	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("rebuild after legacy event: %v", err)
	}
	candidates, err = m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController after legacy event: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("legacy candidates = %v, want empty", candidates)
	}
	snap2, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after legacy event: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found after legacy event")
	}
	legacy := activationByNode(&snap2.Instance, "execute")
	if legacy == nil {
		t.Fatal("legacy activation not found")
	}
	if legacy.ReadyAt != nil {
		t.Fatalf("legacy activation ready_at = %v, want nil", legacy.ReadyAt)
	}
}

// autoDispatchTemplateJSON builds a single run-node template with an auto
// dispatch policy and a retry policy for re-arm coverage.
func autoDispatchTemplateJSON(templateID, controllerID string, priority int) []byte {
	policy := map[string]any{"mode": "auto", "controller_id": controllerID}
	if priority >= 0 {
		policy["priority"] = priority
	}
	template := map[string]any{
		"schema_version":   1,
		"template_id":      templateID,
		"template_version": "1",
		"entry_nodes":      []string{"start"},
		"nodes": []any{map[string]any{
			"id":           "start",
			"action":       map[string]any{"type": "run", "prompt": "do the thing"},
			"dispatch":     policy,
			"retry_policy": map[string]any{"max_attempts": 3, "exhaustion": "block"},
		}},
		"terminal_outcomes":    []string{"done"},
		"default_lease_policy": map[string]any{"ttl_seconds": 900},
	}
	data, err := json.Marshal(template)
	if err != nil {
		panic(err)
	}
	return data
}

// newAutoDispatchFixture stores an auto-dispatch run template and instantiates
// it, registering the fake executor.
func newAutoDispatchFixture(t *testing.T, templateID, controllerID string, priority int) (*Manager, *Store, WorkflowID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	m.RegisterExecutor(ActionRun, &fakeExecutor{})
	if _, err := m.WorkflowCreate(autoDispatchTemplateJSON(templateID, controllerID, priority)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	wf := mustInstantiateTemplate(t, m, templateID, "1")
	return m, s, wf
}

// resetToPostInstantiate returns a copy of the materialized snapshot with the
// entry activation reset to its post-instantiate state (pending, no lease,
// no attempts, ReadyAt nil) and only the instantiate event marked applied,
// so a replay re-derives ReadyAt from the event log. The entry activation's
// materialized ID is preserved because the log's later events reference it
// and activation IDs are generated during reduction (not stored in the log).
func resetToPostInstantiate(snap Snapshot, nodeID NodeID) Snapshot {
	base := snap
	for i := range base.Instance.Activations {
		act := &base.Instance.Activations[i]
		if act.NodeID != nodeID {
			continue
		}
		act.Status = ActivationPending
		act.ReadyAt = nil
		act.ActiveLease = nil
		act.AttemptIDs = nil
		act.SelectedOutcome = ""
		act.Selection = nil
		act.IncomingOutcome = ""
	}
	base.Instance.Attempts = nil
	if len(base.AppliedEventIDs) > 0 {
		base.AppliedEventIDs = base.AppliedEventIDs[:1]
	}
	return base
}

// TestDispatchReadyAtOnCreateAndRearm asserts ready_at is stamped from the
// explicit event timestamp on instantiation, updated on retry re-arm, and
// reproduced identically by replay.
func TestDispatchReadyAtOnCreateAndRearm(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "dispatch-rearm", "ctl-a", -1)
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
	if act.Dispatch == nil || !act.Dispatch.IsAuto() {
		t.Fatalf("activation dispatch = %+v, want auto", act.Dispatch)
	}
	if act.Dispatch.ControllerID != "ctl-a" {
		t.Fatalf("controller_id = %q, want ctl-a", act.Dispatch.ControllerID)
	}
	if act.Dispatch.priority() != DefaultDispatchPriority {
		t.Fatalf("default priority = %d, want %d", act.Dispatch.priority(), DefaultDispatchPriority)
	}
	if act.ReadyAt == nil {
		t.Fatal("ready_at missing on instantiated activation")
	}
	initial := *act.ReadyAt

	// Retry re-arm: a lease already expired, then a failed attempt
	// termination re-arms the SAME activation with a fresh ready_at.
	past := time.Now().UTC().Add(-time.Hour)
	claimWithLease(t, s, wf, node, Lease{
		ID:           "lease-a1",
		ActivationID: act.ID,
		Owner:        "alice",
		TokenDigest:  ownerTokenDigest("tok-a1"),
		AcquiredAt:   past.Add(-time.Minute),
		ExpiresAt:    past,
	}, "alice")
	started := startWithToken(t, m, wf, "start", string(act.ID), "lease-a1", "tok-a1")
	attemptID, ok := started["attempt_id"].(string)
	if !ok || attemptID == "" {
		t.Fatalf("start result missing attempt_id: %#v", started)
	}
	mustTerminate(t, m, wf, node, act.ID, attemptID, "lease-a1")

	snap, ok, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after terminate: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found after terminate")
	}
	act = activationByNode(&snap.Instance, node)
	if act.Status != ActivationReady {
		t.Fatalf("status after terminate = %q, want ready", act.Status)
	}
	if act.ReadyAt == nil || !act.ReadyAt.After(initial) {
		t.Fatalf("ready_at after re-arm = %v, want after %v", act.ReadyAt, initial)
	}
	rearmed := *act.ReadyAt

	// A genuine from-scratch replay (replayEvents(Snapshot{}, ...)) is
	// impossible: activation IDs are generated during reduction and are not
	// stored in the event log, so the log's later events (which reference the
	// materialized activation ID) cannot resolve against a freshly generated
	// one. The minimal base the replay needs therefore preserves the entry
	// activation's materialized ID while resetting the fields under test and
	// marking only the instantiate event as applied, so the subsequent events
	// replay and re-derive ReadyAt from the log.
	base := resetToPostInstantiate(snap, node)
	replayed, _, _, err := replayEvents(base, s.eventsPath(wf))
	if err != nil {
		t.Fatalf("replayEvents: %v", err)
	}
	ract := activationByNode(&replayed.Instance, node)
	if ract == nil {
		t.Fatal("replayed start activation not found")
	}
	if ract.ReadyAt == nil || !ract.ReadyAt.Equal(rearmed) {
		t.Fatalf("replayed ready_at = %v, want %v", ract.ReadyAt, rearmed)
	}
	if ract.Dispatch == nil || !ract.Dispatch.IsAuto() || ract.Dispatch.ControllerID != "ctl-a" {
		t.Fatalf("replayed dispatch = %+v, want auto/ctl-a", ract.Dispatch)
	}
}

// TestRerouteStampsReadyAtAndDispatch asserts a rerouted auto-dispatch node
// gets a stamped ReadyAt and an auto dispatch policy, and surfaces as a
// candidate after a rebuild.
func TestRerouteStampsReadyAtAndDispatch(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "dispatch-reroute", "ctl-a", -1)
	node := NodeID("start")

	snap, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found")
	}
	payload, err := json.Marshal(struct {
		Selection *ExecutionSelection `json:"selection,omitempty"`
		Iteration int                 `json:"iteration,omitempty"`
	}{
		Selection: &ExecutionSelection{Role: "role-x", Backend: "b1", Model: "m1"},
		Iteration: 2,
	})
	if err != nil {
		t.Fatalf("marshal reroute payload: %v", err)
	}
	snap, err = s.ApplyCommand(wf, Command{
		Kind:             CommandReroute,
		ExpectedRevision: snap.Instance.Revision,
		IdempotencyKey:   "reroute-1",
		Identity:         ExecutionIdentity{WorkflowID: wf, NodeID: node},
		Payload:          payload,
	})
	if err != nil {
		t.Fatalf("ApplyCommand reroute: %v", err)
	}

	// The rerouted activation is the one carrying the supplied iteration.
	var rerouted *Activation
	for i := range snap.Instance.Activations {
		act := &snap.Instance.Activations[i]
		if act.Iteration == 2 {
			rerouted = act
		}
	}
	if rerouted == nil {
		t.Fatal("rerouted activation not found")
	}
	if rerouted.ReadyAt == nil {
		t.Fatal("rerouted activation ready_at is nil")
	}
	if rerouted.Dispatch == nil || !rerouted.Dispatch.IsAuto() {
		t.Fatalf("rerouted activation dispatch = %+v, want auto", rerouted.Dispatch)
	}
	if rerouted.Dispatch.ControllerID != "ctl-a" {
		t.Fatalf("rerouted controller_id = %q, want ctl-a", rerouted.Dispatch.ControllerID)
	}

	// The rerouted activation must surface as a candidate after a rebuild.
	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	candidates, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	found := false
	for _, c := range candidates {
		if c.Identity.NodeID == node && c.Identity.ActivationID == rerouted.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("rerouted activation not in candidates: %v", candidates)
	}
}

// TestCandidatesForControllerQuery covers recovery gating, controller scoping,
// lease exclusion, revision stability, and rebuild reproducibility.
func TestCandidatesForControllerQuery(t *testing.T) {
	m, s, wf := newAutoDispatchFixture(t, "dispatch-query", "ctl-a", 70)

	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	snap, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found")
	}
	act := activationByNode(&snap.Instance, "start")

	candidates, err := m.CandidatesForController("ctl-a", 10)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates = %v, want exactly one", candidates)
	}
	c := candidates[0]
	if c.Identity.SupervisorID != "sup-1" || c.Identity.WorkflowID != wf ||
		c.Identity.NodeID != NodeID("start") || c.Identity.ActivationID != act.ID {
		t.Fatalf("candidate identity = %+v, want sup-1/%s/start/%s", c.Identity, wf, act.ID)
	}
	if c.Identity.AttemptID != "" {
		t.Fatalf("candidate attempt id = %q, want empty", c.Identity.AttemptID)
	}
	if c.Revision != snap.Instance.Revision {
		t.Fatalf("candidate revision = %d, want %d", c.Revision, snap.Instance.Revision)
	}
	if c.Priority != 70 {
		t.Fatalf("candidate priority = %d, want 70", c.Priority)
	}
	if c.ReadyAt.IsZero() {
		t.Fatal("candidate ready_at is zero")
	}
	if c.ControllerID != "ctl-a" {
		t.Fatalf("candidate controller = %q, want ctl-a", c.ControllerID)
	}

	// Other controllers never see it.
	if others, err := m.CandidatesForController("ctl-b", 10); err != nil || len(others) != 0 {
		t.Fatalf("ctl-b candidates = %v, err = %v, want none", others, err)
	}

	// Claiming through the command boundary removes it until rebuild; the
	// query itself never mutates state.
	res := claimActivation(t, m, wf, "start", string(act.ID), "alice")
	snapClaimed, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after claim: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found after claim")
	}
	claimedRevision := snapClaimed.Instance.Revision
	if _, err := m.CandidatesForController("ctl-a", 10); err != nil {
		t.Fatalf("post-claim query: %v", err)
	}
	if err := m.RebuildCandidateIndex("sup-1"); err != nil {
		t.Fatalf("rebuild after claim: %v", err)
	}
	if leased, err := m.CandidatesForController("ctl-a", 10); err != nil || len(leased) != 0 {
		t.Fatalf("post-claim candidates = %v, err = %v, want none", leased, err)
	}

	// Query is side-effect free: the workflow revision is unchanged.
	snapAfter, ok, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("loadCurrent after queries: %v", err)
	}
	if !ok {
		t.Fatal("instance snapshot not found after queries")
	}
	if snapAfter.Instance.Revision != claimedRevision {
		t.Fatalf("revision changed by queries: %d -> %d", claimedRevision, snapAfter.Instance.Revision)
	}
	_ = res
}

// TestClaimableActivationStatus pins the shared claim-eligibility predicate:
// gate-parked, checkpoint-blocked, and terminal statuses are never claimable.
func TestClaimableActivationStatus(t *testing.T) {
	claimable := []ActivationStatus{
		ActivationPending, ActivationReady, ActivationLeaseExpired,
	}
	for _, status := range claimable {
		if !claimableActivationStatus(status) {
			t.Fatalf("status %q should be claimable", status)
		}
	}
	notClaimable := []ActivationStatus{
		ActivationLeased, ActivationRunning, ActivationSkipped,
		ActivationAttemptFailed, ActivationAwaitingCompletion,
		ActivationAwaitingGate, ActivationAwaitingChild, ActivationBlocked,
		ActivationSatisfied, ActivationRejected,
	}
	for _, status := range notClaimable {
		if claimableActivationStatus(status) {
			t.Fatalf("status %q should not be claimable", status)
		}
	}
	if strings.TrimSpace(ErrCandidatesNotRecovered.Error()) == "" {
		t.Fatal("sentinel error message is empty")
	}
}
