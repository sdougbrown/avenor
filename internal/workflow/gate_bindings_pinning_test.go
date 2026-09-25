package workflow

// Tests for command-time gate pinning: a transition onto a node with bound
// gates pins the exact causal source activation, output revision, and
// subject onto the new activation (carried on the event, copied by the
// reducer, reproduced by replay). A fan-in has no unique pin and fails the
// command; a missing source output stays unresolved.

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

// boundPublicationOutputs builds the declared publication output set with the
// given pull request identity.
func boundPublicationOutputs(repository string, pr int, head string) []map[string]any {
	return []map[string]any{
		{"definition_id": "repository", "value": repository},
		{"definition_id": "pr_number", "value": pr},
		{"definition_id": "pr_head", "value": head},
	}
}

// driveBoundPublication claims, starts, and completes the newest pending
// publication activation with the given outputs (outcome "published", which
// branches to review), returning the activation ID.
func driveBoundPublication(t *testing.T, m *Manager, s *Store, wf WorkflowID, outputs []map[string]any) ActivationID {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	actID := ActivationID("")
	for i := range snap.Instance.Activations {
		a := &snap.Instance.Activations[i]
		if a.NodeID == "publication" && a.Status == ActivationPending {
			actID = a.ID
		}
	}
	if actID == "" {
		t.Fatalf("no pending publication activation")
	}
	res := claimActivation(t, m, wf, "publication", string(actID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "publication", string(actID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID := AttemptID(out.(map[string]any)["attempt_id"].(string))
	leaseID := LeaseID(res["lease_id"].(string))
	if err := m.RecordAttemptTerminated(wf, "publication", actID, attemptID, leaseID, AttemptSucceeded); err != nil {
		t.Fatalf("record attempt terminated: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"node_id":       "publication",
		"activation_id": actID,
		"attempt_id":    attemptID,
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
		"outcome":       "published",
		"outputs":       outputs,
	})
	if err != nil {
		t.Fatalf("marshal complete: %v", err)
	}
	if _, err := m.commandComplete(wf, payload); err != nil {
		t.Fatalf("publication complete: %v", err)
	}
	return actID
}

// latestActivation returns the newest activation of a node.
func latestActivation(t *testing.T, s *Store, wf WorkflowID, nodeID NodeID) *Activation {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	var found *Activation
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].NodeID == nodeID {
			found = &snap.Instance.Activations[i]
		}
	}
	if found == nil {
		t.Fatalf("no activation for node %q", nodeID)
	}
	return found
}

// outputRevision returns the recorded revision of one output value.
func outputRevision(t *testing.T, s *Store, wf WorkflowID, actID ActivationID, defID OutputID) int64 {
	t.Helper()
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	for _, o := range snap.Instance.Outputs {
		if o.ActivationID == actID && o.DefinitionID == defID {
			return o.Revision
		}
	}
	t.Fatalf("output %q not recorded for activation %s", defID, actID)
	return 0
}

func TestTransitionPinsBoundGateSubjectAndInputs(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	pubID := driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))

	review := latestActivation(t, s, wf, "review")
	if review.Status != ActivationPending {
		t.Fatalf("review status = %q, want pending", review.Status)
	}
	if !reflect.DeepEqual(review.CausedBy, []ActivationID{pubID}) {
		t.Fatalf("review caused_by = %v, want [%s]", review.CausedBy, pubID)
	}
	resolved, ok := review.ResolvedGates[GateID("pr-review")]
	if !ok {
		t.Fatalf("review has no pinned gates: %+v", review.ResolvedGates)
	}
	wantSubject := &Subject{Type: "pull_request", Repository: "org/repo", PullRequest: 42, Revision: "abc123"}
	if !reflect.DeepEqual(resolved.Subject, wantSubject) {
		t.Fatalf("pinned subject = %+v, want %+v", resolved.Subject, wantSubject)
	}
	prRef := resolved.Inputs["pull_number"].Reference
	if prRef == nil {
		t.Fatalf("pull_number input has no pinned reference")
	}
	wantRef := OutputReference{
		WorkflowID:   wf,
		NodeID:       "publication",
		ActivationID: pubID,
		OutputID:     "pr_number",
		Revision:     outputRevision(t, s, wf, pubID, "pr_number"),
	}
	if *prRef != wantRef {
		t.Fatalf("pull_number reference = %+v, want %+v", *prRef, wantRef)
	}
	headRef := resolved.Inputs["head_sha"].Reference
	if headRef == nil || headRef.ActivationID != pubID || headRef.OutputID != "pr_head" {
		t.Fatalf("head_sha reference = %+v, want pinned to %s/pr_head", headRef, pubID)
	}
	if string(resolved.Inputs["level"].Literal) != `"high"` {
		t.Fatalf("level literal = %s, want \"high\"", resolved.Inputs["level"].Literal)
	}
	if string(resolved.Inputs["strict"].Literal) != "true" {
		t.Fatalf("strict literal = %s, want true", resolved.Inputs["strict"].Literal)
	}
}

// parkBoundReview drives the newest pending review activation to a parked
// awaiting_gate state (claim, start, complete with the declared "clean"
// outcome; the required gate parks it) and returns the activation ID.
func parkBoundReview(t *testing.T, m *Manager, s *Store, wf WorkflowID) ActivationID {
	t.Helper()
	rev := latestActivation(t, s, wf, "review")
	if rev.Status != ActivationPending {
		t.Fatalf("review status = %q, want pending", rev.Status)
	}
	res := claimActivation(t, m, wf, "review", string(rev.ID), "alice")
	out, err := m.WorkflowCommand(string(wf), startCommandPayload(t, "review", string(rev.ID), res, nil))
	if err != nil {
		t.Fatalf("review start: %v", err)
	}
	attemptID := AttemptID(out.(map[string]any)["attempt_id"].(string))
	leaseID := LeaseID(res["lease_id"].(string))
	if err := m.RecordAttemptTerminated(wf, "review", rev.ID, attemptID, leaseID, AttemptSucceeded); err != nil {
		t.Fatalf("review attempt terminated: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"node_id":       "review",
		"activation_id": rev.ID,
		"attempt_id":    attemptID,
		"lease_id":      res["lease_id"],
		"owner_token":   res["owner_token"],
		"outcome":       "clean",
	})
	if err != nil {
		t.Fatalf("marshal review complete: %v", err)
	}
	result, err := m.commandComplete(wf, payload)
	if err != nil {
		t.Fatalf("review complete: %v", err)
	}
	if mm := result.(map[string]any); mm["activation_status"] != string(ActivationAwaitingGate) {
		t.Fatalf("review complete result = %#v, want parked awaiting_gate", mm)
	}
	return rev.ID
}

func TestCorrectionLoopRepinsNewHead(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	pub1 := driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	review1ID := parkBoundReview(t, m, s, wf)
	review1 := latestActivation(t, s, wf, "review")
	revBefore := revision(t, s, wf)

	// The mapped changes_requested result routes back to publication.
	subject := map[string]any{"type": "pull_request", "repository": "org/repo", "pull_request": 42, "revision": "abc123"}
	gatePayload, err := json.Marshal(map[string]any{
		"op":            "gate",
		"node_id":       "review",
		"activation_id": review1ID,
		"gate_id":       "pr-review",
		"operation":     "external_result",
		"result":        "changes_requested",
		"poll_id":       "poll-1",
		"source":        "github",
		"subject":       subject,
		"response_hash": "hash-1",
		"observed_at":   time.Now().UTC(),
		"evidence_ids":  []string{"ev-1"},
	})
	if err != nil {
		t.Fatalf("marshal gate payload: %v", err)
	}
	out, err := m.WorkflowCommand(string(wf), gatePayload)
	if err != nil {
		t.Fatalf("changes_requested gate command: %v", err)
	}
	if mm := out.(map[string]any); mm["activation_status"] != string(ActivationRejected) {
		t.Fatalf("gate result = %#v, want rejected review", mm)
	}
	pub2 := driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "def456"))

	review2 := latestActivation(t, s, wf, "review")
	if review2.ID == review1.ID {
		t.Fatalf("correction did not create a new review activation")
	}
	resolved := review2.ResolvedGates[GateID("pr-review")]
	if resolved.Subject == nil || resolved.Subject.Revision != "def456" {
		t.Fatalf("new review subject = %+v, want revision def456", resolved.Subject)
	}
	if resolved.Inputs["head_sha"].Reference == nil || resolved.Inputs["head_sha"].Reference.ActivationID != pub2 {
		t.Fatalf("new review head_sha pin = %+v, want activation %s", resolved.Inputs["head_sha"].Reference, pub2)
	}
	if !reflect.DeepEqual(review2.CausedBy, []ActivationID{pub2}) {
		t.Fatalf("new review caused_by = %v, want [%s]", review2.CausedBy, pub2)
	}
	// The old activation's pins are immutable and point at the old head.
	old := review1.ResolvedGates[GateID("pr-review")]
	if old.Subject == nil || old.Subject.Revision != "abc123" || old.Inputs["head_sha"].Reference.ActivationID != pub1 {
		t.Fatalf("old review pins mutated: %+v", old)
	}
	if got := revision(t, s, wf); got < revBefore {
		t.Fatalf("revision regressed %d -> %d", revBefore, got)
	}
}

func TestFanInAmbiguousBindingFailsCommand(t *testing.T) {
	// The joiner node is completed by a command whose causal chain fans in to
	// two completed publication activations, so the review gate's references
	// have no unique nearest source.
	fixture := mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		template["nodes"] = append(template["nodes"].([]any), map[string]any{
			"id":       "joiner",
			"action":   map[string]any{"type": "manual"},
			"branches": map[string]any{"done": "review"},
		})
	})
	var tmpl Template
	if err := decodeStrict(fixture, &tmpl); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	SetBindingTemplateResolver(func(templateID TemplateID, templateVersion TemplateVersion) (*Template, error) {
		return &tmpl, nil
	})
	t.Cleanup(func() { SetBindingTemplateResolver(nil) })

	// Two completed publication activations are causal parents of the
	// completing joiner activation, so no unique pin exists.
	state := Snapshot{Instance: WorkflowInstance{
		WorkflowID:      "wf-fanin",
		TemplateID:      "bound-gates",
		TemplateVersion: "1.0.0",
		Revision:        5,
		Activations: []Activation{
			{ID: "pub-1", NodeID: "publication", Status: ActivationSatisfied},
			{ID: "pub-2", NodeID: "publication", Status: ActivationSatisfied},
			{ID: "join", NodeID: "joiner", Status: ActivationRunning, CausedBy: []ActivationID{"pub-1", "pub-2"}},
		},
	}}
	branch, err := json.Marshal(&Transition{ActivationID: "join", Outcome: "done", TargetNodeID: "review"})
	if err != nil {
		t.Fatalf("marshal transition: %v", err)
	}
	events, err := buildCommandEvents(state, Command{
		Kind:             CommandComplete,
		ExpectedRevision: 5,
		Outcome:          "done",
		Identity:         ExecutionIdentity{WorkflowID: "wf-fanin", NodeID: "joiner", ActivationID: "join"},
		Payload:          branch,
	})
	if !errors.Is(err, ErrAmbiguousBinding) {
		t.Fatalf("buildCommandEvents error = %v, want ErrAmbiguousBinding", err)
	}
	if events != nil {
		t.Fatalf("events = %+v, want none (nothing appended on ambiguity)", events)
	}
}

func TestMissingOutputLeavesGateUnresolved(t *testing.T) {
	fixture := boundGateFixtureWithoutRequiredHead()
	m, s, wf := newCompleteFixture(t, string(fixture), "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, []map[string]any{
		{"definition_id": "repository", "value": "org/repo"},
		{"definition_id": "pr_number", "value": 42},
	})

	review := latestActivation(t, s, wf, "review")
	resolved, ok := review.ResolvedGates[GateID("pr-review")]
	if !ok {
		t.Fatalf("review has no pinned gates")
	}
	if resolved.Subject != nil {
		t.Fatalf("pinned subject = %+v, want nil (revision output missing)", resolved.Subject)
	}
	for _, want := range []string{"head_sha", "subject.revision"} {
		found := false
		for _, name := range resolved.Unresolved {
			if name == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("unresolved = %v, want to contain %q", resolved.Unresolved, want)
		}
	}
	// The resolvable references still pin.
	if resolved.Inputs["pull_number"].Reference == nil {
		t.Fatalf("pull_number pin = %+v, want resolved", resolved.Inputs["pull_number"])
	}
}

func TestReplayReproducesResolvedGates(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	// Base snapshot before the completing command: replay advances it with
	// the completion's event batch (activation IDs are reducer-generated, so
	// the event log alone cannot rebuild state from empty).
	baseSnap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	base := cloneSnapshot(baseSnap)
	pubID := driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))

	rebuilt, _, _, err := replayEvents(base, s.eventsPath(wf))
	var want, got *Activation
	for i := range rebuilt.Instance.Activations {
		if rebuilt.Instance.Activations[i].NodeID == "review" {
			want = &rebuilt.Instance.Activations[i]
		}
	}
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].NodeID == "review" {
			got = &snap.Instance.Activations[i]
		}
	}
	if want == nil || got == nil {
		t.Fatalf("review activation missing (replayed=%v live=%v)", want == nil, got == nil)
	}
	if !reflect.DeepEqual(want.ResolvedGates, got.ResolvedGates) {
		t.Fatalf("replayed resolved gates = %+v, want %+v", want.ResolvedGates, got.ResolvedGates)
	}
	if !reflect.DeepEqual(want.CausedBy, []ActivationID{pubID}) {
		t.Fatalf("replayed caused_by = %v, want [%s]", want.CausedBy, pubID)
	}
}

func TestChildOutputProvenancePinning(t *testing.T) {
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	child := Template{
		SchemaVersion:   1,
		TemplateID:      "bnd-child",
		TemplateVersion: "1",
		EntryNodes:      []NodeID{"start"},
		Nodes: []NodeDefinition{{
			ID:      "start",
			Action:  Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "do"}},
			Outputs: []OutputDefinition{{ID: "co", Name: "co", Type: OutputString}},
		}},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	spawn := compositionWorkflowNode("spawn", "bnd-child", "c1")
	spawn.Action.Workflow.OutcomeMap = map[OutcomeName]OutcomeName{"done": "done"}
	spawn.Branches = map[OutcomeName]NodeID{"done": "review"}
	spawn.Outputs = []OutputDefinition{{ID: "po", Name: "po", Type: OutputString}}
	spawn.Action.Workflow.OutputBindings = []OutputBinding{{ChildOutput: "co", ParentOutput: "po"}}
	review := NodeDefinition{
		ID:           "review",
		Dependencies: []NodeID{"spawn"},
		Action:       Action{Kind: ActionExternal, External: &ExternalAction{Source: "github"}},
		Gates: []GateDefinition{{
			ID:        GateID("pr-review"),
			Type:      GateExternal,
			Required:  true,
			AdapterID: "gh-review",
			Inputs: map[string]GateInputValue{
				"target": {FromNodeOutput: &TemplateOutputReference{NodeID: "spawn", OutputID: "po"}},
			},
		}},
	}
	parent := Template{
		SchemaVersion:    1,
		TemplateID:       "bnd-parent",
		TemplateVersion:  "1",
		EntryNodes:       []NodeID{"spawn"},
		Nodes:            []NodeDefinition{spawn, review},
		TerminalOutcomes: []OutcomeName{"done"},
	}
	for _, template := range []Template{child, parent} {
		if err := s.StoreTemplate(template.TemplateID, template.TemplateVersion, template); err != nil {
			t.Fatalf("StoreTemplate %s: %v", template.TemplateID, err)
		}
	}
	payload, _ := json.Marshal(map[string]string{"template_id": "bnd-parent", "template_version": "1"})
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))
	childID := DeriveChildWorkflowID(wf, "spawn", "c1")
	registerBoundedWorkflowExecutor(t, m, 10*time.Second)

	childSnap, exists, err := s.loadCurrent(childID)
	if err != nil || !exists || len(childSnap.Instance.Activations) == 0 {
		t.Fatalf("child loadCurrent: exists=%v err=%v", exists, err)
	}
	childActID := childSnap.Instance.Activations[0].ID

	driveErr := make(chan error, 1)
	go func() {
		if err := waitParentAwaitingChild(s, wf, 10*time.Second); err != nil {
			driveErr <- err
			return
		}
		driveErr <- driveChildTerminalErr(s, childID, "done", []OutputValue{{
			ID:           "ov1",
			DefinitionID: "co",
			ActivationID: childActID,
			Revision:     1,
			Value:        json.RawMessage(`"head-sha-9"`),
		}})
	}()

	parentPayload, _ := claimStartSpawn(t, m, s, wf)
	if _, err := m.WorkflowCommand(string(wf), parentPayload); err != nil {
		t.Fatalf("start: %v", err)
	}
	if e := <-driveErr; e != nil {
		t.Fatalf("child driver: %v", e)
	}

	revAct := latestActivation(t, s, wf, "review")
	resolved, ok := revAct.ResolvedGates[GateID("pr-review")]
	if !ok {
		t.Fatalf("review has no pinned gates")
	}
	ref := resolved.Inputs["target"].Reference
	if ref == nil {
		t.Fatalf("target input has no pinned reference")
	}
	// The pin carries the child workflow identity, not the parent's.
	want := OutputReference{
		WorkflowID:   childID,
		NodeID:       "start",
		ActivationID: childActID,
		OutputID:     "co",
		Revision:     1,
	}
	if *ref != want {
		t.Fatalf("target reference = %+v, want child identity %+v", *ref, want)
	}
}
