package workflow

// Tests for decisions on bound gates: the supplied subject must equal the
// subject pinned at activation creation (every field), an unresolved pin
// refuses any decision, external results are idempotent by poll ID with a
// response-hash replay conflict, unmapped advisory results park with a
// diagnostic, and an auto external node's success_outcome fires only when
// every required gate has passed.

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// boundSubject builds a full pull_request subject payload.
func boundSubject(repository string, pr int, revision string) map[string]any {
	return map[string]any{"type": "pull_request", "repository": repository, "pull_request": pr, "revision": revision}
}

// boundExternalResult builds an external_result gate command payload.
func boundExternalResult(t *testing.T, actID ActivationID, gateID, result, pollID, hash string, subject map[string]any) json.RawMessage {
	t.Helper()
	payload := map[string]any{
		"op":            "gate",
		"node_id":       "review",
		"activation_id": actID,
		"gate_id":       gateID,
		"operation":     "external_result",
		"result":        result,
		"poll_id":       pollID,
		"source":        "github",
		"response_hash": hash,
		"observed_at":   time.Now().UTC(),
		"evidence_ids":  []string{"ev-1"},
	}
	if subject != nil {
		payload["subject"] = subject
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal external_result: %v", err)
	}
	return data
}

// reviewStillParked asserts the review activation is untouched: same revision
// and still awaiting_gate.
func reviewStillParked(t *testing.T, s *Store, wf WorkflowID, actID ActivationID, rev int64) {
	t.Helper()
	if got := revision(t, s, wf); got != rev {
		t.Fatalf("revision changed %d -> %d", rev, got)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].ID == actID {
			if got := snap.Instance.Activations[i].Status; got != ActivationAwaitingGate {
				t.Fatalf("review status = %q, want awaiting_gate (no mutation)", got)
			}
			return
		}
	}
	t.Fatalf("review activation %s vanished", actID)
}

func TestBoundGateSubjectMismatchRejectedPerField(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)
	rev := revision(t, s, wf)

	mismatches := []struct {
		name    string
		subject map[string]any
	}{
		{"wrong type", map[string]any{"type": "commit", "repository": "org/repo", "pull_request": 42, "revision": "abc123"}},
		{"wrong repository", boundSubject("other/repo", 42, "abc123")},
		{"wrong pull request", boundSubject("org/repo", 43, "abc123")},
		{"wrong revision", boundSubject("org/repo", 42, "def456")},
		{"missing subject", nil},
	}
	for _, tc := range mismatches {
		_, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "passed", "poll-"+tc.name, tc.name, tc.subject))
		if !errors.Is(err, ErrSubjectMismatch) {
			t.Fatalf("%s: error = %v, want ErrSubjectMismatch", tc.name, err)
		}
		reviewStillParked(t, s, wf, actID, rev)
	}
}

func TestBoundGateStaleHeadRejected(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)
	rev := revision(t, s, wf)

	// A poll claiming a head that was never pinned is a subject mismatch:
	// the decision does not address the recorded review subject.
	_, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "passed", "poll-stale", "hash", boundSubject("org/repo", 42, "older-head")))
	if !errors.Is(err, ErrSubjectMismatch) {
		t.Fatalf("error = %v, want ErrSubjectMismatch", err)
	}
	reviewStillParked(t, s, wf, actID, rev)
}

// TestCommandSkipWaivesRequiredBoundGate skips an auto external review node
// parked on a required BOUND external gate: the skip waives the gate, the
// activation resolves to satisfied, and the transition follows the branch
// determined by the activation's SelectedOutcome. gateTransitionPayload's
// success_outcome override on auto external nodes applies only to a GatePassed
// status, so a waive falls back to SelectedOutcome — "clean", recorded when
// the completion parked the review — whose declared branch target is "merge".
func TestCommandSkipWaivesRequiredBoundGate(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)
	rev := revision(t, s, wf)

	payload, err := json.Marshal(map[string]any{
		"op":            "skip",
		"node_id":       "review",
		"activation_id": actID,
		"actor":         "alice",
		"reason":        "review not needed for this run",
		"evidence_ids":  []string{"ev_skip"},
	})
	if err != nil {
		t.Fatalf("marshal skip: %v", err)
	}
	out, err := m.WorkflowCommand(string(wf), payload)
	if err != nil {
		t.Fatalf("skip: %v", err)
	}
	mm, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("skip result = %#v, want map", out)
	}
	if mm["skipped"] != true || mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("skip result = %#v, want skipped with satisfied activation", mm)
	}
	waived, ok := mm["waived_gates"].([]string)
	if !ok || len(waived) != 1 || waived[0] != "pr-review" {
		t.Fatalf("waived_gates = %#v, want [pr-review]", mm["waived_gates"])
	}
	if got := revision(t, s, wf); got <= rev {
		t.Fatalf("revision = %d, want advanced past %d", got, rev)
	}

	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	var review *Activation
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].ID == actID {
			review = &snap.Instance.Activations[i]
		}
	}
	if review == nil {
		t.Fatalf("review activation %s vanished", actID)
	}
	if review.Status != ActivationSatisfied {
		t.Fatalf("review status = %q, want satisfied", review.Status)
	}
	if review.SelectedOutcome != "clean" {
		t.Fatalf("review selected outcome = %q, want clean", review.SelectedOutcome)
	}
	seen := map[GateID]GateStatus{}
	for _, gi := range snap.Instance.Gates {
		if gi.ActivationID == actID {
			seen[gi.GateID] = gi.Status
		}
	}
	if seen["pr-review"] != GateWaived {
		t.Fatalf("pr-review gate status = %q, want waived", seen["pr-review"])
	}
	// The waived branch is SelectedOutcome's declared branch ("clean" ->
	// "merge"), not the node's success_outcome path nor the "failed" branch.
	merge := activationByNode(&snap.Instance, "merge")
	if merge == nil {
		t.Fatal("branch target activation \"merge\" was not created")
	}
	if merge.Status != ActivationPending {
		t.Fatalf("merge activation status = %q, want pending", merge.Status)
	}
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].NodeID == "publication" && snap.Instance.Activations[i].Status == ActivationPending {
			t.Fatal("unexpected pending publication activation (\"failed\" branch followed)")
		}
	}
}

func TestBoundGateSubjectUnresolvedRefusesDecision(t *testing.T) {
	fixture := boundGateFixtureWithoutRequiredHead()
	m, s, wf := newCompleteFixture(t, string(fixture), "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, []map[string]any{
		{"definition_id": "repository", "value": "org/repo"},
		{"definition_id": "pr_number", "value": 42},
	})
	actID := parkBoundReview(t, m, s, wf)
	rev := revision(t, s, wf)

	_, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "passed", "poll-1", "hash", boundSubject("org/repo", 42, "abc123")))
	if !errors.Is(err, ErrSubjectUnresolved) {
		t.Fatalf("error = %v, want ErrSubjectUnresolved", err)
	}
	reviewStillParked(t, s, wf, actID, rev)
}

func TestBoundHumanGateSubjectValidation(t *testing.T) {
	fixture := mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		review := boundGateNode(template, "review")
		review["action"] = map[string]any{"type": "manual"}
		delete(review, "dispatch")
		gate := review["gates"].([]any)[0].(map[string]any)
		gate["type"] = "human"
		delete(gate, "adapter_id")
		delete(gate, "inputs")
		delete(gate, "result_outcomes")
	})
	m, s, wf := newCompleteFixture(t, string(fixture), "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)
	rev := revision(t, s, wf)

	humanPayload := func(subject map[string]any) json.RawMessage {
		payload := map[string]any{
			"op": "gate", "node_id": "review", "activation_id": actID,
			"gate_id": "pr-review", "operation": "satisfy",
			"actor": "alice", "reason": "looks good", "evidence_ids": []string{"ev-1"},
		}
		if subject != nil {
			payload["subject"] = subject
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal satisfy: %v", err)
		}
		return data
	}
	// A human decision on a bound gate needs the exact pinned subject too.
	for _, subject := range []map[string]any{nil, boundSubject("org/repo", 42, "stale")} {
		if _, err := m.WorkflowCommand(string(wf), humanPayload(subject)); !errors.Is(err, ErrSubjectMismatch) {
			t.Fatalf("satisfy subject %+v: error = %v, want ErrSubjectMismatch", subject, err)
		}
		reviewStillParked(t, s, wf, actID, rev)
	}
	out, err := m.WorkflowCommand(string(wf), humanPayload(boundSubject("org/repo", 42, "abc123")))
	if err != nil {
		t.Fatalf("satisfy with pinned subject: %v", err)
	}
	if mm := out.(map[string]any); mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("satisfy result = %#v, want satisfied", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "merge"); act == nil {
		t.Fatalf("satisfy did not follow the clean branch to merge")
	}
}

func TestBoundPollReplayIdempotency(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)

	// A pending poll parks the gate and records the poll with its hash.
	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "pending", "poll-1", "hash-1", boundSubject("org/repo", 42, "abc123"))); err != nil {
		t.Fatalf("pending poll: %v", err)
	}
	rev := revision(t, s, wf)

	// Same poll ID and hash: an idempotent no-op, whatever the phrasing.
	for _, result := range []string{"pending", "passed"} {
		out, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", result, "poll-1", "hash-1", boundSubject("org/repo", 42, "abc123")))
		if err != nil {
			t.Fatalf("replay %s: %v", result, err)
		}
		if mm := out.(map[string]any); mm["idempotent"] != true {
			t.Fatalf("replay %s result = %#v, want idempotent", result, mm)
		}
		reviewStillParked(t, s, wf, actID, rev)
	}

	// Same poll ID with a different response hash: a replay conflict.
	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "passed", "poll-1", "hash-2", boundSubject("org/repo", 42, "abc123"))); !errors.Is(err, ErrPollReplayConflict) {
		t.Fatalf("error = %v, want ErrPollReplayConflict", err)
	}
	reviewStillParked(t, s, wf, actID, rev)

	// A genuinely new poll ID records a new final fact and resolves the gate.
	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "passed", "poll-2", "hash-2", boundSubject("org/repo", 42, "abc123"))); err != nil {
		t.Fatalf("new poll: %v", err)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "review"); act.Status != ActivationSatisfied {
		t.Fatalf("review status = %q, want satisfied", act.Status)
	}
	if act := activationByNode(&snap.Instance, "merge"); act == nil {
		t.Fatalf("passing poll did not follow the success_outcome branch to merge")
	}
	polls := map[string]int{}
	for _, gi := range snap.Instance.Gates {
		if gi.ActivationID == actID {
			polls[gi.PollID]++
		}
	}
	if polls["poll-1"] != 1 || polls["poll-2"] != 1 {
		t.Fatalf("recorded polls = %v, want one instance per poll", polls)
	}
}

func TestUnmappedAdvisoryResultParksWithDiagnostic(t *testing.T) {
	fixture := mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		gate := boundGateNode(template, "review")["gates"].([]any)[0].(map[string]any)
		delete(gate, "result_outcomes")
	})
	m, s, wf := newCompleteFixture(t, string(fixture), "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)

	for _, result := range []string{"action_required", "failed"} {
		if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", result, "poll-"+result, "hash", boundSubject("org/repo", 42, "abc123"))); err != nil {
			t.Fatalf("%s poll: %v", result, err)
		}
		snap, _, err := s.loadCurrent(wf)
		if err != nil {
			t.Fatalf("load current: %v", err)
		}
		// Unmapped results never advance the activation, and never follow a
		// branch even when the node declares one.
		if act := activationByNode(&snap.Instance, "review"); act.Status != ActivationAwaitingGate {
			t.Fatalf("%s: review status = %q, want parked awaiting_gate", result, act.Status)
		}
		publications := 0
		for i := range snap.Instance.Activations {
			if snap.Instance.Activations[i].NodeID == "publication" {
				publications++
			}
		}
		if publications != 1 {
			t.Fatalf("%s: publication activations = %d, want 1 (no branch followed)", result, publications)
		}
		var diagnostic string
		for _, gi := range snap.Instance.Gates {
			if gi.ActivationID == actID && gi.PollID == "poll-"+result {
				diagnostic = gi.Diagnostic
			}
		}
		if !strings.Contains(diagnostic, "no result_outcomes mapping") {
			t.Fatalf("%s: gate diagnostic = %q, want unmapped-routing note", result, diagnostic)
		}
	}
}

func TestMappedChangesRequestedTransitionsToDeclaredBranch(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)

	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "changes_requested", "poll-1", "hash-1", boundSubject("org/repo", 42, "abc123"))); err != nil {
		t.Fatalf("changes_requested poll: %v", err)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "review"); act.Status != ActivationRejected {
		t.Fatalf("review status = %q, want rejected after mapped routing", act.Status)
	}
	publications := 0
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].NodeID == "publication" {
			publications++
		}
	}
	if publications != 2 {
		t.Fatalf("publication activations = %d, want 2 (mapped changes_requested re-opens publication)", publications)
	}
}

// TestMappedFailedTransitionsToDeclaredBranch mirrors the changes_requested
// routing for a hard failure: a bound gate whose result_outcomes maps the
// `failed` result onto the node's declared `failed` branch resolves the
// parked review onto that branch — the activation is rejected and a fresh
// publication activation is created.
func TestMappedFailedTransitionsToDeclaredBranch(t *testing.T) {
	fixture := mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		gate := boundGateNode(template, "review")["gates"].([]any)[0].(map[string]any)
		gate["result_outcomes"] = map[string]any{"failed": "failed"}
	})
	m, s, wf := newCompleteFixture(t, string(fixture), "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)

	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "failed", "poll-1", "hash-1", boundSubject("org/repo", 42, "abc123"))); err != nil {
		t.Fatalf("failed poll: %v", err)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "review"); act.Status != ActivationRejected {
		t.Fatalf("review status = %q, want rejected after mapped failed routing", act.Status)
	}
	publications := 0
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].NodeID == "publication" {
			publications++
		}
	}
	if publications != 2 {
		t.Fatalf("publication activations = %d, want 2 (mapped failed re-opens publication)", publications)
	}
}

func TestSuccessOutcomeRequiresAllGatesPassed(t *testing.T) {
	fixture := mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		review := boundGateNode(template, "review")
		gate := review["gates"].([]any)[0].(map[string]any)
		second := json.RawMessage("{}")
		data, err := json.Marshal(gate)
		if err != nil {
			t.Fatalf("marshal gate copy: %v", err)
		}
		second = json.RawMessage(data)
		var copy map[string]any
		if err := json.Unmarshal(second, &copy); err != nil {
			t.Fatalf("decode gate copy: %v", err)
		}
		copy["id"] = "ci-review"
		copy["adapter_id"] = "gh-ci"
		review["gates"] = append(review["gates"].([]any), copy)
	})
	m, s, wf := newCompleteFixture(t, string(fixture), "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	actID := parkBoundReview(t, m, s, wf)

	// One gate passing never advances the activation or its siblings.
	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "pr-review", "passed", "poll-1", "hash-1", boundSubject("org/repo", 42, "abc123"))); err != nil {
		t.Fatalf("first gate pass: %v", err)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	if act := activationByNode(&snap.Instance, "review"); act.Status != ActivationAwaitingGate {
		t.Fatalf("review status = %q, want still parked with one gate remaining", act.Status)
	}
	if act := activationByNode(&snap.Instance, "merge"); act != nil {
		t.Fatalf("merge activation exists after a single gate pass")
	}

	// The second required gate passing selects the node's success_outcome.
	if _, err := m.WorkflowCommand(string(wf), boundExternalResult(t, actID, "ci-review", "passed", "poll-2", "hash-2", boundSubject("org/repo", 42, "abc123"))); err != nil {
		t.Fatalf("second gate pass: %v", err)
	}
	snap, _, err = s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	review := activationByNode(&snap.Instance, "review")
	if review.Status != ActivationSatisfied || review.SelectedOutcome != "clean" {
		t.Fatalf("review = %s/%s, want satisfied/clean", review.Status, review.SelectedOutcome)
	}
	if act := activationByNode(&snap.Instance, "merge"); act == nil {
		t.Fatalf("success_outcome branch did not create the merge activation")
	}
}

func TestInspectExposesResolvedGates(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))

	out, err := m.WorkflowInspect(string(wf))
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	activations, ok := out.(map[string]any)["activations"].([]Activation)
	if !ok {
		t.Fatalf("inspect activations shape = %#v", out.(map[string]any)["activations"])
	}
	var review *Activation
	for i := range activations {
		if activations[i].NodeID == "review" {
			review = &activations[i]
		}
	}
	if review == nil {
		t.Fatalf("inspect has no review activation")
	}
	resolved, ok := review.ResolvedGates[GateID("pr-review")]
	if !ok || resolved.Subject == nil || resolved.Subject.Revision != "abc123" {
		t.Fatalf("inspect resolved_gates = %+v, want pinned pr-review subject at abc123", review.ResolvedGates)
	}
	if len(review.CausedBy) != 1 {
		t.Fatalf("inspect caused_by = %v, want the publication activation", review.CausedBy)
	}
}
