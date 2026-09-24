package workflow

// Tests for kernel-local parking of auto external activations: an eligible
// activation parks into awaiting_gate under the declared success_outcome with
// no attempt, no admission, and no gate result; unresolved bindings leave the
// activation ready; manual-claim and revision races are stale candidates; and
// the candidate query classifies auto external nodes as external_park.

import (
	"encoding/json"
	"errors"
	"testing"
)

// unresolvedAutoExternalTemplate makes the review gate's revision output
// optional so a publication without a head SHA leaves the binding unresolved.
// The base fixture's review node already declares the auto external dispatch.
func unresolvedAutoExternalTemplate() []byte {
	return mutateBoundTemplate(boundGateTemplateJSON, func(template map[string]any) {
		publication := boundGateNode(template, "publication")
		outputs := publication["outputs"].([]any)
		for _, raw := range outputs {
			def := raw.(map[string]any)
			if def["id"] == "pr_head" {
				delete(def, "required")
			}
		}
	})
}

// parkFixture instantiates the auto external review fixture, drives one
// publication, and recovers the candidate index.
func parkFixture(t *testing.T, templateJSON string) (*Manager, *Store, WorkflowID) {
	t.Helper()
	m, s, wf := newCompleteFixture(t, templateJSON, "bound-gates", "1.0.0")
	if err := m.RebuildCandidateIndex("sup"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	return m, s, wf
}

func TestParkExternalParksEligibleActivation(t *testing.T) {
	m, s, wf := parkFixture(t, boundGateTemplateJSON)
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	review := latestActivation(t, s, wf, "review")
	rev := revision(t, s, wf)

	// The candidate query classifies the activation as an external park.
	cands, err := m.CandidatesForController("ctl", 0)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	if len(cands) != 1 || cands[0].Identity.ActivationID != review.ID {
		t.Fatalf("candidates = %+v, want the review activation", cands)
	}
	if cands[0].Kind != ReadyCandidateExternalPark {
		t.Fatalf("candidate kind = %q, want %q", cands[0].Kind, ReadyCandidateExternalPark)
	}
	if cands[0].ConcurrencyKey != "" {
		t.Fatalf("external park candidate carries a concurrency key: %+v", cands[0])
	}

	res, err := m.ParkExternal(ParkExternalRequest{
		WorkflowID:       wf,
		NodeID:           "review",
		ActivationID:     review.ID,
		ExpectedRevision: rev,
		ControllerID:     "ctl",
	})
	if err != nil {
		t.Fatalf("ParkExternal: %v", err)
	}
	if res.SuccessOutcome != OutcomeName("clean") {
		t.Fatalf("success outcome = %q, want clean", res.SuccessOutcome)
	}
	if len(res.Gates) != 1 || res.Gates[0].GateID != GateID("pr-review") || res.Gates[0].AdapterID != "gh-review" {
		t.Fatalf("gate seeds = %+v, want the pr-review gate on gh-review", res.Gates)
	}
	pinned := review.ResolvedGates[GateID("pr-review")].Subject
	if res.Gates[0].SubjectHash != SubjectHash(pinned) {
		t.Fatalf("subject hash = %q, want %q", res.Gates[0].SubjectHash, SubjectHash(pinned))
	}
	if res.Revision != rev+1 {
		t.Fatalf("revision = %d, want %d", res.Revision, rev+1)
	}

	// The activation is parked with the pinned outcome and nothing else: no
	// attempt, no lease, no gate instance, no outputs.
	snap, ok, err := s.loadCurrent(wf)
	if err != nil || !ok {
		t.Fatalf("load current: %v", err)
	}
	var parked *Activation
	for i := range snap.Instance.Activations {
		if snap.Instance.Activations[i].ID == review.ID {
			parked = &snap.Instance.Activations[i]
		}
	}
	if parked == nil || parked.Status != ActivationAwaitingGate {
		t.Fatalf("parked activation = %+v, want awaiting_gate", parked)
	}
	if parked.SelectedOutcome != OutcomeName("clean") {
		t.Fatalf("selected outcome = %q, want clean", parked.SelectedOutcome)
	}
	if len(parked.AttemptIDs) != 0 || parked.ActiveLease != nil {
		t.Fatalf("parked activation recorded runtime state: %+v", parked)
	}
	if len(snap.Instance.Gates) != 0 {
		t.Fatalf("park recorded gate instances: %+v", snap.Instance.Gates)
	}
	for _, at := range snap.Instance.Attempts {
		if at.Identity.ActivationID == review.ID {
			t.Fatalf("park recorded an attempt: %+v", at)
		}
	}

	// A re-park is idempotent and reports the same seeds.
	again, err := m.ParkExternal(ParkExternalRequest{
		WorkflowID:       wf,
		NodeID:           "review",
		ActivationID:     review.ID,
		ExpectedRevision: rev + 1,
		ControllerID:     "ctl",
	})
	if err != nil {
		t.Fatalf("idempotent ParkExternal: %v", err)
	}
	if again.SuccessOutcome != res.SuccessOutcome || len(again.Gates) != 1 || again.Gates[0].SubjectHash != res.Gates[0].SubjectHash {
		t.Fatalf("idempotent park result = %+v, want %+v", again, res)
	}

	// A parked activation is no longer a candidate.
	cands, err = m.CandidatesForController("ctl", 0)
	if err != nil {
		t.Fatalf("CandidatesForController after park: %v", err)
	}
	if len(cands) != 0 {
		t.Fatalf("candidates after park = %+v, want none", cands)
	}
}

func TestParkExternalUnresolvedBindingStaysReady(t *testing.T) {
	m, s, wf := parkFixture(t, string(unresolvedAutoExternalTemplate()))
	driveBoundPublication(t, m, s, wf, []map[string]any{
		{"definition_id": "repository", "value": "org/repo"},
		{"definition_id": "pr_number", "value": 42},
	})
	review := latestActivation(t, s, wf, "review")
	rev := revision(t, s, wf)

	_, err := m.ParkExternal(ParkExternalRequest{
		WorkflowID:       wf,
		NodeID:           "review",
		ActivationID:     review.ID,
		ExpectedRevision: rev,
		ControllerID:     "ctl",
	})
	if !errors.Is(err, ErrUnresolvedBinding) {
		t.Fatalf("error = %v, want ErrUnresolvedBinding", err)
	}
	if got := revision(t, s, wf); got != rev {
		t.Fatalf("revision changed %d -> %d on unresolved binding", rev, got)
	}
	after := latestActivation(t, s, wf, "review")
	if after.Status != ActivationPending {
		t.Fatalf("activation status = %q, want still pending (ready)", after.Status)
	}
	// The activation stays dispatchable to a controller.
	cands, err := m.CandidatesForController("ctl", 0)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	if len(cands) != 1 || cands[0].Identity.ActivationID != review.ID {
		t.Fatalf("candidates = %+v, want the still-ready review activation", cands)
	}
}

func TestParkExternalStaleCandidate(t *testing.T) {
	m, s, wf := parkFixture(t, boundGateTemplateJSON)
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	review := latestActivation(t, s, wf, "review")
	rev := revision(t, s, wf)

	req := ParkExternalRequest{
		WorkflowID:       wf,
		NodeID:           "review",
		ActivationID:     review.ID,
		ExpectedRevision: rev,
		ControllerID:     "ctl",
	}
	// A stale expected revision is benign.
	staleRev := req
	staleRev.ExpectedRevision = rev - 1
	if _, err := m.ParkExternal(staleRev); !errors.Is(err, ErrStaleCandidate) {
		t.Fatalf("stale revision: error = %v, want ErrStaleCandidate", err)
	}
	// A different controller's policy does not match.
	wrongCtl := req
	wrongCtl.ControllerID = "other"
	if _, err := m.ParkExternal(wrongCtl); !errors.Is(err, ErrStaleCandidate) {
		t.Fatalf("controller mismatch: error = %v, want ErrStaleCandidate", err)
	}
	// A competing manual claim wins the race: the park is stale and leaves
	// the claim intact.
	if _, err := m.commandClaim(wf, mustRawJSON(t, map[string]string{
		"node_id": "review", "activation_id": string(review.ID), "actor": "alice",
	})); err != nil {
		t.Fatalf("manual claim: %v", err)
	}
	if _, err := m.ParkExternal(req); !errors.Is(err, ErrStaleCandidate) {
		t.Fatalf("claimed activation: error = %v, want ErrStaleCandidate", err)
	}
	if _, _, err := s.loadCurrent(wf); err != nil {
		t.Fatalf("load current: %v", err)
	}
	claimed := latestActivation(t, s, wf, "review")
	if claimed.Status != ActivationLeased {
		t.Fatalf("activation status = %q, want the manual claim intact (leased)", claimed.Status)
	}
}

func TestCandidatesClassifyProviderNodes(t *testing.T) {
	m, s, wf := newCompleteFixture(t, boundGateTemplateJSON, "bound-gates", "1.0.0")
	driveBoundPublication(t, m, s, wf, boundPublicationOutputs("org/repo", 42, "abc123"))
	if err := m.RebuildCandidateIndex("sup"); err != nil {
		t.Fatalf("RebuildCandidateIndex: %v", err)
	}
	if _, _, err := s.loadCurrent(wf); err != nil {
		t.Fatalf("load current: %v", err)
	}
	cands, err := m.CandidatesForController("ctl", 0)
	if err != nil {
		t.Fatalf("CandidatesForController: %v", err)
	}
	for _, c := range cands {
		if c.Identity.NodeID == "review" && c.Kind != ReadyCandidateExternalPark {
			t.Fatalf("review candidate kind = %q, want external_park", c.Kind)
		}
		if c.Identity.NodeID != "review" && c.Kind != ReadyCandidateProvider {
			t.Fatalf("non-external candidate %s kind = %q, want provider", c.Identity.NodeID, c.Kind)
		}
	}
}

func mustRawJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
