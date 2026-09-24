package workflow

// Tests for the primitive hardening of workflow.complete: values that feed
// bound gate inputs and pull_request subjects must be well-formed before any
// output or completion is recorded.

import (
	"encoding/json"
	"strings"
	"testing"
)

// boundPublicationFixture instantiates the bound-gate template and drives the
// publication activation to running with a live lease, returning the claim
// result, activation ID, and attempt ID.
func boundPublicationFixture(t *testing.T) (*Manager, *Store, WorkflowID, map[string]any, ActivationID, AttemptID) {
	t.Helper()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	m := NewManager(s)
	if _, err := m.WorkflowCreate([]byte(boundGateTemplateJSON)); err != nil {
		t.Fatalf("WorkflowCreate: %v", err)
	}
	payload, err := json.Marshal(map[string]string{"template_id": "bound-gates", "template_version": "1.0.0"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := m.WorkflowInstantiate(payload)
	if err != nil {
		t.Fatalf("WorkflowInstantiate: %v", err)
	}
	wf := WorkflowID(out.(map[string]any)["workflow_id"].(string))
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	actID := snap.Instance.Activations[0].ID
	res := claimActivation(t, m, wf, "publication", string(actID), "publisher")
	out, err = m.WorkflowCommand(string(wf), startCommandPayload(t, "publication", string(actID), res, nil))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	attemptID := AttemptID(out.(map[string]any)["attempt_id"].(string))
	return m, s, wf, res, actID, attemptID
}

// boundCompletePayload builds a workflow.complete payload for the publication
// node with a raw outputs JSON fragment so malformed primitives are
// expressible verbatim.
func boundCompletePayload(t *testing.T, actID, attemptID string, res map[string]any, outputs string) json.RawMessage {
	t.Helper()
	payload := `{"node_id":"publication","activation_id":"` + string(actID) +
		`","attempt_id":"` + string(attemptID) +
		`","lease_id":"` + res["lease_id"].(string) +
		`","owner_token":"` + res["owner_token"].(string) +
		`","outcome":"published","outputs":[` + outputs + `]}`
	return json.RawMessage(payload)
}

// validPublicationOutputs completes the publication contract well-formed.
const validPublicationOutputs = `{"definition_id":"repository","value":"org/repo"},` +
	`{"definition_id":"pr_number","value":42},` +
	`{"definition_id":"pr_head","value":"abc123"}`

// TestCompleteRejectsMalformedBoundOutputs pins the primitive hardening of
// workflow.complete: strings are strings, numbers finite, numbers consumed by
// gate bindings integral within the safe integer range, and required outputs
// never null. Every rejection leaves the store untouched.
func TestCompleteRejectsMalformedBoundOutputs(t *testing.T) {
	malformed := []struct {
		name    string
		outputs string
		want    string
	}{
		{"non-integral bound number", `{"definition_id":"repository","value":"org/repo"},{"definition_id":"pr_number","value":12.5},{"definition_id":"pr_head","value":"abc"}`, "integral number"},
		{"bound number beyond safe range", `{"definition_id":"repository","value":"org/repo"},{"definition_id":"pr_number","value":9007199254740993},{"definition_id":"pr_head","value":"abc"}`, "safe integer range"},
		{"non-finite number", `{"definition_id":"repository","value":"org/repo"},{"definition_id":"pr_number","value":1e999},{"definition_id":"pr_head","value":"abc"}`, "finite number"},
		{"null required output", `{"definition_id":"repository","value":"org/repo"},{"definition_id":"pr_number","value":null},{"definition_id":"pr_head","value":"abc"}`, "cannot be null"},
		{"number in string output", `{"definition_id":"repository","value":42},{"definition_id":"pr_number","value":7},{"definition_id":"pr_head","value":"abc"}`, "requires a string value"},
		{"number in boolean output", `{"definition_id":"repository","value":"org/repo"},{"definition_id":"pr_number","value":7},{"definition_id":"pr_head","value":"abc"},{"definition_id":"is_draft","value":42}`, "requires a boolean value"},
	}
	for _, tc := range malformed {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			m, s, wf, res, actID, attemptID := boundPublicationFixture(t)
			revBefore := revision(t, s, wf)
			if _, err := m.commandComplete(wf, boundCompletePayload(t, string(actID), string(attemptID), res, tc.outputs)); err == nil {
				t.Fatalf("malformed completion accepted")
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
			if got := revision(t, s, wf); got != revBefore {
				t.Fatalf("revision changed %d -> %d on rejected completion", revBefore, got)
			}
		})
	}
}

// TestCompleteAcceptsWellFormedBoundOutputs records the declared publication
// outputs once the values satisfy the hardened primitive contract. The
// optional report output is omitted and therefore not recorded.
func TestCompleteAcceptsWellFormedBoundOutputs(t *testing.T) {
	m, s, wf, res, actID, attemptID := boundPublicationFixture(t)

	out, err := m.commandComplete(wf, boundCompletePayload(t, string(actID), string(attemptID), res,
		validPublicationOutputs))
	if err != nil {
		t.Fatalf("well-formed completion rejected: %v", err)
	}
	mm := out.(map[string]any)
	if mm["activation_status"] != string(ActivationSatisfied) {
		t.Fatalf("completion result = %#v, want satisfied", mm)
	}
	snap, _, err := s.loadCurrent(wf)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	recorded := 0
	for _, o := range snap.Instance.Outputs {
		if o.ActivationID == actID {
			recorded++
		}
	}
	if recorded != 3 {
		t.Fatalf("recorded outputs for activation = %d, want 3", recorded)
	}
}
