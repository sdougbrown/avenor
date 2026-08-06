package workflow

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestActionJSONRoundTrip(t *testing.T) {
	tests := []Action{
		{Kind: ActionRun, Run: &RunAction{Prompt: "review this"}},
		{Kind: ActionLoop, Loop: &LoopAction{LoopFile: "loop.json"}},
		{Kind: ActionTeam, Team: &TeamAction{TeamFile: "team.json"}},
		{Kind: ActionManual, Manual: &ManualAction{Instructions: "approve"}},
		{Kind: ActionExternal, External: &ExternalAction{Source: "github", SubjectType: "pull_request"}},
		{Kind: ActionWorkflow, Workflow: &WorkflowAction{
			TemplateID: "review-unit", TemplateVersion: "2", ChildKey: "review-1",
			InputBindings:  []InputBinding{{Input: "head", Value: json.RawMessage(`"abc123"`)}},
			OutputBindings: []OutputBinding{{ChildOutput: "verdict", ParentOutput: "review_verdict"}},
			OutcomeMap:     map[OutcomeName]OutcomeName{"clean": "approved"},
		}},
	}

	for _, action := range tests {
		t.Run(string(action.Kind), func(t *testing.T) {
			data, err := json.Marshal(action)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			var decoded Action
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal() error = %v; JSON = %s", err, data)
			}
			if !reflect.DeepEqual(decoded, action) {
				t.Fatalf("action round trip mismatch\n got: %#v\nwant: %#v", decoded, action)
			}
		})
	}
}

func TestActionRejectsCrossVariantAndUnknownFields(t *testing.T) {
	for _, input := range []string{
		`{"type":"manual","loop_file":"loop.json"}`,
		`{"type":"loop","instructions":"wrong","loop_file":"loop.json"}`,
		`{"type":"run","unknown":true}`,
		`{"Type":"manual"}`,
		`{"type":"manual","type":"run"}`,
		`{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","input_bindings":[{"Input":"head","value":1}],"outcome_map":{"clean":"clean"}}`,
	} {
		var action Action
		if err := json.Unmarshal([]byte(input), &action); err == nil {
			t.Fatalf("Unmarshal(%s) unexpectedly succeeded", input)
		}
	}
}

func TestActionRejectsMissingRequiredPayloads(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "run source", input: `{"type":"run"}`, wantErr: "exactly one of prompt or prompt_file"},
		{name: "run sources are exclusive", input: `{"type":"run","prompt":"do it","prompt_file":"prompt.md"}`, wantErr: "exactly one of prompt or prompt_file"},
		{name: "loop file", input: `{"type":"loop"}`, wantErr: "requires loop_file"},
		{name: "blank loop file", input: `{"type":"loop","loop_file":"  "}`, wantErr: "requires loop_file"},
		{name: "team file", input: `{"type":"team"}`, wantErr: "requires team_file"},
		{name: "external source", input: `{"type":"external"}`, wantErr: "requires source"},
		{name: "workflow template", input: `{"type":"workflow","template_version":"1","child_key":"child","outcome_map":{"clean":"clean"}}`, wantErr: "requires template_id"},
		{name: "workflow version", input: `{"type":"workflow","template_id":"child","child_key":"child","outcome_map":{"clean":"clean"}}`, wantErr: "requires template_version"},
		{name: "workflow child key", input: `{"type":"workflow","template_id":"child","template_version":"1","outcome_map":{"clean":"clean"}}`, wantErr: "requires child_key"},
		{name: "workflow outcome map", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child"}`, wantErr: "requires outcome_map"},
		{name: "blank input name", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","input_bindings":[{"input":" ","value":1}],"outcome_map":{"clean":"clean"}}`, wantErr: "requires input"},
		{name: "missing input source", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","input_bindings":[{"input":"head"}],"outcome_map":{"clean":"clean"}}`, wantErr: "exactly one of value or from"},
		{name: "multiple input sources", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","input_bindings":[{"input":"head","value":1,"from":{"node_id":"build","output_id":"head"}}],"outcome_map":{"clean":"clean"}}`, wantErr: "exactly one of value or from"},
		{name: "input reference node", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","input_bindings":[{"input":"head","from":{"output_id":"head"}}],"outcome_map":{"clean":"clean"}}`, wantErr: "requires node_id"},
		{name: "input reference output", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","input_bindings":[{"input":"head","from":{"node_id":"build"}}],"outcome_map":{"clean":"clean"}}`, wantErr: "requires output_id"},
		{name: "blank child output", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","output_bindings":[{"child_output":" ","parent_output":"result"}],"outcome_map":{"clean":"clean"}}`, wantErr: "requires child_output"},
		{name: "blank parent output", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","output_bindings":[{"child_output":"result","parent_output":" "}],"outcome_map":{"clean":"clean"}}`, wantErr: "requires parent_output"},
		{name: "blank child outcome", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","outcome_map":{" ":"clean"}}`, wantErr: "blank child outcome"},
		{name: "blank parent outcome", input: `{"type":"workflow","template_id":"child","template_version":"1","child_key":"child","outcome_map":{"clean":" "}}`, wantErr: "blank parent outcome"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var action Action
			err := json.Unmarshal([]byte(test.input), &action)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Unmarshal(%s) error = %v, want containing %q", test.input, err, test.wantErr)
			}
		})
	}
}

func TestActionMarshalRejectsMismatchedVariants(t *testing.T) {
	action := Action{Kind: ActionManual, Manual: &ManualAction{}, Loop: &LoopAction{LoopFile: "loop.json"}}
	if _, err := json.Marshal(action); err == nil {
		t.Fatal("Marshal() accepted multiple action variants")
	}
	action = Action{Kind: ActionManual, Loop: &LoopAction{LoopFile: "loop.json"}}
	if _, err := json.Marshal(action); err == nil {
		t.Fatal("Marshal() accepted a mismatched action variant")
	}
	for _, value := range []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`{"key":1,"key":2}`)} {
		action = Action{Kind: ActionWorkflow, Workflow: &WorkflowAction{
			TemplateID: "child", TemplateVersion: "1", ChildKey: "child",
			InputBindings: []InputBinding{{Input: "value", Value: value}},
			OutcomeMap:    map[OutcomeName]OutcomeName{"clean": "clean"},
		}}
		if _, err := json.Marshal(action); err == nil {
			t.Fatalf("Marshal() accepted binding value %s", value)
		}
	}
}

func TestTemplateJSONRoundTrip(t *testing.T) {
	template := sampleTemplate()
	data, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := ValidateTemplateJSON(data); err != nil {
		t.Fatalf("ValidateTemplateJSON() error = %v\nJSON: %s", err, data)
	}
	var decoded Template
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, template) {
		t.Fatalf("template round trip mismatch\n got: %#v\nwant: %#v", decoded, template)
	}
}

func TestTemplateJSONRoundTripPreservesLargeMetadataNumbers(t *testing.T) {
	template := sampleTemplate()
	template.Metadata["large"] = json.Number("1e400")
	data, err := json.Marshal(template)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	if err := ValidateTemplateJSON(data); err != nil {
		t.Fatalf("ValidateTemplateJSON() error = %v", err)
	}
	var decoded Template
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got, ok := decoded.Metadata["large"].(json.Number); !ok || got.String() != "1e400" {
		t.Fatalf("decoded metadata number = %#v, want json.Number(1e400)", decoded.Metadata["large"])
	}

	roundTrip, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("second Marshal() error = %v", err)
	}
	var final Template
	if err := json.Unmarshal(roundTrip, &final); err != nil {
		t.Fatalf("second Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(final, decoded) {
		t.Fatalf("large-number round trip mismatch\n got: %#v\nwant: %#v", final, decoded)
	}
}

func TestTemplateUnmarshalJSONEnforcesStrictBoundary(t *testing.T) {
	data, err := json.Marshal(sampleTemplate())
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	for _, input := range []string{
		strings.Replace(string(data), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		strings.Replace(string(data), `"ttl_seconds":900`, `"TTL_SECONDS":900`, 1),
	} {
		var template Template
		if err := json.Unmarshal([]byte(input), &template); err == nil {
			t.Fatalf("Unmarshal() accepted ambiguous template: %s", input)
		}
	}

	template := sampleTemplate()
	template.Metadata["ambiguous"] = json.RawMessage(`{"key":1,"key":2}`)
	if _, err := json.Marshal(template); err == nil {
		t.Fatal("Marshal() emitted a template that strict decoding would reject")
	}
}

func TestSnapshotJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	ended := now.Add(time.Minute)
	heartbeat := now.Add(30 * time.Second)
	workflowID := WorkflowID("wf_test")
	activationID := ActivationID("act_test")
	attemptID := AttemptID("att_test")
	snapshot := Snapshot{
		SchemaVersion: 1,
		Instance: WorkflowInstance{
			WorkflowID: workflowID, InstanceID: "wfi_test", TemplateID: "factory", TemplateVersion: "1",
			Revision: 4, CreatedAt: now, UpdatedAt: ended, Status: WorkflowAwaitingGate,
			Metadata: map[string]any{"large": json.Number("1e400")},
			Activations: []Activation{{
				ID: activationID, NodeID: "review", Iteration: 1, Status: ActivationAwaitingGate,
				Selection:  &ExecutionSelection{Role: "reviewer", Backend: "pi", Model: "model", RosterDigest: "sha256:abc"},
				AttemptIDs: []AttemptID{attemptID}, CreatedAt: now, UpdatedAt: ended,
			}},
			Attempts: []Attempt{{
				ID: attemptID, Identity: ExecutionIdentity{SupervisorID: "stable-1", WorkflowID: workflowID, NodeID: "review", ActivationID: activationID, AttemptID: attemptID, RuntimeID: "rt_1"},
				Status: AttemptSucceeded, Backend: "pi", StartedAt: now, EndedAt: &ended,
			}},
			Evidence: []Evidence{{ID: "ev_test", Kind: "artifact", Source: EvidenceAgent, Authority: "worker", CreatedAt: now, ActivationID: activationID}},
			Gates:    []GateInstance{{ID: "gate_test", GateID: "approval", ActivationID: activationID, Status: GatePending}},
			Outputs:  []OutputValue{{ID: "out_value", DefinitionID: "result", ActivationID: activationID, Revision: 1, Value: json.RawMessage(`{"ok":true}`), CreatedAt: now}},
		},
		AppliedEventIDs: []EventID{"wfe_1", "wfe_2"},
		Idempotency:     map[string][]EventID{"complete:review": {"wfe_2"}},
	}
	snapshot.Instance.Activations[0].ActiveLease = &Lease{ID: "lease_test", ActivationID: activationID, Owner: "controller", TokenDigest: "sha256:def", AcquiredAt: now, ExpiresAt: ended, LastHeartbeatAt: &heartbeat}

	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("snapshot round trip mismatch\n got: %#v\nwant: %#v", decoded, snapshot)
	}

	for _, input := range []string{
		strings.Replace(string(data), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		strings.Replace(string(data), `{"ok":true}`, `{"ok":true,"ok":false}`, 1),
		strings.Replace(string(data), `"workflow_id":"wf_test"`, `"Workflow_ID":"wf_test"`, 1),
	} {
		var invalid Snapshot
		if err := json.Unmarshal([]byte(input), &invalid); err == nil {
			t.Fatalf("Unmarshal() accepted ambiguous snapshot: %s", input)
		}
	}

	invalid := snapshot
	invalid.Instance.Outputs = append([]OutputValue(nil), snapshot.Instance.Outputs...)
	invalid.Instance.Outputs[0].Value = json.RawMessage(`{"ok":true,"ok":false}`)
	if _, err := json.Marshal(invalid); err == nil {
		t.Fatal("Marshal() emitted a snapshot with an ambiguous output value")
	}
	invalid = snapshot
	invalid.Instance.Evidence = append([]Evidence(nil), snapshot.Instance.Evidence...)
	invalid.Instance.Evidence[0].Result = json.RawMessage(`null`)
	if _, err := json.Marshal(invalid); err == nil {
		t.Fatal("Marshal() emitted a snapshot with a null evidence result")
	}
}

func TestGeneratedIdentifiersAreDistinctAndPrefixed(t *testing.T) {
	ids := []struct {
		value  string
		prefix string
	}{
		{string(NewWorkflowID()), "wf_"},
		{string(NewInstanceID()), "wfi_"},
		{string(NewActivationID()), "act_"},
		{string(NewAttemptID()), "att_"},
		{string(NewEventID()), "wfe_"},
		{string(NewCommandID()), "wfc_"},
		{string(NewLeaseID()), "lease_"},
		{string(NewEvidenceID()), "ev_"},
		{string(NewGateInstanceID()), "gate_"},
		{string(NewOutputID()), "out_"},
		{string(NewChildReferenceID()), "child_"},
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !strings.HasPrefix(id.value, id.prefix) {
			t.Errorf("ID %q does not have prefix %q", id.value, id.prefix)
		}
		if seen[id.value] {
			t.Errorf("duplicate generated ID %q", id.value)
		}
		seen[id.value] = true
	}
}

func sampleTemplate() Template {
	return Template{
		SchemaVersion:   1,
		TemplateID:      "factory",
		TemplateVersion: "1.0.0",
		Metadata:        map[string]any{"title": "Factory"},
		EntryNodes:      []NodeID{"intake"},
		Nodes: []NodeDefinition{
			{ID: "intake", Name: "Intake", Action: Action{Kind: ActionManual, Manual: &ManualAction{Instructions: "approve"}}, Branches: map[OutcomeName]NodeID{"ready": "execute"}},
			{ID: "execute", Dependencies: []NodeID{"intake"}, Action: Action{Kind: ActionLoop, Loop: &LoopAction{LoopFile: "loop.json"}}, RetryPolicy: &RetryPolicy{MaximumAttempts: 2, Exhaustion: RetryExhaustionBlock}},
		},
		TerminalOutcomes:  []OutcomeName{"clean", "abandoned"},
		DefaultLease:      &LeasePolicy{TTLSeconds: 900, HeartbeatIntervalSeconds: 30},
		DefaultRetry:      &RetryPolicy{MaximumAttempts: 2, Exhaustion: RetryExhaustionBlock},
		CompositionLimits: &CompositionLimits{MaximumDepth: 4, MaximumChildren: 32},
	}
}
