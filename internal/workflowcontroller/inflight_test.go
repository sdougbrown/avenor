package workflowcontroller

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/workflow"
)

func TestHeldKeys(t *testing.T) {
	tests := []struct {
		name     string
		inflight []InFlightAttempt
		want     map[string]struct{}
	}{
		{
			name:     "empty",
			inflight: nil,
			want:     map[string]struct{}{},
		},
		{
			name: "terminal attempt releases key",
			inflight: []InFlightAttempt{
				{Identity: ident("wf1", "n1", "a1", "t1"), ConcurrencyKey: "k1", Terminal: true},
			},
			want: map[string]struct{}{},
		},
		{
			name: "non-terminal attempt holds key",
			inflight: []InFlightAttempt{
				{Identity: ident("wf1", "n1", "a1", "t1"), ConcurrencyKey: "k1"},
			},
			want: map[string]struct{}{"k1": {}},
		},
		{
			name: "manual and foreign-controller attempts hold keys",
			inflight: []InFlightAttempt{
				{Identity: ident("wf1", "n1", "a1", "t1"), ConcurrencyKey: "manual-key"},
				{Identity: ident("wf2", "n2", "a2", "t2"), ControllerID: "other", ConcurrencyKey: "other-key"},
			},
			want: map[string]struct{}{"manual-key": {}, "other-key": {}},
		},
		{
			name: "empty key is ignored",
			inflight: []InFlightAttempt{
				{Identity: ident("wf1", "n1", "a1", "t1")},
			},
			want: map[string]struct{}{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := heldKeys(tt.inflight)
			if len(got) != len(tt.want) {
				t.Fatalf("heldKeys() = %v, want %v", got, tt.want)
			}
			for key := range tt.want {
				if _, ok := got[key]; !ok {
					t.Fatalf("heldKeys() missing key %q: %v", key, got)
				}
			}
		})
	}
}

func TestActiveCount(t *testing.T) {
	tests := []struct {
		name         string
		inflight     []InFlightAttempt
		controllerID string
		want         int
	}{
		{
			name:         "empty",
			controllerID: "ctrl",
			want:         0,
		},
		{
			name: "counts only this controller's non-terminal attempts",
			inflight: []InFlightAttempt{
				{Identity: ident("wf1", "n1", "a1", "t1"), ControllerID: "ctrl"},
				{Identity: ident("wf2", "n2", "a2", "t2"), ControllerID: "ctrl"},
				{Identity: ident("wf3", "n3", "a3", "t3"), ControllerID: "other"},
				{Identity: ident("wf4", "n4", "a4", "t4")},
				{Identity: ident("wf5", "n5", "a5", "t5"), ControllerID: "ctrl", Terminal: true},
			},
			controllerID: "ctrl",
			want:         2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := activeCount(tt.inflight, tt.controllerID); got != tt.want {
				t.Fatalf("activeCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestActiveActivations(t *testing.T) {
	inflight := []InFlightAttempt{
		{Identity: ident("wf1", "n1", "a1", "t1"), ControllerID: "ctrl"},
		{Identity: ident("wf2", "n2", "a2", "t2"), Terminal: true},
		{Identity: ident("wf3", "n3", "a3", "t3")},
	}
	got := activeActivations(inflight)
	if len(got) != 2 {
		t.Fatalf("activeActivations() size = %d, want 2: %v", len(got), got)
	}
	for _, want := range []activationKey{
		{WorkflowID: "wf1", NodeID: "n1", ActivationID: "a1"},
		{WorkflowID: "wf3", NodeID: "n3", ActivationID: "a3"},
	} {
		if _, ok := got[want]; !ok {
			t.Fatalf("activeActivations() missing %v: %v", want, got)
		}
	}
}

func ident(wf, node, act, attempt string) workflow.ExecutionIdentity {
	return workflow.ExecutionIdentity{
		WorkflowID:   workflow.WorkflowID(wf),
		NodeID:       workflow.NodeID(node),
		ActivationID: workflow.ActivationID(act),
		AttemptID:    workflow.AttemptID(attempt),
	}
}
