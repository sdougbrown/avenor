package workflowcontroller

import (
	"fmt"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

var now = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

func cand(wf, node, act string, opts ...func(*Candidate)) Candidate {
	c := Candidate{
		Identity: workflow.ExecutionIdentity{
			WorkflowID:   workflow.WorkflowID(wf),
			NodeID:       workflow.NodeID(node),
			ActivationID: workflow.ActivationID(act),
		},
		ControllerID: "ctrl",
		ReadyAt:      now,
	}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

func withPriority(p int) func(*Candidate) {
	return func(c *Candidate) { c.Priority = p }
}

func withReadyAt(t time.Time) func(*Candidate) {
	return func(c *Candidate) { c.ReadyAt = t }
}

func withKey(key string) func(*Candidate) {
	return func(c *Candidate) { c.ConcurrencyKey = key }
}

func withController(id string) func(*Candidate) {
	return func(c *Candidate) { c.ControllerID = id }
}

func withKind(k CandidateKind) func(*Candidate) {
	return func(c *Candidate) { c.Kind = k }
}

func withRevision(rev int64) func(*Candidate) {
	return func(c *Candidate) { c.Revision = rev }
}

func inf(wf, node, act string, opts ...func(*InFlightAttempt)) InFlightAttempt {
	a := InFlightAttempt{
		Identity: workflow.ExecutionIdentity{
			WorkflowID:   workflow.WorkflowID(wf),
			NodeID:       workflow.NodeID(node),
			ActivationID: workflow.ActivationID(act),
			AttemptID:    "att-1",
		},
		ControllerID: "ctrl",
	}
	for _, opt := range opts {
		opt(&a)
	}
	return a
}

func withInfController(id string) func(*InFlightAttempt) {
	return func(a *InFlightAttempt) { a.ControllerID = id }
}

func withInfKey(key string) func(*InFlightAttempt) {
	return func(a *InFlightAttempt) { a.ConcurrencyKey = key }
}

func terminal(a *InFlightAttempt) { a.Terminal = true }

func ids(decisions []Decision) string {
	out := ""
	for i, d := range decisions {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%s/%s/%s", d.Candidate.Identity.WorkflowID, d.Candidate.Identity.NodeID, d.Candidate.Identity.ActivationID)
	}
	return out
}

func TestSelectOrdering(t *testing.T) {
	tests := []struct {
		name     string
		input    SelectInput
		wantIDs  string
		wantPrio []int
	}{
		{
			name: "effective priority orders by aged priority",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf1", "n1", "a1", withPriority(10)),
					cand("wf2", "n2", "a2", withPriority(50)),
				},
			},
			wantIDs:  "wf2/n2/a2,wf1/n1/a1",
			wantPrio: []int{50, 10},
		},
		{
			name: "aging lifts priority by whole minutes waited",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf1", "n1", "a1", withPriority(10)),
					cand("wf2", "n2", "a2", withPriority(5), withReadyAt(now.Add(-30*time.Minute))),
				},
			},
			wantIDs:  "wf2/n2/a2,wf1/n1/a1",
			wantPrio: []int{35, 10},
		},
		{
			name: "aging caps at 100",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf1", "n1", "a1", withPriority(95)),
					cand("wf2", "n2", "a2", withPriority(99), withReadyAt(now.Add(-90*time.Minute))),
				},
			},
			wantIDs:  "wf2/n2/a2,wf1/n1/a1",
			wantPrio: []int{100, 95},
		},
		{
			name: "future ready time counts as no wait",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf1", "n1", "a1", withPriority(90), withReadyAt(now.Add(5*time.Minute))),
					cand("wf2", "n2", "a2", withPriority(90)),
				},
			},
			// Equal effective priority; ReadyAt breaks the tie ascending.
			wantIDs:  "wf2/n2/a2,wf1/n1/a1",
			wantPrio: []int{90, 90},
		},
		{
			name: "ready time breaks priority tie ascending",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf1", "n1", "a1", withPriority(50), withReadyAt(now.Add(-10*time.Minute))),
					cand("wf2", "n2", "a2", withPriority(50)),
				},
			},
			// wf1 ages to 60 and outranks wf2's unaged 50.
			wantIDs:  "wf1/n1/a1,wf2/n2/a2",
			wantPrio: []int{60, 50},
		},
		{
			name: "workflow, node, activation break remaining ties",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf2", "n1", "a1", withPriority(50)),
					cand("wf1", "n2", "a2", withPriority(50)),
					cand("wf1", "n1", "a9", withPriority(50)),
					cand("wf1", "n1", "a1", withPriority(50)),
				},
			},
			wantIDs:  "wf1/n1/a1,wf1/n1/a9,wf1/n2/a2,wf2/n1/a1",
			wantPrio: []int{50, 50, 50, 50},
		},
		{
			name: "interleaved workflows order globally",
			input: SelectInput{
				ControllerID: "ctrl",
				Now:          now,
				MaxInflight:  10,
				Candidates: []Candidate{
					cand("wf2", "n1", "a1", withPriority(80)),
					cand("wf1", "n1", "a1", withPriority(90)),
					cand("wf3", "n1", "a1", withPriority(70)),
					cand("wf1", "n2", "a2", withPriority(80)),
				},
			},
			wantIDs:  "wf1/n1/a1,wf1/n2/a2,wf2/n1/a1,wf3/n1/a1",
			wantPrio: []int{90, 80, 80, 70},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Select(tt.input)
			if ids(got) != tt.wantIDs {
				t.Fatalf("Select() order = %s, want %s", ids(got), tt.wantIDs)
			}
			for i, want := range tt.wantPrio {
				if got[i].EffectivePriority != want {
					t.Fatalf("decision %d effective priority = %d, want %d", i, got[i].EffectivePriority, want)
				}
			}
		})
	}
}

func TestSelectPermutationInvariance(t *testing.T) {
	candidates := []Candidate{
		cand("wf2", "n1", "a1", withPriority(80), withKey("k1")),
		cand("wf1", "n1", "a1", withPriority(90), withKey("k1")),
		cand("wf3", "n1", "a1", withPriority(70)),
		cand("wf1", "n2", "a2", withPriority(80)),
		cand("wf2", "n2", "a2", withPriority(70), withReadyAt(now.Add(-2*time.Minute))),
	}
	inflight := []InFlightAttempt{
		inf("wf0", "n0", "a0", withInfController("other"), withInfKey("k2")),
		inf("wf9", "n9", "a9"),
	}

	want := ids(Select(SelectInput{
		ControllerID: "ctrl",
		Candidates:   candidates,
		InFlight:     inflight,
		Now:          now,
		MaxInflight:  3,
	}))
	if want == "" {
		t.Fatal("baseline selection was empty")
	}

	// Repeated rotations of the input slices must produce identical output.
	rotate := func(s []Candidate) []Candidate {
		out := make([]Candidate, len(s))
		copy(out, s)
		return out
	}
	rotated := rotate(candidates)
	for i := 0; i < len(candidates); i++ {
		rotatedInFlight := append([]InFlightAttempt{inflight[len(inflight)-1]}, inflight[:len(inflight)-1]...)
		got := ids(Select(SelectInput{
			ControllerID: "ctrl",
			Candidates:   rotated,
			InFlight:     rotatedInFlight,
			Now:          now,
			MaxInflight:  3,
		}))
		if got != want {
			t.Fatalf("rotation %d: Select() = %s, want %s", i, got, want)
		}
		rotated = append(rotated[len(rotated)-1:], rotated[:len(rotated)-1]...)
	}
}

func TestSelectStaleCandidateExcluded(t *testing.T) {
	input := SelectInput{
		ControllerID: "ctrl",
		Now:          now,
		Candidates: []Candidate{
			cand("wf1", "n1", "a1", withPriority(90)),
			cand("wf2", "n2", "a2", withPriority(50)),
		},
		InFlight: []InFlightAttempt{
			// Manual start already claimed the activation.
			inf("wf1", "n1", "a1", withInfController("")),
		},
		MaxInflight: 5,
	}
	got := Select(input)
	if len(got) != 1 || got[0].Candidate.Identity.WorkflowID != "wf2" {
		t.Fatalf("Select() = %s, want only wf2/n2/a2", ids(got))
	}
}

func TestSelectControllerMismatchIgnored(t *testing.T) {
	input := SelectInput{
		ControllerID: "ctrl",
		Now:          now,
		Candidates: []Candidate{
			cand("wf1", "n1", "a1", withController("other")),
			cand("wf2", "n2", "a2"),
		},
		InFlight: []InFlightAttempt{
			inf("wf3", "n3", "a3", withInfController("other")),
		},
		MaxInflight: 5,
	}
	got := Select(input)
	if len(got) != 1 || got[0].Candidate.Identity.WorkflowID != "wf2" {
		t.Fatalf("Select() = %s, want only wf2/n2/a2", ids(got))
	}
}

func TestSelectInflightCounting(t *testing.T) {
	tests := []struct {
		name        string
		inflight    []InFlightAttempt
		maxInflight int
		wantIDs     string
	}{
		{
			name: "exhaustion skips remaining candidates",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0"),
			},
			maxInflight: 1,
			wantIDs:     "",
		},
		{
			name: "terminal attempt frees the slot",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", terminal),
			},
			maxInflight: 1,
			wantIDs:     "wf1/n1/a1",
		},
		{
			name: "other controller's attempts do not consume slots",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", withInfController("other")),
				inf("wf0", "n0", "a1", withInfController("other")),
			},
			maxInflight: 1,
			wantIDs:     "wf1/n1/a1",
		},
		{
			name: "manual attempts do not consume slots",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", withInfController("")),
			},
			maxInflight: 1,
			wantIDs:     "wf1/n1/a1",
		},
		{
			name:        "max_inflight at or below zero selects no provider candidates",
			maxInflight: -1,
			wantIDs:     "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Select(SelectInput{
				ControllerID: "ctrl",
				Candidates:   []Candidate{cand("wf1", "n1", "a1")},
				InFlight:     tt.inflight,
				Now:          now,
				MaxInflight:  tt.maxInflight,
			})
			if ids(got) != tt.wantIDs {
				t.Fatalf("Select() = %s, want %s", ids(got), tt.wantIDs)
			}
		})
	}
}

func TestSelectConcurrencyKeys(t *testing.T) {
	tests := []struct {
		name       string
		inflight   []InFlightAttempt
		candidates []Candidate
		wantIDs    string
	}{
		{
			name: "cross-controller attempt holds key",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", withInfController("other"), withInfKey("k1")),
			},
			wantIDs: "",
		},
		{
			name: "manual attempt holds key without consuming slots",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", withInfController(""), withInfKey("k1")),
			},
			wantIDs: "",
		},
		{
			name: "manual attempt holds key while leaving slot free",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", withInfController(""), withInfKey("k1")),
			},
			candidates: []Candidate{
				cand("wf1", "n1", "a1", withKey("k1")),
				cand("wf2", "n2", "a2"),
			},
			wantIDs: "wf2/n2/a2",
		},
		{
			name: "terminal attempt releases key and slot",
			inflight: []InFlightAttempt{
				inf("wf0", "n0", "a0", withInfKey("k1"), terminal),
			},
			wantIDs: "wf1/n1/a1",
		},
		{
			name: "key held on different activation of same workflow",
			inflight: []InFlightAttempt{
				inf("wf9", "n9", "a9", withInfKey("k1")),
			},
			wantIDs: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := tt.candidates
			if candidates == nil {
				candidates = []Candidate{cand("wf1", "n1", "a1", withKey("k1"))}
			}
			got := Select(SelectInput{
				ControllerID: "ctrl",
				Candidates:   candidates,
				InFlight:     tt.inflight,
				Now:          now,
				MaxInflight:  1,
			})
			if ids(got) != tt.wantIDs {
				t.Fatalf("Select() = %s, want %s", ids(got), tt.wantIDs)
			}
		})
	}
}

func TestSelectSameKeyOnlyFirstSelected(t *testing.T) {
	input := SelectInput{
		ControllerID: "ctrl",
		Now:          now,
		Candidates: []Candidate{
			cand("wf1", "n1", "a1", withPriority(90), withKey("k1")),
			cand("wf2", "n2", "a2", withPriority(80), withKey("k1")),
			cand("wf3", "n3", "a3", withPriority(70)),
		},
		MaxInflight: 5,
	}
	got := Select(input)
	if ids(got) != "wf1/n1/a1,wf3/n3/a3" {
		t.Fatalf("Select() = %s, want wf1/n1/a1,wf3/n3/a3", ids(got))
	}
}

func TestSelectExternalPark(t *testing.T) {
	input := SelectInput{
		ControllerID: "ctrl",
		Now:          now,
		Candidates: []Candidate{
			cand("wf1", "n1", "a1", withPriority(50)),
			cand("wf2", "n2", "a2", withPriority(90), withKind(CandidateExternalPark), withKey("k-park")),
			cand("wf3", "n3", "a3", withPriority(80)),
		},
		MaxInflight: 1,
	}
	got := Select(input)
	// External park is selected despite exhausted slots, keeps its global
	// position, and never blocks a same-key provider candidate later.
	if ids(got) != "wf2/n2/a2,wf3/n3/a3" {
		t.Fatalf("Select() = %s, want wf2/n2/a2,wf3/n3/a3", ids(got))
	}

	sameKey := Select(SelectInput{
		ControllerID: "ctrl",
		Now:          now,
		Candidates: []Candidate{
			cand("wf2", "n2", "a2", withPriority(90), withKind(CandidateExternalPark), withKey("k1")),
			cand("wf1", "n1", "a1", withPriority(80), withKey("k1")),
		},
		MaxInflight: 5,
	})
	if ids(sameKey) != "wf2/n2/a2,wf1/n1/a1" {
		t.Fatalf("Select() = %s, want external park not to hold key", ids(sameKey))
	}
}

func TestSelectDuplicateCandidateSelectedOnce(t *testing.T) {
	input := SelectInput{
		ControllerID: "ctrl",
		Now:          now,
		Candidates: []Candidate{
			cand("wf1", "n1", "a1", withPriority(90), withKey("k1"), withRevision(7)),
			cand("wf1", "n1", "a1", withPriority(90), withKey("k1"), withRevision(7)),
			cand("wf1", "n1", "a1", withPriority(50)),
		},
		MaxInflight: 5,
	}
	got := Select(input)
	if len(got) != 1 {
		t.Fatalf("Select() = %d decisions, want 1: %s", len(got), ids(got))
	}
	if got[0].Candidate.Revision != 7 {
		t.Fatalf("revision = %d, want 7", got[0].Candidate.Revision)
	}
	if got[0].EffectivePriority != 90 {
		t.Fatalf("effective priority = %d, want 90", got[0].EffectivePriority)
	}
}

func TestSelectRevisionPassedThrough(t *testing.T) {
	revisions := []int64{0, 42, -1}
	candidates := make([]Candidate, 0, len(revisions))
	for i, rev := range revisions {
		candidates = append(candidates, cand(
			fmt.Sprintf("wf%d", i), "n1", "a1", withRevision(rev), withPriority(100-i)))
	}
	got := Select(SelectInput{
		ControllerID: "ctrl",
		Candidates:   candidates,
		Now:          now,
		MaxInflight:  len(revisions),
	})
	if len(got) != len(revisions) {
		t.Fatalf("Select() = %d decisions, want %d", len(got), len(revisions))
	}
	for i, d := range got {
		want := revisions[i]
		if d.Candidate.Revision != want {
			t.Fatalf("decision %d revision = %d, want %d", i, d.Candidate.Revision, want)
		}
	}
}
