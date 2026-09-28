package workflowcontroller

import (
	"sort"
	"time"

	"github.com/sdougbrown/avenor/internal/workflow"
)

// CandidateKind classifies what a candidate consumes when selected.
type CandidateKind string

const (
	// CandidateProvider is a run/loop/team node that consumes an in-flight
	// slot and its concurrency key when selected.
	CandidateProvider CandidateKind = "provider"

	// CandidateExternalPark is a bound external auto node that parks
	// without admission: no slot, no key, no dispatch.
	CandidateExternalPark CandidateKind = "external_park"
)

// Candidate is one dispatchable activation as reported by the candidate
// query. The caller fills AttemptID-free identities; the controller passes
// them through unchanged.
type Candidate struct {
	Identity       workflow.ExecutionIdentity
	Kind           CandidateKind
	ControllerID   string
	Revision       int64
	ReadyAt        time.Time
	Priority       int
	ConcurrencyKey string
}

// InFlightAttempt is one attempt currently recorded by the workflow store.
type InFlightAttempt struct {
	Identity       workflow.ExecutionIdentity
	ControllerID   string
	ConcurrencyKey string
	Terminal       bool
}

// SelectInput is the full state for one selection pass. Now is supplied by
// the caller; the policy never reads the clock itself.
type SelectInput struct {
	ControllerID string
	Candidates   []Candidate
	InFlight     []InFlightAttempt
	Now          time.Time
	MaxInflight  int
}

// Decision is one selected candidate, in global dispatch order.
type Decision struct {
	Candidate         Candidate
	EffectivePriority int
}

// Select orders the candidates for one controller and returns those it may
// dispatch, honoring effective-priority aging, deterministic tie breaking,
// the controller's in-flight limit, derived concurrency-key occupancy, and
// stale-candidate exclusion. The result depends only on the input slices'
// contents, never their order.
func Select(input SelectInput) []Decision {
	eligible := make([]Candidate, 0, len(input.Candidates))
	for _, candidate := range input.Candidates {
		if candidate.ControllerID != input.ControllerID {
			continue
		}
		eligible = append(eligible, candidate)
	}

	sort.SliceStable(eligible, func(i, j int) bool {
		a, b := eligible[i], eligible[j]
		pa, pb := effectivePriority(a, input.Now), effectivePriority(b, input.Now)
		if pa != pb {
			return pa > pb
		}
		if !a.ReadyAt.Equal(b.ReadyAt) {
			return a.ReadyAt.Before(b.ReadyAt)
		}
		if a.Identity.WorkflowID != b.Identity.WorkflowID {
			return a.Identity.WorkflowID < b.Identity.WorkflowID
		}
		if a.Identity.NodeID != b.Identity.NodeID {
			return a.Identity.NodeID < b.Identity.NodeID
		}
		if a.Identity.ActivationID != b.Identity.ActivationID {
			return a.Identity.ActivationID < b.Identity.ActivationID
		}
		// Same activation: a stable kind ordering keeps the result
		// identical for any input permutation.
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.ConcurrencyKey < b.ConcurrencyKey
	})

	held := heldKeys(input.InFlight)
	claimed := activeActivations(input.InFlight)
	remaining := input.MaxInflight - activeCount(input.InFlight, input.ControllerID)

	decisions := make([]Decision, 0, len(eligible))
	selected := make(map[activationKey]struct{})
	for _, candidate := range eligible {
		key := activationKey{
			WorkflowID:   candidate.Identity.WorkflowID,
			NodeID:       candidate.Identity.NodeID,
			ActivationID: candidate.Identity.ActivationID,
		}
		if _, stale := claimed[key]; stale {
			continue
		}
		if _, dup := selected[key]; dup {
			continue
		}
		if candidate.Kind == CandidateExternalPark {
			selected[key] = struct{}{}
			decisions = append(decisions, Decision{
				Candidate:         candidate,
				EffectivePriority: effectivePriority(candidate, input.Now),
			})
			continue
		}
		if remaining <= 0 {
			continue
		}
		if candidate.ConcurrencyKey != "" {
			if _, busy := held[candidate.ConcurrencyKey]; busy {
				continue
			}
			held[candidate.ConcurrencyKey] = struct{}{}
		}
		selected[key] = struct{}{}
		remaining--
		decisions = append(decisions, Decision{
			Candidate:         candidate,
			EffectivePriority: effectivePriority(candidate, input.Now),
		})
	}
	return decisions
}

// effectivePriority ages a candidate's priority by the whole minutes it has
// been waiting past its ready time, capped at 100. A future ready time
// counts as no wait.
func effectivePriority(candidate Candidate, now time.Time) int {
	wait := now.Sub(candidate.ReadyAt)
	if wait < 0 {
		wait = 0
	}
	priority := candidate.Priority + int(wait.Minutes())
	if priority > 100 {
		priority = 100
	}
	return priority
}
