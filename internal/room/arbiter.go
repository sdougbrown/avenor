package room

import (
	"fmt"
	"strings"
)

// WriteKinds are the tool kinds the arbiter gates during parallel windows.
// bash is deliberately not gated: it is too coarse (tests, reads via shell)
// and the spike treats mutation detection as advisory. The gate is a
// rebase-by-nudge, not a correctness mechanism.
var WriteKinds = map[string]bool{"write": true, "edit": true, "patch": true, "multiedit": true, "notebookedit": true}

// ArbiterDecision is the coarse serialized-mutation policy: during a parallel
// window (more than one head in flight), the first head to request a
// mutating tool wins and every other head's mutation attempt is denied with
// a re-read instruction. Outside parallel windows there is a single writer
// by construction, so requests pass through.
type ArbiterDecision struct {
	Allow   bool
	Message string
}

// Gate decides one permission request. requester is the head name; holder is
// the current write holder ("" if none); parallel is true when more than one
// activation is in flight; kind is the tool kind from the canonical event.
func Gate(requester, holder, kind string, parallel bool) ArbiterDecision {
	if !WriteKinds[strings.ToLower(kind)] {
		return ArbiterDecision{Allow: true}
	}
	if !parallel {
		return ArbiterDecision{Allow: true}
	}
	if holder == "" || holder == requester {
		return ArbiterDecision{Allow: true}
	}
	return ArbiterDecision{
		Allow: false,
		Message: fmt.Sprintf(
			"Workspace was just mutated by peer %s while you were working from the same snapshot. "+
				"Do not write over their change: re-read the affected files, reconcile your edit with theirs, and try again.",
			holder),
	}
}

// optionID picks the option id matching the requested kind from a
// permission.request event's options array.
func optionID(options []any, wantKind string) string {
	for _, o := range options {
		m, ok := o.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := m["kind"].(string)
		if kind == wantKind {
			if id, _ := m["optionId"].(string); id != "" {
				return id
			}
		}
	}
	// Fallback to the conventional ids when kinds are missing.
	if wantKind == "allow" {
		return "allow"
	}
	return "reject"
}
