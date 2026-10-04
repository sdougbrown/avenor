package room

import "strings"

// Activation is one turn of one head, finalized when its session ends.
type Activation struct {
	Participant string
	EventID     string // room log event carrying the final output
	Mode        string // answer | react | bootstrap
	Depth       int
	StopReason  string
	FinalOutput string
	Blocked     bool
}

// State is the bounded view the governor decides over. It is deliberately
// small and structured so a Jev backend can consume it as named fields.
type State struct {
	HumanInput      string
	Participants    []string        // head names in the room
	Settled         []Activation    // this operator turn, causal order
	ReactedTo       map[string]bool // speaker event IDs already used for a react round
	Depth           int             // deepest peer hop so far this turn
	BudgetRemaining int             // auto activations left
	MaxDepth        int             // peer-hop cap, enforced by the coordinator
}

// Decision names which heads to activate next and why. An empty Activate list
// returns control to the operator.
type Decision struct {
	Activate       []string
	SpeakerEventID string // room event the activated heads should react to
	Mode           string
	Reason         string
}

// Governor decides whether another head deserves a turn. Implementations must
// be cheap and side-effect free; the coordinator owns all enforcement.
type Governor interface {
	Decide(State) Decision
}

// MarkerGovernor is the deterministic stub: a head gets a peer round only when
// some settled output contains an ask-peer marker. It exists so the room is
// fully testable and terminable without any model in the control plane.
type MarkerGovernor struct{}

func (MarkerGovernor) Decide(s State) Decision {
	if s.BudgetRemaining <= 0 {
		return Decision{Reason: "budget exhausted"}
	}
	if s.Depth >= s.MaxDepth {
		return Decision{Reason: "peer depth cap"}
	}
	// Most recent unreacted output carrying a marker wins; scanning from the
	// end keeps repeat rounds fresh instead of re-injecting the same event.
	for i := len(s.Settled) - 1; i >= 0; i-- {
		a := s.Settled[i]
		if s.ReactedTo[a.EventID] {
			continue
		}
		if hasAskMarker(a.FinalOutput) {
			return Decision{
				Activate:       others(s, a.Participant),
				SpeakerEventID: a.EventID,
				Mode:           "react",
				Reason:         "ask-peer marker in " + a.EventID,
			}
		}
	}
	return Decision{Reason: "no peer request"}
}

// MaxDepth is enforced by the coordinator and carried in State so a Jev
// backend sees the same envelope.

func hasAskMarker(out string) bool {
	l := strings.ToLower(out)
	for _, m := range AskPeerMarkers {
		if strings.Contains(l, m) {
			return true
		}
	}
	return false
}

func others(s State, speaker string) []string {
	seen := map[string]bool{}
	var out []string
	// Prefer the room roster so a head can wake a peer that has not spoken
	// this turn yet; fall back to settled participants.
	source := s.Participants
	if len(source) == 0 {
		for _, a := range s.Settled {
			source = append(source, a.Participant)
		}
	}
	for _, p := range source {
		if p != speaker && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}
