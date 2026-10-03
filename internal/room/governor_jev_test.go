package room

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJevGovernorAsksAndRoutes(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "jev-test",
			"answers": map[string]any{
				"next":          map[string]any{"type": "choice", "choice": "b", "confidence": 0.9, "probabilities": map[string]any{"b": 0.9, "human": 0.1}},
				"mode":          map[string]any{"type": "choice", "choice": "challenge", "confidence": 0.8},
				"needs_human":   map[string]any{"type": "noul", "noul": 0.05},
				"requests_peer": map[string]any{"type": "noul", "noul": 0.1},
			},
			"usage": map[string]any{"input_tokens": 10, "output_tokens": 5},
		})
	}))
	defer srv.Close()

	g := NewJevGovernor("test-key", MarkerGovernor{})
	g.Endpoint = srv.URL
	d := g.Decide(State{
		HumanInput:      "fix the flaky test",
		Participants:    []string{"a", "b"},
		Settled:         []Activation{{Participant: "a", EventID: "a2", FinalOutput: "my answer"}},
		BudgetRemaining: 4,
		MaxDepth:        2,
	})
	if len(d.Activate) != 1 || d.Activate[0] != "b" {
		t.Fatalf("decision = %+v, want activate [b]", d)
	}
	if d.SpeakerEventID != "a2" {
		t.Fatalf("speaker event = %q, want a2", d.SpeakerEventID)
	}
	if d.Mode != "challenge" {
		t.Fatalf("mode = %q, want challenge", d.Mode)
	}
	if !strings.Contains(d.Reason, "jev: next=b") {
		t.Fatalf("reason = %q", d.Reason)
	}

	// The state payload must carry the named fields the questions reference.
	st := gotBody["state"].(map[string]any)
	for _, key := range []string{"human_request", "participants", "peer_depth", "budget_remaining", "last_speaker", "turns_this_request"} {
		if _, ok := st[key]; !ok {
			t.Fatalf("state missing %q", key)
		}
	}
}

func TestJevGovernorDeadbandReturnsToHuman(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"answers": map[string]any{
				"next":        map[string]any{"type": "choice", "choice": "b", "confidence": 0.2, "probabilities": map[string]any{"b": 0.5, "human": 0.5}},
				"mode":        map[string]any{"type": "choice", "choice": "review", "confidence": 0.5},
				"needs_human": map[string]any{"type": "noul", "noul": 0.2},
			},
		})
	}))
	defer srv.Close()
	g := NewJevGovernor("k", MarkerGovernor{})
	g.Endpoint = srv.URL
	d := g.Decide(State{Participants: []string{"a", "b"}, BudgetRemaining: 4, MaxDepth: 2})
	if len(d.Activate) != 0 {
		t.Fatalf("low confidence must return to operator: %+v", d)
	}
	if !strings.Contains(d.Reason, "deadband") {
		t.Fatalf("reason = %q", d.Reason)
	}
}

func TestJevGovernorNeedsHumanShortCircuit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"answers": map[string]any{
				"next":        map[string]any{"type": "choice", "choice": "b", "confidence": 0.9},
				"mode":        map[string]any{"type": "choice", "choice": "review"},
				"needs_human": map[string]any{"type": "noul", "noul": 0.95},
			},
		})
	}))
	defer srv.Close()
	g := NewJevGovernor("k", MarkerGovernor{})
	g.Endpoint = srv.URL
	d := g.Decide(State{Participants: []string{"a", "b"}, BudgetRemaining: 4, MaxDepth: 2})
	if len(d.Activate) != 0 || !strings.Contains(d.Reason, "operator") {
		t.Fatalf("needs_human must short-circuit: %+v", d)
	}
}

func TestJevGovernorFallsBackOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	g := NewJevGovernor("k", MarkerGovernor{})
	g.Endpoint = srv.URL
	d := g.Decide(State{
		Participants:    []string{"a", "b"},
		Settled:         []Activation{{Participant: "a", EventID: "a2", FinalOutput: "[[ask-peer]]"}},
		BudgetRemaining: 4, MaxDepth: 2,
	})
	if len(d.Activate) != 1 || d.Activate[0] != "b" {
		t.Fatalf("fallback decision = %+v, want marker behavior", d)
	}
	if !strings.Contains(d.Reason, "jev unavailable") {
		t.Fatalf("reason = %q", d.Reason)
	}
}

func TestJevGovernorHonorsExplicitPeerRequest(t *testing.T) {
	// The model prefers stop, but the last speaker explicitly asked for peer
	// input; the envelope rule forces the peer round (who/mode still Jev's).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"answers": map[string]any{
				"next":          map[string]any{"type": "choice", "choice": "stop", "confidence": 0.6, "probabilities": map[string]any{"stop": 0.6, "b": 0.3, "human": 0.1}},
				"mode":          map[string]any{"type": "choice", "choice": "review", "confidence": 0.7},
				"needs_human":   map[string]any{"type": "noul", "noul": 0.2},
				"requests_peer": map[string]any{"type": "noul", "noul": 0.95},
			},
		})
	}))
	defer srv.Close()
	g := NewJevGovernor("k", MarkerGovernor{})
	g.Endpoint = srv.URL
	d := g.Decide(State{
		Participants:    []string{"a", "b"},
		Settled:         []Activation{{Participant: "a", EventID: "a2", FinalOutput: "done [[ask-peer]]"}},
		BudgetRemaining: 4, MaxDepth: 2,
	})
	if len(d.Activate) != 1 || d.Activate[0] != "b" || d.SpeakerEventID != "a2" || d.Mode != "review" {
		t.Fatalf("peer request not honored: %+v", d)
	}
	if !strings.Contains(d.Reason, "peer request honored") {
		t.Fatalf("reason = %q", d.Reason)
	}
}
