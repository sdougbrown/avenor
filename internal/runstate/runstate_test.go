package runstate

import "testing"

func TestTranslate(t *testing.T) {
	tests := []struct {
		name         string
		status       string
		phase        string
		wantStatus   string
		wantPhase    string
		wantComplete bool
	}{
		{name: "running", status: "running", phase: "working", wantStatus: "running", wantPhase: "working"},
		{name: "running terminal phase", status: "running", phase: "done", wantStatus: "running", wantPhase: ""},
		{name: "idle active phase", status: "idle", phase: "working", wantStatus: "running", wantPhase: "working"},
		{name: "idle terminal phase", status: "idle", phase: "failed", wantStatus: "failed", wantPhase: "failed", wantComplete: true},
		{name: "ended", status: "ended", wantStatus: "done", wantComplete: true},
		{name: "terminal", status: "timeout", phase: "timeout", wantStatus: "timeout", wantPhase: "timeout", wantComplete: true},
		{name: "waiting", status: "waiting", phase: "waiting", wantStatus: "waiting", wantPhase: "waiting"},
		{name: "blocked", status: "blocked", wantStatus: "failed", wantComplete: true},
		{name: "unknown", status: "unknown", phase: "working", wantStatus: "running", wantPhase: "working"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Translate(tt.status, tt.phase)
			if got.Status != tt.wantStatus || got.Phase != tt.wantPhase || got.TurnComplete != tt.wantComplete {
				t.Fatalf("Translate(%q, %q) = %#v, want status=%q phase=%q complete=%v", tt.status, tt.phase, got, tt.wantStatus, tt.wantPhase, tt.wantComplete)
			}
			if gotPredicate := IsTurnComplete(tt.status, tt.phase); gotPredicate != tt.wantComplete {
				t.Fatalf("IsTurnComplete(%q, %q) = %v, want %v", tt.status, tt.phase, gotPredicate, tt.wantComplete)
			}
		})
	}
}

func TestTerminalStatuses(t *testing.T) {
	for _, status := range []string{"done", "failed", "timeout", "killed"} {
		if !IsTerminalStatus(status) {
			t.Errorf("IsTerminalStatus(%q) = false, want true", status)
		}
	}
	for _, status := range []string{"", "running", "idle", "ended", "waiting", "blocked"} {
		if IsTerminalStatus(status) {
			t.Errorf("IsTerminalStatus(%q) = true, want false", status)
		}
	}
}
