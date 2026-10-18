package runstate

import "testing"

func TestTranslate(t *testing.T) {
	tests := []struct {
		name           string
		status         RuntimeStatus
		phase          Phase
		wantStatus     RuntimeStatus
		wantPhase      Phase
		wantComplete   bool
		wantRecognized bool
	}{
		{name: "running", status: StatusRunning, phase: PhaseWorking, wantStatus: StatusRunning, wantPhase: PhaseWorking, wantRecognized: true},
		{name: "running terminal phase", status: StatusRunning, phase: PhaseDone, wantStatus: StatusRunning, wantPhase: PhaseNone, wantRecognized: true},
		{name: "idle active phase", status: StatusIdle, phase: PhaseWorking, wantStatus: StatusRunning, wantPhase: PhaseWorking, wantRecognized: true},
		{name: "idle terminal phase", status: StatusIdle, phase: "failed", wantStatus: StatusFailed, wantPhase: "failed", wantComplete: true, wantRecognized: true},
		{name: "ended", status: StatusEnded, wantStatus: StatusDone, wantComplete: true, wantRecognized: true},
		{name: "terminal", status: StatusTimeout, phase: "timeout", wantStatus: StatusTimeout, wantPhase: "timeout", wantComplete: true, wantRecognized: true},
		{name: "waiting", status: StatusWaiting, phase: "waiting", wantStatus: StatusWaiting, wantPhase: "waiting", wantRecognized: true},
		{name: "blocked", status: StatusBlocked, wantStatus: StatusFailed, wantComplete: true, wantRecognized: true},
		{name: "unknown", status: StatusUnknown, phase: PhaseWorking, wantStatus: StatusUnknown, wantPhase: PhaseWorking, wantRecognized: false},
		{name: "empty status", status: "", phase: PhaseWorking, wantStatus: StatusUnknown, wantPhase: PhaseWorking, wantRecognized: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Translate(tt.status, tt.phase)
			if got.Status != tt.wantStatus || got.Phase != tt.wantPhase || got.TurnComplete != tt.wantComplete {
				t.Fatalf("Translate(%q, %q) = %#v, want status=%q phase=%q complete=%v", tt.status, tt.phase, got, tt.wantStatus, tt.wantPhase, tt.wantComplete)
			}
			if got.StatusRecognized != tt.wantRecognized {
				t.Fatalf("Translate(%q, %q).StatusRecognized = %v, want %v", tt.status, tt.phase, got.StatusRecognized, tt.wantRecognized)
			}
			if gotPredicate := IsTurnComplete(tt.status, tt.phase); gotPredicate != tt.wantComplete {
				t.Fatalf("IsTurnComplete(%q, %q) = %v, want %v", tt.status, tt.phase, gotPredicate, tt.wantComplete)
			}
		})
	}
}

func TestParsePhase(t *testing.T) {
	tests := []struct {
		raw    string
		want   Phase
		wantOK bool
	}{
		{raw: "", want: PhaseNone, wantOK: true},
		{raw: "thinking", want: PhaseThinking, wantOK: true},
		{raw: "working", want: PhaseWorking, wantOK: true},
		{raw: "waiting", want: PhaseWaiting, wantOK: true},
		{raw: "done", want: PhaseDone, wantOK: true},
		{raw: "failed", want: PhaseUnknown},
		{raw: "running", want: PhaseUnknown},
		{raw: "WORKING", want: PhaseUnknown},
		{raw: "working ", want: PhaseUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, ok := ParsePhase(tt.raw)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("ParsePhase(%q) = (%q, %v), want (%q, %v)", tt.raw, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestSentinelStatus(t *testing.T) {
	tests := []struct {
		raw  string
		want RuntimeStatus
	}{
		{raw: "DONE", want: StatusDone},
		{raw: "FAILED", want: StatusFailed},
		{raw: "BLOCKED", want: StatusFailed},
		{raw: "TIMEOUT", want: StatusTimeout},
		{raw: "KILLED", want: StatusKilled},
		{raw: "STOPPED", want: "stopped"},
		{raw: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			if got := SentinelStatus(tt.raw); got != tt.want {
				t.Fatalf("SentinelStatus(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestTerminalStatuses(t *testing.T) {
	for _, status := range []RuntimeStatus{StatusDone, StatusFailed, StatusTimeout, StatusKilled} {
		if !IsTerminalStatus(status) {
			t.Errorf("IsTerminalStatus(%q) = false, want true", status)
		}
	}
	for _, status := range []RuntimeStatus{"", StatusRunning, StatusIdle, StatusEnded, StatusWaiting, StatusBlocked, StatusUnknown} {
		if IsTerminalStatus(status) {
			t.Errorf("IsTerminalStatus(%q) = true, want false", status)
		}
	}
}
