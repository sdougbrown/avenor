package runstate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type conformanceFixture struct {
	Translate []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
		Phase  string `json:"phase"`
		Want   struct {
			Status       string `json:"status"`
			Phase        string `json:"phase"`
			TurnComplete bool   `json:"turnComplete"`
			Recognized   bool   `json:"recognized"`
		} `json:"want"`
	} `json:"translate"`
	SentinelStatus []wordCase[string] `json:"sentinelStatus"`
	TerminalStatus []struct {
		Status string `json:"status"`
		Want   bool   `json:"want"`
	} `json:"terminalStatus"`
	TerminalSentinel  []wordCase[bool] `json:"terminalSentinel"`
	ResumableSentinel []wordCase[bool] `json:"resumableSentinel"`
}

type wordCase[T any] struct {
	Word string `json:"word"`
	Want T      `json:"want"`
}

// loadConformanceFixture reads schemas/runstate.conformance.json, the table the
// TypeScript run-state module is pinned to as well, relative to this file so
// the test runs the same from any working directory.
func loadConformanceFixture(t *testing.T) conformanceFixture {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file to resolve the shared fixture")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "..", "..", "schemas", "runstate.conformance.json"))
	if err != nil {
		t.Fatalf("read runstate conformance fixture: %v", err)
	}
	var f conformanceFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse runstate conformance fixture: %v", err)
	}
	if len(f.Translate) == 0 || len(f.SentinelStatus) == 0 || len(f.TerminalSentinel) == 0 || len(f.ResumableSentinel) == 0 || len(f.TerminalStatus) == 0 {
		t.Fatal("runstate conformance fixture has an empty section")
	}
	return f
}

func TestTranslate(t *testing.T) {
	for _, tt := range loadConformanceFixture(t).Translate {
		t.Run(tt.Name, func(t *testing.T) {
			status, phase := RuntimeStatus(tt.Status), Phase(tt.Phase)
			got := Translate(status, phase)
			if got.Status != RuntimeStatus(tt.Want.Status) || got.Phase != Phase(tt.Want.Phase) || got.TurnComplete != tt.Want.TurnComplete {
				t.Fatalf("Translate(%q, %q) = %#v, want status=%q phase=%q complete=%v", tt.Status, tt.Phase, got, tt.Want.Status, tt.Want.Phase, tt.Want.TurnComplete)
			}
			if got.StatusRecognized != tt.Want.Recognized {
				t.Fatalf("Translate(%q, %q).StatusRecognized = %v, want %v", tt.Status, tt.Phase, got.StatusRecognized, tt.Want.Recognized)
			}
			if gotPredicate := IsTurnComplete(status, phase); gotPredicate != tt.Want.TurnComplete {
				t.Fatalf("IsTurnComplete(%q, %q) = %v, want %v", tt.Status, tt.Phase, gotPredicate, tt.Want.TurnComplete)
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
	for _, tt := range loadConformanceFixture(t).SentinelStatus {
		t.Run(tt.Word, func(t *testing.T) {
			if got := SentinelStatus(tt.Word); string(got) != tt.Want {
				t.Fatalf("SentinelStatus(%q) = %q, want %q", tt.Word, got, tt.Want)
			}
		})
	}
}

func TestSentinelClassification(t *testing.T) {
	f := loadConformanceFixture(t)
	for _, tt := range f.TerminalSentinel {
		t.Run("terminal "+tt.Word, func(t *testing.T) {
			if got := IsTerminalSentinel(tt.Word); got != tt.Want {
				t.Fatalf("IsTerminalSentinel(%q) = %v, want %v", tt.Word, got, tt.Want)
			}
		})
	}
	for _, tt := range f.ResumableSentinel {
		t.Run("resumable "+tt.Word, func(t *testing.T) {
			if got := IsResumableStatus(tt.Word); got != tt.Want {
				t.Fatalf("IsResumableStatus(%q) = %v, want %v", tt.Word, got, tt.Want)
			}
		})
	}
}

func TestTerminalStatuses(t *testing.T) {
	for _, tt := range loadConformanceFixture(t).TerminalStatus {
		if got := IsTerminalStatus(RuntimeStatus(tt.Status)); got != tt.Want {
			t.Errorf("IsTerminalStatus(%q) = %v, want %v", tt.Status, got, tt.Want)
		}
		if got := IsTerminalPhase(Phase(tt.Status)); got != tt.Want {
			t.Errorf("IsTerminalPhase(%q) = %v, want %v", tt.Status, got, tt.Want)
		}
	}
}
