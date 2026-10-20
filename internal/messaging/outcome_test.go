package messaging

import (
	"encoding/json"
	"os"
	"testing"
)

type outcomeFixture struct {
	Outcomes []struct {
		Outcome                string `json:"outcome"`
		MayHaveBeenTransmitted bool   `json:"mayHaveBeenTransmitted"`
		Terminal               bool   `json:"terminal"`
		ReuseMessageID         bool   `json:"reuseMessageId"`
	} `json:"outcomes"`
}

type admissionFixture struct {
	AdmissionRejections []struct {
		Case                   string `json:"case"`
		Outcome                string `json:"outcome"`
		MayHaveBeenTransmitted bool   `json:"mayHaveBeenTransmitted"`
	} `json:"admissionRejections"`
}

func TestOutcomeFixtures(t *testing.T) {
	vocabulary := readJSON[outcomeFixture](t, "testdata/outcome_vocabulary.json")
	known := map[Outcome]bool{
		OutcomeSent: true, OutcomeReplied: true, OutcomeTimedOut: true,
		OutcomeWithdrawn: true, OutcomeEdgeAbsent: true, OutcomeAuthRejected: true,
		OutcomeNotSent: true, OutcomePossiblyPending: true, OutcomeLost: true,
	}
	if len(vocabulary.Outcomes) != len(known) {
		t.Fatalf("fixture lists %d outcomes, want %d", len(vocabulary.Outcomes), len(known))
	}
	for _, row := range vocabulary.Outcomes {
		outcome := Outcome(row.Outcome)
		if !known[outcome] {
			t.Errorf("fixture outcome %q has no constant", row.Outcome)
			continue
		}
		delete(known, outcome)
		if row.ReuseMessageID {
			t.Errorf("outcome %q claims message ID reuse", row.Outcome)
		}
	}
	if len(known) != 0 {
		t.Errorf("constants missing from fixture: %v", known)
	}

	admission := readJSON[admissionFixture](t, "testdata/outcome_admission.json")
	want := map[string]Outcome{
		"admission-rejected":                           OutcomeNotSent,
		"credential-rejected":                          OutcomeAuthRejected,
		"generation-mismatch-after-transmission":       OutcomePossiblyPending,
		"generation-replaced-after-transmission":       OutcomeLost,
		"cancellation-rejected-without-terminal-fence": OutcomeEdgeAbsent,
	}
	if len(admission.AdmissionRejections) != len(want) {
		t.Fatalf("fixture lists %d admission rejections, want %d", len(admission.AdmissionRejections), len(want))
	}
	for _, row := range admission.AdmissionRejections {
		expected, ok := want[row.Case]
		if !ok {
			t.Errorf("unexpected admission case %q", row.Case)
			continue
		}
		if Outcome(row.Outcome) != expected {
			t.Errorf("case %q outcome = %q, want %q", row.Case, row.Outcome, expected)
		}
		if row.MayHaveBeenTransmitted != (expected == OutcomePossiblyPending || expected == OutcomeLost || expected == OutcomeEdgeAbsent) {
			t.Errorf("case %q transmitted flag = %v", row.Case, row.MayHaveBeenTransmitted)
		}
	}
}

func readJSON[T any](t *testing.T, path string) T {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return value
}
