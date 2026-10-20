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

// outcomeSpecFixture is schemas/message.outcome.json's row shape. It is a test
// shape on purpose: OutcomeRow carries no json tags.
type outcomeSpecFixture struct {
	Outcomes []struct {
		Outcome                string `json:"outcome"`
		Source                 string `json:"source"`
		MayHaveBeenTransmitted bool   `json:"mayHaveBeenTransmitted"`
		Terminal               bool   `json:"terminal"`
		ReuseMessageID         bool   `json:"reuseMessageId"`
	} `json:"outcomes"`
}

func loadOutcomeSpec(t *testing.T) outcomeSpecFixture {
	t.Helper()
	return readJSON[outcomeSpecFixture](t, "../../schemas/message.outcome.json")
}

// outcomeColumns is the four-column projection the vocabulary is compared on.
type outcomeColumns struct {
	Outcome                string
	MayHaveBeenTransmitted bool
	Terminal               bool
	ReuseMessageID         bool
}

// TestOutcomeSpecMatchesVocabulary asserts the messaging half of the outcome
// contract against the vocabulary fixture, which is the vocabulary's authority:
// the same nine outcomes in the same order with the same three flag columns,
// from both the checked-in file and the generated artifact. The only tool-level
// rows are resumed and renamed.
func TestOutcomeSpecMatchesVocabulary(t *testing.T) {
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
	file := loadOutcomeSpec(t)

	var fromFile []outcomeColumns
	var toolOutcomes []string
	for _, row := range file.Outcomes {
		switch row.Source {
		case "tool":
			toolOutcomes = append(toolOutcomes, row.Outcome)
		case "messaging":
			fromFile = append(fromFile, outcomeColumns{row.Outcome, row.MayHaveBeenTransmitted, row.Terminal, row.ReuseMessageID})
		default:
			t.Fatalf("outcome %q declares source %q", row.Outcome, row.Source)
		}
	}
	spec := LoadOutcomeSpec()
	if len(fromFile) != len(vocabulary.Outcomes) || len(spec.Messaging) != len(vocabulary.Outcomes) {
		t.Fatalf("file lists %d and the artifact %d messaging outcomes, vocabulary lists %d",
			len(fromFile), len(spec.Messaging), len(vocabulary.Outcomes))
	}
	for index, want := range vocabulary.Outcomes {
		expected := outcomeColumns{want.Outcome, want.MayHaveBeenTransmitted, want.Terminal, want.ReuseMessageID}
		if fromFile[index] != expected {
			t.Fatalf("outcome %d in the file = %+v, vocabulary = %+v", index, fromFile[index], expected)
		}
		row := spec.Messaging[index]
		got := outcomeColumns{row.Outcome, row.MayHaveBeenTransmitted, row.Terminal, row.ReuseMessageID}
		if got != expected {
			t.Fatalf("outcome %d in the artifact = %+v, vocabulary = %+v", index, got, expected)
		}
	}
	if len(toolOutcomes) != 2 || toolOutcomes[0] != "resumed" || toolOutcomes[1] != "renamed" {
		t.Fatalf("tool-level rows = %v, want [resumed renamed]", toolOutcomes)
	}
	if len(spec.Tool) != 2 || spec.Tool[0].Outcome != "resumed" || spec.Tool[1].Outcome != "renamed" {
		t.Fatalf("generated tool rows = %+v", spec.Tool)
	}
}
