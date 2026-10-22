package messaging

import (
	"reflect"
	"testing"
)

func TestLabelCandidates(t *testing.T) {
	records := []PeerRecord{
		{RunID: "rt-one", Label: "solo"},
		{RunID: "rt-two", Label: "dup"},
		{RunID: "rt-three", Label: "dup"},
		{RunID: "rt-four", Label: "other"},
	}
	tests := []struct {
		name      string
		ref       string
		want      []string
		wantLabel string
	}{
		{name: "no match", ref: "missing"},
		{name: "one match", ref: "solo", want: []string{"rt-one"}, wantLabel: "solo"},
		// Candidates keep the records' order, which is the broker listing order.
		{name: "many matches", ref: "dup", want: []string{"rt-two", "rt-three"}, wantLabel: "dup"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, label := LabelCandidates(records, tc.ref)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("LabelCandidates(%q) candidates = %#v, want %#v", tc.ref, got, tc.want)
			}
			if label != tc.wantLabel {
				t.Fatalf("LabelCandidates(%q) label = %q, want %q", tc.ref, label, tc.wantLabel)
			}
		})
	}
}
