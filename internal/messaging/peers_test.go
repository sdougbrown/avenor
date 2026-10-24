package messaging

import (
	"reflect"
	"testing"
)

// TestDecodePeerRecordsKeepsThePeerID pins that a session record's own
// run_id cannot rename the peer the listing names, so a peer cannot claim
// another run's id.
func TestDecodePeerRecordsKeepsThePeerID(t *testing.T) {
	records, err := DecodePeerRecords([]Peer{
		{RunID: "real-peer", Session: []byte(`{"run_id":"other-run","label":"spoofed"}`)},
		{RunID: "no-record", Session: nil},
	})
	if err != nil {
		t.Fatalf("DecodePeerRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %+v, want two peers", records)
	}
	if records[0].RunID != "real-peer" {
		t.Fatalf("record run id = %q, want the peer's id", records[0].RunID)
	}
	if records[0].Label != "spoofed" {
		t.Fatalf("record label = %q, want the record's own metadata", records[0].Label)
	}
	if records[1].RunID != "no-record" {
		t.Fatalf("record run id = %q, want the peer's id", records[1].RunID)
	}
}

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
