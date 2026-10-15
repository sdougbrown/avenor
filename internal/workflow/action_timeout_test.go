package workflow

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestActionTimeoutRoundTripAndValidation covers the action timeout field that
// gives workflow-controlled run, loop, and team actions an explicit enforceable
// execution deadline.
func TestActionTimeoutRoundTripAndValidation(t *testing.T) {
	for _, tc := range []struct {
		input, wantTimeout string
	}{
		{`{"type":"run","prompt":"do it","timeout":900}`, `"timeout":900`},
		{`{"type":"loop","loop_file":"loop.json","timeout":600}`, `"timeout":600`},
		{`{"type":"team","team_file":"team.json","timeout":300}`, `"timeout":300`},
	} {
		var action Action
		if err := json.Unmarshal([]byte(tc.input), &action); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", tc.input, err)
		}
		data, err := json.Marshal(action)
		if err != nil {
			t.Fatalf("Marshal(%s) error = %v", tc.input, err)
		}
		// The exact value is pinned: omitempty means a zeroed or dropped
		// timeout must fail, not just a missing key.
		if !strings.Contains(string(data), tc.wantTimeout) {
			t.Fatalf("Marshal(%s) = %s, want it to contain %s", tc.input, data, tc.wantTimeout)
		}
	}
	for _, input := range []string{
		`{"type":"run","prompt":"do it","timeout":-1}`,
		`{"type":"loop","loop_file":"loop.json","timeout":-1}`,
		`{"type":"team","team_file":"team.json","timeout":-1}`,
	} {
		var action Action
		if err := json.Unmarshal([]byte(input), &action); err == nil {
			t.Fatalf("negative timeout unexpectedly accepted: %s", input)
		}
	}
}
