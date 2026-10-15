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
	for _, input := range []string{
		`{"type":"run","prompt":"do it","timeout":900}`,
		`{"type":"loop","loop_file":"loop.json","timeout":600}`,
		`{"type":"team","team_file":"team.json","timeout":300}`,
	} {
		var action Action
		if err := json.Unmarshal([]byte(input), &action); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", input, err)
		}
		data, err := json.Marshal(action)
		if err != nil {
			t.Fatalf("Marshal(%s) error = %v", input, err)
		}
		if !strings.Contains(string(data), `"timeout":`) {
			t.Fatalf("Marshal(%s) = %s, want the timeout field on the wire", input, data)
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
