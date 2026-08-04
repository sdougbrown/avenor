package spawnselection

import (
	"encoding/json"
	"os"
)

// ConformanceCase mirrors a single case in schemas/spawn_selection.conformance.json.
// The Input is kept as raw JSON so the strict ValidateJSON path sees the exact
// on-wire keys (including unknown/deferred/misspelled ones) that a raw boundary
// would. ErrorContains is a substring expected in the evaluator error message
// for invalid cases; it is empty for valid cases.
type ConformanceCase struct {
	Name             string          `json:"name"`
	Input            json.RawMessage `json:"input"`
	RosterConfigured bool            `json:"rosterConfigured"`
	Valid            bool            `json:"valid"`
	StrictOnly       bool            `json:"strictOnly"`
	ErrorContains    string          `json:"errorContains"`
}

// LoadConformanceCases reads and parses the shared conformance fixture at path.
// Both internal/spawnselection tests and external package tests (e.g.
// internal/mcpserver) use this so fixture loading stays in one place.
func LoadConformanceCases(path string) ([]ConformanceCase, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fixture struct {
		Cases []ConformanceCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		return nil, err
	}
	return fixture.Cases, nil
}