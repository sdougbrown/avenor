package spawnselection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Input contains the selector fields accepted by a direct spawn request.
type Input struct {
	Agent       string `json:"agent"`
	Model       string `json:"model"`
	Backend     string `json:"backend"`
	RosterFile  string `json:"roster_file"`
	RosterEntry string `json:"roster_entry"`
}

// Validate checks a direct or roster spawn selector. An empty roster file is
// permitted when rosterConfigured is true, because the entry can be resolved
// from that configured context.
func Validate(input Input, rosterConfigured bool) error {
	fields := SpawnSelectionFields{
		Agent:       present(input.Agent),
		Model:       present(input.Model),
		Backend:     present(input.Backend),
		RosterFile:  present(input.RosterFile),
		RosterEntry: present(input.RosterEntry),
	}
	availability := Check(
		fields,
		SpawnSelectionConditions{RosterConfigured: rosterConfigured},
		SpawnSelectionFields{},
	)

	for _, supplied := range []struct {
		name  string
		value string
		state FieldStatus
	}{
		{name: "agent", value: input.Agent, state: availability.Agent},
		{name: "model", value: input.Model, state: availability.Model},
		{name: "backend", value: input.Backend, state: availability.Backend},
		{name: "roster_file", value: input.RosterFile, state: availability.RosterFile},
		{name: "roster_entry", value: input.RosterEntry, state: availability.RosterEntry},
	} {
		if supplied.value == "" || (supplied.state.Enabled && supplied.state.Fair) {
			continue
		}
		if supplied.state.Reason == nil {
			return fmt.Errorf("invalid spawn selector: %s is disabled", supplied.name)
		}
		if !supplied.state.Enabled && input.RosterEntry != "" {
			// Two rules disable a field: roster_file's requires (no entry selected)
			// and the roster-mode disables (an entry selected). Only the second
			// blocks a direct identity field, and its reason describes the rule
			// rather than the field, so name the field the caller has to drop.
			return fmt.Errorf("invalid spawn selector: %s is a direct identity field, disabled in roster mode (roster_entry is set); drop %s to use the roster entry, or drop roster_file and roster_entry to use direct identity", supplied.name, supplied.name)
		}
		return fmt.Errorf("invalid spawn selector: %s", *supplied.state.Reason)
	}

	return nil
}

func present(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// ValidateJSON strictly decodes a selector before applying Validate. It is
// useful at wire boundaries where misspelled and deferred fields must not be
// silently ignored.
func ValidateJSON(data []byte, rosterConfigured bool) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var input Input
	if err := decoder.Decode(&input); err != nil {
		return fmt.Errorf("invalid spawn selector: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("invalid spawn selector: multiple JSON values")
		}
		return fmt.Errorf("invalid spawn selector: %w", err)
	}
	return Validate(input, rosterConfigured)
}
