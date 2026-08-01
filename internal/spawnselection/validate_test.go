package spawnselection

import "testing"

func TestValidateParityCases(t *testing.T) {
	tests := []struct {
		name             string
		input            Input
		rosterConfigured bool
		wantErr          bool
	}{
		{name: "direct neither supplied", input: Input{}},
		{name: "direct backend only", input: Input{Backend: "agy"}},
		{name: "direct agent only", input: Input{Agent: "reviewer"}},
		{name: "direct model only", input: Input{Model: "provider/model"}},
		{name: "direct valid identity", input: Input{Agent: "reviewer", Model: "provider/model", Backend: "opencode-acp"}},
		{name: "roster pair", input: Input{RosterFile: "/repo/roster.json", RosterEntry: "planner"}},
		{name: "configured roster context", input: Input{RosterEntry: "planner"}, rosterConfigured: true},
		{name: "missing roster entry", input: Input{RosterFile: "/repo/roster.json"}, wantErr: true},
		{name: "missing roster file", input: Input{RosterEntry: "planner"}, wantErr: true},
		{name: "mixed agent", input: Input{RosterFile: "/repo/roster.json", RosterEntry: "planner", Agent: "reviewer"}, wantErr: true},
		{name: "mixed model", input: Input{RosterFile: "/repo/roster.json", RosterEntry: "planner", Model: "provider/model"}, wantErr: true},
		{name: "mixed backend", input: Input{RosterFile: "/repo/roster.json", RosterEntry: "planner", Backend: "agy"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.input, tt.rosterConfigured)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateJSONRejectsDeferredAndMisspelledFields(t *testing.T) {
	for _, data := range []string{
		`{"roster_file":"/repo/roster.json","thinking":"high"}`,
		`{"roster_file":"/repo/roster.json","system":"deferred"}`,
		`{"rosterFile":"/repo/roster.json","roster_entry":"planner"}`,
	} {
		if err := ValidateJSON([]byte(data), false); err == nil {
			t.Fatalf("ValidateJSON(%s) unexpectedly accepted", data)
		}
	}
}
