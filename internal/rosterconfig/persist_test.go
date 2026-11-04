package rosterconfig

import "testing"

// A roster entry's persisted-session choice carries into the resolved
// selection, and a selection without an entry keeps nil so the environment
// decides.
func TestResolveCarriesPersistSessions(t *testing.T) {
	on := true
	off := false
	selection, err := Resolve(ResolveInput{
		Backend: "pi",
		Roster:  &Entry{Backend: "pi", Model: "m", PersistSessions: &on},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if selection.PersistSessions == nil || !*selection.PersistSessions {
		t.Fatalf("resolved PersistSessions = %v, want true", selection.PersistSessions)
	}
	selection, err = Resolve(ResolveInput{
		Backend: "pi",
		Roster:  &Entry{Backend: "pi", Model: "m", PersistSessions: &off},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if selection.PersistSessions == nil || *selection.PersistSessions {
		t.Fatalf("resolved PersistSessions = %v, want false", selection.PersistSessions)
	}
	selection, err = Resolve(ResolveInput{Backend: "pi"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if selection.PersistSessions != nil {
		t.Fatalf("resolved PersistSessions = %v, want nil without a roster entry", selection.PersistSessions)
	}
}

// MergePersistChoice is the one home of the tri-state layering every
// persist-sessions merge goes through: the explicit request wins, the
// roster entry's choice fills the gap, and nil on both inherits the
// environment.
func TestMergePersistChoice(t *testing.T) {
	on := true
	off := false
	cases := []struct {
		name    string
		request *bool
		entry   *bool
		want    *bool
	}{
		{name: "explicit request wins over the entry", request: &off, entry: &on, want: &off},
		{name: "explicit persist wins over an ephemeral entry", request: &on, entry: &off, want: &on},
		{name: "entry fills the gap", request: nil, entry: &on, want: &on},
		{name: "ephemeral entry fills the gap", request: nil, entry: &off, want: &off},
		{name: "request without an entry", request: &on, entry: nil, want: &on},
		{name: "nil on both inherits the environment", request: nil, entry: nil, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MergePersistChoice(tc.request, tc.entry)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("MergePersistChoice = %v, want nil", got)
				}
				return
			}
			if got == nil || *got != *tc.want {
				t.Fatalf("MergePersistChoice = %v, want %v", got, *tc.want)
			}
		})
	}
}
