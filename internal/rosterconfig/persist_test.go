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
