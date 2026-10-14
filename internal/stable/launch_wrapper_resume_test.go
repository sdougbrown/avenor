package stable

import (
	"slices"
	"strings"
	"testing"
)

func TestResolveDirectResumeIdentityLaunchWrapper(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/launch-wrapper-resume.sock"})
	sup.rememberSessionIdentity("wrap-session", effectiveIdentity{
		Backend: "pi", Agent: "planner", Model: "planner-model", LaunchWrapper: []string{"nice", "-n", "5"},
	}, nil)

	// Same wrapper resumes cleanly.
	resolved, err := sup.resolveDirectResumeIdentity("wrap-session",
		effectiveIdentity{Backend: "pi", LaunchWrapper: []string{"nice", "-n", "5"}},
		effectiveIdentity{Backend: "pi", LaunchWrapper: []string{"nice", "-n", "5"}})
	if err != nil {
		t.Fatalf("matching wrapper rejected: %v", err)
	}
	if !slices.Equal(resolved.LaunchWrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("resolved wrapper = %#v, want [nice -n 5]", resolved.LaunchWrapper)
	}

	// An omitted wrapper inherits the authoritative one.
	resolved, err = sup.resolveDirectResumeIdentity("wrap-session",
		effectiveIdentity{Backend: "pi"},
		effectiveIdentity{Backend: "pi"})
	if err != nil {
		t.Fatalf("omitted wrapper rejected: %v", err)
	}
	if !slices.Equal(resolved.LaunchWrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("inherited wrapper = %#v, want [nice -n 5]", resolved.LaunchWrapper)
	}

	// A different wrapper is a conflict, like backend/agent/model.
	_, err = sup.resolveDirectResumeIdentity("wrap-session",
		effectiveIdentity{Backend: "pi", LaunchWrapper: []string{"firejail"}},
		effectiveIdentity{Backend: "pi", LaunchWrapper: []string{"firejail"}})
	if err == nil || !strings.Contains(err.Error(), "launch_wrapper") {
		t.Fatalf("wrapper mismatch: err = %v, want launch_wrapper conflict", err)
	}
}

func TestResolveDirectResumeIdentityRosterLaunchWrapper(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/launch-wrapper-roster-resume.sock"})
	sup.rememberSessionIdentity("roster-wrap-session", effectiveIdentity{
		Backend: "pi", Agent: "planner", Model: "planner-model",
		RosterFile: "/repo/roster.json", RosterEntry: "planner",
		LaunchWrapper: []string{"nice", "-n", "5"},
	}, nil)

	// Roster entries do not carry wrappers, so an omitted wrapper on a
	// roster resume inherits the authoritative one instead of conflicting.
	resolved, err := sup.resolveDirectResumeIdentity("roster-wrap-session",
		effectiveIdentity{Backend: "pi", Agent: "planner", Model: "planner-model", RosterFile: "/repo/roster.json", RosterEntry: "planner"},
		effectiveIdentity{Backend: "pi", Agent: "planner", Model: "planner-model", RosterFile: "/repo/roster.json", RosterEntry: "planner"})
	if err != nil {
		t.Fatalf("omitted wrapper rejected on roster resume: %v", err)
	}
	if !slices.Equal(resolved.LaunchWrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("inherited wrapper = %#v, want [nice -n 5]", resolved.LaunchWrapper)
	}

	// A supplied wrapper that differs still conflicts, like the direct path.
	_, err = sup.resolveDirectResumeIdentity("roster-wrap-session",
		effectiveIdentity{Backend: "pi", Agent: "planner", Model: "planner-model", RosterFile: "/repo/roster.json", RosterEntry: "planner", LaunchWrapper: []string{"firejail"}},
		effectiveIdentity{Backend: "pi", Agent: "planner", Model: "planner-model", RosterFile: "/repo/roster.json", RosterEntry: "planner", LaunchWrapper: []string{"firejail"}})
	if err == nil || !strings.Contains(err.Error(), "launch_wrapper") {
		t.Fatalf("roster wrapper mismatch: err = %v, want launch_wrapper conflict", err)
	}
}

func TestResolveWorkflowResumeIdentityLaunchWrapperConflict(t *testing.T) {
	sup := NewSupervisor(Config{ControlSocket: "/tmp/launch-wrapper-workflow-resume.sock"})
	sup.rememberSessionIdentity("phase-session", effectiveIdentity{
		Backend: "pi", Agent: "planner", Model: "planner-model", LaunchWrapper: []string{"nice"},
	}, nil)

	_, err := sup.resolveWorkflowResumeIdentity("phase-session", effectiveIdentity{
		Backend: "pi", Agent: "planner", Model: "planner-model", LaunchWrapper: []string{"firejail"},
	})
	if err == nil || !strings.Contains(err.Error(), "launch_wrapper") {
		t.Fatalf("workflow wrapper mismatch: err = %v, want launch_wrapper conflict", err)
	}

	if _, err := sup.resolveWorkflowResumeIdentity("phase-session", effectiveIdentity{
		Backend: "pi", Agent: "planner", Model: "planner-model", LaunchWrapper: []string{"nice"},
	}); err != nil {
		t.Fatalf("workflow resume with the same wrapper rejected: %v", err)
	}
}
