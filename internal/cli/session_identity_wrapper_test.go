package cli

import (
	"slices"
	"strings"
	"testing"
)

func TestSessionIdentityResolveResumeLaunchWrapper(t *testing.T) {
	mapping := newSessionBackendMap()
	owner := &cliSessionAttempt{}
	authoritative := cliSessionIdentity{backend: "pi", launchWrapper: []string{"nice", "-n", "5"}}
	if err := mapping.claim("wrap-session", authoritative, owner, ""); err != nil {
		t.Fatal(err)
	}

	// Same wrapper resumes cleanly.
	resolved, err := mapping.resolveResume("wrap-session", cliSessionIdentity{backend: "pi", launchWrapper: []string{"nice", "-n", "5"}})
	if err != nil {
		t.Fatalf("matching wrapper rejected: %v", err)
	}
	if !slices.Equal(resolved.launchWrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("resolved wrapper = %#v, want [nice -n 5]", resolved.launchWrapper)
	}

	// A different wrapper is a conflict, like backend/agent/model.
	_, err = mapping.resolveResume("wrap-session", cliSessionIdentity{backend: "pi", launchWrapper: []string{"firejail"}})
	if err == nil || !strings.Contains(err.Error(), "launch_wrapper") {
		t.Fatalf("wrapper mismatch: err = %v, want launch_wrapper conflict", err)
	}
}
