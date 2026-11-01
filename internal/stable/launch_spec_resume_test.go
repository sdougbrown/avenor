package stable

import (
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/runtime"
)

var pooResumeLaunch = backendlaunch.Spec{
	Command:  "/opt/bin/umpire-poo",
	Args:     []string{"serve"},
	Settings: "/etc/poo.yaml",
}

func TestResolveDirectResumeIdentityLaunchSpec(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/poo-launch-resume.sock"})
	sup.rememberSessionIdentity("poo-session", effectiveIdentity{
		Backend: "poo", Agent: "search", RosterFile: "/repo/roster.json", RosterEntry: "reviewer",
		Launch: pooResumeLaunch,
	}, nil)

	resolved, err := sup.resolveDirectResumeIdentity("poo-session",
		effectiveIdentity{Backend: "poo"},
		effectiveIdentity{Backend: "poo", Agent: "search", RosterFile: "/repo/roster.json", RosterEntry: "reviewer", Launch: pooResumeLaunch})
	if err != nil {
		t.Fatalf("matching launch rejected: %v", err)
	}
	if !resolved.Launch.Equal(pooResumeLaunch) {
		t.Fatalf("resolved launch = %+v, want %+v", resolved.Launch, pooResumeLaunch)
	}

	for _, tc := range []struct {
		name   string
		launch backendlaunch.Spec
	}{
		{name: "command", launch: backendlaunch.Spec{Command: "/opt/bin/other-poo", Args: []string{"serve"}, Settings: "/etc/poo.yaml"}},
		{name: "args", launch: backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve", "--verbose"}, Settings: "/etc/poo.yaml"}},
		{name: "settings", launch: backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve"}, Settings: "/etc/other.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sup.resolveDirectResumeIdentity("poo-session",
				effectiveIdentity{Backend: "poo"},
				effectiveIdentity{Backend: "poo", Agent: "search", RosterFile: "/repo/roster.json", RosterEntry: "reviewer", Launch: tc.launch})
			if err == nil || !strings.Contains(err.Error(), tc.name) {
				t.Fatalf("resolveDirectResumeIdentity() error = %v, want a %s conflict", err, tc.name)
			}
		})
	}
}

func TestResolveWorkflowResumeIdentityLaunchSpec(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/poo-launch-workflow-resume.sock"})
	sup.rememberSessionIdentity("phase-session", effectiveIdentity{
		Backend: "poo", Agent: "search", Launch: pooResumeLaunch,
	}, nil)

	if _, err := sup.resolveWorkflowResumeIdentity("phase-session", effectiveIdentity{
		Backend: "poo", Agent: "search", Launch: pooResumeLaunch,
	}); err != nil {
		t.Fatalf("matching launch rejected: %v", err)
	}

	changed := pooResumeLaunch
	changed.Settings = "/etc/other.yaml"
	_, err := sup.resolveWorkflowResumeIdentity("phase-session", effectiveIdentity{
		Backend: "poo", Agent: "search", Launch: changed,
	})
	if err == nil || !strings.Contains(err.Error(), "settings") {
		t.Fatalf("workflow launch mismatch: err = %v, want a settings conflict", err)
	}
}

func TestStableDirectRosterPooLaunchIsRecorded(t *testing.T) {
	sup := newTestSupervisor(t, Config{ControlSocket: "/tmp/poo-launch-record.sock", MaxRuntimes: 1})
	provider := scriptedStage5Provider("ses_poo", "end_turn")
	var got runtime.StartOptions
	sup.newProviderFunc = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		if backend != "poo" {
			t.Fatalf("backend = %q, want poo", backend)
		}
		got = opts
		return provider, nil
	}

	dir := t.TempDir()
	rosterPath := writeStage5Roster(t, dir, `{"reviewer":{"backend":"poo","command":"/opt/bin/umpire-poo","args":["serve"],"settings":"/etc/poo.yaml","agent":"search"}}`)
	result, err := sup.spawn(SpawnParams{Prompt: "review", Dir: dir, RosterFile: rosterPath, RosterEntry: "reviewer"})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	defer sup.cancelRuntime(result.RuntimeID)

	want := backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve"}, Settings: "/etc/poo.yaml"}
	if !got.Launch.Equal(want) {
		t.Fatalf("StartOptions.Launch = %+v, want %+v", got.Launch, want)
	}
	identity, ok := sup.sessionIdentity("ses_poo")
	if !ok || !identity.Launch.Equal(want) {
		t.Fatalf("session identity launch = %+v, ok=%v; want %+v", identity.Launch, ok, want)
	}
	sup.controlMu.Lock()
	child := sup.runtimes[result.RuntimeID]
	sup.controlMu.Unlock()
	if child == nil {
		t.Fatal("runtime was not registered")
	}
	child.mu.Lock()
	childLaunch := child.launch
	child.mu.Unlock()
	if !childLaunch.Equal(want) {
		t.Fatalf("childRuntime.launch = %+v, want %+v", childLaunch, want)
	}
}
