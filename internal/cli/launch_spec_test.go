package cli

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
	"github.com/sdougbrown/avenor/internal/runtime"
)

func TestSessionIdentityResolveResumeLaunchSpec(t *testing.T) {
	mapping := newSessionBackendMap()
	authoritative := cliSessionIdentity{
		backend: "poo",
		launch:  backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve"}, Settings: "/etc/poo.yaml"},
	}
	if err := mapping.claim("poo-session", authoritative, &cliSessionAttempt{}, ""); err != nil {
		t.Fatal(err)
	}

	resolved, err := mapping.resolveResume("poo-session", authoritative)
	if err != nil {
		t.Fatalf("matching launch rejected: %v", err)
	}
	if !resolved.launch.Equal(authoritative.launch) {
		t.Fatalf("resolved launch = %+v, want %+v", resolved.launch, authoritative.launch)
	}

	for _, tc := range []struct {
		name   string
		launch backendlaunch.Spec
		want   string
	}{
		{name: "command", launch: backendlaunch.Spec{Command: "/opt/bin/other-poo", Args: []string{"serve"}, Settings: "/etc/poo.yaml"}, want: "command"},
		{name: "args", launch: backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve", "--verbose"}, Settings: "/etc/poo.yaml"}, want: "args"},
		{name: "settings", launch: backendlaunch.Spec{Command: "/opt/bin/umpire-poo", Args: []string{"serve"}, Settings: "/etc/other.yaml"}, want: "settings"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			supplied := cliSessionIdentity{backend: "poo", launch: tc.launch}
			_, err := mapping.resolveResume("poo-session", supplied)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("resolveResume() error = %v, want a %s conflict", err, tc.want)
			}
		})
	}
}

func TestRunRosterPooResolvesNamedCommandLaunch(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var got runtime.StartOptions
	newProvider = func(opts runtime.StartOptions, backend string) (runtime.Provider, error) {
		if backend != "poo" {
			t.Fatalf("backend = %q, want poo", backend)
		}
		got = opts
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	contents := `{"reviewer":{"backend":"poo","command":"/opt/umpire-bot/bin/umpire-poo","args":["serve","--verbose"],"settings":"/etc/umpire-bot/poo-settings.yaml","agent":"search"}}`
	if err := os.WriteFile(rosterPath, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if code := run([]string{
		"--dir", dir,
		"--prompt", "review",
		"--roster-file", rosterPath,
		"--roster-entry", "reviewer",
	}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	want := backendlaunch.Spec{Command: "/opt/umpire-bot/bin/umpire-poo", Args: []string{"serve", "--verbose"}, Settings: "/etc/umpire-bot/poo-settings.yaml"}
	if got.Launch.Command != want.Command || !slices.Equal(got.Launch.Args, want.Args) || got.Launch.Settings != want.Settings {
		t.Fatalf("StartOptions.Launch = %+v, want %+v", got.Launch, want)
	}
}

func TestRunRosterPooResolvesPooOnPath(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var got runtime.StartOptions
	newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		got = opts
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	pathDir := t.TempDir()
	stub := filepath.Join(pathDir, "poo")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"reviewer":{"backend":"poo","agent":"search"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	if code := run([]string{
		"--dir", dir,
		"--prompt", "review",
		"--roster-file", rosterPath,
		"--roster-entry", "reviewer",
	}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	abs, err := filepath.Abs(stub)
	if err != nil {
		t.Fatal(err)
	}
	if got.Launch.Command != abs || !slices.Equal(got.Launch.Args, []string{"serve"}) {
		t.Fatalf("StartOptions.Launch = %+v, want PATH poo at %q with serve args", got.Launch, abs)
	}
}

func TestRunRosterPooMissingBinaryNamesEntry(t *testing.T) {
	oldNewProvider := newProvider
	called := false
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
		called = true
		return newScriptedProvider(), nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })
	t.Setenv("PATH", t.TempDir())

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"umpire-reviewer":{"backend":"poo","agent":"search"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	if code := run([]string{
		"--dir", dir,
		"--prompt", "review",
		"--roster-file", rosterPath,
		"--roster-entry", "umpire-reviewer",
	}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
	}
	if called {
		t.Fatal("a provider was created for a launch that did not resolve")
	}
	if !strings.Contains(stderr.String(), `roster entry "umpire-reviewer"`) || !strings.Contains(stderr.String(), `"poo" not found on PATH`) {
		t.Fatalf("stderr = %q, want the roster entry and missing executable named", stderr.String())
	}
}

func TestRunRosterPooThinkingIsRefused(t *testing.T) {
	oldNewProvider := newProvider
	called := false
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
		called = true
		return newScriptedProvider(), nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"reviewer":{"backend":"poo","command":"/opt/bin/umpire-poo","agent":"search","thinking":"high"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	if code := run([]string{
		"--dir", dir,
		"--prompt", "review",
		"--roster-file", rosterPath,
		"--roster-entry", "reviewer",
	}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
	}
	if called {
		t.Fatal("a provider was created for a poo entry carrying thinking")
	}
	if !strings.Contains(stderr.String(), `backend "poo" does not support parameter "thinking"`) {
		t.Fatalf("stderr = %q, want the shared unsupported-parameter error", stderr.String())
	}
}

func TestRunLoopPhaseRosterPooLaunchReachesStartOptions(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var got runtime.StartOptions
	newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		got = opts
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	dir := t.TempDir()
	rosterPath := filepath.Join(dir, "roster.json")
	if err := os.WriteFile(rosterPath, []byte(`{"reviewer":{"backend":"poo","command":"/opt/bin/umpire-poo","settings":"/etc/poo.yaml","agent":"search"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loopPath := filepath.Join(dir, "loop.json")
	if err := os.WriteFile(loopPath, []byte(`{"pre":[{"name":"review","prompt":"review","roster_entry":"reviewer"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stderr strings.Builder
	if code := run([]string{"--dir", dir, "--loop-file", loopPath, "--roster-file", rosterPath}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	if got.Launch.Command != "/opt/bin/umpire-poo" || !slices.Equal(got.Launch.Args, []string{"serve"}) || got.Launch.Settings != "/etc/poo.yaml" {
		t.Fatalf("phase StartOptions.Launch = %+v, want the roster entry's launch", got.Launch)
	}
}

func TestRunBackendPooWithoutAnEntryResolvesPooOnPath(t *testing.T) {
	oldNewProvider := newProvider
	provider := newScriptedProvider()
	var got runtime.StartOptions
	newProvider = func(opts runtime.StartOptions, _ string) (runtime.Provider, error) {
		got = opts
		return provider, nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })

	pathDir := t.TempDir()
	stub := filepath.Join(pathDir, "poo")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)

	var stderr strings.Builder
	if code := run([]string{
		"--dir", t.TempDir(),
		"--prompt", "review",
		"--backend", "poo",
		"--agent", "search",
	}, func(string) string { return "" }, &stderr); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr=%s", code, stderr.String())
	}
	abs, err := filepath.Abs(stub)
	if err != nil {
		t.Fatal(err)
	}
	if got.Launch.Command != abs || !slices.Equal(got.Launch.Args, []string{"serve"}) {
		t.Fatalf("StartOptions.Launch = %+v, want PATH poo at %q with serve args", got.Launch, abs)
	}
}

func TestRunBackendPooWithoutAnEntryAndNoBinaryFails(t *testing.T) {
	oldNewProvider := newProvider
	called := false
	newProvider = func(runtime.StartOptions, string) (runtime.Provider, error) {
		called = true
		return newScriptedProvider(), nil
	}
	t.Cleanup(func() { newProvider = oldNewProvider })
	t.Setenv("PATH", t.TempDir())

	var stderr strings.Builder
	if code := run([]string{
		"--dir", t.TempDir(),
		"--prompt", "review",
		"--backend", "poo",
		"--agent", "search",
	}, func(string) string { return "" }, &stderr); code != 1 {
		t.Fatalf("run() = %d, want 1; stderr=%s", code, stderr.String())
	}
	if called {
		t.Fatal("a provider was created for a launch that did not resolve")
	}
	if !strings.Contains(stderr.String(), `"poo" not found on PATH`) {
		t.Fatalf("stderr = %q, want the missing executable named", stderr.String())
	}
}
